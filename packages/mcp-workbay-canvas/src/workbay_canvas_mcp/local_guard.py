"""Local-loopback ASGI reference monitor for canvas HTTP (WBC-06 S8)."""

from __future__ import annotations

import hmac
import ipaddress
import secrets
from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass
from typing import Any

from workbay_canvas_mcp.http_api import _error_response

SESSION_HEADER = "X-Workbay-Canvas-Session"
_SESSION_HEADER_BYTES = SESSION_HEADER.lower().encode("ascii")
_SESSION_MESSAGE = "Session token missing or invalid. Reload the page to start a new session."
_SAFE_METHODS = frozenset({"GET", "HEAD"})
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]


def _host_with_port(host: str, port: int) -> str:
    value = host.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        return f"{value.lower()}:{port}"
    if parsed.version == 6:
        return f"[{parsed.compressed}]:{port}"
    return f"{parsed.compressed}:{port}"


def _header_values(scope: Scope, name: bytes) -> list[str]:
    values: list[str] = []
    for key, raw in scope.get("headers") or []:
        if bytes(key).lower() != name:
            continue
        values.append(bytes(raw).decode("latin-1"))
    return values


def _header_name(raw: bytes | str) -> bytes:
    if isinstance(raw, str):
        return raw.encode("latin-1")
    return bytes(raw)


def _header_value(raw: bytes | str) -> bytes:
    if isinstance(raw, str):
        return raw.encode("latin-1")
    return bytes(raw)


def _response_media_type(headers: list[tuple[bytes, bytes]]) -> str:
    for name, value in headers:
        if name.lower() == b"content-type":
            return value.decode("latin-1").split(";", 1)[0].strip().lower()
    return ""


def _body_is_nonempty(scope: Scope) -> bool:
    for raw in _header_values(scope, b"content-length"):
        try:
            if int(raw) > 0:
                return True
        except ValueError:
            continue
    for raw in _header_values(scope, b"transfer-encoding"):
        parts = {part.strip().lower() for part in raw.split(",")}
        if "chunked" in parts:
            return True
    return False


def _request_media_type(scope: Scope) -> str:
    values = _header_values(scope, b"content-type")
    if not values:
        return ""
    return values[0].split(";", 1)[0].strip().lower()


def _route_path(scope: Scope) -> str:
    """Path Starlette routes on: `scope["path"]` with `root_path` stripped when it is a prefix."""
    path = scope.get("path") or ""
    root_path = scope.get("root_path") or ""
    if not root_path:
        return path
    if not path.startswith(root_path):
        return path
    if path == root_path:
        return ""
    if path[len(root_path)] == "/":
        return path[len(root_path) :]
    return path


def _is_mutating_api(scope: Scope) -> bool:
    path = _route_path(scope)
    method = str(scope.get("method") or "").upper()
    return path.startswith("/api/") and method not in _SAFE_METHODS


def _wrap_send(send: Send) -> Send:
    async def wrapped(message: MutableMapping[str, Any]) -> None:
        if message.get("type") != "http.response.start":
            await send(message)
            return
        headers: list[tuple[bytes, bytes]] = []
        present: set[bytes] = set()
        for raw_name, raw_value in message.get("headers") or []:
            name = _header_name(raw_name)
            value = _header_value(raw_value)
            headers.append((name, value))
            present.add(name.lower())

        def add(name: bytes, value: bytes) -> None:
            lowered = name.lower()
            if lowered in present:
                return
            headers.append((name, value))
            present.add(lowered)

        add(b"x-content-type-options", b"nosniff")
        add(b"cross-origin-resource-policy", b"same-origin")
        if _response_media_type(headers) == "text/html":
            add(b"x-frame-options", b"DENY")
            add(b"content-security-policy", b"frame-ancestors 'none'")
        outgoing = dict(message)
        outgoing["headers"] = headers
        await send(outgoing)

    return wrapped


@dataclass(frozen=True)
class LocalRequestPolicy:
    port: int
    session_token: str
    bind_host: str = "127.0.0.1"
    allow_origins: tuple[str, ...] = ()

    @staticmethod
    def new_token() -> str:
        return secrets.token_urlsafe(32)

    @property
    def allowed_hosts(self) -> frozenset[str]:
        hosts = {_host_with_port(host, self.port) for host in _LOOPBACK_HOSTS}
        hosts.add(_host_with_port(self.bind_host, self.port))
        return frozenset(host.lower() for host in hosts)

    @property
    def allowed_origins(self) -> frozenset[str]:
        generated = {f"http://{host}" for host in self.allowed_hosts}
        generated.update(self.allow_origins)
        return frozenset(generated)


class LocalRequestGuard:
    """Pure ASGI reference monitor for loopback Host, Origin, and session checks."""

    def __init__(self, app: Callable[..., Any], *, policy: LocalRequestPolicy) -> None:
        self.app = app
        self.policy = policy

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope_type == "websocket":
            await send({"type": "websocket.close"})
            return
        if scope_type != "http":
            return

        send = _wrap_send(send)
        hosts = _header_values(scope, b"host")
        if len(hosts) != 1 or hosts[0].lower() not in self.policy.allowed_hosts:
            await self._refuse(
                scope,
                receive,
                send,
                status=403,
                code="host_not_allowed",
                message="Host header is not allowed.",
            )
            return

        origins = _header_values(scope, b"origin")
        if any(origin not in self.policy.allowed_origins for origin in origins):
            await self._refuse(
                scope,
                receive,
                send,
                status=403,
                code="origin_not_allowed",
                message="Origin is not allowed.",
            )
            return

        if _is_mutating_api(scope):
            session_values = _header_values(scope, _SESSION_HEADER_BYTES)
            if not session_values:
                await self._refuse(
                    scope,
                    receive,
                    send,
                    status=403,
                    code="session_required",
                    message=_SESSION_MESSAGE,
                    details={"header": SESSION_HEADER},
                )
                return
            submitted = session_values[0] if len(session_values) == 1 else ""
            try:
                token_ok = hmac.compare_digest(submitted, self.policy.session_token)
            except (TypeError, ValueError):
                token_ok = False
            if not token_ok:
                await self._refuse(
                    scope,
                    receive,
                    send,
                    status=403,
                    code="session_invalid",
                    message=_SESSION_MESSAGE,
                    details={"header": SESSION_HEADER},
                )
                return

            if _body_is_nonempty(scope) and _request_media_type(scope) != "application/json":
                await self._refuse(
                    scope,
                    receive,
                    send,
                    status=415,
                    code="unsupported_media_type",
                    message="Request content type must be application/json.",
                )
                return

        await self.app(scope, receive, send)

    async def _refuse(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        status: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        response = _error_response(status=status, code=code, message=message, details=details)
        await response(scope, receive, send)
