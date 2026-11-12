"""Loopback launcher for the canvas web app (WBC-06 S10)."""

from __future__ import annotations

import contextlib
import errno
import http.client
import io
import ipaddress
import json
import re
import socket
import sys
import time
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Any, cast

import uvicorn
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from workbay_canvas_mcp.handoff_reader import default_handoff_reader
from workbay_canvas_mcp.http_api import _error_response
from workbay_canvas_mcp.lane_reader import default_lane_reader
from workbay_canvas_mcp.local_guard import SESSION_HEADER, LocalRequestGuard, LocalRequestPolicy
from workbay_canvas_mcp.reconcile_reader import default_reconcile_readers
from workbay_canvas_mcp.server import WORKSPACE_DB_RELPATH, build_server, resolve_db_path

_ENCODED_UNSAFE = re.compile(r"%00|%2e|%2f|%5c", re.IGNORECASE)
_MIME_BY_SUFFIX = {
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".ico": "image/x-icon",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".map": "application/json; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".txt": "text/plain; charset=utf-8",
    ".woff2": "font/woff2",
}
_LAUNCH_MESSAGES = {
    "remote_bind_refused": "Refusing to bind to a non-loopback host.",
    "origin_not_loopback": "Allow-origin must be an http loopback URL with an explicit port and no path.",
    "web_assets_missing": (
        "Web assets are missing. Reinstall the Canvas package, or pass --web-dist to a directory that contains index.html."
    ),
    "port_in_use": "Port is already in use.",
    "bind_failed": "Bind failed. Try --host 127.0.0.1.",
}
_PORT_IN_USE_HINT = "use --port 0 for a free port"
_HOLDER_PROBE_SECONDS = 1.0
_HOLDER_PROBE_BYTES = 1024 * 1024


class LaunchError(Exception):
    """Typed launch refusal with a JSON-serializable envelope."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message if message is not None else _LAUNCH_MESSAGES.get(code, code)
        self.details: dict[str, Any] = dict(details) if details else {}
        super().__init__(self.message)


def check_bind_host(host: str) -> str:
    value = host.strip()
    if value.lower() == "localhost":
        return "127.0.0.1"
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        raise LaunchError("remote_bind_refused") from None
    if not parsed.is_loopback:
        raise LaunchError("remote_bind_refused")
    return parsed.compressed


def launch_url(host: str, port: int) -> str:
    try:
        parsed = ipaddress.ip_address(host)
    except ValueError:
        display = host
    else:
        display = f"[{parsed.compressed}]" if parsed.version == 6 else parsed.compressed
    return f"http://{display}:{port}/"


def _require_loopback_origin(origin: str) -> None:
    parsed = urllib.parse.urlsplit(origin)
    if (
        parsed.scheme != "http"
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or parsed.port is None
    ):
        raise LaunchError("origin_not_loopback")
    hostname = parsed.hostname
    if hostname.lower() == "localhost":
        return
    try:
        if ipaddress.ip_address(hostname).is_loopback:
            return
    except ValueError:
        pass
    raise LaunchError("origin_not_loopback")


def _require_web_index(web_dist: Path) -> None:
    index = web_dist / "index.html"
    try:
        present = index.is_file()
    except OSError:
        present = False
    if not present:
        raise LaunchError("web_assets_missing")


def _probe_existing_server(host: str, port: int) -> dict[str, Any] | None:
    """One read-only snapshot GET, with a total deadline and no redirects/proxy.

    Read raw bytes under the deadline before parsing HTTP in memory, so even
    trickled headers/body cannot extend the timeout. Never resolve a hostname.
    """
    if not ipaddress.ip_address(host).is_loopback or not port:
        return None
    url = launch_url(host, port)
    authority = urllib.parse.urlsplit(url).netloc
    deadline = time.monotonic() + _HOLDER_PROBE_SECONDS
    try:
        with socket.create_connection((host, port), timeout=_HOLDER_PROBE_SECONDS) as connection:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            connection.settimeout(remaining)
            connection.sendall(
                f"GET /api/fleet/snapshot HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n".encode("ascii")
            )
            wire = bytearray()
            while len(wire) <= _HOLDER_PROBE_BYTES:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                connection.settimeout(remaining)
                chunk = connection.recv(min(65536, _HOLDER_PROBE_BYTES + 1 - len(wire)))
                if not chunk:
                    break
                wire.extend(chunk)
            if len(wire) > _HOLDER_PROBE_BYTES:
                return None

        class BufferedSocket:
            def makefile(self, _mode: str) -> io.BytesIO:
                return io.BytesIO(wire)

        # HTTPResponse only needs makefile; no second socket is opened here.
        with http.client.HTTPResponse(cast(socket.socket, BufferedSocket())) as response:
            response.begin()
            if response.status != 200:
                return None
            document = json.loads(response.read(_HOLDER_PROBE_BYTES + 1))
        if (
            not isinstance(document, dict)
            or type(document.get("schema_version")) is not int
            or document["schema_version"] != 1
            or document.get("state") not in ("fresh", "partial", "stale", "unknown")
            or not isinstance(document.get("lanes"), list)
            or not isinstance(document.get("passes"), list)
        ):
            return None
        existing = {"url": url + "#/fleet", "kind": "fleet"}
        if isinstance(document.get("root"), str):
            existing["root"] = document["root"]
        return existing
    except (OSError, ValueError, http.client.HTTPException, RecursionError):
        return None


def _prebind(host: str, port: int, *, port_explicit: bool = True) -> socket.socket:
    family = socket.AF_INET6 if ipaddress.ip_address(host).version == 6 else socket.AF_INET
    sock: socket.socket | None = None
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if family == socket.AF_INET6:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            sock.bind((host, port, 0, 0))
        else:
            sock.bind((host, port))
        sock.listen()
        return sock
    except OSError as exc:
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.close()
        if exc.errno == errno.EADDRINUSE:
            existing = _probe_existing_server(host, port) if port_explicit and port else None
            raise LaunchError("port_in_use", details={"existing_server": existing}) from exc
        symbol = errno.errorcode.get(exc.errno or 0, f"errno {exc.errno}")
        raise LaunchError(
            "bind_failed",
            f"Bind failed ({symbol}). Try --host 127.0.0.1.",
        ) from exc


def _not_found() -> JSONResponse:
    return _error_response(status=404, code="not_found", message="Not found.")


def _asset_rel_refused(rest: str) -> bool:
    if rest.startswith("/") or rest.startswith("\\") or "\x00" in rest or "\\" in rest:
        return True
    if rest == "":
        return True
    segments = rest.split("/")
    if any(part == "" or part == "." or part == ".." for part in segments):
        return True
    suffix = Path(segments[-1]).suffix.lower()
    return suffix not in _MIME_BY_SUFFIX


def _asset_url_refused(raw_path: bytes, url_path: str) -> bool:
    if b"\x00" in raw_path or "\x00" in url_path:
        return True
    if b"\\" in raw_path or "\\" in url_path:
        return True
    try:
        raw_text = raw_path.decode("ascii")
    except UnicodeDecodeError:
        return True
    if _ENCODED_UNSAFE.search(raw_text):
        return True
    decoded = urllib.parse.unquote(raw_text)
    if "\x00" in decoded or "\\" in decoded:
        return True
    if not decoded.startswith("/assets/"):
        return True
    return _asset_rel_refused(decoded[len("/assets/") :])


def _open_asset(web_dist: Path, rest: str) -> tuple[bytes, str] | None:
    try:
        web_root = web_dist.resolve()
        assets_root = (web_root / "assets").resolve()
        candidate = web_root / "assets" / rest
        if candidate.is_symlink():
            return None
        resolved = candidate.resolve()
    except OSError:
        return None
    try:
        resolved.relative_to(assets_root)
        resolved.relative_to(web_root)
    except ValueError:
        return None
    if not resolved.is_file():
        return None
    media = _MIME_BY_SUFFIX.get(resolved.suffix.lower())
    if media is None:
        return None
    try:
        data = resolved.read_bytes()
    except OSError:
        return None
    return data, media


def build_local_http_app(
    *,
    workspace_root: str | Path,
    db_path: str | Path,
    port: int,
    web_dist: str | Path | None,
    allow_origins: tuple[str, ...] | list[str] = (),
    bind_host: str = "127.0.0.1",
    session_token: str | None = None,
) -> Any:
    token = session_token if session_token is not None else LocalRequestPolicy.new_token()
    origins = tuple(allow_origins)
    server = build_server(
        db_path=db_path,
        workspace_root=workspace_root,
        handoff_reader=default_handoff_reader,
        lane_reader=default_lane_reader,
        reconcile_readers=default_reconcile_readers(),
    )

    from workbay_canvas_mcp.fleet_http import register_fleet_api

    register_fleet_api(server, workspace_root=workspace_root)

    @server.custom_route("/api/session", methods=["GET"])
    async def api_session(_request: Request) -> Response:
        return JSONResponse(
            {"session_token": token, "header": SESSION_HEADER},
            headers={"Cache-Control": "no-store"},
        )

    if web_dist is not None:
        dist = Path(web_dist)

        @server.custom_route("/", methods=["GET", "HEAD"])
        async def serve_index(_request: Request) -> Response:
            try:
                data = (dist / "index.html").read_bytes()
            except OSError:
                return _error_response(
                    status=503,
                    code="web_assets_missing",
                    message="Web assets are missing.",
                )
            return Response(
                content=data,
                headers={
                    "content-type": "text/html; charset=utf-8",
                    "cache-control": "no-cache",
                },
            )

        @server.custom_route("/assets/{path:path}", methods=["GET", "HEAD"])
        async def serve_asset(request: Request) -> Response:
            raw_path = bytes(request.scope.get("raw_path") or b"")
            url_path = str(request.scope.get("path") or "")
            captured = request.path_params.get("path")
            if _asset_url_refused(raw_path, url_path):
                return _not_found()
            if not isinstance(captured, str) or _asset_rel_refused(captured):
                return _not_found()
            opened = _open_asset(dist, captured)
            if opened is None:
                return _not_found()
            data, media = opened
            return Response(
                content=data,
                headers={
                    "content-type": media,
                    "cache-control": "public, max-age=31536000, immutable",
                },
            )

    policy = LocalRequestPolicy(
        port=port,
        session_token=token,
        bind_host=bind_host,
        allow_origins=origins,
    )
    return server.http_app(
        transport="http",
        middleware=[Middleware(LocalRequestGuard, policy=policy)],
    )


def _serve(app: Any, sock: socket.socket) -> None:
    uvicorn.Server(uvicorn.Config(app, log_level="warning")).run(sockets=[sock])


def run_local(
    *,
    workspace_root: str | Path | None,
    db_path: str | Path | None,
    host: str,
    port: int,
    web_dist: str | Path | None,
    port_explicit: bool = True,
    allow_origins: tuple[str, ...] | list[str] = (),
    pin_workspace_db: bool = False,
    view: str | None = None,
    open_browser: bool = False,
) -> int:
    try:
        root = Path(workspace_root) if workspace_root is not None else Path.cwd()
        if pin_workspace_db:
            resolved_db = Path(db_path) if db_path is not None else root / WORKSPACE_DB_RELPATH
        else:
            resolved_db = resolve_db_path(db_path=db_path, workspace_root=root)
        bind_host = check_bind_host(host)
        origins = tuple(allow_origins)
        for origin in origins:
            _require_loopback_origin(origin)
        dist: Path | None
        if web_dist is None:
            dist = None
        else:
            dist = Path(web_dist)
            _require_web_index(dist)
        # Port zero has no holder to identify; retain the two-argument free-port
        # bind hook while forwarding the policy for ports that can have a holder.
        sock = (
            _prebind(bind_host, port, port_explicit=port_explicit) if port else _prebind(bind_host, port)
        )
        try:
            bound_port = int(sock.getsockname()[1])
            app = build_local_http_app(
                workspace_root=root,
                db_path=resolved_db,
                port=bound_port,
                web_dist=dist,
                allow_origins=origins,
                bind_host=bind_host,
            )
            # The shell resolves '#' to its existing default map selection.
            fragment = {None: "", "flows": "#", "library": "#/library/components", "lab": "#/lab", "fleet": "#/fleet"}[view]
            url = launch_url(bind_host, bound_port) + fragment
            sys.stdout.write(json.dumps({"url": url}) + "\n")
            sys.stdout.flush()
            if open_browser:
                try:
                    webbrowser.open(url)
                except (webbrowser.Error, OSError):
                    print("workbay studio: could not open the browser; use the printed URL", file=sys.stderr)
            _serve(app, sock)
        finally:
            with contextlib.suppress(OSError):
                sock.close()
        return 0
    except LaunchError as exc:
        if exc.code == "port_in_use":
            existing = exc.details.get("existing_server")
            hint = (
                f"already serving at {existing['url']}; open it, stop that process, or pass --port 0"
                if existing
                else _PORT_IN_USE_HINT
            )
            sys.stderr.write(hint + "\n")
        sys.stdout.write(
            json.dumps({"error": {"code": exc.code, "message": exc.message, "details": exc.details}}) + "\n"
        )
        sys.stdout.flush()
        return 2
