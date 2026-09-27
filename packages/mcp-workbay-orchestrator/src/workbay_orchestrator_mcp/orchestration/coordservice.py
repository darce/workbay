"""Trusted local MCP and one-shot routes to the shared claims/channels authority.

The launcher boundary is cooperative and same-UID; this module adds no auth or
network listener. Admission is bounded per process (4 running workers and 16
queued calls). A deadline covers request admission, queueing, and execution. A
timed-out filesystem call cannot be killed: its response is classified as
unknown and its admission slot remains occupied until the worker really exits.
Python cannot hard-cancel a thread blocked in an OS or storage call; process
shutdown can therefore wait for such calls to return.

Design references: [DATA-14]/[GRPH-14]/[GRPH-27] keep one durable authority
and its state transitions; [GRPH-32] keeps the input contract explicit;
[CON-01]/[RES-02]/[RES-04] bound blocking work and retain slots until release.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import copy
import json
import math
import sys
import threading
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

from fastmcp import FastMCP

from . import coordclaims_transport as transport

REQUEST_LIMIT = transport.REQUEST_LIMIT
RESPONSE_LIMIT = transport.RESPONSE_LIMIT
CONFIG_LIMIT = transport.CONFIG_LIMIT
MAX_WORKERS = 4
MAX_QUEUED = 16
MAX_ADMITTED = MAX_WORKERS + MAX_QUEUED
TOTAL_DEADLINE_SECONDS = 2.5
SCOPE = transport.SCOPE
_BINDING_FIELDS = frozenset({"root", "authority", "principal", "session_id"})
_REQUEST_OVERRIDES = frozenset({"root", "principal", "session_id", "owner", "family", "binding", "authority"})


@dataclass(frozen=True)
class _Binding:
    root: Path
    authority: Mapping[str, Any]
    principal: str
    session_id: str


def _refusal(code: str) -> dict[str, Any]:
    return transport._refusal(code)[0]


def _reject_constant(_value: str) -> None:
    raise ValueError("nonfinite number")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("nonfinite number")
    return parsed


def _strict_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _decode(raw: bytes) -> Any:
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_strict_pairs,
        parse_constant=_reject_constant,
        parse_float=_finite_float,
    )


def _encode(response: Any) -> bytes:
    return transport._encode(response)


def _validated_binding(binding: Any) -> _Binding:
    """Copy, validate, then close over immutable trusted binding values."""
    if not isinstance(binding, dict) or set(binding) != _BINDING_FIELDS:
        raise ValueError("binding shape")
    snapshot = copy.deepcopy(binding)
    root = transport._binding(snapshot)
    authority = MappingProxyType(copy.deepcopy(snapshot["authority"]))
    return _Binding(root, authority, snapshot["principal"], snapshot["session_id"])


def _load_binding(config_path: str | Path, alias: str) -> _Binding:
    path = Path(config_path)
    if not path.is_absolute():
        raise ValueError("absolute config path required")
    with path.open("rb") as stream:
        raw = stream.read(CONFIG_LIMIT + 1)
    if len(raw) > CONFIG_LIMIT:
        raise ValueError("config too large")
    config = _decode(raw)
    if (
        not isinstance(config, dict)
        or type(config.get("schema_version")) is not int
        or config["schema_version"] != 1
        or not isinstance(config.get("bindings"), dict)
        or alias not in config["bindings"]
    ):
        raise ValueError("invalid config")
    return _validated_binding(config["bindings"][alias])


def _json_snapshot(value: Any, *, limit: int) -> Any:
    """Require JSON-native values, bound their bytes, and detach caller state."""
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 64:
            raise ValueError("JSON nesting too deep")
        if item is None or type(item) in (str, bool, int):
            continue
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError("nonfinite number")
            continue
        if type(item) is list:
            pending.extend((child, depth + 1) for child in item)
            continue
        if type(item) is dict:
            if any(type(key) is not str for key in item):
                raise ValueError("object keys must be strings")
            pending.extend((child, depth + 1) for child in item.values())
            continue
        raise ValueError("not a JSON value")
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > limit:
        raise ValueError("JSON value too large")
    return _decode(encoded)


def _request_snapshot(request: Any, binding: _Binding) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    if _REQUEST_OVERRIDES & request.keys():
        raise ValueError("request contains a trusted binding override")
    copied = _json_snapshot(request, limit=REQUEST_LIMIT)
    if (
        type(copied.get("schema_version")) is not int
        or copied["schema_version"] != 1
        or not isinstance(copied.get("operation_id"), str)
        or not copied["operation_id"].strip()
    ):
        raise ValueError("invalid request envelope")
    for key in SCOPE:
        if key not in copied:
            raise ValueError("missing scope")
        expected = binding.authority[key]
        actual = copied[key]
        if type(actual) is not type(expected) or actual != expected:
            raise ValueError("scope mismatch")
    return copied


def _resolve_execute(family: str) -> Callable[..., Any]:
    if family == "claims":
        from .coordclaims import execute

        return execute
    if family == "channels":
        from .coordchannels import execute

        return execute
    raise ValueError("unknown family")


def _response(response: Any) -> tuple[dict[str, Any], int]:
    if (
        not isinstance(response, dict)
        or type(response.get("schema_version")) is not int
        or response["schema_version"] != 1
        or type(response.get("ok")) is not bool
    ):
        return _refusal("outcome_unknown"), 3
    try:
        encoded = _encode(response)
    except (TypeError, ValueError, OverflowError, RecursionError):
        return _refusal("outcome_unknown"), 3
    if len(encoded) > RESPONSE_LIMIT:
        return _refusal("outcome_unknown"), 3
    if response.get("error_code") == "storage_error":
        return response, 3
    return response, (0 if response["ok"] else 2)


def _dispatch(binding: _Binding, family: str, request: dict[str, Any]) -> tuple[dict[str, Any], int]:
    try:
        execute = _resolve_execute(family)
    except Exception:
        return _refusal("unavailable"), 3
    try:
        # [DATA-14]/[GRPH-14]: adapters route to the core receipt journal; they
        # must not create a second authority or a parallel receipt history.
        result = execute(
            binding.root,
            request,
            authority=dict(binding.authority),
            principal=binding.principal,
            session_id=binding.session_id,
        )
    except Exception:
        # The core can have committed before an exception reaches this boundary.
        return _refusal("outcome_unknown"), 3
    return _response(result)


class _Bulkhead:
    """Per-process 4+16 bulkhead; blocking core calls stay off-loop [CON-01]."""

    def __init__(self) -> None:
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS, thread_name_prefix="coordservice"
        )
        self._lock = threading.Lock()
        self._admitted = 0

    def _release(self, _future: concurrent.futures.Future[Any]) -> None:
        # [RES-04]: a slot belongs to the actual worker lifetime, including
        # storage calls whose caller has already received outcome_unknown.
        with self._lock:
            self._admitted -= 1

    async def call(self, binding: _Binding, family: str, request: Any) -> dict[str, Any]:
        deadline = time.monotonic() + TOTAL_DEADLINE_SECONDS
        try:
            snapshot = _request_snapshot(request, binding)
        except (TypeError, ValueError, OverflowError, RecursionError):
            return _refusal("invalid_request")
        if deadline <= time.monotonic():
            return _refusal("busy")
        with self._lock:
            if self._admitted >= MAX_ADMITTED:
                return _refusal("busy")
            self._admitted += 1

        def invoke() -> tuple[dict[str, Any], int]:
            # The event loop may be blocked while this job waits in the queue.
            # Enforce the total deadline in the worker before entering the core.
            if time.monotonic() >= deadline:
                return _refusal("busy"), 3
            return _dispatch(binding, family, snapshot)

        try:
            future = self._executor.submit(invoke)
        except Exception:
            with self._lock:
                self._admitted -= 1
            return _refusal("unavailable")
        future.add_done_callback(self._release)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return self._expire(future)
        wrapped = asyncio.wrap_future(future)
        try:
            result, _status = await asyncio.wait_for(asyncio.shield(wrapped), timeout=remaining)
            return result
        except asyncio.TimeoutError:
            return self._expire(future)
        except asyncio.CancelledError:
            # MCP cancellation cannot undo an operation already in the worker.
            return self._expire(future)

    @staticmethod
    def _expire(future: concurrent.futures.Future[Any]) -> dict[str, Any]:
        if future.done():
            if future.cancelled():
                return _refusal("busy")
            try:
                result, _status = future.result()
                return result
            except Exception:
                return _refusal("outcome_unknown")
        if future.cancel():
            # It was still in the executor queue, so the core was never entered.
            return _refusal("busy")
        # [RES-01]: no immediate retry; the caller replays its same operation ID.
        return _refusal("outcome_unknown")

    def close(self) -> None:
        # Queued jobs are canceled on shutdown; already-running storage calls are
        # allowed to finish and keep their slots until their callbacks run.
        self._executor.shutdown(wait=False, cancel_futures=True)


def build_mcp(binding: dict[str, Any]) -> FastMCP:
    """Build a stdio-capable server with exactly the claims and channels tools."""
    frozen = _validated_binding(binding)
    bulkhead = _Bulkhead()

    @asynccontextmanager
    async def lifespan(_server: FastMCP) -> AsyncIterator[dict[str, Any]]:
        try:
            yield {}
        finally:
            bulkhead.close()

    server = FastMCP(
        name="Workbay Coordination Service",
        instructions="Routes trusted local claims and channels requests to the shared authority.",
        lifespan=lifespan,
        mask_error_details=True,
    )

    @server.tool(name="coord_claim", run_in_thread=False)
    async def coord_claim(request: dict[str, Any]) -> dict[str, Any]:
        return await bulkhead.call(frozen, "claims", request)

    @server.tool(name="coord_channel", run_in_thread=False)
    async def coord_channel(request: dict[str, Any]) -> dict[str, Any]:
        return await bulkhead.call(frozen, "channels", request)

    return server


def _serve_one(stdin: Any, binding: _Binding, family: str) -> tuple[dict[str, Any], int]:
    try:
        raw = stdin.read(REQUEST_LIMIT + 1)
        if len(raw) > REQUEST_LIMIT:
            return _refusal("invalid_request"), 2
        request = _decode(raw)
        snapshot = _request_snapshot(request, binding)
    except Exception:
        return _refusal("invalid_request"), 2
    return _dispatch(binding, family, snapshot)


class _Parser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ValueError("invalid arguments")


def _parser() -> _Parser:
    parser = _Parser(description=__doc__, add_help=False)
    parser.add_argument("--bindings", required=True)
    parser.add_argument("--binding", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--family", choices=("claims", "channels"))
    mode.add_argument("--mcp", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        binding = _load_binding(args.bindings, args.binding)
    except Exception:
        response, status = _refusal("binding_invalid"), 2
    else:
        if args.mcp:
            try:
                build_mcp(
                    {
                        "root": str(binding.root),
                        "authority": dict(binding.authority),
                        "principal": binding.principal,
                        "session_id": binding.session_id,
                    }
                ).run(transport="stdio", show_banner=False)
                return 0
            except Exception:
                return 3
        response, status = _serve_one(sys.stdin.buffer, binding, args.family)
    try:
        sys.stdout.buffer.write(_encode(response))
    except Exception:
        return 3
    return status


if __name__ == "__main__":
    sys.exit(main())
