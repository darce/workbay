"""Bounded SSH/stdio client for the trusted coordination claims adapter.

The client never takes an endpoint, root, principal, or session from a request.
It persists each exact request and its trusted endpoint/binding identity before
starting SSH. A pending operation is retried only from the same durable bytes.

The strict client config has schema_version 1, outbox_dir, ssh_executable,
timeout_seconds, connect_timeout_seconds, and a bindings object. Each selected
binding has destination, remote_python, bindings_path, binding, and a trusted
binding_sha256 calculated from the remote binding with
coordclaims_transport.binding_digest(). Optional server_module and family
select only the claims transport (default) or coordservice with pinnedclaims
or channels. A local selector named codex-remote is an ordinary trusted name;
the client applies no harness-name allowlist. Older configs without a digest
can return matching completed legacy receipts locally, but cannot dispatch new
requests or retry pending legacy operations.

Local file writes use atomic replacement, fsync of the file, and fsync of the
parent directory. A filesystem syscall (including fsync) cannot be interrupted
reliably by Python, so the monotonic deadline strictly bounds lock waits,
subprocess I/O, and child cleanup, but cannot promise an OS-level wall-time cap
for a stalled local filesystem.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import math
import os
import re
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator

from . import coordclaims_transport

REMOTE_MODULE = "workbay_orchestrator_mcp.orchestration.coordclaims_transport"
SERVICE_MODULE = "workbay_orchestrator_mcp.orchestration.coordservice"
REQUEST_LIMIT = coordclaims_transport.REQUEST_LIMIT
RESPONSE_LIMIT = coordclaims_transport.RESPONSE_LIMIT
CONFIG_LIMIT = 64 * 1024
STDERR_LIMIT = 16 * 1024
MAX_BINDINGS = 64
MAX_OUTBOX_OPERATIONS = 10_000
MAX_RECORD_BYTES = REQUEST_LIMIT * 2 + RESPONSE_LIMIT * 2 + 16 * 1024
_BINDING_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_DESTINATION = re.compile(r"^(?:[A-Za-z0-9_.-]+@)?(?:[A-Za-z0-9][A-Za-z0-9_.:-]*|\[[0-9A-Fa-f:]+\])$")
_ENTRY_NAME = re.compile(r"^[0-9a-f]{64}\.json$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_JSON_REFUSALS = {"binding_invalid", "binding_mismatch", "invalid_request", "outcome_unknown", "unavailable"}
_RECORD_PENDING_KEYS = {
    "schema_version",
    "operation_id",
    "request_b64",
    "request_sha256",
    "identity",
    "kind",
    "status",
}
_RECORD_COMPLETE_KEYS = _RECORD_PENDING_KEYS | {"response_b64", "response_sha256", "exit_status"}
_RECORD_V2_PENDING_KEYS = _RECORD_PENDING_KEYS | {"namespace", "family", "operation"}
_RECORD_V2_COMPLETE_KEYS = _RECORD_V2_PENDING_KEYS | {"response_b64", "response_sha256", "exit_status"}


class _Failure(Exception):
    def __init__(self, code: str, status: int = 3):
        super().__init__(code)
        self.code = code
        self.status = status


class _WaitExpired(Exception):
    pass


class _CorruptOutbox(Exception):
    pass


@dataclass(frozen=True)
class _Binding:
    destination: str
    remote_python: str
    bindings_path: str
    remote_binding: str
    binding_sha256: str | None
    server_module: str
    family: str


@dataclass(frozen=True)
class _ClientConfig:
    outbox_dir: Path
    ssh_executable: str
    timeout_seconds: float
    connect_timeout_seconds: int
    binding_name: str
    binding: _Binding
    server_module: str
    family: str
    identity: dict[str, Any]


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _constant(_value: str) -> None:
    raise ValueError("nonfinite number")


def _float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite number")
    return number


def _decode(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float)


def _encoded(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")


def _refusal(code: str, message: str | None = None) -> bytes:
    error: dict[str, str] = {"code": code}
    if message is not None:
        error["message"] = message
    return _encoded({"ok": False, "error": error})


def _text(value: Any, *, max_bytes: int, allow_whitespace: bool = False) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        if len(value.encode("utf-8")) > max_bytes:
            return False
    except UnicodeEncodeError:
        return False
    return allow_whitespace or bool(value.strip())


def _absolute_path(value: Any, max_bytes: int = 4096) -> bool:
    return _text(value, max_bytes=max_bytes) and Path(value).is_absolute()


def _load_config(config_path: str, binding_name: str) -> _ClientConfig:
    if not _absolute_path(config_path):
        raise ValueError("config path")
    if not _BINDING_NAME.fullmatch(binding_name):
        raise ValueError("binding name")
    with Path(config_path).open("rb") as stream:
        raw = stream.read(CONFIG_LIMIT + 1)
    if len(raw) > CONFIG_LIMIT:
        raise ValueError("config size")
    value = _decode(raw)
    required = {
        "schema_version",
        "outbox_dir",
        "ssh_executable",
        "timeout_seconds",
        "connect_timeout_seconds",
        "bindings",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("config fields")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("config version")
    if not _absolute_path(value["outbox_dir"]):
        raise ValueError("outbox path")
    if not _absolute_path(value["ssh_executable"]):
        raise ValueError("ssh executable")
    timeout = value["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout")
    try:
        timeout_number = float(timeout)
    except (OverflowError, ValueError):
        raise ValueError("timeout") from None
    if not math.isfinite(timeout_number) or not 0.05 <= timeout_number <= 120:
        raise ValueError("timeout range")
    connect_timeout = value["connect_timeout_seconds"]
    if type(connect_timeout) is not int or not 1 <= connect_timeout <= 30:
        raise ValueError("connect timeout")
    bindings = value["bindings"]
    if not isinstance(bindings, dict) or not 1 <= len(bindings) <= MAX_BINDINGS:
        raise ValueError("bindings")
    if any(not isinstance(name, str) or not _BINDING_NAME.fullmatch(name) for name in bindings):
        raise ValueError("binding selector")
    entry = bindings.get(binding_name)
    required_binding_fields = {"destination", "remote_python", "bindings_path", "binding"}
    optional_binding_fields = {"binding_sha256", "server_module", "family"}
    if (
        not isinstance(entry, dict)
        or not required_binding_fields <= entry.keys()
        or entry.keys() - required_binding_fields - optional_binding_fields
    ):
        raise ValueError("binding fields")
    destination = entry["destination"]
    remote_python = entry["remote_python"]
    bindings_path = entry["bindings_path"]
    remote_binding = entry["binding"]
    if not isinstance(destination, str) or not _DESTINATION.fullmatch(destination):
        raise ValueError("destination")
    if not _absolute_path(remote_python) or not _absolute_path(bindings_path):
        raise ValueError("remote paths")
    if not isinstance(remote_binding, str) or not _BINDING_NAME.fullmatch(remote_binding):
        raise ValueError("remote binding")
    binding_sha256 = entry.get("binding_sha256")
    if binding_sha256 is not None and (not isinstance(binding_sha256, str) or not _SHA256.fullmatch(binding_sha256)):
        raise ValueError("binding digest")
    server_module = entry.get("server_module", REMOTE_MODULE)
    family = entry.get("family", "pinnedclaims")
    if server_module not in {REMOTE_MODULE, SERVICE_MODULE}:
        raise ValueError("server module")
    if server_module == REMOTE_MODULE and family != "pinnedclaims":
        raise ValueError("transport family")
    if server_module == SERVICE_MODULE and family not in {"pinnedclaims", "channels"}:
        raise ValueError("service family")
    if not isinstance(family, str):
        raise ValueError("family")
    binding = _Binding(destination, remote_python, bindings_path, remote_binding, binding_sha256, server_module, family)
    identity: dict[str, Any] = {
        "binding_name": binding_name,
        "destination": destination,
        "remote_python": remote_python,
        "remote_module": server_module,
        "bindings_path": bindings_path,
        "remote_binding": remote_binding,
        "binding_sha256": binding_sha256,
        "family": family,
    }
    return _ClientConfig(
        outbox_dir=Path(value["outbox_dir"]),
        ssh_executable=value["ssh_executable"],
        timeout_seconds=timeout_number,
        connect_timeout_seconds=connect_timeout,
        binding_name=binding_name,
        binding=binding,
        server_module=server_module,
        family=family,
        identity=identity,
    )


def _request_scope(request: dict[str, Any]) -> dict[str, str]:
    nested = request.get("scope")
    if "scope" in request and not isinstance(nested, dict):
        raise _Failure("invalid_request", 2)
    source = nested if isinstance(nested, dict) else request
    scope: dict[str, str] = {}
    for field in ("task_ref", "wave_id"):
        value = source.get(field)
        if not _text(value, max_bytes=REQUEST_LIMIT):
            raise _Failure("invalid_request", 2)
        scope[field] = value
    for field in ("task_ref", "wave_id"):
        if field in request and request[field] != scope[field]:
            raise _Failure("invalid_request", 2)
    return scope


def _operation_id(raw: bytes, family: str = "pinnedclaims") -> tuple[str, str, str, dict[str, str]]:
    if len(raw) > REQUEST_LIMIT:
        raise _Failure("invalid_request", 2)
    try:
        request = _decode(raw)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
        raise _Failure("invalid_request", 2) from None
    if not isinstance(request, dict):
        raise _Failure("invalid_request", 2)
    if type(request.get("schema_version")) is not int or request["schema_version"] != 1:
        raise _Failure("invalid_request", 2)
    operation_id = request.get("operation_id")
    if not _text(operation_id, max_bytes=REQUEST_LIMIT):
        raise _Failure("invalid_request", 2)
    operation = request.get("operation")
    if family == "pinnedclaims":
        if not isinstance(operation, str) or operation not in {"get", "claim", "renew", "release", "complete"}:
            raise _Failure("invalid_request", 2)
        kind = "read" if operation == "get" else "mutation"
    elif family == "channels":
        if not isinstance(operation, str) or operation not in {"send", "read", "ack", "events"}:
            raise _Failure("invalid_request", 2)
        kind = "read" if operation in {"read", "events"} else "mutation"
        if operation == "send" and not (
            _text(request.get("message_id"), max_bytes=REQUEST_LIMIT)
            and type(request.get("version")) is int
            and request["version"] == 1
            and isinstance(request.get("recipients"), list)
            and all(_text(recipient, max_bytes=REQUEST_LIMIT) for recipient in request["recipients"])
            and isinstance(request.get("body"), dict)
        ):
            raise _Failure("invalid_request", 2)
        if operation in {"read", "ack"} and not _text(request.get("message_id"), max_bytes=REQUEST_LIMIT):
            raise _Failure("invalid_request", 2)
        if operation == "ack" and (type(request.get("version")) is not int or request["version"] != 1):
            raise _Failure("invalid_request", 2)
        if operation == "events":
            after_cursor, limit = request.get("after_cursor"), request.get("limit")
            filters = request.get("filters", {})
            if (
                type(after_cursor) is not int
                or after_cursor < 0
                or (limit is not None and (type(limit) is not int or not 1 <= limit <= 100))
                or not isinstance(filters, dict)
                or filters.keys() - {"kind", "message_id"}
                or any(not _text(value, max_bytes=REQUEST_LIMIT) for value in filters.values())
            ):
                raise _Failure("invalid_request", 2)
    else:
        raise _Failure("invalid_request", 2)
    scope = _request_scope(request)
    if family == "pinnedclaims" and (
        not all(_text(request.get(field), max_bytes=REQUEST_LIMIT) for field in ("authority_id", "task_ref", "wave_id"))
        or type(request.get("authority_epoch")) is not int
        or request["authority_epoch"] < 1
    ):
        raise _Failure("invalid_request", 2)
    return operation_id, kind, operation, scope


def _key(operation_id: str) -> str:
    """Legacy v1 outbox key; retained for conservative receipt recovery."""
    return hashlib.sha256(operation_id.encode("utf-8")).hexdigest() + ".json"


def _key_v2(identity: dict[str, Any], scope: dict[str, str], operation_id: str) -> str:
    namespace = {
        "schema_version": 2,
        "identity": identity,
        "task_ref": scope["task_ref"],
        "wave_id": scope["wave_id"],
        "operation_id": operation_id,
    }
    return hashlib.sha256(_encoded(namespace)).hexdigest() + ".json"


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_outbox(path: Path) -> None:
    if not path.is_absolute():
        raise _CorruptOutbox
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    for directory in reversed(missing):
        _fsync_directory(directory.parent)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise _CorruptOutbox


@contextmanager
def _outbox_lock(path: Path, deadline: float) -> Iterator[None]:
    fd: int | None = None
    try:
        _ensure_outbox(path)
        lock_path = path / "outbox.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        lock_info = os.fstat(fd)
        if not stat.S_ISREG(lock_info.st_mode):
            raise _CorruptOutbox
    except (OSError, _CorruptOutbox):
        if fd is not None:
            os.close(fd)
        raise _Failure("unavailable", 3) from None
    assert fd is not None
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _Failure("unavailable", 3) from None
                time.sleep(min(0.01, remaining))
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _record_bytes(record: dict[str, Any]) -> bytes:
    raw = _encoded(record)
    if len(raw) > MAX_RECORD_BYTES:
        raise _CorruptOutbox
    return raw


def _atomic_store(directory: Path, target: Path, record: dict[str, Any]) -> None:
    raw = _record_bytes(record)
    fd, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, target)
        _fsync_directory(directory)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _check_layout(directory: Path, deadline: float) -> int:
    count = 0
    removed_temps = False
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if time.monotonic() >= deadline:
                    raise _Failure("unavailable", 3)
                name = entry.name
                if name == "outbox.lock":
                    if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                        raise _CorruptOutbox
                    continue
                if name.startswith(".tmp-"):
                    if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                        raise _CorruptOutbox
                    os.unlink(entry.path)
                    removed_temps = True
                    continue
                if not _ENTRY_NAME.fullmatch(name):
                    raise _CorruptOutbox
                if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                    raise _CorruptOutbox
                if entry.stat(follow_symlinks=False).st_size > MAX_RECORD_BYTES:
                    raise _CorruptOutbox
                count += 1
                if count > MAX_OUTBOX_OPERATIONS:
                    raise _CorruptOutbox
        if removed_temps:
            _fsync_directory(directory)
    except OSError:
        raise _CorruptOutbox from None
    return count


def _valid_identity(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value)
        == {"binding_name", "destination", "remote_python", "remote_module", "bindings_path", "remote_binding"}
        and all(isinstance(item, str) and item for item in value.values())
        and value["remote_module"] == REMOTE_MODULE
    )


def _valid_identity_v2(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value)
        == {
            "binding_name",
            "destination",
            "remote_python",
            "remote_module",
            "bindings_path",
            "remote_binding",
            "binding_sha256",
            "family",
        }
        and all(
            isinstance(value[key], str) and value[key]
            for key in ("binding_name", "destination", "remote_python", "bindings_path", "remote_binding")
        )
        and value["remote_module"] in {REMOTE_MODULE, SERVICE_MODULE}
        and isinstance(value["binding_sha256"], str)
        and _SHA256.fullmatch(value["binding_sha256"])
        and value["family"] in {"pinnedclaims", "channels"}
        and (value["remote_module"] == SERVICE_MODULE or value["family"] == "pinnedclaims")
    )


def _read_record(path: Path, operation_id: str) -> dict[str, Any] | None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return None
    except OSError:
        raise _CorruptOutbox from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_RECORD_BYTES:
            raise _CorruptOutbox
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_RECORD_BYTES + 1)
        if len(raw) > MAX_RECORD_BYTES:
            raise _CorruptOutbox
    except OSError:
        raise _CorruptOutbox from None
    try:
        record = _decode(raw)
        if not isinstance(record, dict) or type(record.get("schema_version")) is not int:
            raise ValueError("record")
        version = record["schema_version"]
        if version not in {1, 2} or record.get("operation_id") != operation_id:
            raise ValueError("record identity")
        if (version == 1 and not _valid_identity(record.get("identity"))) or (
            version == 2 and not _valid_identity_v2(record.get("identity"))
        ):
            raise ValueError("record identity")
        request_b64 = record.get("request_b64")
        if not isinstance(request_b64, str):
            raise ValueError("request")
        request = base64.b64decode(request_b64, validate=True)
        if record.get("request_sha256") != _digest(request):
            raise ValueError("request digest")
        family = "pinnedclaims" if version == 1 else record.get("family")
        stored_id, kind, operation, scope = _operation_id(request, family)
        if stored_id != operation_id or record.get("kind") != kind:
            raise ValueError("request mismatch")
        if version == 2 and (
            set(record.get("namespace", {})) != {"task_ref", "wave_id"}
            or record["namespace"] != scope
            or record.get("operation") != operation
        ):
            raise ValueError("request namespace")
        status = record.get("status")
        if status == "pending":
            expected_keys = _RECORD_PENDING_KEYS if version == 1 else _RECORD_V2_PENDING_KEYS
            if set(record) != expected_keys:
                raise ValueError("pending record")
        elif status == "complete":
            expected_keys = _RECORD_COMPLETE_KEYS if version == 1 else _RECORD_V2_COMPLETE_KEYS
            if set(record) != expected_keys or kind != "mutation":
                raise ValueError("complete record")
            response_b64 = record.get("response_b64")
            exit_status = record.get("exit_status")
            if not isinstance(response_b64, str) or type(exit_status) is not int or exit_status not in (0, 2):
                raise ValueError("response")
            response = base64.b64decode(response_b64, validate=True)
            if record.get("response_sha256") != _digest(response):
                raise ValueError("response digest")
            parsed = _parse_response(response, family=family, operation=operation)
            if parsed is None or parsed.get("schema_version") != 1 or not _cache_mutation(parsed, family, operation):
                raise ValueError("completed response")
            if (exit_status == 0) != parsed["ok"]:
                raise ValueError("response status")
        else:
            raise ValueError("record status")
        expected_name = (
            _key(operation_id) if version == 1 else _key_v2(record["identity"], record["namespace"], operation_id)
        )
        if path.name != expected_name:
            raise ValueError("record key")
        return record
    except (ValueError, TypeError, UnicodeDecodeError, RecursionError, json.JSONDecodeError, _Failure):
        raise _CorruptOutbox from None


def _valid_channels_response(value: dict[str, Any], operation: str | None) -> bool:
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1 or value.get("ok") is not True:
        return False
    if operation in {"send", "read"}:
        return isinstance(value.get("message"), dict)
    if operation == "ack":
        return (
            _text(value.get("message_id"), max_bytes=REQUEST_LIMIT)
            and type(value.get("version")) is int
            and value["version"] == 1
            and "acknowledgment" in value
        )
    if operation == "events":
        return (
            isinstance(value.get("events"), list)
            and all(isinstance(event, dict) for event in value["events"])
            and type(value.get("next_cursor")) is int
            and value["next_cursor"] >= 0
            and type(value.get("has_more")) is bool
        )
    return False


def _parse_response(raw: bytes, *, family: str = "pinnedclaims", operation: str | None = None) -> dict[str, Any] | None:
    if not raw or len(raw) > RESPONSE_LIMIT:
        return None
    try:
        value = _decode(raw)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or type(value.get("ok")) is not bool:
        return None
    if "schema_version" in value:
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            return None
        if value["ok"]:
            if family == "channels":
                if not _valid_channels_response(value, operation):
                    return None
            elif not _valid_view(value):
                return None
            return value
        if family == "channels":
            if "error_code" in value:
                return value if isinstance(value["error_code"], str) else None
            error = value.get("error")
            if isinstance(error, dict) and isinstance(error.get("code"), str):
                return value
            if _text(error, max_bytes=256):
                return value
            return None
        code = value.get("error_code")
        if not isinstance(code, str) or code not in {
            "invalid_request",
            "scope_mismatch",
            "stale_token",
            "held_by",
            "completed",
            "idempotency_conflict",
            "capacity",
            "deadline",
            "state_corrupt",
            "storage_error",
        }:
            return None
        if code == "held_by":
            if set(value) != {"schema_version", "ok", "error_code", "held_by"}:
                return None
            if not _valid_view(value["held_by"]) or value["held_by"]["state"] != "claimed":
                return None
        elif code == "state_corrupt":
            if set(value) - {"schema_version", "ok", "error_code", "detail"}:
                return None
            if "detail" in value and not _text(value["detail"], max_bytes=256):
                return None
        elif set(value) != {"schema_version", "ok", "error_code"}:
            return None
        return value
    if value["ok"] is False and set(value) == {"ok", "error"} and isinstance(value["error"], dict):
        error = value["error"]
        if set(error) - {"code", "message"} or not isinstance(error.get("code"), str):
            return None
        if "message" in error and not _text(error["message"], max_bytes=256):
            return None
        if error["code"] in _JSON_REFUSALS or (
            family == "channels" and error["code"] in {"not_found", "storage_error"}
        ):
            return value
    return None


def _valid_view(value: Any) -> bool:
    if not isinstance(value, dict) or not {"schema_version", "ok", "work_item_id", "state"} <= value.keys():
        return False
    if value.keys() - {"schema_version", "ok", "work_item_id", "state", "owner", "token", "expires_at"}:
        return False
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["ok"] is not True
        or not _text(value["work_item_id"], max_bytes=REQUEST_LIMIT)
        or not isinstance(value["state"], str)
        or value["state"] not in {"available", "claimed", "completed"}
    ):
        return False
    lineage = {"owner", "token", "expires_at"} & value.keys()
    if lineage and lineage != {"owner", "token", "expires_at"}:
        return False
    if value["state"] in {"claimed", "completed"} and not lineage:
        return False
    if lineage:
        owner = value["owner"]
        if not isinstance(owner, dict) or set(owner) != {"principal", "session_id"}:
            return False
        if not all(_text(owner[key], max_bytes=REQUEST_LIMIT) for key in ("principal", "session_id")):
            return False
        if type(value["token"]) is not int or value["token"] <= 0:
            return False
        expiry = value["expires_at"]
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)):
            return False
        try:
            if not math.isfinite(expiry):
                return False
        except OverflowError:
            return False
    return True


def _cache_mutation(response: dict[str, Any], family: str = "pinnedclaims", operation: str | None = None) -> bool:
    if family == "channels":
        return operation in {"send", "ack"} and response.get("ok") is True
    if response.get("ok") is True:
        return True
    return response.get("error_code") in {"held_by", "stale_token", "completed"}


def _remote_command(config: _ClientConfig) -> list[str]:
    remote_argv = [
        config.binding.remote_python,
        "-m",
        config.server_module,
        "--bindings",
        config.binding.bindings_path,
        "--binding",
        config.binding.remote_binding,
    ]
    if config.binding.binding_sha256 is not None:
        remote_argv.extend(["--binding-sha256", config.binding.binding_sha256])
    if config.server_module == SERVICE_MODULE:
        remote_argv.extend(["--family", "channels" if config.family == "channels" else "claims"])
    return [
        config.ssh_executable,
        "-oBatchMode=yes",
        f"-oConnectTimeout={config.connect_timeout_seconds}",
        "-T",
        config.binding.destination,
        shlex.join(remote_argv),
    ]


def _stop_and_reap(process: subprocess.Popen[bytes], deadline: float) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError:
        try:
            process.terminate()
        except OSError:
            pass
    term_end = min(deadline, time.monotonic() + 0.05)
    while process.poll() is None and time.monotonic() < term_end:
        time.sleep(min(0.005, max(0, term_end - time.monotonic())))
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        try:
            process.kill()
        except OSError:
            pass
    remaining = max(0.0, deadline - time.monotonic())
    try:
        process.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        pass


def _run_ssh(
    config: _ClientConfig, request: bytes, work_deadline: float, total_deadline: float
) -> tuple[bytes, int] | None:
    """Return bounded stdout/status; None means a post-spawn unknown outcome."""
    try:
        process = subprocess.Popen(
            _remote_command(config),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            start_new_session=True,
            bufsize=0,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        raise _Failure("unavailable", 3) from None

    selector = selectors.DefaultSelector()
    stdout = bytearray()
    stderr = bytearray()
    offset = 0
    failed = False
    try:
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        for stream, event in ((process.stdout, selectors.EVENT_READ), (process.stderr, selectors.EVENT_READ)):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, event, "stdout" if stream is process.stdout else "stderr")
        os.set_blocking(process.stdin.fileno(), False)
        selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
        while selector.get_map() or process.poll() is None:
            remaining = work_deadline - time.monotonic()
            if remaining <= 0:
                failed = True
                break
            try:
                events = selector.select(min(remaining, 0.05))
            except InterruptedError:
                continue
            if not events and not selector.get_map() and process.poll() is None:
                time.sleep(min(0.005, max(0, work_deadline - time.monotonic())))
                continue
            for key, _mask in events:
                stream = key.fileobj
                kind = key.data
                try:
                    if kind == "stdin":
                        if offset >= len(request):
                            selector.unregister(stream)
                            stream.close()
                            continue
                        written = os.write(stream.fileno(), request[offset : offset + 8192])
                        offset += written
                        if offset >= len(request):
                            selector.unregister(stream)
                            stream.close()
                    else:
                        target = stdout if kind == "stdout" else stderr
                        limit = RESPONSE_LIMIT if kind == "stdout" else STDERR_LIMIT
                        chunk = os.read(stream.fileno(), min(8192, limit - len(target) + 1))
                        if not chunk:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        target.extend(chunk)
                        if len(target) > limit:
                            failed = True
                            break
                except (BrokenPipeError, OSError, ValueError):
                    failed = True
                    break
            if failed:
                break
        if failed:
            _stop_and_reap(process, total_deadline)
            return None
        remaining = work_deadline - time.monotonic()
        if remaining <= 0:
            _stop_and_reap(process, total_deadline)
            return None
        try:
            status = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            _stop_and_reap(process, total_deadline)
            return None
        return bytes(stdout), status
    except BaseException:
        _stop_and_reap(process, total_deadline)
        return None
    finally:
        selector.close()
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


def _deadline_pair(timeout: float) -> tuple[float, float]:
    total = time.monotonic() + timeout
    reserve = min(0.5, max(0.02, timeout * 0.1))
    return total - reserve, total


def _read_bounded(stream: BinaryIO, deadline: float) -> bytes:
    try:
        fd = stream.fileno()
    except (AttributeError, OSError):
        try:
            raw = stream.read(REQUEST_LIMIT + 1)
        except OSError:
            raise _Failure("invalid_request", 2) from None
        if len(raw) > REQUEST_LIMIT:
            raise _Failure("invalid_request", 2)
        return raw
    try:
        mode = os.fstat(fd).st_mode
    except OSError:
        raise _Failure("invalid_request", 2) from None
    if stat.S_ISREG(mode):
        try:
            raw = stream.read(REQUEST_LIMIT + 1)
        except OSError:
            raise _Failure("invalid_request", 2) from None
        if len(raw) > REQUEST_LIMIT:
            raise _Failure("invalid_request", 2)
        return raw
    selector = selectors.DefaultSelector()
    result = bytearray()
    try:
        selector.register(fd, selectors.EVENT_READ)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _WaitExpired
            try:
                ready = selector.select(remaining)
            except InterruptedError:
                continue
            if not ready:
                raise _WaitExpired
            chunk = os.read(fd, min(4096, REQUEST_LIMIT + 1 - len(result)))
            if not chunk:
                return bytes(result)
            result.extend(chunk)
            if len(result) > REQUEST_LIMIT:
                raise _Failure("invalid_request", 2)
    except OSError:
        raise _Failure("invalid_request", 2) from None
    finally:
        selector.close()


def _unknown() -> tuple[bytes, int]:
    return _refusal("outcome_unknown", "Replay the identical request with the same operation_id and binding."), 3


def _handle_pending(
    config: _ClientConfig,
    target: Path,
    record: dict[str, Any],
    request: bytes,
    kind: str,
    family: str,
    operation: str,
    work_deadline: float,
    total_deadline: float,
) -> tuple[bytes, int]:
    if record["status"] == "complete":
        return base64.b64decode(record["response_b64"], validate=True), record["exit_status"]
    if time.monotonic() >= work_deadline:
        return _refusal("unavailable"), 3
    outcome = _run_ssh(config, request, work_deadline, total_deadline)
    if outcome is None:
        return _unknown()
    raw_response, status = outcome
    # Exit 255 is an SSH transport failure. It does not establish that the
    # remote adapter was unauthenticated or that the operation was not run.
    if status == 255:
        return _unknown()
    response = _parse_response(raw_response, family=family, operation=operation)
    if response is None:
        return _unknown()
    if "schema_version" not in response:
        code = response["error"]["code"]
        if code in {"outcome_unknown", "storage_error"}:
            return raw_response, 3
        expected_status = 3 if code == "unavailable" else 2
        if status != expected_status:
            return _unknown()
        return raw_response, status
    if response.get("error_code") == "storage_error":
        return raw_response, 3
    if status not in (0, 2, 3) or (status == 0) != response["ok"]:
        return _unknown()
    if status == 3 and response.get("error_code") not in {"deadline", "capacity", "scope_mismatch"}:
        return _unknown()
    if kind == "mutation" and _cache_mutation(response, family, operation):
        record = dict(record)
        record.update(
            status="complete",
            response_b64=base64.b64encode(raw_response).decode("ascii"),
            response_sha256=_digest(raw_response),
            exit_status=status,
        )
        try:
            _atomic_store(config.outbox_dir, target, record)
        except (OSError, _CorruptOutbox, ValueError):
            return _unknown()
    return raw_response, status


def _legacy_identity(config: _ClientConfig) -> dict[str, Any] | None:
    if config.server_module != REMOTE_MODULE or config.family != "pinnedclaims":
        return None
    return {
        "binding_name": config.binding_name,
        "destination": config.binding.destination,
        "remote_python": config.binding.remote_python,
        "remote_module": REMOTE_MODULE,
        "bindings_path": config.binding.bindings_path,
        "remote_binding": config.binding.remote_binding,
    }


def _peek_operation_id(path: Path) -> str:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        raise _CorruptOutbox from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_RECORD_BYTES:
            raise _CorruptOutbox
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_RECORD_BYTES + 1)
        if len(raw) > MAX_RECORD_BYTES:
            raise _CorruptOutbox
    except OSError:
        raise _CorruptOutbox from None
    try:
        value = _decode(raw)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError, json.JSONDecodeError):
        raise _CorruptOutbox from None
    if not isinstance(value, dict) or not isinstance(value.get("operation_id"), str):
        raise _CorruptOutbox
    return value["operation_id"]


def _record_request_scope(record: dict[str, Any], family: str) -> dict[str, str]:
    raw = base64.b64decode(record["request_b64"], validate=True)
    _operation_id(raw, family)
    request = _decode(raw)
    return _request_scope(request)


def _find_replay_record(
    config: _ClientConfig,
    operation_id: str,
    scope: dict[str, str] | None,
    deadline: float,
) -> tuple[Path, dict[str, Any]] | None:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    legacy_identity = _legacy_identity(config)
    try:
        with os.scandir(config.outbox_dir) as entries:
            for entry in entries:
                if time.monotonic() >= deadline:
                    raise _Failure("unavailable", 3)
                if not _ENTRY_NAME.fullmatch(entry.name):
                    continue
                path = Path(entry.path)
                if _peek_operation_id(path) != operation_id:
                    continue
                record = _read_record(path, operation_id)
                if record is None:
                    continue
                version = record["schema_version"]
                if version == 1:
                    if legacy_identity is None or record["identity"] != legacy_identity:
                        continue
                    candidate_scope = _record_request_scope(record, "pinnedclaims")
                else:
                    if record["identity"] != config.identity:
                        continue
                    candidate_scope = record["namespace"]
                if scope is not None and candidate_scope != scope:
                    continue
                candidates.append((path, record))
                if len(candidates) > 1 and scope is not None:
                    raise _Failure("unavailable", 3)
    except OSError:
        raise _CorruptOutbox from None
    if len(candidates) > 1:
        raise _Failure("operation_ambiguous", 2)
    if candidates:
        return candidates[0]
    if _find_binding_drift_record(config, operation_id, scope, deadline) or _find_route_drift_record(
        config, operation_id, scope, deadline
    ):
        raise _Failure("binding_mismatch", 2)
    return None


def _find_binding_drift_record(
    config: _ClientConfig,
    operation_id: str,
    scope: dict[str, str] | None,
    deadline: float,
) -> bool:
    expected = dict(config.identity)
    expected.pop("binding_sha256", None)
    try:
        with os.scandir(config.outbox_dir) as entries:
            for entry in entries:
                if time.monotonic() >= deadline:
                    raise _Failure("unavailable", 3)
                if not _ENTRY_NAME.fullmatch(entry.name):
                    continue
                path = Path(entry.path)
                if _peek_operation_id(path) != operation_id:
                    continue
                record = _read_record(path, operation_id)
                if record is None or record["schema_version"] != 2:
                    continue
                prior_identity = dict(record["identity"])
                prior_identity.pop("binding_sha256", None)
                if prior_identity != expected or record["identity"]["binding_sha256"] == config.binding.binding_sha256:
                    continue
                if scope is None or record["namespace"] == scope:
                    return True
    except OSError:
        raise _CorruptOutbox from None
    return False


def _find_route_drift_record(
    config: _ClientConfig,
    operation_id: str,
    scope: dict[str, str] | None,
    deadline: float,
) -> bool:
    """Refuse a replay when its selected trusted alias moved to another route."""
    legacy_identity = _legacy_identity(config)
    try:
        with os.scandir(config.outbox_dir) as entries:
            for entry in entries:
                if time.monotonic() >= deadline:
                    raise _Failure("unavailable", 3)
                if not _ENTRY_NAME.fullmatch(entry.name):
                    continue
                path = Path(entry.path)
                if _peek_operation_id(path) != operation_id:
                    continue
                record = _read_record(path, operation_id)
                if record is None:
                    continue
                if record["schema_version"] == 1:
                    if legacy_identity is None or record["identity"].get("binding_name") != config.binding_name:
                        continue
                    candidate_scope = _record_request_scope(record, "pinnedclaims")
                else:
                    old_identity = record["identity"]
                    if old_identity["binding_name"] != config.binding_name or old_identity["family"] != config.family:
                        continue
                    candidate_scope = record["namespace"]
                if scope is None or candidate_scope == scope:
                    return True
    except OSError:
        raise _CorruptOutbox from None
    return False


def _legacy_replay_result(
    config: _ClientConfig,
    record: dict[str, Any],
    request: bytes | None,
    kind: str | None,
    scope: dict[str, str] | None,
) -> tuple[bytes, int] | None:
    stored_request = base64.b64decode(record["request_b64"], validate=True)
    _stored_id, stored_kind, _operation, stored_scope = _operation_id(stored_request, "pinnedclaims")
    if request is not None and (stored_request != request or stored_kind != kind):
        return _refusal("operation_conflict"), 2
    if scope is not None and stored_scope != scope:
        return None
    if record["status"] == "complete":
        return base64.b64decode(record["response_b64"], validate=True), record["exit_status"]
    return _refusal("binding_unverified"), 2


def _run_operation(
    config: _ClientConfig,
    operation_id: str,
    request: bytes | None,
    *,
    kind: str | None,
    operation: str | None,
    scope: dict[str, str] | None,
    replay: bool,
    work_deadline: float,
    total_deadline: float,
) -> tuple[bytes, int, bool]:
    try:
        with _outbox_lock(config.outbox_dir, work_deadline):
            count = _check_layout(config.outbox_dir, work_deadline)
            if replay:
                found = _find_replay_record(config, operation_id, scope, work_deadline)
                if found is None:
                    return _refusal("invalid_request"), 2, False
                target, record = found
                if record["schema_version"] == 1:
                    legacy_result = _legacy_replay_result(config, record, request, kind, scope)
                    if legacy_result is None:
                        return _refusal("invalid_request"), 2, False
                    return legacy_result[0], legacy_result[1], True
                family = record["family"]
                record_existed = True
            else:
                assert request is not None and kind is not None and operation is not None and scope is not None
                legacy_target = config.outbox_dir / _key(operation_id)
                legacy = _read_record(legacy_target, operation_id)
                if legacy is not None:
                    legacy_identity = _legacy_identity(config)
                    if legacy_identity is not None and legacy["identity"] == legacy_identity:
                        legacy_result = _legacy_replay_result(config, legacy, request, kind, scope)
                        if legacy_result is not None:
                            return legacy_result[0], legacy_result[1], True
                if config.binding.binding_sha256 is None:
                    return _refusal("binding_unverified"), 2, False
                target = config.outbox_dir / _key_v2(config.identity, scope, operation_id)
                record = _read_record(target, operation_id)
                family = config.family
                if record is not None and (
                    record["schema_version"] != 2
                    or record["identity"] != config.identity
                    or record["namespace"] != scope
                    or record["family"] != family
                ):
                    return _refusal("binding_mismatch"), 2, False
                if record is None and _find_binding_drift_record(config, operation_id, scope, work_deadline):
                    return _refusal("binding_mismatch"), 2, False
                record_existed = record is not None
                if record is None:
                    if count >= MAX_OUTBOX_OPERATIONS:
                        return _refusal("capacity"), 3, False
                    record = {
                        "schema_version": 2,
                        "operation_id": operation_id,
                        "request_b64": base64.b64encode(request).decode("ascii"),
                        "request_sha256": _digest(request),
                        "identity": config.identity,
                        "namespace": scope,
                        "family": family,
                        "operation": operation,
                        "kind": kind,
                        "status": "pending",
                    }
                    try:
                        _atomic_store(config.outbox_dir, target, record)
                    except (OSError, _CorruptOutbox, ValueError):
                        raise _Failure("unavailable", 3) from None
                else:
                    if base64.b64decode(record["request_b64"], validate=True) != request:
                        return _refusal("operation_conflict"), 2, True
            stored_request = base64.b64decode(record["request_b64"], validate=True)
            if record["status"] == "pending" and record["schema_version"] == 1:
                return _refusal("binding_unverified"), 2, True
            record_operation = record.get("operation")
            if record["schema_version"] == 1:
                _stored_id, _stored_kind, record_operation, _stored_scope = _operation_id(stored_request)
            raw, status = _handle_pending(
                config,
                target,
                record,
                stored_request,
                record["kind"],
                family,
                record_operation,
                work_deadline,
                total_deadline,
            )
            return raw, status, record_existed
    except _Failure as failure:
        return _refusal(failure.code), failure.status, False
    except (OSError, _CorruptOutbox, ValueError):
        return _refusal("unavailable"), 3, False


def _start(config_path: str, binding_name: str) -> tuple[_ClientConfig | None, float, float, tuple[bytes, int] | None]:
    try:
        config = _load_config(config_path, binding_name)
    except (OSError, ValueError, TypeError, UnicodeError, OverflowError, RecursionError, json.JSONDecodeError):
        return None, 0, 0, (_refusal("binding_invalid"), 2)
    work_deadline, total_deadline = _deadline_pair(config.timeout_seconds)
    return config, work_deadline, total_deadline, None


def _api_status(response: dict[str, Any], exit_status: int) -> str:
    if exit_status == 0:
        return "completed"
    error = response.get("error")
    if response.get("error_code") == "storage_error":
        return "unknown"
    if isinstance(error, dict) and error.get("code") == "outcome_unknown":
        return "unknown"
    return "refused"


def _api_envelope(raw: bytes, exit_status: int, replayed: bool) -> dict[str, Any]:
    try:
        response = _decode(raw)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError, json.JSONDecodeError):
        response = {"ok": False, "error": {"code": "outcome_unknown"}}
        exit_status = 3
    if not isinstance(response, dict):
        response = {"ok": False, "error": {"code": "outcome_unknown"}}
        exit_status = 3
    return {
        "schema_version": 1,
        "status": _api_status(response, exit_status),
        "response": response,
        "replayed": replayed,
        "exit_status": exit_status,
    }


def execute(
    config_path: str,
    binding_name: str,
    request: bytes | dict[str, Any] | None = None,
    *,
    operation_id: str | None = None,
    task_ref: str | None = None,
    wave_id: str | None = None,
) -> dict[str, Any]:
    """Submit exact bytes or replay one durable operation through trusted SSH.

    ``bytes`` are forwarded and persisted unchanged. A mapping is encoded as
    compact UTF-8 JSON. Passing only ``operation_id`` replays its unique scoped
    outbox record; task_ref and wave_id may select one record explicitly.
    ``replayed`` reports whether this operation already had a durable outbox
    record when the call began; reads still contact the remote service.
    """
    config, work_deadline, total_deadline, failure = _start(config_path, binding_name)
    if failure is not None:
        return _api_envelope(failure[0], failure[1], False)
    if request is None:
        if not _text(operation_id, max_bytes=REQUEST_LIMIT):
            return _api_envelope(_refusal("invalid_request"), 2, False)
        if (task_ref is None) != (wave_id is None):
            return _api_envelope(_refusal("invalid_request"), 2, False)
        scope = None
        if task_ref is not None and wave_id is not None:
            if not _text(task_ref, max_bytes=REQUEST_LIMIT) or not _text(wave_id, max_bytes=REQUEST_LIMIT):
                return _api_envelope(_refusal("invalid_request"), 2, False)
            scope = {"task_ref": task_ref, "wave_id": wave_id}
        raw, status, replayed = _run_operation(
            config,
            operation_id,
            None,
            kind=None,
            operation=None,
            scope=scope,
            replay=True,
            work_deadline=work_deadline,
            total_deadline=total_deadline,
        )
        return _api_envelope(raw, status, replayed)
    if isinstance(request, bytes):
        raw_request = request
    elif isinstance(request, dict):
        try:
            raw_request = _encoded(request)
        except (TypeError, ValueError, RecursionError):
            return _api_envelope(_refusal("invalid_request"), 2, False)
    else:
        return _api_envelope(_refusal("invalid_request"), 2, False)
    try:
        parsed_id, kind, operation, scope = _operation_id(raw_request, config.family)
    except _Failure as invalid:
        return _api_envelope(_refusal(invalid.code), invalid.status, False)
    if operation_id is not None and operation_id != parsed_id:
        return _api_envelope(_refusal("invalid_request"), 2, False)
    if (task_ref is None) != (wave_id is None):
        return _api_envelope(_refusal("invalid_request"), 2, False)
    if task_ref is not None and wave_id is not None and scope != {"task_ref": task_ref, "wave_id": wave_id}:
        return _api_envelope(_refusal("invalid_request"), 2, False)
    raw, status, replayed = _run_operation(
        config,
        parsed_id,
        raw_request,
        kind=kind,
        operation=operation,
        scope=scope,
        replay=False,
        work_deadline=work_deadline,
        total_deadline=total_deadline,
    )
    return _api_envelope(raw, status, replayed)


class _Parser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ValueError("arguments")


def _parser(replay: bool) -> argparse.ArgumentParser:
    parser = _Parser(description=__doc__)
    if replay:
        parser.add_argument("--operation-id", required=True)
        parser.add_argument("--task-ref")
        parser.add_argument("--wave-id")
    parser.add_argument("--config", required=True)
    parser.add_argument("--binding", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    replay = bool(args_list and args_list[0] == "replay")
    if replay:
        args_list = args_list[1:]
    try:
        args = _parser(replay).parse_args(args_list)
    except (ValueError, SystemExit) as exc:
        status = int(exc.code) if isinstance(exc, SystemExit) else 2
        if status == 0:
            return 0
        try:
            sys.stdout.buffer.write(_refusal("binding_invalid"))
            sys.stdout.buffer.flush()
        except OSError:
            return 1
        return 2
    config, work_deadline, total_deadline, failure = _start(args.config, args.binding)
    if failure is not None:
        response, status = failure
    elif replay:
        if not _text(args.operation_id, max_bytes=REQUEST_LIMIT):
            response, status = _refusal("invalid_request"), 2
        elif (args.task_ref is None) != (args.wave_id is None):
            response, status = _refusal("invalid_request"), 2
        elif args.task_ref is not None and (
            not _text(args.task_ref, max_bytes=REQUEST_LIMIT) or not _text(args.wave_id, max_bytes=REQUEST_LIMIT)
        ):
            response, status = _refusal("invalid_request"), 2
        else:
            scope = None if args.task_ref is None else {"task_ref": args.task_ref, "wave_id": args.wave_id}
            response, status, _replayed = _run_operation(
                config,
                args.operation_id,
                None,
                kind=None,
                operation=None,
                scope=scope,
                replay=True,
                work_deadline=work_deadline,
                total_deadline=total_deadline,
            )
    else:
        try:
            request = _read_bounded(sys.stdin.buffer, work_deadline)
            operation_id, kind, operation, scope = _operation_id(request, config.family)
            response, status, _replayed = _run_operation(
                config,
                operation_id,
                request,
                kind=kind,
                operation=operation,
                scope=scope,
                replay=False,
                work_deadline=work_deadline,
                total_deadline=total_deadline,
            )
        except _WaitExpired:
            response, status = _refusal("unavailable"), 3
        except _Failure as error:
            response, status = _refusal(error.code), error.status
    try:
        sys.stdout.buffer.write(response)
        sys.stdout.buffer.flush()
    except OSError:
        return 1
    return status


if __name__ == "__main__":
    sys.exit(main())
