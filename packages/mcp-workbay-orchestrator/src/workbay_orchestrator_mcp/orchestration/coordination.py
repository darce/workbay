"""Durable, typed coordination holds and events for local WorkBay workflows.

This module deliberately depends only on the Python standard library so it can
be used by small orchestration processes without importing the MCP server.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import fcntl
import json
import math
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator, NoReturn

_STATE_PATH = Path(".task-state") / "coordination"
_SCHEMA_VERSION = 1
_JOURNAL_SCHEMA_VERSION = 1
_SYSTEM_ACTOR = "coordination"
_COORDCHANNELS_MAX_MESSAGES = 1000
_COORDCHANNELS_MAX_RECEIPTS = 10000
_COORDCHANNELS_MAX_EVENTS = 32000
_COORDCHANNELS_MAX_BODY_BYTES = 4 * 1024
_COORDCHANNELS_MAX_EXTENSION_BYTES = 8 * 1024 * 1024
_COORDCONTROLS_MAX_HOLDS = 1000
_COORDCONTROLS_MAX_RECEIPTS = 10000
_COORDCONTROLS_MAX_EVENTS = 32000
_COORDCONTROLS_MAX_EXTENSION_BYTES = 8 * 1024 * 1024
_COORDCONTROLS_MAX_TEXT_BYTES = 256
_COORDCONTROLS_MAX_RESPONSE_BYTES = 64 * 1024

EVENT_KINDS: dict[str, dict[str, type]] = {
    "gate.final": {"lane": str, "sha": str, "verdict": str},
    "lane.integrated": {"lane": str, "sha": str, "into": str},
    "main.merged": {"sha": str, "lane": str},
    "hold.granted": {"resource": str, "owner": str, "token": int},
    "hold.released": {"resource": str, "owner": str, "token": int, "reason": str},
    "hold.expired": {"resource": str, "owner": str, "token": int},
}
_REGISTRY_LOCK = threading.RLock()


class _CoordinationStoreError(Exception):
    """Raised only for unreadable or structurally invalid durable state."""


class _LockDeadline(Exception):
    """The monotonic budget expired while acquiring the coordination lock."""


class _UsageError(Exception):
    """Raised by the CLI parser so usage failures can use the JSON envelope."""


class _JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise _UsageError


def _is_number(value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def _matches_type(value: Any, expected: type) -> bool:
    if expected is int:
        return type(value) is int
    if expected is float:
        return type(value) is float
    return isinstance(value, expected)


def _schema_for(kind: Any) -> dict[str, type] | None:
    if not isinstance(kind, str):
        return None
    with _REGISTRY_LOCK:
        schema = EVENT_KINDS.get(kind)
        return dict(schema) if schema is not None else None


def register_kind(kind: str, fields: dict[str, type]) -> dict[str, Any]:
    """Register a new event schema, allowing idempotent identical registration."""
    if not isinstance(kind, str) or not kind or not isinstance(fields, dict):
        return {"ok": False, "reason": "invalid_event_schema"}
    if any(
        not isinstance(name, str) or not name or not isinstance(value_type, type) for name, value_type in fields.items()
    ):
        return {"ok": False, "reason": "invalid_event_schema"}

    with _REGISTRY_LOCK:
        current = EVENT_KINDS.get(kind)
        if current is not None:
            if current == fields:
                return {"ok": True, "kind": kind}
            return {"ok": False, "reason": "conflicting_event_kind"}
        EVENT_KINDS[kind] = dict(fields)
    return {"ok": True, "kind": kind}


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_state_directory(path: Path) -> None:
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    path.mkdir(parents=True, exist_ok=True)
    for directory in reversed(missing):
        _fsync_directory(directory.parent)


@contextlib.contextmanager
def _locked(
    root: str | os.PathLike[str],
    *,
    deadline: float | None = None,
    now_fn: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    poll_s: float = 0.25,
) -> Iterator[Path]:
    state_dir = Path(root) / _STATE_PATH
    _ensure_state_directory(state_dir)
    lock_path = state_dir / "coord.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "r+b") as lock_file:
        if deadline is None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        else:
            # Always allow one immediate attempt, including a zero budget.
            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = deadline - now_fn()
                    if remaining <= 0:
                        raise _LockDeadline from None
                    sleep(min(poll_s, remaining))
                    if now_fn() >= deadline:
                        raise _LockDeadline
        try:
            _recover_journal_locked(state_dir)
            yield state_dir
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _default_state() -> dict[str, Any]:
    return {"schema_version": _SCHEMA_VERSION, "next_token": 1, "holds": [], "resources": {}}


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _claim_text(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        value.encode("utf-8")
    except UnicodeError:
        return False
    return True


def _claim_authority(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"authority_id", "authority_epoch", "task_ref", "wave_id"}
        and all(_claim_text(value[k]) for k in ("authority_id", "task_ref", "wave_id"))
        and type(value["authority_epoch"]) is int
        and value["authority_epoch"] > 0
    )


def _claim_wave_key(authority: dict[str, Any]) -> str:
    return json.dumps([authority["task_ref"], authority["wave_id"]], ensure_ascii=False, separators=(",", ":"))


def _coordcontrols_canonical_resource(authority: dict[str, Any], resource: str) -> str:
    """Injectively map an authority-scoped control resource into its namespace."""
    payload = json.dumps(
        [
            authority["authority_id"],
            authority["authority_epoch"],
            authority["task_ref"],
            authority["wave_id"],
            resource,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    import base64

    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return "mcpf-coordcontrol:v1:" + encoded


def _coordcontrols_legacy_owner(owner: dict[str, Any]) -> str:
    """Keep the base holds list compatible without exposing ambiguous delimiters."""
    import base64

    payload = json.dumps(
        [owner["principal"], owner["session_id"]], ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ).encode("ascii")
    return "mcpf-coordcontrols-owner:v1:" + base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _validate_claim_extension(extension: Any, next_token: int) -> None:
    """Validate the optional v1 expansion on every legacy load and journal replay.

    Keep this validator here so legacy-only writers cannot silently preserve an
    unsupported claim version or overwrite invalid claim state.
    """

    def require(condition: bool) -> None:
        if not condition:
            raise _CoordinationStoreError("coordclaims_state_invalid")

    def validate_record(record: Any) -> None:
        require(isinstance(record, dict) and set(record) == {"state", "owner", "token", "expires_at"})
        require(record["state"] in ("available", "claimed", "completed"))
        owner = record["owner"]
        require(isinstance(owner, dict) and set(owner) == {"principal", "session_id"})
        require(all(_claim_text(value) for value in owner.values()))
        require(type(record["token"]) is int and 0 < record["token"] < next_token)
        require(_is_number(record["expires_at"]))

    def validate_view(view: Any) -> None:
        require(isinstance(view, dict))
        require(set(view) == {"schema_version", "ok", "work_item_id", "state", "owner", "token", "expires_at"})
        require(type(view["schema_version"]) is int and view["schema_version"] == 1 and view["ok"] is True)
        require(_claim_text(view["work_item_id"]))
        validate_record({key: view[key] for key in ("state", "owner", "token", "expires_at")})

    require(isinstance(extension, dict) and set(extension) == {"schema_version", "waves"})
    require(type(extension["schema_version"]) is int and extension["schema_version"] == 1)
    require(isinstance(extension["waves"], dict))
    tokens: set[int] = set()
    for key, wave in extension["waves"].items():
        require(isinstance(wave, dict) and set(wave) == {"authority", "last_time", "items", "receipts"})
        require(_claim_authority(wave["authority"]))
        require(key == _claim_wave_key(wave["authority"]))
        require(_is_number(wave["last_time"]))
        require(isinstance(wave["items"], dict) and isinstance(wave["receipts"], dict))
        require(len(wave["items"]) <= len(wave["receipts"]) <= 10000)
        for work_item, record in wave["items"].items():
            require(_claim_text(work_item))
            validate_record(record)
            token = record["token"]
            require(token not in tokens)
            tokens.add(token)
        for operation_id, receipt in wave["receipts"].items():
            require(_claim_text(operation_id) and isinstance(receipt, dict))
            require(set(receipt) == {"fingerprint", "response"})
            digest = receipt["fingerprint"]
            require(isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest))
            response = receipt["response"]
            require(isinstance(response, dict) and type(response.get("ok")) is bool)
            require(type(response.get("schema_version")) is int and response["schema_version"] == 1)
            if response["ok"]:
                validate_view(response)
                require(response["work_item_id"] in wave["items"])
            else:
                require(response.get("error_code") in ("held_by", "stale_token", "completed"))
                if response["error_code"] == "held_by":
                    require(set(response) == {"schema_version", "ok", "error_code", "held_by"})
                    validate_view(response["held_by"])
                    require(response["held_by"]["state"] == "claimed")
                    require(response["held_by"]["work_item_id"] in wave["items"])
                else:
                    require(set(response) == {"schema_version", "ok", "error_code"})


def _validate_coordchannels_extension(extension: Any) -> None:
    """Reject unsupported or inconsistent durable channel state on every load."""

    def require(condition: bool) -> None:
        if not condition:
            raise _CoordinationStoreError("coordchannels_state_invalid")

    def text(value: Any, limit: int = 256) -> bool:
        if not _claim_text(value):
            return False
        try:
            return len(value.encode("utf-8")) <= limit
        except UnicodeError:
            return False

    def owner(value: Any) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"principal", "session_id"}
            and text(value["principal"])
            and text(value["session_id"])
        )

    def json_value(value: Any, depth: int = 0) -> bool:
        if depth > 32:
            return False
        if value is None or type(value) in (bool, int):
            return True
        if type(value) is float:
            return math.isfinite(value)
        if isinstance(value, str):
            try:
                value.encode("utf-8")
                return True
            except UnicodeError:
                return False
        if isinstance(value, list):
            return all(json_value(item, depth + 1) for item in value)
        if isinstance(value, dict):
            return all(
                isinstance(key, str) and json_value(key, depth + 1) and json_value(item, depth + 1)
                for key, item in value.items()
            )
        return False

    def recipients(value: Any) -> bool:
        return (
            isinstance(value, list)
            and 1 <= len(value) <= 32
            and all(text(item) for item in value)
            and len(set(value)) == len(value)
        )

    def valid_header(response: Any, success: bool) -> bool:
        return (
            isinstance(response, dict)
            and response.get("schema_version") == 1
            and type(response.get("schema_version")) is int
            and response.get("ok") is success
        )

    try:
        extension_size = len(
            json.dumps(extension, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
                "utf-8"
            )
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _CoordinationStoreError("coordchannels_state_invalid") from exc
    require(extension_size <= _COORDCHANNELS_MAX_EXTENSION_BYTES)
    require(isinstance(extension, dict) and set(extension) == {"schema_version", "waves"})
    require(type(extension["schema_version"]) is int and extension["schema_version"] == 1)
    waves = extension["waves"]
    require(isinstance(waves, dict))
    message_total = 0
    receipt_total = 0
    event_total = 0
    for key, wave in waves.items():
        require(isinstance(wave, dict) and set(wave) == {"authority", "messages", "receipts", "events", "highwater"})
        authority = wave["authority"]
        require(_claim_authority(authority) and key == _claim_wave_key(authority))
        require(all(text(authority[name]) for name in ("authority_id", "task_ref", "wave_id")))
        require(isinstance(wave["messages"], dict) and isinstance(wave["receipts"], dict))
        require(isinstance(wave["events"], list))
        messages = wave["messages"]
        receipts_map = wave["receipts"]
        channel_events = wave["events"]
        highwater = wave["highwater"]
        require(type(highwater) is int and highwater == len(channel_events))
        message_total += len(messages)
        receipt_total += len(receipts_map)
        event_total += len(channel_events)
        sent_by_id: dict[str, dict[str, Any]] = {}
        ack_events: set[tuple[str, str]] = set()

        for message_id, message in messages.items():
            require(
                text(message_id)
                and isinstance(message, dict)
                and set(message) == {"message_id", "version", "recipients", "body", "sender", "acknowledgments"}
            )
            require(message["message_id"] == message_id and type(message["version"]) is int and message["version"] == 1)
            require(recipients(message["recipients"]) and owner(message["sender"]))
            require(isinstance(message["body"], dict) and json_value(message["body"]))
            try:
                body_size = len(
                    json.dumps(
                        message["body"], sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
                    ).encode("utf-8")
                )
            except (TypeError, ValueError, OverflowError, RecursionError):
                body_size = _COORDCHANNELS_MAX_EXTENSION_BYTES + 1
            require(body_size <= 4096)
            acknowledgments = message["acknowledgments"]
            require(isinstance(acknowledgments, dict) and set(acknowledgments) <= set(message["recipients"]))
            for principal, receipt in acknowledgments.items():
                require(text(principal) and owner(receipt) and receipt["principal"] == principal)

        for index, event in enumerate(channel_events, start=1):
            require(isinstance(event, dict) and type(event.get("cursor")) is int and event["cursor"] == index)
            kind = event.get("kind")
            if kind == "message.sent":
                require(
                    set(event) == {"cursor", "kind", "message_id", "version", "sender", "recipients"}
                    and text(event["message_id"])
                    and type(event["version"]) is int
                    and event["version"] == 1
                    and owner(event["sender"])
                    and recipients(event["recipients"])
                    and event["message_id"] not in sent_by_id
                )
                message = messages.get(event["message_id"])
                require(
                    message is not None
                    and event["sender"] == message["sender"]
                    and event["recipients"] == message["recipients"]
                    and event["version"] == message["version"]
                )
                sent_by_id[event["message_id"]] = event
            elif kind == "message.acked":
                require(
                    set(event) == {"cursor", "kind", "message_id", "version", "sender", "recipients", "acknowledgment"}
                    and text(event["message_id"])
                    and type(event["version"]) is int
                    and event["version"] == 1
                    and owner(event["sender"])
                    and recipients(event["recipients"])
                    and owner(event["acknowledgment"])
                )
                message = messages.get(event["message_id"])
                principal = event["acknowledgment"]["principal"]
                require(
                    message is not None
                    and event["sender"] == message["sender"]
                    and event["recipients"] == message["recipients"]
                    and principal in message["recipients"]
                    and message["acknowledgments"].get(principal) == event["acknowledgment"]
                    and (event["message_id"], principal) not in ack_events
                )
                ack_events.add((event["message_id"], principal))
            else:
                require(False)

        require(set(sent_by_id) == set(messages))
        expected_acks = {
            (message_id, principal)
            for message_id, message in messages.items()
            for principal in message["acknowledgments"]
        }
        require(ack_events == expected_acks)

        for operation_id, receipt in receipts_map.items():
            require(text(operation_id) and isinstance(receipt, dict) and set(receipt) == {"fingerprint", "response"})
            digest = receipt["fingerprint"]
            require(
                isinstance(digest, str) and len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)
            )
            response = receipt["response"]
            require(isinstance(response, dict))
            if response.get("ok") is True:
                require(valid_header(response, True))
                if "message" in response:
                    require(set(response) == {"schema_version", "ok", "message"})
                    view = response["message"]
                    require(
                        isinstance(view, dict)
                        and set(view) == {"message_id", "version", "recipients", "body", "sender", "acknowledgments"}
                        and text(view["message_id"])
                        and type(view["version"]) is int
                        and view["version"] == 1
                        and recipients(view["recipients"])
                        and isinstance(view["body"], dict)
                        and json_value(view["body"])
                        and owner(view["sender"])
                        and isinstance(view["acknowledgments"], dict)
                        and set(view["acknowledgments"]) <= set(view["recipients"])
                    )
                    try:
                        view_body_size = len(
                            json.dumps(
                                view["body"],
                                sort_keys=True,
                                separators=(",", ":"),
                                ensure_ascii=False,
                                allow_nan=False,
                            ).encode("utf-8")
                        )
                    except (TypeError, ValueError, OverflowError, RecursionError):
                        view_body_size = _COORDCHANNELS_MAX_BODY_BYTES + 1
                    require(view_body_size <= _COORDCHANNELS_MAX_BODY_BYTES)
                    for principal, acknowledgment in view["acknowledgments"].items():
                        require(text(principal) and owner(acknowledgment) and acknowledgment["principal"] == principal)
                    message = messages.get(view["message_id"])
                    require(
                        message is not None
                        and view["version"] == message["version"]
                        and view["recipients"] == message["recipients"]
                        and view["body"] == message["body"]
                        and view["sender"] == message["sender"]
                        and view["acknowledgments"] == {}
                    )
                else:
                    require(
                        set(response) == {"schema_version", "ok", "message_id", "version", "acknowledgment"}
                        and text(response["message_id"])
                        and type(response["version"]) is int
                        and response["version"] == 1
                        and owner(response["acknowledgment"])
                        and response["message_id"] in messages
                        and response["acknowledgment"]["principal"] in messages[response["message_id"]]["recipients"]
                        and messages[response["message_id"]]["acknowledgments"].get(
                            response["acknowledgment"]["principal"]
                        )
                        == response["acknowledgment"]
                    )
            else:
                require(
                    valid_header(response, False)
                    and set(response) == {"schema_version", "ok", "error_code"}
                    and isinstance(response["error_code"], str)
                    and response["error_code"] in {"message_exists", "not_found", "version_mismatch", "capacity"}
                )

    require(message_total <= _COORDCHANNELS_MAX_MESSAGES)
    require(receipt_total <= _COORDCHANNELS_MAX_RECEIPTS)
    require(event_total <= _COORDCHANNELS_MAX_EVENTS)


def _validate_coordcontrols_extension(extension: Any, next_token: int, base_holds: list[Any]) -> None:
    """Validate controls state and its projection into the legacy holds list.

    The controls ledger is an extension of the existing coordination journal.
    Keeping this validator in the journal module means every legacy load and
    writer rejects an unsupported or malformed extension before replacing it.
    """

    def require(condition: bool) -> None:
        if not condition:
            raise _CoordinationStoreError("coordcontrols_state_invalid")

    def text(value: Any) -> bool:
        if not _claim_text(value):
            return False
        try:
            return len(value.encode("utf-8")) <= _COORDCONTROLS_MAX_TEXT_BYTES
        except UnicodeError:
            return False

    def owner(value: Any) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"principal", "session_id"}
            and text(value["principal"])
            and text(value["session_id"])
        )

    def strict_json(value: Any, depth: int = 0) -> bool:
        if depth > 32:
            return False
        if value is None or type(value) in (bool, int):
            return True
        if type(value) is float:
            return _is_number(value)
        if isinstance(value, str):
            try:
                value.encode("utf-8")
                return True
            except UnicodeError:
                return False
        if isinstance(value, list):
            return all(strict_json(item, depth + 1) for item in value)
        if isinstance(value, dict):
            return all(
                isinstance(key, str) and strict_json(key, depth + 1) and strict_json(item, depth + 1)
                for key, item in value.items()
            )
        return False

    def transport_fits(value: Any) -> bool:
        try:
            data = (json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")
        except (TypeError, ValueError, OverflowError, RecursionError):
            return False
        return len(data) <= _COORDCONTROLS_MAX_RESPONSE_BYTES

    def signal_fields(fields: Any) -> bool:
        return (
            isinstance(fields, dict)
            and set(fields) == {"name", "generation", "success"}
            and text(fields["name"])
            and type(fields["generation"]) is int
            and type(fields["success"]) is bool
        )

    def until(value: Any) -> bool:
        if value is None:
            return True
        return (
            isinstance(value, dict)
            and set(value) == {"kind", "match", "publisher"}
            and value["kind"] == "coord.signal"
            and isinstance(value["match"], dict)
            and set(value["match"]) <= {"name", "generation", "success"}
            and all(
                (key == "name" and text(item))
                or (key == "generation" and type(item) is int)
                or (key == "success" and type(item) is bool)
                for key, item in value["match"].items()
            )
            and text(value["publisher"])
        )

    def hold_view(value: Any, authority: dict[str, Any]) -> bool:
        if not isinstance(value, dict):
            return False
        common = {"resource", "mapped_resource", "state"}
        if value.get("state") == "available":
            return (
                set(value) == common
                and text(value["resource"])
                and value["mapped_resource"] == _coordcontrols_canonical_resource(authority, value["resource"])
                and transport_fits({"schema_version": 1, "ok": True, "hold": value})
            )
        return (
            set(value) == common | {"token", "owner", "expires_at", "until"}
            and value["state"] == "held"
            and text(value["resource"])
            and value["mapped_resource"] == _coordcontrols_canonical_resource(authority, value["resource"])
            and type(value["token"]) is int
            and 0 < value["token"] < next_token
            and owner(value["owner"])
            and _is_number(value["expires_at"])
            and until(value["until"])
            and transport_fits({"schema_version": 1, "ok": True, "hold": value})
        )

    try:
        extension_size = len(
            json.dumps(extension, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
                "utf-8"
            )
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _CoordinationStoreError("coordcontrols_state_invalid") from exc
    require(extension_size <= _COORDCONTROLS_MAX_EXTENSION_BYTES)
    require(
        isinstance(extension, dict)
        and set(extension) == {"schema_version", "last_time", "waves"}
        and type(extension["schema_version"]) is int
        and extension["schema_version"] == 1
        and _is_number(extension["last_time"])
        and isinstance(extension["waves"], dict)
    )

    base_by_token: dict[int, list[dict[str, Any]]] = {}
    for base_record in base_holds:
        if isinstance(base_record, dict) and type(base_record.get("token")) is int:
            base_by_token.setdefault(base_record["token"], []).append(base_record)

    hold_total = 0
    receipt_total = 0
    event_total = 0
    control_tokens: set[int] = set()
    control_by_token: dict[int, dict[str, Any]] = {}
    event_ids: set[str] = set()
    for key, wave in extension["waves"].items():
        require(
            isinstance(wave, dict)
            and set(wave) == {"authority", "holds", "receipts", "events", "highwater"}
            and _claim_authority(wave["authority"])
            and all(text(wave["authority"][name]) for name in ("authority_id", "task_ref", "wave_id"))
            and key == _claim_wave_key(wave["authority"])
            and isinstance(wave["holds"], list)
            and isinstance(wave["receipts"], dict)
            and isinstance(wave["events"], list)
            and type(wave["highwater"]) is int
            and wave["highwater"] == len(wave["events"])
        )
        hold_total += len(wave["holds"])
        receipt_total += len(wave["receipts"])
        event_total += len(wave["events"])
        require(hold_total <= _COORDCONTROLS_MAX_HOLDS)

        for record in wave["holds"]:
            require(
                isinstance(record, dict)
                and set(record) == {"resource", "mapped_resource", "owner", "token", "expires_at", "state", "until"}
                and text(record["resource"])
                and record["mapped_resource"]
                == _coordcontrols_canonical_resource(wave["authority"], record["resource"])
                and owner(record["owner"])
                and type(record["token"]) is int
                and 0 < record["token"] < next_token
                and record["token"] not in control_tokens
                and _is_number(record["expires_at"])
                and record["state"] in ("held", "released", "expired")
                and until(record["until"])
            )
            control_tokens.add(record["token"])
            control_by_token[record["token"]] = record
            backing = base_by_token.get(record["token"], [])
            require(len(backing) == 1)
            base_record = backing[0]
            require(
                base_record.get("resource") == record["mapped_resource"]
                and base_record.get("owner") == _coordcontrols_legacy_owner(record["owner"])
                and base_record.get("token") == record["token"]
                and base_record.get("expires_at") == record["expires_at"]
                and base_record.get("until_event") is None
                and base_record.get("match") is None
            )
            if record["state"] in ("released", "expired"):
                require(base_record.get("released", False) is True)
            view = {
                "resource": record["resource"],
                "mapped_resource": record["mapped_resource"],
                "state": "held",
                "token": record["token"],
                "owner": record["owner"],
                "expires_at": record["expires_at"],
                "until": record["until"],
            }
            require(hold_view(view, wave["authority"]))

        for cursor, item in enumerate(wave["events"], start=1):
            require(
                isinstance(item, dict)
                and set(item) == {"cursor", "event_id", "kind", "fields", "publisher", "session_id"}
                and type(item["cursor"]) is int
                and item["cursor"] == cursor
                and isinstance(item["event_id"], str)
                and len(item["event_id"]) == 32
                and all(char in "0123456789abcdef" for char in item["event_id"])
                and item["event_id"] not in event_ids
                and item["kind"] == "coord.signal"
                and signal_fields(item["fields"])
                and text(item["publisher"])
                and text(item["session_id"])
            )
            event_ids.add(item["event_id"])
            projected = {
                "cursor": item["cursor"],
                "kind": item["kind"],
                "fields": item["fields"],
                "publisher": item["publisher"],
                "session_id": item["session_id"],
            }
            require(transport_fits({"schema_version": 1, "ok": True, "event": projected}))

        events_by_cursor = {item["cursor"]: item for item in wave["events"]}

        for operation_id, receipt in wave["receipts"].items():
            require(
                text(operation_id)
                and isinstance(receipt, dict)
                and set(receipt) == {"fingerprint", "response"}
                and isinstance(receipt["fingerprint"], str)
                and len(receipt["fingerprint"]) == 64
                and all(char in "0123456789abcdef" for char in receipt["fingerprint"])
            )
            response = receipt["response"]
            require(
                isinstance(response, dict)
                and type(response.get("schema_version")) is int
                and response["schema_version"] == 1
                and type(response.get("ok")) is bool
                and strict_json(response)
                and transport_fits(response)
            )
            if response["ok"]:
                require(response.get("error_code") is None)
                if "hold" in response:
                    view = response["hold"]
                    require(set(response) == {"schema_version", "ok", "hold"} and hold_view(view, wave["authority"]))
                    require(view["state"] == "held")
                    control_record = control_by_token.get(view["token"])
                    require(control_record is not None)
                    assert control_record is not None
                    require(
                        view["resource"] == control_record["resource"]
                        and view["mapped_resource"] == control_record["mapped_resource"]
                        and view["owner"] == control_record["owner"]
                        and view["expires_at"] == control_record["expires_at"]
                        and view["until"] == control_record["until"]
                    )
                elif "event" in response:
                    event_view = response["event"]
                    require(
                        set(response) == {"schema_version", "ok", "event"}
                        and isinstance(event_view, dict)
                        and set(event_view) == {"cursor", "kind", "fields", "publisher", "session_id"}
                        and type(event_view["cursor"]) is int
                        and 1 <= event_view["cursor"] <= wave["highwater"]
                        and event_view["kind"] == "coord.signal"
                        and signal_fields(event_view["fields"])
                        and text(event_view["publisher"])
                        and text(event_view["session_id"])
                    )
                    persisted_event = events_by_cursor.get(event_view["cursor"])
                    require(
                        persisted_event is not None
                        and event_view["kind"] == persisted_event["kind"]
                        and event_view["fields"] == persisted_event["fields"]
                        and event_view["publisher"] == persisted_event["publisher"]
                        and event_view["session_id"] == persisted_event["session_id"]
                    )
                else:
                    require(
                        set(response) == {"schema_version", "ok", "resource", "token", "state"}
                        and text(response["resource"])
                        and type(response["token"]) is int
                        and 0 < response["token"] < next_token
                        and response["state"] == "released"
                    )
                    released_record = control_by_token.get(response["token"])
                    require(
                        released_record is not None
                        and released_record["resource"] == response["resource"]
                        and released_record["state"] == "released"
                    )
            else:
                code = response.get("error_code")
                require(code in {"held_by", "owner_mismatch", "stale_token", "not_held", "capacity"})
                if code == "held_by":
                    require(set(response) == {"schema_version", "ok", "error_code", "held_by"})
                    view = response["held_by"]
                    require(hold_view(view, wave["authority"]) and view["state"] == "held")
                    control_record = control_by_token.get(view["token"])
                    if control_record is not None:
                        require(
                            view["resource"] == control_record["resource"]
                            and view["mapped_resource"] == control_record["mapped_resource"]
                            and view["owner"] == control_record["owner"]
                            and view["expires_at"] == control_record["expires_at"]
                            and view["until"] == control_record["until"]
                        )
                    else:
                        backing = base_by_token.get(view["token"], [])
                        require(
                            len(backing) == 1
                            and backing[0].get("resource") == view["mapped_resource"]
                            and backing[0].get("expires_at") == view["expires_at"]
                            and view["owner"] == {"principal": "coordination", "session_id": "legacy"}
                            and view["until"] is None
                        )
                else:
                    require(set(response) == {"schema_version", "ok", "error_code"})

    require(hold_total <= _COORDCONTROLS_MAX_HOLDS)
    require(receipt_total <= _COORDCONTROLS_MAX_RECEIPTS)
    require(event_total <= _COORDCONTROLS_MAX_EVENTS)


def _validate_state_data(state: Any) -> dict[str, Any]:
    if (
        not isinstance(state, dict)
        or state.get("schema_version") != _SCHEMA_VERSION
        or type(state.get("next_token")) is not int
        or state["next_token"] < 1
        or not isinstance(state.get("holds"), list)
    ):
        raise _CoordinationStoreError("holds_state_invalid")
    state.setdefault("resources", {})
    if not isinstance(state["resources"], dict):
        raise _CoordinationStoreError("holds_state_invalid")
    highest_token = 0
    for hold_record in state["holds"]:
        if not isinstance(hold_record, dict):
            raise _CoordinationStoreError("holds_state_invalid")
        if (
            not isinstance(hold_record.get("resource"), str)
            or not isinstance(hold_record.get("owner"), str)
            or type(hold_record.get("token")) is not int
            or hold_record["token"] < 1
            or not _is_number(hold_record.get("expires_at"))
            or not isinstance(hold_record.get("released", False), bool)
            or not _valid_match(hold_record.get("match"))
            or (hold_record.get("until_event") is not None and not isinstance(hold_record.get("until_event"), str))
        ):
            raise _CoordinationStoreError("holds_state_invalid")
        highest_token = max(highest_token, hold_record["token"])
    if len({record["token"] for record in state["holds"]}) != len(state["holds"]):
        raise _CoordinationStoreError("holds_state_invalid")
    if state["next_token"] <= highest_token:
        raise _CoordinationStoreError("holds_state_invalid")
    for resource, record in state["resources"].items():
        if (
            not isinstance(resource, str)
            or not resource
            or not isinstance(record, dict)
            or type(record.get("highest_token")) is not int
            or record["highest_token"] < 0
            or "value" not in record
        ):
            raise _CoordinationStoreError("holds_state_invalid")
        try:
            json.dumps(record["value"], ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError, OverflowError) as exc:
            raise _CoordinationStoreError("holds_state_invalid") from exc
    if "coordclaims" in state:
        _validate_claim_extension(state["coordclaims"], state["next_token"])
        allocated_tokens = {record["token"] for record in state["holds"]}
        for wave in state["coordclaims"]["waves"].values():
            for record in wave["items"].values():
                if record["token"] in allocated_tokens:
                    raise _CoordinationStoreError("coordclaims_state_invalid")
                allocated_tokens.add(record["token"])
    if "coordchannels" in state:
        _validate_coordchannels_extension(state["coordchannels"])
    if "coordcontrols" in state:
        _validate_coordcontrols_extension(state["coordcontrols"], state["next_token"], state["holds"])
    return state


def _load_state(state_dir: Path) -> dict[str, Any]:
    path = state_dir / "holds.json"
    if not path.exists():
        return _default_state()
    try:
        state = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _CoordinationStoreError("holds_state_unreadable") from exc
    return _validate_state_data(state)


def _write_bytes_atomic(path: Path, data: bytes) -> None:
    temp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        with contextlib.suppress(OSError):
            temp_path.unlink()
        raise


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    data = (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")
    _write_bytes_atomic(path, data)


def _journal_path(state_dir: Path) -> Path:
    return state_dir / "journal.json"


def _load_journal(state_dir: Path) -> dict[str, Any] | None:
    path = _journal_path(state_dir)
    if not path.exists():
        return None
    try:
        snapshot = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise _CoordinationStoreError("coordination_journal_unreadable") from exc
    if (
        not isinstance(snapshot, dict)
        or snapshot.get("schema_version") != _JOURNAL_SCHEMA_VERSION
        or not isinstance(snapshot.get("events"), list)
    ):
        raise _CoordinationStoreError("coordination_journal_invalid")
    state = _validate_state_data(snapshot.get("state"))
    for event_record in snapshot["events"]:
        if (
            not isinstance(event_record, dict)
            or type(event_record.get("seq")) is not int
            or event_record["seq"] < 1
            or not isinstance(event_record.get("kind"), str)
            or not isinstance(event_record.get("event_id"), str)
            or not isinstance(event_record.get("fields"), dict)
            or not isinstance(event_record.get("actor"), str)
        ):
            raise _CoordinationStoreError("coordination_journal_invalid")
    return {"schema_version": _JOURNAL_SCHEMA_VERSION, "state": state, "events": snapshot["events"]}


def _event_records_from_file(events_path: Path) -> list[dict[str, Any]]:
    if not events_path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in events_path.read_bytes().splitlines():
        try:
            item = json.loads(line, parse_constant=_reject_json_constant)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            continue
        if (
            isinstance(item, dict)
            and type(item.get("seq")) is int
            and item["seq"] >= 1
            and isinstance(item.get("kind"), str)
        ):
            records.append(item)
    return records


def _append_events_idempotently_locked(state_dir: Path, events: list[dict[str, Any]]) -> None:
    events_path = state_dir / "events.jsonl"
    _repair_torn_tail_locked(events_path)
    existed = events_path.exists()
    recorded_ids = {
        event_id
        for record in _event_records_from_file(events_path)
        if isinstance((event_id := record.get("event_id")), str)
    }
    for event_record in events:
        event_id = event_record["event_id"]
        if event_id in recorded_ids:
            continue
        line = (
            json.dumps(event_record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode("utf-8")
        fd = os.open(events_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            written = os.write(fd, line)
            if written != len(line):
                raise OSError("short append to events.jsonl")
            os.fsync(fd)
        finally:
            os.close(fd)
        if not existed:
            _fsync_directory(state_dir)
            existed = True
        recorded_ids.add(event_id)


def _project_journal_locked(state_dir: Path, snapshot: dict[str, Any]) -> None:
    state_path = state_dir / "holds.json"
    state_bytes = (
        json.dumps(snapshot["state"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")
    try:
        state_is_current = state_path.read_bytes() == state_bytes
    except FileNotFoundError:
        state_is_current = False
    if not state_is_current:
        _write_json_atomic(state_path, snapshot["state"])

    _append_events_idempotently_locked(state_dir, snapshot["events"])
    _journal_path(state_dir).unlink()
    _fsync_directory(state_dir)


def _recover_journal_locked(state_dir: Path) -> None:
    snapshot = _load_journal(state_dir)
    if snapshot is not None:
        _project_journal_locked(state_dir, snapshot)


def _commit_transition(
    state_dir: Path,
    state: dict[str, Any],
    event_specs: list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]] | None = None,
) -> list[dict[str, Any]]:
    if event_specs:
        events_path = state_dir / "events.jsonl"
        _repair_torn_tail_locked(events_path)
        events = _event_records_from_file(events_path)
        next_sequence = max((item["seq"] for item in events), default=0) + 1
    else:
        next_sequence = 0

    _validate_state_data(state)
    new_events: list[dict[str, Any]] = []
    for event_spec in event_specs or []:
        if len(event_spec) == 3:
            kind, fields, actor = event_spec
            event_id = uuid.uuid4().hex
        else:
            kind, fields, actor, event_id = event_spec
        event_record = {
            "schema_version": _SCHEMA_VERSION,
            "event_id": event_id,
            "seq": next_sequence,
            "ts": time.time(),
            "kind": kind,
            "actor": actor,
            "fields": fields,
        }
        new_events.append(event_record)
        next_sequence += 1

    snapshot = {
        "schema_version": _JOURNAL_SCHEMA_VERSION,
        "state": state,
        "events": new_events,
    }
    _write_json_atomic(_journal_path(state_dir), snapshot)
    _recover_journal_locked(state_dir)
    return new_events


def _repair_torn_tail_locked(events_path: Path) -> None:
    if not events_path.exists():
        return
    data = events_path.read_bytes()
    if not data or data.endswith(b"\n"):
        return

    final_newline = data.rfind(b"\n")
    tail = data[final_newline + 1 :]
    torn_path = events_path.with_name("events.torn")
    torn_was_present = torn_path.exists()
    torn_fd = os.open(torn_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    with os.fdopen(torn_fd, "ab") as torn_file:
        marker = f"--- torn-tail ts={time.time():.6f} bytes={len(tail)} ---\n".encode("ascii")
        torn_file.write(marker)
        torn_file.write(tail)
        if not tail.endswith(b"\n"):
            torn_file.write(b"\n")
        torn_file.flush()
        os.fsync(torn_file.fileno())
    if not torn_was_present:
        _fsync_directory(events_path.parent)

    fd = os.open(events_path, os.O_WRONLY)
    try:
        os.ftruncate(fd, final_newline + 1)
        os.fsync(fd)
    finally:
        os.close(fd)


def _held_view(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "resource": record["resource"],
        "owner": record["owner"],
        "token": record["token"],
        "expires_at": record["expires_at"],
        "until_event": record.get("until_event"),
        "match": record.get("match"),
    }


def _active_record(state: dict[str, Any], resource: str, now: float) -> dict[str, Any] | None:
    for record in reversed(state["holds"]):
        if record["resource"] == resource and not record.get("released", False) and record["expires_at"] > now:
            return record
    return None


def _expire_locked(state_dir: Path, state: dict[str, Any], now: float) -> bool:
    expired: list[dict[str, Any]] = []
    for record in state["holds"]:
        if not record.get("released", False) and record["expires_at"] <= now:
            record["released"] = True
            record["release_reason"] = "expired"
            expired.append(record)

    if not expired:
        return False
    event_specs: list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]] = [
        (
            "hold.expired",
            {"resource": record["resource"], "owner": record["owner"], "token": record["token"]},
            _SYSTEM_ACTOR,
        )
        for record in expired
    ]
    _commit_transition(state_dir, state, event_specs)
    return True


def _valid_match(match: Any) -> bool:
    if match is None:
        return True
    if not isinstance(match, dict) or any(not isinstance(key, str) for key in match):
        return False
    try:
        json.dumps(match, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return False
    return True


def hold(
    root: str | os.PathLike[str],
    *,
    resource: str,
    owner: str,
    ttl_s: float,
    until_event: str | None = None,
    match: dict[str, Any] | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Acquire or renew a resource hold, returning a monotonically fenced token."""
    if not isinstance(resource, str) or not resource:
        return {"ok": False, "reason": "invalid_resource"}
    if not isinstance(owner, str) or not owner:
        return {"ok": False, "reason": "invalid_owner"}
    if not _is_number(ttl_s) or ttl_s <= 0:
        return {"ok": False, "reason": "invalid_ttl"}
    if now is not None and not _is_number(now):
        return {"ok": False, "reason": "invalid_now"}
    if not _valid_match(match):
        return {"ok": False, "reason": "invalid_match"}
    if until_event is not None and (not isinstance(until_event, str) or _schema_for(until_event) is None):
        return {"ok": False, "reason": "unknown_event_kind"}

    with _locked(root) as state_dir:
        try:
            state = _load_state(state_dir)
        except _CoordinationStoreError:
            return {"ok": False, "reason": "state_corrupt"}
        current_time = time.time() if now is None else float(now)
        requested_expiry = current_time + float(ttl_s)
        if not math.isfinite(requested_expiry):
            return {"ok": False, "reason": "invalid_ttl"}
        _expire_locked(state_dir, state, current_time)

        current = _active_record(state, resource, current_time)
        if current is not None:
            if current["owner"] != owner:
                return {"ok": False, "reason": "held_by", "held_by": _held_view(current)}
            current["expires_at"] = max(current["expires_at"], requested_expiry)
            _commit_transition(state_dir, state)
            return {"ok": True, **_held_view(current)}

        token = state["next_token"]
        state["next_token"] = token + 1
        record = {
            "resource": resource,
            "owner": owner,
            "token": token,
            "expires_at": requested_expiry,
            "until_event": until_event,
            "match": match,
            "released": False,
        }
        state["holds"].append(record)
        _commit_transition(
            state_dir,
            state,
            [("hold.granted", {"resource": resource, "owner": owner, "token": token}, owner)],
        )
        return {"ok": True, **_held_view(record)}


def release(root: str | os.PathLike[str], *, resource: str, owner: str, token: int) -> dict[str, Any]:
    """Release a hold only when both its owner and active fencing token match."""
    if not isinstance(resource, str) or not resource:
        return {"ok": False, "reason": "invalid_resource"}
    if not isinstance(owner, str) or not owner:
        return {"ok": False, "reason": "invalid_owner"}
    if type(token) is not int:
        return {"ok": False, "reason": "invalid_token"}

    with _locked(root) as state_dir:
        try:
            state = _load_state(state_dir)
        except _CoordinationStoreError:
            return {"ok": False, "reason": "state_corrupt"}
        current_time = time.time()
        _expire_locked(state_dir, state, current_time)
        current = _active_record(state, resource, current_time)
        if current is None:
            return {"ok": False, "reason": "not_held"}
        if current["owner"] != owner:
            return {"ok": False, "reason": "owner_mismatch"}
        if current["token"] != token:
            return {"ok": False, "reason": "stale_token"}

        current["released"] = True
        current["release_reason"] = "explicit"
        event_record = _commit_transition(
            state_dir,
            state,
            [("hold.released", {"resource": resource, "owner": owner, "token": token, "reason": "explicit"}, owner)],
        )[0]
        return {"ok": True, "resource": resource, "owner": owner, "token": token, "event_id": event_record["event_id"]}


def check_held(
    root: str | os.PathLike[str], *, resource: str, owner: str | None = None, now: float | None = None
) -> dict[str, Any] | None:
    """Return an active hold unless it is owned by ``owner``."""
    with _locked(root) as state_dir:
        state = _load_state(state_dir)
        current_time = time.time() if now is None else float(now)
        _expire_locked(state_dir, state, current_time)
        current = _active_record(state, resource, current_time)
        if current is None or current["owner"] == owner:
            return None
        return _held_view(current)


def check_fence(root: str | os.PathLike[str], *, resource: str, token: int) -> dict[str, Any]:
    """Advisory lease check; this read alone does not fence a protected write."""
    if type(token) is not int:
        return {"ok": False, "reason": "invalid_token"}
    with _locked(root) as state_dir:
        state = _load_state(state_dir)
        current_time = time.time()
        _expire_locked(state_dir, state, current_time)
        current = _active_record(state, resource, current_time)
        if current is None:
            return {"ok": False, "reason": "not_held"}
        if current["token"] != token:
            return {"ok": False, "reason": "stale_token"}
        return {"ok": True, **_held_view(current)}


def fenced_apply(
    root: str | os.PathLike[str],
    *,
    resource: str,
    token: int,
    mutate: Callable[[Any], Any],
    now: float | None = None,
) -> dict[str, Any]:
    """Atomically apply a JSON resource update with its highest accepted fencing token.

    ``mutate`` receives a copy of the prior stored value and returns its replacement.
    The value and token are committed together in the authoritative coordination
    journal, so callers must represent protected writes through this resource value.
    """
    if not isinstance(resource, str) or not resource:
        return {"ok": False, "reason": "invalid_resource"}
    if type(token) is not int or token < 1:
        return {"ok": False, "reason": "invalid_token"}
    if not callable(mutate):
        return {"ok": False, "reason": "invalid_mutator"}
    if now is not None and not _is_number(now):
        return {"ok": False, "reason": "invalid_now"}

    with _locked(root) as state_dir:
        try:
            state = _load_state(state_dir)
        except _CoordinationStoreError:
            return {"ok": False, "reason": "state_corrupt"}
        current_time = time.time() if now is None else float(now)
        _expire_locked(state_dir, state, current_time)

        resource_state = state["resources"].get(resource, {"highest_token": 0, "value": None})
        if token < resource_state["highest_token"]:
            return {"ok": False, "reason": "stale_fence_token"}
        current = _active_record(state, resource, current_time)
        if current is None:
            return {"ok": False, "reason": "not_held"}
        if current["token"] != token:
            return {"ok": False, "reason": "stale_fence_token"}

        try:
            prior_value = copy.deepcopy(resource_state["value"])
            replacement = mutate(prior_value)
            updated_value = prior_value if replacement is None else replacement
            updated_value = json.loads(
                json.dumps(updated_value, ensure_ascii=False, allow_nan=False),
                parse_constant=_reject_json_constant,
            )
        except Exception:
            return {"ok": False, "reason": "resource_mutation_failed"}

        state["resources"][resource] = {
            "highest_token": max(token, resource_state["highest_token"]),
            "value": updated_value,
        }
        _commit_transition(state_dir, state)
        return {"ok": True, "resource": resource, "token": token, "value": updated_value}


def active_holds(root: str | os.PathLike[str], now: float | None = None) -> list[dict[str, Any]]:
    """Return all non-expired holds, expiring old entries as part of the read."""
    with _locked(root) as state_dir:
        state = _load_state(state_dir)
        current_time = time.time() if now is None else float(now)
        _expire_locked(state_dir, state, current_time)
        active = [
            record
            for record in state["holds"]
            if not record.get("released", False) and record["expires_at"] > current_time
        ]
        active.sort(key=lambda item: (item["resource"], item["token"]))
        return [_held_view(record) for record in active]


def _validate_event(kind: str, fields: Any, actor: Any) -> dict[str, Any] | None:
    if not isinstance(kind, str) or not kind:
        return {"ok": False, "reason": "unknown_event_kind"}
    schema = _schema_for(kind)
    if schema is None:
        return {"ok": False, "reason": "unknown_event_kind"}
    if not isinstance(fields, dict):
        return {"ok": False, "reason": "event_schema_violation", "field": "fields"}
    if not isinstance(actor, str) or not actor:
        return {"ok": False, "reason": "event_schema_violation", "field": "actor"}
    for field_name, expected_type in schema.items():
        if field_name not in fields or not _matches_type(fields[field_name], expected_type):
            return {"ok": False, "reason": "event_schema_violation", "field": field_name}
    try:
        json.dumps(fields, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return {"ok": False, "reason": "event_schema_violation", "field": "fields"}
    return None


def event(root: str | os.PathLike[str], kind: str, fields: dict[str, Any], *, actor: str) -> dict[str, Any]:
    """Append a schema-checked event and release matching until-event holds."""
    validation = _validate_event(kind, fields, actor)
    if validation is not None:
        return validation

    with _locked(root) as state_dir:
        try:
            state = _load_state(state_dir)
        except _CoordinationStoreError:
            return {"ok": False, "reason": "state_corrupt"}
        current_time = time.time()
        _expire_locked(state_dir, state, current_time)
        event_id = uuid.uuid4().hex
        event_specs: list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]] = [
            (kind, fields, actor, event_id)
        ]

        for record in state["holds"]:
            if record.get("released", False) or record.get("expires_at", 0) <= current_time:
                continue
            if record.get("until_event") != kind:
                continue
            match = record.get("match") or {}
            if not all(fields.get(key) == value for key, value in match.items()):
                continue
            reason = f"until_event:{event_id}"
            record["released"] = True
            record["release_reason"] = reason
            event_specs.append(
                (
                    "hold.released",
                    {
                        "resource": record["resource"],
                        "owner": record["owner"],
                        "token": record["token"],
                        "reason": reason,
                    },
                    _SYSTEM_ACTOR,
                )
            )
        event_records = _commit_transition(state_dir, state, event_specs)
        event_record = event_records[0]
        if event_record["event_id"] != event_id:
            raise _CoordinationStoreError("coordination_journal_invalid")
        return event_record


def _scan_events(events_path: Path) -> tuple[list[dict[str, Any]], int, int]:
    try:
        data = events_path.read_bytes()
    except FileNotFoundError:
        return [], 0, 0

    events: list[dict[str, Any]] = []
    skipped = 0
    last_seq = 0
    lines = data.splitlines(keepends=True)
    for line in lines:
        if not line.endswith(b"\n"):
            skipped += 1
            continue
        try:
            item = json.loads(line, parse_constant=_reject_json_constant)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            skipped += 1
            continue
        if not isinstance(item, dict) or not isinstance(item.get("kind"), str) or type(item.get("seq")) is not int:
            skipped += 1
            continue
        events.append(item)
        last_seq = max(last_seq, item["seq"])
    return events, skipped, last_seq


def wait(
    root: str | os.PathLike[str],
    kind: str,
    match: dict[str, Any] | None = None,
    *,
    deadline_s: float,
    after_seq: int = 0,
    poll_s: float = 0.25,
    now_fn: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Poll events with a monotonic budget for lock contention and poll sleeps.

    A zero budget permits one immediate uncontended scan. Filesystem operations,
    journal recovery and whole-log scans are not hard-bounded by this budget.
    """
    if not isinstance(kind, str) or not kind:
        return {"ok": False, "reason": "invalid_kind", "last_seq": 0, "skipped_lines": 0}
    if not _valid_match(match):
        return {"ok": False, "reason": "invalid_match", "last_seq": 0, "skipped_lines": 0}
    if not _is_number(deadline_s) or deadline_s < 0:
        return {"ok": False, "reason": "invalid_deadline", "last_seq": 0, "skipped_lines": 0}
    if type(after_seq) is not int or after_seq < 0:
        return {"ok": False, "reason": "invalid_after_seq", "last_seq": 0, "skipped_lines": 0}
    if not _is_number(poll_s) or poll_s <= 0:
        return {"ok": False, "reason": "invalid_poll_interval", "last_seq": 0, "skipped_lines": 0}

    deadline = now_fn() + float(deadline_s)
    last_seq = 0
    skipped_lines = 0
    while True:
        try:
            with _locked(root, deadline=deadline, now_fn=now_fn, sleep=sleep, poll_s=float(poll_s)) as state_dir:
                event_records, skipped_lines, last_seq = _scan_events(state_dir / "events.jsonl")
        except _LockDeadline:
            return {"ok": False, "reason": "deadline", "last_seq": last_seq, "skipped_lines": skipped_lines}
        for event_record in event_records:
            fields = event_record.get("fields")
            if event_record["seq"] <= after_seq or event_record["kind"] != kind or not isinstance(fields, dict):
                continue
            if match is not None and not all(fields.get(key) == value for key, value in match.items()):
                continue
            return {"ok": True, "event": event_record, "skipped_lines": skipped_lines}

        remaining = deadline - now_fn()
        if remaining <= 0:
            return {"ok": False, "reason": "deadline", "last_seq": last_seq, "skipped_lines": skipped_lines}
        sleep(min(float(poll_s), remaining))
        if now_fn() >= deadline:
            return {"ok": False, "reason": "deadline", "last_seq": last_seq, "skipped_lines": skipped_lines}


def _parse_pairs(items: list[str], schema: dict[str, type] | None) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in items:
        key, separator, raw_value = item.partition("=")
        if not separator or not key:
            raise _UsageError
        expected = schema.get(key) if schema is not None else None
        try:
            if expected is int:
                value: Any = int(raw_value)
            elif expected is float:
                value = float(raw_value)
            elif expected is bool:
                if raw_value.lower() not in ("true", "false"):
                    raise ValueError
                value = raw_value.lower() == "true"
            else:
                value = raw_value
        except ValueError as exc:
            raise _UsageError from exc
        parsed[key] = value
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = _JsonArgumentParser(prog="coordination")
    subparsers = parser.add_subparsers(dest="command", required=True)

    hold_parser = subparsers.add_parser("hold")
    hold_parser.add_argument("--root", required=True)
    hold_parser.add_argument("--resource", required=True)
    hold_parser.add_argument("--owner", required=True)
    hold_parser.add_argument("--ttl-s", required=True, type=float)
    hold_parser.add_argument("--until-event")
    hold_parser.add_argument("--match", action="append", default=[])

    release_parser = subparsers.add_parser("release")
    release_parser.add_argument("--root", required=True)
    release_parser.add_argument("--resource", required=True)
    release_parser.add_argument("--owner", required=True)
    release_parser.add_argument("--token", required=True, type=int)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--root", required=True)
    check_parser.add_argument("--resource", required=True)
    check_parser.add_argument("--owner")

    fence_parser = subparsers.add_parser("fence")
    fence_parser.add_argument("--root", required=True)
    fence_parser.add_argument("--resource", required=True)
    fence_parser.add_argument("--token", required=True, type=int)

    holds_parser = subparsers.add_parser("holds")
    holds_parser.add_argument("--root", required=True)

    event_parser = subparsers.add_parser("event")
    event_parser.add_argument("--root", required=True)
    event_parser.add_argument("--kind", required=True)
    event_parser.add_argument("--actor", default="cli")
    event_parser.add_argument("--field", action="append", default=[])

    wait_parser = subparsers.add_parser("wait")
    wait_parser.add_argument("--root", required=True)
    wait_parser.add_argument("--kind", required=True)
    wait_parser.add_argument("--match", action="append", default=[])
    wait_parser.add_argument("--deadline-s", required=True, type=float)
    wait_parser.add_argument("--after-seq", type=int, default=0)
    wait_parser.add_argument("--poll-s", type=float, default=0.25)
    return parser


def _exit_code(result: dict[str, Any]) -> int:
    if result.get("ok") is True or ("event_id" in result and "seq" in result):
        return 0
    if result.get("reason") == "deadline":
        return 4
    if result.get("reason") in {"unknown_event_kind", "event_schema_violation", "invalid_event_schema", "usage_error"}:
        return 2
    return 3


def main(argv: list[str] | None = None) -> int:
    try:
        args = _build_parser().parse_args(argv)
        if args.command == "hold":
            match = _parse_pairs(args.match, _schema_for(args.until_event) if args.until_event else None)
            result = hold(
                args.root,
                resource=args.resource,
                owner=args.owner,
                ttl_s=args.ttl_s,
                until_event=args.until_event,
                match=match or None,
            )
        elif args.command == "release":
            result = release(args.root, resource=args.resource, owner=args.owner, token=args.token)
        elif args.command == "check":
            held = check_held(args.root, resource=args.resource, owner=args.owner)
            result = {"ok": held is None, "held_by": held}
            if held is not None:
                result["reason"] = "held_by"
        elif args.command == "fence":
            result = check_fence(args.root, resource=args.resource, token=args.token)
        elif args.command == "holds":
            result = {"ok": True, "holds": active_holds(args.root)}
        elif args.command == "event":
            schema = _schema_for(args.kind)
            fields = _parse_pairs(args.field, schema)
            result = event(args.root, args.kind, fields, actor=args.actor)
        elif args.command == "wait":
            schema = _schema_for(args.kind)
            match = _parse_pairs(args.match, schema)
            result = wait(
                args.root,
                args.kind,
                match or None,
                deadline_s=args.deadline_s,
                after_seq=args.after_seq,
                poll_s=args.poll_s,
            )
        else:
            result = {"ok": False, "reason": "usage_error"}
    except _UsageError:
        result = {"ok": False, "reason": "usage_error"}
    except _CoordinationStoreError:
        result = {"ok": False, "reason": "state_corrupt"}
    except OSError:
        result = {"ok": False, "reason": "storage_error"}

    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return _exit_code(result)


if __name__ == "__main__":
    sys.exit(main())
