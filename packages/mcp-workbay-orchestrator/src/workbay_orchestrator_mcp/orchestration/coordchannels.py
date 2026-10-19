"""Durable single-authority messages, acknowledgments, and bounded event cursors.

Channel state, idempotency receipts, and channel events share the coordination
journal and its serial writer. This module is intentionally harness agnostic.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, cast

from . import coordination as journal

MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_BODY_BYTES = 4 * 1024
MAX_MESSAGES = journal._COORDCHANNELS_MAX_MESSAGES
MAX_OPERATIONS = journal._COORDCHANNELS_MAX_RECEIPTS
MAX_EVENTS = journal._COORDCHANNELS_MAX_EVENTS
MAX_EXTENSION_BYTES = journal._COORDCHANNELS_MAX_EXTENSION_BYTES
LOCK_BUDGET_SECONDS = 2.0
_AUTH_FIELDS = {"authority_id", "authority_epoch", "task_ref", "wave_id"}
_BASE_FIELDS = _AUTH_FIELDS | {"schema_version", "operation_id", "operation"}
_IDENTITY_FIELDS = {"root", "principal", "session_id", "owner"}
_OPS = {"send", "read", "ack", "events"}


def _error(code: str) -> dict[str, Any]:
    return {"schema_version": 1, "ok": False, "error_code": code}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _text(value: Any, limit: int = 256) -> bool:
    if not journal._claim_text(value):
        return False
    try:
        return len(value.encode("utf-8")) <= limit
    except UnicodeError:
        return False


def _json_value(value: Any, depth: int = 0) -> bool:
    if depth > 32:
        return False
    if value is None or type(value) in (bool, int):
        return True
    if type(value) is float:
        return journal._is_number(value)
    if isinstance(value, str):
        try:
            value.encode("utf-8")
            return True
        except UnicodeError:
            return False
    if isinstance(value, list):
        return all(_json_value(item, depth + 1) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _json_value(key, depth + 1) and _json_value(item, depth + 1)
            for key, item in value.items()
        )
    return False


def _body_size(body: dict[str, Any]) -> int:
    # Request bodies and persisted extensions use canonical UTF-8 accounting.
    return len(_canonical(body).encode("utf-8"))


def _validate_request(request: Any, authority: Any, principal: Any, session_id: Any) -> tuple[dict, dict, dict]:
    if not journal._claim_authority(authority) or not all(
        _text(authority[k]) for k in ("authority_id", "task_ref", "wave_id")
    ):
        raise ValueError("invalid trusted authority")
    if not _text(principal) or not _text(session_id):
        raise ValueError("invalid trusted identity")
    if not isinstance(request, dict) or not _BASE_FIELDS <= request.keys() or request.keys() & _IDENTITY_FIELDS:
        raise ValueError("invalid request fields")
    operation = request["operation"]
    if not isinstance(operation, str) or operation not in _OPS:
        raise ValueError("invalid operation")
    operation_fields = {
        "send": {"message_id", "version", "recipients", "body"},
        "read": {"message_id"},
        "ack": {"message_id", "version"},
        "events": {"after_cursor", "limit"},
    }[operation]
    optional = {"filters"} if operation == "events" else set()
    if request.keys() != _BASE_FIELDS | operation_fields | (request.keys() & optional):
        raise ValueError("unexpected request fields")
    if type(request["schema_version"]) is not int or request["schema_version"] != 1:
        raise ValueError("invalid request version")
    if (
        not _text(request["operation_id"])
        or type(authority["authority_epoch"]) is not int
        or authority["authority_epoch"] <= 0
    ):
        raise ValueError("invalid request identity")
    encoded = _canonical(request).encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ValueError("request too large")
    if not _json_value(request):
        raise ValueError("request is not strict JSON")
    snapshot = cast(dict, json.loads(encoded, parse_constant=journal._reject_json_constant))
    trusted_authority = cast(dict, json.loads(_canonical(authority)))
    identity = {"principal": principal, "session_id": session_id}

    if not journal._claim_authority({key: snapshot[key] for key in _AUTH_FIELDS}):
        raise ValueError("invalid request authority")
    if operation in ("send", "read", "ack"):
        if not _text(snapshot["message_id"]):
            raise ValueError("invalid message id")
    if operation == "send":
        if type(snapshot["version"]) is not int or snapshot["version"] != 1:
            raise ValueError("unsupported message version")
        recipients = snapshot["recipients"]
        if (
            not isinstance(recipients, list)
            or not 1 <= len(recipients) <= 32
            or not all(_text(item) for item in recipients)
            or len(set(recipients)) != len(recipients)
        ):
            raise ValueError("invalid recipients")
        body = snapshot["body"]
        if not isinstance(body, dict) or not _json_value(body) or _body_size(body) > MAX_BODY_BYTES:
            raise ValueError("invalid message body")
    elif operation == "ack":
        if type(snapshot["version"]) is not int or snapshot["version"] <= 0:
            raise ValueError("invalid acknowledgment version")
    elif operation == "events":
        if type(snapshot["after_cursor"]) is not int or snapshot["after_cursor"] < 0:
            raise ValueError("invalid cursor")
        if type(snapshot["limit"]) is not int or not 1 <= snapshot["limit"] <= 100:
            raise ValueError("invalid event limit")
        filters = snapshot.get("filters", {})
        if not isinstance(filters, dict) or filters.keys() - {"kind", "message_id"}:
            raise ValueError("invalid event filters")
        if "kind" in filters and filters["kind"] not in ("message.sent", "message.acked"):
            raise ValueError("invalid event kind filter")
        if "message_id" in filters and not _text(filters["message_id"]):
            raise ValueError("invalid message filter")
    return snapshot, trusted_authority, identity


def _fingerprint(request: dict, authority: dict, identity: dict) -> str:
    payload = {"request": request, "authority": authority, "identity": identity}
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _message_view(message: dict) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ok": True,
        "message": {
            "message_id": message["message_id"],
            "version": message["version"],
            "recipients": copy.deepcopy(message["recipients"]),
            "body": copy.deepcopy(message["body"]),
            "sender": copy.deepcopy(message["sender"]),
            "acknowledgments": copy.deepcopy(message["acknowledgments"]),
        },
    }


def _wave_key(authority: dict) -> str:
    return journal._claim_wave_key(authority)


def _default_extension() -> dict[str, Any]:
    return {"schema_version": 1, "waves": {}}


def _default_wave(authority: dict) -> dict[str, Any]:
    return {"authority": copy.deepcopy(authority), "messages": {}, "receipts": {}, "events": [], "highwater": 0}


def _extension_size(extension: dict) -> int:
    # The journal extension is persisted as canonical UTF-8 JSON, independently
    # of the ASCII-escaped stdio response representation.
    return len(_canonical(extension).encode("utf-8"))


def _counts(extension: dict) -> tuple[int, int, int]:
    waves = extension["waves"].values()
    messages = sum(len(wave["messages"]) for wave in waves)
    receipts = sum(len(wave["receipts"]) for wave in waves)
    events = sum(len(wave["events"]) for wave in waves)
    return messages, receipts, events


def _response_fits(response: dict) -> bool:
    try:
        # Match coordclaims_transport._encode exactly: ensure_ascii JSON,
        # compact separators, strict numbers, and the terminating newline.
        encoded = json.dumps(response, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
        return len(encoded.encode("ascii")) <= MAX_RESPONSE_BYTES
    except (TypeError, ValueError, OverflowError, RecursionError, RuntimeError):
        return False


def _record_mutation(
    state: dict,
    extension: dict,
    key: str,
    wave: dict,
    operation_id: str,
    fingerprint: str,
    response: dict,
    event: dict | None,
    journal_event: tuple[str, dict, str] | None,
) -> tuple[dict, list[tuple[str, dict, str] | tuple[str, dict, str, str]]] | None:
    if not _response_fits(response):
        return None
    candidate = copy.deepcopy(state)
    candidate_extension = copy.deepcopy(extension)
    candidate_wave = copy.deepcopy(wave)
    if event is not None:
        candidate_wave["events"].append(event)
        candidate_wave["highwater"] = event["cursor"]
    candidate_wave["receipts"][operation_id] = {"fingerprint": fingerprint, "response": copy.deepcopy(response)}
    candidate_extension["waves"][key] = candidate_wave
    candidate["coordchannels"] = candidate_extension
    if _extension_size(candidate_extension) > MAX_EXTENSION_BYTES:
        return None
    event_specs: list[tuple[str, dict, str] | tuple[str, dict, str, str]] = []
    if journal_event is not None:
        event_specs.append(journal_event)
    return candidate, event_specs


def _commit_mutation(
    state_dir: Path,
    state: dict,
    extension: dict,
    key: str,
    wave: dict,
    operation_id: str,
    fingerprint: str,
    response: dict,
    *,
    event: dict | None = None,
    journal_event: tuple[str, dict, str] | None = None,
) -> dict:
    candidate = _record_mutation(state, extension, key, wave, operation_id, fingerprint, response, event, journal_event)
    if candidate is None:
        response = _error("capacity")
        candidate = _record_mutation(state, extension, key, wave, operation_id, fingerprint, response, None, None)
    if candidate is None:
        return _error("capacity")
    candidate_state, event_specs = candidate
    journal._commit_transition(state_dir, candidate_state, event_specs)
    return response


def _visible(event: dict, principal: str) -> bool:
    return event["sender"]["principal"] == principal or principal in event["recipients"]


def _event_matches(event: dict, filters: dict) -> bool:
    return all(event[field] == value for field, value in filters.items())


def _read_events(wave: dict, principal: str, request: dict) -> dict:
    after = request["after_cursor"]
    highwater = wave["highwater"]
    if after > highwater:
        return _error("invalid_cursor")
    filters = request.get("filters", {})
    selected: list[dict] = []
    next_cursor = after
    stopped_before = False
    for event in wave["events"][after:]:
        cursor = event["cursor"]
        if _visible(event, principal) and _event_matches(event, filters):
            candidate = {
                "schema_version": 1,
                "ok": True,
                "events": selected + [copy.deepcopy(event)],
                # Size against the largest possible final watermark and the
                # longer boolean literal so the final envelope cannot overflow.
                "next_cursor": highwater,
                "has_more": False,
            }
            if len(selected) >= request["limit"] or not _response_fits(candidate):
                stopped_before = True
                break
            selected.append(copy.deepcopy(event))
        next_cursor = cursor
    if not stopped_before:
        next_cursor = highwater
    result = {
        "schema_version": 1,
        "ok": True,
        "events": selected,
        "next_cursor": next_cursor,
        "has_more": stopped_before,
    }
    return result if _response_fits(result) else _error("response_too_large")


def _execute_locked(
    state_dir: Path, request: dict, authority: dict, principal: str, identity: dict, fingerprint: str
) -> dict:
    state = journal._load_state(state_dir)
    extension = copy.deepcopy(state.get("coordchannels", _default_extension()))
    key = _wave_key(authority)
    wave = extension["waves"].get(key)
    if wave is not None and wave["authority"] != authority:
        return _error("scope_mismatch")
    if wave is None:
        wave = _default_wave(authority)

    operation = request["operation"]
    if operation == "read":
        message = wave["messages"].get(request["message_id"])
        if message is None or (message["sender"]["principal"] != principal and principal not in message["recipients"]):
            return _error("not_found")
        return _message_view(message)
    if operation == "events":
        return _read_events(wave, principal, request)

    operation_id = request["operation_id"]
    old_receipt = wave["receipts"].get(operation_id)
    if old_receipt is not None:
        if old_receipt["fingerprint"] != fingerprint:
            return _error("idempotency_conflict")
        return copy.deepcopy(old_receipt["response"])

    message_id = request["message_id"]
    if operation == "send":
        messages, receipts, channel_events = _counts(extension)
        if receipts >= MAX_OPERATIONS:
            return _error("capacity")
        if message_id in wave["messages"]:
            response = _error("message_exists")
            return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, response)
        if messages >= MAX_MESSAGES or channel_events >= MAX_EVENTS:
            response = _error("capacity")
            return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, response)
        message = {
            "message_id": message_id,
            "version": 1,
            "recipients": copy.deepcopy(request["recipients"]),
            "body": copy.deepcopy(request["body"]),
            "sender": copy.deepcopy(identity),
            "acknowledgments": {},
        }
        response = _message_view(message)
        cursor = wave["highwater"] + 1
        event = {
            "cursor": cursor,
            "kind": "message.sent",
            "message_id": message_id,
            "version": 1,
            "sender": copy.deepcopy(identity),
            "recipients": copy.deepcopy(message["recipients"]),
        }
        candidate_extension = copy.deepcopy(extension)
        candidate_wave = copy.deepcopy(wave)
        candidate_wave["messages"][message_id] = message
        candidate_extension["waves"][key] = candidate_wave
        # Keep the single message authority and its cursor record in the same
        # candidate snapshot passed to the journal commit.
        candidate_state = copy.deepcopy(state)
        candidate_state["coordchannels"] = candidate_extension
        fields = {**authority, **event}
        journal_event = ("message.sent", fields, principal)
        candidate = _record_mutation(
            candidate_state,
            candidate_extension,
            key,
            candidate_wave,
            operation_id,
            fingerprint,
            response,
            event,
            journal_event,
        )
        if candidate is None:
            return _commit_mutation(
                state_dir,
                state,
                extension,
                key,
                wave,
                operation_id,
                fingerprint,
                _error("capacity"),
            )
        committed_state, event_specs = candidate
        # _record_mutation deep-copies the supplied state and extension, and
        # the journal validator checks the complete extension before commit.
        journal._commit_transition(state_dir, committed_state, event_specs)
        return response

    messages, receipts, channel_events = _counts(extension)
    if receipts >= MAX_OPERATIONS:
        return _error("capacity")
    message = wave["messages"].get(message_id)
    if message is None or principal not in message["recipients"]:
        response = _error("not_found")
        return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, response)
    if request["version"] != message["version"]:
        response = _error("version_mismatch")
        return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, response)
    existing_ack = message["acknowledgments"].get(principal)
    if existing_ack is not None:
        response = {
            "schema_version": 1,
            "ok": True,
            "message_id": message_id,
            "version": message["version"],
            "acknowledgment": copy.deepcopy(existing_ack),
        }
        return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, response)
    if channel_events >= MAX_EVENTS:
        return _commit_mutation(state_dir, state, extension, key, wave, operation_id, fingerprint, _error("capacity"))

    acknowledgment = copy.deepcopy(identity)
    response = {
        "schema_version": 1,
        "ok": True,
        "message_id": message_id,
        "version": message["version"],
        "acknowledgment": acknowledgment,
    }
    projected_message = copy.deepcopy(message)
    projected_message["acknowledgments"][principal] = acknowledgment
    if not _response_fits(_message_view(projected_message)):
        return _commit_mutation(
            state_dir,
            state,
            extension,
            key,
            wave,
            operation_id,
            fingerprint,
            _error("capacity"),
        )
    cursor = wave["highwater"] + 1
    event = {
        "cursor": cursor,
        "kind": "message.acked",
        "message_id": message_id,
        "version": message["version"],
        "sender": copy.deepcopy(message["sender"]),
        "recipients": copy.deepcopy(message["recipients"]),
        "acknowledgment": acknowledgment,
    }
    candidate_extension = copy.deepcopy(extension)
    candidate_wave = copy.deepcopy(wave)
    candidate_wave["messages"][message_id]["acknowledgments"][principal] = acknowledgment
    candidate_extension["waves"][key] = candidate_wave
    candidate_state = copy.deepcopy(state)
    candidate_state["coordchannels"] = candidate_extension
    fields = {**authority, **event}
    journal_event = ("message.acked", fields, principal)
    candidate = _record_mutation(
        candidate_state,
        candidate_extension,
        key,
        candidate_wave,
        operation_id,
        fingerprint,
        response,
        event,
        journal_event,
    )
    if candidate is None:
        return _commit_mutation(
            state_dir,
            state,
            extension,
            key,
            wave,
            operation_id,
            fingerprint,
            _error("capacity"),
        )
    committed_state, event_specs = candidate
    journal._commit_transition(state_dir, committed_state, event_specs)
    return response


def execute(root: Path, request: dict, *, authority: dict, principal: str, session_id: str) -> dict:
    """Execute a validated channel operation through the coordination journal."""
    try:
        request, authority, identity = _validate_request(request, authority, principal, session_id)
        for field in _AUTH_FIELDS:
            if request[field] != authority[field]:
                return _error("scope_mismatch")
        fingerprint = _fingerprint(request, authority, identity)
    except (TypeError, ValueError, OverflowError, RecursionError):
        return _error("invalid_request")
    try:
        with journal._locked(root, deadline=time.monotonic() + LOCK_BUDGET_SECONDS) as state_dir:
            result = _execute_locked(state_dir, request, authority, principal, identity, fingerprint)
            return result if _response_fits(result) else _error("response_too_large")
    except journal._LockDeadline:
        return _error("deadline")
    except journal._CoordinationStoreError:
        return _error("state_corrupt")
    except OSError:
        return _error("storage_error")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    return journal._unique_json_object(pairs)


def main(argv: list[str] | None = None) -> int:
    parser = journal._JsonArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--authority-id", required=True)
    parser.add_argument("--authority-epoch", required=True, type=int)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--wave-id", required=True)
    parser.add_argument("--principal", required=True)
    parser.add_argument("--session-id", required=True)
    try:
        args = parser.parse_args(argv)
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError("request too large")
        request = json.loads(
            raw.decode("utf-8"),
            parse_constant=journal._reject_json_constant,
            object_pairs_hook=_unique_object,
        )
        authority = {
            "authority_id": args.authority_id,
            "authority_epoch": args.authority_epoch,
            "task_ref": args.task_ref,
            "wave_id": args.wave_id,
        }
        result = execute(
            args.root,
            request,
            authority=authority,
            principal=args.principal,
            session_id=args.session_id,
        )
    except (journal._UsageError, ValueError, TypeError, RecursionError):
        result = _error("invalid_request")
    except OSError:
        result = _error("storage_error")
    try:
        print(_canonical(result))
    except (TypeError, ValueError, OverflowError, RecursionError):
        print(_canonical(_error("storage_error")))
        return 3
    if result["ok"]:
        return 0
    return 3 if result["error_code"] in {"storage_error", "deadline", "state_corrupt"} else 2


if __name__ == "__main__":
    sys.exit(main())
