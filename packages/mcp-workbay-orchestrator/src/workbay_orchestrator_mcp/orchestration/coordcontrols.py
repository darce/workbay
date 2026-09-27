"""Receipt-aware cooperative holds and signal cursors over the coordination journal.

Control resources are namespaced by trusted authority scope. These controls
coordinate cooperative participants; they do not fence raw lane provisioning
keys, git operations, or other external side effects.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any, cast

from . import coordination as journal

MAX_REQUEST_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = journal._COORDCONTROLS_MAX_RESPONSE_BYTES
MAX_EXTENSION_BYTES = journal._COORDCONTROLS_MAX_EXTENSION_BYTES
MAX_HOLDS = journal._COORDCONTROLS_MAX_HOLDS
MAX_OPERATIONS = journal._COORDCONTROLS_MAX_RECEIPTS
MAX_EVENTS = journal._COORDCONTROLS_MAX_EVENTS
# RES-02: never wait indefinitely to enter the shared journal lock.
LOCK_BUDGET_SECONDS = 2.0

_AUTH_FIELDS = {"authority_id", "authority_epoch", "task_ref", "wave_id"}
_BASE_FIELDS = _AUTH_FIELDS | {"schema_version", "operation_id", "operation"}
_IDENTITY_FIELDS = {"root", "principal", "session_id", "owner", "authority"}
_OPS = {"hold.acquire", "hold.release", "hold.inspect", "event.emit", "event.read"}
_SIGNAL_FIELDS = {"name", "generation", "success"}


def _error(code: str, **fields: Any) -> dict[str, Any]:
    return {"schema_version": 1, "ok": False, "error_code": code, **fields}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


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


def _encode_transport(value: Any) -> bytes | None:
    try:
        return (json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")
    except (TypeError, ValueError, OverflowError, RecursionError):
        return None


def _response_fits(response: dict[str, Any]) -> bool:
    encoded = _encode_transport(response)
    return encoded is not None and len(encoded) <= MAX_RESPONSE_BYTES


def canonical_resource(authority: dict[str, Any], resource: str) -> str:
    """Return an injective, stable control key for trusted scope and resource.

    The encoded JSON tuple contains all authority fields and the resource, so
    separator characters in any component cannot produce a namespace clash.
    Principal, session, time, and operation identity are intentionally absent.
    """
    if (
        not journal._claim_authority(authority)
        or not all(_text(authority[key]) for key in ("authority_id", "task_ref", "wave_id"))
        or not _text(resource)
    ):
        raise ValueError("invalid control resource scope")
    return journal._coordcontrols_canonical_resource(authority, resource)


def _legacy_owner(owner: dict[str, str]) -> str:
    return journal._coordcontrols_legacy_owner(owner)


def _wave_key(authority: dict[str, Any]) -> str:
    return journal._claim_wave_key(authority)


def _valid_match(value: Any) -> bool:
    if not isinstance(value, dict) or value.keys() - _SIGNAL_FIELDS:
        return False
    for key, item in value.items():
        if key == "name" and not _text(item):
            return False
        if key == "generation" and type(item) is not int:
            return False
        if key == "success" and type(item) is not bool:
            return False
    return True


def _valid_signal(fields: Any) -> bool:
    return (
        isinstance(fields, dict)
        and set(fields) == _SIGNAL_FIELDS
        and _text(fields["name"])
        and type(fields["generation"]) is int
        and type(fields["success"]) is bool
    )


def _valid_until(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"kind", "match", "publisher"}
        and value["kind"] == "coord.signal"
        and _valid_match(value["match"])
        and _text(value["publisher"])
    )


def _validate_request(
    request: Any, authority: Any, principal: Any, session_id: Any
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    if (
        not journal._claim_authority(authority)
        or not all(_text(authority[key]) for key in ("authority_id", "task_ref", "wave_id"))
        or not _text(principal)
        or not _text(session_id)
    ):
        raise ValueError("invalid trusted identity")
    if (
        not isinstance(request, dict)
        or not _BASE_FIELDS <= request.keys()
        or request.keys() & _IDENTITY_FIELDS
        or any(not isinstance(key, str) for key in request)
    ):
        raise ValueError("invalid request fields")
    if type(request["schema_version"]) is not int or request["schema_version"] != 1:
        raise ValueError("invalid schema version")
    if not _text(request["operation_id"]):
        raise ValueError("invalid operation id")
    operation = request["operation"]
    if not isinstance(operation, str) or operation not in _OPS:
        raise ValueError("invalid operation")
    operation_fields = {
        "hold.acquire": {"resource", "ttl_seconds"},
        "hold.release": {"resource", "token"},
        "hold.inspect": {"resource"},
        "event.emit": {"kind", "fields"},
        "event.read": {"after_cursor", "limit"},
    }[operation]
    optional_fields = {
        "hold.acquire": {"until"},
        "hold.release": set(),
        "hold.inspect": set(),
        "event.emit": set(),
        "event.read": {"kind", "match", "publisher"},
    }[operation]
    if request.keys() != _BASE_FIELDS | operation_fields | (request.keys() & optional_fields):
        raise ValueError("unexpected operation fields")
    if not _json_value(request):
        raise ValueError("request is not strict JSON")
    try:
        encoded = (_canonical(request) + "\n").encode("ascii")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("request is not encodable") from exc
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ValueError("request too large")

    snapshot = cast(
        dict[str, Any],
        json.loads(_canonical(request), parse_constant=journal._reject_json_constant),
    )
    trusted_authority = cast(
        dict[str, Any],
        json.loads(_canonical(authority), parse_constant=journal._reject_json_constant),
    )
    if not journal._claim_authority({key: snapshot[key] for key in _AUTH_FIELDS}):
        raise ValueError("invalid request authority")
    if not all(_text(snapshot[key]) for key in ("authority_id", "task_ref", "wave_id")):
        raise ValueError("invalid request authority text")

    if operation in {"hold.acquire", "hold.release", "hold.inspect"}:
        if not _text(snapshot["resource"]):
            raise ValueError("invalid resource")
    if operation == "hold.acquire":
        ttl = snapshot["ttl_seconds"]
        if not journal._is_number(ttl) or not 0 < ttl <= 300:
            raise ValueError("invalid ttl")
        if "until" in snapshot and not _valid_until(snapshot["until"]):
            raise ValueError("invalid until predicate")
    elif operation == "hold.release":
        if type(snapshot["token"]) is not int or snapshot["token"] <= 0:
            raise ValueError("invalid token")
    elif operation == "event.emit":
        if snapshot["kind"] != "coord.signal" or not _valid_signal(snapshot["fields"]):
            raise ValueError("invalid signal event")
    elif operation == "event.read":
        if type(snapshot["after_cursor"]) is not int or snapshot["after_cursor"] < 0:
            raise ValueError("invalid event cursor")
        if type(snapshot["limit"]) is not int or not 1 <= snapshot["limit"] <= 100:
            raise ValueError("invalid event limit")
        if "kind" in snapshot and snapshot["kind"] != "coord.signal":
            raise ValueError("invalid event kind filter")
        if "match" in snapshot and not _valid_match(snapshot["match"]):
            raise ValueError("invalid event match filter")
        if "publisher" in snapshot and not _text(snapshot["publisher"]):
            raise ValueError("invalid publisher filter")

    owner = {"principal": principal, "session_id": session_id}
    owner_snapshot = cast(dict[str, str], json.loads(_canonical(owner)))
    return snapshot, trusted_authority, owner_snapshot


def _default_extension() -> dict[str, Any]:
    return {"schema_version": 1, "last_time": 0.0, "waves": {}}


def _default_wave(authority: dict[str, Any]) -> dict[str, Any]:
    return {"authority": copy.deepcopy(authority), "holds": [], "receipts": {}, "events": [], "highwater": 0}


def _extension_size(extension: dict[str, Any]) -> int:
    try:
        return len(_canonical_unicode(extension).encode("utf-8"))
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
        return MAX_EXTENSION_BYTES + 1


def _canonical_unicode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _counts(extension: dict[str, Any]) -> tuple[int, int, int]:
    waves = extension["waves"].values()
    holds = sum(len(wave["holds"]) for wave in waves)
    receipts = sum(len(wave["receipts"]) for wave in waves)
    events = sum(len(wave["events"]) for wave in waves)
    return holds, receipts, events


def _hold_view(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "resource": record["resource"],
        "mapped_resource": record["mapped_resource"],
        "state": "held",
        "token": record["token"],
        "owner": copy.deepcopy(record["owner"]),
        "expires_at": record["expires_at"],
        "until": copy.deepcopy(record["until"]),
    }


def _available_view(resource: str, mapped_resource: str) -> dict[str, Any]:
    return {"resource": resource, "mapped_resource": mapped_resource, "state": "available"}


def _event_view(event_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "cursor": event_record["cursor"],
        "kind": event_record["kind"],
        "fields": copy.deepcopy(event_record["fields"]),
        "publisher": event_record["publisher"],
        "session_id": event_record["session_id"],
    }


def _find_control_hold(wave: dict[str, Any], token: int) -> dict[str, Any] | None:
    return next((record for record in wave["holds"] if record["token"] == token), None)


def _base_hold_by_token(state: dict[str, Any], token: int) -> dict[str, Any] | None:
    return next((record for record in state["holds"] if record["token"] == token), None)


def _active_record(state: dict[str, Any], mapped_resource: str, now: float) -> dict[str, Any] | None:
    return journal._active_record(state, mapped_resource, now)


def _active_view(
    state: dict[str, Any], wave: dict[str, Any], resource: str, mapped_resource: str, now: float
) -> dict[str, Any]:
    base_record = _active_record(state, mapped_resource, now)
    if base_record is None:
        return _available_view(resource, mapped_resource)
    control_record = _find_control_hold(wave, base_record["token"])
    if control_record is not None:
        return _hold_view(control_record)
    # A legacy writer can use this namespace directly. Keep its owner private
    # while reporting the contention; only control-owned metadata is exposed.
    return {
        "resource": resource,
        "mapped_resource": mapped_resource,
        "state": "held",
        "token": base_record["token"],
        "owner": {"principal": "coordination", "session_id": "legacy"},
        "expires_at": base_record["expires_at"],
        "until": None,
    }


def _event_matches(event_record: dict[str, Any], request: dict[str, Any]) -> bool:
    if "kind" in request and event_record["kind"] != request["kind"]:
        return False
    if "publisher" in request and event_record["publisher"] != request["publisher"]:
        return False
    return all(event_record["fields"].get(key) == value for key, value in request.get("match", {}).items())


def _event_read(wave: dict[str, Any] | None, request: dict[str, Any]) -> dict[str, Any]:
    highwater = 0 if wave is None else wave["highwater"]
    after_cursor = request["after_cursor"]
    if after_cursor > highwater:
        return _error("invalid_cursor")
    events = [] if wave is None else wave["events"]
    matching = [
        event_record
        for event_record in events
        if event_record["cursor"] > after_cursor and _event_matches(event_record, request)
    ]
    maximum = min(request["limit"], len(matching))
    for count in range(maximum, -1, -1):
        selected = matching[:count]
        has_more = len(matching) > count
        if has_more and not selected:
            return _error("storage_error")
        next_cursor = selected[-1]["cursor"] if has_more and selected else highwater
        response = {
            "schema_version": 1,
            "ok": True,
            "events": [_event_view(event_record) for event_record in selected],
            "next_cursor": next_cursor,
            "has_more": has_more,
        }
        if _response_fits(response):
            return response
    return _error("storage_error")


def _append_hold_event_specs(
    state: dict[str, Any],
    wave: dict[str, Any],
    now: float,
) -> list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]]:
    """Expire this bounded wave and sync legacy changes without nested locks."""
    event_specs: list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]] = []
    for control_record in wave["holds"]:
        base_record = _base_hold_by_token(state, control_record["token"])
        if base_record is None:
            # The whole-state validator will reject this before mutation.
            raise journal._CoordinationStoreError("coordcontrols_state_invalid")
        if control_record["state"] != "held":
            continue
        if base_record.get("released", False):
            control_record["state"] = "expired" if control_record["expires_at"] <= now else "released"
            continue
        if control_record["expires_at"] > now:
            continue
        base_record["released"] = True
        base_record["release_reason"] = "expired"
        control_record["state"] = "expired"
        event_specs.append(
            (
                "hold.expired",
                {
                    "resource": control_record["mapped_resource"],
                    "owner": _legacy_owner(control_record["owner"]),
                    "token": control_record["token"],
                },
                journal._SYSTEM_ACTOR,
            )
        )
    return event_specs


def _ensure_wave(extension: dict[str, Any], key: str, authority: dict[str, Any]) -> dict[str, Any]:
    wave = extension["waves"].get(key)
    if wave is None:
        wave = _default_wave(authority)
        extension["waves"][key] = wave
    return wave


def _candidate_fits(state: dict[str, Any], response: dict[str, Any]) -> bool:
    extension = state.get("coordcontrols")
    if not isinstance(extension, dict) or not _response_fits(response):
        return False
    holds, receipts, events = _counts(extension)
    # RES-05 / RES-14: persisted control state and every response stay bounded;
    # callers receive a capacity refusal instead of growing unbounded queues.
    return (
        holds <= MAX_HOLDS
        and receipts <= MAX_OPERATIONS
        and events <= MAX_EVENTS
        and _extension_size(extension) <= MAX_EXTENSION_BYTES
    )


def _commit_mutation(
    state_dir: Path,
    base_state: dict[str, Any],
    authority: dict[str, Any],
    wave_key: str,
    operation_id: str,
    fingerprint: str,
    now: float,
    apply: Any,
) -> dict[str, Any]:
    """Apply one bounded mutation and commit it with receipt and journal events."""
    original = copy.deepcopy(base_state)

    def prepare() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[Any]]:
        state = copy.deepcopy(original)
        extension = copy.deepcopy(state.get("coordcontrols", _default_extension()))
        extension["last_time"] = now
        wave = _ensure_wave(extension, wave_key, authority)
        event_specs = _append_hold_event_specs(state, wave, now)
        return state, extension, wave, event_specs

    state, extension, wave, event_specs = prepare()
    response, operation_events = apply(state, extension, wave)
    event_specs.extend(operation_events)
    wave["receipts"][operation_id] = {"fingerprint": fingerprint, "response": copy.deepcopy(response)}
    state["coordcontrols"] = extension
    if _candidate_fits(state, response):
        # DATA-13 / DATA-14: the receipt and resulting projection share the
        # coordination journal commit, so a retry returns the stored outcome.
        journal._commit_transition(state_dir, state, event_specs)
        return response

    # The requested transition did not fit. Rebuild a capacity refusal from
    # the original snapshot so a partial acquisition or event cannot escape.
    fallback_state, fallback_extension, fallback_wave, fallback_events = prepare()
    response = _error("capacity")
    fallback_wave["receipts"][operation_id] = {"fingerprint": fingerprint, "response": copy.deepcopy(response)}
    fallback_state["coordcontrols"] = fallback_extension
    if _candidate_fits(fallback_state, response):
        journal._commit_transition(state_dir, fallback_state, fallback_events)
    return response


def _mutation_apply(
    request: dict[str, Any],
    authority: dict[str, Any],
    owner: dict[str, str],
    operation: str,
):
    def apply(
        state: dict[str, Any],
        extension: dict[str, Any],
        wave: dict[str, Any],
    ) -> tuple[dict[str, Any], list[Any]]:
        more_events: list[Any] = []
        if operation == "hold.acquire":
            resource = request["resource"]
            mapped = canonical_resource(authority, resource)
            active = _active_record(state, mapped, extension["last_time"])
            if active is not None:
                control_record = _find_control_hold(wave, active["token"])
                held_by = (
                    _hold_view(control_record)
                    if control_record is not None
                    else {
                        "resource": resource,
                        "mapped_resource": mapped,
                        "state": "held",
                        "token": active["token"],
                        "owner": {"principal": "coordination", "session_id": "legacy"},
                        "expires_at": active["expires_at"],
                        "until": None,
                    }
                )
                return _error("held_by", held_by=held_by), more_events
            holds, _receipts, _events = _counts(extension)
            if holds >= MAX_HOLDS:
                return _error("capacity"), more_events
            ttl = float(request["ttl_seconds"])
            expires_at = extension["last_time"] + ttl
            if not journal._is_number(expires_at) or expires_at <= extension["last_time"]:
                return _error("state_corrupt"), more_events
            token = state["next_token"]
            state["next_token"] = token + 1
            stored_until = copy.deepcopy(request.get("until"))
            legacy_owner = _legacy_owner(owner)
            base_record = {
                "resource": mapped,
                "owner": legacy_owner,
                "token": token,
                "expires_at": expires_at,
                "until_event": None,
                "match": None,
                "released": False,
            }
            control_record = {
                "resource": resource,
                "mapped_resource": mapped,
                "owner": copy.deepcopy(owner),
                "token": token,
                "expires_at": expires_at,
                "state": "held",
                "until": stored_until,
            }
            state["holds"].append(base_record)
            wave["holds"].append(control_record)
            more_events.append(
                (
                    "hold.granted",
                    {"resource": mapped, "owner": legacy_owner, "token": token},
                    legacy_owner,
                )
            )
            response = {"schema_version": 1, "ok": True, "hold": _hold_view(control_record)}
            return response, more_events

        if operation == "hold.release":
            resource = request["resource"]
            mapped = canonical_resource(authority, resource)
            token = request["token"]
            control_record = next(
                (
                    record
                    for record in wave["holds"]
                    if record["mapped_resource"] == mapped and record["token"] == token
                ),
                None,
            )
            release_base = _base_hold_by_token(state, token)
            if control_record is None or release_base is None:
                active = _active_record(state, mapped, extension["last_time"])
                if active is not None and active["token"] != token:
                    return _error("stale_token"), more_events
                return _error("not_held"), more_events
            if control_record["state"] != "held" or release_base.get("released", False):
                return _error("stale_token"), more_events
            if _active_record(state, mapped, extension["last_time"]) is None:
                return _error("stale_token"), more_events
            current = _active_record(state, mapped, extension["last_time"])
            if current is None or current["token"] != token:
                return _error("stale_token"), more_events
            if control_record["owner"] != owner:
                return _error("owner_mismatch"), more_events
            release_base["released"] = True
            release_base["release_reason"] = "explicit"
            control_record["state"] = "released"
            legacy_owner = _legacy_owner(owner)
            more_events.append(
                (
                    "hold.released",
                    {"resource": mapped, "owner": legacy_owner, "token": token, "reason": "explicit"},
                    legacy_owner,
                )
            )
            return {
                "schema_version": 1,
                "ok": True,
                "resource": resource,
                "token": token,
                "state": "released",
            }, more_events

        if operation == "event.emit":
            _holds, _receipts, events = _counts(extension)
            if events >= MAX_EVENTS:
                return _error("capacity"), more_events
            cursor = wave["highwater"] + 1
            event_id = uuid.uuid4().hex
            event_record = {
                "cursor": cursor,
                "event_id": event_id,
                "kind": "coord.signal",
                "fields": copy.deepcopy(request["fields"]),
                "publisher": owner["principal"],
                "session_id": owner["session_id"],
            }
            wave["events"].append(event_record)
            wave["highwater"] = cursor
            # DATA-15: the signal has a stable logical identity before release
            # lineage is appended to the same journal transition.
            more_events.append(("coord.signal", copy.deepcopy(request["fields"]), owner["principal"], event_id))
            for control_record in wave["holds"]:
                if control_record["state"] != "held":
                    continue
                if control_record["expires_at"] <= extension["last_time"]:
                    continue
                until = control_record["until"]
                if until is None or until["kind"] != "coord.signal" or until["publisher"] != owner["principal"]:
                    continue
                if not all(request["fields"].get(key) == value for key, value in until["match"].items()):
                    continue
                signal_base = _base_hold_by_token(state, control_record["token"])
                if signal_base is None or signal_base.get("released", False):
                    continue
                control_record["state"] = "released"
                signal_base["released"] = True
                reason = f"until_event:{event_id}:{cursor}"
                signal_base["release_reason"] = reason
                legacy_owner = _legacy_owner(control_record["owner"])
                more_events.append(
                    (
                        "hold.released",
                        {
                            "resource": control_record["mapped_resource"],
                            "owner": legacy_owner,
                            "token": control_record["token"],
                            "reason": reason,
                        },
                        journal._SYSTEM_ACTOR,
                    )
                )
            return {"schema_version": 1, "ok": True, "event": _event_view(event_record)}, more_events

        raise AssertionError(f"unexpected mutation operation: {operation}")

    return apply


def execute(root: Path, request: dict, *, authority: dict, principal: str, session_id: str) -> dict:
    """Execute one strict control request using the shared coordination journal.

    Exact mutation retries return their durable prior response. A storage error
    can follow a journal commit, so callers should retry the same operation ID.
    """
    try:
        snapshot, trusted_authority, owner = _validate_request(request, authority, principal, session_id)
        fingerprint = hashlib.sha256(
            _canonical(
                {
                    "request": snapshot,
                    "authority": trusted_authority,
                    "owner": owner,
                }
            ).encode("ascii")
        ).hexdigest()
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
        return _error("invalid_request")

    if any(snapshot[key] != trusted_authority[key] for key in _AUTH_FIELDS):
        return _error("scope_mismatch")

    operation = snapshot["operation"]
    key = _wave_key(trusted_authority)
    resource = snapshot.get("resource")
    mapped = canonical_resource(trusted_authority, resource) if isinstance(resource, str) else None
    try:
        with journal._locked(root, deadline=time.monotonic() + LOCK_BUDGET_SECONDS) as state_dir:
            state = journal._load_state(state_dir)
            extension = state.get("coordcontrols", _default_extension())
            wave = extension["waves"].get(key)
            if wave is not None and wave["authority"] != trusted_authority:
                return _error("scope_mismatch")

            if operation == "event.read":
                return _event_read(wave, snapshot)

            if operation == "hold.inspect":
                now = time.time()
                if not journal._is_number(now) or now < extension["last_time"]:
                    return _error("state_corrupt")
                empty_wave = _default_wave(trusted_authority) if wave is None else wave
                hold = _active_view(state, empty_wave, snapshot["resource"], cast(str, mapped), now)
                response = {"schema_version": 1, "ok": True, "hold": hold}
                return response if _response_fits(response) else _error("storage_error")

            if wave is not None:
                receipt = wave["receipts"].get(snapshot["operation_id"])
                if receipt is not None:
                    if receipt["fingerprint"] != fingerprint:
                        return _error("idempotency_conflict")
                    return copy.deepcopy(receipt["response"])

            _holds, receipt_count, _events = _counts(extension)
            if receipt_count >= MAX_OPERATIONS:
                return _error("capacity")
            now = time.time()  # Sample only after shared lock acquisition and recovery.
            if not journal._is_number(now) or now < extension["last_time"]:
                return _error("state_corrupt")

            response = _commit_mutation(
                state_dir,
                state,
                trusted_authority,
                key,
                snapshot["operation_id"],
                fingerprint,
                now,
                _mutation_apply(snapshot, trusted_authority, owner, operation),
            )
            return response
    except journal._LockDeadline:
        return _error("deadline")
    except journal._CoordinationStoreError:
        return _error("state_corrupt")
    except OSError:
        return _error("storage_error")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = journal._JsonArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    for name in ("authority-id", "task-ref", "wave-id", "principal", "session-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--authority-epoch", required=True, type=int)
    return parser


def _exit_code(response: dict[str, Any]) -> int:
    if response.get("ok") is True:
        return 0
    if response.get("error_code") in {"state_corrupt", "storage_error", "deadline"}:
        return 3
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
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
        response = execute(
            args.root,
            request,
            authority=authority,
            principal=args.principal,
            session_id=args.session_id,
        )
    except (journal._UsageError, ValueError, TypeError, RecursionError, UnicodeError):
        response = _error("invalid_request")
    except OSError:
        response = _error("storage_error")
    encoded = _encode_transport({key: value for key, value in response.items()})
    if encoded is None or len(encoded) > MAX_RESPONSE_BYTES:
        response = _error("storage_error")
        encoded = _encode_transport(response)
    sys.stdout.buffer.write(cast(bytes, encoded))
    return _exit_code(response)


if __name__ == "__main__":
    sys.exit(main())
