"""Trusted local v1 claims sharing the coordination journal and its lock.

This is admission bookkeeping, not authentication or fencing of external effects.
See docs/workbay/contracts/coordclaims-v1.md for the wire and recovery contract.
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

MAX_REQUEST_BYTES = 64 * 1024
MAX_OPERATIONS = 10000
LOCK_BUDGET_SECONDS = 2.0
_AUTH_FIELDS = {"authority_id", "authority_epoch", "task_ref", "wave_id"}
_REQUIRED = _AUTH_FIELDS | {"schema_version", "operation_id", "work_item_id", "operation"}
_OPERATIONS = ("claim", "renew", "release", "complete", "get")
# GRPH27: the complete set of admitted state/operation transitions. Expiry is
# evaluated before admission; owner+token are additional predicates below.
_TRANSITIONS = {
    ("available", "claim"): "claimed",
    ("claimed", "renew"): "claimed",
    ("claimed", "release"): "available",
    ("claimed", "complete"): "completed",
    ("claimed", "expiry"): "available",
}


def _error(code: str, **fields: Any) -> dict:
    return {"schema_version": 1, "ok": False, "error_code": code, **fields}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _validate_request(request: Any, authority: Any, principal: Any, session_id: Any) -> dict:
    if not journal._claim_authority(authority) or not all(journal._claim_text(v) for v in (principal, session_id)):
        raise ValueError("invalid trusted identity")
    if (
        not isinstance(request, dict)
        or not _REQUIRED <= request.keys()
        or request.keys() - (_REQUIRED | {"ttl_seconds", "token"})
    ):
        raise ValueError("invalid fields")
    if type(request["schema_version"]) is not int or request["schema_version"] != 1:
        raise ValueError("invalid version")
    if not journal._claim_authority({k: request[k] for k in _AUTH_FIELDS}):
        raise ValueError("invalid scope")
    if not all(journal._claim_text(request[k]) for k in ("operation_id", "work_item_id")):
        raise ValueError("invalid key")
    operation = request["operation"]
    if operation not in _OPERATIONS:
        raise ValueError("invalid operation")
    if operation in ("renew", "release", "complete"):
        if type(request.get("token")) is not int or request["token"] <= 0:
            raise ValueError("invalid token")
    elif "token" in request:
        raise ValueError("unexpected token")
    if "ttl_seconds" in request:
        ttl = request["ttl_seconds"]
        if operation not in ("claim", "renew") or not journal._is_number(ttl) or not 0 < ttl <= 300:
            raise ValueError("invalid ttl")
    encoded = _canonical(request).encode("utf-8")
    if len(encoded) > MAX_REQUEST_BYTES:
        raise ValueError("request too large")
    # Own the validated snapshot throughout admission, independent of caller edits.
    return cast(dict[str, Any], json.loads(encoded))


def _view(work_item: str, record: dict | None) -> dict:
    return {
        "schema_version": 1,
        "ok": True,
        "work_item_id": work_item,
        **(copy.deepcopy(record) if record else {"state": "available"}),
    }


def execute(root: Path, request: dict, *, authority: dict, principal: str, session_id: str) -> dict:
    """Validate and execute one request using trusted configuration and identity.

    Identical mutation retries return their committed response, even after expiry.
    Storage errors may follow a durable commit: retry the same operation ID.
    """
    try:
        request = _validate_request(request, authority, principal, session_id)
        authority = json.loads(_canonical(authority))
        owner = {"principal": principal, "session_id": session_id}
        fingerprint = hashlib.sha256(
            _canonical({"request": request, "authority": authority, "owner": owner}).encode("utf-8")
        ).hexdigest()
    except (TypeError, ValueError, OverflowError, RecursionError):
        return _error("invalid_request")
    if any(request[k] != authority[k] for k in _AUTH_FIELDS):
        return _error("scope_mismatch")
    try:
        with journal._locked(root, deadline=time.monotonic() + LOCK_BUDGET_SECONDS) as state_dir:
            state = journal._load_state(state_dir)
            extension = state.get("coordclaims", {"schema_version": 1, "waves": {}})
            wave_key = journal._claim_wave_key(authority)
            wave = extension["waves"].get(wave_key)
            if wave is not None and wave["authority"] != authority:
                return _error("scope_mismatch")
            now = time.time()  # Server time is sampled only after lock and recovery.
            if not journal._is_number(now) or (wave is not None and now < wave["last_time"]):
                return _error("state_corrupt", detail="clock_rollback")
            if wave is None:
                wave = {"authority": authority, "last_time": now, "items": {}, "receipts": {}}
            operation_id = request["operation_id"]
            receipt = wave["receipts"].get(operation_id)
            if receipt is not None:
                if receipt["fingerprint"] != fingerprint:
                    return _error("idempotency_conflict")
                return copy.deepcopy(receipt["response"])
            operation, work_item = request["operation"], request["work_item_id"]
            record = wave["items"].get(work_item)
            expired = record is not None and record["state"] == "claimed" and record["expires_at"] <= now
            if operation == "get":
                # No binding, receipt, clock watermark or expiry writes on reads.
                return _view(work_item, None if expired else record)
            if len(wave["receipts"]) >= MAX_OPERATIONS:
                return _error("capacity")
            event_specs: list[tuple[str, dict[str, Any], str] | tuple[str, dict[str, Any], str, str]] = []

            def transition_event(kind: str, item: dict) -> None:
                event_specs.append(
                    (
                        "claim." + kind,
                        {
                            **authority,
                            "work_item_id": work_item,
                            "operation_id": operation_id,
                            "owner": copy.deepcopy(item["owner"]),
                            "token": item["token"],
                        },
                        principal,
                    )
                )

            if expired:
                record["state"] = _TRANSITIONS[("claimed", "expiry")]
                transition_event("expired", record)
            status = record["state"] if record else "available"
            if status == "completed":
                response = _error("completed")
            elif operation == "claim" and status == "claimed":
                response = _error("held_by", held_by=_view(work_item, record))
            elif operation != "claim" and (
                status != "claimed" or record["owner"] != owner or record["token"] != request["token"]
            ):
                response = _error("stale_token")
            else:
                if operation == "claim":
                    record = {"state": "claimed", "owner": owner, "token": state["next_token"], "expires_at": now}
                    state["next_token"] += 1
                    wave["items"][work_item] = record
                record["state"] = _TRANSITIONS[(status, operation)]
                if operation in ("claim", "renew"):
                    record["expires_at"] = now + request.get("ttl_seconds", 60)
                    if not journal._is_number(record["expires_at"]):
                        return _error("state_corrupt", detail="invalid_clock")
                transition_event(
                    {"claim": "granted", "renew": "renewed", "release": "released", "complete": "completed"}[operation],
                    record,
                )
                response = _view(work_item, record)
            wave["last_time"] = now
            wave["receipts"][operation_id] = {"fingerprint": fingerprint, "response": copy.deepcopy(response)}
            extension["waves"][wave_key] = wave
            state["coordclaims"] = extension
            # DATA14: transition, receipt and lineage events share one commit.
            journal._commit_transition(state_dir, state, event_specs)
            return response
    except journal._LockDeadline:
        return _error("deadline")
    except journal._CoordinationStoreError:
        return _error("state_corrupt")
    except OSError:
        return _error("storage_error")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def main(argv: list[str] | None = None) -> int:
    parser = journal._JsonArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    for name in ("authority-id", "task-ref", "wave-id", "principal", "session-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--authority-epoch", required=True, type=int)
    try:
        args = parser.parse_args(argv)
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError("request too large")
        request = json.loads(
            raw.decode("utf-8"), parse_constant=journal._reject_json_constant, object_pairs_hook=_unique_object
        )
        authority = {key: getattr(args, key) for key in _AUTH_FIELDS}
        result = execute(args.root, request, authority=authority, principal=args.principal, session_id=args.session_id)
    except (journal._UsageError, ValueError, TypeError, RecursionError):
        result = _error("invalid_request")
    except OSError:
        result = _error("storage_error")
    print(_canonical(result))
    return 0 if result["ok"] else (2 if result["error_code"] == "invalid_request" else 3)


if __name__ == "__main__":
    sys.exit(main())
