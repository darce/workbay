"""Versioned request identity for the native offload pass record."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

_REQUEST_FIELDS = frozenset(
    {
        "lane_id",
        "task_ref",
        "backend",
        "model",
        "reasoning_effort",
        "speed",
        "tier",
        "token_budget",
        "timeout_seconds",
        "max_review_cycles",
        "turn_timeout_seconds",
        "session",
        "session_mode",
        "dry_run",
        "pass_id",
        "grok_max_turns",
        "admission_override",
        "canon_proposal_path",
    }
)
_ACTIVE_CLAIM: ContextVar[tuple[Path, str, str] | None] = ContextVar("offload_queue_claim", default=None)


def normalize_request(run_args: dict, *, task_ref: str) -> dict:
    if run_args.keys() != _REQUEST_FIELDS:
        raise ValueError("invalid offload queue request fields")
    request = {**run_args, "task_ref": task_ref}
    if request["backend"] is not None:
        request["backend"] = request["backend"].strip()
    request["session"] = request["session"] or f"{task_ref}-{request['lane_id']}"
    for key in ("timeout_seconds", "turn_timeout_seconds"):
        if request[key] is not None:
            request[key] = float(request[key])
    return request


def runtime_scope(config: Any) -> dict[str, str]:
    return {
        key: str(Path(getattr(config, key)).resolve())
        for key in ("workspace_root", "git_workspace_root", "state_workspace_root", "state_dir", "db_path")
    }


def completion_inputs(prepared: dict) -> dict:
    """Capture the request inputs before the engine constructs its completion receipt."""
    from .native_validation import select_request_pair

    config, digest = select_request_pair(None, None)
    routing = prepared["routing"]
    return {
        "backend": prepared["resolved_backend"],
        "model": routing["model"] or prepared["lane_row"].get("model"),
        "reasoning_effort": routing["reasoning_effort"],
        "speed": routing["speed"],
        "tier": routing["tier"],
        "test_cmd": prepared["lane_row"].get("test_cmd"),
        "native_validation_config": config,
        "native_validation_digest": digest,
    }


def _canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def make_intent(request: dict, prepared: dict, config: Any) -> dict:
    intent = {
        "version": 1,
        "request": request,
        "runtime": runtime_scope(config),
        "completion_inputs": completion_inputs(prepared),
    }
    return {**intent, "sha256": hashlib.sha256(_canonical(intent)).hexdigest()}


def read_intent(state: dict, pass_id: str) -> dict:
    intent = state["queue_intent"]
    try:
        request = intent["request"]
        normalized = normalize_request(request, task_ref=state["task_ref"])
        body = {key: value for key, value in intent.items() if key != "sha256"}
        if (
            intent["version"] == 1
            and request["pass_id"] == pass_id
            and request["lane_id"] == state["lane_id"]
            and request["dry_run"] is False
            and _canonical(normalized) == _canonical(request)
            and hashlib.sha256(_canonical(body)).hexdigest() == intent["sha256"]
        ):
            return intent
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        pass
    raise ValueError("invalid persisted offload queue intent")


def matches_request(intent: dict, run_args: dict) -> bool:
    accepted = intent["request"]
    task_ref = run_args["task_ref"] or accepted["task_ref"]
    return _canonical(normalize_request(run_args, task_ref=task_ref)) == _canonical(accepted)


def claimable(state: dict) -> bool:
    return (
        "queue_intent" in state
        and bool(state.get("invocation_token"))
        and state.get("status") == "submitted"
        and not state.keys() & {"queue_claim", "job_id", "completion_config", "result", "progress", "administration"}
    )


@contextmanager
def owning_claim(state_dir: Path, pass_id: str, claim: str) -> Iterator[None]:
    token = _ACTIVE_CLAIM.set((state_dir.resolve(), pass_id, claim))
    try:
        yield
    finally:
        _ACTIVE_CLAIM.reset(token)


def owns_claim(state_dir: Path, pass_id: str, claim: str | None) -> bool:
    return bool(claim and _ACTIVE_CLAIM.get() == (state_dir.resolve(), pass_id, claim))
