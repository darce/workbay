from __future__ import annotations

import json
import logging
from typing import Any

OPEN_HANDOFF_SECTIONS = "findings_open,blockers_open,actions_pending"
REVIEW_READY_STATE_SECTIONS = "tests_recent"
GLOBAL_CONTEXT_SECTIONS = "blockers_open,actions_pending,findings_open,decisions_recent,tests_recent"
REVIEW_READY_TEST_LIMIT = 4

_log = logging.getLogger(__name__)
_protocol_warning_logged = False


def validate_active_task(envelope: Any) -> Any:
    """Round-trip ``envelope['data']['active']`` through workbay_protocol.ActiveTask.

    The orchestrator's read paths consume ``get_handoff_state`` envelopes
    that carry an ``active`` row inside ``data``. This helper enforces
    the cross-repo wire-shape contract on every consumed row without
    requiring callers to import the protocol package directly.

    Returns the validated model on success, or ``None`` when the
    envelope shape is missing data, the protocol package is not
    installed, or validation fails. A failure is logged once per
    process to surface drift without flooding stderr.
    """
    global _protocol_warning_logged
    try:
        from workbay_protocol import ActiveTask
    except ImportError:
        if not _protocol_warning_logged:
            _log.debug("workbay-protocol not installed; skipping orchestrator contract validation.")
            _protocol_warning_logged = True
        return None
    if isinstance(envelope, str):
        try:
            envelope = json.loads(envelope)
        except (ValueError, TypeError):
            return None
    if not isinstance(envelope, dict):
        return None
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    active = data.get("active")
    if not isinstance(active, dict):
        return None
    try:
        return ActiveTask.model_validate(active)
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "orchestrator received handoff active row failing workbay_protocol.ActiveTask: %s",
            exc,
        )
        return None


def active_task_identity_kwargs() -> dict[str, Any]:
    return {"read_profile": "identity"}


def open_handoff_items_kwargs(task_ref: str) -> dict[str, Any]:
    return {
        "task_ref": task_ref,
        "read_profile": "open_items",
    }


def review_ready_state_kwargs(task_ref: str) -> dict[str, Any]:
    return {
        "task_ref": task_ref,
        "sections": REVIEW_READY_STATE_SECTIONS,
        "detail": "summary",
        "top_n_tests": REVIEW_READY_TEST_LIMIT,
    }


def global_context_kwargs(task_ref: str, *, limit: int) -> dict[str, Any]:
    return {
        "task_ref": task_ref,
        "sections": GLOBAL_CONTEXT_SECTIONS,
        "top_n_blockers": limit,
        "top_n_actions": limit,
        "top_n_decisions": limit,
        "top_n_tests": limit,
        "top_n_findings": limit,
    }


def hot_state_metric_kwargs(task_ref: str, *, limits: dict[str, int]) -> dict[str, Any]:
    return {
        "task_ref": task_ref,
        "top_n_blockers": limits["blockers"],
        "top_n_actions": limits["actions"],
        "top_n_decisions": limits["decisions"],
        "top_n_tests": limits["tests"],
        "top_n_findings": limits["findings"],
    }


def read_handoff_state(**kwargs: Any) -> dict[str, Any]:
    """Single chokepoint for orchestrator-side handoff reads.

    Calls ``workbay_handoff_mcp.get_handoff_state(**kwargs)`` and runs
    every consumed envelope through ``validate_active_task`` so the
    cross-server wire-shape contract is enforced at every read site —
    not just at ``_resolve_task_ref``. Validation failures still log a
    warning rather than raising; production paths must continue under
    drift to avoid bricking the daemon. Strict mode lives in tests
    today.

    Returns the envelope unchanged so call sites that already inspect
    ``envelope["data"]["active"]`` continue to work.
    """
    # Lazy import: handoff is a sibling package and runtime callers
    # already import it inline; mirroring that here keeps import
    # ordering and test isolation working.
    from workbay_handoff_mcp import get_handoff_state  # noqa: PLC0415

    envelope = get_handoff_state(**kwargs)
    validate_active_task(envelope)
    return envelope
