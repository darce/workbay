"""Typed close path for lanes proven stale by orchestration."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from types import SimpleNamespace
from typing import Any

from workbay_handoff_mcp import lane_completion_attention as journal
from workbay_handoff_mcp.enums import LaneStatus

from workbay_orchestrator_mcp.lane_reaping import _close_blocked_lane_cas
from workbay_orchestrator_mcp.lanes_support import (
    CLOSEABLE_LANE_STATUSES,
    TERMINAL_LANE_STATUSES,
    _get_db_connection,
    _workspace_root,
)

from . import lane_completion as lc
from . import lane_completion_gate as gate
from . import lane_completion_receipt as receipts

ABSENT_BRANCH_STALE_MIN_AGE_S = 24 * 60 * 60
ABSENT_BRANCH_LIVE_ABANDONED_MIN_AGE_S = 7 * 24 * 60 * 60
_LIVE_LANE_STATUSES = frozenset({LaneStatus.PLANNED.value, LaneStatus.ACTIVE.value})
_KNOWN_LANE_STATUSES = frozenset(member.value for member in LaneStatus)


class StaleReason(StrEnum):
    """Closed set of orchestration proofs that authorize a stale close."""

    ABSENT_BRANCH_NO_EVIDENCE = "absent_branch_no_evidence"
    ABSENT_BRANCH_LIVE_ABANDONED = "absent_branch_live_abandoned"
    EMPTY_LANE_NEVER_PRODUCED_COMMITS = "empty_lane_never_produced_commits"
    DAEMON_EMPTY_TERMINALIZATION = "daemon_empty_terminalization"


def _timestamp_epoch(value: object) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        stamp = datetime.fromisoformat(raw)
    except (OverflowError, ValueError):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    try:
        return stamp.timestamp()
    except (OverflowError, OSError, ValueError):
        return None


def absent_branch_stale_eligible(
    row: dict[str, Any],
    *,
    now_epoch: float,
    worktree_exists: bool,
    driver_live: bool | None,
    branch_absent: bool | None = None,
    session_live: bool | None = None,
) -> tuple[bool, str]:
    """Require an independent age and liveness proof before absent-branch close.

    Filesystem and process probes are supplied by the caller so this decision
    stays deterministic and side-effect free. Unknown row facts fail closed.
    """
    if not isinstance(row, dict):
        return False, "absent_branch_fresh_or_live:row_unreadable"
    status = str(row.get("status") or "").strip().lower()
    if status in _LIVE_LANE_STATUSES and branch_absent is None and session_live is None:
        return False, f"absent_branch_fresh_or_live:live_status_{status}"
    if status not in _KNOWN_LANE_STATUSES:
        return False, f"absent_branch_fresh_or_live:unknown_status_{status or 'missing'}"
    if status in TERMINAL_LANE_STATUSES:
        return False, f"absent_branch_fresh_or_live:terminal_status_{status}"

    raw_updated_at = row.get("updated_at")
    raw_stamp = raw_updated_at if raw_updated_at else row.get("created_at")
    stamp_epoch = _timestamp_epoch(raw_stamp)
    if stamp_epoch is None:
        return False, "absent_branch_fresh_or_live:timestamp_unparseable"
    if isinstance(now_epoch, bool) or not isinstance(now_epoch, (int, float)) or not math.isfinite(now_epoch):
        return False, "absent_branch_fresh_or_live:clock_unavailable"
    live_abandoned = status in _LIVE_LANE_STATUSES
    min_age = ABSENT_BRANCH_LIVE_ABANDONED_MIN_AGE_S if live_abandoned else ABSENT_BRANCH_STALE_MIN_AGE_S
    if now_epoch - stamp_epoch <= min_age:
        age_label = "604800s" if live_abandoned else "86400s"
        return False, f"absent_branch_fresh_or_live:age_below_{age_label}"

    worktree_path = row.get("worktree_path")
    if not isinstance(worktree_path, str) or not worktree_path.strip():
        return False, "absent_branch_fresh_or_live:worktree_path_missing"
    if worktree_exists:
        return False, "absent_branch_fresh_or_live:worktree_exists"
    if live_abandoned and branch_absent is not True:
        return False, "absent_branch_fresh_or_live:branch_present_or_unknown"
    if driver_live is not False:
        return False, "absent_branch_fresh_or_live:live_driver_lock"
    if live_abandoned and session_live is not False:
        reason = "session_heartbeat_live" if session_live is True else "session_heartbeat_unknown"
        return False, f"absent_branch_fresh_or_live:{reason}"
    if live_abandoned:
        return True, "absent_branch_live_abandoned_eligible"
    return True, "absent_branch_stale_eligible"


def _lookup_lane(conn: Any, task_ref: str, lane_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    return dict(row) if row is not None else None


def _cas_stale_close(
    conn: Any,
    lane: dict[str, Any],
    *,
    task_ref: str,
    lane_id: str,
    reason: StaleReason,
    notes: str | None,
    expected_status: str | None,
    expected_updated_at: str,
    expected_row: Mapping[str, Any],
) -> dict[str, Any]:
    expected = (
        expected_status.strip().lower()
        if isinstance(expected_status, str)
        else str(lane.get("status") or "").strip().lower()
    )
    current_status = str(lane.get("status") or "").strip().lower() or None
    row_changed = any(key in lane and lane[key] != value for key, value in expected_row.items())
    if row_changed or lane.get("updated_at") != expected_updated_at or current_status != expected:
        return _envelope(
            ok=False,
            lane=lane,
            status=current_status,
            error="Lane status or updated_at changed before stale close CAS",
            error_kind="lane_stale_close_conflict",
            stale_reason=reason,
        )

    terminal_result = _terminal_lane_result(lane, reason)
    if terminal_result is not None:
        return terminal_result

    note = (notes or "").strip() or f"Closed stale after {reason.value}"
    note = f"{note} [stale_reason={reason.value}]"
    changed = _close_blocked_lane_cas(
        conn,
        lane_pk=int(lane["id"]),
        probed_updated_at=expected_updated_at,
        note=note,
        expected_status=expected,
    )
    if not changed:
        latest_lane = _lookup_lane(conn, task_ref, lane_id)
        latest_status = str((latest_lane or {}).get("status") or "").strip().lower() or None
        return _envelope(
            ok=False,
            lane=latest_lane,
            status=latest_status,
            error="Lane status or updated_at changed before stale close CAS",
            error_kind="lane_stale_close_conflict",
            stale_reason=reason,
        )

    closed_row = conn.execute("SELECT * FROM worktree_lanes WHERE id = ?", (lane["id"],)).fetchone()
    closed_lane = dict(closed_row) if closed_row is not None else None
    return _envelope(
        ok=True,
        lane=closed_lane,
        status=str((closed_lane or {}).get("status") or LaneStatus.CLOSED_STALE.value).strip().lower(),
        stale_reason=reason,
    )


def _envelope(
    *,
    ok: bool,
    lane: dict[str, Any] | None = None,
    status: str | None = None,
    error: str | None = None,
    error_kind: str | None = None,
    stale_reason: StaleReason | None = None,
) -> dict[str, Any]:
    return {
        "ok": ok,
        "lane": lane,
        "status": status,
        "error": error,
        "error_kind": error_kind,
        "stale_reason": stale_reason.value if stale_reason is not None else None,
    }


def _terminal_lane_result(lane: dict[str, Any], reason: StaleReason) -> dict[str, Any] | None:
    current_status = str(lane.get("status") or "").strip().lower()
    if current_status == LaneStatus.CLOSED_STALE.value:
        return _envelope(ok=True, lane=lane, status=current_status, stale_reason=reason)
    if current_status in CLOSEABLE_LANE_STATUSES:
        return _envelope(
            ok=False,
            lane=lane,
            status=current_status,
            error=f"Refusing stale close of already-terminal lane status {current_status!r}",
            error_kind="lane_already_terminal",
            stale_reason=reason,
        )
    return None


def _completion_fields(
    result: dict[str, Any],
    *,
    verdict: str,
    outcome: receipts.ReceiptOutcome | None = None,
    receipt: Mapping[str, Any] | None = None,
    missing: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        **result,
        "completion_verdict": verdict,
        "completion_outcome": outcome.kind.value if outcome else receipts.ReceiptOutcomeKind.HELD_UNKNOWN.value,
        "completion_event_id": outcome.authorization.event_id if outcome and outcome.authorization else None,
        "unmet_finding_ids": list((receipt or {}).get("unmet_finding_ids", [])),
        "missing_evidence": list(missing),
    }


def _operation_intent(lane: Mapping[str, Any]) -> str:
    # cycle_epoch advances on the close itself; assessment_id already binds
    # the contract's cycle. The operation key must survive that status write.
    return f"stale_close:{lane['id']}:{lane.get('identity_seq')}"


def _repair_closed_warning(lane: dict[str, Any], reason: StaleReason) -> dict[str, Any]:
    """Find this cycle's journaled operation, then recover by observation only.

    The public inbox locates pending events. It omits authorization fields, so
    read those by the returned event ID rather than constructing fresh authority
    or preparing a different assessment after the row's status has changed.
    """
    cursor = None
    matched = []
    for _ in range(10):
        attention = journal.read_completion_attention(lane["task_ref"], limit=100, cursor=cursor)
        for event in attention["events"]:
            if (
                event["lane_row_id"] == lane["id"]
                and event["lane_id"] == lane["lane_id"]
                and event["policy_action"] == "stale_close"
                and event["storage_outcome"] in ("prepared", "storage_removed")
                and event["verdict"] in ("unfinished_partial", "unfinished_never_produced")
            ):
                with _get_db_connection() as conn:
                    stored = conn.execute(
                        "SELECT * FROM lane_completion_events WHERE event_id=?", (event["event_id"],)
                    ).fetchone()
                if stored is not None and stored["operation_intent"] == _operation_intent(lane):
                    matched.append(
                        (
                            event,
                            journal.CompletionAuthorization(
                                stored["event_id"],
                                stored["assessment_id"],
                                stored["obligation_fingerprint"],
                                stored["contract_hash"],
                            ),
                        )
                    )
        cursor = attention["cursor"]
        if cursor is None:
            break
    else:
        raise journal.CompletionRefusal(
            journal.CompletionRefusalCode.RECEIPT_FAILED, "warning repair inbox exceeds read budget"
        )

    def observe_removed() -> bool:
        with _get_db_connection() as conn:
            current = _lookup_lane(conn, lane["task_ref"], lane["lane_id"])
        return current == lane and current["status"] == LaneStatus.CLOSED_STALE.value

    result = _envelope(ok=True, lane=lane, status=lane["status"], stale_reason=reason)
    for event, authorization in matched:
        outcome = receipts.recover_completion_operation(
            authorization,
            actuator=SimpleNamespace(observe_removed=observe_removed),
            next_action=event["next_action"],
        )
        result = _completion_fields(
            _envelope(ok=outcome.storage_removed, lane=lane, status=lane["status"], stale_reason=reason),
            verdict=event["verdict"],
            outcome=outcome,
            receipt=event,
        )
    return result


def close_lane_stale(
    *,
    task_ref: str,
    lane_id: str,
    reason: StaleReason,
    notes: str | None = None,
    expected_status: str | None = None,
    expected_updated_at: str,
    expected_row: Mapping[str, Any],
) -> dict[str, Any]:
    """CAS a proven stale lane to ``closed_stale``.

    This is the orchestration-only stale writer. The operator close surface
    continues to accept only ``closed`` and ``merged``. The CAS binds the write
    to the status and exact ``updated_at`` supplied by the caller's probe so a
    stale probe cannot overwrite a concurrent lane transition. SQLite stores
    ``updated_at`` at one-second resolution, so callers must supply their full
    probed row to compare every probed column while holding an immediate
    transaction; this detects same-second refreshes before the CAS write.
    """
    if not isinstance(reason, StaleReason):
        valid = ", ".join(member.value for member in StaleReason)
        return _envelope(
            ok=False,
            error=f"Invalid stale reason {reason!r}. Valid: {valid}",
            error_kind="stale_reason_invalid",
        )

    normalized_task_ref = task_ref.strip() if isinstance(task_ref, str) else ""
    normalized_lane_id = lane_id.strip() if isinstance(lane_id, str) else ""
    if not normalized_task_ref or not normalized_lane_id:
        return _envelope(
            ok=False,
            error="task_ref and lane_id are required for a stale close",
            error_kind="lane_identity_invalid",
            stale_reason=reason,
        )

    if not isinstance(expected_updated_at, str) or not expected_updated_at.strip():
        return _envelope(
            ok=False,
            error="A non-blank probed updated_at is required for a stale close",
            error_kind="lane_stale_close_unversioned",
            stale_reason=reason,
        )

    if not isinstance(expected_row, Mapping) or not expected_row:
        return _envelope(
            ok=False,
            error="A full probed row is required for a stale close",
            error_kind="lane_stale_close_unversioned",
            stale_reason=reason,
        )

    lane = None
    assessment = None
    try:
        # Assessment and the journal each own read/write connections. Never
        # hold the lanes BEGIN IMMEDIATE while either of them runs.
        with _get_db_connection() as conn:
            lane = _lookup_lane(conn, normalized_task_ref, normalized_lane_id)
        if lane is None:
            return _envelope(
                ok=False,
                error=f"Lane {normalized_task_ref}/{normalized_lane_id} was not found",
                error_kind="lane_not_found",
                stale_reason=reason,
            )
        if lane["status"] == LaneStatus.CLOSED_STALE.value:
            return _repair_closed_warning(lane, reason)
        expected = expected_status.strip().lower() if isinstance(expected_status, str) else lane["status"]
        if (
            any(key in lane and lane[key] != value for key, value in expected_row.items())
            or lane["updated_at"] != expected_updated_at
            or lane["status"] != expected
        ):
            return _envelope(
                ok=False,
                lane=lane,
                status=lane["status"],
                error="Lane status or updated_at changed before stale close CAS",
                error_kind="lane_stale_close_conflict",
                stale_reason=reason,
            )
        terminal = _terminal_lane_result(lane, reason)
        if terminal is not None:
            return terminal
        root = _workspace_root()
        assessment = gate.compose_completion(
            task_ref=normalized_task_ref,
            lane_id=normalized_lane_id,
            lane=lane,
            repo_root=root,
            phase=lc.CompletionPhase.INTEGRATION,
            policy_action="stale_close",
        )
        if assessment.kind == gate.CompletionGateKind.HELD_UNKNOWN:
            return _completion_fields(
                _envelope(
                    ok=False,
                    lane=lane,
                    status=lane["status"],
                    error="Completion evidence unavailable; backfill required",
                    error_kind="completion_unknown",
                    stale_reason=reason,
                ),
                verdict=lc.CompletionVerdict.UNKNOWN.value,
                receipt=assessment.receipt,
                missing=assessment.missing_evidence,
            )
        close_result = None

        def actuator() -> bool:
            nonlocal close_result
            gate.validate_completion_authority(assessment, repo_root=root)
            try:
                with _get_db_connection(begin_immediate=True) as conn:
                    current = _lookup_lane(conn, normalized_task_ref, normalized_lane_id)
                    if current is None:
                        close_result = _envelope(
                            ok=False,
                            error="Lane disappeared before stale close CAS",
                            error_kind="lane_stale_close_conflict",
                            stale_reason=reason,
                        )
                        return False
                    close_result = _cas_stale_close(
                        conn,
                        current,
                        task_ref=normalized_task_ref,
                        lane_id=normalized_lane_id,
                        reason=reason,
                        notes=notes,
                        expected_status=expected_status,
                        expected_updated_at=expected_updated_at,
                        expected_row=expected_row,
                    )
                    removed = close_result["ok"] is True and current["status"] != LaneStatus.CLOSED_STALE.value
            except Exception as exc:  # noqa: BLE001 — preserve the existing typed CAS failure
                close_result = _envelope(
                    ok=False,
                    lane=lane,
                    status=lane["status"],
                    error=str(exc),
                    error_kind="lane_stale_close_failed",
                    stale_reason=reason,
                )
                return False
            # True means the CAS committed by this invocation, not just observed.
            return removed

        outcome = receipts.with_completion_receipt(
            assessment.receipt,
            operation_intent=_operation_intent(lane),
            next_action="Address unmet contract obligations and record fresh completion evidence before task closure",
            observer="orchestrator:stale_close",
            actuator=actuator,
        )
        if close_result is None:
            error_kind = {
                receipts.ReceiptOutcomeKind.HELD_UNKNOWN: "completion_unknown",
                receipts.ReceiptOutcomeKind.HELD_RECEIPT_FAILED: "completion_receipt_failed",
                receipts.ReceiptOutcomeKind.HELD_AUTHORITY_STALE: "completion_authority_stale",
                receipts.ReceiptOutcomeKind.HELD_WITH_WARNING: "completion_authority_stale",
            }.get(outcome.kind)
            close_result = _envelope(
                ok=False,
                lane=lane,
                status=lane["status"],
                error=str(outcome.refusal) if outcome.refusal else "Completion operation held; re-probe lane",
                error_kind=error_kind or "completion_authority_stale",
                stale_reason=reason,
            )
        return _completion_fields(
            close_result,
            verdict=assessment.receipt["verdict"],
            outcome=outcome,
            receipt=assessment.receipt,
            missing=assessment.missing_evidence,
        )
    except journal.CompletionRefusal as exc:
        kind = (
            receipts.ReceiptOutcomeKind.HELD_AUTHORITY_STALE
            if exc.code == journal.CompletionRefusalCode.AUTHORITY_STALE
            else receipts.ReceiptOutcomeKind.HELD_RECEIPT_FAILED
        )
        return _completion_fields(
            _envelope(
                ok=False,
                lane=lane,
                status=(lane or {}).get("status"),
                error=str(exc),
                error_kind=exc.code.value,
                stale_reason=reason,
            ),
            verdict=assessment.receipt["verdict"] if assessment and assessment.receipt else "unknown",
            outcome=receipts.ReceiptOutcome(kind, "Repair the durable completion operation", refusal=exc),
            receipt=assessment.receipt if assessment else None,
        )
    except Exception as exc:  # noqa: BLE001 — keep close failures typed for orchestration callers
        return _completion_fields(
            _envelope(
                ok=False,
                lane=lane,
                status=(lane or {}).get("status"),
                error=str(exc),
                error_kind="completion_unknown",
                stale_reason=reason,
            ),
            verdict="unknown",
            missing=("read_unavailable",),
        )
