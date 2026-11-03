"""Typed close path for lanes proven stale by orchestration."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from workbay_handoff_mcp.enums import LaneStatus

from workbay_orchestrator_mcp.lane_reaping import _close_blocked_lane_cas
from workbay_orchestrator_mcp.lanes_support import CLOSEABLE_LANE_STATUSES, _get_db_connection

ABSENT_BRANCH_STALE_MIN_AGE_S = 24 * 60 * 60
ABSENT_BRANCH_LIVE_ABANDONED_MIN_AGE_S = 7 * 24 * 60 * 60
_LIVE_LANE_STATUSES = frozenset({LaneStatus.PLANNED.value, LaneStatus.ACTIVE.value})
_KNOWN_LANE_STATUSES = frozenset(member.value for member in LaneStatus)
_TERMINAL_LANE_STATUSES = frozenset({LaneStatus.MERGED.value, LaneStatus.CLOSED.value, LaneStatus.CLOSED_STALE.value})


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
    if status in _TERMINAL_LANE_STATUSES or status in CLOSEABLE_LANE_STATUSES:
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

    try:
        with _get_db_connection(begin_immediate=True) as conn:
            lane = _lookup_lane(conn, normalized_task_ref, normalized_lane_id)
            if lane is None:
                return _envelope(
                    ok=False,
                    error=f"Lane {normalized_task_ref}/{normalized_lane_id} was not found",
                    error_kind="lane_not_found",
                    stale_reason=reason,
                )

            return _cas_stale_close(
                conn,
                lane,
                task_ref=normalized_task_ref,
                lane_id=normalized_lane_id,
                reason=reason,
                notes=notes,
                expected_status=expected_status,
                expected_updated_at=expected_updated_at,
                expected_row=expected_row,
            )
    except Exception as exc:  # noqa: BLE001 — keep close failures typed for orchestration callers
        return _envelope(
            ok=False,
            error=str(exc),
            error_kind="lane_stale_close_failed",
            stale_reason=reason,
        )
