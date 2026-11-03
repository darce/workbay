"""Periodic drain for obsolete blockers on aged terminal lanes."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from .lanes_support import (
    _LANE_STATUS_CLOSED_STALE,
    CLOSEABLE_LANE_STATUSES,
    _get_db_connection,
    _normalize_optional_text,
)

_logger = logging.getLogger(__name__)
_Payload = TypeVar("_Payload")


def reap_terminal_lane_blockers(
    *,
    apply: bool = False,
    task_ref: str | None = None,
    now: datetime | None = None,
    min_age_hours: float = 24.0,
    limit: int = 500,
) -> dict[str, object]:
    """Resolve old terminal-lane blockers, preserving deliberate producer reports.

    Dry-run by default and never raises. Each write rechecks terminal status so
    a worker that revives a lane cannot lose its open blocker to this drain.
    """
    resolved: list[dict[str, object]] = []
    would_resolve: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    raced: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    payload: dict[str, object] = {
        "ok": True,
        "applied": apply,
        "scanned": 0,
        "resolved": resolved,
        "would_resolve": would_resolve,
        "skipped": skipped,
        "raced": raced,
        "failed": failed,
        "truncated": False,
        "remaining": 0,
    }
    try:
        clock = now if now is not None else datetime.now(timezone.utc)
        if clock.tzinfo is None:
            clock = clock.replace(tzinfo=timezone.utc)
        else:
            clock = clock.astimezone(timezone.utc)
        cutoff_sqlite = (clock - timedelta(hours=min_age_hours)).strftime("%Y-%m-%d %H:%M:%S")
        task_ref = _normalize_optional_text(task_ref)
        terminal_statuses = tuple(sorted(CLOSEABLE_LANE_STATUSES | {_LANE_STATUS_CLOSED_STALE}))
        placeholders = ", ".join("?" for _ in terminal_statuses)
        scope_sql = " AND b.task_ref = ?" if task_ref is not None else ""
        params = (*terminal_statuses, cutoff_sqlite, *((task_ref,) if task_ref is not None else ()))
        batch_limit = max(0, limit)
        with _get_db_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT b.id, b.task_ref, b.lane_id, l.status AS lane_status, b.description,
                       COALESCE(datetime(b.created_at) <=
                           datetime(COALESCE(l.updated_at, l.created_at), '+1 hour'), 0) AS filed_in_time
                FROM blockers b
                JOIN worktree_lanes l ON l.task_ref = b.task_ref AND l.lane_id = b.lane_id
                WHERE b.status = 'open' AND TRIM(COALESCE(b.lane_id, '')) != ''
                  AND l.status IN ({placeholders})
                  AND datetime(COALESCE(l.updated_at, l.created_at)) <= datetime(?)
                  {scope_sql}
                ORDER BY b.id
                """,
                params,
            ).fetchall()
            payload["scanned"] = len(rows)
            eligible: list[dict[str, object]] = []
            for row in rows:
                entry = {key: row[key] for key in ("id", "task_ref", "lane_id", "lane_status")}
                description = row["description"]
                try:
                    event = json.loads(description)
                except (ValueError, TypeError):
                    event = None
                reason = None
                if isinstance(event, dict) and "kind" in event:
                    reason = "typed_event_blocker"
                elif description.startswith("reaping_claim_unavailable:"):
                    reason = "remediation_required"
                elif not row["filed_in_time"]:
                    reason = "filed_after_lane_update"
                if reason is not None:
                    skipped.append({**entry, "reason": reason})
                else:
                    eligible.append(entry)
            remaining = max(0, len(eligible) - batch_limit)
            payload["truncated"] = remaining > 0
            payload["remaining"] = remaining
            for entry in eligible[:batch_limit]:
                if not apply:
                    would_resolve.append(entry)
                    continue
                try:
                    cursor = conn.execute(
                        f"""
                        UPDATE blockers SET status = 'resolved', resolved_at = datetime('now')
                        WHERE id = ? AND status = 'open'
                          AND EXISTS (SELECT 1 FROM worktree_lanes l
                                      WHERE l.task_ref = blockers.task_ref AND l.lane_id = blockers.lane_id
                                        AND l.status IN ({placeholders}))
                        """,
                        (entry["id"], *terminal_statuses),
                    )
                except sqlite3.Error as exc:
                    failed.append({**entry, "error": str(exc)})
                    payload.update(ok=False, error=str(exc), error_type=type(exc).__name__)
                    continue
                if cursor.rowcount == 0:
                    raced.append(entry)
                else:
                    resolved.append(entry)
        if resolved:
            _logger.info(
                "Resolved %d terminal lane blockers (first_id=%s, last_id=%s)",
                len(resolved),
                resolved[0]["id"],
                resolved[-1]["id"],
            )
    except Exception as exc:  # noqa: BLE001 — never-raise maintenance drain
        if isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc):
            payload["unavailable"] = "table_missing"
        else:
            payload.update(ok=False, error=str(exc), error_type=type(exc).__name__)
    return payload


def with_terminal_lane_blocker_drain(
    payload: _Payload,
    *,
    apply: bool,
    task_ref: str | None = None,
    now: datetime | None = None,
) -> _Payload:
    """Attach the drain result and surface any degradation to CLI callers."""
    if not isinstance(payload, dict):
        return payload
    try:
        arm = reap_terminal_lane_blockers(apply=apply, task_ref=task_ref, now=now)
    except Exception as exc:  # noqa: BLE001 — per-arm degrade
        arm = {"ok": False, "applied": apply, "error": str(exc), "error_type": type(exc).__name__}
    payload["terminal_lane_blockers"] = arm
    if arm.get("ok") is False:
        payload["ok"] = False
        payload["degraded"] = True
        if not payload.get("error"):
            payload["error"] = arm.get("error") or "terminal lane blocker drain failed"
            payload["error_type"] = arm.get("error_type") or "TerminalLaneBlockerDrainError"
    return payload
