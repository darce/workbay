"""Durable single-spend environment retries on a caller-owned SQLite connection.

The orchestrator owns this additive table; no handoff migration is needed.
The caller supplies a WAL connection with sqlite3.Row rows and commits or rolls
back the claim/reset with its other state changes. This module never commits,
closes, or opens a connection and does not dispatch work.

A window begins with its first grant and remains spent through 24 hours later.
The INSERT selects the existing active window or starts a new one *within the
write statement*. Its UNIQUE key arbitrates concurrent spends, without a
read-then-write budget check. Historical windows remain until the lane resets.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from uuid import uuid4

_ABSOLUTE_PATH = re.compile(
    r""""(?:/|[a-z]:[\\/]|\\\\)[^"]*"|'(?:/|[a-z]:[\\/]|\\\\)[^']*'|(?<!\w)(?:[a-z]:[\\/]|\\\\|/)[^\s"'`:,;!?()\[\]{}]*"""
)


def normalize_environment_cause(text: str) -> str:
    """Remove volatile paths/numbers and fold case and whitespace in a cause."""
    text = _ABSOLUTE_PATH.sub("<path>", text.casefold())
    text = re.sub(r"\d+", "<number>", text)
    return " ".join(text.split())


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS environment_retry_records (
            task_ref TEXT NOT NULL,
            lane_id TEXT NOT NULL,
            cause TEXT NOT NULL,
            window_start TEXT NOT NULL,
            request_id TEXT NOT NULL,
            dispatch_id TEXT NOT NULL,
            UNIQUE (task_ref, lane_id, cause, window_start)
        )
        """
    )


def claim_environment_retry(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
    cause: str,
    request_id: str,
    now: datetime | None = None,
) -> dict:
    """Stage one grant, replay its request, or return a spent-budget refusal.

    ``now`` is a timezone-aware UTC datetime. A refusal and a replay preserve
    the original window start; neither extends the budget's lifetime.
    """
    current = now if now is not None else datetime.now(timezone.utc)
    window_start = current.isoformat(timespec="microseconds")
    cutoff = (current - timedelta(hours=24)).isoformat(timespec="microseconds")
    cause = normalize_environment_cause(cause)
    _ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO environment_retry_records (
            task_ref, lane_id, cause, window_start, request_id, dispatch_id
        ) VALUES (
            ?, ?, ?, COALESCE((
                SELECT window_start FROM environment_retry_records
                WHERE task_ref = ? AND lane_id = ? AND cause = ?
                    AND window_start >= ?
                ORDER BY window_start DESC LIMIT 1
            ), ?), ?, ?
        )
        ON CONFLICT (task_ref, lane_id, cause, window_start) DO NOTHING
        """,
        (task_ref, lane_id, cause, task_ref, lane_id, cause, cutoff, window_start, request_id, uuid4().hex),
    )
    row = conn.execute(
        """
        SELECT dispatch_id FROM environment_retry_records
        WHERE task_ref = ? AND lane_id = ? AND cause = ?
            AND window_start >= ? AND request_id = ?
        ORDER BY window_start DESC LIMIT 1
        """,
        (task_ref, lane_id, cause, cutoff, request_id),
    ).fetchone()
    if row is not None:
        return {"granted": True, "reason": "granted", "dispatch_id": row["dispatch_id"], "attempt": 1}
    return {"granted": False, "reason": "environment_retry_exhausted", "dispatch_id": None, "attempt": 1}


def reset_environment_retry(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
    now: datetime | None = None,
) -> int:
    """Stage a lane reset after a non-environment outcome; count cause budgets.

    Reset covers all windows regardless of ``now``, which is accepted for the
    same caller interface as claim. Other tasks and lanes retain their budgets.
    """
    _ensure_schema(conn)
    rows = conn.execute(
        """
        DELETE FROM environment_retry_records
        WHERE task_ref = ? AND lane_id = ?
        RETURNING cause
        """,
        (task_ref, lane_id),
    ).fetchall()
    return len({row["cause"] for row in rows})
