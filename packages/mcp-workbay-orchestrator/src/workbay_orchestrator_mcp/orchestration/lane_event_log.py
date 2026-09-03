"""Append-only lane event log: durable sqlite row transitions, not a broker.

This module is the event-driven primitive for lane dispatch and reaping. A
sibling lane wires it into the daemon; this file owns only the sqlite contract
so that producer and consumer can be parallelised against a documented API.

Contract
--------
- Not a broker and not a worker process. It is a durable sqlite row
  transition that a caller performs on a connection it already holds.
- Transactional outbox: a producer appends an immutable event row on the
  same connection (and therefore the same transaction) as its state change.
  This module never commits or closes the connection.
- Log-based consumption (DDIA ch.11): a consumer reads rows with
  ``id > offset`` and advances the offset with :func:`ack_lane_events` in
  one statement. Retry is "do not ack" (or reset the offset). Delivery is
  at-least-once; the receiver is idempotent because
  ``UNIQUE(task_ref, lane_id, dispatch_id, kind)`` makes a re-delivered
  append a no-op that returns the original id.
- Schema is created with ``CREATE TABLE IF NOT EXISTS`` on the connection
  passed in. Callers pass the same sqlite connection style as ``lanes.py``
  (``sqlite3.Row`` row_factory). This module sets ``row_factory`` when it
  is unset; it does not open a database of its own.
- Compaction is a bounded TTL delete that never removes rows above any
  consumer's offset. With ``keep_terminal=True`` it also retains the latest
  ``completed`` / ``failed`` / ``reaped`` row per ``(task_ref, lane_id)``.

Schema
------
``lane_events(id INTEGER PRIMARY KEY AUTOINCREMENT, task_ref TEXT NOT NULL,
lane_id TEXT NOT NULL, dispatch_id TEXT NOT NULL, kind TEXT NOT NULL,
payload_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
UNIQUE(task_ref, lane_id, dispatch_id, kind))`` plus index
``(task_ref, lane_id, id)``.

``lane_event_consumers(consumer TEXT PRIMARY KEY, last_event_id INTEGER
NOT NULL DEFAULT 0, updated_at TEXT NOT NULL)``.

Kinds
-----
``dispatched``, ``started``, ``checkpoint``, ``completed``, ``failed``,
``needs_guidance``, ``acked``, ``reaped``. Unknown kinds raise
``ValueError``.

Public API
----------
- :func:`append_lane_event` — INSERT OR IGNORE then SELECT the id;
  returns ``(id, inserted)``. Re-append of the same key returns the
  original id and ``inserted=False`` and never overwrites the payload.
- :func:`read_lane_events` — rows with ``id > offset`` ordered by id;
  ``limit`` is clamped to 1..500.
- :func:`ack_lane_events` — upsert ``offset = max(existing, upto_id)``
  in one statement; never regresses; returns the resulting offset.
- :func:`derive_lane_state` — fold events in id order into a derived
  view. An empty log is ``event_count=0`` with every flag ``False``.
- :func:`compact_lane_events` — delete rows older than the cutoff whose
  id is ``<= min(last_event_id)`` over all consumers, bounded by
  ``limit`` per call.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

LANE_EVENT_KINDS = frozenset(
    {
        "dispatched",
        "started",
        "checkpoint",
        "completed",
        "failed",
        "needs_guidance",
        "acked",
        "reaped",
    }
)
TERMINAL_LANE_EVENT_KINDS = frozenset({"completed", "failed", "reaped"})
READ_LIMIT_MAX = 500
READ_LIMIT_DEFAULT = 100
COMPACT_LIMIT_DEFAULT = 1000

_FLAG_KINDS = frozenset({"dispatched", "completed", "failed", "acked"})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-empty string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must be a non-empty string")
    return normalized


def _require_kind(kind: object) -> str:
    normalized = _require_text(kind, "kind")
    if normalized not in LANE_EVENT_KINDS:
        raise ValueError(f"unknown lane event kind: {normalized!r}")
    return normalized


def _payload_json(payload: Mapping[str, Any] | None) -> str:
    if payload is None:
        return "{}"
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a mapping or None")
    return json.dumps(dict(payload), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _clamp_read_limit(limit: object) -> int:
    try:
        value = int(limit)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    return max(1, min(value, READ_LIMIT_MAX))


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    mapping = dict(row)
    raw = mapping.get("payload_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {}
        mapping["payload"] = parsed if isinstance(parsed, dict) else {}
    else:
        mapping["payload"] = {}
    return mapping


def _ensure_schema(conn: sqlite3.Connection) -> None:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lane_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_ref TEXT NOT NULL,
            lane_id TEXT NOT NULL,
            dispatch_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            UNIQUE (task_ref, lane_id, dispatch_id, kind)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_lane_events_task_lane_id
        ON lane_events (task_ref, lane_id, id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lane_event_consumers (
            consumer TEXT PRIMARY KEY,
            last_event_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )


def _consumer_offset(conn: sqlite3.Connection, consumer: str) -> int:
    row = conn.execute(
        "SELECT last_event_id FROM lane_event_consumers WHERE consumer = ?",
        (consumer,),
    ).fetchone()
    if row is None:
        return 0
    return int(row["last_event_id"])


def append_lane_event(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
    dispatch_id: str,
    kind: str,
    payload: Mapping[str, Any] | None = None,
) -> tuple[int, bool]:
    """Insert an immutable event, or return the existing id on replay.

    ``INSERT OR IGNORE`` then ``SELECT`` the id. A re-append of the same
    ``(task_ref, lane_id, dispatch_id, kind)`` key returns the original id
    with ``inserted=False`` and does not overwrite ``payload_json``.
    """
    _ensure_schema(conn)
    task_ref = _require_text(task_ref, "task_ref")
    lane_id = _require_text(lane_id, "lane_id")
    dispatch_id = _require_text(dispatch_id, "dispatch_id")
    kind = _require_kind(kind)
    payload_json = _payload_json(payload)
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO lane_events (
            task_ref, lane_id, dispatch_id, kind, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (task_ref, lane_id, dispatch_id, kind, payload_json, _utc_now_iso()),
    )
    inserted = cursor.rowcount > 0
    row = conn.execute(
        """
        SELECT id FROM lane_events
        WHERE task_ref = ? AND lane_id = ? AND dispatch_id = ? AND kind = ?
        """,
        (task_ref, lane_id, dispatch_id, kind),
    ).fetchone()
    if row is None:
        raise RuntimeError("lane event insert did not persist a row")
    return int(row["id"]), inserted


def read_lane_events(
    conn: sqlite3.Connection,
    consumer: str,
    limit: int = READ_LIMIT_DEFAULT,
) -> list[dict[str, Any]]:
    """Return events with ``id >`` the consumer's offset, in id order."""
    _ensure_schema(conn)
    consumer = _require_text(consumer, "consumer")
    clamped = _clamp_read_limit(limit)
    offset = _consumer_offset(conn, consumer)
    rows = conn.execute(
        """
        SELECT id, task_ref, lane_id, dispatch_id, kind, payload_json, created_at
        FROM lane_events
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (offset, clamped),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def ack_lane_events(conn: sqlite3.Connection, consumer: str, upto_id: int) -> int:
    """Advance ``consumer`` to ``max(existing, upto_id)`` in one statement.

    Never moves the offset backwards. Returns the resulting offset.
    """
    _ensure_schema(conn)
    consumer = _require_text(consumer, "consumer")
    try:
        target = int(upto_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("upto_id must be an integer") from exc
    conn.execute(
        """
        INSERT INTO lane_event_consumers (consumer, last_event_id, updated_at)
        VALUES (?, MAX(?, 0), ?)
        ON CONFLICT(consumer) DO UPDATE SET
            last_event_id = MAX(lane_event_consumers.last_event_id, excluded.last_event_id),
            updated_at = excluded.updated_at
        """,
        (consumer, target, _utc_now_iso()),
    )
    return _consumer_offset(conn, consumer)


def derive_lane_state(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> dict[str, Any]:
    """Fold a lane's events in id order into a derived view.

    An empty log returns ``event_count=0``, every flag ``False``, and
    ``last_dispatch_id`` / ``latest_kind`` as ``None``.
    """
    _ensure_schema(conn)
    task_ref = _require_text(task_ref, "task_ref")
    lane_id = _require_text(lane_id, "lane_id")
    rows = conn.execute(
        """
        SELECT dispatch_id, kind
        FROM lane_events
        WHERE task_ref = ? AND lane_id = ?
        ORDER BY id ASC
        """,
        (task_ref, lane_id),
    ).fetchall()
    state: dict[str, Any] = {
        "last_dispatch_id": None,
        "dispatched": False,
        "completed": False,
        "failed": False,
        "acked": False,
        "latest_kind": None,
        "event_count": 0,
    }
    for row in rows:
        kind = str(row["kind"])
        state["last_dispatch_id"] = str(row["dispatch_id"])
        state["latest_kind"] = kind
        state["event_count"] = int(state["event_count"]) + 1
        if kind in _FLAG_KINDS:
            state[kind] = True
    return state


def compact_lane_events(
    conn: sqlite3.Connection,
    *,
    older_than_days: int,
    keep_terminal: bool = True,
    limit: int = COMPACT_LIMIT_DEFAULT,
) -> int:
    """Delete old events at or below every consumer's offset.

    Rows with ``id > min(last_event_id)`` over ``lane_event_consumers`` are
    never deleted. With no consumers (or a watermark of 0) this is a no-op.
    When ``keep_terminal`` is true, the latest completed/failed/reaped row
    per ``(task_ref, lane_id)`` is retained. Bounded by ``limit`` per call.
    """
    _ensure_schema(conn)
    try:
        days = int(older_than_days)
    except (TypeError, ValueError) as exc:
        raise ValueError("older_than_days must be an integer") from exc
    if days < 0:
        raise ValueError("older_than_days must be >= 0")
    try:
        delete_limit = int(limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if delete_limit <= 0:
        return 0

    watermark_row = conn.execute("SELECT MIN(last_event_id) AS watermark FROM lane_event_consumers").fetchone()
    if watermark_row is None or watermark_row["watermark"] is None:
        return 0
    watermark = int(watermark_row["watermark"])
    if watermark <= 0:
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    terminal_clause = ""
    params: tuple[object, ...]
    if keep_terminal:
        terminal_clause = """
            AND id NOT IN (
                SELECT MAX(id) FROM lane_events
                WHERE kind IN ('completed', 'failed', 'reaped')
                GROUP BY task_ref, lane_id
            )
        """
    params = (watermark, cutoff, delete_limit)
    cursor = conn.execute(
        f"""
        DELETE FROM lane_events
        WHERE id IN (
            SELECT id FROM (
                SELECT id FROM lane_events
                WHERE id <= ?
                  AND created_at < ?
                  {terminal_clause}
                ORDER BY id ASC
                LIMIT ?
            )
        )
        """,
        params,
    )
    return int(cursor.rowcount)
