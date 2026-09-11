"""Projection event-id dedupe helpers."""

from __future__ import annotations

import sqlite3


def normalize_event_id(event_id: str | None) -> str | None:
    if event_id is None:
        return None
    normalized = event_id.strip()
    return normalized or None


def fetch_projection_event(conn: sqlite3.Connection, event_id: str | None) -> sqlite3.Row | None:
    normalized = normalize_event_id(event_id)
    if normalized is None:
        return None
    row: sqlite3.Row | None = conn.execute(
        "SELECT * FROM projection_event_dedupe WHERE event_id = ?",
        (normalized,),
    ).fetchone()
    return row


def claim_projection_event(
    conn: sqlite3.Connection,
    *,
    event_id: str | None,
    tool_name: str,
    target_table: str,
    task_ref: str | None,
) -> sqlite3.Row | None:
    """Reserve an event id, returning the existing row when already claimed."""

    normalized = normalize_event_id(event_id)
    if normalized is None:
        return None
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO projection_event_dedupe (
            event_id, tool_name, target_table, target_id, task_ref, created_at
        )
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        (normalized, tool_name, target_table, None, task_ref),
    )
    if cur.rowcount == 0:
        return fetch_projection_event(conn, normalized)
    return None


def complete_projection_event(
    conn: sqlite3.Connection,
    *,
    event_id: str | None,
    target_table: str,
    target_id: int | None,
    task_ref: str | None,
) -> None:
    normalized = normalize_event_id(event_id)
    if normalized is None:
        return
    conn.execute(
        """
        UPDATE projection_event_dedupe
        SET target_table = ?, target_id = ?, task_ref = COALESCE(?, task_ref)
        WHERE event_id = ?
        """,
        (target_table, target_id, task_ref, normalized),
    )


def clear_projection_event_claim(conn: sqlite3.Connection, event_id: str | None) -> None:
    normalized = normalize_event_id(event_id)
    if normalized is None:
        return
    conn.execute("DELETE FROM projection_event_dedupe WHERE event_id = ?", (normalized,))
