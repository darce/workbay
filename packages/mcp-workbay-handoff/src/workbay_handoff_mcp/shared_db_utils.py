"""Generic database utilities for workbay_handoff_mcp.

Extracted from _shared.py (implementation note of internal). Contains:
  - _fetch_handoff_rows: filtered row fetch with table-aware decode dispatch
  - _paginated_query: COUNT + paginated SELECT helper
  - _count_task_rows: per-table row counts for a task_ref
  - _resolve_output_path: resolve/validate an export output path

All symbols are re-exported from _shared.py for backward compatibility.

Imports from _shared are done at function level (late imports) to avoid a circular
module dependency: _shared.py re-exports from this module at its top level, so
module-level imports here would create a deadlock when this module is loaded first.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable

from .runtime import get_runtime_config
from .shared_primitives import _decode_lane_message_row_dict, _decode_turn_metric_row_dict


def _fetch_handoff_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    where_sql: str,
    order_sql: str,
    limit: int,
    params: tuple[object, ...],
) -> list[dict]:
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE {where_sql} ORDER BY {order_sql} LIMIT ?",
        (*params, limit),
    ).fetchall()
    payload = [dict(row) for row in rows]
    if table == "lane_messages":
        return [_decode_lane_message_row_dict(row) for row in payload]
    if table == "turn_metrics":
        return [_decode_turn_metric_row_dict(row) for row in payload]
    return payload


def _paginated_query(
    conn: sqlite3.Connection,
    table: str,
    where_sql: str,
    params: tuple[object, ...],
    limit: int,
    offset: int,
    order_sql: str,
    row_decoder: Callable[[dict], dict] = dict,
) -> tuple[int, list[dict]]:
    """Run a COUNT then a paginated SELECT, returning (total, rows)."""
    total = int(conn.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE {where_sql}", params).fetchone()["count"])
    rows = [
        row_decoder(dict(row))
        for row in conn.execute(
            f"SELECT * FROM {table} WHERE {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    ]
    return total, rows


def _count_task_rows(conn: sqlite3.Connection, task_ref: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key in (
        "blockers",
        "next_actions",
        "decisions",
        "verified_tests",
        "test_traces",
        "review_findings",
        "worktree_lanes",
        "worker_reports",
        "lane_messages",
        "plan_cursors",
        "turn_metrics",
        "terminal_guard_events",
    ):
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {key} WHERE task_ref = ?", (task_ref,)).fetchone()
        counts[key] = int(row["count"]) if row else 0
    return counts


def _resolve_output_path(output_path: str | None, task_ref: str) -> Path:
    cfg = get_runtime_config()
    if output_path:
        path = Path(output_path)
        if not path.is_absolute():
            path = cfg.workspace_root / path
    else:
        safe_task_ref = task_ref.replace("/", "_").replace("..", "_")
        path = cfg.exports_dir / f"handoff-{safe_task_ref}.json"
        resolved = path.resolve()
        allowed_root = cfg.workspace_root.resolve()
        if not str(resolved).startswith(str(allowed_root) + "/") and resolved != allowed_root:
            raise ValueError(f"Output path escapes workspace root: {resolved}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
