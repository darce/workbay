"""Shared low-level helpers for the lanes domain split.

Bottom of the one-directional import DAG ``lanes -> {plan_cursors, lane_reaping}
-> lanes_support``. This module must never import from ``.lanes``,
``.plan_cursors``, or ``.lane_reaping`` — that is what breaks the import cycle
that used to exist when ``plan_cursors``/``lane_reaping`` imported these
helpers from ``.lanes`` while ``lanes`` tail-imported symbols back from them.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

CLOSEABLE_LANE_STATUSES = frozenset({"closed", "merged"})

# internal: blocked-lane aging + conclusive-close.
DEFAULT_BLOCKED_LANE_REAP_BATCH = 50
_LANE_STATUS_BLOCKED = "blocked"
_LANE_STATUS_CLOSED_STALE = "closed_stale"
_BRANCH_PROBE_TIMEOUT_S = 5.0


def _json_response(payload: dict[str, object]) -> dict[str, object]:
    return payload.copy()


def _normalize_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized if normalized else None


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return dict(row) if row is not None else None


def _workspace_root() -> Path:
    from workbay_handoff_mcp import get_runtime_config  # noqa: PLC0415

    return get_runtime_config().workspace_root


def _paginated_query(
    conn: sqlite3.Connection,
    table: str,
    where_sql: str,
    params: tuple[object, ...],
    limit: int,
    offset: int,
    order_sql: str,
    row_decoder: Callable[[dict[str, object]], dict[str, object]] = dict,
) -> tuple[int, list[dict[str, object]]]:
    total = int(conn.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE {where_sql}", params).fetchone()["count"])
    rows = [
        row_decoder(dict(row))
        for row in conn.execute(
            f"SELECT * FROM {table} WHERE {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    ]
    return total, rows


def _get_db_connection(**kwargs: object) -> AbstractContextManager[sqlite3.Connection]:
    from workbay_handoff_mcp.shared_schema import _get_db_connection as _handoff_get_db_connection  # noqa: PLC0415

    return _handoff_get_db_connection(**kwargs)


def _resolve_task_ref(conn: sqlite3.Connection, task_ref: str | None) -> str:
    from workbay_handoff_mcp.shared_primitives import _resolve_task_ref as _handoff_resolve_task_ref  # noqa: PLC0415

    return _handoff_resolve_task_ref(conn, task_ref)


_VALID_DETAIL_LEVELS = {"full", "summary"}
_LIST_SECTION_IDENTITY = "identity"
_LIST_SECTION_COUNTS = "counts"

_PLAN_CURSOR_IDENTITY_FIELDS = frozenset({"id", "task_ref", "plan_item_id", "lane_id", "state"})


def _normalize_read_detail(detail: str) -> str:
    return detail if detail in _VALID_DETAIL_LEVELS else "full"


def _parse_projection_fields(fields: str | None) -> frozenset[str] | None:
    if fields is None:
        return None
    return frozenset(part.strip() for part in fields.split(",") if part.strip())


def _parse_sections(sections: str | None, valid_sections: frozenset[str]) -> frozenset[str] | None:
    if sections is None:
        return None
    requested = frozenset(part.strip() for part in sections.split(",") if part.strip())
    if not requested:
        return None
    return requested & valid_sections


def _project_mapping(
    mapping: dict[str, object],
    requested_fields: frozenset[str] | None,
    identity_fields: frozenset[str],
) -> dict[str, object]:
    if requested_fields is None:
        allowed_fields: frozenset[str] | None = None
    else:
        allowed_fields = requested_fields or identity_fields
    return {key: value for key, value in mapping.items() if allowed_fields is None or key in allowed_fields}


def _truncate_text(value: object, limit: int = 160) -> object:
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    return value


def _summarize_value(value: object) -> object:
    if isinstance(value, str):
        return _truncate_text(value)
    if isinstance(value, dict):
        return {key: _summarize_value(raw_value) for key, raw_value in value.items()}
    if isinstance(value, list):
        preview = [_summarize_value(item) for item in value[:5]]
        if len(value) > 5:
            preview.append("...")
        return preview
    return value


def _summarize_generic_row(row: dict[str, object]) -> dict[str, object]:
    return {key: _summarize_value(value) for key, value in row.items()}


def _effective_limit(limit: int, top_n: int | None) -> int:
    if top_n is not None:
        return max(1, int(top_n))
    return max(1, limit)


def _invalid_sections_error(valid_sections: frozenset[str]) -> dict[str, object]:
    return {"ok": False, "error": f"Invalid sections. Valid: {', '.join(sorted(valid_sections))}"}


def _shape_list_payload(
    payload: dict[str, object],
    *,
    sections: str | None,
    detail: str,
    fields: str | None,
    row_key: str,
    identity_fields: frozenset[str],
    summary_fn: Callable[[dict[str, object]], dict[str, object]] | None = None,
) -> dict[str, object]:
    detail = _normalize_read_detail(detail)
    requested_fields = _parse_projection_fields(fields)
    valid_sections = frozenset({_LIST_SECTION_IDENTITY, _LIST_SECTION_COUNTS, row_key})
    requested_sections = _parse_sections(sections, valid_sections)
    if sections is not None and requested_sections == frozenset():
        return _invalid_sections_error(valid_sections)
    if requested_sections is None:
        requested_sections = valid_sections
    shaped: dict[str, object] = {"ok": payload["ok"]}
    if _LIST_SECTION_IDENTITY in requested_sections:
        for key, value in payload.items():
            if key not in {"ok", "total_matching", "returned", "has_more", row_key}:
                shaped[key] = value
    if _LIST_SECTION_COUNTS in requested_sections:
        for key in ("total_matching", "returned", "has_more"):
            if key in payload:
                shaped[key] = payload[key]
    if row_key in requested_sections:
        rows = payload.get(row_key, [])
        shaped_rows: list[dict[str, object]] = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                summarized = summary_fn(row) if detail == "summary" and callable(summary_fn) else dict(row)
                shaped_rows.append(_project_mapping(summarized, requested_fields, identity_fields))
        shaped[row_key] = shaped_rows
    return shaped
