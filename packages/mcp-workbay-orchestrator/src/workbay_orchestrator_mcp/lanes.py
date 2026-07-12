"""Lanes domain module.

Contains worktree lane management, turn metrics, worker reports, and lane messages.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict, cast

CLOSEABLE_LANE_STATUSES = frozenset({"closed", "merged"})
LANE_MESSAGE_DIRECTIONS = frozenset({"orchestrator_to_worker", "worker_to_orchestrator"})
# closed_stale: schema v27 terminal for blocked-lane reclaimer (internal).
LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "merged", "closed", "closed_stale"})
MESSAGE_STATUSES = frozenset({"open", "acknowledged", "closed"})
REPORT_STATUSES = frozenset({"submitted", "acknowledged", "superseded"})
# Terminal consumption statuses written by acknowledge_worker_report (never 'submitted').
REPORT_ACK_STATUSES = frozenset({"acknowledged", "superseded"})
# "no_actionable_work" is the canonical empty-inbox outcome shared with
# worker_start / run_offload_pass (HARM-A-006); "no_work" is retained as a legacy
# alias so historical worker_reports rows remain valid.
WORKER_REPORT_OUTCOMES = frozenset({"finished", "failed", "exhausted", "stopped", "no_actionable_work", "no_work"})
REVIEW_KINDS = frozenset({"branch", "planning"})

# internal: blocked-lane aging + conclusive-close.
DEFAULT_BLOCKED_LANE_REAP_BATCH = 50
_LANE_STATUS_BLOCKED = "blocked"
_LANE_STATUS_CLOSED_STALE = "closed_stale"
_BRANCH_PROBE_TIMEOUT_S = 5.0


@dataclass
class TokenUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_output_tokens: int | None = None
    total_tokens: int | None = None
    usage_source: str | None = None


@dataclass
class PromptMetrics:
    model_context_window: int | None = None
    prompt_tokens: int | None = None
    prompt_chars: int | None = None
    prompt_token_source: str | None = None
    utilization_ratio: float | None = None
    domain_signal_ratio: float | None = None
    pressure_level: str | None = None


class WriteActor(TypedDict, total=False):
    agent: str
    model: str
    model_label: str
    reasoning_level: str
    branch: str
    commit_sha: str
    lane_id: str


def _json_response(payload: dict[str, object]) -> dict[str, object]:
    return dict(payload)


def _normalize_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized if normalized else None


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return dict(row) if row is not None else None


def _decode_lane_message_row_dict(row: dict[str, object]) -> dict[str, object]:
    payload_json = row.get("payload_json")
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            row["payload"] = payload
    return row


def _decode_turn_metric_row_dict(row: dict[str, object]) -> dict[str, object]:
    for key, empty in (("attribution_json", {}), ("section_sizes_json", {}), ("raw_usage_json", None)):
        raw_value = row.get(key)
        if not isinstance(raw_value, str) or not raw_value.strip():
            row[key.removesuffix("_json")] = empty
            continue
        try:
            row[key.removesuffix("_json")] = json.loads(raw_value)
        except json.JSONDecodeError:
            row[key.removesuffix("_json")] = empty
    return row


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        normalized = _normalize_optional_text(item)
        if normalized is not None:
            result.append(normalized)
    return result


def _normalize_lane_message_payload(payload: object) -> tuple[dict[str, object] | None, str | None]:
    if payload is None:
        return None, None
    if not isinstance(payload, dict):
        return None, "lane message payload must be an object when provided."
    normalized: dict[str, object] = {}
    for key in ("source_lane", "reason", "summary", "dispatch_id"):
        value = _normalize_optional_text(payload.get(key))
        if value is not None:
            normalized[key] = value
    for key in ("required_actions", "artifacts"):
        values = _coerce_string_list(payload.get(key))
        if values:
            normalized[key] = values
    raw_override = payload.get("owned_paths_override")
    if isinstance(raw_override, str):
        raw_override = [raw_override]
    override_values = _coerce_string_list(raw_override)
    if override_values:
        normalized["owned_paths_override"] = override_values
    return normalized, None


def _workspace_root() -> Path:
    from workbay_handoff_mcp import get_runtime_config  # noqa: PLC0415

    return get_runtime_config().workspace_root


def _normalize_path_for_match(path_value: str | Path) -> str:
    return os.path.normcase(str(Path(path_value).expanduser().resolve()))


def _adapt_lane_envelope(envelope: dict[str, object]) -> dict[str, object]:
    """Map handoff v2 lane envelopes to orchestrator JSON responses."""
    if envelope.get("schema_version") == 2:
        data = envelope.get("data")
        if not isinstance(data, dict):
            data = {}
        if envelope.get("ok"):
            return _json_response({"ok": True, **data})
        error = data.get("error", "lane operation failed")
        return _json_response({"ok": False, "error": error})
    return _json_response(envelope)


def _resolve_current_lane_row(conn: sqlite3.Connection, task_ref: str) -> dict[str, object] | None:
    del conn
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    workspace_path = _normalize_path_for_match(_workspace_root())
    listed = list_lanes(task_ref=task_ref, status="all", limit=1000, offset=0)
    if not listed.get("ok"):
        return None
    data = listed.get("data") if isinstance(listed.get("data"), dict) else listed
    lanes = data.get("lanes") if isinstance(data, dict) else None
    if not isinstance(lanes, list):
        return None
    for row in lanes:
        if not isinstance(row, dict):
            continue
        raw_path = _normalize_optional_text(row.get("worktree_path"))
        if raw_path is None:
            continue
        if _normalize_path_for_match(raw_path) == workspace_path:
            return row
    return None


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


def _fetch_handoff_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    where_sql: str,
    order_sql: str,
    limit: int,
    params: tuple[object, ...],
) -> list[dict[str, object]]:
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


def _excerpt_text(value: str | None, *, limit: int = 240) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    collapsed = " ".join(normalized.split())
    if len(collapsed) <= limit:
        return collapsed
    if limit <= 3:
        return "." * limit
    return f"{collapsed[: limit - 3].rstrip()}..."


def _count_by_value(
    conn: sqlite3.Connection,
    *,
    table: str,
    field: str,
    task_ref: str,
    lane_id: str,
    allowed_values: frozenset[str],
) -> dict[str, int]:
    counts = {value: 0 for value in sorted(allowed_values)}
    rows = conn.execute(
        f"SELECT {field} AS value, COUNT(*) AS count FROM {table} WHERE task_ref = ? AND lane_id = ? GROUP BY {field}",
        (task_ref, lane_id),
    ).fetchall()
    for row in rows:
        value = _normalize_optional_text(row["value"])
        if value is not None and value in counts:
            counts[value] = int(row["count"])
    return counts


def _build_archival_lane_activity_summary(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    lane_id: str,
) -> dict[str, object]:
    decisions_total_row = conn.execute(
        "SELECT COUNT(*) AS count FROM decisions WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    latest_decision_row = conn.execute(
        """
        SELECT rationale
        FROM decisions
        WHERE task_ref = ? AND lane_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (task_ref, lane_id),
    ).fetchone()
    reports_total_row = conn.execute(
        "SELECT COUNT(*) AS count FROM worker_reports WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    latest_report_row = conn.execute(
        """
        SELECT merge_ready
        FROM worker_reports
        WHERE task_ref = ? AND lane_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (task_ref, lane_id),
    ).fetchone()
    tests_summary_row = conn.execute(
        """
        SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END), 0) AS passed
        FROM verified_tests
        WHERE task_ref = ? AND lane_id = ?
        """,
        (task_ref, lane_id),
    ).fetchone()
    tests_total = int(tests_summary_row["total"]) if tests_summary_row else 0
    tests_passed = int(tests_summary_row["passed"]) if tests_summary_row else 0
    return {
        "decisions": {
            "count": int(decisions_total_row["count"]) if decisions_total_row else 0,
            "latest_rationale_excerpt": _excerpt_text(
                str(latest_decision_row["rationale"])
                if latest_decision_row and latest_decision_row["rationale"] is not None
                else None
            ),
        },
        "findings": {
            "counts_by_status": _count_by_value(
                conn,
                table="review_findings",
                field="status",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=frozenset({"open", "fixed", "wontfix", "deferred", "resolved_on_branch", "integrated"}),
            ),
        },
        "reports": {
            "count": int(reports_total_row["count"]) if reports_total_row else 0,
            "latest_merge_ready": (
                bool(latest_report_row["merge_ready"])
                if latest_report_row is not None and latest_report_row["merge_ready"] is not None
                else None
            ),
            "counts_by_outcome": _count_by_value(
                conn,
                table="worker_reports",
                field="outcome",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=WORKER_REPORT_OUTCOMES,
            ),
        },
        "messages": {
            "counts_by_direction": _count_by_value(
                conn,
                table="lane_messages",
                field="direction",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=LANE_MESSAGE_DIRECTIONS,
            ),
            "counts_by_status": _count_by_value(
                conn,
                table="lane_messages",
                field="status",
                task_ref=task_ref,
                lane_id=lane_id,
                allowed_values=MESSAGE_STATUSES,
            ),
        },
        "tests": {
            "total": tests_total,
            "passed": tests_passed,
            "pass_rate": round(tests_passed / tests_total, 3) if tests_total else None,
        },
    }


def _write_current_task_md_for_task(conn: sqlite3.Connection, task_ref: str) -> None:
    del conn
    from workbay_handoff_mcp import generate_current_task_md  # noqa: PLC0415

    generate_current_task_md(task_ref=task_ref, write_file=True)


def _get_db_connection() -> AbstractContextManager[sqlite3.Connection]:
    from workbay_handoff_mcp.shared_schema import _get_db_connection as _handoff_get_db_connection  # noqa: PLC0415

    return _handoff_get_db_connection()


def _resolve_task_ref(conn: sqlite3.Connection, task_ref: str | None) -> str:
    from workbay_handoff_mcp.shared_primitives import _resolve_task_ref as _handoff_resolve_task_ref  # noqa: PLC0415

    return _handoff_resolve_task_ref(conn, task_ref)


def _resolve_write_actor(conn: sqlite3.Connection, actor: WriteActor | None):
    from workbay_handoff_mcp.shared_write_context import (
        _resolve_write_actor as _handoff_resolve_write_actor,  # noqa: PLC0415
    )

    return _handoff_resolve_write_actor(conn, actor)


_VALID_DETAIL_LEVELS = {"full", "summary"}
_LIST_SECTION_IDENTITY = "identity"
_LIST_SECTION_COUNTS = "counts"

_LANE_MESSAGE_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "status"})
_TURN_METRIC_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "session", "phase", "backend", "model"})
_WORKER_REPORT_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "session", "status", "merge_ready", "outcome"})
_PLAN_CURSOR_IDENTITY_FIELDS = frozenset({"id", "task_ref", "plan_item_id", "lane_id", "state"})
_LANE_ACTIVITY_LANE_IDENTITY_FIELDS = frozenset({"id", "task_ref", "lane_id", "status", "title", "objective"})
_LANE_ACTIVITY_DECISION_IDENTITY_FIELDS = frozenset({"id", "decision", "created_at"})
_LANE_ACTIVITY_TEST_IDENTITY_FIELDS = frozenset({"id", "command", "passed", "verified_at"})
_LANE_ACTIVITY_BLOCKER_IDENTITY_FIELDS = frozenset({"id", "description", "status", "created_at"})
_LANE_ACTIVITY_ACTION_IDENTITY_FIELDS = frozenset({"id", "action", "status", "priority", "updated_at"})
_LANE_ACTIVITY_FINDING_IDENTITY_FIELDS = frozenset({"id", "title", "severity", "status", "created_at"})


def _get_lane_row(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> dict[str, object] | None:
    del conn
    from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

    envelope = get_lane(lane_id=lane_id, task_ref=task_ref)
    if not envelope.get("ok"):
        return None
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    lane = data.get("lane") if isinstance(data, dict) else None
    return lane if isinstance(lane, dict) else None


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


def _summarize_turn_metric_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized.pop("attribution_json", None)
    summarized.pop("section_sizes_json", None)
    summarized.pop("raw_usage_json", None)
    for key in ("attribution", "section_sizes", "raw_usage"):
        if key in summarized:
            summarized[key] = _summarize_value(summarized.get(key))
    return summarized


def _summarize_worker_report_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized.pop("changed_files_json", None)
    summarized.pop("test_commands_json", None)
    summarized.pop("blockers_json", None)
    return summarized


def _summarize_lane_message_row(row: dict[str, object]) -> dict[str, object]:
    summarized = dict(row)
    summarized["message"] = _truncate_text(summarized.get("message"), 240)
    summarized.pop("payload_json", None)
    if "payload" in summarized:
        summarized["payload"] = _summarize_value(summarized.get("payload"))
    return summarized


def _summarize_generic_row(row: dict[str, object]) -> dict[str, object]:
    return {key: _summarize_value(value) for key, value in row.items()}


def _effective_limit(limit: int, top_n: int | None) -> int:
    if top_n is not None:
        return max(1, int(top_n))
    return max(1, limit)


def _invalid_sections_error(valid_sections: frozenset[str]) -> dict[str, object]:
    return {"ok": False, "error": f"Invalid sections. Valid: {', '.join(sorted(valid_sections))}"}


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


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


def upsert_worktree_lane(
    lane_id: str,
    worktree_path: str,
    branch: str,
    title: str | None = None,
    objective: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    status: str = "planned",
    notes: str | None = None,
    task_ref: str | None = None,
) -> dict:
    valid_statuses = LANE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_path = _normalize_optional_text(worktree_path)
    normalized_branch = _normalize_optional_text(branch)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if normalized_path is None:
        return _json_response({"ok": False, "error": "worktree_path is required."})
    if normalized_branch is None:
        return _json_response({"ok": False, "error": "branch is required."})
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import open_lane  # noqa: PLC0415

    return _adapt_lane_envelope(
        open_lane(
            lane_id=normalized_lane_id,
            worktree_path=normalized_path,
            branch=normalized_branch,
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            status=status,
            notes=notes,
            task_ref=task_ref,
        )
    )


def close_worktree_lane(
    lane_id: str,
    status: str = "closed",
    notes: str | None = None,
    task_ref: str | None = None,
) -> dict:
    """Transition a worktree lane to closed or merged status in the handoff database."""
    valid_close_statuses = CLOSEABLE_LANE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if status not in valid_close_statuses:
        return _json_response(
            {"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_close_statuses))}"}
        )
    from workbay_handoff_mcp.lanes_api import close_lane  # noqa: PLC0415

    return _adapt_lane_envelope(
        close_lane(
            lane_id=normalized_lane_id,
            status=status,
            notes=notes,
            task_ref=task_ref,
        )
    )


def list_worktree_lanes(task_ref: str | None = None, status: str = "all", limit: int = 100, offset: int = 0) -> dict:
    limit = max(1, limit)
    offset = max(0, offset)
    valid_statuses = {"all", *LANE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    return _adapt_lane_envelope(list_lanes(task_ref=task_ref, status=status, limit=limit, offset=offset))


def manage_worktree_lane(
    operation: str,
    lane_id: str | None = None,
    worktree_path: str | None = None,
    branch: str | None = None,
    title: str | None = None,
    objective: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Discriminated wrapper for worktree lane upsert, close, and list operations."""
    valid_operations = {"close", "list", "upsert"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "upsert":
        return upsert_worktree_lane(
            lane_id=str(lane_id or ""),
            worktree_path=str(worktree_path or ""),
            branch=str(branch or ""),
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            status=status or "planned",
            notes=notes,
            task_ref=task_ref,
        )
    if operation == "close":
        return close_worktree_lane(
            lane_id=str(lane_id or ""),
            status=status or "closed",
            notes=notes,
            task_ref=task_ref,
        )
    return list_worktree_lanes(
        task_ref=task_ref,
        status=status or "all",
        limit=limit,
        offset=offset,
    )


def record_turn_metric(
    session: str,
    phase: str,
    backend: str,
    cycle: int | None = None,
    lane_id: str | None = None,
    model: str | None = None,
    thread_id: str | None = None,
    turn_id: str | None = None,
    token_usage: TokenUsage | None = None,
    prompt_metrics: PromptMetrics | None = None,
    attribution: dict[str, Any] | None = None,
    section_sizes: dict[str, Any] | None = None,
    raw_usage: dict[str, Any] | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
) -> dict:
    if _normalize_optional_text(session) is None:
        return _json_response({"ok": False, "error": "session is required."})
    normalized_phase = _normalize_optional_text(phase)
    if normalized_phase is None:
        return _json_response({"ok": False, "error": "phase is required."})
    normalized_backend = _normalize_optional_text(backend)
    if normalized_backend is None:
        return _json_response({"ok": False, "error": "backend is required."})
    resolved_usage_source = token_usage.usage_source if token_usage else None
    resolved_prompt_token_source = prompt_metrics.prompt_token_source if prompt_metrics else None
    # usage_source allows grok_context_delta (implementation note S2 / PR-0094-01); prompt
    # token sources remain the observed/estimate set only (different unit).
    from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (  # noqa: PLC0415
        USAGE_SOURCE_GROK_CONTEXT_DELTA,
    )

    valid_usage_sources = {
        "observed",
        "tokenizer_estimate",
        "char_estimate",
        USAGE_SOURCE_GROK_CONTEXT_DELTA,
    }
    valid_prompt_sources = {"observed", "tokenizer_estimate", "char_estimate"}
    if resolved_usage_source is not None and resolved_usage_source not in valid_usage_sources:
        return _json_response({"ok": False, "error": "Invalid usage_source."})
    if resolved_prompt_token_source is not None and resolved_prompt_token_source not in valid_prompt_sources:
        return _json_response({"ok": False, "error": "Invalid prompt_token_source."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor)
        resolved_lane_id = _normalize_optional_text(lane_id) or ctx.lane_id
        cur = conn.execute(
            """
            INSERT INTO turn_metrics (
                task_ref, lane_id, session, cycle, phase, backend, model, thread_id, turn_id,
                input_tokens, output_tokens, cached_input_tokens, reasoning_output_tokens,
                total_tokens, usage_source, model_context_window, prompt_tokens, prompt_chars,
                prompt_token_source, utilization_ratio, domain_signal_ratio, pressure_level,
                attribution_json, section_sizes_json, raw_usage_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                resolved_lane_id,
                session,
                cycle,
                normalized_phase,
                normalized_backend,
                _normalize_optional_text(model),
                _normalize_optional_text(thread_id),
                _normalize_optional_text(turn_id),
                token_usage.input_tokens if token_usage else None,
                token_usage.output_tokens if token_usage else None,
                token_usage.cached_input_tokens if token_usage else None,
                token_usage.reasoning_output_tokens if token_usage else None,
                token_usage.total_tokens if token_usage else None,
                resolved_usage_source,
                prompt_metrics.model_context_window if prompt_metrics else None,
                prompt_metrics.prompt_tokens if prompt_metrics else None,
                prompt_metrics.prompt_chars if prompt_metrics else None,
                resolved_prompt_token_source,
                prompt_metrics.utilization_ratio if prompt_metrics else None,
                prompt_metrics.domain_signal_ratio if prompt_metrics else None,
                _normalize_optional_text(prompt_metrics.pressure_level if prompt_metrics else None),
                json.dumps(attribution or {}, sort_keys=True),
                json.dumps(section_sizes or {}, sort_keys=True),
                json.dumps(raw_usage, sort_keys=True) if raw_usage is not None else None,
            ),
        )
        row = conn.execute("SELECT * FROM turn_metrics WHERE id = ?", (cur.lastrowid,)).fetchone()
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "turn_metric": _decode_turn_metric_row_dict(_row_to_dict(row) or {}),
            }
        )


def list_turn_metrics(
    task_ref: str | None = None,
    lane_id: str | None = None,
    backend: str | None = None,
    model: str | None = None,
    phase: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_turn_metrics: int | None = None,
) -> dict:
    limit = _effective_limit(limit, top_n_turn_metrics)
    offset = max(0, offset)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        for field_name, value in (
            ("lane_id", _normalize_optional_text(lane_id)),
            ("backend", _normalize_optional_text(backend)),
            ("model", _normalize_optional_text(model)),
            ("phase", _normalize_optional_text(phase)),
        ):
            if value is None:
                continue
            where_sql += f" AND {field_name} = ?"
            params.append(value)
        total, rows = _paginated_query(
            conn,
            "turn_metrics",
            where_sql,
            tuple(params),
            limit,
            offset,
            "created_at DESC, id DESC",
            _decode_turn_metric_row_dict,
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": _normalize_optional_text(lane_id),
                    "backend": _normalize_optional_text(backend),
                    "model": _normalize_optional_text(model),
                    "phase": _normalize_optional_text(phase),
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "turn_metrics": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="turn_metrics",
                identity_fields=_TURN_METRIC_IDENTITY_FIELDS,
                summary_fn=_summarize_turn_metric_row,
            )
        )


def get_turn_metrics_summary(
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> dict:
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        normalized_lane_id = _normalize_optional_text(lane_id)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)

        rows = conn.execute(
            f"""
            SELECT usage_source, prompt_token_source, pressure_level, backend, model, lane_id,
                   total_tokens, prompt_tokens, input_tokens
            FROM turn_metrics
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchall()
        total_turns = len(rows)
        from workbay_orchestrator_mcp.orchestration.adapters.grok_session_tokens import (  # noqa: PLC0415
            USAGE_SOURCE_GROK_CONTEXT_DELTA,
        )

        usage_counts = {
            "observed": 0,
            "tokenizer_estimate": 0,
            "char_estimate": 0,
            USAGE_SOURCE_GROK_CONTEXT_DELTA: 0,
        }
        prompt_counts = {"observed": 0, "tokenizer_estimate": 0, "char_estimate": 0}
        pressure_counts: dict[str, int] = {}
        tokens_by_lane: dict[str, int] = {}
        tokens_by_backend_model: dict[str, int] = {}
        tokens_by_usage_source: dict[str, int] = {
            "observed": 0,
            "tokenizer_estimate": 0,
            "char_estimate": 0,
            USAGE_SOURCE_GROK_CONTEXT_DELTA: 0,
        }
        prompt_tokens_total = 0
        total_tokens_total = 0
        comparable_turns = 0
        exact_preflight_turns = 0
        estimated_preflight_turns = 0
        drift_sum = 0
        abs_drift_sum = 0
        max_abs_drift = 0

        for row in rows:
            usage = row["usage_source"]
            prompt_source = row["prompt_token_source"]
            pressure_level = row["pressure_level"] or "unknown"
            lane_key = str(row["lane_id"] or "unscoped")
            backend_model_key = f"{row['backend']}::{row['model'] or 'default'}"
            if isinstance(usage, str) and usage in usage_counts:
                usage_counts[usage] += 1
            if isinstance(prompt_source, str) and prompt_source in prompt_counts:
                prompt_counts[prompt_source] += 1
            pressure_counts[str(pressure_level)] = pressure_counts.get(str(pressure_level), 0) + 1
            total_tokens_value = int(row["total_tokens"] or 0)
            prompt_tokens_value = int(row["prompt_tokens"] or 0)
            input_tokens_value = row["input_tokens"]
            # PR-0094-05: grok_context_delta is cumulative context fill — a different
            # unit from observed input/output. Label/bucket it; never sum into the
            # observed-style totals (total_tokens / by_lane / by_backend_model).
            if isinstance(usage, str) and usage in tokens_by_usage_source:
                tokens_by_usage_source[usage] += total_tokens_value
            if usage == USAGE_SOURCE_GROK_CONTEXT_DELTA:
                continue
            total_tokens_total += total_tokens_value
            prompt_tokens_total += prompt_tokens_value
            tokens_by_lane[lane_key] = tokens_by_lane.get(lane_key, 0) + total_tokens_value
            tokens_by_backend_model[backend_model_key] = (
                tokens_by_backend_model.get(backend_model_key, 0) + total_tokens_value
            )
            if prompt_tokens_value > 0 and input_tokens_value is not None:
                comparable_turns += 1
                drift = int(input_tokens_value) - prompt_tokens_value
                drift_sum += drift
                abs_drift = abs(drift)
                abs_drift_sum += abs_drift
                if abs_drift > max_abs_drift:
                    max_abs_drift = abs_drift
                if prompt_source == "observed":
                    exact_preflight_turns += 1
                elif isinstance(prompt_source, str):
                    estimated_preflight_turns += 1

        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "summary": {
                    "total_turns": total_turns,
                    "usage_source_counts": usage_counts,
                    "prompt_token_source_counts": prompt_counts,
                    "pressure_level_counts": pressure_counts,
                    "total_tokens": total_tokens_total,
                    "prompt_tokens": prompt_tokens_total,
                    "by_lane_total_tokens": tokens_by_lane,
                    "by_backend_model_total_tokens": tokens_by_backend_model,
                    "total_tokens_by_usage_source": tokens_by_usage_source,
                    "preflight_observed_drift": {
                        "comparable_turns": comparable_turns,
                        "exact_preflight_turns": exact_preflight_turns,
                        "estimated_preflight_turns": estimated_preflight_turns,
                        "net_token_drift": drift_sum,
                        "mean_signed_token_drift": (
                            round(drift_sum / comparable_turns, 3) if comparable_turns else None
                        ),
                        "mean_absolute_token_drift": (
                            round(abs_drift_sum / comparable_turns, 3) if comparable_turns else None
                        ),
                        "max_absolute_token_drift": max_abs_drift if comparable_turns else None,
                    },
                },
            }
        )


def get_lane_activity(
    lane_id: str,
    task_ref: str | None = None,
    limit_decisions: int = 20,
    limit_tests: int = 20,
    limit_blockers: int = 20,
    limit_actions: int = 20,
    limit_findings: int = 20,
    limit_reports: int = 20,
    limit_messages: int = 20,
    format: str = "full",
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_decisions: int | None = None,
    top_n_tests: int | None = None,
    top_n_blockers: int | None = None,
    top_n_actions: int | None = None,
    top_n_findings: int | None = None,
    top_n_reports: int | None = None,
    top_n_messages: int | None = None,
) -> dict:
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if format not in {"full", "archival"}:
        return _json_response({"ok": False, "error": "Invalid format. Valid: archival, full."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        lane = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if lane is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        requested_fields = _parse_projection_fields(fields)
        if format == "archival":
            summary = _build_archival_lane_activity_summary(
                conn,
                task_ref=resolved_task_ref,
                lane_id=normalized_lane_id,
            )
            valid_sections = frozenset({"identity", "lane", "summary"})
            requested_sections = _parse_sections(sections, valid_sections)
            if sections is not None and requested_sections == frozenset():
                return _json_response(_invalid_sections_error(valid_sections))
            archival_sections: frozenset[str] = requested_sections or valid_sections
            archival_payload: dict[str, object] = {"ok": True, "task_ref": resolved_task_ref, "format": format}
            if "identity" in archival_sections or "lane" in archival_sections:
                archival_payload["lane"] = _project_mapping(
                    dict(lane), requested_fields, _LANE_ACTIVITY_LANE_IDENTITY_FIELDS
                )
            if "summary" in archival_sections:
                archival_payload["summary"] = summary
            return _json_response(archival_payload)

        detail = _normalize_read_detail(detail)
        valid_sections = frozenset(
            {"identity", "lane", "decisions", "tests", "blockers", "actions", "findings", "reports", "messages"}
        )
        requested_sections = _parse_sections(sections, valid_sections)
        if sections is not None and requested_sections == frozenset():
            return _json_response(_invalid_sections_error(valid_sections))
        activity_sections: frozenset[str] = requested_sections or valid_sections
        activity_payload: dict[str, object] = {"ok": True, "task_ref": resolved_task_ref, "format": format}
        if "identity" in activity_sections or "lane" in activity_sections:
            lane_row = _summarize_generic_row(dict(lane)) if detail == "summary" else dict(lane)
            activity_payload["lane"] = _project_mapping(lane_row, requested_fields, _LANE_ACTIVITY_LANE_IDENTITY_FIELDS)

        section_fetchers: dict[str, Callable[[], list[dict[str, object]]]] = {
            "decisions": lambda: _fetch_handoff_rows(
                conn,
                table="decisions",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_decisions, top_n_decisions),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "tests": lambda: _fetch_handoff_rows(
                conn,
                table="verified_tests",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="verified_at DESC, id DESC",
                limit=_effective_limit(limit_tests, top_n_tests),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "blockers": lambda: _fetch_handoff_rows(
                conn,
                table="blockers",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_blockers, top_n_blockers),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "actions": lambda: _fetch_handoff_rows(
                conn,
                table="next_actions",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="updated_at DESC, id DESC",
                limit=_effective_limit(limit_actions, top_n_actions),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "findings": lambda: _fetch_handoff_rows(
                conn,
                table="review_findings",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="COALESCE(updated_at, created_at) DESC, id DESC",
                limit=_effective_limit(limit_findings, top_n_findings),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "reports": lambda: _fetch_handoff_rows(
                conn,
                table="worker_reports",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="created_at DESC, id DESC",
                limit=_effective_limit(limit_reports, top_n_reports),
                params=(resolved_task_ref, normalized_lane_id),
            ),
            "messages": lambda: _fetch_handoff_rows(
                conn,
                table="lane_messages",
                where_sql="task_ref = ? AND lane_id = ?",
                order_sql="updated_at DESC, id DESC",
                limit=_effective_limit(limit_messages, top_n_messages),
                params=(resolved_task_ref, normalized_lane_id),
            ),
        }

        section_specs: tuple[tuple[str, frozenset[str], Callable[[dict[str, object]], dict[str, object]]], ...] = (
            ("decisions", _LANE_ACTIVITY_DECISION_IDENTITY_FIELDS, _summarize_generic_row),
            ("tests", _LANE_ACTIVITY_TEST_IDENTITY_FIELDS, _summarize_generic_row),
            ("blockers", _LANE_ACTIVITY_BLOCKER_IDENTITY_FIELDS, _summarize_generic_row),
            ("actions", _LANE_ACTIVITY_ACTION_IDENTITY_FIELDS, _summarize_generic_row),
            ("findings", _LANE_ACTIVITY_FINDING_IDENTITY_FIELDS, _summarize_generic_row),
            ("reports", _WORKER_REPORT_IDENTITY_FIELDS, _summarize_worker_report_row),
            ("messages", _LANE_MESSAGE_IDENTITY_FIELDS, _summarize_lane_message_row),
        )
        for section_name, identity_fields, summary_fn in section_specs:
            if section_name not in activity_sections:
                continue
            rows = section_fetchers[section_name]()
            shaped_rows: list[dict[str, object]] = []
            for row in rows:
                summarized = summary_fn(row) if detail == "summary" else dict(row)
                shaped_rows.append(_project_mapping(summarized, requested_fields, identity_fields))
            activity_payload[section_name] = shaped_rows
        return _json_response(activity_payload)


def get_latest_slice_review_packet(
    task_ref: str | None = None,
    lane_id: str | None = None,
    review_kind: str | None = None,
    slice_decision_id: str | None = None,
    slice_label: str | None = None,
) -> dict:
    """Return one slice review packet for ``task_ref`` (latest by default).

    ``slice_decision_id`` selects a specific historical slice and matches the
    ``decision`` id **string** (e.g. ``cdx_slice_complete_<work>_<slug>``) — NOT
    the numeric ``decision_id`` returned inside a packet. Pass the projected
    ``decision`` value from ``search_handoff(decision_fields=["decision"])``;
    passing the numeric id resolves nothing and returns ``ok=False`` (it does
    not silently fall back to the latest slice). ``slice_label`` is the
    alternative selector; supplying both is rejected. With neither selector the
    latest ``slice_complete_*`` packet is returned.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_review_kind = _normalize_optional_text(review_kind)
    normalized_slice_decision_id = _normalize_optional_text(slice_decision_id)
    normalized_slice_label = _normalize_optional_text(slice_label)
    if normalized_review_kind is not None and normalized_review_kind not in REVIEW_KINDS:
        valid_review_kinds = ", ".join(sorted(REVIEW_KINDS))
        return _json_response({"ok": False, "error": f"Invalid review_kind. Valid: {valid_review_kinds}."})
    if normalized_slice_decision_id is not None and normalized_slice_label is not None:
        return _json_response(
            {
                "ok": False,
                "error": "Provide only one of slice_decision_id or slice_label, not both.",
            }
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        from .orchestration.slice_review_packet import get_latest_slice_review_packet_data  # noqa: PLC0415

        packet = get_latest_slice_review_packet_data(
            conn,
            workspace_root=_workspace_root(),
            task_ref=resolved_task_ref,
            lane_id=normalized_lane_id,
            review_kind=normalized_review_kind,
            slice_decision_id=normalized_slice_decision_id,
            slice_label=normalized_slice_label,
        )
        if packet is None:
            return _json_response(
                {
                    "ok": False,
                    "error": "No matching slice review packet found.",
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "review_kind": normalized_review_kind,
                    "slice_decision_id": normalized_slice_decision_id,
                    "slice_label": normalized_slice_label,
                }
            )
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "review_kind": normalized_review_kind or packet["review_kind"],
                "slice_decision_id": normalized_slice_decision_id,
                "slice_label": normalized_slice_label,
                "packet": packet,
            }
        )


def turn_metrics(
    operation: str,
    session: str | None = None,
    phase: str | None = None,
    backend: str | None = None,
    cycle: int | None = None,
    lane_id: str | None = None,
    model: str | None = None,
    thread_id: str | None = None,
    turn_id: str | None = None,
    token_usage: TokenUsage | None = None,
    prompt_metrics: PromptMetrics | None = None,
    attribution: dict[str, Any] | None = None,
    section_sizes: dict[str, Any] | None = None,
    raw_usage: dict[str, Any] | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_turn_metrics: int | None = None,
) -> dict:
    """Discriminated wrapper for turn metric record, list, and summary operations."""
    valid_operations = {"list", "record", "summary"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "record":
        return record_turn_metric(
            session=str(session or ""),
            phase=str(phase or ""),
            backend=str(backend or ""),
            cycle=cycle,
            lane_id=lane_id,
            model=model,
            thread_id=thread_id,
            turn_id=turn_id,
            token_usage=token_usage,
            prompt_metrics=prompt_metrics,
            attribution=attribution,
            section_sizes=section_sizes,
            raw_usage=raw_usage,
            actor=actor,
            task_ref=task_ref,
        )
    if operation == "list":
        return list_turn_metrics(
            task_ref=task_ref,
            lane_id=lane_id,
            backend=backend,
            model=model,
            phase=phase,
            limit=limit,
            offset=offset,
            sections=sections,
            detail=detail,
            fields=fields,
            top_n_turn_metrics=top_n_turn_metrics,
        )
    return get_turn_metrics_summary(task_ref=task_ref, lane_id=lane_id)


def record_worker_report(
    lane_id: str,
    session: str,
    summary: str,
    changed_files: list[str] | None = None,
    test_commands: list[str] | None = None,
    blockers: list[str] | None = None,
    merge_ready: bool = False,
    status: str = "submitted",
    outcome: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_statuses = REPORT_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_outcome = _normalize_optional_text(outcome)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    if normalized_outcome is not None and normalized_outcome not in WORKER_REPORT_OUTCOMES:
        return _json_response(
            {"ok": False, "error": f"Invalid outcome. Valid: {', '.join(sorted(WORKER_REPORT_OUTCOMES))}"}
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        cur = conn.execute(
            """
            INSERT INTO worker_reports (
                task_ref, lane_id, session, summary, changed_files_json, test_commands_json, blockers_json,
                merge_ready, status, outcome, agent, branch, commit_sha, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                session,
                summary,
                json.dumps(changed_files or []),
                json.dumps(test_commands or []),
                json.dumps(blockers or []),
                1 if merge_ready else 0,
                status,
                normalized_outcome,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
        row = _row_to_dict(conn.execute("SELECT * FROM worker_reports WHERE id = ?", (cur.lastrowid,)).fetchone())
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _json_response({"ok": True, "report": row})


def acknowledge_worker_report(
    lane_id: str,
    report_id: int,
    status: str,
    task_ref: str | None = None,
) -> dict:
    """CAS-transition a worker_report ``status`` out of ``submitted``.

    First UPDATE path on ``worker_reports`` (``record_worker_report`` is INSERT-only).
    Writes only ``status`` — never ``outcome`` ([DATA-14]). Compare-and-set predicate
    is ``WHERE id=? AND lane_id=? AND status='submitted'`` so concurrent ack/insert
    cannot clobber a terminal row ([CON-11]); re-acking matches 0 rows = no-op ([RES-01]).
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    try:
        normalized_report_id = int(report_id)
    except (TypeError, ValueError):
        return _json_response({"ok": False, "error": "report_id must be an integer."})
    if status not in REPORT_ACK_STATUSES:
        return _json_response(
            {"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(REPORT_ACK_STATUSES))}"}
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = conn.execute(
            """
            SELECT * FROM worker_reports
            WHERE id = ? AND task_ref = ? AND lane_id = ?
            """,
            (normalized_report_id, resolved_task_ref, normalized_lane_id),
        ).fetchone()
        if existing is None:
            return _json_response(
                {
                    "ok": False,
                    "error": "Worker report not found for task_ref/lane_id.",
                    "updated": False,
                }
            )
        cur = conn.execute(
            """
            UPDATE worker_reports
            SET status = ?
            WHERE id = ? AND task_ref = ? AND lane_id = ? AND status = 'submitted'
            """,
            (status, normalized_report_id, resolved_task_ref, normalized_lane_id),
        )
        updated = int(cur.rowcount or 0) > 0
        row = _row_to_dict(
            conn.execute(
                "SELECT * FROM worker_reports WHERE id = ?",
                (normalized_report_id,),
            ).fetchone()
        )
        if updated:
            _write_current_task_md_for_task(conn, resolved_task_ref)
        return _json_response(
            {
                "ok": True,
                "updated": updated,
                "report": row,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
            }
        )


def consume_lane_worker_reports(
    lane_id: str,
    *,
    report_id: int | None = None,
    task_ref: str | None = None,
) -> dict:
    """Close-cycle consumer: acknowledge the merge-ready report; supersede other submitted rows.

    Idempotent — already-terminal rows are CAS no-ops. Does not write ``outcome``.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        submitted = list(
            conn.execute(
                """
                SELECT id, merge_ready, created_at
                FROM worker_reports
                WHERE task_ref = ? AND lane_id = ? AND status = 'submitted'
                ORDER BY created_at DESC, id DESC
                """,
                (resolved_task_ref, normalized_lane_id),
            ).fetchall()
        )
        if not submitted:
            return _json_response(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "acknowledged": [],
                    "superseded": [],
                    "noop": True,
                }
            )

    ack_target_id: int | None = None
    if report_id is not None:
        try:
            want = int(report_id)
        except (TypeError, ValueError):
            return _json_response({"ok": False, "error": "report_id must be an integer."})
        if any(int(row["id"]) == want for row in submitted):
            ack_target_id = want
    if ack_target_id is None:
        for row in submitted:
            if int(row["merge_ready"] or 0) == 1:
                ack_target_id = int(row["id"])
                break

    acknowledged: list[int] = []
    superseded: list[int] = []
    for row in submitted:
        rid = int(row["id"])
        if ack_target_id is not None and rid == ack_target_id:
            result = acknowledge_worker_report(
                lane_id=normalized_lane_id,
                report_id=rid,
                status="acknowledged",
                task_ref=resolved_task_ref,
            )
            if result.get("updated"):
                acknowledged.append(rid)
        else:
            result = acknowledge_worker_report(
                lane_id=normalized_lane_id,
                report_id=rid,
                status="superseded",
                task_ref=resolved_task_ref,
            )
            if result.get("updated"):
                superseded.append(rid)
    return _json_response(
        {
            "ok": True,
            "task_ref": resolved_task_ref,
            "lane_id": normalized_lane_id,
            "acknowledged": acknowledged,
            "superseded": superseded,
            "noop": not acknowledged and not superseded,
        }
    )


def backfill_worker_report_acks(
    *,
    task_ref: str | None = None,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict:
    """Mark stranded ``status='submitted'`` worker_reports as ``acknowledged``.

    Historical rows never left ``submitted`` because no consumer existed. CAS-guarded
    per row so concurrent acks and re-runs are safe ([RES-01]). Empty backlog is a
    successful no-op.
    """
    with _get_db_connection() as conn:
        params: list[object] = []
        where = "status = 'submitted'"
        if task_ref is not None:
            resolved = _resolve_task_ref(conn, task_ref)
            where += " AND task_ref = ?"
            params.append(resolved)
        else:
            resolved = None
        sql = f"SELECT id, task_ref, lane_id FROM worker_reports WHERE {where} ORDER BY created_at ASC, id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = list(conn.execute(sql, tuple(params)).fetchall())

    if dry_run:
        return _json_response(
            {
                "ok": True,
                "dry_run": True,
                "would_update": len(rows),
                "updated": 0,
                "noop": len(rows) == 0,
                "task_ref": resolved,
            }
        )

    updated = 0
    for row in rows:
        result = acknowledge_worker_report(
            lane_id=str(row["lane_id"]),
            report_id=int(row["id"]),
            status="acknowledged",
            task_ref=str(row["task_ref"]),
        )
        if result.get("updated"):
            updated += 1
    return _json_response(
        {
            "ok": True,
            "dry_run": False,
            "would_update": len(rows),
            "updated": updated,
            "noop": updated == 0,
            "task_ref": resolved,
        }
    )


def list_worker_reports(
    task_ref: str | None = None,
    lane_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_reports: int | None = None,
) -> dict:
    limit = _effective_limit(limit, top_n_reports)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        total, rows = _paginated_query(
            conn, "worker_reports", where_sql, tuple(params), limit, offset, "created_at DESC, id DESC"
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "reports": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="reports",
                identity_fields=_WORKER_REPORT_IDENTITY_FIELDS,
                summary_fn=_summarize_worker_report_row,
            )
        )


def worker_reports(
    operation: str,
    lane_id: str | None = None,
    session: str | None = None,
    summary: str | None = None,
    changed_files: list[str] | None = None,
    test_commands: list[str] | None = None,
    blockers: list[str] | None = None,
    merge_ready: bool = False,
    status: str | None = None,
    outcome: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    limit: int = 20,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_reports: int | None = None,
    report_id: int | None = None,
) -> dict:
    """Discriminated wrapper for worker report record, list, and acknowledge operations."""
    valid_operations = {"list", "record", "acknowledge", "consume", "backfill_acks"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "record":
        return record_worker_report(
            lane_id=str(lane_id or ""),
            session=str(session or ""),
            summary=str(summary or ""),
            changed_files=changed_files,
            test_commands=test_commands,
            blockers=blockers,
            merge_ready=merge_ready,
            status=status or "submitted",
            outcome=outcome,
            task_ref=task_ref,
            actor=actor,
        )
    if operation == "acknowledge":
        return acknowledge_worker_report(
            lane_id=str(lane_id or ""),
            report_id=int(report_id) if report_id is not None else -1,
            status=status or "acknowledged",
            task_ref=task_ref,
        )
    if operation == "consume":
        return consume_lane_worker_reports(
            lane_id=str(lane_id or ""),
            report_id=report_id,
            task_ref=task_ref,
        )
    if operation == "backfill_acks":
        # Full backlog by default; ``limit`` from the list default is not applied.
        return backfill_worker_report_acks(task_ref=task_ref, dry_run=False, limit=None)
    return list_worker_reports(
        task_ref=task_ref,
        lane_id=lane_id,
        limit=limit,
        offset=offset,
        sections=sections,
        detail=detail,
        fields=fields,
        top_n_reports=top_n_reports,
    )


def record_lane_message(
    lane_id: str,
    session: str,
    direction: str,
    message: str,
    subject: str | None = None,
    status: str = "open",
    payload: dict[str, object] | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_directions = LANE_MESSAGE_DIRECTIONS
    valid_statuses = MESSAGE_STATUSES
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _json_response({"ok": False, "error": "lane_id is required."})
    if direction not in valid_directions:
        return _json_response(
            {"ok": False, "error": f"Invalid direction. Valid: {', '.join(sorted(valid_directions))}"}
        )
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    normalized_payload, payload_error = _normalize_lane_message_payload(payload)
    if payload_error is not None:
        return _json_response({"ok": False, "error": payload_error})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        if _get_lane_row(conn, resolved_task_ref, normalized_lane_id) is None:
            return _json_response({"ok": False, "error": "Lane not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        dispatch_id = normalized_payload.get("dispatch_id") if normalized_payload is not None else None
        duplicate_dispatch = False
        try:
            cur = conn.execute(
                """
                INSERT INTO lane_messages (task_ref, lane_id, session, direction, subject, message, status, dispatch_id, payload_json, agent, branch, commit_sha, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (
                    resolved_task_ref,
                    normalized_lane_id,
                    session,
                    direction,
                    subject,
                    message,
                    status,
                    dispatch_id,
                    json.dumps(normalized_payload, sort_keys=True) if normalized_payload is not None else None,
                    ctx.agent,
                    ctx.branch,
                    ctx.commit_sha,
                ),
            )
            message_id = cur.lastrowid
        except sqlite3.IntegrityError:
            # HARM-A-005: the idx_lane_messages_dispatch_id unique index makes a
            # repeated (task_ref, lane_id, dispatch_id) a no-op replay, not a crash.
            # Mirror dispatch_lane_work: return the existing row with a marker.
            if dispatch_id is None:
                raise
            duplicate_dispatch = True
            existing = conn.execute(
                "SELECT id FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND dispatch_id = ?",
                (resolved_task_ref, normalized_lane_id, dispatch_id),
            ).fetchone()
            message_id = existing[0] if existing is not None else None
        row = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        if row is not None:
            row = _decode_lane_message_row_dict(row)
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _json_response({"ok": True, "message": row, "duplicate_dispatch": duplicate_dispatch})


def record_lane_brief(
    lane_id: str,
    session: str,
    source_lane: str,
    reason: str,
    summary: str,
    message: str | None = None,
    required_actions: list[str] | None = None,
    artifacts: list[str] | None = None,
    status: str = "open",
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    normalized_reason = _normalize_optional_text(reason)
    normalized_summary = _normalize_optional_text(summary)
    normalized_source_lane = _normalize_optional_text(source_lane)
    if normalized_reason is None:
        return _json_response({"ok": False, "error": "reason is required."})
    if normalized_summary is None:
        return _json_response({"ok": False, "error": "summary is required."})
    if normalized_source_lane is None:
        return _json_response({"ok": False, "error": "source_lane is required."})
    brief_payload: dict[str, object] = {
        "source_lane": normalized_source_lane,
        "reason": normalized_reason,
        "summary": normalized_summary,
    }
    if required_actions:
        brief_payload["required_actions"] = [
            item for item in required_actions if isinstance(item, str) and item.strip()
        ]
    if artifacts:
        brief_payload["artifacts"] = [item for item in artifacts if isinstance(item, str) and item.strip()]
    return record_lane_message(
        lane_id=lane_id,
        session=session,
        direction="orchestrator_to_worker",
        subject=f"brief:{normalized_reason}",
        message=(message or normalized_summary),
        status=status,
        payload=brief_payload,
        task_ref=task_ref,
        actor=actor,
    )


def update_lane_message(
    message_id: int,
    status: str,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    valid_statuses = MESSAGE_STATUSES
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            "SELECT * FROM lane_messages WHERE id = ? AND task_ref = ?", (message_id, resolved_task_ref)
        ).fetchone()
        if row is None:
            return _json_response({"ok": False, "error": "Message not found for task_ref."})
        ctx = _resolve_write_actor(conn, actor)
        conn.execute(
            "UPDATE lane_messages SET status = ?, agent = COALESCE(agent, ?), branch = COALESCE(branch, ?), commit_sha = COALESCE(commit_sha, ?), updated_at = datetime('now') WHERE id = ? AND task_ref = ?",
            (status, ctx.agent, ctx.branch, ctx.commit_sha, message_id, resolved_task_ref),
        )
        updated = _row_to_dict(conn.execute("SELECT * FROM lane_messages WHERE id = ?", (message_id,)).fetchone())
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _json_response({"ok": True, "message": updated})


def list_lane_messages(
    task_ref: str | None = None,
    lane_id: str | None = None,
    status: str = "all",
    limit: int = 20,
    offset: int = 0,
    direction: str | None = None,
    subject_prefix: str | None = None,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_messages: int | None = None,
) -> dict:
    """List lane messages with optional scope and content filters.

    ``direction`` restricts to a specific message direction (e.g. ``"orchestrator_to_worker"``).
    ``subject_prefix`` restricts to messages whose subject starts with the given prefix
    (e.g. ``"brief:"``), making this function capable of subsuming ``list_lane_briefs``.
    """
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    limit = _effective_limit(limit, top_n_messages)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        inferred_lane = None
        if normalized_lane_id is None:
            inferred_lane_row = _resolve_current_lane_row(conn, resolved_task_ref)
            if inferred_lane_row is not None:
                normalized_lane_id = str(inferred_lane_row["lane_id"])
                # _resolve_current_lane_row already returns a dict; copy it (matching the
                # prior _row_to_dict semantics) instead of re-converting a non-Row.
                inferred_lane = dict(inferred_lane_row)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if direction is not None:
            where_sql += " AND direction = ?"
            params.append(direction)
        if subject_prefix is not None:
            where_sql += " AND subject LIKE ? ESCAPE '\\'"
            params.append(f"{_escape_like(subject_prefix)}%")
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            limit,
            offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "current_lane": inferred_lane,
                    "status": status,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "messages": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="messages",
                identity_fields=_LANE_MESSAGE_IDENTITY_FIELDS,
                summary_fn=_summarize_lane_message_row,
            )
        )


def list_lane_briefs(
    task_ref: str | None = None, lane_id: str | None = None, status: str = "open", limit: int = 20, offset: int = 0
) -> dict:
    valid_statuses = {"all", *MESSAGE_STATUSES}
    if status not in valid_statuses:
        return _json_response({"ok": False, "error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"})
    limit = max(1, limit)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref, "orchestrator_to_worker", "brief:%"]
        where_sql = "task_ref = ? AND direction = ? AND subject LIKE ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total, rows = _paginated_query(
            conn,
            "lane_messages",
            where_sql,
            tuple(params),
            limit,
            offset,
            "updated_at DESC, id DESC",
            _decode_lane_message_row_dict,
        )
        return _json_response(
            {
                "ok": True,
                "task_ref": resolved_task_ref,
                "lane_id": normalized_lane_id,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": offset + len(rows) < total,
                "briefs": rows,
            }
        )


def lane_communication(
    kind: str,
    operation: str,
    lane_id: str | None = None,
    session: str | None = None,
    direction: str | None = None,
    message: str | None = None,
    subject: str | None = None,
    status: str = "open",
    payload: dict[str, object] | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
    source_lane: str | None = None,
    reason: str | None = None,
    summary: str | None = None,
    required_actions: list[str] | None = None,
    artifacts: list[str] | None = None,
    message_id: int | None = None,
    limit: int = 20,
    offset: int = 0,
    subject_prefix: str | None = None,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_messages: int | None = None,
) -> dict:
    """Discriminated wrapper for lane message and brief operations."""
    valid_kinds = {"message", "brief"}
    valid_operations = {"record", "update", "list"}
    if kind not in valid_kinds:
        return _json_response({"ok": False, "error": f"Invalid kind. Valid: {', '.join(sorted(valid_kinds))}"})
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )

    if operation == "record":
        if kind == "message":
            return record_lane_message(
                lane_id=str(lane_id or ""),
                session=str(session or ""),
                direction=str(direction or ""),
                message=str(message or ""),
                subject=subject,
                status=status,
                payload=payload,
                task_ref=task_ref,
                actor=actor,
            )
        return record_lane_brief(
            lane_id=str(lane_id or ""),
            session=str(session or ""),
            source_lane=str(source_lane or ""),
            reason=str(reason or ""),
            summary=str(summary or ""),
            message=message,
            required_actions=required_actions,
            artifacts=artifacts,
            status=status,
            task_ref=task_ref,
            actor=actor,
        )

    if operation == "update":
        if message_id is None:
            return _json_response({"ok": False, "error": "message_id is required for update."})
        return update_lane_message(
            message_id=message_id,
            status=status,
            task_ref=task_ref,
            actor=actor,
        )

    if kind == "message":
        return list_lane_messages(
            task_ref=task_ref,
            lane_id=lane_id,
            status=status,
            limit=limit,
            offset=offset,
            direction=direction,
            subject_prefix=subject_prefix,
            sections=sections,
            detail=detail,
            fields=fields,
            top_n_messages=top_n_messages,
        )
    return list_lane_briefs(
        task_ref=task_ref,
        lane_id=lane_id,
        status=status,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Plan cursor CRUD (moved from workbay-handoff-mcp/core.py in internal)
# ---------------------------------------------------------------------------


def _evaluate_clean_slice_gate(
    conn: "sqlite3.Connection",
    task_ref: str,
    lane_id: str | None,
    since: str | None,
) -> dict | None:
    """Check clean-slice preconditions. Returns error payload dict or None if clean."""
    from workbay_handoff_mcp.enums import FindingSeverity, FindingStatus  # noqa: PLC0415

    open_high_query = [
        "SELECT COUNT(*) AS count FROM review_findings WHERE task_ref = ? AND status = ? AND severity = ?"
    ]
    open_high_params: list[object] = [task_ref, FindingStatus.OPEN, FindingSeverity.HIGH]
    if lane_id is not None:
        open_high_query.append("AND lane_id = ?")
        open_high_params.append(lane_id)
    open_high_count = int(conn.execute(" ".join(open_high_query), tuple(open_high_params)).fetchone()["count"])
    test_query = ["SELECT COUNT(*) AS count FROM verified_tests WHERE task_ref = ?"]
    test_params: list[object] = [task_ref]
    if since is not None:
        test_query.append("AND verified_at >= ?")
        test_params.append(since)
    fresh_test_count = int(conn.execute(" ".join(test_query), tuple(test_params)).fetchone()["count"])
    missing_gates: list[str] = []
    if open_high_count > 0:
        missing_gates.append("open_high_findings")
    if fresh_test_count == 0:
        missing_gates.append("missing_recent_test")
    if not missing_gates:
        return None
    return {
        "ok": False,
        "error": "require_clean_slice gate failed.",
        "missing_gates": missing_gates,
        "gate": {
            "require_clean_slice": True,
            "lane_scope": lane_id,
            "task_ref": task_ref,
            "open_high_count": open_high_count,
            "fresh_test_count": fresh_test_count,
            "tests_since": since,
        },
    }


def upsert_plan_cursor(
    plan_item_id: str,
    state: str,
    lane_id: str | None = None,
    mcp_action_id: int | None = None,
    worker_message_id: int | None = None,
    source_heading: str | None = None,
    summary: str | None = None,
    task_ref: str | None = None,
    require_clean_slice: bool = False,
) -> dict:
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    valid_states = frozenset(
        {
            PlanCursorState.DISPATCHED,
            PlanCursorState.COMPLETED,
            PlanCursorState.SKIPPED,
            PlanCursorState.ESCALATED,
        }
    )
    normalized_plan_item_id = _normalize_optional_text(plan_item_id)
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_heading = _normalize_optional_text(source_heading)
    normalized_summary = _normalize_optional_text(summary)
    if normalized_plan_item_id is None:
        return _json_response({"ok": False, "error": "plan_item_id is required."})
    if state not in valid_states:
        return _json_response({"ok": False, "error": f"Invalid state. Valid: {', '.join(sorted(valid_states))}"})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = conn.execute(
            "SELECT * FROM plan_cursors WHERE task_ref = ? AND plan_item_id = ?",
            (resolved_task_ref, normalized_plan_item_id),
        ).fetchone()
        if existing is None and normalized_summary is None:
            return _json_response({"ok": False, "error": "summary is required when creating a new plan cursor."})
        next_lane_id = (
            (normalized_lane_id or _normalize_optional_text(existing["lane_id"]))
            if existing is not None
            else normalized_lane_id
        )
        if require_clean_slice:
            since_value = existing["updated_at"] if existing is not None else None
            gate_failure = _evaluate_clean_slice_gate(conn, resolved_task_ref, next_lane_id, since_value)
            if gate_failure is not None:
                return _json_response(gate_failure)
        if existing is None:
            cur = conn.execute(
                """
                INSERT INTO plan_cursors (
                    task_ref, plan_item_id, state, lane_id, mcp_action_id, worker_message_id,
                    source_heading, summary, dispatch_count, dispatched_at, completed_at, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    CASE WHEN ? = ? THEN 1 ELSE 0 END,
                    CASE WHEN ? = ? THEN datetime('now') ELSE NULL END,
                    CASE WHEN ? = ? THEN datetime('now') ELSE NULL END,
                    datetime('now'), datetime('now')
                )
                """,
                (
                    resolved_task_ref,
                    normalized_plan_item_id,
                    state,
                    normalized_lane_id,
                    mcp_action_id,
                    worker_message_id,
                    normalized_heading,
                    normalized_summary,
                    state,
                    PlanCursorState.DISPATCHED,
                    state,
                    PlanCursorState.DISPATCHED,
                    state,
                    PlanCursorState.COMPLETED,
                ),
            )
            row = _row_to_dict(conn.execute("SELECT * FROM plan_cursors WHERE id = ?", (cur.lastrowid,)).fetchone())
            return _json_response({"ok": True, "cursor": row})
        next_summary = normalized_summary or str(existing["summary"])
        next_heading = normalized_heading or _normalize_optional_text(existing["source_heading"])
        next_action_id = mcp_action_id if mcp_action_id is not None else existing["mcp_action_id"]
        next_worker_message_id = worker_message_id if worker_message_id is not None else existing["worker_message_id"]
        dispatch_count = int(existing["dispatch_count"] or 0) + (1 if state == PlanCursorState.DISPATCHED else 0)
        conn.execute(
            """
            UPDATE plan_cursors
            SET state = ?, lane_id = ?, mcp_action_id = ?, worker_message_id = ?,
                source_heading = ?, summary = ?, dispatch_count = ?,
                dispatched_at = CASE WHEN ? = ? THEN datetime('now') ELSE dispatched_at END,
                completed_at = CASE WHEN ? = ? THEN datetime('now') ELSE completed_at END,
                updated_at = datetime('now')
            WHERE task_ref = ? AND plan_item_id = ?
            """,
            (
                state,
                next_lane_id,
                next_action_id,
                next_worker_message_id,
                next_heading,
                next_summary,
                dispatch_count,
                state,
                PlanCursorState.DISPATCHED,
                state,
                PlanCursorState.COMPLETED,
                resolved_task_ref,
                normalized_plan_item_id,
            ),
        )
        row = _row_to_dict(
            conn.execute(
                "SELECT * FROM plan_cursors WHERE task_ref = ? AND plan_item_id = ?",
                (resolved_task_ref, normalized_plan_item_id),
            ).fetchone()
        )
        return _json_response({"ok": True, "cursor": row})


def get_plan_cursor(plan_item_id: str, task_ref: str | None = None) -> dict:
    normalized_plan_item_id = _normalize_optional_text(plan_item_id)
    if normalized_plan_item_id is None:
        return _json_response({"ok": False, "error": "plan_item_id is required."})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = conn.execute(
            "SELECT * FROM plan_cursors WHERE task_ref = ? AND plan_item_id = ?",
            (resolved_task_ref, normalized_plan_item_id),
        ).fetchone()
        return _json_response({"ok": True, "task_ref": resolved_task_ref, "cursor": _row_to_dict(row)})


def list_plan_cursors(
    task_ref: str | None = None,
    state: str = "all",
    lane_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_cursors: int | None = None,
) -> dict:
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    valid_states = frozenset(
        {
            "all",
            PlanCursorState.DISPATCHED,
            PlanCursorState.COMPLETED,
            PlanCursorState.SKIPPED,
            PlanCursorState.ESCALATED,
        }
    )
    if state not in valid_states:
        return _json_response({"ok": False, "error": f"Invalid state. Valid: {', '.join(sorted(valid_states))}"})
    limit = _effective_limit(limit, top_n_cursors)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if state != "all":
            where_sql += " AND state = ?"
            params.append(state)
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        total, rows = _paginated_query(
            conn, "plan_cursors", where_sql, tuple(params), limit, offset, "updated_at DESC, id DESC"
        )
        return _json_response(
            _shape_list_payload(
                {
                    "ok": True,
                    "task_ref": resolved_task_ref,
                    "lane_id": normalized_lane_id,
                    "state": state,
                    "total_matching": total,
                    "returned": len(rows),
                    "has_more": offset + len(rows) < total,
                    "cursors": rows,
                },
                sections=sections,
                detail=detail,
                fields=fields,
                row_key="cursors",
                identity_fields=_PLAN_CURSOR_IDENTITY_FIELDS,
                summary_fn=_summarize_generic_row,
            )
        )


def plan_cursor(
    operation: str,
    plan_item_id: str | None = None,
    state: str | None = None,
    lane_id: str | None = None,
    mcp_action_id: int | None = None,
    worker_message_id: int | None = None,
    source_heading: str | None = None,
    summary: str | None = None,
    task_ref: str | None = None,
    require_clean_slice: bool = False,
    limit: int = 50,
    offset: int = 0,
    sections: str | None = None,
    detail: str = "full",
    fields: str | None = None,
    top_n_cursors: int | None = None,
) -> dict:
    """Discriminated wrapper for plan cursor upsert, get, and list operations."""
    valid_operations = {"get", "list", "upsert"}
    if operation not in valid_operations:
        return _json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation == "upsert":
        return upsert_plan_cursor(
            plan_item_id=str(plan_item_id or ""),
            state=str(state or ""),
            lane_id=lane_id,
            mcp_action_id=mcp_action_id,
            worker_message_id=worker_message_id,
            source_heading=source_heading,
            summary=summary,
            task_ref=task_ref,
            require_clean_slice=require_clean_slice,
        )
    if operation == "get":
        return get_plan_cursor(plan_item_id=str(plan_item_id or ""), task_ref=task_ref)
    return list_plan_cursors(
        task_ref=task_ref,
        state=state or "all",
        lane_id=lane_id,
        limit=limit,
        offset=offset,
        sections=sections,
        detail=detail,
        fields=fields,
        top_n_cursors=top_n_cursors,
    )


# ---------------------------------------------------------------------------
# internal — blocked-lane aging report + conclusive-close
# ---------------------------------------------------------------------------


def _parse_sqlite_utc(ts: object) -> datetime | None:
    """Parse SQLite ``datetime('now')`` style timestamps as UTC-naive or aware."""
    if not isinstance(ts, str) or not ts.strip():
        return None
    raw = ts.strip().replace("T", " ")
    # Drop fractional seconds / trailing Z for fromisoformat friendliness.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if "." in raw and "+" not in raw[10:] and raw.count(":") >= 2:
        # "YYYY-MM-DD HH:MM:SS.ffffff" — keep whole string for fromisoformat
        pass
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw[:19] if len(raw) >= 19 else raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_lane_age_label(
    updated_at: object = None,
    created_at: object = None,
    *,
    now: datetime | None = None,
) -> str:
    """Human age label for a blocked lane (``5d``, ``12h``, ``unknown``)."""
    stamp = _parse_sqlite_utc(updated_at) or _parse_sqlite_utc(created_at)
    if stamp is None:
        return "unknown"
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    delta = current - stamp
    seconds = max(0, int(delta.total_seconds()))
    days = seconds // 86400
    if days >= 1:
        return f"{days}d"
    hours = max(1, seconds // 3600) if seconds >= 3600 else 0
    if hours >= 1:
        return f"{hours}h"
    minutes = max(1, seconds // 60) if seconds >= 60 else 0
    if minutes >= 1:
        return f"{minutes}m"
    return "0m"


def _blocker_text_from_reports(
    *,
    lane_id: str | None,
    task_ref: str | None,
    reports: Sequence[Mapping[str, object]] | None,
) -> str | None:
    if not reports:
        return None
    for report in reports:
        r_lane = _normalize_optional_text(report.get("lane_id"))
        r_task = _normalize_optional_text(report.get("task_ref"))
        if lane_id is not None and r_lane is not None and r_lane != lane_id:
            continue
        if task_ref is not None and r_task is not None and r_task != task_ref:
            continue
        raw = report.get("blockers_json")
        if raw is None:
            raw = report.get("blockers")
        items: list[object]
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = None
            items = parsed if isinstance(parsed, list) else []
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        for item in items:
            if isinstance(item, str) and item.strip():
                return item.strip()
            if isinstance(item, Mapping):
                for key in ("description", "text", "summary", "blocker"):
                    text = _normalize_optional_text(item.get(key))
                    if text is not None:
                        return text
    return None


def _last_blocker_text_for_lane(
    conn: sqlite3.Connection | None,
    *,
    task_ref: str,
    lane_id: str,
    notes: object = None,
    reports: Sequence[Mapping[str, object]] | None = None,
) -> str:
    """Best-effort last open blocker text; degrades to notes / report / placeholder."""
    if conn is not None:
        try:
            row = conn.execute(
                """
                SELECT description FROM blockers
                WHERE status = 'open'
                  AND task_ref = ?
                  AND (lane_id = ? OR lane_id IS NULL OR lane_id = '')
                ORDER BY
                  CASE WHEN lane_id = ? THEN 0 ELSE 1 END,
                  datetime(created_at) DESC,
                  id DESC
                LIMIT 1
                """,
                (task_ref, lane_id, lane_id),
            ).fetchone()
            if row is not None:
                text = _normalize_optional_text(row["description"] if isinstance(row, sqlite3.Row) else row[0])
                if text is not None:
                    return text
        except sqlite3.Error:
            pass
    from_reports = _blocker_text_from_reports(lane_id=lane_id, task_ref=task_ref, reports=reports)
    if from_reports is not None:
        return from_reports
    note = _normalize_optional_text(notes)
    if note is not None:
        return note
    return "(no blocker text)"


def format_blocked_lane_aging_line(entry: Mapping[str, object]) -> str:
    """Single DASHBOARD report line: age + task_ref + last blocker."""
    lane_id = entry.get("lane_id") or entry.get("id") or "?"
    task_ref = entry.get("task_ref") or "?"
    age = entry.get("age") or "unknown"
    blocker = entry.get("blocker") or entry.get("last_blocker") or "(no blocker text)"
    # Keep single-line for dashboard; trim long blocker text.
    blocker_text = str(blocker).replace("\n", " ").strip()
    if len(blocker_text) > 120:
        blocker_text = blocker_text[:117] + "..."
    return f"  ⚠ {lane_id}  task={task_ref}  age={age}  blocker: {blocker_text}"


def collect_blocked_lane_aging_entries(
    lanes: Sequence[Mapping[str, object]],
    *,
    reports: Sequence[Mapping[str, object]] | None = None,
    now: datetime | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, object]]:
    """Build aging report entries for blocked lanes (dashboard + reaper share shape)."""
    entries: list[dict[str, object]] = []
    for lane in lanes:
        if str(lane.get("status") or "") != _LANE_STATUS_BLOCKED:
            continue
        lane_id = _normalize_optional_text(lane.get("lane_id")) or str(lane.get("id") or "?")
        task_ref = _normalize_optional_text(lane.get("task_ref")) or "?"
        age = format_lane_age_label(lane.get("updated_at"), lane.get("created_at"), now=now)
        blocker = _last_blocker_text_for_lane(
            conn,
            task_ref=task_ref,
            lane_id=lane_id,
            notes=lane.get("notes"),
            reports=reports,
        )
        entries.append(
            {
                "id": lane.get("id"),
                "task_ref": task_ref,
                "lane_id": lane_id,
                "status": _LANE_STATUS_BLOCKED,
                "worktree_path": lane.get("worktree_path"),
                "branch": lane.get("branch"),
                "updated_at": lane.get("updated_at"),
                "created_at": lane.get("created_at"),
                "age": age,
                "blocker": blocker,
                "notes": lane.get("notes"),
            }
        )
    return entries


def _probe_worktree_gone(worktree_path: object) -> bool | None:
    """Return True if path is gone, False if present, None if probe unavailable."""
    path = _normalize_optional_text(worktree_path)
    if path is None:
        # Empty worktree path: treat as gone (nothing on disk to recover).
        return True
    try:
        return not Path(path).exists()
    except OSError:
        return None


def _probe_branch_dead(
    branch: object,
    *,
    repo_root: Path | None = None,
) -> bool | None:
    """Return True if branch is deleted or merged into HEAD; False if live; None if unknown.

    Uses workspace git repo when ``repo_root`` is provided. Never raises.
    """
    name = _normalize_optional_text(branch)
    if name is None:
        return None
    # Strip remote-style refs to a bare branch name for local checks.
    ref = name
    if ref.startswith("refs/heads/"):
        ref = ref[len("refs/heads/") :]
    cwd = repo_root
    if cwd is None:
        try:
            cwd = _workspace_root()
        except Exception:  # noqa: BLE001 — probe degrade
            return None
    try:
        cwd_str = str(cwd)
        # Deleted: local branch ref missing.
        show = subprocess.run(
            ["git", "-C", cwd_str, "show-ref", "--verify", "--quiet", f"refs/heads/{ref}"],
            capture_output=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
            check=False,
        )
        if show.returncode != 0:
            return True
        # Merged: branch tip is an ancestor of HEAD.
        merged = subprocess.run(
            ["git", "-C", cwd_str, "merge-base", "--is-ancestor", f"refs/heads/{ref}", "HEAD"],
            capture_output=True,
            timeout=_BRANCH_PROBE_TIMEOUT_S,
            check=False,
        )
        if merged.returncode == 0:
            return True
        if merged.returncode == 1:
            return False
        return None
    except (OSError, subprocess.TimeoutExpired, subprocess.SubprocessError):
        return None


def _classify_blocked_lane_liveness(
    *,
    worktree_gone: bool | None,
    branch_dead: bool | None,
) -> tuple[str, str]:
    """Conclusive-dead only when BOTH probes prove dead; else report-only classes."""
    if worktree_gone is True and branch_dead is True:
        return "dead", "worktree gone and branch merged/deleted"
    if worktree_gone is False and branch_dead is False:
        return "alive", "worktree present and branch still live"
    if worktree_gone is None or branch_dead is None:
        return "ambiguous", "probe unavailable or inconclusive"
    # Exactly one condition proven dead — ambiguous (do NOT close).
    if worktree_gone is True and branch_dead is not True:
        return "ambiguous", "worktree gone but branch not proven merged/deleted"
    if branch_dead is True and worktree_gone is not True:
        return "ambiguous", "branch merged/deleted but worktree still present"
    return "ambiguous", "inconclusive liveness"


def _close_blocked_lane_cas(
    conn: sqlite3.Connection,
    *,
    lane_pk: int,
    probed_updated_at: object,
    note: str,
) -> bool:
    """CAS: ``blocked`` → ``closed_stale`` only if row still blocked with probed updated_at."""
    existing_notes = conn.execute(
        "SELECT notes FROM worktree_lanes WHERE id = ?",
        (lane_pk,),
    ).fetchone()
    prior = ""
    if existing_notes is not None:
        prior_raw = existing_notes["notes"] if isinstance(existing_notes, sqlite3.Row) else existing_notes[0]
        prior = str(prior_raw or "").strip()
    new_notes = f"{prior} [{note}]".strip() if prior else note
    cur = conn.execute(
        """
        UPDATE worktree_lanes
        SET status = ?,
            notes = ?,
            updated_at = datetime('now')
        WHERE id = ?
          AND status = ?
          AND ((updated_at IS NULL AND ? IS NULL) OR updated_at = ?)
        """,
        (
            _LANE_STATUS_CLOSED_STALE,
            new_notes,
            lane_pk,
            _LANE_STATUS_BLOCKED,
            probed_updated_at,
            probed_updated_at,
        ),
    )
    return int(cur.rowcount or 0) == 1


def reap_blocked_lanes(
    *,
    apply: bool = False,
    max_batch: int = DEFAULT_BLOCKED_LANE_REAP_BATCH,
    worktree_probe: Callable[[object], bool | None] | None = None,
    branch_probe: Callable[[object], bool | None] | None = None,
    now: datetime | None = None,
) -> dict:
    """Report blocked-lane age/task/blocker; CAS-close conclusive-dead to ``closed_stale``.

    Conclusive-dead requires **both** worktree-gone **and** branch merged/deleted.
    Ambiguous (only one condition, or probe unavailable) → report only, never close.
    Dry-run by default (``apply=False``). Never raises.
    """
    try:
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        batch = DEFAULT_BLOCKED_LANE_REAP_BATCH

    reported: list[dict[str, object]] = []
    closed: list[dict[str, object]] = []
    would_close: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    alive: list[dict[str, object]] = []
    triage: list[str] = []
    failed: list[dict[str, object]] = []

    path_probe = worktree_probe or _probe_worktree_gone
    br_probe = branch_probe or _probe_branch_dead

    try:
        with _get_db_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, task_ref, lane_id, title, objective, worktree_path, branch,
                       owner_agent, status, notes, created_at, updated_at
                FROM worktree_lanes
                WHERE status = ?
                ORDER BY datetime(COALESCE(updated_at, created_at)) ASC, id ASC
                LIMIT ?
                """,
                (_LANE_STATUS_BLOCKED, batch),
            ).fetchall()

            if not rows:
                return _json_response(
                    {
                        "ok": True,
                        "applied": apply,
                        "max_batch": batch,
                        "reported": [],
                        "closed": [],
                        "would_close": [],
                        "ambiguous": [],
                        "alive": [],
                        "triage": [],
                        "failed": [],
                        "dashboard_lines": [],
                    }
                )

            lane_maps = [dict(row) for row in rows]
            # Pull recent reports once for blocker-text fallback.
            try:
                report_rows = conn.execute(
                    """
                    SELECT task_ref, lane_id, blockers_json, created_at
                    FROM worker_reports
                    ORDER BY created_at DESC, id DESC
                    LIMIT 100
                    """
                ).fetchall()
                reports = [dict(r) for r in report_rows]
            except sqlite3.Error:
                reports = []

            entries = collect_blocked_lane_aging_entries(
                lane_maps,
                reports=reports,
                now=now,
                conn=conn,
            )
            by_pk = {int(cast(int, e["id"])): e for e in entries if e.get("id") is not None}

            for row in rows:
                entry = by_pk.get(int(row["id"]))
                if entry is None:
                    continue
                reported.append(entry)
                try:
                    worktree_gone = path_probe(row["worktree_path"])
                except Exception as exc:  # noqa: BLE001 — per-row degrade
                    worktree_gone = None
                    triage.append(f"lane {entry['lane_id']}: worktree probe raised: {exc}")
                try:
                    branch_dead = br_probe(row["branch"])
                except Exception as exc:  # noqa: BLE001 — per-row degrade
                    branch_dead = None
                    triage.append(f"lane {entry['lane_id']}: branch probe raised: {exc}")

                verdict, reason = _classify_blocked_lane_liveness(
                    worktree_gone=worktree_gone,
                    branch_dead=branch_dead,
                )
                entry = {
                    **entry,
                    "worktree_gone": worktree_gone,
                    "branch_dead": branch_dead,
                    "verdict": verdict,
                    "reason": reason,
                }
                # Refresh reported list item with probe fields.
                reported[-1] = entry

                if verdict == "alive":
                    alive.append(entry)
                    continue
                if verdict != "dead":
                    ambiguous.append(entry)
                    triage.append(
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']} age={entry['age']}: {reason}"
                    )
                    continue

                note = f"closed_stale by blocked-lane reaper: {reason}"
                close_entry = {**entry, "note": note}
                would_close.append(close_entry)
                if not apply:
                    continue
                try:
                    ok = _close_blocked_lane_cas(
                        conn,
                        lane_pk=int(row["id"]),
                        probed_updated_at=row["updated_at"],
                        note=note,
                    )
                except sqlite3.Error as exc:
                    failed.append({**close_entry, "stage": "close", "error": str(exc)})
                    continue
                if ok:
                    closed.append({**close_entry, "status": _LANE_STATUS_CLOSED_STALE})
                else:
                    ambiguous.append({**close_entry, "reason": "CAS miss: row changed since probe"})
                    triage.append(
                        f"blocked lane {entry['lane_id']} task={entry['task_ref']}: CAS miss; re-probe next tick"
                    )
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
        triage.append(f"blocked-lane sweep failed: {exc}")
        return _json_response(
            {
                "ok": True,
                "applied": apply,
                "max_batch": batch,
                "error": str(exc),
                "reported": reported,
                "closed": closed,
                "would_close": would_close,
                "ambiguous": ambiguous,
                "alive": alive,
                "triage": triage,
                "failed": failed,
                "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
            }
        )

    return _json_response(
        {
            "ok": True,
            "applied": apply,
            "max_batch": batch,
            "reported": reported,
            "closed": closed,
            "would_close": would_close,
            "ambiguous": ambiguous,
            "alive": alive,
            "triage": triage,
            "failed": failed,
            "dashboard_lines": [format_blocked_lane_aging_line(e) for e in reported],
        }
    )
