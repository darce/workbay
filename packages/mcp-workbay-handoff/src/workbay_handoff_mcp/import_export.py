"""Import/export domain module.

Contains export_handoff_state, import_handoff_state, archive_task_state,
get_archived_task, update_task_status, and switch_task.
"""

from __future__ import annotations

import dataclasses
import json
import re
import sqlite3
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from workbay_protocol import resolve_env_alias

from .current_task_rendering import (
    TaskSnapshot,
    _build_current_task_state_from_snapshot,
    _collect_task_snapshot,
    _render_current_task_md,
    _write_current_task_md_for_task,
)
from .enums import HandoffStatus
from .git_merge import branch_exists, branch_is_merged
from .shared_db_utils import _count_task_rows, _resolve_output_path
from .shared_primitives import (
    HANDOFF_ACTIVE_STATUSES,
    LIVE_ACTIVE_STATUSES,
    _envelope,
    _normalize_optional_text,
    _resolve_import_lane_id,
    _resolve_import_row_actor,
    _resolve_task_ref,
    _row_to_dict,
    _utcnow_iso,
    _workspace_root,
)
from .shared_schema import _get_db_connection
from .shared_write_context import (
    ResolvedWriteContext,
    WriteActor,
    _detect_git_write_context,
    _resolve_core_override,
    _resolve_write_actor,
    build_write_actor,
    clear_worktree_pointer_for_close,
    collect_target_context_warnings,
    normalize_actor_harness,
)

_internal_TASK_REF_RE = re.compile(r"\bAHMCP-\d+\b")


def _supersede_open_findings_for_archive(conn: sqlite3.Connection, task_ref: str) -> int:
    from .review_findings_recording import _supersede_review_findings

    rows = conn.execute(
        "SELECT id FROM review_findings WHERE task_ref = ? AND status = 'open' ORDER BY id ASC",
        (task_ref,),
    ).fetchall()
    return _supersede_review_findings(
        conn,
        source_ids=[int(row["id"]) for row in rows],
        resolution_note="task_archived",
    )


def _persist_task_archive_snapshot(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    snapshot: TaskSnapshot | Mapping[str, object],
    ctx: ResolvedWriteContext,
    notes: str,
) -> None:
    conn.execute(
        """
        INSERT INTO task_archives (task_ref, archived_at, archived_by, archived_branch, archived_commit_sha, notes, snapshot_json)
        VALUES (?, datetime('now'), ?, ?, ?, ?, ?)
        ON CONFLICT(task_ref) DO UPDATE SET
            archived_at = datetime('now'),
            archived_by = excluded.archived_by,
            archived_branch = excluded.archived_branch,
            archived_commit_sha = excluded.archived_commit_sha,
            notes = excluded.notes,
            snapshot_json = excluded.snapshot_json
        """,
        (
            task_ref,
            ctx.agent,
            ctx.branch,
            ctx.commit_sha,
            notes,
            json.dumps(snapshot, sort_keys=True),
        ),
    )


def _load_test_trace_map(conn: sqlite3.Connection, test_ids: list[int]) -> dict[int, list[str]]:
    if not test_ids:
        return {}
    placeholders = ",".join("?" for _ in test_ids)
    rows = conn.execute(
        f"""
        SELECT verified_test_id, trace
        FROM test_traces
        WHERE verified_test_id IN ({placeholders})
        ORDER BY verified_test_id ASC, trace_order ASC, id ASC
        """,
        tuple(test_ids),
    ).fetchall()
    trace_map: dict[int, list[str]] = {}
    for row in rows:
        trace_map.setdefault(int(row["verified_test_id"]), []).append(str(row["trace"]))
    return trace_map


def _snapshot_with_test_traces(
    conn: sqlite3.Connection, snapshot: TaskSnapshot | Mapping[str, object]
) -> dict[str, object]:
    payload = dict(snapshot)
    raw_tests = payload.get("verified_tests")
    if not isinstance(raw_tests, list):
        return payload
    tests = [dict(row) for row in raw_tests if isinstance(row, Mapping)]
    trace_map = _load_test_trace_map(conn, [int(row["id"]) for row in tests if "id" in row])
    for row in tests:
        row["traces"] = trace_map.get(int(row["id"]), [])
    payload["verified_tests"] = tests
    return payload


def export_handoff_state(
    task_ref: str | None = None, output_path: str | None = None, include_markdown: bool = False
) -> dict:
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        snapshot = _snapshot_with_test_traces(conn, _collect_task_snapshot(conn, resolved_task_ref))
    payload: dict[str, object] = {
        "export_version": 1,
        "task_ref": resolved_task_ref,
        "exported_at": _utcnow_iso(),
        "snapshot": snapshot,
    }
    snapshot_typed = cast(TaskSnapshot, snapshot)
    if include_markdown:
        render_state = _build_current_task_state_from_snapshot(snapshot_typed)
        payload["current_task_markdown"] = _render_current_task_md(render_state)
    destination = _resolve_output_path(output_path, resolved_task_ref)
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return _envelope(
        ok=True,
        tool="export_handoff_state",
        data={
            "path": str(destination),
            "counts": {
                "blockers": len(snapshot_typed["blockers"]),
                "next_actions": len(snapshot_typed["next_actions"]),
                "decisions": len(snapshot_typed["decisions"]),
                "verified_tests": len(snapshot_typed["verified_tests"]),
                "review_findings": len(snapshot_typed["review_findings"]),
                "worktree_lanes": len(snapshot_typed["worktree_lanes"]),
                "worker_reports": len(snapshot_typed["worker_reports"]),
                "lane_messages": len(snapshot_typed["lane_messages"]),
                "plan_cursors": len(snapshot_typed.get("plan_cursors", [])),
                "turn_metrics": len(snapshot_typed.get("turn_metrics", [])),
                "repo_instances": len(snapshot_typed.get("repo_instances", [])),
                "terminal_guard_events": len(snapshot_typed.get("terminal_guard_events", [])),
            },
        },
        task_ref=resolved_task_ref,
        artifacts=[{"type": "file", "path": str(destination)}],
    )


def _set_import_active_state(conn: sqlite3.Connection, task_ref: str, active: dict) -> None:
    detect_fn = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
    git_branch, git_commit = detect_fn()
    updated_by = (
        _normalize_optional_text(active.get("updated_by"))
        or _normalize_optional_text(resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_AGENT"))
        or "unknown"
    )
    updated_branch = _normalize_optional_text(active.get("updated_branch")) or git_branch or "unknown-branch"
    updated_commit_sha = _normalize_optional_text(active.get("updated_commit_sha")) or git_commit
    # internal: routing metadata must round-trip through import.
    # Without these, a fresh-DB import produces a live projection with
    # null target_branch/target_worktree_path/task_plan_path, breaking
    # canonical-root/worktree resolution for the imported task.
    target_branch = _normalize_optional_text(active.get("target_branch"))
    target_worktree_path = _normalize_optional_text(active.get("target_worktree_path"))
    task_plan_path = _normalize_optional_text(active.get("task_plan_path"))
    current = conn.execute("SELECT revision FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    if current is None:
        conn.execute(
            """
            INSERT INTO handoff_state (
                id, task_ref, objective, focus, status, target_branch, target_worktree_path, task_plan_path,
                revision, updated_at, updated_by, updated_branch, updated_commit_sha
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'), ?, ?, ?)
            """,
            (
                task_ref,
                active.get("objective", ""),
                active.get("focus"),
                active.get("status", "in_progress"),
                target_branch,
                target_worktree_path,
                task_plan_path,
                updated_by,
                updated_branch,
                updated_commit_sha,
            ),
        )
        return
    conn.execute(
        "UPDATE handoff_state SET objective = ?, focus = ?, status = ?, "
        "target_branch = ?, target_worktree_path = ?, task_plan_path = ?, "
        "revision = revision + 1, updated_at = datetime('now'), "
        "updated_by = ?, updated_branch = ?, updated_commit_sha = ? "
        "WHERE task_ref = ?",
        (
            active.get("objective", ""),
            active.get("focus"),
            active.get("status", "in_progress"),
            target_branch,
            target_worktree_path,
            task_plan_path,
            updated_by,
            updated_branch,
            updated_commit_sha,
            task_ref,
        ),
    )


def _import_plan_cursors(conn: sqlite3.Connection, task_ref: str, rows: list[dict], now: str) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO plan_cursors (
                task_ref, plan_item_id, state, lane_id, mcp_action_id, worker_message_id,
                source_heading, summary, dispatch_count, dispatched_at, completed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_ref,
                row.get("plan_item_id", ""),
                row.get("state", "dispatched"),
                row.get("lane_id"),
                row.get("mcp_action_id"),
                row.get("worker_message_id"),
                row.get("source_heading"),
                row.get("summary", ""),
                int(row.get("dispatch_count") or 0),
                row.get("dispatched_at"),
                row.get("completed_at"),
                row.get("created_at") or now,
                row.get("updated_at") or row.get("created_at") or now,
            ),
        )


def _import_turn_metrics(conn: sqlite3.Connection, task_ref: str, rows: list[dict], now: str) -> None:
    for row in rows:
        attribution_json = row.get("attribution_json")
        if attribution_json is None:
            attribution_json = json.dumps(row.get("attribution", {}), sort_keys=True)
        section_sizes_json = row.get("section_sizes_json")
        if section_sizes_json is None:
            section_sizes_json = json.dumps(row.get("section_sizes", {}), sort_keys=True)
        raw_usage_json = row.get("raw_usage_json")
        if raw_usage_json is None and row.get("raw_usage") is not None:
            raw_usage_json = json.dumps(row.get("raw_usage"), sort_keys=True)
        conn.execute(
            """
            INSERT INTO turn_metrics (
                task_ref, lane_id, session, cycle, phase, backend, model, thread_id, turn_id,
                input_tokens, output_tokens, cached_input_tokens, reasoning_output_tokens,
                total_tokens, usage_source, model_context_window, prompt_tokens, prompt_chars,
                prompt_token_source, utilization_ratio, domain_signal_ratio, pressure_level,
                attribution_json, section_sizes_json, raw_usage_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_ref,
                row.get("lane_id"),
                row.get("session", "import"),
                row.get("cycle"),
                row.get("phase", "execution"),
                row.get("backend", "unknown"),
                row.get("model"),
                row.get("thread_id"),
                row.get("turn_id"),
                row.get("input_tokens"),
                row.get("output_tokens"),
                row.get("cached_input_tokens"),
                row.get("reasoning_output_tokens"),
                row.get("total_tokens"),
                row.get("usage_source"),
                row.get("model_context_window"),
                row.get("prompt_tokens"),
                row.get("prompt_chars"),
                row.get("prompt_token_source"),
                row.get("utilization_ratio"),
                row.get("domain_signal_ratio"),
                row.get("pressure_level"),
                attribution_json,
                section_sizes_json,
                raw_usage_json,
                row.get("created_at") or now,
            ),
        )


def _resolve_import_fallbacks(
    active: object, *, fallback_agent: str, fallback_branch: str, fallback_commit: str | None
) -> tuple[str, str, str | None]:
    if not isinstance(active, dict):
        return fallback_agent, fallback_branch, fallback_commit
    return (
        _normalize_optional_text(active.get("updated_by")) or fallback_agent,
        _normalize_optional_text(active.get("updated_branch")) or fallback_branch,
        _normalize_optional_text(active.get("updated_commit_sha")) or fallback_commit,
    )


def _resolve_import_actor_values(
    row: dict,
    *,
    fallback_agent: str,
    fallback_branch: str,
    fallback_commit: str | None,
) -> tuple[str, str, str | None, str | None, str | None, str | None, str | None]:
    agent, branch, commit_sha, model, model_label, reasoning_level = _resolve_import_row_actor(
        row,
        fallback_agent=fallback_agent,
        fallback_branch=fallback_branch,
        fallback_commit=fallback_commit,
    )
    return (
        agent,
        normalize_actor_harness(row.get("harness") or agent),
        branch,
        commit_sha,
        model,
        model_label,
        reasoning_level,
    )


def _row_created_at(row: dict, now: str) -> object:
    return row.get("created_at") or now


def _row_updated_at(row: dict, now: str, *, fallback_keys: tuple[str, ...] = ()) -> object:
    value = row.get("updated_at")
    if value is not None:
        return value
    for key in fallback_keys:
        candidate = row.get(key)
        if candidate is not None:
            return candidate
    return row.get("created_at") or now


@dataclasses.dataclass
class SnapshotImportData:
    blockers: list
    actions: list
    decisions: list
    tests: list
    findings: list
    lanes: list
    reports: list
    messages: list
    plan_cursors: list
    turn_metrics: list
    repo_instances: list
    terminal_guard_events: list
    active: dict | None


_SNAPSHOT_LIST_FIELDS: tuple[tuple[str, str], ...] = (
    ("blockers", "blockers"),
    ("next_actions", "actions"),
    ("decisions", "decisions"),
    ("verified_tests", "tests"),
    ("review_findings", "findings"),
    ("worktree_lanes", "lanes"),
    ("worker_reports", "reports"),
    ("lane_messages", "messages"),
    ("plan_cursors", "plan_cursors"),
    ("turn_metrics", "turn_metrics"),
    ("repo_instances", "repo_instances"),
    ("terminal_guard_events", "terminal_guard_events"),
)


def _parse_import_snapshot(snapshot: dict) -> SnapshotImportData:
    """Validate and normalize snapshot dict into a typed SnapshotImportData.

    Raises ValueError for any child array field that is not a list so that
    callers can reject malformed snapshots before any DB writes begin.
    """
    extracted: dict[str, list] = {}
    for snapshot_key, attr_name in _SNAPSHOT_LIST_FIELDS:
        raw = snapshot.get(snapshot_key, [])
        if not isinstance(raw, list):
            raise ValueError(f"snapshot field '{snapshot_key}' must be a list, got {type(raw).__name__}")
        extracted[attr_name] = raw
    return SnapshotImportData(
        blockers=extracted["blockers"],
        actions=extracted["actions"],
        decisions=extracted["decisions"],
        tests=extracted["tests"],
        findings=extracted["findings"],
        lanes=extracted["lanes"],
        reports=extracted["reports"],
        messages=extracted["messages"],
        plan_cursors=extracted["plan_cursors"],
        turn_metrics=extracted["turn_metrics"],
        repo_instances=extracted.get("repo_instances", []),
        terminal_guard_events=extracted.get("terminal_guard_events", []),
        active=snapshot.get("active"),
    )


def _import_snapshot(
    conn: sqlite3.Connection, task_ref: str, snapshot: dict, mode: str, set_active: bool
) -> dict[str, int]:
    data = _parse_import_snapshot(snapshot)
    blockers = data.blockers
    actions = data.actions
    decisions = data.decisions
    tests = data.tests
    findings = data.findings
    lanes = data.lanes
    reports = data.reports
    messages = data.messages
    plan_cursors = data.plan_cursors
    turn_metrics = data.turn_metrics
    repo_instances = data.repo_instances
    terminal_guard_events = data.terminal_guard_events
    active = data.active
    now = _utcnow_iso().replace("T", " ").replace("Z", "")
    detect_fn = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
    git_branch, git_commit = detect_fn()
    fallback_agent, fallback_branch, fallback_commit = _resolve_import_fallbacks(
        active,
        fallback_agent=_normalize_optional_text(resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_AGENT")) or "unknown",
        fallback_branch=git_branch or "unknown-branch",
        fallback_commit=git_commit,
    )
    if mode == "replace_task":
        for table in (
            "blockers",
            "next_actions",
            "decisions",
            "test_traces",
            "verified_tests",
            "review_findings",
            "worktree_lanes",
            "worker_reports",
            "lane_messages",
            "plan_cursors",
            "turn_metrics",
            "terminal_guard_events",
        ):
            conn.execute(f"DELETE FROM {table} WHERE task_ref = ?", (task_ref,))
    for row in repo_instances:
        conn.execute(
            """
            INSERT INTO repo_instances (repo_instance_id, workspace_root, git_common_dir, created_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(repo_instance_id) DO NOTHING
            """,
            (
                row.get("repo_instance_id", ""),
                row.get("workspace_root", ""),
                row.get("git_common_dir", ""),
                row.get("created_at") or now,
                row.get("last_seen_at") or row.get("created_at") or now,
            ),
        )
    for row in blockers:
        agent, _harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        conn.execute(
            "INSERT INTO blockers (task_ref, lane_id, description, status, agent, branch, commit_sha, resolved_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                _resolve_import_lane_id(row),
                row.get("description", ""),
                row.get("status", "open"),
                agent,
                branch,
                commit_sha,
                row.get("resolved_at"),
                _row_created_at(row, now),
            ),
        )
    for row in actions:
        agent, _harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        conn.execute(
            "INSERT INTO next_actions (task_ref, lane_id, action, priority, status, agent, branch, commit_sha, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                _resolve_import_lane_id(row),
                row.get("action", ""),
                int(row.get("priority", 100)),
                row.get("status", "pending"),
                agent,
                branch,
                commit_sha,
                _row_created_at(row, now),
                _row_updated_at(row, now),
            ),
        )
    for row in decisions:
        agent, harness, branch, commit_sha, model, model_label, reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        conn.execute(
            "INSERT INTO decisions (task_ref, lane_id, session, decision, rationale, agent, harness, model, model_label, reasoning_level, input_tokens, output_tokens, total_tokens, changed_files_json, slice_number, branch, commit_sha, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(task_ref, decision, session) DO NOTHING",
            (
                task_ref,
                _resolve_import_lane_id(row),
                row.get("session", "import"),
                row.get("decision", ""),
                row.get("rationale"),
                agent,
                harness,
                model,
                model_label,
                reasoning_level,
                row.get("input_tokens"),
                row.get("output_tokens"),
                row.get("total_tokens"),
                row.get("changed_files_json", "[]"),
                row.get("slice_number"),
                branch,
                commit_sha,
                _row_created_at(row, now),
            ),
        )
    for row in tests:
        agent, _harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        cursor = conn.execute(
            "INSERT INTO verified_tests (task_ref, lane_id, command, passed, exit_code, result, session, agent, branch, commit_sha, verified_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                _resolve_import_lane_id(row),
                row.get("command", ""),
                1 if row.get("passed") else 0,
                row.get("exit_code"),
                row.get("result"),
                row.get("session", "import"),
                agent,
                branch,
                commit_sha,
                row.get("verified_at") or now,
            ),
        )
        traces = row.get("traces")
        if isinstance(traces, list):
            for trace_order, trace in enumerate(traces):
                if not isinstance(trace, str):
                    continue
                conn.execute(
                    "INSERT INTO test_traces (verified_test_id, task_ref, trace_order, trace, created_at) VALUES (?, ?, ?, ?, ?)",
                    (int(cursor.lastrowid or 0), task_ref, trace_order, trace, row.get("verified_at") or now),
                )
    for row in findings:
        agent, harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        conn.execute(
            "INSERT INTO review_findings (task_ref, lane_id, finding_id, severity, file_path, line_start, line_end, description, fix, status, review_mode, session, agent, harness, branch, commit_sha, resolution_notes, reopen_count, last_reopen_reason, last_reopened_at, resolved_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                _resolve_import_lane_id(row),
                row.get("finding_id", ""),
                row.get("severity", "low"),
                row.get("file_path", ""),
                row.get("line_start"),
                row.get("line_end"),
                row.get("description", ""),
                row.get("fix"),
                row.get("status", "open"),
                row.get("review_mode"),
                row.get("session", "import"),
                agent,
                harness,
                branch,
                commit_sha,
                row.get("resolution_notes"),
                int(row.get("reopen_count") or 0),
                row.get("last_reopen_reason"),
                row.get("last_reopened_at"),
                row.get("resolved_at"),
                _row_created_at(row, now),
                _row_updated_at(row, now, fallback_keys=("resolved_at",)),
            ),
        )
    for row in lanes:
        conn.execute(
            "INSERT INTO worktree_lanes (task_ref, lane_id, title, objective, worktree_path, branch, owner_agent, status, notes, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                row.get("lane_id", ""),
                row.get("title"),
                row.get("objective"),
                row.get("worktree_path", ""),
                row.get("branch", ""),
                row.get("owner_agent"),
                row.get("status", "planned"),
                row.get("notes"),
                _row_created_at(row, now),
                _row_updated_at(row, now),
            ),
        )
    for row in reports:
        agent, _harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        outcome = row.get("outcome")
        if outcome not in {"finished", "failed", "exhausted", "stopped"}:
            outcome = None
        conn.execute(
            "INSERT INTO worker_reports (task_ref, lane_id, session, summary, changed_files_json, test_commands_json, blockers_json, merge_ready, status, outcome, agent, branch, commit_sha, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                row.get("lane_id", ""),
                row.get("session", "import"),
                row.get("summary", ""),
                row.get("changed_files_json") or json.dumps(row.get("changed_files", [])),
                row.get("test_commands_json") or json.dumps(row.get("test_commands", [])),
                row.get("blockers_json") or json.dumps(row.get("blockers", [])),
                1 if row.get("merge_ready") else 0,
                row.get("status", "submitted"),
                outcome,
                agent,
                branch,
                commit_sha,
                _row_created_at(row, now),
            ),
        )
    for row in messages:
        agent, _harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        payload_json = row.get("payload_json")
        payload = row.get("payload")
        if isinstance(payload, dict):
            payload_json = json.dumps(payload, sort_keys=True)
        # HARM-A-005: the idx_lane_messages_dispatch_id unique index (task_ref,
        # lane_id, dispatch_id) constrains every writer. A snapshot that carries a
        # message whose (lane_id, dispatch_id) already exists for this task must
        # not abort the whole import — ignore the duplicate rather than raise.
        conn.execute(
            "INSERT OR IGNORE INTO lane_messages (task_ref, lane_id, session, direction, subject, message, status, dispatch_id, payload_json, agent, branch, commit_sha, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_ref,
                row.get("lane_id", ""),
                row.get("session", "import"),
                row.get("direction", "worker_to_orchestrator"),
                row.get("subject"),
                row.get("message", ""),
                row.get("status", "open"),
                row.get("dispatch_id"),
                payload_json,
                agent,
                branch,
                commit_sha,
                _row_created_at(row, now),
                _row_updated_at(row, now),
            ),
        )
    for row in terminal_guard_events:
        conn.execute(
            """
            INSERT INTO terminal_guard_events (
                event_key,
                repo_instance_id,
                task_ref,
                worktree_path,
                harness,
                tool_name,
                decision,
                trigger,
                native_tool_hint,
                command_preview,
                policy_version,
                policy_source,
                fallback_source,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO NOTHING
            """,
            (
                row.get("event_key", ""),
                row.get("repo_instance_id", ""),
                row.get("task_ref") or task_ref,
                row.get("worktree_path"),
                row.get("harness", ""),
                row.get("tool_name", ""),
                row.get("decision", ""),
                row.get("trigger"),
                row.get("native_tool_hint"),
                row.get("command_preview", ""),
                row.get("policy_version", ""),
                row.get("policy_source", ""),
                row.get("fallback_source"),
                row.get("created_at") or now,
            ),
        )
    _import_plan_cursors(conn, task_ref, plan_cursors, now)
    _import_turn_metrics(conn, task_ref, turn_metrics, now)
    if set_active and isinstance(active, dict):
        _set_import_active_state(conn, task_ref, active)
    return {
        "blockers": len(blockers),
        "next_actions": len(actions),
        "decisions": len(decisions),
        "verified_tests": len(tests),
        "review_findings": len(findings),
        "worktree_lanes": len(lanes),
        "worker_reports": len(reports),
        "lane_messages": len(messages),
        "plan_cursors": len(plan_cursors),
        "turn_metrics": len(turn_metrics),
        "repo_instances": len(repo_instances),
        "terminal_guard_events": len(terminal_guard_events),
    }


def import_handoff_state(
    input_path: str, mode: str = "merge", set_active: bool = False, allow_destructive_clear: bool = False
) -> dict:
    if mode not in {"merge", "replace_task"}:
        return _envelope(
            ok=False, tool="import_handoff_state", data={"error": "Invalid mode. Valid: merge, replace_task."}
        )
    source = Path(input_path)
    if not source.is_absolute():
        source = _workspace_root() / source
    if not source.exists():
        return _envelope(ok=False, tool="import_handoff_state", data={"error": f"Input file not found: {source}"})
    payload = json.loads(source.read_text())
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        return _envelope(
            ok=False, tool="import_handoff_state", data={"error": "Invalid import payload: snapshot must be an object."}
        )
    task_ref = payload.get("task_ref") or snapshot.get("task_ref")
    if not task_ref:
        return _envelope(ok=False, tool="import_handoff_state", data={"error": "Missing task_ref in import payload."})
    required_sections = (
        "blockers",
        "next_actions",
        "decisions",
        "verified_tests",
        "review_findings",
        "worktree_lanes",
        "worker_reports",
        "lane_messages",
    )
    optional_sections = ("plan_cursors", "turn_metrics", "repo_instances", "terminal_guard_events")
    if mode == "replace_task":
        missing_sections = [key for key in required_sections if key not in snapshot]
        if missing_sections:
            return _envelope(
                ok=False,
                tool="import_handoff_state",
                data={
                    "error": f"Invalid replace_task payload: missing required snapshot sections {', '.join(missing_sections)}.",
                },
            )
    for key in (*required_sections, *optional_sections):
        items = snapshot.get(key, [])
        if not isinstance(items, list):
            return _envelope(
                ok=False,
                tool="import_handoff_state",
                data={"error": f"Invalid import payload: snapshot.{key} must be an array."},
            )
        for item in items:
            if not isinstance(item, dict):
                return _envelope(
                    ok=False,
                    tool="import_handoff_state",
                    data={"error": f"Invalid import payload: items in snapshot.{key} must be objects."},
                )
    if "active" in snapshot and snapshot["active"] is not None and not isinstance(snapshot["active"], dict):
        return _envelope(
            ok=False,
            tool="import_handoff_state",
            data={"error": "Invalid import payload: snapshot.active must be an object."},
        )
    with _get_db_connection() as conn:
        if mode == "replace_task" and not allow_destructive_clear:
            existing_counts = _count_task_rows(conn, task_ref)
            incoming_counts = {key: len(snapshot.get(key, [])) for key in (*required_sections, *optional_sections)}
            incoming_tests = snapshot.get("verified_tests") or []
            incoming_counts["test_traces"] = sum(
                len(row.get("traces") or []) for row in incoming_tests if isinstance(row, Mapping)
            )
            potentially_cleared = [
                section
                for section, existing_count in existing_counts.items()
                if existing_count > 0 and incoming_counts.get(section, 0) == 0
            ]
            if potentially_cleared:
                return _envelope(
                    ok=False,
                    tool="import_handoff_state",
                    data={
                        "error": f"replace_task would clear existing handoff rows in sections: {', '.join(potentially_cleared)}. Re-run with allow_destructive_clear=true to confirm.",
                        "existing_counts": existing_counts,
                        "incoming_counts": incoming_counts,
                    },
                    task_ref=task_ref,
                )
        counts = _import_snapshot(conn, task_ref=task_ref, snapshot=snapshot, mode=mode, set_active=set_active)
    if set_active and isinstance(snapshot.get("active"), dict):
        from .current_task_rendering import _write_per_task_projection

        _write_per_task_projection(task_ref)
    return _envelope(
        ok=True,
        tool="import_handoff_state",
        data={
            "mode": mode,
            "set_active": set_active,
            "allow_destructive_clear": allow_destructive_clear,
            "counts": counts,
        },
        task_ref=task_ref,
        mutation={
            "entity": "handoff_state",
            "operation": f"import_{mode}",
            "affected_ids": [task_ref],
            "task_revision": snapshot.get("active", {}).get("revision")
            if isinstance(snapshot.get("active"), dict)
            else None,
        },
    )


def _slugify_task_ref_for_decision_id(task_ref: str) -> str:
    """Lower-case + collapse non-id chars so a task_ref can fit a decision id slug."""
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in task_ref).strip("_") or "task"


def _cascade_archive_maint_planning_review_rows(
    conn: sqlite3.Connection,
    *,
    parent_task_ref: str,
    ctx: ResolvedWriteContext,
) -> list[str]:
    """Archive internal-* rows referencing ``parent_task_ref``.

    Returns the list of cascade-archived task_refs. Records one
    ``cascade_archive`` decision when at least one row is archived. The
    cascade and decision write happen inside the caller's transaction so
    the audit trail is atomic with the archive.
    """
    like_pattern = f"%{parent_task_ref}%"
    # Coarse SQL filter, then anchored Python check: SQL LIKE has no
    # token boundary, so "internal" otherwise matches "internal" inside
    # objective/task_plan_path. Require non-alphanumeric (or string edge)
    # on each side so adjacent digits/letters disqualify the match while
    # hyphens and other separators still count as a boundary.
    boundary_pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(parent_task_ref)}(?![A-Za-z0-9])")
    cascade_candidates = conn.execute(
        """
        SELECT task_ref,
               COALESCE(objective, '') AS objective,
               COALESCE(task_plan_path, '') AS task_plan_path
        FROM handoff_state
        WHERE task_ref LIKE 'internal-%'
          AND task_ref != ?
          AND (
              COALESCE(objective, '') LIKE ?
              OR COALESCE(task_plan_path, '') LIKE ?
          )
        """,
        (parent_task_ref, like_pattern, like_pattern),
    ).fetchall()

    cascade_archived: list[str] = []
    for row in cascade_candidates:
        if not (boundary_pattern.search(row["objective"]) or boundary_pattern.search(row["task_plan_path"])):
            continue
        child_ref = str(row["task_ref"])
        child_snapshot = _snapshot_with_test_traces(conn, _collect_task_snapshot(conn, child_ref))
        _persist_task_archive_snapshot(
            conn,
            task_ref=child_ref,
            snapshot=child_snapshot,
            ctx=ctx,
            notes=f"Cascade-archived alongside {parent_task_ref}",
        )
        conn.execute("DELETE FROM handoff_state WHERE task_ref = ?", (child_ref,))
        cascade_archived.append(child_ref)

    if cascade_archived:
        cascade_archived_sorted = sorted(cascade_archived)
        slug = _slugify_task_ref_for_decision_id(parent_task_ref) + "_planning_review"
        decision_id = f"internal41_cascade_archive_{parent_task_ref}_{slug}"
        rationale_lines = [
            "## Cascade-archived internal rows",
            "",
            f"Parent: `{parent_task_ref}`",
            "",
            "Archived as a side effect of the parent archive:",
        ]
        rationale_lines.extend(f"- `{ref}`" for ref in cascade_archived_sorted)
        rationale = "\n".join(rationale_lines)
        conn.execute(
            """
            INSERT INTO decisions (
                task_ref, lane_id, session, decision, rationale, agent, harness,
                model, model_label, reasoning_level,
                input_tokens, output_tokens, total_tokens,
                branch, commit_sha, changed_files_json, created_at
            )
            VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, '[]', datetime('now'))
            """,
            (
                parent_task_ref,
                "archive_cascade",
                decision_id,
                rationale,
                ctx.agent,
                ctx.harness,
                ctx.model,
                ctx.model_label,
                ctx.reasoning_level,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
    return cascade_archived


def tasks_gc(apply: bool = False) -> dict:
    """Archive ``status=done`` on-main ``MAINT-*`` rows that are safely completable.

    Parented ``internal-*`` rows archive when their internal
    internal parent is in ``task_archives``. Parentless on-main ``MAINT-*``
    rows (no ``internal-\\d+`` reference) archive on their own. Dry-run by
    default; pass ``apply=True`` to mutate. Idempotent: a second invocation
    after ``apply=True`` is a no-op because archived rows are no longer
    present in ``handoff_state``.

    Safety scope: this GC only reclaims done rows on the integration branch
    whose parent (if any) is already archived. Done rows on unmerged feature
    branches, or whose parent task is still live, are intentionally left in
    place -- archiving them would prematurely drop lifecycle tracking for work
    that is not yet integrated. The broader "any done row with no open findings
    on a merged/integration branch" backfill is served by the sibling
    ``reap_done_nonscratch_handoff_rows`` (``archive --operation reap_done``),
    which carries the merged-branch and no-open-findings guards; ``tasks_gc``
    stays the narrow MAINT janitor rather than duplicating it less safely.
    """
    archived: list[str] = []
    would_archive: list[str] = []

    with _get_db_connection() as conn:
        candidates = conn.execute(
            """
            SELECT task_ref, objective, task_plan_path, target_branch
            FROM handoff_state
            WHERE task_ref LIKE 'MAINT-%'
              AND status = 'done'
            """
        ).fetchall()

        if not candidates:
            return _envelope(
                ok=True,
                tool="tasks_gc",
                data={
                    "applied": apply,
                    "archived": [],
                    "would_archive": [],
                },
            )

        ctx = _resolve_write_actor(conn, build_write_actor(agent="tasks-gc"))

        for row in candidates:
            # Skip done rows that are not on the integration branch: a done row
            # pinned to an unmerged feature branch is not safely archivable.
            if not _is_integration_target_branch(row["target_branch"]):
                continue

            child_ref = str(row["task_ref"])
            search_text = " ".join(str(row[col] or "") for col in ("task_ref", "objective", "task_plan_path"))
            parent_refs = sorted(
                {parent_ref for parent_ref in _internal_TASK_REF_RE.findall(search_text) if parent_ref != child_ref}
            )
            archived_parent: str | None
            if parent_refs:
                # Parented planning-review rows only archive once their parent
                # task is itself archived; skip while the parent is still live.
                archived_parent = None
                for parent_ref in parent_refs:
                    exists = conn.execute(
                        "SELECT 1 FROM task_archives WHERE task_ref = ?",
                        (parent_ref,),
                    ).fetchone()
                    if exists is not None:
                        archived_parent = parent_ref
                        break
                if archived_parent is None:
                    continue
            else:
                archived_parent = child_ref

            would_archive.append(child_ref)
            if not apply:
                continue

            child_snapshot = _snapshot_with_test_traces(conn, _collect_task_snapshot(conn, child_ref))
            notes = (
                f"tasks-gc: parent {archived_parent} archived"
                if archived_parent != child_ref
                else "tasks-gc: parentless on-main MAINT row"
            )
            _persist_task_archive_snapshot(
                conn,
                task_ref=child_ref,
                snapshot=child_snapshot,
                ctx=ctx,
                notes=notes,
            )
            # Tombstone the archived row's open findings in the same transaction,
            # exactly as ``archive_task_state`` does. Without this, the janitor
            # archives the row but leaves its findings ``open`` -- the stale-findings
            # graveyard this GC exists to drain.
            _supersede_open_findings_for_archive(conn, child_ref)
            conn.execute("DELETE FROM handoff_state WHERE task_ref = ?", (child_ref,))

            slug = _slugify_task_ref_for_decision_id(child_ref) + "_gc"
            decision_task_ref = archived_parent
            if archived_parent == child_ref:
                decision_id = f"internal41_tasks_gc_archive_{slug}"
                rationale = f"## tasks-gc archive\n\nArchived parentless on-main MAINT row `{child_ref}`."
            else:
                decision_id = f"internal41_cascade_archive_{archived_parent}_{slug}"
                rationale = f"## tasks-gc cascade archive\n\nArchived `{child_ref}` because parent `{archived_parent}` is archived."
            conn.execute(
                """
                INSERT INTO decisions (
                    task_ref, lane_id, session, decision, rationale, agent, harness,
                    model, model_label, reasoning_level,
                    input_tokens, output_tokens, total_tokens,
                    branch, commit_sha, changed_files_json, created_at
                )
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, '[]', datetime('now'))
                """,
                (
                    decision_task_ref,
                    "tasks_gc",
                    decision_id,
                    rationale,
                    ctx.agent,
                    ctx.harness,
                    ctx.model,
                    ctx.model_label,
                    ctx.reasoning_level,
                    ctx.branch,
                    ctx.commit_sha,
                ),
            )
            archived.append(child_ref)

    return _envelope(
        ok=True,
        tool="tasks_gc",
        data={
            "applied": apply,
            "archived": archived,
            "would_archive": would_archive,
        },
    )


def _count_open_findings(conn: sqlite3.Connection, task_ref: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS count FROM review_findings WHERE task_ref = ? AND status = 'open'",
        (task_ref,),
    ).fetchone()
    return int(row["count"]) if row is not None else 0


def _branch_is_merged(target_branch: str | None, integration_ref: str = "main") -> bool:
    return branch_is_merged(_normalize_optional_text(target_branch) or "", integration_ref)


def _is_integration_target_branch(target_branch: str | None, integration_ref: str = "main") -> bool:
    branch = _normalize_optional_text(target_branch)
    integration = _normalize_optional_text(integration_ref)
    protected = {"main", "master"}
    if integration:
        protected.update({integration, f"origin/{integration}"})
    return bool(branch and branch in protected)


DEFAULT_MAINT_ON_MAIN_REAP_DAYS = 7


def _maint_on_main_reap_days() -> int:
    """Return the age threshold for auto-reaping stale MAINT-on-main rows.

      ``WORKBAY_HANDOFF_MAINT_ON_MAIN_REAP_DAYS`` gates the feature; ``0``
    disables auto-reap. Unset env falls back to :data:`DEFAULT_MAINT_ON_MAIN_REAP_DAYS`.
    """
    raw = resolve_env_alias("WORKBAY_HANDOFF_MAINT_ON_MAIN_REAP_DAYS")
    if raw is None or str(raw).strip() == "":
        return DEFAULT_MAINT_ON_MAIN_REAP_DAYS
    try:
        return max(0, int(str(raw).strip()))
    except ValueError:
        return DEFAULT_MAINT_ON_MAIN_REAP_DAYS


def _is_ad_hoc_maint_on_main_row(task_ref: str, target_branch: str | None, *, integration_ref: str = "main") -> bool:
    """Return True for parentless ad-hoc ``MAINT-*`` rows targeting main."""
    if not task_ref.startswith("MAINT-"):
        return False
    if task_ref.startswith("internal-"):
        return False
    return _is_integration_target_branch(target_branch, integration_ref)


def _row_is_older_than_days(conn: sqlite3.Connection, updated_at: object, days: int) -> bool:
    row = conn.execute(
        "SELECT datetime(?) < datetime('now', ?) AS stale",
        (str(updated_at), f"-{days} days"),
    ).fetchone()
    return bool(row is not None and row["stale"])


def _stale_maint_on_main_closeable(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    integration_ref: str = "main",
) -> tuple[bool, str]:
    """Return whether a live MAINT-on-main row is eligible for auto-reap."""
    task_ref = str(row["task_ref"])
    target_branch = _normalize_optional_text(row["target_branch"])
    if not _is_ad_hoc_maint_on_main_row(task_ref, target_branch, integration_ref=integration_ref):
        return False, ""
    if _count_open_findings(conn, task_ref) > 0:
        return False, ""
    days = _maint_on_main_reap_days()
    if days <= 0:
        return False, ""
    if not _row_is_older_than_days(conn, row["updated_at"], days):
        return False, ""
    return True, f"stale MAINT-on-main row older than {days} days with no open findings"


def _worktree_is_live(
    target_worktree_path: str | None,
    target_branch: str | None,
    *,
    merged: bool,
    integration_ref: str = "main",
) -> bool:
    wt_path = _normalize_optional_text(target_worktree_path)
    if not wt_path:
        return False
    path = Path(wt_path)
    if not path.exists():
        return False
    try:
        proc = subprocess.run(
            ["git", "-C", str(path), "status", "--porcelain"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    if (proc.stdout or "").strip():
        return True
    if target_branch and not merged:
        try:
            proc = subprocess.run(
                ["git", "-C", str(path), "merge-base", "--is-ancestor", "HEAD", integration_ref],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
            return False
        return proc.returncode != 0
    return False


def _classify_live_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    integration_ref: str = "main",
) -> tuple[str, str]:
    task_ref = str(row["task_ref"])
    target_branch = _normalize_optional_text(row["target_branch"])
    target_worktree_path = _normalize_optional_text(row["target_worktree_path"])
    stale_maint, stale_reason = _stale_maint_on_main_closeable(conn, row, integration_ref)
    if stale_maint:
        return "closeable", stale_reason
    if _is_integration_target_branch(target_branch, integration_ref):
        return "active", "main-target maintenance/planning row"
    merged = _branch_is_merged(target_branch, integration_ref)
    has_open = _count_open_findings(conn, task_ref) > 0
    worktree_live = _worktree_is_live(
        target_worktree_path,
        target_branch,
        merged=merged,
        integration_ref=integration_ref,
    )

    if merged and not has_open:
        return "closeable", "branch merged with no open findings"
    if not merged and (worktree_live or has_open):
        if has_open:
            return "active", "unmerged branch with open findings"
        return "active", "unmerged branch with live worktree"
    if merged and has_open:
        return "ambiguous", "branch merged but open findings remain"
    if target_branch and not merged and not branch_exists(target_branch):
        return "ambiguous", "branch missing with no merge proof"
    return "ambiguous", "insufficient signals to classify safely"


def _classify_live_rows(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
    integration_ref: str = "main",
) -> dict[str, list[dict[str, object]]]:
    closeable: list[dict[str, object]] = []
    active: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    placeholders = ",".join(["?"] * len(LIVE_ACTIVE_STATUSES))
    rows = conn.execute(
        f"""
        SELECT task_ref, status, target_branch, target_worktree_path, updated_at
        FROM handoff_state
        WHERE status IN ({placeholders})
        ORDER BY updated_at DESC, task_ref ASC
        """,
        LIVE_ACTIVE_STATUSES,
    ).fetchall()
    for row in rows:
        ref = str(row["task_ref"])
        if task_ref is not None and ref != task_ref:
            continue
        bucket, reason = _classify_live_row(conn, row, integration_ref)
        entry: dict[str, object] = {
            "task_ref": ref,
            "reason": reason,
            "target_branch": row["target_branch"],
        }
        if bucket == "closeable":
            closeable.append(entry)
        elif bucket == "active":
            active.append(entry)
        else:
            ambiguous.append(entry)
    return {"closeable": closeable, "active": active, "ambiguous": ambiguous}


def _reap_error(result: dict) -> str:
    """Best-effort single-string error extracted from a tool envelope."""
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        return str(data["error"])
    if result.get("error"):
        return str(result["error"])
    return "unknown_error"


def classify_live_tasks(
    task_ref: str | None = None,
    integration_ref: str = "main",
) -> dict:
    """Partition live handoff rows into closeable, active, and ambiguous buckets."""
    with _get_db_connection() as conn:
        buckets = _classify_live_rows(conn, task_ref=task_ref, integration_ref=integration_ref)
    return _envelope(
        ok=True,
        tool="classify_live_tasks",
        data=buckets,
    )


def reap_tasks(
    apply: bool = False,
    task_ref: str | None = None,
    integration_ref: str = "main",
) -> dict:
    """Classify live rows and optionally close+archive the closeable set."""
    with _get_db_connection() as conn:
        buckets = _classify_live_rows(conn, task_ref=task_ref, integration_ref=integration_ref)

    reaped: list[str] = []
    failed: list[dict[str, object]] = []
    stale_maint_closeable = [
        entry
        for entry in buckets["closeable"]
        if isinstance((reason := entry.get("reason")), str) and reason.startswith("stale MAINT-on-main row")
    ]
    if apply:
        for entry in buckets["closeable"]:
            ref = str(entry["task_ref"])
            status_result = update_task_status(task_ref=ref, status="done")
            if not status_result.get("ok"):
                # Status flip failed; the row is untouched and remains live, so
                # a later reap will retry it. Surface it rather than silently
                # dropping it from the result.
                failed.append({"task_ref": ref, "stage": "status", "error": _reap_error(status_result)})
                continue
            archive_result = archive_task_state(task_ref=ref, cascade_maint_review=True)
            if archive_result.get("ok"):
                reaped.append(ref)
            else:
                # Status is now 'done' but archive failed: the row is no longer
                # in LIVE_ACTIVE_STATUSES, so a subsequent reap will NOT re-pick
                # it. Report it so the operator can recover the orphan instead of
                # it vanishing from `reaped` with no signal.
                failed.append({"task_ref": ref, "stage": "archive", "error": _reap_error(archive_result)})

    return _envelope(
        ok=True,
        tool="reap_tasks",
        data={
            "applied": apply,
            **buckets,
            "reaped": reaped,
            "failed": failed,
            "stale_maint_on_main": {
                "reap_days": _maint_on_main_reap_days(),
                "closeable_count": len(stale_maint_closeable),
                "closeable": stale_maint_closeable,
            },
        },
    )


def _classify_scratch_handoff_rows(conn: sqlite3.Connection) -> dict[str, list[dict[str, object]]]:
    from .review_findings_queries import is_reviewer_scratch_task_ref

    closeable: list[dict[str, object]] = []
    active: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    scratch_statuses = tuple(dict.fromkeys((*LIVE_ACTIVE_STATUSES, HandoffStatus.DONE.value)))
    placeholders = ",".join(["?"] * len(scratch_statuses))
    rows = conn.execute(
        f"""
        SELECT task_ref, status, target_branch, target_worktree_path, updated_at
        FROM handoff_state
        WHERE status IN ({placeholders})
        ORDER BY updated_at DESC, task_ref ASC
        """,
        scratch_statuses,
    ).fetchall()
    for row in rows:
        ref = str(row["task_ref"])
        if not is_reviewer_scratch_task_ref(ref):
            continue
        open_count = _count_open_findings(conn, ref)
        entry: dict[str, object] = {
            "task_ref": ref,
            "target_branch": row["target_branch"],
        }
        if open_count > 0:
            ambiguous.append({**entry, "reason": "reviewer scratch with open findings"})
        else:
            closeable.append({**entry, "reason": "reviewer scratch with no open findings"})
    return {"closeable": closeable, "active": active, "ambiguous": ambiguous}


def reap_scratch_handoff_rows(*, apply: bool = False) -> dict:
    """Close and archive live reviewer-scratch handoff rows with no open findings."""
    with _get_db_connection() as conn:
        buckets = _classify_scratch_handoff_rows(conn)

    reaped: list[str] = []
    failed: list[dict[str, object]] = []
    if apply:
        for entry in buckets["closeable"]:
            ref = str(entry["task_ref"])
            status_result = update_task_status(task_ref=ref, status="done")
            if not status_result.get("ok"):
                failed.append({"task_ref": ref, "stage": "status", "error": _reap_error(status_result)})
                continue
            archive_result = archive_task_state(task_ref=ref, cascade_maint_review=False)
            if archive_result.get("ok"):
                reaped.append(ref)
            else:
                failed.append({"task_ref": ref, "stage": "archive", "error": _reap_error(archive_result)})

    return _envelope(
        ok=True,
        tool="reap_scratch_handoff_rows",
        data={
            "applied": apply,
            **buckets,
            "reaped": reaped,
            "failed": failed,
        },
    )


def _classify_done_nonscratch_handoff_rows(conn: sqlite3.Connection) -> dict[str, list[dict[str, object]]]:
    from .review_findings_queries import is_reviewer_scratch_task_ref

    closeable: list[dict[str, object]] = []
    active: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    rows = conn.execute(
        """
        SELECT task_ref, status, target_branch, target_worktree_path, updated_at
        FROM handoff_state
        WHERE status = ?
        ORDER BY updated_at DESC, task_ref ASC
        """,
        (HandoffStatus.DONE.value,),
    ).fetchall()
    for row in rows:
        ref = str(row["task_ref"])
        if is_reviewer_scratch_task_ref(ref):
            continue
        open_count = _count_open_findings(conn, ref)
        entry: dict[str, object] = {
            "task_ref": ref,
            "target_branch": row["target_branch"],
        }
        target_branch = _normalize_optional_text(row["target_branch"])
        if open_count > 0:
            ambiguous.append({**entry, "reason": "done row with open findings"})
        elif _is_integration_target_branch(target_branch):
            closeable.append({**entry, "reason": "done integration-target row with no open findings"})
        elif _branch_is_merged(target_branch):
            closeable.append({**entry, "reason": "done merged branch with no open findings"})
        else:
            ambiguous.append({**entry, "reason": "done row but branch not merged"})
    return {"closeable": closeable, "active": active, "ambiguous": ambiguous}


def reap_done_nonscratch_handoff_rows(*, apply: bool = False) -> dict:
    """Archive live non-scratch handoff rows already marked done with no open findings."""
    with _get_db_connection() as conn:
        buckets = _classify_done_nonscratch_handoff_rows(conn)

    reaped: list[str] = []
    failed: list[dict[str, object]] = []
    if apply:
        for entry in buckets["closeable"]:
            ref = str(entry["task_ref"])
            archive_result = archive_task_state(task_ref=ref, cascade_maint_review=False)
            if archive_result.get("ok"):
                reaped.append(ref)
            else:
                failed.append({"task_ref": ref, "stage": "archive", "error": _reap_error(archive_result)})

    return _envelope(
        ok=True,
        tool="reap_done_nonscratch_handoff_rows",
        data={
            "applied": apply,
            **buckets,
            "reaped": reaped,
            "failed": failed,
        },
    )


DEFAULT_ARCHIVE_RETENTION_DAYS = 90


def archives_retention_gc(
    apply: bool = False,
    older_than_days: int = DEFAULT_ARCHIVE_RETENTION_DAYS,
) -> dict:
    """Prune ``task_archives`` rows older than ``older_than_days``.

    Dry-run by default; pass ``apply=True`` to delete stale archive rows.
    Only touches ``task_archives`` — live ``handoff_state`` rows are never
    mutated.
    """
    if older_than_days < 1:
        return _envelope(
            ok=False,
            tool="archives_retention_gc",
            data={"error": "older_than_days must be >= 1"},
        )

    would_prune: list[dict[str, object]] = []
    pruned: list[str] = []

    with _get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT task_ref, archived_at
            FROM task_archives
            WHERE datetime(archived_at) < datetime('now', ?)
            ORDER BY archived_at ASC, task_ref ASC
            """,
            (f"-{older_than_days} days",),
        ).fetchall()
        for row in rows:
            ref = str(row["task_ref"])
            would_prune.append(
                {
                    "task_ref": ref,
                    "archived_at": row["archived_at"],
                }
            )
            if apply:
                conn.execute("DELETE FROM task_archives WHERE task_ref = ?", (ref,))
                pruned.append(ref)

    return _envelope(
        ok=True,
        tool="archives_retention_gc",
        data={
            "applied": apply,
            "older_than_days": older_than_days,
            "would_prune": would_prune,
            "pruned": pruned,
        },
    )


def archive_task_state(
    task_ref: str | None = None,
    notes: str | None = None,
    archive_by: str | None = None,
    archive_branch: str | None = None,
    archive_commit_sha: str | None = None,
    clear_active_if_matches: bool = True,
    prune_working_rows: bool = False,
    allow_destructive_clear: bool = False,
    cascade_maint_review: bool = False,
    tombstone_findings: bool = True,
) -> dict:
    cascade_archived: list[str] = []
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        # internal: clear a stale target_branch/target_worktree_path before
        # the write actor derives (and would otherwise raise on) a deleted
        # worktree. The clear returns the pre-clear pointer so the snapshot
        # below can restore the forensic target_branch (the deleted-worktree
        # trail must survive the close, not just live on a prior archive row).
        pre_clear_pointer = clear_worktree_pointer_for_close(conn, resolved_task_ref)
        ctx = _resolve_write_actor(
            conn,
            build_write_actor(
                agent=archive_by,
                branch=archive_branch,
                commit_sha=archive_commit_sha,
            ),
            task_ref=resolved_task_ref,
        )
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        archive_notes = notes or f"Archived {resolved_task_ref}"
        status_flipped = False
        if prune_working_rows and not allow_destructive_clear:
            working_counts = _count_task_rows(conn, resolved_task_ref)
            non_zero_sections = [section for section, count in working_counts.items() if count > 0]
            if non_zero_sections:
                return _envelope(
                    ok=False,
                    tool="archive_task_state",
                    data={
                        "error": f"prune_working_rows would clear handoff rows in sections: {', '.join(non_zero_sections)}. Re-run with allow_destructive_clear=true to confirm.",
                        "existing_counts": working_counts,
                    },
                    task_ref=resolved_task_ref,
                )
        active_row = conn.execute(
            "SELECT status, revision FROM handoff_state WHERE task_ref = ?",
            (resolved_task_ref,),
        ).fetchone()
        if active_row is not None and str(active_row["status"]) != "done":
            from .handoff_state import _set_handoff_state_with_conn

            flip_result = _set_handoff_state_with_conn(
                conn,
                task_ref=resolved_task_ref,
                status="done",
                expected_revision=int(active_row["revision"]),
                actor=build_write_actor(
                    agent=archive_by,
                    branch=archive_branch,
                    commit_sha=archive_commit_sha,
                ),
            )
            if not flip_result.get("ok"):
                flip_data = flip_result.get("data", {}) or {}
                return _envelope(
                    ok=False,
                    tool="archive_task_state",
                    data={
                        "error": flip_data.get("error") or "Failed to flip task status to done before archive.",
                        "status_flip_error": flip_data.get("error"),
                    },
                    task_ref=resolved_task_ref,
                )
            status_flipped = True
            # The flip is an operational note, not a user/forensic archive note:
            # surface it in warnings so `notes` stays the caller's value (or the
            # default), which forensic readers and the regression tests expect.
            warnings = [*warnings, "archive auto-flipped non-done status to done before snapshot"]
        snapshot = _snapshot_with_test_traces(conn, _collect_task_snapshot(conn, resolved_task_ref))
        # Restore the pre-clear target_branch/worktree into the snapshot so the
        # deleted-worktree forensic trail survives an off-canonical close.
        active_snapshot = snapshot.get("active")
        if pre_clear_pointer and isinstance(active_snapshot, dict):
            active_snapshot["target_branch"] = pre_clear_pointer.get("target_branch")
            active_snapshot["target_worktree_path"] = pre_clear_pointer.get("target_worktree_path")
        _persist_task_archive_snapshot(
            conn,
            task_ref=resolved_task_ref,
            snapshot=snapshot,
            ctx=ctx,
            notes=archive_notes,
        )
        tombstoned_findings = 0
        if tombstone_findings:
            tombstoned_findings = _supersede_open_findings_for_archive(conn, resolved_task_ref)
        # Steady-state reclaimer (internal S1): the snapshot persisted
        # above already captured lane_messages, so the archive always retires the
        # task's live brief/inbox rows — their only other deletion path is the
        # opt-in destructive prune below.
        conn.execute("DELETE FROM lane_messages WHERE task_ref = ?", (resolved_task_ref,))
        active_cleared = False
        if clear_active_if_matches:
            deleted = conn.execute("DELETE FROM handoff_state WHERE task_ref = ?", (resolved_task_ref,))
            active_cleared = deleted.rowcount > 0
        pruned = False
        if prune_working_rows:
            for table in (
                "decisions",
                "blockers",
                "next_actions",
                "test_traces",
                "verified_tests",
                "review_findings",
                "worktree_lanes",
                "worker_reports",
                "lane_messages",
                "plan_cursors",
            ):
                conn.execute(f"DELETE FROM {table} WHERE task_ref = ?", (resolved_task_ref,))
            pruned = True
        if cascade_maint_review:
            cascade_archived = _cascade_archive_maint_planning_review_rows(
                conn,
                parent_task_ref=resolved_task_ref,
                ctx=ctx,
            )
    if active_cleared:
        # internal sub-implementation note.3: reap the per-task projection file once
        # its backing handoff_state row is gone. Prevents orphan files
        # under .task-state/current/ that the workspace summary derive
        # path would otherwise have to filter out.
        from .current_task_rendering import (  # noqa: PLC0415
            _remove_per_task_projection,
            _write_workspace_summary_current_task_json,
        )

        _remove_per_task_projection(resolved_task_ref)
        for cascaded_ref in cascade_archived:
            _remove_per_task_projection(cascaded_ref)
        # internal: archive is a terminal transition for the
        # workspace summary. Mirror decisions.py:758 (close_check
        # unconditional flush) so the on-disk CURRENT_TASK.json reflects
        # the post-archive derive immediately. Without this, legacy file
        # readers see a stale summary that still lists the archived task
        # and trip `task_ref_ambiguous` on the next make task-start.
        _write_workspace_summary_current_task_json(unconditional=True)
    return _envelope(
        ok=True,
        tool="archive_task_state",
        data={
            "active_cleared": active_cleared,
            "pruned_working_rows": pruned,
            "allow_destructive_clear": allow_destructive_clear,
            "cascade_archived": cascade_archived,
            "status_flipped_to_done": status_flipped,
            "tombstoned_findings": tombstoned_findings,
        },
        task_ref=resolved_task_ref,
        mutation={
            "entity": "task_archive",
            "operation": "archive",
            "affected_ids": [resolved_task_ref, *cascade_archived],
            "task_revision": None,
        },
        warnings=warnings or None,
    )


def get_archived_task(task_ref: str, include_snapshot: bool = True) -> dict:
    """Read an archived task row from ``task_archives`` by ``task_ref``.

    The handoff dashboard surfaces archive metadata in the cross-task view,
    but there is no MCP-side read tool for inspecting individual archive
    rows directly. Without this, callers (audit scripts, lifecycle tooling,
    review-handoff agents) had to either drop to raw sqlite — guessing
    column names — or roundtrip through ``export_handoff_state`` which only
    works for the currently-active task. internal closes the gap so the
    archive table is reachable through the same envelope as every other
    handoff read.

    Returns the archive row's metadata (``task_ref``, ``archived_at``,
    ``archived_by``, ``archived_branch``, ``archived_commit_sha``,
    ``notes``) plus the parsed snapshot when ``include_snapshot=True``.
    Returns ``ok=False`` with a structured error when no archive row
    exists for the given ``task_ref``.
    """
    normalized_task_ref = _normalize_optional_text(task_ref)
    if not normalized_task_ref:
        return _envelope(
            ok=False,
            tool="get_archived_task",
            data={"error": "task_ref must not be empty."},
        )
    with _get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT task_ref, archived_at, archived_by, archived_branch,
                   archived_commit_sha, notes, snapshot_json
            FROM task_archives WHERE task_ref = ?
            """,
            (normalized_task_ref,),
        ).fetchone()
    if row is None:
        return _envelope(
            ok=False,
            tool="get_archived_task",
            data={
                "error": f"No archived task found for task_ref={normalized_task_ref!r}.",
                "task_ref": normalized_task_ref,
            },
            task_ref=normalized_task_ref,
        )
    archive_metadata: dict[str, object] = {
        "task_ref": str(row["task_ref"]),
        "archived_at": str(row["archived_at"]) if row["archived_at"] is not None else None,
        "archived_by": str(row["archived_by"]) if row["archived_by"] is not None else None,
        "archived_branch": str(row["archived_branch"]) if row["archived_branch"] is not None else None,
        "archived_commit_sha": str(row["archived_commit_sha"]) if row["archived_commit_sha"] is not None else None,
        "notes": str(row["notes"]) if row["notes"] is not None else None,
    }
    data: dict[str, object] = {"archive": archive_metadata}
    if include_snapshot:
        snapshot_json = row["snapshot_json"]
        if snapshot_json is None:
            data["snapshot"] = None
            data["snapshot_parse_error"] = "snapshot_json column is null"
        else:
            try:
                data["snapshot"] = json.loads(snapshot_json)
            except json.JSONDecodeError as exc:
                # Defensive: surface the parse error rather than swallowing it.
                # The archive write path always serialises via json.dumps so a
                # parse failure indicates external tampering or a schema
                # migration mismatch — both worth flagging loudly.
                data["snapshot"] = None
                data["snapshot_parse_error"] = f"snapshot_json failed to parse: {exc}"
    return _envelope(
        ok=True,
        tool="get_archived_task",
        data=data,
        task_ref=normalized_task_ref,
    )


def update_task_status(
    task_ref: str,
    status: str,
    expected_revision: int | None = None,
    actor: WriteActor | None = None,
) -> dict:
    """Update task status for the active task or an archived/inactive task snapshot."""
    if status not in HANDOFF_ACTIVE_STATUSES:
        return _envelope(
            ok=False,
            tool="update_task_status",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(HANDOFF_ACTIVE_STATUSES))}"},
            task_ref=task_ref,
        )

    active_path_envelope: dict[str, object] | None = None
    active_path_succeeded = False

    with _get_db_connection() as conn:
        # internal: status='done' is the close transition. If the linked
        # worktree was already deleted, clear the stale pointer before the
        # write actor derives (and would otherwise raise on) it, so the
        # status-done write make task-finish issues first can complete on the
        # off-canonical path. Non-close transitions stay strict (the guard
        # still fires for them). Commit the clear so the done-path's
        # BEGIN IMMEDIATE below does not nest inside the implicit transaction
        # the clear UPDATE opened.
        if status == "done" and clear_worktree_pointer_for_close(conn, task_ref) is not None:
            conn.commit()
        ctx = _resolve_write_actor(conn, actor, task_ref=task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=task_ref)
        active_row = conn.execute(
            "SELECT * FROM handoff_state WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()

        if active_row is not None:
            from .handoff_state import _set_handoff_state_with_conn

            inferred_revision = expected_revision
            if expected_revision is None and status == "done":
                # internal: elide expected_revision for status='done'.
                # status='done' is an end-of-lifecycle transition where forcing
                # callers to pre-fetch the revision was pure cold-start friction.
                # Other statuses are mid-lifecycle and still require explicit
                # stale-write protection (rejected below by set_handoff_state).
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    current = conn.execute(
                        "SELECT revision FROM handoff_state WHERE task_ref = ?",
                        (task_ref,),
                    ).fetchone()
                    if current is None:
                        conn.execute("ROLLBACK")
                        return _envelope(
                            ok=False,
                            tool="update_task_status",
                            data={"error": "Active row vanished between resolve and update."},
                            task_ref=task_ref,
                        )
                    inferred_revision = int(current["revision"])
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise

            delegated = _set_handoff_state_with_conn(
                conn,
                task_ref=task_ref,
                status=status,
                expected_revision=inferred_revision,
                actor=actor,
            )
            if not delegated.get("ok"):
                return _envelope(
                    ok=False,
                    tool="update_task_status",
                    data=delegated.get("data", {}),
                    task_ref=task_ref,
                )
            active = delegated.get("data", {}).get("active", {}) or {}
            try:
                # update_task_status is a routine state mutation; respect
                # current_task_auto_regen rather than forcing a write.
                _write_current_task_md_for_task(conn, task_ref)
                regen = "ok"
            except Exception as exc:  # noqa: BLE001
                regen = str(exc)
            data: dict[str, object] = {
                "status": status,
                "updated_scope": "active",
                "active": active,
                "current_task_md_regen": "ok" if regen == "ok" else "failed",
            }
            if regen != "ok":
                data["current_task_md_regen_error"] = regen
            active_path_envelope = _envelope(
                ok=True,
                tool="update_task_status",
                data=data,
                task_ref=task_ref,
                mutation={
                    "entity": "handoff_state",
                    "operation": "update_status",
                    "affected_ids": [task_ref],
                    "task_revision": active.get("revision"),
                },
                artifacts=[{"type": "file", "path": "CURRENT_TASK.json"}] if regen == "ok" else None,
            )
            active_path_succeeded = True

    if active_path_succeeded:
        # internal sub-implementation note.3: refresh the per-task projection so a
        # separate connection sees the post-commit row. Done outside
        # the ``with`` block so the writer's own connection observes
        # the committed data.
        from .current_task_rendering import (  # noqa: PLC0415
            _write_per_task_projection,
            _write_workspace_summary_current_task_json,
        )

        _write_per_task_projection(task_ref)
        # internal: terminal-status transitions flush the
        # workspace summary unconditionally. Gated on LIVE_ACTIVE_STATUSES
        # complement (per shared_primitives.LIVE_ACTIVE_STATUSES) so any
        # future expansion of the terminal vocabulary is honored without
        # touching this branch. Mirrors decisions.py:758 (close_check)
        # and the archive flush above. Live-to-live transitions stay
        # routine-gated on current_task_auto_regen via the existing
        # _write_current_task_md_for_task call in the active path.
        if status not in LIVE_ACTIVE_STATUSES:
            _write_workspace_summary_current_task_json(unconditional=True)
        assert active_path_envelope is not None
        return active_path_envelope

    with _get_db_connection() as conn:
        ctx = _resolve_write_actor(conn, actor, task_ref=task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=task_ref)
        archive_row = conn.execute(
            "SELECT snapshot_json FROM task_archives WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
        if archive_row is None:
            return _envelope(
                ok=False,
                tool="update_task_status",
                data={
                    "error": "Task is neither active nor archived; switch to it or archive it before updating its inactive status.",
                },
                task_ref=task_ref,
            )

        try:
            snapshot = json.loads(archive_row["snapshot_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return _envelope(
                ok=False,
                tool="update_task_status",
                data={
                    "error": "Archived snapshot is invalid JSON.",
                },
                task_ref=task_ref,
            )

        active_block = snapshot.get("active")
        if not isinstance(active_block, dict):
            active_block = {"task_ref": task_ref}
            snapshot["active"] = active_block
        active_block["task_ref"] = task_ref
        active_block["status"] = status
        active_block["updated_by"] = ctx.agent
        active_block["updated_branch"] = ctx.branch
        active_block["updated_commit_sha"] = ctx.commit_sha
        _persist_task_archive_snapshot(
            conn,
            task_ref=task_ref,
            snapshot=snapshot,
            ctx=ctx,
            notes=f"Updated archived status to {status}",
        )

        from .shared_primitives import _resolve_workspace_handoff_row  # noqa: PLC0415

        try:
            active_task_row = _resolve_workspace_handoff_row(conn)
        except ValueError:
            active_task_row = None
        regen_result = "skipped"
        if active_task_row is not None:
            try:
                _write_current_task_md_for_task(conn, str(active_task_row["task_ref"]))
                regen_result = "ok"
            except Exception as exc:  # noqa: BLE001
                regen_result = str(exc)

        data_archived: dict[str, object] = {
            "status": status,
            "updated_scope": "archived",
            "current_task_md_regen": "ok" if regen_result == "ok" else regen_result,
        }
        if regen_result not in {"ok", "skipped"}:
            data_archived["current_task_md_regen"] = "failed"
            data_archived["current_task_md_regen_error"] = regen_result
        return _envelope(
            ok=True,
            tool="update_task_status",
            data=data_archived,
            task_ref=task_ref,
            mutation={
                "entity": "task_archive",
                "operation": "update_status",
                "affected_ids": [task_ref],
                "task_revision": None,
            },
            artifacts=[{"type": "file", "path": "CURRENT_TASK.json"}] if regen_result == "ok" else None,
            warnings=warnings or None,
        )


def switch_task(
    task_ref: str,
    objective: str | None = None,
    focus: str | None = None,
    status: str = "in_progress",
    actor: WriteActor | None = None,
    target_branch: str | None = None,
) -> dict:
    """Ensure a handoff row exists for ``task_ref`` without evicting other rows.

    If the target task was previously archived, its objective is restored
    automatically. Pass *objective* explicitly to override.
    """
    if status not in HANDOFF_ACTIVE_STATUSES:
        return _envelope(
            ok=False,
            tool="switch_task",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(HANDOFF_ACTIVE_STATUSES))}"},
            task_ref=task_ref,
        )

    with _get_db_connection() as conn:
        ctx = _resolve_write_actor(conn, actor, task_ref=task_ref)
        existing = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
        archived = None
        if existing is None:
            archived = conn.execute("SELECT task_ref FROM task_archives WHERE task_ref = ?", (task_ref,)).fetchone()
        warnings = collect_target_context_warnings(
            conn,
            ctx,
            # Resolve drift warnings against the currently active row before the
            # switch. switch_task exists to replace that pointer, so branch
            # mismatch is surfaced as guidance rather than a hard failure here.
            task_ref=task_ref if existing is not None or archived is not None else None,
            enforce_branch=False,
        )

        # The task already has an active row; update only explicitly requested fields.
        if existing is not None:
            conn.execute(
                """
                UPDATE handoff_state
                SET objective = ?,
                    focus = ?,
                    status = ?,
                    target_branch = ?,
                    revision = revision + 1,
                    updated_at = datetime('now'),
                    updated_by = ?,
                    updated_branch = ?,
                    updated_commit_sha = ?
                WHERE task_ref = ?
                """,
                (
                    objective if objective is not None else existing["objective"],
                    focus if focus is not None else existing["focus"],
                    status,
                    target_branch if target_branch is not None else existing["target_branch"],
                    ctx.agent,
                    ctx.branch,
                    ctx.commit_sha,
                    task_ref,
                ),
            )
            active = _row_to_dict(
                conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
            )
            return _envelope(
                ok=True,
                tool="switch_task",
                data={"already_active": True, "active": active},
                task_ref=task_ref,
                warnings=warnings or None,
            )

        # Resolve objective and target_branch for the target task.
        resolved_objective = objective
        resolved_target_branch = target_branch
        resolved_focus = focus
        archive_row = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref = ?", (task_ref,)).fetchone()
        if archive_row is not None:
            try:
                snapshot = json.loads(archive_row["snapshot_json"])
                active_block = snapshot.get("active")
                if isinstance(active_block, dict):
                    if resolved_objective is None and active_block.get("objective"):
                        resolved_objective = active_block["objective"]
                    if resolved_target_branch is None and active_block.get("target_branch"):
                        resolved_target_branch = active_block["target_branch"]
                    if focus is None:
                        resolved_focus = None
            except (json.JSONDecodeError, TypeError):
                pass
        if resolved_objective is None:
            return _envelope(
                ok=False,
                tool="switch_task",
                data={
                    "error": "Cannot determine objective for the target task. Pass --objective explicitly or archive the current task first.",
                },
                task_ref=task_ref,
            )

        archived_previous = False
        previous_task_ref = None

        conn.execute(
            """
            INSERT INTO handoff_state (
                id, task_ref, objective, focus, status, target_branch,
                revision, updated_at, updated_by, updated_branch, updated_commit_sha
            ) VALUES (NULL, ?, ?, ?, ?, ?, 0, datetime('now'), ?, ?, ?)
            """,
            (
                task_ref,
                resolved_objective,
                resolved_focus,
                status,
                resolved_target_branch,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )

        active = _row_to_dict(conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone())
        if active is None:
            return _envelope(
                ok=False,
                tool="switch_task",
                data={"error": "Target handoff state missing after task switch."},
                task_ref=task_ref,
            )
        regen_error: str | None = None
        try:
            # switch_task is a routine state-mutation, gated by
            # current_task_auto_regen. Use unconditional=True only for
            # explicit export/import round-trips.
            _write_current_task_md_for_task(conn, task_ref)
        except Exception as exc:  # noqa: BLE001
            regen_error = str(exc)
        switch_data: dict[str, object] = {
            "switched": True,
            "active": active,
            "archived_previous": archived_previous,
            "previous_task_ref": previous_task_ref,
            "current_task_md_regen": "failed" if regen_error else "ok",
        }
        if regen_error is not None:
            switch_data["current_task_md_regen_error"] = regen_error
        return _envelope(
            ok=True,
            tool="switch_task",
            data=switch_data,
            task_ref=task_ref,
            mutation={
                "entity": "handoff_state",
                "operation": "switch_task",
                "affected_ids": [task_ref],
                "task_revision": active.get("revision"),
            },
            artifacts=[{"type": "file", "path": "CURRENT_TASK.json"}] if regen_error is None else None,
            warnings=warnings or None,
        )
