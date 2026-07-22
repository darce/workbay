"""Decisions domain module.

Contains record_decision, update_next_actions, list_next_actions,
record_test_result, report_blocker, handoff_close_check,
and integrity helpers.
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import sqlite3
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .concept_embed_hook import embed_concept_on_write
from .current_task_rendering import _collect_task_snapshot
from .enums import ActionStatus, BlockerStatus, FindingStatus
from .projection_event_dedupe import claim_projection_event, complete_projection_event
from .shared_primitives import (
    ACTION_STATUSES,
    _current_task_path,
    _decision_rationale_size_warning,
    _envelope,
    _has_structured_slice_summary,
    _normalize_optional_text,
    _resolve_task_ref,
    _row_to_dict,
    _summarize_test_result,
    _validate_decision_payload,
)
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings
from .slice_decision import classify_decision_id, is_slice_complete_decision


def _normalize_changed_files_payload(changed_files: Sequence[object] | None) -> tuple[list[str] | None, str | None]:
    """Validate and normalize optional changed-file paths for decision rows."""

    if changed_files is None:
        return None, None

    normalized_paths: list[str] = []
    for raw_path in changed_files:
        if not isinstance(raw_path, str):
            return None, "changed_files must be a list of non-empty monorepo-relative path strings."
        candidate = raw_path.strip().replace("\\", "/")
        pure_path = PurePosixPath(candidate)
        if (
            not candidate
            or pure_path.is_absolute()
            or not pure_path.parts
            or any(part == ".." for part in pure_path.parts)
        ):
            return None, "changed_files must contain only non-empty monorepo-relative paths."
        normalized_paths.append("/".join(part for part in pure_path.parts if part not in ("", ".")))
    return normalized_paths, None


def backfill_latest_slice_changed_files(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    commit_sha: str,
    changed_files: Sequence[str],
) -> tuple[bool, str | None]:
    """Patch the latest slice-complete decision with derived ``changed_files``."""

    from .slice_decision import is_slice_complete_decision  # noqa: PLC0415

    # Scan all of the task's decisions (newest first) for the latest
    # slice-complete row. A previous `LIMIT 50` silently missed the slice_complete
    # decision on tasks with >50 interleaved decisions after it, leaving
    # changed_files_json NULL and breaking downstream checklist projection for
    # that slice. Decision counts per task are bounded (dozens-hundreds), so the
    # unbounded scan is cheap.
    rows = conn.execute(
        """
        SELECT id, decision, changed_files_json, commit_sha
        FROM decisions
        WHERE task_ref = ?
        ORDER BY id DESC
        """,
        (task_ref,),
    ).fetchall()
    target = None
    for candidate in rows:
        if is_slice_complete_decision(str(candidate["decision"])):
            target = candidate
            break
    if target is None:
        return False, "no_slice_complete_decision"
    normalized, err = _normalize_changed_files_payload(list(changed_files))
    if err is not None:
        return False, err
    paths = normalized or []
    payload = json.dumps(paths)
    if str(target["changed_files_json"] or "[]") == payload and str(target["commit_sha"] or "") == commit_sha:
        return True, None
    conn.execute(
        """
        UPDATE decisions
        SET changed_files_json = ?, commit_sha = ?
        WHERE id = ?
        """,
        (payload, commit_sha, int(target["id"])),
    )
    return True, None


def _current_task_revision(conn: sqlite3.Connection, task_ref: str) -> int | None:
    row = conn.execute(
        "SELECT revision FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    return int(row["revision"]) if row is not None else None


def _normalize_test_traces_payload(
    traces: Sequence[object] | None,
    *,
    fallback_result: str | None,
) -> tuple[list[str], str | None]:
    if traces is None:
        return ([fallback_result] if isinstance(fallback_result, str) and fallback_result != "" else []), None

    normalized: list[str] = []
    for raw_trace in traces:
        if not isinstance(raw_trace, str):
            return [], "traces must be a list of raw trace strings."
        normalized.append(raw_trace)
    return normalized, None


def record_decision(
    session: str,
    decision: str,
    rationale: str | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    event_id: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    changed_files: list[str] | None = None,
    slice_number: int | None = None,
    decision_origin: str | None = None,
) -> dict:
    validation_error = _validate_decision_payload(decision, rationale)
    if validation_error is not None:
        return _envelope(ok=False, tool="record_decision", data={"error": validation_error})
    if decision_origin is not None and decision_origin not in {"agent", "system"}:
        return _envelope(
            ok=False,
            tool="record_decision",
            data={
                "error": (
                    f"decision_origin must be 'agent' or 'system' (or omitted); got {decision_origin!r}"
                )
            },
        )
    normalized_changed_files, changed_files_error = _normalize_changed_files_payload(changed_files)
    if changed_files_error is not None:
        return _envelope(ok=False, tool="record_decision", data={"error": changed_files_error})
    with _get_db_connection() as conn:
        result = _record_decision_with_conn(
            conn,
            session=session,
            decision=decision,
            rationale=rationale,
            actor=actor,
            task_ref=task_ref,
            event_id=event_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            changed_files=normalized_changed_files,
            slice_number=slice_number,
            decision_origin=decision_origin,
        )
    mutation = m if isinstance((m := result.get("mutation")), dict) else {}
    if result.get("ok") and mutation.get("operation") == "insert":
        affected_ids = mutation.get("affected_ids") or []
        if affected_ids:
            scope = s if isinstance((s := result.get("scope")), dict) else {}
            data = d if isinstance((d := result.get("data")), dict) else {}
            resolved_task_ref = scope.get("task_ref") or (data.get("decision") or {}).get("task_ref")
            if resolved_task_ref is not None:
                embed_concept_on_write("decision.rationale", affected_ids[0], resolved_task_ref, rationale)
    return result


def _record_decision_with_conn(
    conn: sqlite3.Connection,
    *,
    session: str,
    decision: str,
    rationale: str | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    event_id: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    changed_files: list[str] | None = None,
    slice_number: int | None = None,
    decision_origin: str | None = None,
) -> dict:
    if decision_origin is not None and decision_origin not in {"agent", "system"}:
        return _envelope(
            ok=False,
            tool="record_decision",
            data={
                "error": (
                    f"decision_origin must be 'agent' or 'system' (or omitted); got {decision_origin!r}"
                )
            },
        )
    resolved_task_ref = _resolve_task_ref(conn, task_ref)
    existing_event = claim_projection_event(
        conn,
        event_id=event_id,
        tool_name="record_event.decision",
        target_table="decisions",
        task_ref=resolved_task_ref,
    )
    if existing_event is not None:
        existing_decision = None
        if existing_event["target_table"] == "decisions" and existing_event["target_id"] is not None:
            existing_decision = conn.execute(
                "SELECT * FROM decisions WHERE id = ?",
                (existing_event["target_id"],),
            ).fetchone()
        decision_row = _row_to_dict(existing_decision)
        event_task_ref = existing_event["task_ref"] or resolved_task_ref
        task_revision = _current_task_revision(conn, str(event_task_ref)) if event_task_ref else None
        affected_id = int(existing_event["target_id"]) if existing_event["target_id"] is not None else None
        return _envelope(
            ok=True,
            tool="record_decision",
            data={"decision": decision_row, "idempotent": True},
            task_ref=str(event_task_ref) if event_task_ref else None,
            mutation={
                "entity": "decision",
                "operation": "noop",
                "affected_ids": [affected_id] if affected_id is not None else [],
                "task_revision": task_revision,
            },
        )
    ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
    warnings: list[str] = []
    rationale_warning = _decision_rationale_size_warning(decision, rationale)
    if rationale_warning is not None:
        warnings.append(rationale_warning)
    if not ctx.model and not ctx.model_label:
        warnings.append(
            "actor is missing model/model_label; decision will render without model identity. Pass actor.model and actor.model_label for accurate provenance."
        )
    warnings.extend(collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref))
    changed_files_json = json.dumps(changed_files) if changed_files is not None else "[]"
    cur = conn.execute(
        """
        INSERT INTO decisions (
            task_ref, lane_id, session, decision, rationale, agent, harness,
            model, model_label, reasoning_level,
            input_tokens, output_tokens, total_tokens,
            branch, commit_sha, changed_files_json, slice_number, decision_origin, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(task_ref, decision, session) DO NOTHING
        """,
        (
            resolved_task_ref,
            ctx.lane_id,
            session,
            decision,
            rationale,
            ctx.agent,
            ctx.harness,
            ctx.model,
            ctx.model_label,
            ctx.reasoning_level,
            input_tokens,
            output_tokens,
            total_tokens,
            ctx.branch,
            ctx.commit_sha,
            changed_files_json,
            slice_number,
            decision_origin,
        ),
    )
    if cur.lastrowid:
        decision_row = _row_to_dict(conn.execute("SELECT * FROM decisions WHERE id = ?", (cur.lastrowid,)).fetchone())
        mutation_operation = "insert"
        affected_id = cur.lastrowid
    else:
        existing = conn.execute(
            "SELECT * FROM decisions WHERE task_ref = ? AND decision = ? AND session = ?",
            (resolved_task_ref, decision, session),
        ).fetchone()
        decision_row = _row_to_dict(existing)
        mutation_operation = "noop"
        affected_id = int(existing["id"]) if existing is not None else None
    if affected_id is not None:
        complete_projection_event(
            conn,
            event_id=event_id,
            target_table="decisions",
            target_id=affected_id,
            task_ref=resolved_task_ref,
        )
    task_revision = _current_task_revision(conn, resolved_task_ref)
    return _envelope(
        ok=True,
        tool="record_decision",
        data={"decision": decision_row, "idempotent": mutation_operation == "noop"},
        task_ref=resolved_task_ref,
        mutation={
            "entity": "decision",
            "operation": mutation_operation,
            "affected_ids": [affected_id] if affected_id is not None else [],
            "task_revision": task_revision,
        },
        warnings=warnings if warnings else None,
    )


def update_next_actions(
    operation: str,
    action_id: int | None = None,
    action: str | None = None,
    priority: int | None = None,
    status: str | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
) -> dict:
    valid_operations = {"add", "update", "complete", "skip"}
    if operation not in valid_operations:
        return _envelope(
            ok=False,
            tool="update_next_actions",
            data={"error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"},
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)
        if operation == "add":
            if not action:
                return _envelope(
                    ok=False,
                    tool="update_next_actions",
                    data={"error": "action is required for add."},
                    task_ref=resolved_task_ref,
                )
            cur = conn.execute(
                """
                INSERT INTO next_actions (task_ref, lane_id, action, priority, status, agent, branch, commit_sha, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (
                    resolved_task_ref,
                    ctx.lane_id,
                    action,
                    priority if priority is not None else 100,
                    ctx.agent,
                    ctx.branch,
                    ctx.commit_sha,
                ),
            )
            action_row = _row_to_dict(
                conn.execute("SELECT * FROM next_actions WHERE id = ?", (cur.lastrowid,)).fetchone()
            )
            return _envelope(
                ok=True,
                tool="update_next_actions",
                data={"operation": operation, "action": action_row},
                task_ref=resolved_task_ref,
                mutation={
                    "entity": "next_action",
                    "operation": "insert",
                    "affected_ids": [cur.lastrowid],
                    "task_revision": task_revision,
                },
                warnings=warnings or None,
            )
        if action_id is None:
            return _envelope(
                ok=False,
                tool="update_next_actions",
                data={"error": "action_id is required for update/complete/skip."},
                task_ref=resolved_task_ref,
            )
        existing = conn.execute(
            "SELECT * FROM next_actions WHERE id = ? AND task_ref = ?", (action_id, resolved_task_ref)
        ).fetchone()
        if existing is None:
            return _envelope(
                ok=False,
                tool="update_next_actions",
                data={"error": "Action not found for task_ref."},
                task_ref=resolved_task_ref,
            )
        if operation == "update":
            if action is None and priority is None and status is None:
                return _envelope(
                    ok=False,
                    tool="update_next_actions",
                    data={"error": "At least one of action, priority, or status is required for update."},
                    task_ref=resolved_task_ref,
                )
            use_status = status if status is not None else str(existing["status"])
            if use_status not in ACTION_STATUSES:
                return _envelope(
                    ok=False,
                    tool="update_next_actions",
                    data={"error": "Invalid status value."},
                    task_ref=resolved_task_ref,
                )
            conn.execute(
                "UPDATE next_actions SET action = ?, priority = ?, status = ?, agent = ?, branch = ?, commit_sha = ?, lane_id = COALESCE(lane_id, ?), updated_at = datetime('now') WHERE id = ? AND task_ref = ?",
                (
                    action if action is not None else str(existing["action"]),
                    priority if priority is not None else int(existing["priority"]),
                    use_status,
                    ctx.agent,
                    ctx.branch,
                    ctx.commit_sha,
                    ctx.lane_id,
                    action_id,
                    resolved_task_ref,
                ),
            )
        elif operation == "complete":
            conn.execute(
                "UPDATE next_actions SET status = 'done', agent = ?, branch = ?, commit_sha = ?, lane_id = COALESCE(lane_id, ?), updated_at = datetime('now') WHERE id = ? AND task_ref = ?",
                (ctx.agent, ctx.branch, ctx.commit_sha, ctx.lane_id, action_id, resolved_task_ref),
            )
        else:
            conn.execute(
                "UPDATE next_actions SET status = 'skipped', agent = ?, branch = ?, commit_sha = ?, lane_id = COALESCE(lane_id, ?), updated_at = datetime('now') WHERE id = ? AND task_ref = ?",
                (ctx.agent, ctx.branch, ctx.commit_sha, ctx.lane_id, action_id, resolved_task_ref),
            )
        action_row = _row_to_dict(conn.execute("SELECT * FROM next_actions WHERE id = ?", (action_id,)).fetchone())
        return _envelope(
            ok=True,
            tool="update_next_actions",
            data={"operation": operation, "action": action_row},
            task_ref=resolved_task_ref,
            mutation={
                "entity": "next_action",
                "operation": operation,
                "affected_ids": [action_id],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


def list_next_actions(
    task_ref: str | None = None,
    lane_id: str | None = None,
    status: str = "all",
    limit: int = 100,
    offset: int = 0,
) -> dict:
    valid_statuses = {"all", *ACTION_STATUSES}
    if status not in valid_statuses:
        return _envelope(
            ok=False,
            tool="list_next_actions",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"},
        )
    limit = max(1, limit)
    offset = max(0, offset)
    normalized_lane_id = _normalize_optional_text(lane_id)
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if normalized_lane_id is not None:
            where_sql += " AND lane_id = ?"
            params.append(normalized_lane_id)
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total = int(
            conn.execute(f"SELECT COUNT(*) AS count FROM next_actions WHERE {where_sql}", tuple(params)).fetchone()[
                "count"
            ]
        )
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM next_actions WHERE {where_sql} ORDER BY priority ASC, updated_at DESC, id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        ]
        return _envelope(
            ok=True,
            tool="list_next_actions",
            data={
                "lane_id": normalized_lane_id,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": offset + len(rows) < total,
                "actions": rows,
            },
            task_ref=resolved_task_ref,
        )


def record_test_result(
    session: str,
    command: str,
    passed: bool,
    result: str | None = None,
    traces: Sequence[str] | None = None,
    exit_code: int | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
    event_id: str | None = None,
) -> dict:
    summarized_result = _summarize_test_result(result)
    normalized_traces, traces_error = _normalize_test_traces_payload(traces, fallback_result=result)
    if traces_error is not None:
        return _envelope(ok=False, tool="record_test_result", data={"error": traces_error})
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing_event = claim_projection_event(
            conn,
            event_id=event_id,
            tool_name="record_event.test_result",
            target_table="verified_tests",
            task_ref=resolved_task_ref,
        )
        if existing_event is not None:
            existing_test = None
            if existing_event["target_table"] == "verified_tests" and existing_event["target_id"] is not None:
                existing_test = conn.execute(
                    "SELECT * FROM verified_tests WHERE id = ?",
                    (existing_event["target_id"],),
                ).fetchone()
            test_row = _row_to_dict(existing_test)
            event_task_ref = existing_event["task_ref"] or resolved_task_ref
            task_revision = _current_task_revision(conn, str(event_task_ref)) if event_task_ref else None
            affected_id = int(existing_event["target_id"]) if existing_event["target_id"] is not None else None
            return _envelope(
                ok=True,
                tool="record_test_result",
                data={"test": test_row, "idempotent": True},
                task_ref=str(event_task_ref) if event_task_ref else None,
                mutation={
                    "entity": "verified_test",
                    "operation": "noop",
                    "affected_ids": [affected_id] if affected_id is not None else [],
                    "task_revision": task_revision,
                },
            )
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)
        cur = conn.execute(
            """
            INSERT INTO verified_tests (task_ref, lane_id, command, passed, exit_code, result, session, agent, branch, commit_sha, verified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                ctx.lane_id,
                command,
                1 if passed else 0,
                exit_code,
                summarized_result,
                session,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
        for trace_order, trace in enumerate(normalized_traces):
            conn.execute(
                """
                INSERT INTO test_traces (verified_test_id, task_ref, trace_order, trace, created_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                """,
                (cur.lastrowid, resolved_task_ref, trace_order, trace),
            )
        test_row = _row_to_dict(conn.execute("SELECT * FROM verified_tests WHERE id = ?", (cur.lastrowid,)).fetchone())
        if test_row is not None:
            test_row["trace_count"] = len(normalized_traces)
        complete_projection_event(
            conn,
            event_id=event_id,
            target_table="verified_tests",
            target_id=cur.lastrowid,
            task_ref=resolved_task_ref,
        )
        return _envelope(
            ok=True,
            tool="record_test_result",
            data={"test": test_row, "idempotent": False},
            task_ref=resolved_task_ref,
            mutation={
                "entity": "verified_test",
                "operation": "insert",
                "affected_ids": [cur.lastrowid],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


def report_blocker(
    operation: str,
    description: str | None = None,
    blocker_id: int | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
) -> dict:
    """Public entry: delegate, then embed a newly added blocker description after it commits."""
    result = _report_blocker_impl(
        operation,
        description=description,
        blocker_id=blocker_id,
        actor=actor,
        task_ref=task_ref,
    )
    if operation == "add" and result.get("ok"):
        blocker = result.get("data", {}).get("blocker") or {}
        embed_concept_on_write(
            "blocker.description", blocker.get("id"), str(blocker.get("task_ref")), blocker.get("description")
        )
    return result


def _report_blocker_impl(
    operation: str,
    description: str | None = None,
    blocker_id: int | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
) -> dict:
    valid_operations = {"add", "resolve", "reopen"}
    if operation not in valid_operations:
        return _envelope(
            ok=False,
            tool="report_blocker",
            data={"error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"},
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)
        if operation == "add":
            if not description:
                return _envelope(
                    ok=False,
                    tool="report_blocker",
                    data={"error": "description is required for add."},
                    task_ref=resolved_task_ref,
                )
            cur = conn.execute(
                """
                INSERT INTO blockers (task_ref, lane_id, description, status, agent, branch, commit_sha, resolved_at, created_at)
                VALUES (?, ?, ?, 'open', ?, ?, ?, NULL, datetime('now'))
                """,
                (resolved_task_ref, ctx.lane_id, description, ctx.agent, ctx.branch, ctx.commit_sha),
            )
            blocker_row = _row_to_dict(conn.execute("SELECT * FROM blockers WHERE id = ?", (cur.lastrowid,)).fetchone())
            return _envelope(
                ok=True,
                tool="report_blocker",
                data={"operation": operation, "blocker": blocker_row},
                task_ref=resolved_task_ref,
                mutation={
                    "entity": "blocker",
                    "operation": "insert",
                    "affected_ids": [cur.lastrowid],
                    "task_revision": task_revision,
                },
                warnings=warnings or None,
            )
        if blocker_id is None:
            return _envelope(
                ok=False,
                tool="report_blocker",
                data={"error": "blocker_id is required for resolve/reopen."},
                task_ref=resolved_task_ref,
            )
        existing = conn.execute(
            "SELECT * FROM blockers WHERE id = ? AND task_ref = ?", (blocker_id, resolved_task_ref)
        ).fetchone()
        if existing is None:
            return _envelope(
                ok=False,
                tool="report_blocker",
                data={"error": "Blocker not found for task_ref."},
                task_ref=resolved_task_ref,
            )
        if operation == "resolve":
            conn.execute(
                "UPDATE blockers SET status = 'resolved', resolved_at = datetime('now'), agent = ?, branch = ?, commit_sha = ?, lane_id = COALESCE(lane_id, ?) WHERE id = ? AND task_ref = ?",
                (ctx.agent, ctx.branch, ctx.commit_sha, ctx.lane_id, blocker_id, resolved_task_ref),
            )
        else:
            conn.execute(
                "UPDATE blockers SET status = 'open', resolved_at = NULL, agent = ?, branch = ?, commit_sha = ?, lane_id = COALESCE(lane_id, ?) WHERE id = ? AND task_ref = ?",
                (ctx.agent, ctx.branch, ctx.commit_sha, ctx.lane_id, blocker_id, resolved_task_ref),
            )
        blocker_row = _row_to_dict(conn.execute("SELECT * FROM blockers WHERE id = ?", (blocker_id,)).fetchone())
        return _envelope(
            ok=True,
            tool="report_blocker",
            data={"operation": operation, "blocker": blocker_row},
            task_ref=resolved_task_ref,
            mutation={
                "entity": "blocker",
                "operation": operation,
                "affected_ids": [blocker_id],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


def _collect_task_provenance_integrity(conn: sqlite3.Connection, task_ref: str) -> dict:
    table_checks: dict[str, dict[str, object]] = {}
    total_issues = 0
    for table_name in (
        "decisions",
        "blockers",
        "next_actions",
        "verified_tests",
        "review_findings",
        "worker_reports",
        "lane_messages",
    ):
        rows = conn.execute(
            f"SELECT id AS row_id, agent, branch, commit_sha FROM {table_name} WHERE task_ref = ? AND (agent IS NULL OR TRIM(agent) = '' OR branch IS NULL OR TRIM(branch) = '') ORDER BY id DESC",
            (task_ref,),
        ).fetchall()
        items = [
            {
                "row_id": int(row["row_id"]),
                "agent": row["agent"],
                "branch": row["branch"],
                "commit_sha": row["commit_sha"],
            }
            for row in rows
        ]
        table_checks[table_name] = {"count": len(items), "items": items}
        total_issues += len(items)
    active_row = conn.execute(
        "SELECT updated_by, updated_branch, updated_commit_sha FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    active_missing = None
    if active_row is not None:
        missing = (
            _normalize_optional_text(active_row["updated_by"]) is None
            or _normalize_optional_text(active_row["updated_branch"]) is None
        )
        active_missing = {
            "count": 1 if missing else 0,
            "is_violation": missing,
            "updated_by": active_row["updated_by"],
            "updated_branch": active_row["updated_branch"],
            "updated_commit_sha": active_row["updated_commit_sha"],
        }
        total_issues += 1 if missing else 0
    return {
        "healthy": total_issues == 0,
        "total_issues": total_issues,
        "tables": table_checks,
        "active_state": active_missing,
    }


def _slice_complete_decision_rows(
    conn: sqlite3.Connection,
    task_ref: str,
    *,
    commit_sha: str | None = None,
) -> list:
    """Load slice-complete decision rows for a task, optionally exact-commit filtered.

    Escape the literal underscores: '_' is a single-char SQL LIKE wildcard,
    so an unescaped pattern false-matches unrelated decisions.
    """
    if commit_sha is not None:
        rows = conn.execute(
            r"""
            SELECT id, decision, rationale, created_at, commit_sha, branch
            FROM decisions
            WHERE task_ref = ?
              AND commit_sha = ?
              AND (
                decision LIKE 'slice\_complete\_%' ESCAPE '\'
                OR decision LIKE '%\_slice\_complete\_%' ESCAPE '\'
              )
            ORDER BY id DESC
            """,
            (task_ref, commit_sha),
        ).fetchall()
    else:
        rows = conn.execute(
            r"""
            SELECT id, decision, rationale, created_at, commit_sha, branch
            FROM decisions
            WHERE task_ref = ?
              AND (
                decision LIKE 'slice\_complete\_%' ESCAPE '\'
                OR decision LIKE '%\_slice\_complete\_%' ESCAPE '\'
              )
            ORDER BY id DESC
            """,
            (task_ref,),
        ).fetchall()
    return [row for row in rows if is_slice_complete_decision(str(row["decision"]))]


def _decision_commit_is_reachable_ancestor(decision_commit: str | None, head_commit: str | None) -> bool:
    """True when the decision commit equals HEAD or is a git ancestor of HEAD (T7)."""
    from .shared_write_context import _git_is_ancestor  # noqa: PLC0415

    return _git_is_ancestor(decision_commit, head_commit) is True


def _decision_commit_ancestor_distance(decision_commit: str | None, head_commit: str | None) -> int | None:
    """Commits between the satisfying decision commit and HEAD (S5-A-01).

    0 for an exact-HEAD match; N for a reachable ancestor N commits behind HEAD;
    None when git cannot answer (missing SHAs, non-git workspace, unknown objects).
    """
    from .shared_primitives import _normalize_optional_text as _norm  # noqa: PLC0415
    from .shared_write_context import _run_cmd  # noqa: PLC0415

    ancestor = _norm(decision_commit)
    head = _norm(head_commit)
    if ancestor is None or head is None:
        return None
    if ancestor == head:
        return 0
    try:
        proc = _run_cmd(["git", "rev-list", "--count", f"{ancestor}..{head}"])
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    try:
        return int(str(proc.stdout).strip())
    except (TypeError, ValueError):
        return None


def _collect_commit_verification_data(
    conn: sqlite3.Connection,
    task_ref: str,
    commit_sha: str | None,
    require_fresh_tests: bool,
    require_summary: bool,
) -> tuple[int, list, list, str | None, str | None, int | None]:
    """Collect fresh-test count and commit-level slice decisions.

    T7: a structured slice decision satisfies close-check when its commit is
    the exact HEAD *or* a reachable ancestor of HEAD (same lineage). S5-A-01
    bounds the ancestor gate: only the MOST RECENT structured slice-complete
    decision for the task may satisfy it (an older ancestor decision cannot
    green the gate past a newer one on a foreign commit), and the receipt
    surfaces ``ancestor_distance`` (commits between the satisfying decision
    commit and HEAD; 0 for the preferred exact-HEAD match). Returns
    ``(fresh_test_count, slice_decisions, structured, satisfied_by_commit,
    match_kind, ancestor_distance)`` where ``match_kind`` is ``"exact"``,
    ``"reachable_ancestor"``, or ``None`` when no structured decision
    qualifies.
    """
    fresh_test_count = 0
    commit_slice_decisions: list = []
    match_kind: str | None = None
    satisfied_by_commit: str | None = None
    ancestor_distance: int | None = None
    if require_fresh_tests and commit_sha is not None:
        fresh_test_count = int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM verified_tests WHERE task_ref = ? AND commit_sha = ?",
                (task_ref, commit_sha),
            ).fetchone()["count"]
        )
    if require_summary and commit_sha is not None:
        # Fast path: exact HEAD match (historical contract).
        exact_rows = _slice_complete_decision_rows(conn, task_ref, commit_sha=commit_sha)
        exact_structured = [row for row in exact_rows if _has_structured_slice_summary(str(row["rationale"] or ""))]
        if exact_structured:
            commit_slice_decisions = exact_rows
            structured = exact_structured
            match_kind = "exact"
            satisfied_by_commit = _normalize_optional_text(exact_structured[0]["commit_sha"]) or commit_sha
            return fresh_test_count, commit_slice_decisions, structured, satisfied_by_commit, match_kind, 0

        # T7 / S5-A-01: the ancestor gate is bounded to the MOST RECENT
        # structured slice-complete decision for the task. Only that decision
        # may satisfy the gate via reachable-ancestor (finalize-plan /
        # auto-fix / merge commits legitimately advance HEAD past the last
        # close_slice); an older reachable decision must NOT green the gate
        # when a newer structured decision exists elsewhere.
        candidate_rows = _slice_complete_decision_rows(conn, task_ref, commit_sha=None)
        structured_candidates = [
            row for row in candidate_rows if _has_structured_slice_summary(str(row["rationale"] or ""))
        ]
        latest_structured = structured_candidates[0] if structured_candidates else None
        if latest_structured is not None:
            decision_commit = _normalize_optional_text(latest_structured["commit_sha"])
            if decision_commit is not None and _decision_commit_is_reachable_ancestor(decision_commit, commit_sha):
                match_kind = "reachable_ancestor"
                satisfied_by_commit = decision_commit
                ancestor_distance = _decision_commit_ancestor_distance(decision_commit, commit_sha)
                commit_slice_decisions = [latest_structured]
                return (
                    fresh_test_count,
                    commit_slice_decisions,
                    [latest_structured],
                    satisfied_by_commit,
                    match_kind,
                    ancestor_distance,
                )
        # Fall through with empty structured (exact miss + most-recent
        # structured decision absent or not a reachable ancestor of HEAD).
        commit_slice_decisions = exact_rows
        return fresh_test_count, commit_slice_decisions, [], None, None, None
    structured = [row for row in commit_slice_decisions if _has_structured_slice_summary(str(row["rationale"] or ""))]
    return fresh_test_count, commit_slice_decisions, structured, satisfied_by_commit, match_kind, ancestor_distance


@dataclass(frozen=True)
class CloseCheckCommandResult:
    command: str
    exit_code: int | None
    stdout: str | None
    stderr: str | None
    launch_error: str | None = None
    timed_out: bool = False

    @property
    def failed(self) -> bool:
        if self.launch_error is not None or self.timed_out:
            return True
        return self.exit_code not in (0, None)


def _resolve_close_check_cwd(conn: sqlite3.Connection, task_ref: str) -> Path:
    """Resolve cwd for configured close-check commands.

    Mirrors ``review_findings_updates._derive_resolve_worktree_path``: derive
    from ``target_branch`` via ``_canonical_worktree_for_task``; fall back to
    ``git_workspace_root`` or ``workspace_root`` for main-target / missing
    worktree rows.
    """
    from .runtime import get_runtime_config
    from .shared_write_context import (
        WorktreeNotFoundError,
        _canonical_worktree_for_task,
        _worktree_derivation_enabled,
    )

    config = get_runtime_config()
    fallback = Path(config.git_workspace_root or config.workspace_root)
    if not _worktree_derivation_enabled():
        return fallback
    row = conn.execute(
        "SELECT target_branch FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    if row is None:
        return fallback
    target_branch = _normalize_optional_text(row["target_branch"])
    if target_branch is None:
        return fallback
    try:
        worktree = _canonical_worktree_for_task(target_branch, workspace_root=str(config.workspace_root))
    except WorktreeNotFoundError:
        return fallback
    if worktree is None:
        return fallback
    return Path(worktree)


def _kill_close_check_process_group(proc: subprocess.Popen) -> None:
    """Best-effort SIGKILL of the command's whole process group.

    Commands run under ``start_new_session=True`` so the shell is a session /
    process-group leader; killing the group reaps multi-process commands (e.g.
    ``make check-all`` spawning sub-make/compiler/pytest) that would otherwise be
    orphaned when only the direct child is signalled on timeout.
    """
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        # Group already gone, or a platform without POSIX process groups — fall
        # back to killing the direct child so the launcher never lingers.
        try:
            proc.kill()
        except OSError:
            pass


def _run_close_check_commands(
    commands: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: int,
) -> list[CloseCheckCommandResult]:
    from .agent_errors import _DETAIL_LIMIT, _redact_text

    results: list[CloseCheckCommandResult] = []
    for command in commands:
        normalized = _normalize_optional_text(command)
        if normalized is None:
            continue
        try:
            proc = subprocess.Popen(
                normalized,
                cwd=str(cwd),
                shell=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        except OSError as exc:
            # Command could not be launched at all (e.g. missing cwd or shell).
            results.append(
                CloseCheckCommandResult(
                    command=normalized,
                    exit_code=None,
                    stdout=None,
                    stderr=None,
                    launch_error=str(exc),
                )
            )
            continue
        try:
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            # Kill the whole process group, then drain the pipes so grandchildren
            # are reaped rather than left orphaned past the timeout bound.
            _kill_close_check_process_group(proc)
            try:
                stdout, stderr = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = "", ""
            results.append(
                CloseCheckCommandResult(
                    command=normalized,
                    exit_code=None,
                    stdout=_redact_text(stdout, limit=_DETAIL_LIMIT),
                    stderr=_redact_text(stderr, limit=_DETAIL_LIMIT),
                    launch_error=f"timed out after {timeout_seconds}s",
                    timed_out=True,
                )
            )
            continue
        results.append(
            CloseCheckCommandResult(
                command=normalized,
                exit_code=proc.returncode,
                stdout=_redact_text(stdout, limit=_DETAIL_LIMIT),
                stderr=_redact_text(stderr, limit=_DETAIL_LIMIT),
            )
        )
    return results


def _verification_command_failure_message(result: CloseCheckCommandResult) -> str:
    detail_parts: list[str] = []
    if result.launch_error:
        detail_parts.append(result.launch_error)
    elif result.exit_code is not None:
        detail_parts.append(f"exit {result.exit_code}")
    if result.stdout:
        detail_parts.append(f"stdout: {result.stdout}")
    if result.stderr:
        detail_parts.append(f"stderr: {result.stderr}")
    detail = "; ".join(detail_parts) if detail_parts else "non-zero exit"
    return f"verification_command_failed: {result.command!r} — {detail}"


# Repo-relative path of the always-on brand-check gate. Run from ``close_check_cwd``
# (the worktree / git workspace root), so this resolves against that tree.
_BRAND_CHECK_SCRIPT_RELPATH = "scripts/check_brand.py"


def _run_brand_check_invariant(cwd: Path, *, timeout_seconds: int) -> CloseCheckCommandResult | None:
    """Run the always-on brand-check gate — a repo invariant, not a preference.

    Brand-check must NOT live in the operator-configurable
    ``close_check_required_commands`` list: that list is env-resolved
    (``WORKBAY_HANDOFF_CLOSE_CHECK_REQUIRED_COMMANDS``) and an operator override
    *replaces* it wholesale, so a list entry could be silently dropped. Instead
    this gate runs unconditionally in the close-check path, independent of that
    list, so it cannot be configured out.

    ``scripts/check_brand.py`` ``git grep``s tracked source and exits 1 on a
    forbidden prior-brand token, 0 clean; nonzero is treated fail-closed. The gate
    is skipped only when the script is absent from the close-check cwd (a
    non-monorepo workspace with no brand-check) — a file-presence fact, never an
    operator toggle. Returns ``None`` when skipped, else the command result.
    """
    script = cwd / _BRAND_CHECK_SCRIPT_RELPATH
    if not script.is_file():
        return None
    command = f"{shlex.quote(sys.executable)} {_BRAND_CHECK_SCRIPT_RELPATH}"
    results = _run_close_check_commands((command,), cwd=cwd, timeout_seconds=timeout_seconds)
    return results[0] if results else None


def _brand_check_failure_message(result: CloseCheckCommandResult) -> str:
    prefix = (
        "brand-check invariant failed (forbidden prior-brand token in tracked source): "
        if _brand_check_result_is_token_violation(result)
        else "brand-check gate could not verify the brand invariant: "
    )
    return prefix + _verification_command_failure_message(result)


def _brand_check_result_is_token_violation(result: CloseCheckCommandResult) -> bool:
    return result.exit_code == 1 and "forbidden prior-brand token(s)" in (result.stderr or "")


def _close_check_command_result_to_dict(result: CloseCheckCommandResult) -> dict:
    return {
        "command": result.command,
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "launch_error": result.launch_error,
        "timed_out": result.timed_out,
        "failed": result.failed,
    }


def _evaluate_close_failures(
    *,
    active_task_matches: bool,
    active_status: str | None,
    open_blockers: list,
    pending_actions: list,
    open_findings: list,
    review_integrity: dict,
    provenance_integrity: dict,
    current_task_in_sync: bool,
    require_fresh_tests: bool,
    fresh_test_count: int,
    require_current_commit_summary: bool,
    structured_decisions: list,
    working_tree_integrity: dict,
    verification_command_failures: list[str] | None = None,
) -> list[str]:
    """Evaluate all close-check conditions and return failure messages."""
    failures: list[str] = []
    if not active_task_matches:
        failures.append("Target task is not the active handoff task.")
    if active_status != "done":
        failures.append("Active task status must be 'done'.")
    if open_blockers:
        failures.append("Open blockers must be resolved before close.")
    if pending_actions:
        failures.append("Pending next actions must be done or skipped before close.")
    if open_findings:
        failures.append("Open review findings must be fixed, deferred, or wontfix before close.")
    if not review_integrity["healthy"]:
        failures.append("Review finding integrity checks are not healthy.")
    if not provenance_integrity["healthy"]:
        failures.append("Write provenance integrity checks failed (missing agent/branch metadata).")
    if require_fresh_tests and fresh_test_count == 0:
        failures.append("Fresh verification for the current commit is required before close.")
    if require_current_commit_summary and not structured_decisions:
        failures.append(
            "A structured slice-completion summary for the current commit "
            "(or a reachable ancestor commit) is required before close."
        )
    for failure in verification_command_failures or []:
        failures.append(failure)
    if not working_tree_integrity["ok"]:
        unexpected = working_tree_integrity.get("unexpected_dirty") or []
        failures.append(
            "Working tree has drifted from HEAD on "
            f"{len(unexpected)} unexpected path(s). Commit, stash, or add to "
            ".task-state/dirty-allowlist before closing."
        )
    return failures


def handoff_close_check(
    task_ref: str | None = None,
    allow_no_active_task: bool = False,
    enforce: bool = False,
    require_fresh_tests: bool = False,
    current_commit_sha: str | None = None,
) -> dict:
    """Validate that the active task is ready to close and materialize CURRENT_TASK.json.

    Side effect: atomically rewrites ``CURRENT_TASK.json`` from the live
    derive-on-read workspace summary before the in-sync comparison. This
    is the on-demand materialization point for the workspace summary —
    after this call returns, the on-disk export reflects live handoff
    state. Routine MCP writes still respect
    ``current_task_auto_regen`` (default ``False``); close-check is the
    one path that always writes, so callers (orchestrator review-ready,
    archive flows, lifecycle Makefiles) never need a caller-side
    ``render_handoff`` pre-render.
    """

    normalized_current_commit_sha = _normalize_optional_text(current_commit_sha)
    # Validate the SHA against the active git repo and auto-expand
    # abbreviated forms to the canonical 40-char hash. Catches the
    # fabricated-SHA bug that poisoned several internal/internal
    # audit-trail rows when callers expanded short SHAs from memory
    # rather than from `git rev-parse`. Bypassed entirely by
    # WORKBAY_HANDOFF_SKIP_SHA_VALIDATION (set in both packages' test
    # conftests so synthetic test SHAs work).
    from .shared_write_context import (  # noqa: PLC0415 - late import avoids circular dependency
        InvalidCommitShaError,
        _validate_and_expand_commit_sha,
    )

    try:
        normalized_current_commit_sha = _validate_and_expand_commit_sha(normalized_current_commit_sha)
    except InvalidCommitShaError as exc:
        return _envelope(
            ok=False,
            tool="handoff_close_check",
            data={"error": str(exc)},
        )
    if require_fresh_tests and normalized_current_commit_sha is None:
        return _envelope(
            ok=False,
            tool="handoff_close_check",
            data={"error": "current_commit_sha required when require_fresh_tests=True"},
        )
    require_current_commit_summary = bool(normalized_current_commit_sha)
    with _get_db_connection() as conn:
        from .shared_primitives import LIVE_ACTIVE_STATUSES  # noqa: PLC0415

        if task_ref is None:
            # Bind by row count, not by cwd-tier matching. Falling back to
            # _resolve_workspace_handoff_row here lets close-check silently
            # gate the wrong task whenever multiple in_progress rows exist
            # and one of them happens to match cwd as a prefix or as the
            # canonical workspace_root — same bug class as the pre-fix
            # task-finish identity lookup. Counting rows is sufficient
            # because the only safe no-task_ref case is "exactly one
            # active row in the workspace"; everything else demands an
            # explicit task_ref so the gate cannot misroute.
            placeholders = ",".join(["?"] * len(LIVE_ACTIVE_STATUSES))
            active_rows = conn.execute(
                f"SELECT task_ref FROM handoff_state WHERE status IN ({placeholders})",
                LIVE_ACTIVE_STATUSES,
            ).fetchall()
            if not active_rows:
                recovery = (
                    "No active handoff task found. To run the close gate, pass explicit task_ref "
                    "and full HEAD current_commit_sha; run integrity_check directly for the "
                    "bound task."
                )
                if allow_no_active_task:
                    return _envelope(
                        ok=False,
                        tool="handoff_close_check",
                        data={
                            "ready_to_close": False,
                            "skipped": True,
                            "gates_evaluated": False,
                            "warning": recovery,
                            "reason": "No active handoff task found.",
                        },
                    )
                return _envelope(
                    ok=False,
                    tool="handoff_close_check",
                    data={
                        "ready_to_close": False,
                        "skipped": False,
                        "gates_evaluated": False,
                        "error": recovery,
                    },
                )
            if len(active_rows) > 1:
                candidates = sorted(str(row["task_ref"]) for row in active_rows)
                return _envelope(
                    ok=False,
                    tool="handoff_close_check",
                    data={
                        "error": (
                            "Ambiguous active task: multiple in_progress rows exist. "
                            "Pass task_ref explicitly to bind close-check to the row "
                            "you intend to gate."
                        ),
                        "candidates": candidates,
                    },
                )
            resolved_task_ref = str(active_rows[0]["task_ref"])
        else:
            resolved_task_ref = task_ref
        snapshot = _collect_task_snapshot(conn, resolved_task_ref)
        active = snapshot["active"]
        active_task_matches = active is not None
        active_status = str(active["status"]) if active is not None else None
        open_blockers = [row for row in snapshot["blockers"] if row.get("status") == BlockerStatus.OPEN]
        pending_actions = [row for row in snapshot["next_actions"] if row.get("status") == ActionStatus.PENDING]
        open_findings = [row for row in snapshot["review_findings"] if row.get("status") == FindingStatus.OPEN]
        (
            fresh_test_count,
            current_commit_slice_decisions,
            structured_current_commit_decisions,
            handoff_satisfied_by_commit,
            handoff_match_kind,
            handoff_ancestor_distance,
        ) = _collect_commit_verification_data(
            conn,
            resolved_task_ref,
            normalized_current_commit_sha,
            require_fresh_tests,
            require_current_commit_summary,
        )
        from .review_findings import _collect_review_findings_integrity  # noqa: PLC0415

        review_integrity = _collect_review_findings_integrity(conn, resolved_task_ref, apply=False)
        provenance_integrity = _collect_task_provenance_integrity(conn, resolved_task_ref)
        from .current_task_rendering import (  # noqa: PLC0415
            _normalize_current_task_json_for_compare,
            _render_current_task_json,
            _write_workspace_summary_current_task_json,
        )

        # On-demand export: the close check is the materialization point for
        # CURRENT_TASK.json. Routine MCP writes leave the on-disk file stale
        # by design (current_task_auto_regen=False, internal derive-on-read),
        # which used to force every caller — `make review-ready`, archive
        # flows, monorepo lifecycle scripts — to call `render_handoff` first
        # or accept a spurious `is_in_sync=False`. Writing here makes the
        # check self-contained: after close_check returns, the on-disk file
        # always matches the live derivation, so callers never need a
        # caller-side pre-render.
        _write_workspace_summary_current_task_json(unconditional=True)
        expected_current_task = _render_current_task_json()
        current_task_exists = _current_task_path().exists()
        # Tautological in the happy path — we just wrote `expected_current_task`
        # to disk above. The read-back + compare remains as a defense-in-depth
        # sentinel for torn writes, racing concurrent writers, or on-disk
        # corruption between os.replace and the read. Do not delete it as
        # dead code: the comparison turns silent disk-layer failures into a
        # visible `is_in_sync=False` signal.
        current_task_in_sync = bool(
            current_task_exists
            and active_task_matches
            and _normalize_current_task_json_for_compare(_current_task_path().read_text())
            == _normalize_current_task_json_for_compare(expected_current_task)
        )
        close_check_cwd = _resolve_close_check_cwd(conn, resolved_task_ref)
    latest_structured_current_commit_decision = (
        structured_current_commit_decisions[0] if structured_current_commit_decisions else None
    )
    from .working_tree import _check_working_tree_integrity  # noqa: PLC0415 - late import

    working_tree_integrity = _check_working_tree_integrity()
    from .runtime import get_runtime_config  # noqa: PLC0415

    runtime_config = get_runtime_config()
    command_results: list[CloseCheckCommandResult] = []
    verification_command_failures: list[str] = []
    if enforce and runtime_config.close_check_required_commands:
        command_results = _run_close_check_commands(
            runtime_config.close_check_required_commands,
            cwd=close_check_cwd,
            timeout_seconds=runtime_config.close_check_command_timeout_seconds,
        )
        verification_command_failures = [
            _verification_command_failure_message(result) for result in command_results if result.failed
        ]
    # Always-on brand-check invariant. Runs independent of (and cannot be removed
    # via) the operator-configurable close_check_required_commands list above.
    brand_check_result: CloseCheckCommandResult | None = None
    brand_check_failures: list[str] = []
    if enforce:
        brand_check_result = _run_brand_check_invariant(
            close_check_cwd,
            timeout_seconds=runtime_config.close_check_command_timeout_seconds,
        )
        if brand_check_result is not None and brand_check_result.failed:
            brand_check_failures = [_brand_check_failure_message(brand_check_result)]
    evidenced_unticked: int | None = None
    if enforce:
        from .plan_checklist_rendering import count_evidenced_unticked_boxes  # noqa: PLC0415

        evidenced_unticked = count_evidenced_unticked_boxes(resolved_task_ref)
    failures = _evaluate_close_failures(
        active_task_matches=active_task_matches,
        active_status=active_status,
        open_blockers=open_blockers,
        pending_actions=pending_actions,
        open_findings=open_findings,
        review_integrity=review_integrity,
        provenance_integrity=provenance_integrity,
        current_task_in_sync=current_task_in_sync,
        require_fresh_tests=require_fresh_tests,
        fresh_test_count=fresh_test_count,
        require_current_commit_summary=require_current_commit_summary,
        structured_decisions=structured_current_commit_decisions,
        working_tree_integrity=working_tree_integrity,
        verification_command_failures=verification_command_failures + brand_check_failures,
    )
    if evidenced_unticked is not None and evidenced_unticked > 0:
        failures.append(
            f"{evidenced_unticked} evidenced-but-unticked checklist box(es); "
            "run `make sync-task-plan-checklist APPLY=1` or `make finalize-plan`"
        )
    ready_to_close = len(failures) == 0
    data: dict = {
        "ready_to_close": ready_to_close,
        "checks": {
            "active_task": {
                "matches_target": active_task_matches,
                "status": active_status,
                "is_done": active_status == "done",
            },
            "open_blockers": {
                "count": len(open_blockers),
                "is_violation": len(open_blockers) > 0,
                "items": open_blockers,
            },
            "pending_actions": {
                "count": len(pending_actions),
                "is_violation": len(pending_actions) > 0,
                "items": pending_actions,
            },
            "open_review_findings": {
                "count": len(open_findings),
                "is_violation": len(open_findings) > 0,
                "items": open_findings,
            },
            "review_integrity": review_integrity,
            "write_provenance": provenance_integrity,
            "current_task_sync": {
                "path": str(_current_task_path()),
                "exists": current_task_exists,
                "is_in_sync": current_task_in_sync,
                "is_violation": False,
                "mode": "on_demand_export",
            },
            "fresh_tests": {
                "required": require_fresh_tests,
                "current_commit_sha": normalized_current_commit_sha,
                "count": fresh_test_count,
                "is_violation": bool(require_fresh_tests and fresh_test_count == 0),
            },
            "working_tree_integrity": {
                "ok": working_tree_integrity["ok"],
                "unexpected_dirty": working_tree_integrity.get("unexpected_dirty", []),
                "dirty_paths": working_tree_integrity.get("dirty_paths", []),
                "allowlist_source": working_tree_integrity.get("allowlist_source"),
                "is_violation": not working_tree_integrity["ok"],
            },
            **(
                {
                    "verification_commands": {
                        "required": True,
                        "cwd": str(close_check_cwd),
                        "timeout_seconds": runtime_config.close_check_command_timeout_seconds,
                        "results": [_close_check_command_result_to_dict(r) for r in command_results],
                        "is_violation": bool(verification_command_failures),
                    }
                }
                if command_results
                else {}
            ),
            **(
                {
                    "brand_check": {
                        "required": True,
                        "cwd": str(close_check_cwd),
                        "script": _BRAND_CHECK_SCRIPT_RELPATH,
                        "result": _close_check_command_result_to_dict(brand_check_result),
                        "is_violation": _brand_check_result_is_token_violation(brand_check_result),
                    }
                }
                if brand_check_result is not None
                else {}
            ),
            "current_commit_handoff": {
                "required": require_current_commit_summary,
                "current_commit_sha": normalized_current_commit_sha,
                "slice_decision_count": len(current_commit_slice_decisions),
                "structured_slice_decision_count": len(structured_current_commit_decisions),
                "latest_structured_decision_id": int(latest_structured_current_commit_decision["id"])
                if latest_structured_current_commit_decision is not None
                else None,
                "latest_structured_decision": str(latest_structured_current_commit_decision["decision"])
                if latest_structured_current_commit_decision is not None
                else None,
                # T7: which decision commit satisfied the gate (exact HEAD or reachable ancestor).
                "satisfied_by_commit_sha": handoff_satisfied_by_commit,
                "match_kind": handoff_match_kind,
                # S5-A-01: commits between the satisfying decision commit and HEAD
                # (0 = exact HEAD; None = not satisfied or git could not answer).
                "ancestor_distance": handoff_ancestor_distance,
                "is_violation": bool(require_current_commit_summary and not structured_current_commit_decisions),
            },
        },
        "failures": failures,
    }
    if enforce and not ready_to_close:
        data["error"] = "Handoff close checks failed."
    if require_fresh_tests and fresh_test_count == 0:
        data["stale_test"] = {
            "current_commit_sha": normalized_current_commit_sha,
            "reason": "No verification rows recorded for the current commit.",
        }
    return _envelope(
        ok=not (enforce and not ready_to_close),
        tool="handoff_close_check",
        data=data,
        task_ref=resolved_task_ref,
    )


def audit_decision_ids(
    task_ref: str | None = None,
    limit: int = 50,
    include_categories: list[str] | None = None,
) -> dict:
    """Audit recent decision ids for grammar conformance and report violations.

    Classifies every decision row (newest first) for the active or requested
    task into one of four categories and returns a summary with per-row
    detail for any non-canonical rows:

    - ``"canonical"``       - full ``<author_tag>_<decision_kind>_<work_ref>_<slug>`` form.
    - ``"legacy_slice"``    - grandfathered ``slice_complete_*`` form (read-path only).
    - ``"malformed_slice"`` - contains ``slice_complete`` but violates the grammar.
    - ``"freeform"``        - no slice_complete at all and not canonical.

    Args:
        task_ref: Task to audit. Defaults to the active task.
        limit: Maximum number of decisions to inspect (newest first). Max 500.
        include_categories: If provided, only rows matching these categories
            are included in the per-row ``violations`` list. Defaults to
            ``["malformed_slice", "freeform"]`` so the report focuses on
            actionable items.
    """
    limit = max(1, min(limit, 500))
    default_categories = {"malformed_slice", "freeform"}
    if include_categories is not None:
        report_categories = set(include_categories)
    else:
        report_categories = default_categories

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        rows = conn.execute(
            "SELECT id, decision, created_at, agent FROM decisions WHERE task_ref = ? ORDER BY id DESC LIMIT ?",
            (resolved_task_ref, limit),
        ).fetchall()

    counts: dict[str, int] = {
        "canonical": 0,
        "legacy_slice": 0,
        "malformed_slice": 0,
        "freeform": 0,
    }
    violations: list[dict] = []

    for row in rows:
        decision_str = str(row["decision"])
        category = classify_decision_id(decision_str)
        counts[category] += 1
        if category in report_categories:
            violations.append(
                {
                    "id": int(row["id"]),
                    "decision": decision_str,
                    "category": category,
                    "created_at": str(row["created_at"]),
                    "agent": row["agent"],
                }
            )

    total_inspected = len(rows)
    healthy = counts["malformed_slice"] == 0
    summary_lines: list[str] = []
    if counts["malformed_slice"]:
        summary_lines.append(
            f"{counts['malformed_slice']} malformed slice-complete id(s) found; "
            "use <author_tag>_slice_complete_<work_ref>_<slug>."
        )
    if counts["legacy_slice"]:
        summary_lines.append(
            f"{counts['legacy_slice']} legacy slice_complete_* id(s) found; grandfathered for read paths only."
        )
    if counts["freeform"] and "freeform" in report_categories:
        summary_lines.append(
            f"{counts['freeform']} freeform id(s) found; "
            "consider adopting <author_tag>_<decision_kind>_<work_ref>_<slug>."
        )
    if healthy and not summary_lines:
        summary_lines.append("All inspected decision ids conform to the canonical grammar.")

    return _envelope(
        ok=True,
        tool="audit_decision_ids",
        data={
            "healthy": healthy,
            "total_inspected": total_inspected,
            "counts": counts,
            "violations": violations,
            "summary": " ".join(summary_lines),
        },
        task_ref=resolved_task_ref,
    )
