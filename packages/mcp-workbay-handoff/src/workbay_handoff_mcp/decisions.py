"""Decisions domain module.

Contains record_decision, update_next_actions, list_next_actions,
record_test_result, report_blocker, handoff_close_check,
and integrity helpers.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from pathlib import PurePosixPath

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
) -> dict:
    validation_error = _validate_decision_payload(decision, rationale)
    if validation_error is not None:
        return _envelope(ok=False, tool="record_decision", data={"error": validation_error})
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
) -> dict:
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
            task_ref, lane_id, session, decision, rationale, agent,
            model, model_label, reasoning_level,
            input_tokens, output_tokens, total_tokens,
            branch, commit_sha, changed_files_json, slice_number, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(task_ref, decision, session) DO NOTHING
        """,
        (
            resolved_task_ref,
            ctx.lane_id,
            session,
            decision,
            rationale,
            ctx.agent,
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


def _collect_commit_verification_data(
    conn: sqlite3.Connection,
    task_ref: str,
    commit_sha: str | None,
    require_fresh_tests: bool,
    require_summary: bool,
) -> tuple[int, list, list]:
    """Collect fresh-test count and commit-level slice decisions."""
    fresh_test_count = 0
    commit_slice_decisions: list = []
    if require_fresh_tests and commit_sha is not None:
        fresh_test_count = int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM verified_tests WHERE task_ref = ? AND commit_sha = ?",
                (task_ref, commit_sha),
            ).fetchone()["count"]
        )
    if require_summary and commit_sha is not None:
        commit_slice_decisions = conn.execute(
            """
            SELECT id, decision, rationale, created_at
            FROM decisions
            WHERE task_ref = ?
              AND commit_sha = ?
              AND (decision LIKE 'slice_complete_%' OR decision LIKE '%_slice_complete_%')
            ORDER BY id DESC
            """,
            (task_ref, commit_sha),
        ).fetchall()
        commit_slice_decisions = [
            row for row in commit_slice_decisions if is_slice_complete_decision(str(row["decision"]))
        ]
    structured = [row for row in commit_slice_decisions if _has_structured_slice_summary(str(row["rationale"] or ""))]
    return fresh_test_count, commit_slice_decisions, structured


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
        failures.append("A structured slice-completion summary for the current commit is required before close.")
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
                if allow_no_active_task:
                    return _envelope(
                        ok=True,
                        tool="handoff_close_check",
                        data={"ready_to_close": True, "skipped": True, "reason": "No active handoff task found."},
                    )
                return _envelope(ok=False, tool="handoff_close_check", data={"error": "No active handoff task found."})
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
        fresh_test_count, current_commit_slice_decisions, structured_current_commit_decisions = (
            _collect_commit_verification_data(
                conn,
                resolved_task_ref,
                normalized_current_commit_sha,
                require_fresh_tests,
                require_current_commit_summary,
            )
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
    latest_structured_current_commit_decision = (
        structured_current_commit_decisions[0] if structured_current_commit_decisions else None
    )
    from .working_tree import _check_working_tree_integrity  # noqa: PLC0415 - late import

    working_tree_integrity = _check_working_tree_integrity()
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
