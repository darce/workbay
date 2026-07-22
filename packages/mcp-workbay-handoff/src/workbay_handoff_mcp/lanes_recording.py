"""Hub-owned worktree_lanes read/write operations."""

from __future__ import annotations

import sqlite3
from typing import Any

from .shared_primitives import _envelope, _normalize_optional_text, _resolve_task_ref, _row_to_dict
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings

LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "merged", "closed"})
CLOSEABLE_LANE_STATUSES = frozenset({"closed", "merged"})


def _write_current_task_md_for_task(conn: sqlite3.Connection, task_ref: str) -> None:
    del conn
    from . import generate_current_task_md  # noqa: PLC0415

    generate_current_task_md(task_ref=task_ref, write_file=True)


def _get_lane_row(conn: sqlite3.Connection, task_ref: str, lane_id: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
        (task_ref, lane_id),
    ).fetchone()
    return row


def open_lane(
    *,
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
    lane_kind: str | None = None,
    status: str = "planned",
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict[str, Any]:
    """Insert or update a worktree lane row (upsert).

    ``lane_kind`` is ``'implement'`` (default) or ``'review'`` (implementation note R3).
    Omitting it (``None``) defaults a NEW lane to ``'implement'`` and PRESERVES the
    stored kind on a re-upsert (so a status/notes refresh never resets a review
    lane) — the same omit-preserve contract test_cmd uses, kept NOT NULL via the
    column default.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    normalized_path = _normalize_optional_text(worktree_path)
    normalized_branch = _normalize_optional_text(branch)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "lane_id is required."}, entity="lane")
    if normalized_path is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "worktree_path is required."}, entity="lane")
    if normalized_branch is None:
        return _envelope(ok=False, tool="open_lane", data={"error": "branch is required."}, entity="lane")
    if status not in LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="open_lane",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(LANE_STATUSES))}"},
            entity="lane",
        )
    if lane_kind is not None and lane_kind not in ("implement", "review"):
        return _envelope(
            ok=False,
            tool="open_lane",
            data={"error": "Invalid lane_kind. Valid: implement, review"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        resolved_owner = owner_agent
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
            resolved_owner = owner_agent or ctx.agent
        conn.execute(
            """
            INSERT INTO worktree_lanes (
                task_ref, lane_id, title, objective, worktree_path, branch,
                owner_agent, model, backend, reasoning_effort, test_cmd, lane_kind, status, notes,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, 'implement'), ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(task_ref, lane_id) DO UPDATE SET
                title = excluded.title,
                objective = excluded.objective,
                worktree_path = excluded.worktree_path,
                branch = excluded.branch,
                owner_agent = excluded.owner_agent,
                model = COALESCE(excluded.model, worktree_lanes.model),
                backend = COALESCE(excluded.backend, worktree_lanes.backend),
                reasoning_effort = COALESCE(excluded.reasoning_effort, worktree_lanes.reasoning_effort),
                test_cmd = COALESCE(excluded.test_cmd, worktree_lanes.test_cmd),
                lane_kind = COALESCE(?, worktree_lanes.lane_kind),
                status = excluded.status,
                notes = excluded.notes,
                updated_at = datetime('now')
            """,
            (
                resolved_task_ref,
                normalized_lane_id,
                title,
                objective,
                normalized_path,
                normalized_branch,
                resolved_owner,
                model,
                backend,
                reasoning_effort,
                test_cmd,
                lane_kind,
                status,
                notes,
                lane_kind,
            ),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _envelope(
            ok=True,
            tool="open_lane",
            data={"lane": _row_to_dict(row), "task_ref": resolved_task_ref},
            entity="lane",
            warnings=warnings,
        )


def update_lane(
    *,
    lane_id: str,
    title: str | None = None,
    objective: str | None = None,
    worktree_path: str | None = None,
    branch: str | None = None,
    owner_agent: str | None = None,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    test_cmd: str | None = None,
    status: str | None = None,
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict[str, Any]:
    """Patch an existing worktree lane row."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="update_lane", data={"error": "lane_id is required."}, entity="lane")
    if status is not None and status not in LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="update_lane",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(LANE_STATUSES))}"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        existing = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if existing is None:
            return _envelope(
                ok=False,
                tool="update_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        patch: dict[str, object | None] = {
            "title": title,
            "objective": objective,
            "worktree_path": _normalize_optional_text(worktree_path) if worktree_path is not None else None,
            "branch": _normalize_optional_text(branch) if branch is not None else None,
            "owner_agent": owner_agent if owner_agent is not None else (ctx.agent if actor is not None else None),
            "model": model,
            "backend": backend,
            "reasoning_effort": reasoning_effort,
            "test_cmd": test_cmd,
            "status": status,
            "notes": notes,
        }
        fields: list[str] = []
        params: list[object] = []
        for column, value in patch.items():
            if value is not None:
                fields.append(f"{column} = ?")
                params.append(value)
        if not fields:
            return _envelope(
                ok=True,
                tool="update_lane",
                data={"lane": _row_to_dict(existing), "task_ref": resolved_task_ref},
                entity="lane",
                warnings=warnings,
            )
        fields.append("updated_at = datetime('now')")
        params.extend([resolved_task_ref, normalized_lane_id])
        conn.execute(
            f"UPDATE worktree_lanes SET {', '.join(fields)} WHERE task_ref = ? AND lane_id = ?",
            tuple(params),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _envelope(
            ok=True,
            tool="update_lane",
            data={"lane": _row_to_dict(row), "task_ref": resolved_task_ref},
            entity="lane",
            warnings=warnings,
        )


def close_lane(
    *,
    lane_id: str,
    status: str = "closed",
    notes: str | None = None,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict[str, Any]:
    """Transition a lane to a closeable terminal status."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="close_lane", data={"error": "lane_id is required."}, entity="lane")
    if status not in CLOSEABLE_LANE_STATUSES:
        return _envelope(
            ok=False,
            tool="close_lane",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(CLOSEABLE_LANE_STATUSES))}"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        warnings: list[str] = []
        if actor is not None:
            ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
            warnings = collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref)
        existing = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if existing is None:
            return _envelope(
                ok=False,
                tool="close_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        conn.execute(
            """
            UPDATE worktree_lanes
            SET status = ?,
                notes = COALESCE(?, notes),
                updated_at = datetime('now')
            WHERE task_ref = ? AND lane_id = ?
            """,
            (status, notes, resolved_task_ref, normalized_lane_id),
        )
        # Steady-state reclaimer (internal S1): brief/inbox rows have
        # no other deletion path, so a terminal lane status reclaims its inbox to
        # bound accumulation. S1-A-003: prune only already-terminal messages
        # (acknowledged/closed); an 'open' message is unresolved history that
        # archive_task must still snapshot before deleting — close_lane must not
        # destroy it ahead of that snapshot.
        pruned = conn.execute(
            "DELETE FROM lane_messages WHERE task_ref = ? AND lane_id = ? AND status != 'open'",
            (resolved_task_ref, normalized_lane_id),
        )
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        _write_current_task_md_for_task(conn, resolved_task_ref)
        return _envelope(
            ok=True,
            tool="close_lane",
            data={
                "lane": _row_to_dict(row),
                "task_ref": resolved_task_ref,
                "pruned_lane_messages": pruned.rowcount,
            },
            entity="lane",
            warnings=warnings,
        )


def get_lane(*, lane_id: str, task_ref: str | None = None) -> dict[str, Any]:
    """Return a single lane row."""
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(ok=False, tool="get_lane", data={"error": "lane_id is required."}, entity="lane")
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        row = _get_lane_row(conn, resolved_task_ref, normalized_lane_id)
        if row is None:
            return _envelope(
                ok=False,
                tool="get_lane",
                data={"error": f"Lane '{normalized_lane_id}' not found for task '{resolved_task_ref}'."},
                entity="lane",
            )
        return _envelope(
            ok=True,
            tool="get_lane",
            data={"lane": _row_to_dict(row), "task_ref": resolved_task_ref},
            entity="lane",
        )


def latest_lane_landing(*, lane_id: str, task_ref: str) -> dict[str, Any]:
    """Return the newest recorded landing commit for a lane, if one exists.

    Landing rows are ``lane_landed_<task_ref>_<lane_id>`` decisions written by the
    orchestrator before every MERGED transition. Each re-land inserts a new row
    (SHA-scoped session), so ``session`` is deliberately not part of the match;
    ``created_at`` is ``datetime('now')`` at 1-second resolution, so ``id DESC``
    is the deterministic tie-break. An absent landing is ``ok=True`` with
    ``landing=None`` -- it is a normal predicate state, not a read error.

    Both ``lane_id`` and ``task_ref`` are required and keyword-only: the only
    intended caller always knows both values, and the two opaque strings must
    not be swapped by positional call order.
    """
    normalized_lane_id = _normalize_optional_text(lane_id)
    if normalized_lane_id is None:
        return _envelope(
            ok=False,
            tool="latest_lane_landing",
            data={"error": "lane_id is required."},
            entity="lane",
        )
    normalized_task_ref = _normalize_optional_text(task_ref)
    if normalized_task_ref is None:
        return _envelope(
            ok=False,
            tool="latest_lane_landing",
            data={"error": "task_ref is required."},
            entity="lane",
        )
    with _get_db_connection() as conn:
        decision_id = f"lane_landed_{normalized_task_ref}_{normalized_lane_id}"
        row = conn.execute(
            """
            SELECT id, task_ref, lane_id, session, decision, branch, commit_sha, agent,
                   decision_origin, created_at
            FROM decisions
            WHERE task_ref = ?
              AND decision = ?
              AND commit_sha IS NOT NULL
              AND TRIM(commit_sha) <> ''
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized_task_ref, decision_id),
        ).fetchone()
        # Cheap defence: decision ids embed task_ref + lane_id and are ambiguous
        # under underscore-containing components; refuse a row whose column
        # task_ref disagrees with the query (writer inconsistency / SQL drift).
        if row is not None and str(row["task_ref"]) != normalized_task_ref:
            return _envelope(
                ok=False,
                tool="latest_lane_landing",
                data={
                    "error": "landing row task_ref does not match query task_ref.",
                    "task_ref": normalized_task_ref,
                    "lane_id": normalized_lane_id,
                    "decision": decision_id,
                    "row_task_ref": str(row["task_ref"]),
                },
                task_ref=normalized_task_ref,
                entity="lane",
            )
        return _envelope(
            ok=True,
            tool="latest_lane_landing",
            data={
                "task_ref": normalized_task_ref,
                "lane_id": normalized_lane_id,
                "decision": decision_id,
                "landing": _row_to_dict(row),
            },
            task_ref=normalized_task_ref,
            entity="lane",
        )


def list_lanes(
    *,
    task_ref: str | None = None,
    status: str = "all",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """List lane rows for a task with optional status filter."""
    limit = max(1, limit)
    offset = max(0, offset)
    valid_statuses = {"all", *LANE_STATUSES}
    if status not in valid_statuses:
        return _envelope(
            ok=False,
            tool="list_lanes",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}"},
            entity="lane",
        )
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        params: list[object] = [resolved_task_ref]
        where_sql = "task_ref = ?"
        if status != "all":
            where_sql += " AND status = ?"
            params.append(status)
        total = int(
            conn.execute(
                f"SELECT COUNT(*) AS count FROM worktree_lanes WHERE {where_sql}",
                tuple(params),
            ).fetchone()["count"]
        )
        rows = [
            _row_to_dict(row)
            for row in conn.execute(
                f"SELECT * FROM worktree_lanes WHERE {where_sql} ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        ]
        return _envelope(
            ok=True,
            tool="list_lanes",
            data={
                "task_ref": resolved_task_ref,
                "status": status,
                "total_matching": total,
                "returned": len(rows),
                "has_more": offset + len(rows) < total,
                "lanes": rows,
            },
            entity="lane",
        )
