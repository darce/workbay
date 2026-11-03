"""Touched-files domain helpers."""

from __future__ import annotations

import os
from enum import StrEnum

from .shared_primitives import _envelope, _resolve_task_ref, _row_to_dict
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, _validate_and_expand_commit_sha

DEFAULT_TOUCHED_FILES_LIMIT = 20


class ChangeKind(StrEnum):
    EDIT = "edit"
    ADD = "add"
    DELETE = "delete"


# Single source of truth for the announced change_kind set. The MCP input schema
# (api.TouchedFilesRecordOp), the CLI --change-kind choices, and the runtime guard
# below all express this set; a implementation note drift-guard test binds the schema enum to it.
CHANGE_KIND_VALUES: tuple[str, ...] = tuple(kind.value for kind in ChangeKind)


def record_file_touch(
    file_path: str,
    change_kind: str,
    session: str | None = None,
    commit_sha: str | None = None,
    actor: WriteActor | None = None,
    task_ref: str | None = None,
) -> dict:
    normalized_file_path = file_path.strip().replace("\\", "/")
    if not normalized_file_path:
        return _envelope(ok=False, tool="record_file_touch", data={"error": "file_path is required."})
    if os.path.isabs(normalized_file_path) or ".." in normalized_file_path.split("/"):
        return _envelope(
            ok=False,
            tool="record_file_touch",
            data={"error": "file_path must be monorepo-relative (no absolute paths or '..' segments)."},
        )
    try:
        normalized_change_kind = ChangeKind(change_kind.strip()).value
    except ValueError:
        allowed = ", ".join(kind.value for kind in ChangeKind)
        return _envelope(
            ok=False,
            tool="record_file_touch",
            data={"error": f"Invalid change_kind. Valid: {allowed}"},
        )

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)
        raw_commit_sha = commit_sha if commit_sha is not None else ctx.commit_sha
        resolved_commit_sha = _validate_and_expand_commit_sha(raw_commit_sha)
        cur = conn.execute(
            """
            INSERT INTO touched_files (
                task_ref, file_path, change_kind, session, commit_sha, lane_id, agent, branch, touched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                normalized_file_path,
                normalized_change_kind,
                session,
                resolved_commit_sha,
                ctx.lane_id,
                ctx.agent,
                ctx.branch,
            ),
        )
        touch_row = _row_to_dict(conn.execute("SELECT * FROM touched_files WHERE id = ?", (cur.lastrowid,)).fetchone())
        return _envelope(
            ok=True,
            tool="record_file_touch",
            data={"touch": touch_row},
            task_ref=resolved_task_ref,
            mutation={
                "entity": "touched_file",
                "operation": "insert",
                "affected_ids": [cur.lastrowid],
                "task_revision": None,
            },
            entity="touched_file",
        )


def get_touched_files(
    task_ref: str | None = None,
    limit: int = DEFAULT_TOUCHED_FILES_LIMIT,
    offset: int = 0,
) -> dict:
    clamped_limit = max(1, min(int(limit), 200))
    clamped_offset = max(0, int(offset))

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        total = int(
            conn.execute("SELECT COUNT(*) FROM touched_files WHERE task_ref = ?", (resolved_task_ref,)).fetchone()[0]
        )
        rows = []
        for row in conn.execute(
            """
            SELECT *
            FROM touched_files
            WHERE task_ref = ?
            ORDER BY touched_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (resolved_task_ref, clamped_limit, clamped_offset),
        ).fetchall():
            payload = _row_to_dict(row)
            if payload is not None:
                rows.append(payload)

        return _envelope(
            ok=True,
            tool="get_touched_files",
            data={
                "task_ref": resolved_task_ref,
                "total_matching": total,
                "returned": len(rows),
                "has_more": clamped_offset + len(rows) < total,
                "touches": rows,
            },
            task_ref=resolved_task_ref,
            entity="touched_file",
        )
