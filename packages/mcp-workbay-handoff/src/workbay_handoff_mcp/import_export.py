"""Import/export domain module.

Contains export_handoff_state, import_handoff_state, archive_task_state,
get_archived_task, update_task_status, and switch_task.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import cast

from workbay_protocol import resolve_env_alias

from . import lane_completion_production as production
from . import task_supersession as supersession
from .current_task_rendering import (
    TaskSnapshot,
    _build_current_task_state_from_snapshot,
    _collect_task_snapshot,
    _render_current_task_md,
    _write_workspace_current_task_json,
)
from .enums import LIVE_LANE_STATUSES, TERMINAL_LANE_STATUSES, HandoffStatus
from .git_merge import branch_is_merged
from .lane_close_evidence import OPENINGS as _PRIVATE_CLOSE_OPENINGS_TABLE
from .lane_close_evidence import TABLE as _PRIVATE_CLOSE_EVIDENCE_TABLE
from .lane_landing_proof import (
    LANDING_PROBE_STALE,
    StaleLandingProbe,
    fenced_unlanded,
    landing_probe_stale_refusal,
    probe_task_landing,
    unlanded_lane_tips,
    unlanded_lanes_refusal,
)
from .lifecycle_resource_producer import ProducerBinding, _git, prepare_binding, stage_change
from .review_findings_support import _validate_impact_origin
from .runtime import get_runtime_config
from .shared_db_utils import _count_task_rows, _resolve_output_path
from .shared_primitives import (
    HANDOFF_ACTIVE_STATUSES,
    LIVE_ACTIVE_STATUSES,
    _envelope,
    _normalize_optional_text,
    _resolve_import_lane_id,
    _resolve_import_row_actor,
    _resolve_task_ref,
    _resolve_workspace_handoff_row,
    _row_to_dict,
    _side_effect_error_class,
    _surface_side_effect_failure,
    _utcnow_iso,
    _workspace_root,
)
from .shared_schema import (
    REVIEW_FINDINGS_MIRRORED_COLUMNS,
    _get_db_connection,
    _open_bounded_roster_read_connection,
    _retained_registration_store,
    archive_terminal_review_findings,
    connect_handoff_db,
    review_history_unavailable,
)
from .shared_write_context import (
    ResolvedWriteContext,
    WorktreeNotFoundError,
    WriteActor,
    _canonical_worktree_for_task,
    _detect_git_write_context,
    _resolve_core_override,
    _resolve_write_actor,
    _worktree_derivation_enabled,
    build_write_actor,
    collect_target_context_warnings,
    normalize_actor_harness,
)

_log = logging.getLogger("workbay_handoff_mcp")


def _task_source_identity(row) -> tuple | None:
    """Stored task identity only; safe to compare inside a writer transaction."""
    if row is None:
        return None
    values = dict(row)
    return tuple(values.get(key) for key in ("task_ref", "target_branch", "target_worktree_path"))


def _prepare_task_resource_binding(ctx: ResolvedWriteContext, row) -> ProducerBinding:
    """Bind a named task's actual checkout before taking any write lock.

    An actor's cwd/branch is provenance, not permission to adopt its checkout
    as another task's resource. Missing or foreign targets stay unknown.
    Canon v0.26.2 DATA-14 (No dual writes, DDIA ch. 11, engineering:232)
    and DOM-06 (Outbox or it didn't happen, Learning DDD ch. 9, engineering:684).
    """
    identity = _task_source_identity(row)
    if identity is None or not all(identity):
        return ProducerBinding(reason="task_target_unknown")
    _, branch, worktree = identity
    path = Path(worktree)
    if not path.is_absolute():
        return ProducerBinding(reason="task_target_mismatch")
    root = Path(get_runtime_config().git_workspace_root)
    top = _git(path, "rev-parse", "--show-toplevel")
    common = _git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    owner = _git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    actual_branch = _git(path, "branch", "--show-current")
    if (
        not top
        or Path(top).resolve() != path.resolve()
        or not common
        or not owner
        or Path(common).resolve() != Path(owner).resolve()
        or actual_branch != branch
    ):
        return ProducerBinding(reason="task_target_unavailable" if not top else "task_target_mismatch")
    return prepare_binding(ctx, str(path))


def _recheck_task_resource_binding(conn, task_ref, source_row, binding) -> ProducerBinding:
    """Do not attach a pre-lock source packet to a concurrently rebound row."""
    current = conn.execute(
        "SELECT task_ref,target_branch,target_worktree_path FROM handoff_state WHERE task_ref=?", (task_ref,)
    ).fetchone()
    if _task_source_identity(current) != _task_source_identity(source_row):
        return ProducerBinding(reason="task_source_changed")
    return binding


def _resource_warnings(binding_result: dict) -> list[str]:
    return (
        ["lifecycle_resource_unknown: " + binding_result["reason"]]
        if binding_result.get("knowledge") == "unknown"
        else []
    )


_internal_TASK_REF_RE = re.compile(r"\bAHMCP-\d+\b")

# internal: severity-aware archive gate. Low/medium open findings
# auto-migrate to one dated internal-<YYYYMMDD> row (reuse, not
# one-per-archive). Open high findings block archive with a remedy.
FINDING_BACKLOG_PREFIX = "internal-"
_ARCHIVE_MIGRATE_NOTE = "plan:0097 archive auto-migrate from {source} to {backlog}"
_HIGH_ARCHIVE_REMEDY = (
    "Open high-severity findings block archive. Disposition or re-anchor each "
    "high finding in-flow (review_findings disposition/reanchor), then re-run "
    "archive. Low/medium findings auto-migrate to "
    f"{FINDING_BACKLOG_PREFIX}<YYYYMMDD> when archive proceeds."
)


def _finding_backlog_task_ref(*, day: str | None = None) -> str:
    """Return the single dated internal-<YYYYMMDD> task ref."""
    if day is None:
        day = datetime.now(UTC).strftime("%Y%m%d")
    return f"{FINDING_BACKLOG_PREFIX}{day}"


def _append_resolution_note(existing: str | None, note: str) -> str:
    current = (existing or "").strip()
    if not current:
        return note
    if note in current:
        return current
    return f"{current} | {note}"


def _ensure_finding_backlog_row(
    conn: sqlite3.Connection,
    backlog_task_ref: str,
    *,
    source_task_ref: str,
    agent: str | None,
) -> None:
    """Create the dated backlog handoff_state row if missing (reuse if present).

    Drain policy for this row (bounded remedy [RES-07]): weekly debt digest on
    DASHBOARD + periodic findings-triage disposition pass.
    """
    live = conn.execute(
        "SELECT 1 FROM handoff_state WHERE task_ref = ?",
        (backlog_task_ref,),
    ).fetchone()
    if live is not None:
        return
    objective = (
        "Finding backlog drain bucket (implementation note). Receives low/medium findings "
        f"auto-migrated at task archive (latest source: {source_task_ref}). "
        "Drain via weekly debt digest + periodic findings-triage disposition."
    )
    conn.execute(
        """
        INSERT INTO handoff_state (
            id, task_ref, objective, focus, status, target_branch, target_worktree_path,
            task_plan_path,
            revision, updated_at, updated_by, updated_branch, updated_commit_sha
        ) VALUES (NULL, ?, ?, NULL, 'in_progress', NULL, NULL, NULL, 0, datetime('now'), ?, NULL, NULL)
        """,
        (backlog_task_ref, objective, agent or "archive-triage"),
    )


def _migrate_open_findings_to_backlog(
    conn: sqlite3.Connection,
    *,
    source_task_ref: str,
    backlog_task_ref: str,
    rows: list[sqlite3.Row],
) -> int:
    """Move open low/medium findings onto the backlog task with provenance.

    Findings stay ``status=open`` (not superseded). Collision-safe on
    ``(task_ref, finding_id)`` by prefixing the source task when needed.
    """
    migrated = 0
    for row in rows:
        finding_db_id = int(row["id"])
        finding_id = str(row["finding_id"])
        collision = conn.execute(
            """
            SELECT 1 FROM review_findings
            WHERE task_ref = ? AND finding_id = ? AND id != ?
            """,
            (backlog_task_ref, finding_id, finding_db_id),
        ).fetchone()
        target_finding_id = f"{source_task_ref}:{finding_id}" if collision is not None else finding_id
        # Re-check after rewrite (rare double collision).
        if target_finding_id != finding_id:
            collision2 = conn.execute(
                """
                SELECT 1 FROM review_findings
                WHERE task_ref = ? AND finding_id = ? AND id != ?
                """,
                (backlog_task_ref, target_finding_id, finding_db_id),
            ).fetchone()
            if collision2 is not None:
                target_finding_id = f"{source_task_ref}:{finding_id}:{finding_db_id}"
        note = _ARCHIVE_MIGRATE_NOTE.format(source=source_task_ref, backlog=backlog_task_ref)
        existing_notes = row["resolution_notes"] if "resolution_notes" in row.keys() else None
        new_notes = _append_resolution_note(
            str(existing_notes) if existing_notes is not None else None,
            note,
        )
        # SANCTIONED EXCEPTION (implementation note): writes resolution_notes on a row that
        # stays status='open', which update_review_finding()'s input guard
        # ("resolution_notes is not supported for status='open'") forbids on the
        # human close/reopen path. Here the note is archive→backlog *provenance*,
        # not closure rationale, so that guard deliberately does not apply.
        # Benign today (no consumer asserts open ⇒ notes-NULL). A future guard
        # author must special-case these backlog provenance rows. Mirrored at
        # review_findings_updates._reanchor_review_finding_impl.
        cursor = conn.execute(
            """
            UPDATE review_findings
            SET task_ref = ?,
                finding_id = ?,
                resolution_notes = ?,
                updated_at = datetime('now')
            WHERE id = ? AND status = 'open' AND task_ref = ?
            """,
            (backlog_task_ref, target_finding_id, new_notes, finding_db_id, source_task_ref),
        )
        migrated += int(cursor.rowcount)
    return migrated


def _triage_open_findings_for_archive(
    conn: sqlite3.Connection,
    task_ref: str,
    *,
    apply: bool = False,
    agent: str | None = None,
    day: str | None = None,
) -> dict[str, object]:
    """Severity-aware archive gate (internal).

    Replaces the former severity-blind ``_supersede_open_findings_for_archive``:

    - open HIGH → blocked-with-remedy (no writes when apply=True either)
    - open low/medium → when ``apply=True``, auto-migrate to the single dated
      ``internal-<YYYYMMDD>`` row with provenance
    - no open findings → no-op

    Never severity-blind-supersedes open high findings.
    """
    from .enums import FindingSeverity, FindingStatus

    rows = conn.execute(
        """
        SELECT id, finding_id, severity, resolution_notes
        FROM review_findings
        WHERE task_ref = ? AND status = ?
        ORDER BY id ASC
        """,
        (task_ref, FindingStatus.OPEN.value),
    ).fetchall()
    high_rows = [row for row in rows if str(row["severity"]) == FindingSeverity.HIGH.value]
    low_med_rows = [
        row for row in rows if str(row["severity"]) in {FindingSeverity.LOW.value, FindingSeverity.MEDIUM.value}
    ]
    if high_rows:
        high_ids = [str(row["finding_id"]) for row in high_rows]
        return {
            "blocked": True,
            "error": "open_high_findings_block_archive",
            "remedy": _HIGH_ARCHIVE_REMEDY,
            "high_finding_ids": high_ids,
            "open_high_count": len(high_rows),
            "open_low_medium_count": len(low_med_rows),
            "migrated_findings": 0,
            "backlog_task_ref": None,
            "tombstoned_findings": 0,
        }
    if not low_med_rows:
        return {
            "blocked": False,
            "error": None,
            "remedy": None,
            "high_finding_ids": [],
            "open_high_count": 0,
            "open_low_medium_count": 0,
            "migrated_findings": 0,
            "backlog_task_ref": None,
            "tombstoned_findings": 0,
        }
    backlog_task_ref = _finding_backlog_task_ref(day=day)
    if not apply:
        return {
            "blocked": False,
            "error": None,
            "remedy": None,
            "high_finding_ids": [],
            "open_high_count": 0,
            "open_low_medium_count": len(low_med_rows),
            "migrated_findings": 0,
            "backlog_task_ref": backlog_task_ref,
            "tombstoned_findings": 0,
        }
    _ensure_finding_backlog_row(
        conn,
        backlog_task_ref,
        source_task_ref=task_ref,
        agent=agent,
    )
    migrated = _migrate_open_findings_to_backlog(
        conn,
        source_task_ref=task_ref,
        backlog_task_ref=backlog_task_ref,
        rows=low_med_rows,
    )
    return {
        "blocked": False,
        "error": None,
        "remedy": None,
        "high_finding_ids": [],
        "open_high_count": 0,
        "open_low_medium_count": len(low_med_rows),
        "migrated_findings": migrated,
        "backlog_task_ref": backlog_task_ref,
        "tombstoned_findings": 0,
    }


def _supersede_open_findings_for_archive(conn: sqlite3.Connection, task_ref: str) -> int:
    """Deprecated compatibility shim — severity-aware triage, not blind supersede.

    internal replaced severity-blind supersede. Callers that still
    invoke this name get low/medium migration (apply=True). Open high findings
    are left open (not superseded); callers that need the full gate should use
    ``_triage_open_findings_for_archive`` and honour ``blocked``.
    """
    result = _triage_open_findings_for_archive(conn, task_ref, apply=True)
    if result.get("blocked"):
        return 0
    return int(cast(int, result.get("migrated_findings") or 0))


def _registration_id(row: dict, task_ref: str) -> int:
    if (
        not isinstance(row, dict)
        or type(row.get("id")) is not int
        or row["id"] <= 0
        or row.get("task_ref") != task_ref
        or (row.get("identity_seq") is not None and type(row["identity_seq"]) is not int)
        or not all(isinstance(row.get(key), str) for key in ("lane_id", "branch", "worktree_path", "status"))
    ):
        raise ValueError("invalid retained lane registration")
    return row["id"]


def _registration_json(encoded: str):
    try:
        return json.loads(encoded)
    except RecursionError as exc:
        raise ValueError("unreadable lane registration history") from exc


def _archive_registrations(task_ref: str, snapshot: dict, *, require_lanes: bool = True) -> dict[int, dict]:
    if not isinstance(snapshot, dict) or (require_lanes and "worktree_lanes" not in snapshot):
        raise ValueError("archived lane registrations unavailable")
    rows = _parse_import_snapshot(snapshot).lanes
    by_id = {_registration_id(row, task_ref): row for row in rows}
    if len(by_id) != len(rows):
        raise ValueError("duplicate archived lane registration")
    return by_id


def registration_history(conn: sqlite3.Connection, task_ref: str | None = None) -> list[dict]:
    """Resolve current lane state by local row identity in the caller's read transaction."""
    if not conn.in_transaction:
        raise ValueError("registration history requires a transaction")
    where = " WHERE task_ref = ?" if task_ref is not None else ""
    args = (task_ref,) if task_ref is not None else ()
    rows = {row["id"]: dict(row) for row in conn.execute("SELECT * FROM worktree_lanes" + where, args)}

    def include(row: dict):
        existing = rows.get(row["id"])
        if existing is None:
            rows[row["id"]] = dict(row, _archived=True)
        elif (existing["task_ref"], existing["lane_id"]) != (row["task_ref"], row["lane_id"]):
            raise ValueError("conflicting retained lane registration")

    # The row at removal supersedes a snapshot taken while it was still live.
    tables = ["task_archives"]
    if _retained_registration_store(conn):
        tables.insert(0, "retained_registration_snapshots")
    for table in tables:
        for task, encoded in conn.execute(f"SELECT task_ref, snapshot_json FROM {table}" + where, args):
            snapshot = _registration_json(encoded)
            for row in _archive_registrations(task, snapshot, require_lanes=task_ref is None).values():
                include(row)
    return [rows[row_id] for row_id in sorted(rows)]


def _persist_task_archive_snapshot(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    snapshot: TaskSnapshot | Mapping[str, object],
    ctx: ResolvedWriteContext,
    notes: str,
) -> None:
    previous = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref=?", (task_ref,)).fetchone()
    if previous is not None and "worktree_lanes" in snapshot:
        earlier = _registration_json(previous[0])
        retained = _archive_registrations(task_ref, earlier, require_lanes=False)
        if "worktree_lanes" not in earlier:
            _retain_registration_snapshot(conn, task_ref, {})

        for row in cast(list[dict], snapshot["worktree_lanes"]):
            row_id = _registration_id(row, task_ref)
            if row_id in retained and retained[row_id]["lane_id"] != row["lane_id"]:
                raise ValueError("conflicting archived lane registration")
            retained[row_id] = row
        snapshot = dict(snapshot, worktree_lanes=list(retained.values()))
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


def _reap_task_tool_roster_after_archive(conn: sqlite3.Connection, task_ref: str) -> None:
    """Retire next-boot roster intent after its live task has been archived."""
    conn.execute("DELETE FROM task_tool_roster WHERE task_ref = ?", (task_ref,))


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


# Portable identities. SQLite row IDs and generated columns belong to the destination.
_REVIEW_HISTORY_KEYS = {
    "review_runs": ("review_run_id",),
    "branch_review_judgments": ("run_id", "task_ref", "branch", "commit_sha"),
    "dispatch_attempts": ("subject_key", "ordinal", "lane_id"),
}


def _history_scope(table: str) -> str:
    return "subject_key" if table == "dispatch_attempts" else "task_ref"


def _history_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    # table_info omits generated columns. Only schema-owned names enter SQL.
    return [row["name"] for row in conn.execute(f"PRAGMA table_info({table})") if row["name"] != "id"]


def _snapshot_with_test_traces(
    conn: sqlite3.Connection, snapshot: TaskSnapshot | Mapping[str, object]
) -> dict[str, object]:
    payload = dict(snapshot)
    payload["review_findings_archive"] = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM review_findings_archive WHERE task_ref = ? ORDER BY id",
            (snapshot["task_ref"],),
        ).fetchall()
    ]
    task_ref = snapshot["task_ref"]
    payload["lane_prep_metrics"] = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM lane_prep_metrics WHERE task_ref = ? ORDER BY created_at DESC, id DESC",
            (task_ref,),
        )
    ]
    for table, key in _REVIEW_HISTORY_KEYS.items():
        columns = ", ".join(_history_columns(conn, table))
        payload[table] = [
            dict(row)
            for row in conn.execute(
                f"SELECT {columns} FROM {table} WHERE {_history_scope(table)} = ? ORDER BY {', '.join(key)}",
                (task_ref,),
            )
        ]
    payload["review_history_state"] = "unavailable" if review_history_unavailable(conn, task_ref) else "available"
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
        conn.execute("BEGIN")  # Pin the findings, their runs and the attempt history to one read snapshot.
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
                "review_findings_archive": len(snapshot.get("review_findings_archive", [])),
                "worktree_lanes": len(snapshot_typed["worktree_lanes"]),
                "worker_reports": len(snapshot_typed["worker_reports"]),
                "lane_messages": len(snapshot_typed["lane_messages"]),
                "plan_cursors": len(snapshot_typed.get("plan_cursors", [])),
                "turn_metrics": len(snapshot_typed.get("turn_metrics", [])),
                "lane_prep_metrics": len(cast(list, snapshot["lane_prep_metrics"])),
                "repo_instances": len(snapshot_typed.get("repo_instances", [])),
                "terminal_guard_events": len(snapshot_typed.get("terminal_guard_events", [])),
            },
        },
        task_ref=resolved_task_ref,
        artifacts=[{"type": "file", "path": str(destination)}],
    )


def _set_import_active_state(
    conn: sqlite3.Connection, task_ref: str, active: dict, *, git_context: tuple | None = None
) -> None:
    if git_context is None:
        detect_fn = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
        git_context = detect_fn()
    git_branch, git_commit = git_context
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


def _import_lane_prep_metric(
    conn: sqlite3.Connection,
    task_ref: str,
    row: dict,
    now: str,
    *,
    legacy: bool = False,
) -> None:
    attribution_json = row.get("attribution_json")
    if attribution_json is None:
        attribution_json = json.dumps(row.get("attribution", {}), sort_keys=True)
    fields = ("phys_footprint_mb", "phys_footprint_source", "pid", "cost_class")
    metrics = (
        conn.execute(
            """
            WITH legacy AS (SELECT CASE WHEN json_valid(:raw) THEN :raw END AS attribution)
            SELECT json_extract(attribution, '$.phys_footprint_mb'),
                   json_extract(attribution, '$.phys_footprint_source'),
                   json_extract(attribution, '$.pid'),
                   json_extract(attribution, '$.cost_class')
            FROM legacy
            """,
            {"raw": attribution_json},
        ).fetchone()
        if legacy
        else tuple(row.get(field) for field in fields)
    )
    conn.execute(
        """
        INSERT INTO lane_prep_metrics (
            task_ref, lane_id, session, cycle, backend, phys_footprint_mb,
            phys_footprint_source, pid, cost_class, attribution_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_ref,
            row.get("lane_id"),
            row.get("session", "import"),
            row.get("cycle"),
            row.get("backend", "unknown"),
            *metrics,
            attribution_json,
            row.get("created_at") or now,
        ),
    )


def _import_turn_metrics(conn: sqlite3.Connection, task_ref: str, rows: list[dict], now: str) -> int:
    legacy_prep_count = 0
    for row in rows:
        if row.get("phase") == "lane_prep":
            _import_lane_prep_metric(conn, task_ref, row, now, legacy=True)
            legacy_prep_count += 1
            continue
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
    return legacy_prep_count


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
    archived_findings: list
    lanes: list
    reports: list
    messages: list
    plan_cursors: list
    turn_metrics: list
    lane_prep_metrics: list
    repo_instances: list
    terminal_guard_events: list
    task_tool_roster: dict | None
    active: dict | None


_SNAPSHOT_LIST_FIELDS: tuple[tuple[str, str], ...] = (
    ("blockers", "blockers"),
    ("next_actions", "actions"),
    ("decisions", "decisions"),
    ("verified_tests", "tests"),
    ("review_findings", "findings"),
    ("review_findings_archive", "archived_findings"),
    ("worktree_lanes", "lanes"),
    ("worker_reports", "reports"),
    ("lane_messages", "messages"),
    ("plan_cursors", "plan_cursors"),
    ("turn_metrics", "turn_metrics"),
    ("lane_prep_metrics", "lane_prep_metrics"),
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
        extracted[attr_name] = list(raw)
    roster = snapshot.get("task_tool_roster")
    if roster is not None and not isinstance(roster, dict):
        raise ValueError(f"snapshot field 'task_tool_roster' must be an object or null, got {type(roster).__name__}")
    return SnapshotImportData(
        blockers=extracted["blockers"],
        actions=extracted["actions"],
        decisions=extracted["decisions"],
        tests=extracted["tests"],
        findings=extracted["findings"],
        archived_findings=extracted["archived_findings"],
        lanes=extracted["lanes"],
        reports=extracted["reports"],
        messages=extracted["messages"],
        plan_cursors=extracted["plan_cursors"],
        turn_metrics=extracted["turn_metrics"],
        lane_prep_metrics=extracted["lane_prep_metrics"],
        repo_instances=extracted.get("repo_instances", []),
        terminal_guard_events=extracted.get("terminal_guard_events", []),
        task_tool_roster=roster,
        active=snapshot.get("active"),
    )


def _import_review_finding_rows(
    conn: sqlite3.Connection,
    task_ref: str,
    findings: list[dict],
    *,
    archived: bool,
    now: str,
    fallback_agent: str,
    fallback_branch: str,
    fallback_commit: str | None,
    reconcile: bool = False,
) -> None:
    table = "review_findings_archive" if archived else "review_findings"
    columns = [column for column in REVIEW_FINDINGS_MIRRORED_COLUMNS if column != "id"]
    resident_occurrences: Counter[tuple] = Counter()
    if archived:
        columns.append("archived_at")
        if reconcile and findings:
            # Archive IDs are local and several occurrences can share a timestamp.
            # Count portable row values before inserting anything into the archive.
            resident = conn.execute(
                f"SELECT {', '.join(columns)} FROM review_findings_archive WHERE task_ref = ?", (task_ref,)
            )
            resident_occurrences.update(map(tuple, resident))
    for row in findings:
        agent, harness, branch, commit_sha, _model, _model_label, _reasoning_level = _resolve_import_actor_values(
            row,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
        values = {column: row.get(column) for column in columns}
        values.update(
            task_ref=task_ref,
            lane_id=_resolve_import_lane_id(row),
            finding_id=row.get("finding_id", ""),
            severity=row.get("severity", "low"),
            file_path=row.get("file_path", ""),
            description=row.get("description", ""),
            status=row.get("status", "open"),
            session=row.get("session", "import"),
            agent=agent,
            harness=harness,
            branch=branch,
            commit_sha=commit_sha,
            reopen_count=0 if archived else int(row.get("reopen_count") or 0),
            created_at=_row_created_at(row, now),
            updated_at=_row_updated_at(row, now, fallback_keys=("resolved_at",)),
        )
        if archived:
            # Archive exports carry the stored values, including explicit nulls.
            # Keep defaults only for absent fields and bind the task to the import scope.
            values["archived_at"] = now
            for column in columns:
                if column != "task_ref" and column in row:
                    values[column] = row[column]
            if resident_occurrences:
                occurrence = tuple(values[column] for column in columns)
                if resident_occurrences[occurrence] > 0:
                    resident_occurrences[occurrence] -= 1
                    continue
            # Retention copies hot IDs into the cold table. Allocate from both
            # tables and advance the hot sequence so later retention cannot
            # collide with IDs allocated by this import.
            values["id"] = conn.execute(
                "SELECT MAX(id) + 1 FROM (SELECT COALESCE(MAX(id), 0) AS id FROM review_findings "
                "UNION ALL SELECT COALESCE(MAX(id), 0) FROM review_findings_archive)"
            ).fetchone()[0]
        elif reconcile:
            existing = conn.execute(
                "SELECT * FROM review_findings WHERE task_ref=:task_ref AND finding_id=:finding_id", values
            ).fetchone()
            if existing is not None:
                _fill_missing_review_values(conn, table, values, existing, "id=?", (existing["id"],))
                continue
        placeholders = ", ".join("?" for _ in values)
        conn.execute(
            f"INSERT INTO {table} ({', '.join(values)}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        if archived:
            conn.execute(
                "INSERT INTO sqlite_sequence(name, seq) SELECT 'review_findings', ? "
                "WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'review_findings')",
                (values["id"],),
            )
            conn.execute(
                "UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = 'review_findings'",
                (values["id"],),
            )


_IMPORTED_REPORT_COLUMNS = (
    "task_ref",
    "lane_id",
    "session",
    "delivery_id",
    "summary",
    "changed_files_json",
    "test_commands_json",
    "blockers_json",
    "merge_ready",
    "status",
    "outcome",
    "agent",
    "branch",
    "commit_sha",
    "created_at",
)


def _import_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _import_report_values(task_ref, row, active, now, *, fallback=None) -> dict:
    if fallback is None:
        detect = _resolve_core_override("_detect_git_write_context", _detect_git_write_context)
        branch, commit = detect()
        fallback = _resolve_import_fallbacks(
            active,
            fallback_agent=_normalize_optional_text(resolve_env_alias("WORKBAY_HANDOFF_DEFAULT_AGENT")) or "unknown",
            fallback_branch=branch or "unknown-branch",
            fallback_commit=commit,
        )
    agent, _harness, branch, commit, *_ = _resolve_import_actor_values(
        row,
        fallback_agent=fallback[0],
        fallback_branch=fallback[1],
        fallback_commit=fallback[2],
    )
    outcome = row.get("outcome")
    if outcome not in {"finished", "failed", "exhausted", "stopped"}:
        outcome = None
    return dict(
        zip(
            _IMPORTED_REPORT_COLUMNS,
            (
                task_ref,
                row.get("lane_id", ""),
                row.get("session", "import"),
                row.get("delivery_id"),
                row.get("summary", ""),
                row.get("changed_files_json") or json.dumps(row.get("changed_files", [])),
                row.get("test_commands_json") or json.dumps(row.get("test_commands", [])),
                row.get("blockers_json") or json.dumps(row.get("blockers", [])),
                1 if row.get("merge_ready") else 0,
                row.get("status", "submitted"),
                outcome,
                agent,
                branch,
                commit,
                _row_created_at(row, now),
            ),
            strict=True,
        )
    )


@dataclasses.dataclass
class _ReportImport:
    request: production.ProductionAppend
    values: dict
    after_id: int
    settled: dict | None = None
    report_id: int | None = None
    recovering: bool = False


class _ReportImportRefusal(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _import_history(admission: dict) -> dict:
    """Authenticate the retained receipts, within the public two-page bound."""
    pages = []
    cursor = None
    for _ in range(2):
        page = production.get_lane_production_history(
            admission.get("root_binding", admission["binding"]),
            admission["admission_id"],
            cursor=cursor,
            historical=True,
        )["data"]
        pages.extend(page["history"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    if page["has_more"] or page["truncated"] or len(pages) != page["total_matching"]:
        raise ValueError("production history unavailable: incomplete import lookup")
    return dict(page, history=pages)


def _prepare_report_imports(task_ref: str, snapshot: dict, mode: str) -> dict[int, _ReportImport]:
    """Prepare before publication; imported rows never supply coverage authority.

    Optional production_evidence_id names a retained original settlement in
    this admission. Its product_artifacts must contain kind=worker_report and
    sha256 of canonical input JSON excluding that reference. Imported receipts
    cannot authenticate another import. The hash is checked against the host
    journal, never treated as a caller credential. No backfill/seal is written.
    """
    prepared = {}
    for index, row in enumerate(snapshot.get("worker_reports", [])):
        lane_id = row.get("lane_id")
        if not isinstance(lane_id, str) or not lane_id.strip():
            continue
        try:
            admission = production.get_lane_production_admission(task_ref=task_ref, lane_id=lane_id)["data"]
        except production.ProductionRefusal as exc:
            if exc.code in {
                production.RefusalCode.ADMISSION_ABSENT,
                production.RefusalCode.IDENTITY_MISMATCH,
                production.RefusalCode.CONTRACT_MISMATCH,
            }:
                # No authenticated current root: preserve the legacy import.
                # An invalid current identity/contract already makes N unavailable.
                continue
            raise
        if mode == "replace_task":
            # This mode deletes/recreates lane rows. It cannot reuse an
            # admitted identity or skip a settled report after deleting it.
            raise _ReportImportRefusal("replacement_requires_identity_transition")
        data = _import_history(admission)
        operation = "report-import:" + _import_digest([task_ref, admission["admission_id"], row])
        previous = [item for item in data["history"] if item["operation_id"] == operation]
        intent = next((item for item in previous if item["evidence_kind"] == "production_prepare"), None)
        settled = next((item for item in previous if item["evidence_kind"] == "production_settle"), None)
        if intent is None:
            with _get_db_connection() as conn:
                ctx = _resolve_write_actor(conn, None, task_ref=task_ref, allow_missing_worktree_fallback=True)
                after_id = conn.execute("SELECT COALESCE(MAX(id),0) FROM worker_reports").fetchone()[0]
            values = _import_report_values(
                task_ref,
                row,
                snapshot.get("active"),
                _utcnow_iso().replace("T", " ").replace("Z", ""),
            )
            claim = {key: value for key, value in row.items() if key != "production_evidence_id"}
            original = next(
                (
                    item
                    for item in data["history"]
                    if item["evidence_id"] == row.get("production_evidence_id")
                    and item["evidence_kind"] == "production_settle"
                    and item["payload"]["producer_kind"] != "handoff_report_import"
                    and isinstance(item["payload"]["facts"].get("product_artifacts"), list)
                    and {"kind": "worker_report", "sha256": _import_digest(claim)}
                    in item["payload"]["facts"].get("product_artifacts", [])
                ),
                None,
            )
            facts = {
                "report_values": values,
                "after_report_id": after_id,
                "coverage_complete": False,
                "classification": "product" if original else "claim",
                "source_evidence_id": original["evidence_id"] if original else None,
                "reason": "imported_history_unavailable" if original else "original_producer_evidence_unavailable",
            }
            request = production.ProductionAppend(
                binding=admission["binding"],
                admission_id=admission["admission_id"],
                operation_id=operation,
                producer_kind="handoff_report_import",
                producer_version="1",
                writer_authority_ref="host:import_handoff_state:"
                + _import_digest([ctx.agent, ctx.branch, ctx.commit_sha]),
                expected_head=production.ProductionHead(**data["head"]),
                facts=facts,
            )
            record = production.record_production_prepare(request)
        else:
            payload = intent["payload"]
            facts = payload["facts"]
            request = production.ProductionAppend(
                binding=admission["binding"],
                admission_id=admission["admission_id"],
                operation_id=operation,
                producer_kind=payload["producer_kind"],
                producer_version=payload["producer_version"],
                writer_authority_ref=payload["writer_authority_ref"],
                expected_head=production.ProductionHead(**data["head"]),
                facts=facts,
            )
            record = None
        # Each import is only a covered suffix. Even authenticated positive
        # product cannot fill the original producer's missing coverage.
        gap_exists = any(item["evidence_kind"] == "production_gap" for item in previous)
        if not gap_exists:
            production.record_production_gap(
                dataclasses.replace(
                    request,
                    expected_head=record.head if record else production.ProductionHead(**data["head"]),
                    facts={"reason": "imported_history_unavailable", "coverage_complete": False},
                )
            )
        prepared[index] = _ReportImport(
            request, facts["report_values"], facts["after_report_id"], settled, recovering=intent is not None
        )
    return prepared


def _find_imported_report(conn, prepared: _ReportImport) -> dict | None:
    """Reconcile the exact retained values, not a projected SHA or latest report."""
    rows = conn.execute(
        "SELECT * FROM worker_reports WHERE id>? AND "
        + " AND ".join(f"{key} IS ?" for key in _IMPORTED_REPORT_COLUMNS)
        + " LIMIT 2",
        (prepared.after_id, *(prepared.values[key] for key in _IMPORTED_REPORT_COLUMNS)),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("production history unavailable: ambiguous imported report")
    if prepared.recovering and not rows:
        changed = conn.execute(
            "SELECT 1 FROM worker_reports WHERE id>? AND task_ref=? AND lane_id=? LIMIT 1",
            (prepared.after_id, prepared.values["task_ref"], prepared.values["lane_id"]),
        ).fetchone()
        if changed is not None:
            # A consumer may have acknowledged the original row after commit.
            # Missing exact values cannot authorize a duplicate publication.
            raise ValueError("production history unavailable: imported report changed or cannot be identified")
    return dict(rows[0]) if rows else None


def _settle_report_imports(prepared: dict[int, _ReportImport]) -> bool:
    pending = False
    for item in {item.request.operation_id: item for item in prepared.values()}.values():
        if item.settled is not None:
            continue
        try:
            with _get_db_connection() as conn:
                row = conn.execute("SELECT * FROM worker_reports WHERE id=?", (item.report_id,)).fetchone()
                row = dict(row) if row else None
            if row is None or any(row[key] != item.values[key] for key in _IMPORTED_REPORT_COLUMNS):
                raise ValueError("production history unavailable: imported report not retained")
            admission = production.get_lane_production_admission(
                task_ref=item.values["task_ref"],
                lane_id=item.values["lane_id"],
            )["data"]
            if admission["binding"] != item.request.binding or admission["admission_id"] != item.request.admission_id:
                raise ValueError("production history unavailable: import binding changed")
            data = _import_history(admission)
            production.record_production_settle(
                dataclasses.replace(
                    item.request,
                    expected_head=production.ProductionHead(**data["head"]),
                    facts=dict(item.request.facts, report_id=row["id"], artifact_sha256=_import_digest(row)),
                )
            )
        except (ValueError, sqlite3.Error):
            _log.exception("imported report retained; production settlement pending")
            pending = True
    return pending


class _NativeImportRefusal(ValueError):
    """Snapshot labels cannot supply or discard live native authority."""


class _TerminalLaneImportRefusal(ValueError):
    """replace_task cannot displace or rewrite a live terminal merged lane row [GRPH-14]."""


def _check_terminal_lane_import(
    conn: sqlite3.Connection | None,
    task_ref: str,
    incoming: list[dict],
    mode: str,
) -> None:
    if conn is None or mode != "replace_task":
        return
    issuer_fields = ("task_ref", "lane_id", "worktree_path", "branch", "status", "identity_seq")
    imported_by_id = {row["id"]: row for row in incoming if type(row.get("id")) is int}
    for row in conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? AND status = 'merged'",
        (task_ref,),
    ):
        candidate = imported_by_id.get(row["id"])
        in_place = candidate is not None and all(row[key] == candidate.get(key) for key in issuer_fields)
        if not in_place:
            raise _TerminalLaneImportRefusal(
                f"replace_task cannot replace or delete live terminal merged lane '{row['lane_id']}'."
            )


def _terminal_lane_import_refusal(task_ref: str, exc: _TerminalLaneImportRefusal) -> dict:
    return _envelope(
        ok=False,
        tool="import_handoff_state",
        task_ref=task_ref,
        data={"error": str(exc), "error_kind": "lane_terminal"},
    )


def _check_native_import(
    snapshot: dict, conn: sqlite3.Connection | None = None, task_ref: str = "", mode: str = "merge"
) -> None:
    if any(row.get("status") in {"validated", "gated"} for row in snapshot.get("worktree_lanes", [])):
        raise _NativeImportRefusal("Imported native milestones require fresh source-bound qualification.")
    if conn is not None and mode == "replace_task":
        # Destructive import is not a native-policy reset. Preserve the original
        # lane row, generation, policy, audit and custody until revalidation.
        held = conn.execute(
            "SELECT 1 FROM worktree_lanes AS lane WHERE lane.task_ref=? AND "
            "(lane.status IN ('validated','gated') OR EXISTS "
            "(SELECT 1 FROM lane_validation_state AS policy WHERE policy.lane_row_id=lane.id)) LIMIT 1",
            (task_ref,),
        ).fetchone()
        if held is not None:
            raise _NativeImportRefusal("replace_task cannot discard an existing native validation hold.")


def _retain_registration_snapshot(conn: sqlite3.Connection, task_ref: str, snapshot: dict) -> None:
    _retained_registration_store(conn, create=True)
    conn.execute(
        "INSERT INTO retained_registration_snapshots(task_ref, snapshot_json) VALUES (?, ?)",
        (task_ref, json.dumps(snapshot, sort_keys=True)),
    )


def _retain_displaced_lanes(conn: sqlite3.Connection, task_ref: str, incoming: list[dict]) -> set[tuple]:
    """Keep matching local registrations in place. Retain each displaced row before deleting it."""
    # Only the local row can supply native identity and lifecycle facts. An
    # imported namesake with different facts cannot retire that registration.
    issuer_fields = ("task_ref", "lane_id", "worktree_path", "branch", "status", "identity_seq")
    imported = {row["id"]: row for row in incoming if type(row.get("id")) is int}
    kept = set()
    displaced = []
    for row in conn.execute("SELECT * FROM worktree_lanes WHERE task_ref=?", (task_ref,)):
        candidate = imported.get(row["id"])
        if candidate is not None and all(row[key] == candidate.get(key) for key in issuer_fields):
            kept.add((row["id"], task_ref, row["lane_id"]))
        else:
            displaced.append(dict(row))
    if displaced:
        _retain_registration_snapshot(conn, task_ref, {"worktree_lanes": displaced})
        conn.executemany("DELETE FROM worktree_lanes WHERE id=?", [(row["id"],) for row in displaced])
    return kept


def _native_import_refusal(task_ref: str, exc: _NativeImportRefusal) -> dict:
    return _envelope(
        ok=False,
        tool="import_handoff_state",
        task_ref=task_ref,
        data={"error": str(exc), "error_kind": "native_validation_required"},
    )


class _ReviewHistoryRefusal(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code


def _merge_review_history_rows(conn: sqlite3.Connection, task_ref: str, table: str, rows: list[dict]) -> None:
    key = _REVIEW_HISTORY_KEYS[table]
    scope = _history_scope(table)
    columns = _history_columns(conn, table)
    where = " AND ".join(f"{name} = ?" for name in key)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise _ReviewHistoryRefusal("invalid_review_history", f"{table} must be an array of objects")
    for row in rows:
        if table == "dispatch_attempts" and type(row.get("ordinal")) is not int:
            raise _ReviewHistoryRefusal("invalid_review_history", "dispatch_attempts ordinal must be an integer")
        if row.get(scope) != task_ref or any(row.get(name) is None for name in key) or row.keys() - set(columns):
            raise _ReviewHistoryRefusal("invalid_review_history", f"Invalid {table} identity or columns")
        if table == "branch_review_judgments":
            for field, kind in (("sources_json", dict), ("judge_pin_json", dict), ("blocking_ids", list)):
                value = row.get(field)
                if value is not None:
                    try:
                        valid = isinstance(json.loads(value), kind)
                    except (ValueError, TypeError):
                        valid = False
                    if not valid:
                        raise _ReviewHistoryRefusal("invalid_review_history", f"Invalid judgment {field}")
        identity = tuple(row[name] for name in key)
        current = conn.execute(f"SELECT * FROM {table} WHERE {where}", identity).fetchone()
        if current is None:
            names = [name for name in columns if name in row]
            conn.execute(
                f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join('?' for _ in names)})",
                tuple(row[name] for name in names),
            )
            if table != "review_runs":
                continue
            # The branch projection trigger can populate cycle and model on insert.
            # Reconcile the observed row under the same conflict rules as a replay.
            current = conn.execute(f"SELECT * FROM {table} WHERE {where}", identity).fetchone()
        _fill_missing_review_values(conn, table, row, current, where, identity)


def _merge_review_metadata(stored: dict, incoming: dict) -> dict:
    merged = dict(stored)
    for key, value in incoming.items():
        old = merged.get(key)
        if isinstance(old, dict) and isinstance(value, dict):
            merged[key] = _merge_review_metadata(old, value)
        elif old is None:
            merged[key] = value
        elif value is not None and old != value:
            raise _ReviewHistoryRefusal("review_history_conflict", f"Conflicting review metadata: {key}")
    return merged


def _fill_missing_review_values(conn, table, values, current, where, identity):
    updates = {}
    for name, value in values.items():
        if value is None or value == current[name]:
            continue
        if name in ("judge_pin_json", "sources_json") and current[name] is not None:
            try:
                stored, incoming = json.loads(current[name]), json.loads(value)
                if not isinstance(stored, dict) or not isinstance(incoming, dict):
                    raise ValueError("expected an object")
                had_unavailable = name == "judge_pin_json" and (
                    "unavailable_fields" in stored or "unavailable_fields" in incoming
                )
                if had_unavailable:
                    unavailable = stored.pop("unavailable_fields", []) + incoming.pop("unavailable_fields", [])
                merged = _merge_review_metadata(stored, incoming)
                if had_unavailable:
                    observed = dict(current) | {k: v for k, v in values.items() if v is not None} | merged
                    merged["unavailable_fields"] = [k for k in dict.fromkeys(unavailable) if observed.get(k) is None]
                if merged != json.loads(current[name]):
                    updates[name] = json.dumps(merged, sort_keys=True)
            except _ReviewHistoryRefusal:
                raise
            except (ValueError, TypeError) as exc:
                raise _ReviewHistoryRefusal("invalid_review_history", f"Invalid {name}: {exc}") from exc
            continue
        if current[name] is not None:
            code = "dispatch_attempt_evidence_conflict" if table == "dispatch_attempts" else "review_history_conflict"
            raise _ReviewHistoryRefusal(code, f"{table} {identity!r} has conflicting {name}")
        updates[name] = value
    if updates:
        assignments = ", ".join(f"{name} = ?" for name in updates)
        conn.execute(f"UPDATE {table} SET {assignments} WHERE {where}", (*updates.values(), *identity))


# The other legacy sections have their own conflict keys or replay checks.
_APPEND_ONLY_SECTIONS = (
    "blockers",
    "next_actions",
    "verified_tests",
    "worktree_lanes",
    "worker_reports",
    "lane_messages",
    "plan_cursors",
    "turn_metrics",
    "lane_prep_metrics",
    "active",
)


def _legacy_section_digest(conn, task_ref, section, snapshot):
    tables = ("handoff_state",) if section == "active" else (section,)
    if section == "verified_tests":
        tables += ("test_traces",)
    if section == "turn_metrics" and any(row.get("phase") == "lane_prep" for row in snapshot[section]):
        tables += ("lane_prep_metrics",)
    digest = hashlib.sha256()
    for table in tables:
        for row in conn.execute(f"SELECT * FROM {table} WHERE task_ref=? ORDER BY rowid", (task_ref,)):
            digest.update(_import_digest(list(row)).encode("ascii"))
    return digest.hexdigest()


def _restore_review_history(
    conn, task_ref, snapshot, data, mode, set_active, allow_destructive_clear, production_imports
):
    """Reconcile review evidence inside the caller's transaction, including on exact replay."""
    sections = _REVIEW_HISTORY_KEYS.keys() & snapshot.keys()
    state = snapshot.get("review_history_state", "unavailable")
    if sections or "review_history_state" in snapshot:
        if sections != _REVIEW_HISTORY_KEYS.keys() or snapshot.get("review_history_state") not in (
            "available",
            "unavailable",
        ):
            raise _ReviewHistoryRefusal("invalid_review_history", "Incomplete review history sections or state")
    previous = conn.execute("SELECT * FROM review_snapshot_history WHERE task_ref=?", (task_ref,)).fetchone()
    known_local = previous is not None and previous["state"] == "available"
    # A complete snapshot participates in reconciliation even when all three arrays are empty.
    reconcile = bool(sections)
    for table in _REVIEW_HISTORY_KEYS:
        scope = _history_scope(table)
        exists = conn.execute(f"SELECT 1 FROM {table} WHERE {scope}=? LIMIT 1", (task_ref,)).fetchone()
        if exists is not None:
            reconcile = True
            if previous is None and table == "dispatch_attempts":
                known_local = True
        if table not in sections:
            continue  # Absent optional sections never delete local evidence.
        rows = snapshot[table]
        if mode == "replace_task":
            if exists is not None and not rows and not allow_destructive_clear:
                raise _ReviewHistoryRefusal("destructive_clear_required", f"replace_task would clear {table}")
            conn.execute(f"DELETE FROM {table} WHERE {scope}=?", (task_ref,))
    for table in _REVIEW_HISTORY_KEYS:
        if table not in sections:
            continue
        rows = snapshot[table]
        try:
            _merge_review_history_rows(conn, task_ref, table, rows)
        except (sqlite3.IntegrityError, sqlite3.ProgrammingError, OverflowError) as exc:
            raise _ReviewHistoryRefusal("invalid_review_history", f"{table}: {exc}") from exc

    if known_local and (mode == "merge" or not sections):
        state = "available"
    # The legacy importer appends rows that have no portable key. Only skip
    # a replay when both its input and the resident section still match.
    try:
        previous_digests = json.loads(previous["legacy_sections_json"]) if previous else {}
        if not isinstance(previous_digests, dict):
            raise ValueError("expected a mapping")
    except (TypeError, ValueError) as exc:
        raise _ReviewHistoryRefusal("review_history_unavailable", "Unreadable legacy import receipt") from exc
    inputs = {}
    for name, attr in (*_SNAPSHOT_LIST_FIELDS, ("active", "active")):
        if name not in _APPEND_ONLY_SECTIONS or not snapshot.get(name):
            continue
        if (name == "active" and not set_active) or (name == "worker_reports" and production_imports):
            continue
        inputs[name] = _import_digest(snapshot[name])
        if reconcile and mode == "merge" and name in previous_digests:
            if previous_digests[name] == [inputs[name], _legacy_section_digest(conn, task_ref, name, snapshot)]:
                if name == "active":
                    data.active = None
                else:
                    getattr(data, attr).clear()
    return reconcile, state, inputs


def _finish_review_history_import(conn, task_ref, state, inputs, snapshot):
    digests = {name: [value, _legacy_section_digest(conn, task_ref, name, snapshot)] for name, value in inputs.items()}
    conn.execute(
        """INSERT INTO review_snapshot_history(task_ref, state, legacy_sections_json)
           VALUES (?, ?, ?) ON CONFLICT(task_ref) DO UPDATE SET state=excluded.state,
           legacy_sections_json=excluded.legacy_sections_json""",
        (task_ref, state, json.dumps(digests, sort_keys=True)),
    )


def _import_snapshot(
    conn: sqlite3.Connection,
    task_ref: str,
    snapshot: dict,
    mode: str,
    set_active: bool,
    production_imports: dict[int, _ReportImport] | None = None,
    allow_destructive_clear: bool = False,
    supersession_archive: str | None = None,
) -> dict[str, int]:
    _check_native_import(snapshot)
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
    task_tool_roster = data.task_tool_roster
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
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    if (
        set_active
        and isinstance(active, dict)
        and supersession.archived_custody_snapshot(conn, task_ref) != supersession_archive
    ):
        raise supersession.SupersessionRefusal("supersession_archive_changed_during_restore")
    if production_imports:
        # Reconcile every intent before this transaction inserts a report, so
        # a sibling published below never reads as a changed original.
        for prepared in production_imports.values():
            if prepared.settled is None:
                retained = _find_imported_report(conn, prepared)
                if retained is not None:
                    prepared.report_id = retained["id"]
    retained_live: set[tuple] = set()
    if mode == "replace_task":
        _check_native_import(snapshot, conn, task_ref, mode)
        _check_terminal_lane_import(conn, task_ref, lanes, mode)
        retained_live = _retain_displaced_lanes(conn, task_ref, lanes)
        for table in (
            "blockers",
            "next_actions",
            "decisions",
            "test_traces",
            "verified_tests",
            "review_findings",
            "worker_reports",
            "lane_messages",
            "task_tool_roster",
            "plan_cursors",
            "turn_metrics",
            "lane_prep_metrics",
            "terminal_guard_events",
        ):
            conn.execute(f"DELETE FROM {table} WHERE task_ref = ?", (task_ref,))
    if mode == "replace_task" and "review_findings_archive" in snapshot:
        conn.execute("DELETE FROM review_findings_archive WHERE task_ref = ?", (task_ref,))
    reconcile_findings, history_state, legacy_inputs = _restore_review_history(
        conn, task_ref, snapshot, data, mode, set_active, allow_destructive_clear, production_imports
    )
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
    for rows, archived in ((findings, False), (data.archived_findings, True)):
        _import_review_finding_rows(
            conn,
            task_ref,
            rows,
            archived=archived,
            reconcile=reconcile_findings,
            now=now,
            fallback_agent=fallback_agent,
            fallback_branch=fallback_branch,
            fallback_commit=fallback_commit,
        )
    for row in lanes:
        row_id = row.get("id")
        identity = (row_id, row.get("task_ref"), row.get("lane_id"))
        if type(row_id) is int and identity in retained_live:
            conn.execute(
                """UPDATE worktree_lanes SET title=?, objective=?, owner_agent=?, notes=?, updated_at=? WHERE id=?""",
                (
                    row.get("title"),
                    row.get("objective"),
                    row.get("owner_agent"),
                    row.get("notes"),
                    _row_updated_at(row, now),
                    row_id,
                ),
            )
            retained_live.remove(identity)
            continue
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
    published: dict[tuple, int] = {}
    for index, row in enumerate(reports):
        prepared = (production_imports or {}).get(index)
        if prepared is not None:
            if prepared.settled is not None:
                continue
            exact = tuple(prepared.values[column] for column in _IMPORTED_REPORT_COLUMNS)
            if prepared.report_id is None:
                prepared.report_id = published.get(exact)
            if prepared.report_id is not None:
                continue
            values = prepared.values
        else:
            values = _import_report_values(
                task_ref, row, active, now, fallback=(fallback_agent, fallback_branch, fallback_commit)
            )
        cursor = conn.execute(
            "INSERT INTO worker_reports ("
            + ",".join(_IMPORTED_REPORT_COLUMNS)
            + ") VALUES ("
            + ",".join("?" for _ in _IMPORTED_REPORT_COLUMNS)
            + ")",
            tuple(values[key] for key in _IMPORTED_REPORT_COLUMNS),
        )
        if prepared is not None:
            prepared.report_id = published[exact] = cursor.lastrowid
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
    if task_tool_roster is not None:
        conn.execute(
            """
            INSERT INTO task_tool_roster (task_ref, skill_slugs, activated_domains, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_ref) DO UPDATE SET
                skill_slugs=excluded.skill_slugs,
                activated_domains=excluded.activated_domains,
                updated_at=excluded.updated_at
            """,
            (
                task_ref,
                (
                    json.dumps(task_tool_roster.get("skill_slugs"))
                    if task_tool_roster.get("skill_slugs") is not None
                    else None
                ),
                (
                    json.dumps(task_tool_roster.get("activated_domains"))
                    if task_tool_roster.get("activated_domains") is not None
                    else None
                ),
                task_tool_roster.get("updated_at") or now,
            ),
        )
    _import_plan_cursors(conn, task_ref, plan_cursors, now)
    legacy_prep_count = _import_turn_metrics(conn, task_ref, turn_metrics, now)
    for row in data.lane_prep_metrics:
        _import_lane_prep_metric(conn, task_ref, row, now)
    if set_active and isinstance(data.active, dict):
        if production_imports:
            _set_import_active_state(conn, task_ref, active, git_context=(git_branch, git_commit))
        else:
            _set_import_active_state(conn, task_ref, active)
    _finish_review_history_import(conn, task_ref, history_state, legacy_inputs, snapshot)
    counts = {
        "blockers": len(blockers),
        "next_actions": len(actions),
        "decisions": len(decisions),
        "verified_tests": len(tests),
        "review_findings": len(findings),
        "review_findings_archive": len(data.archived_findings),
        "worktree_lanes": len(lanes),
        "worker_reports": len(reports),
        "lane_messages": len(messages),
        "plan_cursors": len(plan_cursors),
        "turn_metrics": len(turn_metrics) - legacy_prep_count,
        "repo_instances": len(repo_instances),
        "terminal_guard_events": len(terminal_guard_events),
        "task_tool_roster": 1 if task_tool_roster is not None else 0,
    }
    lane_prep_total = len(data.lane_prep_metrics) + legacy_prep_count
    if lane_prep_total > 0:
        counts["lane_prep_metrics"] = lane_prep_total
    return counts


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
    private_tables = {_PRIVATE_CLOSE_EVIDENCE_TABLE, _PRIVATE_CLOSE_OPENINGS_TABLE}
    if private_tables.intersection(payload) or private_tables.intersection(snapshot):
        return _envelope(
            ok=False,
            tool="import_handoff_state",
            data={"error": "Lane close evidence is private to the local handoff database."},
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
    optional_sections = (
        "plan_cursors",
        "turn_metrics",
        "lane_prep_metrics",
        "repo_instances",
        "terminal_guard_events",
        "review_findings_archive",
    )
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
    for key in ("review_findings", "review_findings_archive"):
        for row in snapshot.get(key, []):
            error = _validate_impact_origin(row.get("impact"), row.get("origin"))
            if error is not None:
                return _envelope(ok=False, tool="import_handoff_state", data={"error": error})
    if "active" in snapshot and snapshot["active"] is not None and not isinstance(snapshot["active"], dict):
        return _envelope(
            ok=False,
            tool="import_handoff_state",
            data={"error": "Invalid import payload: snapshot.active must be an object."},
        )
    custody_archive = None
    try:
        with _get_db_connection() as conn:
            _check_native_import(snapshot, conn, task_ref, mode)
            _check_terminal_lane_import(conn, task_ref, snapshot.get("worktree_lanes", []), mode)
            if set_active and isinstance(snapshot.get("active"), dict):
                custody_archive = supersession.archived_custody_snapshot(conn, task_ref)
    except _NativeImportRefusal as exc:
        return _native_import_refusal(task_ref, exc)
    except _TerminalLaneImportRefusal as exc:
        return _terminal_lane_import_refusal(task_ref, exc)
    with _get_db_connection() as conn:
        if mode == "replace_task" and not allow_destructive_clear:
            existing_counts = _count_task_rows(conn, task_ref)
            existing_counts["lane_prep_metrics"] = conn.execute(
                "SELECT COUNT(*) FROM lane_prep_metrics WHERE task_ref = ?", (task_ref,)
            ).fetchone()[0]
            if "review_findings_archive" in snapshot:
                existing_counts["review_findings_archive"] = conn.execute(
                    "SELECT COUNT(*) FROM review_findings_archive WHERE task_ref = ?", (task_ref,)
                ).fetchone()[0]
            incoming_counts = {key: len(snapshot.get(key, [])) for key in (*required_sections, *optional_sections)}
            legacy_prep_count = sum(row.get("phase") == "lane_prep" for row in snapshot.get("turn_metrics", []))
            incoming_counts["turn_metrics"] -= legacy_prep_count
            incoming_counts["lane_prep_metrics"] += legacy_prep_count
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
    production_imports = None
    try:
        with ExitStack() as custody:
            proof = json.loads(custody_archive)["task_supersession"] if custody_archive is not None else None
            reserved = custody.enter_context(supersession.reactivation_reservation(proof, task_ref=task_ref))
            try:
                production_imports = _prepare_report_imports(task_ref, snapshot, mode)
            except (ValueError, sqlite3.Error) as exc:
                _log.exception("report import refused before publication")
                return _envelope(
                    ok=False,
                    tool="import_handoff_state",
                    task_ref=task_ref,
                    data={
                        "production_history_unavailable": exc.reason
                        if isinstance(exc, _ReportImportRefusal)
                        else "prepare_failed",
                        "error": "Report import preparation failed.",
                    },
                )
            conn = custody.enter_context(_get_db_connection())
            if reserved:
                conn.execute("PRAGMA synchronous=FULL")
            counts = _import_snapshot(
                conn,
                task_ref=task_ref,
                snapshot=snapshot,
                mode=mode,
                set_active=set_active,
                production_imports=production_imports,
                allow_destructive_clear=allow_destructive_clear,
                supersession_archive=custody_archive,
            )
    except supersession.SupersessionRefusal as exc:
        return _reactivation_refusal("import_handoff_state", task_ref, str(exc))
    except _ReviewHistoryRefusal as exc:
        return _envelope(
            ok=False, tool="import_handoff_state", task_ref=task_ref, data={"error": str(exc), "error_kind": exc.code}
        )
    except _NativeImportRefusal as exc:
        return _native_import_refusal(task_ref, exc)
    except _TerminalLaneImportRefusal as exc:
        return _terminal_lane_import_refusal(task_ref, exc)
    except Exception:
        if not production_imports:
            raise
        # A failed/uncertain transaction is not zero production. Keep original
        # intents pending; a replay reconciles their exact rows before retrying.
        _log.exception("report import transaction failed; production reconciliation pending")
        return _envelope(
            ok=False,
            tool="import_handoff_state",
            task_ref=task_ref,
            data={
                "production_history_unavailable": "import_pending",
                "error": "Report import requires reconciliation.",
            },
        )
    settlement_pending = _settle_report_imports(production_imports)
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
            **({"production_history_unavailable": "settlement_pending"} if settlement_pending else {}),
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
              task_ref LIKE ?
              OR
              COALESCE(objective, '') LIKE ?
              OR COALESCE(task_plan_path, '') LIKE ?
          )
        """,
        (parent_task_ref, like_pattern, like_pattern, like_pattern),
    ).fetchall()

    cascade_archived: list[str] = []
    for row in cascade_candidates:
        if not (
            boundary_pattern.search(str(row["task_ref"]))
            or boundary_pattern.search(row["objective"])
            or boundary_pattern.search(row["task_plan_path"])
        ):
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
        _reap_task_tool_roster_after_archive(conn, child_ref)
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


DEFAULT_SYSTEM_DECISION_GC_DAYS = 180


def gc_system_decisions(
    conn: sqlite3.Connection,
    cutoff_days: int = DEFAULT_SYSTEM_DECISION_GC_DAYS,
    apply: bool = False,
) -> dict:
    """Reclaim aged ``decision_origin='system'`` decision rows.

    Dry-run (default) returns ``{"would_prune": n}`` without mutating.
    Apply deletes only rows with ``decision_origin='system'`` older than
    ``cutoff_days``; ``agent`` and NULL origins are never touched. Lane-landing
    rows (``decision GLOB 'lane_landed_*'`` or ``'*_lane_landed_*'``) are
    predicate evidence, not chatter: they are excluded so their lifetime is
    the task's archive lifetime ([RES-07]), not the default 180-day
    system-decision horizon. Both arms match the origin-stamp trigger so a
    namespaced form stamped ``system`` is never reclaimed while the prefix
    form survives. Immutable coordination facts (dispatch observations,
    terminal authorizations, launch reservations, observation epochs) are
    likewise never eligible: they are remote-obligation evidence, and their
    protection trigger would otherwise abort the whole reclaim. When
    ``apply=True`` and at least one row is pruned, inserts a summary
    decision (``tasks_gc_system_decisions_prune_<UTC stamp>``) whose origin is stamped ``system`` by the S3a insert trigger. FTS cleanup
    is free via the existing ``decisions_fts_delete`` AFTER-DELETE trigger.
    """
    if cutoff_days < 1:
        cutoff_days = DEFAULT_SYSTEM_DECISION_GC_DAYS

    from .coordination_declaration import immutable_fact_condition  # noqa: PLC0415

    age_sql = f"-{int(cutoff_days)} days"
    eligible = conn.execute(
        f"""
        SELECT id FROM decisions
        WHERE decision_origin = 'system'
          AND decision NOT GLOB 'lane_landed_*'
          AND decision NOT GLOB '*_lane_landed_*'
          AND NOT {immutable_fact_condition()}
          AND datetime(created_at) < datetime('now', ?)
        """,
        (age_sql,),
    ).fetchall()
    count = len(eligible)

    if not apply:
        return {"would_prune": count}

    if count == 0:
        return {"pruned": 0}

    ids = [int(row["id"]) for row in eligible]
    placeholders = ",".join("?" for _ in ids)
    conn.execute(f"DELETE FROM decisions WHERE id IN ({placeholders})", ids)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    summary_id = f"tasks_gc_system_decisions_prune_{stamp}"
    rationale = f"## tasks-gc system decision prune\n\nPruned {count} system decision(s) older than {cutoff_days} days."
    conn.execute(
        """
        INSERT INTO decisions (
            task_ref, lane_id, session, decision, rationale, agent, harness,
            model, model_label, reasoning_level,
            input_tokens, output_tokens, total_tokens,
            branch, commit_sha, changed_files_json, created_at
        )
        VALUES (?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '[]', datetime('now'))
        """,
        (
            "tasks_gc",
            "tasks_gc",
            summary_id,
            rationale,
            "tasks-gc",
        ),
    )
    return {"pruned": count, "summary_decision_id": summary_id}


def tasks_gc(apply: bool = False) -> dict:
    """Archive ``status=done`` on-main ``MAINT-*`` rows that are safely completable.

    Parented ``internal-*`` rows archive when their internal
    internal parent is in ``task_archives``. Parentless on-main ``MAINT-*``
    rows (no ``internal-\\d+`` reference) archive on their own. Dry-run by
    default; pass ``apply=True`` to mutate. Idempotent: a second invocation
    after ``apply=True`` is a no-op because archived rows are no longer
    present in ``handoff_state``.

    Also reclaims aged ``decision_origin='system'`` decisions (see
    ``gc_system_decisions``) *before* the no-MAINT-candidates early return so
    an apply run with zero MAINT candidates still prunes system rows.

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
            SELECT *
            FROM handoff_state
            WHERE task_ref LIKE 'MAINT-%'
              AND status = 'done'
            """
        ).fetchall()
        # Probe every target before system GC or any archive can take the
        # write lock. No Git subprocess may run under the ensuing DML lock.
        candidates = [
            row
            for row in candidates
            if not task_completion_hold_reason(
                task_ref=str(row["task_ref"]),
                status=str(row["status"]),
                target_branch=row["target_branch"],
                target_worktree_path=row["target_worktree_path"],
                workspace_root=get_runtime_config().git_workspace_root,
            )
        ]
        # Resolve actor before any apply-path DML. ``gc_system_decisions`` DELETE
        # takes RESERVED; resolving after it shells out under the write lock and
        # the tripwire raises (rolling the prune back) — DURREV-LOCK-2 / [CON-18].
        # Dry-run never mutates and never needs the actor context.
        ctx: ResolvedWriteContext | None = None
        if apply:
            ctx = _resolve_write_actor(conn, build_write_actor(agent="tasks-gc"))

        # implementation note S3b: system-decision reclaim runs even when no MAINT
        # candidates exist (must precede the early return below).
        system_gc = gc_system_decisions(conn, apply=apply)

    for row in candidates:
        with _get_db_connection() as conn:
            # System GC has committed. Refresh Git outside this row's writer
            # transaction, then fence the originally selected generation.
            if _arch_c_archive_hold(conn, row, "gc"):
                continue
            if apply:
                conn.execute("BEGIN IMMEDIATE")
                if not _arch_c_matches(conn, row):
                    continue
            # Skip done rows that are not on the integration branch: a done row
            # pinned to an unmerged feature branch is not safely archivable.
            if row["target_branch"] and not _is_integration_target_branch(row["target_branch"]):
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

            # internal: skip rows with open high findings (same gate
            # as archive_task_state) rather than severity-blind-superseding.
            pre_triage = _triage_open_findings_for_archive(conn, child_ref, apply=False)
            if pre_triage.get("blocked"):
                continue

            would_archive.append(child_ref)
            if not apply:
                continue
            # apply=True always resolves ctx before system-gc DML above.
            assert ctx is not None

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
            _reap_task_tool_roster_after_archive(conn, child_ref)
            # Severity-aware migrate (low/medium → dated internal)
            # replaces the former severity-blind supersede.
            _triage_open_findings_for_archive(
                conn,
                child_ref,
                apply=True,
                agent=ctx.agent if hasattr(ctx, "agent") else None,
            )
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
            **system_gc,
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


def _branch_ref_resolves(target_branch: str) -> bool | None:
    """Return whether the local or remote-tracking branch ref resolves.

    ``None`` is reserved for git failures so callers cannot mistake a broken
    ref probe for proof that a branch was deleted.
    """
    config = get_runtime_config()
    for ref in (f"refs/heads/{target_branch}", f"refs/remotes/origin/{target_branch}"):
        try:
            proc = subprocess.run(
                ["git", "show-ref", "--verify", "--quiet", ref],
                cwd=str(config.git_workspace_root),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
            return None
        if proc.returncode == 0:
            return True
        if proc.returncode != 1:
            return None
    return False


_GIT_MERGE_BRANCH_SUBJECT = re.compile(r"^Merge branch '([^']+)'(?:\s|$)")
_FROM_OWNER_BRANCH_SUBJECT = re.compile(r"(?:^|\s)from ([^\s/]+)/(\S+)$")
_GIT_TRAILER_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9-]*): (.*)$")
_LANDING_TRAILER_KEYS = frozenset(
    {
        "Workbay-Lane",
        "Workbay-Lane-Tip",
        "Workbay-Gate-Receipt",
        "Workbay-Landing-Run",
        "Workbay-Receipt-Version",
    }
)
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def _subject_proves_deleted_branch_merge(subject: str, target_branch: str) -> bool:
    """Return whether ``subject`` is an exact merge of ``target_branch``.

    Accepts only delimiter-bounded git merge subjects (``Merge branch '<branch>'``)
    or GitHub-style ``from <owner>/<branch>``. Mentions, reverts, and superstring
    siblings are not proof.
    """
    branch = _normalize_optional_text(target_branch)
    if not branch:
        return False
    text = (subject or "").strip()
    merge_match = _GIT_MERGE_BRANCH_SUBJECT.match(text)
    if merge_match is not None and merge_match.group(1) == branch:
        return True
    from_match = _FROM_OWNER_BRANCH_SUBJECT.search(text)
    return from_match is not None and from_match.group(2) == branch


def _landing_receipt_identity(message: str) -> tuple[bool, str | None, str | None]:
    """Return whether the final paragraph has landing trailers and its task/tip.

    A malformed or partial landing receipt still counts as a receipt, but it
    cannot prove a task. That prevents falling back to a branch-only subject
    when a merge carries contradictory or incomplete receipt metadata.
    """
    stripped = message.rstrip()
    if not stripped:
        return False, None, None
    paragraphs = [part for part in re.split(r"\n\s*\n", stripped) if part.strip()]
    fields: dict[str, str] = {}
    has_receipt_key = False
    for line in paragraphs[-1].splitlines():
        match = _GIT_TRAILER_LINE.fullmatch(line.rstrip())
        if match is None:
            continue
        key, value = match.group(1), match.group(2).strip()
        if key not in _LANDING_TRAILER_KEYS:
            continue
        has_receipt_key = True
        if key in fields:
            return True, None, None
        fields[key] = value
    if not has_receipt_key:
        return False, None, None

    lane = fields.get("Workbay-Lane", "")
    if lane.count("/") != 1 or any(char.isspace() for char in lane):
        return True, None, None
    receipt_task_ref, lane_id = lane.split("/", 1)
    if not receipt_task_ref or not lane_id:
        return True, None, None
    tip = fields.get("Workbay-Lane-Tip", "")
    if _HEX40.fullmatch(tip) is None:
        return True, None, None
    gate = fields.get("Workbay-Gate-Receipt", "")
    gate_match = re.fullmatch(r"test_result:([1-9][0-9]*)", gate)
    if gate_match is None:
        return True, None, None
    version = fields.get("Workbay-Receipt-Version", "1")
    if version != "1":
        return True, None, None
    run_id = fields.get("Workbay-Landing-Run")
    if run_id is not None and _HEX32.fullmatch(run_id) is None:
        return True, None, None
    return True, receipt_task_ref, tip


class _MergeProofDegraded(str, Enum):
    DEADLINE_EXCEEDED = "deadline_exceeded"
    ROSTER_READ_FAILED = "roster_read_failed"
    HISTORY_PROBE_FAILED = "history_probe_failed"


def _task_merge_proof_window_start(task_ref: str, *, deadline: float) -> tuple[bool, str | None]:
    """Read the task row's current timestamp as a conservative legacy cutoff.

    ``handoff_state`` currently has no immutable ``created_at`` column. Its
    ``updated_at`` is never earlier than task creation, so requiring legacy
    merge commits to be newer than it is conservative: it may defer an older
    valid merge, but cannot admit a merge from before the task existed. The
    read connection's busy timeout is capped by the caller's proof deadline.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False, None
    db_path = get_runtime_config().db_path
    if not Path(db_path).exists():
        return False, None
    busy_timeout_ms = int((deadline - time.monotonic()) * 1000)
    if busy_timeout_ms <= 0:
        return False, None
    conn: sqlite3.Connection | None = None
    try:
        conn = _open_bounded_roster_read_connection(db_path, busy_timeout_ms=busy_timeout_ms)
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            return False, None
        conn.execute(f"PRAGMA busy_timeout={remaining_ms}")
        row = conn.execute("SELECT updated_at FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    except (OSError, sqlite3.Error, RuntimeError):
        return False, None
    finally:
        if conn is not None:
            conn.close()
    if time.monotonic() >= deadline:
        return False, None
    if row is None:
        return True, None
    value = row["updated_at"] if isinstance(row, sqlite3.Row) else row[0]
    timestamp = _normalize_optional_text(value)
    return (timestamp is not None), timestamp


def _merge_is_newer_than_task(commit_timestamp: str, task_created_at: str) -> bool:
    try:
        commit_time = datetime.fromtimestamp(int(commit_timestamp), UTC)
        created_text = task_created_at.strip().replace("Z", "+00:00")
        created_time = datetime.fromisoformat(created_text)
    except (OverflowError, TypeError, ValueError):
        return False
    if created_time.tzinfo is None:
        created_time = created_time.replace(tzinfo=UTC)
    return commit_time > created_time.astimezone(UTC)


def _deleted_branch_merge_proof(
    target_branch: str,
    task_ref: str,
    integration_ref: str = "main",
    *,
    task_created_at: str | None = None,
    expected_tip: str | None = None,
) -> bool | None | _MergeProofDegraded:
    """Recover task-bound merge proof from integration history after deletion.

    A valid landing receipt proves only its own task and target branch: its
    second-parent tip must be named by an exact merge subject for the target
    branch. Both receipted and legacy merges need a known generation cutoff
    and a commit time newer than it, even when deletion leaves no source tip
    to check. Legacy merges also need an exact branch subject. History is read
    from ``integration_ref`` so an unrelated integration line cannot prove the
    branch landed.

    ``None`` means the history search finished and no commit matched. A
    timeout or any elapsed proof deadline returns
    ``_MergeProofDegraded.DEADLINE_EXCEEDED``. A launch or read failure
    (``FileNotFoundError``, ``OSError``, or a non-zero git status) returns
    ``_MergeProofDegraded.HISTORY_PROBE_FAILED``. Those degraded results are
    not a negative proof.
    """
    branch = _normalize_optional_text(target_branch)
    expected_task_ref = _normalize_optional_text(task_ref)
    if not branch or not expected_task_ref:
        return None
    integration = _normalize_optional_text(integration_ref) or "main"
    deadline = time.monotonic() + 5.0
    if task_created_at is None:
        task_created_ok, task_created_at = _task_merge_proof_window_start(expected_task_ref, deadline=deadline)
        if not task_created_ok:
            if time.monotonic() >= deadline:
                return _MergeProofDegraded.DEADLINE_EXCEEDED
            return _MergeProofDegraded.ROSTER_READ_FAILED
        if time.monotonic() >= deadline:
            return _MergeProofDegraded.DEADLINE_EXCEEDED
    config = get_runtime_config()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return _MergeProofDegraded.DEADLINE_EXCEEDED
    try:
        proc = subprocess.run(
            [
                "git",
                "log",
                "--format=%H%x00%ct%x00%P%x00%s%x00%B%x00",
                "--merges",
                "--fixed-strings",
                "--grep=Workbay-Lane:",
                f"--grep=Merge branch '{branch}'",
                f"--grep=/{branch}",
                integration,
            ],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=remaining,
        )
    except subprocess.TimeoutExpired:
        return _MergeProofDegraded.DEADLINE_EXCEEDED
    except (FileNotFoundError, OSError):
        return _MergeProofDegraded.HISTORY_PROBE_FAILED
    if proc.returncode != 0:
        return _MergeProofDegraded.HISTORY_PROBE_FAILED
    if time.monotonic() >= deadline:
        return _MergeProofDegraded.DEADLINE_EXCEEDED
    fields = (proc.stdout or "").split("\x00")
    for offset in range(0, len(fields) - 4, 5):
        if time.monotonic() >= deadline:
            return _MergeProofDegraded.DEADLINE_EXCEEDED
        commit_sha = fields[offset].strip()
        if not commit_sha:
            continue
        commit_timestamp = fields[offset + 1].strip()
        parents = fields[offset + 2].strip()
        subject = fields[offset + 3]
        message = fields[offset + 4]
        # A live branch must prove this exact source tip, not an older merge
        # under a reused branch name (or a baseline already reachable on main).
        if expected_tip is not None and parents.split()[1:] != [expected_tip]:
            continue
        if task_created_at is None or not _merge_is_newer_than_task(commit_timestamp, task_created_at):
            # A new analysis pass can reuse the same task/branch after its
            # source is deleted. An old receipt cannot complete that pass.
            # updated_at is the conservative lower bound for both live and
            # deleted source; missing or invalid generation authority holds.
            continue
        has_landing_receipt, receipt_task_ref, receipt_tip = _landing_receipt_identity(message)
        if has_landing_receipt:
            parent_shas = parents.split()
            if (
                receipt_task_ref == expected_task_ref
                and _subject_proves_deleted_branch_merge(subject, branch)
                and len(parent_shas) == 2
                and parent_shas[1] == receipt_tip
            ):
                if time.monotonic() >= deadline:
                    return _MergeProofDegraded.DEADLINE_EXCEEDED
                return True
            continue
        if not _subject_proves_deleted_branch_merge(subject, branch):
            continue
        if time.monotonic() >= deadline:
            return _MergeProofDegraded.DEADLINE_EXCEEDED
        return True
    if time.monotonic() >= deadline:
        return _MergeProofDegraded.DEADLINE_EXCEEDED
    return None


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
    """Age is a retention threshold, never a completion signal for live work."""
    if row["status"] != HandoffStatus.DONE.value:
        return False, ""
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


def planning_completion_hold_reason(
    *, workspace_root: Path, task_ref: str, branch: str, source_tip: str, integration_ref: str
) -> str | None:
    """Prove accepted bytes using existing task-scoped producer events, read-only.

    This fallback is for explicit completion only. Every path touched by unique
    source history (including reverted changes) needs authoritative custody;
    matching trees or a producer's success banner are insufficient.
    """
    import hashlib

    from workbay_protocol.branch_naming import extract_plan_id

    from .config import RuntimeConfig
    from .shared_schema import connect_handoff_db

    def hold(
        code: str,
        commit: str = source_tip,
        path: str = "-",
        remedy: str = "restore readable source and accepted-event authority",
    ) -> str:
        return f"{code}: commit={commit} file={path} remedy={remedy}"

    def git(*args: str) -> bytes:
        result = subprocess.run(
            ["git", "-C", str(workspace_root), *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            raise ValueError("git_read_failed")
        return result.stdout

    def ancestor(older: str, newer: str) -> bool:
        result = subprocess.run(
            ["git", "-C", str(workspace_root), "merge-base", "--is-ancestor", older, newer],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
        )
        if result.returncode not in {0, 1}:
            raise ValueError("git_ancestry_unreadable")
        return result.returncode == 0

    try:
        integration = git("rev-parse", "--verify", integration_ref + "^{commit}").decode("ascii").strip()
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return hold("planning_integration_unreadable", remedy="restore the named integration ref")
    path, commit = "-", source_tip
    try:
        if not re.fullmatch(r"[0-9a-f]{40}", source_tip):
            return hold("planning_evidence_unreadable")
        source_ref = "refs/heads/" + branch
        if git("rev-parse", "--verify", source_ref + "^{commit}").decode("ascii").strip() != source_tip:
            return hold("planning_source_changed", remedy="reselect the current source tip")
        commits = (
            git("rev-list", "--topo-order", "--max-count=129", integration + ".." + source_tip)
            .decode("ascii")
            .splitlines()
        )
        if not commits or len(commits) > 128:
            return hold("planning_unmerged_source_commit", remedy="integrate or split the bounded source history")
        paths: dict[str, list[str]] = {}
        for commit in commits:
            names = git(
                "diff-tree", "--root", "-m", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", commit
            )
            for name in names.split(b"\0"):
                if name:
                    paths.setdefault(name.decode("utf-8"), []).append(commit)
                    if len(paths) > 256:
                        return hold(
                            "planning_unmerged_source_commit", remedy="integrate or split oversized source history"
                        )
        if not paths:
            return hold("planning_unmerged_source_commit", remedy="integrate empty source history")
        db = RuntimeConfig.for_repo(workspace_root).db_path
        with closing(connect_handoff_db(db, read_only=True)) as conn:
            for path, changes in sorted(paths.items()):
                commit = changes[0]
                source = git("show", source_tip + ":" + path)
                try:
                    integrated_bytes = git("show", integration + ":" + path)
                except ValueError:
                    integrated_bytes = None
                digest = hashlib.sha256(source).hexdigest()
                candidates = [branch]
                # Mirror only the producer's numbered-plan relink: the same plan
                # id (protocol extract_plan_id: basename, any digit count) and
                # plan_accept._pre_relink_branch peel, bounded to 16 segments.
                # Each file peels to its OWN plan segment, not an arbitrary suffix.
                plan_id = extract_plan_id(path)
                name = branch
                for _ in range(16) if plan_id else ():
                    prefix, separator, digits = name.rpartition("-plan")
                    if not separator or not prefix or not digits.isdigit():
                        break
                    if separator + digits == "-plan" + plan_id:
                        candidates.append(prefix)
                        break
                    name = prefix
                accepted = False
                for original in candidates:
                    key = dict(task_ref=task_ref, source_branch=original, path=path, raw_sha256=digest)
                    prefix = (
                        "plan_accept_applied:"
                        + hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
                        + ":"
                    )
                    rows = conn.execute(
                        "SELECT decision,rationale FROM decisions WHERE task_ref=? AND session='plan-accept' "
                        "AND substr(decision,1,?)=? LIMIT 201",
                        (task_ref, len(prefix), prefix),
                    ).fetchall()
                    if len(rows) > 200:
                        return hold(
                            "planning_evidence_unreadable", commit, path, "split oversized accepted-event evidence"
                        )
                    for row in rows:
                        record = json.loads(row[1])
                        if not isinstance(record, dict) or any(record.get(k) != v for k, v in key.items()):
                            return hold("planning_receipt_invalid", commit, path)
                        tip, applied = record.get("source_tip"), record.get("applied_commit")
                        if (
                            not isinstance(tip, str)
                            or not isinstance(applied, str)
                            or not re.fullmatch(r"[0-9a-f]{40}", tip)
                            or not re.fullmatch(r"[0-9a-f]{40}", applied)
                            or row[0] != prefix + tip + ":" + applied
                        ):
                            return hold("planning_receipt_invalid", commit, path)
                        # Cover every unmerged change, even incomparable merge parents
                        # and edit/revert sequences whose final bytes are unchanged.
                        if not ancestor(tip, source_tip) or not all(ancestor(change, tip) for change in changes):
                            continue
                        if not ancestor(applied, integration):
                            return hold(
                                "planning_applied_commit_unmerged",
                                applied,
                                path,
                                "restore accepted integration history",
                            )
                        if git("show", tip + ":" + path) != source or git("show", applied + ":" + path) != source:
                            return hold("planning_receipt_invalid", commit, path)
                        accepted = True
                        break
                    if accepted:
                        break
                if not accepted:
                    if integrated_bytes is None:
                        return hold(
                            "planning_unmerged_source_commit", commit, path, "integrate the unaccepted changed file"
                        )
                    return hold(
                        "planning_receipt_missing",
                        commit,
                        path,
                        "publish original accepted custody; already_accepted is insufficient",
                    )
                if integrated_bytes != source:
                    return hold(
                        "planning_file_drift",
                        commit,
                        path,
                        "reconcile missing or drifted integration bytes and accept again",
                    )
        if git("rev-parse", "--verify", integration_ref + "^{commit}").decode("ascii").strip() != integration:
            return hold("planning_integration_changed", remedy="requalify the named integration tip")
        if git("rev-parse", "--verify", source_ref + "^{commit}").decode("ascii").strip() != source_tip:
            return hold("planning_source_changed", remedy="reselect the current source tip")
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, sqlite3.Error, subprocess.TimeoutExpired):
        return hold("planning_evidence_unreadable", commit, path)
    return None


def task_completion_hold_reason(
    *,
    task_ref: str,
    status: str,
    target_branch: str | None,
    target_worktree_path: str | None,
    workspace_root: str | Path,
    integration_ref: str = "main",
    explicit_completion: bool = False,
    task_updated_at: str | None = None,
    supersession_receipt: str | None = None,
    supersession_tip: str | None = None,
    reap_final_source_proven: bool = False,
) -> str | None:
    """Return a conservative hold before automatic or explicit completion.

    Inspect the stored target, never the actor's checked-out branch. Explicit
    completion may close a clean analysis task without a source commit; an
    automatic sweep additionally needs task-bound landing evidence. Without an
    owned worktree, an integration target must be verified at the workspace root.
    This is a read-only guard; archive custody and explicit archive stay owned
    by their existing paths.
    """
    branch = _normalize_optional_text(target_branch)
    worktree = _normalize_optional_text(target_worktree_path)
    if status in {"review", "blocked"}:
        return f"task remains {status}"
    if status not in {"in_progress", "done"}:
        return "task status unknown"
    if not branch:
        return "target branch unknown"
    integration_target = _is_integration_target_branch(branch, integration_ref)
    if supersession_receipt is not None and (
        not explicit_completion
        or integration_target
        or branch == integration_ref.removeprefix("refs/heads/")
        or not worktree
    ):
        return "supersession_requires_linked_task_target"
    if branch in {"main", "master"}:
        integration_ref = branch
    if integration_target and status != "done" and not explicit_completion:
        return "active integration-target task has no completion signal"
    if not worktree:
        if integration_target and (status == "done" or (explicit_completion and task_ref.startswith("MAINT-"))):
            # The stored integration branch can bind the workspace, but a
            # missing pointer cannot establish identity or cleanliness. Require
            # the same exact branch/source checks as an explicitly owned path.
            worktree = _normalize_optional_text(str(workspace_root))
            if not worktree:
                return "integration worktree unknown"
        else:
            # Reap qualification already proved the final source with the Git
            # roster read regardless of any stored path (_final_source_hold).
            # That replaces the path only where BASE never demanded one: the
            # stored spelling names no local or remote-tracking branch. A
            # resolvable live target keeps the stored-path hold below.
            if reap_final_source_proven and not explicit_completion and _branch_ref_resolves(branch) is False:
                return None
            # Qualified cleanup after a deleted branch still uses the existing
            # task/receipt/time-bound proof. Missing path alone is never proof.
            if not explicit_completion and _branch_ref_resolves(branch) is False:
                if (
                    _deleted_branch_merge_proof(branch, task_ref, integration_ref, task_created_at=task_updated_at)
                    is True
                ):
                    return None
            return "target worktree unknown"

    root = Path(workspace_root)
    path = Path(worktree)
    git = ["git", "--no-optional-locks"] if supersession_receipt is not None else ["git"]

    def probe(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*git, "-C", str(repo), *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )

    try:
        top = probe(path, "rev-parse", "--show-toplevel")
        if top.returncode != 0 or Path(top.stdout.strip()).resolve() != path.resolve():
            return "target worktree missing or mismatched"
        common = probe(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
        owner = probe(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
        if (
            common.returncode != 0
            or owner.returncode != 0
            or Path(common.stdout.strip()).resolve() != Path(owner.stdout.strip()).resolve()
        ):
            return "target belongs to another repository or is unreadable"
        checked_out = probe(path, "symbolic-ref", "--quiet", "HEAD")
        if checked_out.returncode != 0 or checked_out.stdout.strip() != f"refs/heads/{branch}":
            return "target branch/worktree identity mismatch"
        dirty = probe(path, "status", "--porcelain", "--untracked-files=all")
        if dirty.returncode != 0:
            return "target status unreadable"
        if dirty.stdout.strip():
            return "target worktree dirty"
        tip = probe(path, "rev-parse", "--verify", "HEAD")
        if tip.returncode != 0 or not tip.stdout.strip():
            return "target source unreadable"
        if supersession_receipt is not None:
            from workbay_orchestrator_mcp.orchestration.landing_cleanup import _registered_worktrees
            from workbay_orchestrator_mcp.orchestration.landing_log import reviewed_supersession_hold

            branch_ref = f"refs/heads/{branch}"
            registered = [entry.resolve() for entry, ref in _registered_worktrees(root) if ref == branch_ref]
            if path.resolve() == root.resolve() or path.is_symlink() or registered != [path.resolve()]:
                return "supersession_worktree_registration_mismatch"
            if supersession_tip is None or tip.stdout.strip() != supersession_tip:
                return "supersession_source_changed"
            return reviewed_supersession_hold(
                root, branch=branch, tip=supersession_tip, receipt=supersession_receipt, integration_ref=integration_ref
            )
        landed = probe(root, "merge-base", "--is-ancestor", tip.stdout.strip(), integration_ref)
        if landed.returncode != 0:
            # Explicit completion retains the existing single-patch squash /
            # rebase proof, against the named integration ref rather than the
            # actor's HEAD. Probe failures and incomplete equivalence hold.
            # Non-explicit callers keep the legacy hold text; only explicit
            # completion reports the typed planning code.
            if not explicit_completion:
                return "target source unmerged or integration unreadable"
            if landed.returncode != 1:
                return f"planning_integration_unreadable: commit={tip.stdout.strip()} file=- remedy=restore named integration ref"
            cherry = probe(root, "cherry", integration_ref, tip.stdout.strip())
            patches = cherry.stdout.splitlines()
            if (
                cherry.returncode != 0
                or not patches
                or not all(re.fullmatch(r"- [0-9a-f]{40}", patch) for patch in patches)
            ):
                return planning_completion_hold_reason(
                    workspace_root=root,
                    task_ref=task_ref,
                    branch=branch,
                    source_tip=tip.stdout.strip(),
                    integration_ref=integration_ref,
                )
    except (ImportError, OSError, ValueError, RuntimeError, subprocess.TimeoutExpired):
        return "target probe unavailable"

    if status != "done" and not explicit_completion and not reap_final_source_proven:
        proof = _deleted_branch_merge_proof(
            branch,
            task_ref,
            integration_ref,
            task_created_at=task_updated_at,
            expected_tip=tip.stdout.strip(),
        )
        if proof is not True:
            return "active source has no task-bound landing proof"
    return None


# Reap qualification: typed final-source proof. A row closes as done only
# when its final source identity is proven and contained in integration.
# Anything else holds with one of these codes naming the missing proof.
_REAP_HOLD_CODES = frozenset(
    {
        "abandonment_unproven",
        "landing_unproven",
        "custody_unproven",
        "generation_unproven",
        "lane_live",
        "integration_unreadable",
        "source_unreadable",
    }
)


def _reap_hold(code: str, detail: str) -> str:
    return f"{code}: {detail}"


def _reap_reason_code(reason: str) -> str | None:
    code = reason.split(":", 1)[0]
    if code in _REAP_HOLD_CODES:
        return code
    if reason == "branch missing with no merge proof":
        return "abandonment_unproven"
    if reason.startswith("branch missing with degraded merge proof") or reason in {
        "insufficient signals to classify safely",
        "main-target maintenance/planning row",
    }:
        # A subject or task-prefixed commit on main is never final-source proof.
        return "landing_unproven"
    return None


def _reap_git(root: Path, *args: str) -> subprocess.CompletedProcess[bytes] | None:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None


def _reap_no_repository(root: Path) -> bool:
    """True only when Git positively reports that ``root`` is in no repository.

    A workspace outside any repository has no refs and no worktree roster to
    read; every other read failure stays unknown and holds.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--git-dir"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
            env={**os.environ, "LC_ALL": "C", "LANGUAGE": "C"},
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 128 and b"not a git repository" in proc.stderr


def _reap_contained(root: Path, sha: str, integration_sha: str) -> bool | None:
    """Git ancestry of ``sha`` in the resolved integration commit; ``None`` when unreadable."""
    if _HEX40.fullmatch(sha or "") is None:
        return None
    proc = _reap_git(root, "merge-base", "--is-ancestor", sha, integration_sha)
    if proc is None or proc.returncode not in (0, 1):
        return None
    return proc.returncode == 0


def _source_branch_names(target_branch: str) -> set[str]:
    """Every branch name a stored target spelling can denote (superset, fail-safe)."""
    names = {target_branch}
    for prefix in ("refs/heads/", "refs/remotes/origin/", "origin/"):
        if target_branch.startswith(prefix) and len(target_branch) > len(prefix):
            names.add(target_branch[len(prefix) :])
    return names


def _stored_branch_ref_resolves(target_branch: str) -> bool | None:
    """Whether any branch a stored target spelling denotes resolves; ``None`` on a Git failure.

    A live ``refs/heads/<b>`` or ``origin/<b>`` spelling must not read as a
    deleted branch (abandonment) only because the probe prefixes it again.
    """
    unknown = False
    for name in sorted(_source_branch_names(target_branch)):
        resolved = _branch_ref_resolves(name)
        if resolved:
            return True
        unknown = unknown or resolved is None
    return None if unknown else False


def _worktree_roster(root: Path) -> list[tuple[str, str | None]] | None:
    """``(path, branch ref)`` for every registered worktree, decoded losslessly."""
    proc = _reap_git(root, "worktree", "list", "--porcelain", "-z")
    if proc is None or proc.returncode != 0:
        return None
    entries: list[tuple[str, str | None]] = []
    path: str | None = None
    ref: str | None = None
    for field in proc.stdout.split(b"\0"):
        if not field:
            if path is not None:
                entries.append((path, ref))
            path, ref = None, None
            continue
        text = os.fsdecode(field)
        if text.startswith("worktree "):
            path = text[len("worktree ") :]
        elif text.startswith("branch "):
            ref = text[len("branch ") :]
    if path is not None:
        entries.append((path, ref))
    return entries


def _lane_final_receipt(
    lanes: Sequence[Mapping[str, object]],
    names: set[str],
    root: Path,
    integration_ref: str,
    *,
    expected_tip: str | None = None,
) -> tuple[bool, str]:
    """Deleted-source proof from the current-generation terminal lane record.

    The current generation is the lane on the target branch with the latest
    ``cycle_epoch`` (bumped by storage on every status change, so a reopen
    supersedes earlier closes). Its final tip must have been observed from the
    branch (``branch_tip_source='branch'``) by the terminal write itself
    (``branch_tip_observed_at == updated_at``); a tip carried over from open or
    an earlier generation is not final-source proof. A ``closed`` lane also
    needs its recorded landing; both must be contained in integration. With
    ``expected_tip`` (a live source), the receipt must name exactly that tip.
    """
    on_target = [
        lane
        for lane in lanes
        if isinstance(lane.get("branch"), str) and _source_branch_names(str(lane["branch"])) & names
    ]
    if not on_target:
        return False, _reap_hold("abandonment_unproven", "no lane record for the deleted target branch")
    if len(on_target) > 1 and any(lane.get("cycle_epoch") is None for lane in on_target):
        return False, _reap_hold("generation_unproven", "lane generations on the target branch are unordered")
    current = max(on_target, key=lambda lane: int(cast(int, lane.get("cycle_epoch") or 0)))
    lane_id = current.get("lane_id")
    status = current.get("status")
    tip = current.get("branch_tip_sha")
    landing = current.get("landing_commit_sha")
    if status not in {"merged", "closed"}:
        return False, _reap_hold("landing_unproven", f"current lane {lane_id} is {status}, not merged or closed")
    if (
        not isinstance(tip, str)
        or current.get("branch_tip_source") != "branch"
        or current.get("branch_tip_observed_at") is None
        or current.get("branch_tip_observed_at") != current.get("updated_at")
    ):
        return False, _reap_hold(
            "landing_unproven", f"current lane {lane_id} has no final tip observed from the branch at close"
        )
    if status == "closed" and not isinstance(landing, str):
        return False, _reap_hold("landing_unproven", f"closed lane {lane_id} recorded no landing commit")
    if expected_tip is not None and tip != expected_tip:
        return False, _reap_hold(
            "landing_unproven", f"lane {lane_id} final tip {tip} is not the live tip {expected_tip}"
        )
    integration_sha = _resolve_integration_sha(root, integration_ref)
    if integration_sha is None:
        return False, _reap_hold("integration_unreadable", f"integration ref {integration_ref} does not resolve")
    if _reap_contained(root, tip, integration_sha) is not True:
        return False, _reap_hold("landing_unproven", f"lane {lane_id} final tip {tip} is not in integration")
    if isinstance(landing, str) and (
        _reap_contained(root, landing, integration_sha) is not True or _reap_contained(root, tip, landing) is not True
    ):
        return False, _reap_hold("landing_unproven", f"lane {lane_id} landing {landing} does not land {tip}")
    return True, f"lane {lane_id} final tip {tip} contained in integration"


def _task_lanes(conn: sqlite3.Connection, task_ref: str) -> list[dict[str, object]]:
    return [dict(lane) for lane in conn.execute("SELECT * FROM worktree_lanes WHERE task_ref = ?", (task_ref,))]


def _resolve_integration_sha(root: Path, integration_ref: str) -> str | None:
    proc = _reap_git(root, "rev-parse", "--verify", "--quiet", "--end-of-options", integration_ref + "^{commit}")
    sha = proc.stdout.decode("ascii", "replace").strip() if proc is not None and proc.returncode == 0 else ""
    return sha if _HEX40.fullmatch(sha) else None


def _deleted_source_lane_receipt_hold(
    conn: sqlite3.Connection, task_ref: str, target_branch: str, integration_ref: str
) -> str | None:
    """``None`` when the current-generation lane receipt proves the deleted source landed."""
    root = Path(get_runtime_config().git_workspace_root)
    proven, detail = _lane_final_receipt(
        _task_lanes(conn, task_ref), _source_branch_names(target_branch), root, integration_ref
    )
    return None if proven else detail


def _reap_source_names(lanes: Iterable[Mapping[str, object]], target_branch: str, integration_ref: str) -> set[str]:
    """Every branch name final-source proof covers: the target and each non-integration lane branch.

    Shared by the qualifier (:func:`_final_source_hold`) and the locked
    fence (:class:`_QualifiedRefIdentities`), so the refs re-read under the
    writer lock are exactly the refs the qualification proved [DBG-10].
    """
    names = _source_branch_names(target_branch)
    for lane in lanes:
        branch = _normalize_optional_text(lane.get("branch"))
        if branch and not _is_integration_target_branch(branch, integration_ref):
            names |= _source_branch_names(branch)
    return names


def _reap_source_refs(names: Iterable[str]) -> set[str]:
    """Full ref names whose tips final-source proof reads for ``names``."""
    wanted = set()
    for name in names:
        wanted |= {f"refs/heads/{name}", f"refs/remotes/origin/{name}"}
        if name.startswith("refs/"):
            wanted.add(name)
    return wanted


def _live_tip_task_binding_hold(
    root: Path,
    task_ref: str,
    target_names: set[str],
    lanes: Sequence[Mapping[str, object]],
    tip: str,
    integration_sha: str,
    integration_ref: str,
    updated_at: str,
) -> str | None:
    """``None`` when the live tip is bound to this task, else a typed hold.

    Containment in integration and a tip newer than the row only say that some
    work landed. The tip is this task's source only with (a) BASE's task-bound
    landing proof (receipt or exact merge subject, tip as second parent), (b)
    the current-generation terminal lane receipt for exactly this tip, or (c)
    for a fast-forward landing, a commit newer than the row in the tip's
    history carrying this task's identity (``<task_ref>:`` subject or a
    Workbay-Lane receipt naming it). A landing receipt for the tip that names
    another task contradicts all three [SECD-05].
    """
    unbound = _reap_hold("landing_unproven", f"live source tip {tip} has no task-bound landing proof")
    receipts = _reap_git(
        root,
        "log",
        "--merges",
        "--fixed-strings",
        "--grep=Workbay-Lane:",
        "--format=%H%x00%P%x00%B%x00",
        integration_sha,
    )
    if receipts is None or receipts.returncode != 0:
        return _reap_hold("source_unreadable", f"landing receipts for live tip {tip} unreadable")
    fields = os.fsdecode(receipts.stdout).split("\0")
    for offset in range(0, len(fields) - 2, 3):
        merge, parents, message = fields[offset].strip(), fields[offset + 1].split(), fields[offset + 2]
        if parents[1:] != [tip]:
            continue
        has_receipt, receipt_task_ref, _ = _landing_receipt_identity(message)
        if has_receipt and receipt_task_ref != task_ref:
            return _reap_hold(
                "landing_unproven",
                f"landing receipt {merge} for live tip {tip} names {receipt_task_ref or 'no valid task'}",
            )
    for name in sorted(target_names):
        if (
            _deleted_branch_merge_proof(name, task_ref, integration_ref, task_created_at=updated_at, expected_tip=tip)
            is True
        ):
            return None
    if _lane_final_receipt(lanes, target_names, root, integration_ref, expected_tip=tip)[0]:
        return None
    try:
        generation = datetime.fromisoformat(updated_at.strip().replace("Z", "+00:00"))
        since = int((generation if generation.tzinfo else generation.replace(tzinfo=UTC)).timestamp())
    except (OverflowError, TypeError, ValueError):
        return unbound
    history = _reap_git(root, "log", f"--max-age={since}", "--format=%H%x00%ct%x00%B%x00", tip)
    if history is None or history.returncode != 0:
        return _reap_hold("source_unreadable", f"history of live tip {tip} unreadable")
    fields = os.fsdecode(history.stdout).split("\0")
    for offset in range(0, len(fields) - 2, 3):
        stamp, message = fields[offset + 1].strip(), fields[offset + 2].strip()
        if not _merge_is_newer_than_task(stamp, updated_at):
            continue
        if message.startswith(f"{task_ref}:") or _landing_receipt_identity(message)[1] == task_ref:
            return None
    return unbound


def _final_source_hold(
    conn: sqlite3.Connection,
    row: Mapping[str, object],
    integration_ref: str,
    *,
    deleted_source_proof: bool,
) -> str | None:
    """The single reap predicate: typed hold, or ``None`` when final source is proven.

    Live source: every existing local and remote-tracking ref of the target is
    contained in integration, every registered worktree on it (Git roster,
    always read) is clean at that tip, and the tip postdates the task row and
    is bound to this task (:func:`_live_tip_task_binding_hold`).
    Deleted source: the caller's typed proof (current-generation lane receipt
    or the task-bound merge proof) and no registered worktree on the branch.
    Every lane registration of the task must be terminal and none may record a
    tip or landing outside integration. A ``done`` row (reap_done) carries its
    own completion signal and skips only the live generation/binding step.
    """
    task_ref = str(row["task_ref"])
    target_branch = _normalize_optional_text(row["target_branch"])
    if not target_branch:
        return _reap_hold("abandonment_unproven", "target branch unknown")
    root = Path(get_runtime_config().git_workspace_root)
    unreadable_integration = _reap_hold("integration_unreadable", f"integration ref {integration_ref} does not resolve")
    resolved: list[str | None] = []

    def contained_in_integration(sha: str) -> bool | None:
        # Resolve once, on first use: every containment answer below is
        # against the same integration commit.
        if not resolved:
            resolved.append(_resolve_integration_sha(root, integration_ref))
        return None if resolved[0] is None else _reap_contained(root, sha, resolved[0])

    lanes = _task_lanes(conn, task_ref)
    for lane in lanes:
        if lane.get("status") not in TERMINAL_LANE_STATUSES:
            return _reap_hold("lane_live", f"lane {lane.get('lane_id')} is {lane.get('status')}")
        for column in ("branch_tip_sha", "landing_commit_sha"):
            sha = lane.get(column)
            if sha is None:
                continue
            contained = contained_in_integration(str(sha))
            if resolved[0] is None:
                return unreadable_integration
            if contained is None:
                return _reap_hold("source_unreadable", f"lane {lane.get('lane_id')} {column} {sha} unreadable")
            if not contained:
                return _reap_hold(
                    "landing_unproven", f"lane {lane.get('lane_id')} {column} {sha} is not in {integration_ref}"
                )

    target_names = _source_branch_names(target_branch)
    names = _reap_source_names(lanes, target_branch, integration_ref)
    wanted = _reap_source_refs(names)
    listed = _reap_git(root, "for-each-ref", "--format=%(refname)%00%(objectname)%00%(objecttype)")
    no_repository = (listed is None or listed.returncode != 0) and _reap_no_repository(root)
    if (listed is None or listed.returncode != 0) and not no_repository:
        return _reap_hold("source_unreadable", "source refs unreadable")
    tips: dict[str, str] = {}
    for line in listed.stdout.splitlines() if listed is not None and not no_repository else ():
        ref, _, rest = os.fsdecode(line).partition("\0")
        if ref not in wanted:
            continue
        sha, _, kind = rest.partition("\0")
        if kind != "commit" or _HEX40.fullmatch(sha) is None:
            return _reap_hold("source_unreadable", f"source ref {ref} is not a commit")
        tips[ref] = sha
    for ref, sha in sorted(tips.items()):
        contained = contained_in_integration(sha)
        if resolved[0] is None:
            return unreadable_integration
        if contained is None:
            return _reap_hold("source_unreadable", f"source ref {ref} ancestry unreadable")
        if not contained:
            return _reap_hold("landing_unproven", f"source ref {ref} tip {sha} is not in {integration_ref}")

    roster = [] if no_repository else _worktree_roster(root)
    if roster is None:
        return _reap_hold("custody_unproven", "worktree roster unreadable")
    registered: set[Path] = set()
    for path, ref in roster:
        if ref is None or ref.removeprefix("refs/heads/") not in names:
            continue
        registered.add(Path(path).resolve())
        tip = tips.get(ref)
        if tip is None:
            return _reap_hold("custody_unproven", f"worktree {path} holds {ref} with no commit on it")
        head = _reap_git(Path(path), "rev-parse", "--verify", "HEAD")
        dirty = _reap_git(Path(path), "status", "--porcelain", "--untracked-files=all")
        if head is None or dirty is None or head.returncode != 0 or dirty.returncode != 0:
            return _reap_hold("custody_unproven", f"worktree {path} on {ref} unreadable")
        if head.stdout.decode("ascii", "replace").strip() != tip or dirty.stdout.strip():
            return _reap_hold("custody_unproven", f"worktree {path} on {ref} is not clean at {tip}")
    stored = _normalize_optional_text(row["target_worktree_path"])
    if stored and Path(stored).exists() and Path(stored).resolve() not in registered:
        return _reap_hold("custody_unproven", f"stored worktree {stored} is not registered on {target_branch}")

    live = {
        ref: sha
        for ref, sha in tips.items()
        if ref.removeprefix("refs/heads/").removeprefix("refs/remotes/origin/") in target_names
    }
    if live:
        if row["status"] == HandoffStatus.DONE.value:
            # A done row carries its own completion signal, so BASE asks no
            # task-bound proof of it. The lane, sibling-ref and custody proof
            # above still applies: reap_done cannot archive a row reap holds.
            return None
        # A source still at a commit older than the task carries no task work.
        # That generation check is necessary, not sufficient: the tip must also
        # be bound to this task, or any later landing would complete it.
        updated_at = _normalize_optional_text(row["updated_at"])
        hold: str | None = _reap_hold(
            "generation_unproven", f"no live source tip of {target_branch} postdates the task row"
        )
        for sha in sorted(set(live.values())):
            stamp = _reap_git(root, "log", "-1", "--format=%ct", sha)
            if (
                updated_at is None
                or not resolved
                or resolved[0] is None
                or stamp is None
                or stamp.returncode != 0
                or not _merge_is_newer_than_task(stamp.stdout.decode("ascii", "replace").strip(), updated_at)
            ):
                continue
            hold = _live_tip_task_binding_hold(
                root, task_ref, target_names, lanes, sha, resolved[0], integration_ref, updated_at
            )
            if hold is None:
                return None
        return hold
    if not deleted_source_proof:
        return _reap_hold("landing_unproven", f"deleted source {target_branch} has no final-source receipt")
    return None


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
    if row["status"] in {"review", "blocked"}:
        return "active", f"task remains {row['status']}"
    stale_maint, stale_reason = _stale_maint_on_main_closeable(conn, row, integration_ref)
    if stale_maint:
        return "closeable", stale_reason
    if _is_integration_target_branch(target_branch, integration_ref):
        return "active", "main-target maintenance/planning row"
    merged = _branch_is_merged(target_branch, integration_ref)
    branch_ref_resolves: bool | None = None
    merge_proof: bool | None | _MergeProofDegraded = None
    lane_receipt_hold: str | None = None
    deleted_source_proof = False
    if target_branch and not merged:
        branch_ref_resolves = _stored_branch_ref_resolves(target_branch)
        if branch_ref_resolves is False:
            lane_receipt_hold = _deleted_source_lane_receipt_hold(conn, task_ref, target_branch, integration_ref)
            if lane_receipt_hold is None:
                merged = deleted_source_proof = True
            elif lane_receipt_hold.startswith("abandonment_unproven"):
                # No lane registered on the target: only the task-bound merge
                # proof (receipt or exact merge subject, newer than the row) can
                # recover a landing. A registered lane generation is authority.
                merge_proof = _deleted_branch_merge_proof(
                    target_branch,
                    task_ref,
                    integration_ref,
                    task_created_at=_normalize_optional_text(row["updated_at"]),
                )
                merged = deleted_source_proof = merge_proof is True
    has_open = _count_open_findings(conn, task_ref) > 0
    worktree_live = _worktree_is_live(
        target_worktree_path,
        target_branch,
        merged=merged,
        integration_ref=integration_ref,
    )

    if merged and not has_open:
        hold = _final_source_hold(conn, row, integration_ref, deleted_source_proof=deleted_source_proof)
        if hold is None:
            # The exact live tip is proven above; this retains the stored
            # worktree identity and cleanliness checks, not a merge-subject proof.
            hold = task_completion_hold_reason(
                task_ref=task_ref,
                status=str(row["status"]),
                target_branch=target_branch,
                target_worktree_path=target_worktree_path,
                workspace_root=get_runtime_config().git_workspace_root,
                integration_ref=integration_ref,
                task_updated_at=_normalize_optional_text(row["updated_at"]),
                reap_final_source_proven=True,
            )
        if hold:
            return "ambiguous", hold
        return "closeable", "branch merged with no open findings"
    if not merged and (worktree_live or has_open):
        if has_open:
            return "active", "unmerged branch with open findings"
        return "active", "unmerged branch with live worktree"
    if merged and has_open:
        return "ambiguous", "branch merged but open findings remain"
    if target_branch and not merged and branch_ref_resolves is not True:
        if isinstance(merge_proof, _MergeProofDegraded):
            return "ambiguous", f"branch missing with degraded merge proof: {merge_proof.value}"
        if lane_receipt_hold is not None and not lane_receipt_hold.startswith("abandonment_unproven"):
            return "ambiguous", lane_receipt_hold
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
        reason_code = _reap_reason_code(reason) if bucket != "closeable" else None
        if reason_code is not None:
            entry["reason_code"] = reason_code
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


def _run_terminal_review_findings_archive() -> BaseException | None:
    """Bounded MOVE of terminal findings. Never fails the outer mutation.

    Callers keep ``ok=True`` and attach a typed ``side_effect_failures`` entry
    when this returns an exception so the MOVE miss is MCP-visible.
    """
    try:
        # Bounded MOVE only. File-size reclaim is a separate operator
        # exclusive-lock step, never first-open bootstrap:
        #   sqlite3 .task-state/handoff.db 'VACUUM;'
        with _get_db_connection() as cull_conn:
            archive_terminal_review_findings(cull_conn)
    except Exception as exc:  # noqa: BLE001 — cull must not fail the outer mutation
        _log.exception(
            "archive_terminal_review_findings failed; terminal rows remain on the hot table until the next reap"
        )
        return exc
    return None


def reap_tasks(
    apply: bool = False,
    task_ref: str | None = None,
    integration_ref: str = "main",
) -> dict:
    """Classify live rows and optionally close+archive the closeable set."""
    with _get_db_connection() as conn:
        selected = {
            str(row["task_ref"]): dict(row)
            for row in conn.execute("SELECT * FROM handoff_state").fetchall()
            if task_ref is None or row["task_ref"] == task_ref
        }
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
            # A broad sweep can spend time completing earlier rows. Re-read
            # the exact task and re-probe its target before the status flip;
            # selection-time evidence is not authority for a changed source.
            with _get_db_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM handoff_state WHERE task_ref = ?",
                    (ref,),
                ).fetchone()
                if row is None or dict(row) != selected.get(ref) or row["status"] not in LIVE_ACTIVE_STATUSES:
                    bucket, reason = "ambiguous", "live task changed after selection"
                else:
                    bucket, reason = _classify_live_row(conn, row, integration_ref)
            if bucket != "closeable":
                failed.append({"task_ref": ref, "stage": "guard", "error": reason})
                continue
            status_result = update_task_status(
                task_ref=ref,
                status="done",
                expected_revision=int(row["revision"]),
                expected_state=dict(row),
                integration_ref=integration_ref,
            )
            if not status_result.get("ok"):
                # Status flip failed; the row is untouched and remains live, so
                # a later reap will retry it. Surface it rather than silently
                # dropping it from the result.
                failed.append({"task_ref": ref, "stage": "status", "error": _reap_error(status_result)})
                continue
            # Parent landing is not completion evidence for a review child.
            # Each child must qualify independently; explicit cascade archive
            # and the done-row MAINT janitor retain their existing custody.
            # The status receipt identifies the generation we just wrote. Never
            # archive a later row if another writer wins between these calls.
            completed = dict(row)
            completed.update(status_result.get("data", {}).get("active", {}))
            with _get_db_connection() as conn:
                post_status = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (ref,)).fetchone()
                if (
                    post_status is None
                    or int(post_status["revision"]) != int(row["revision"]) + 1
                    or post_status["status"] != "done"
                    or any(post_status[key] != completed.get(key) for key in dict(row))
                ):
                    failed.append({"task_ref": ref, "stage": "archive", "error": "task changed after status"})
                    continue
                archive_state = dict(post_status)
            archive_result = archive_task_state(
                task_ref=ref,
                cascade_maint_review=False,
                expected_state=archive_state,
                completion_source=dict(row),
                integration_ref=integration_ref,
            )
            if archive_result.get("ok"):
                reaped.append(ref)
            else:
                # Nothing was archived. When the archive held this reap's done
                # row, it already put back the pre-reap status, compare-and-
                # swapped on the row this reap wrote, so the row is not left
                # 'done' on an unproven final source; a row another writer
                # changed keeps that writer's state. Report the typed hold.
                held: dict[str, object] = {"task_ref": ref, "stage": "archive", "error": _reap_error(archive_result)}
                held_data = archive_result.get("data")
                if isinstance(held_data, dict) and "status_restored" in held_data:
                    held["status_restored"] = held_data["status_restored"]
                    if held_data.get("status_restore_error"):
                        held["status_restore_error"] = held_data["status_restore_error"]
                failed.append(held)

    # internal: stale plan-cursor sweep rides the same task-reap cadence.
    plan_cursors = reap_stale_plan_cursors(apply=apply)
    plan_cursor_data = plan_cursors.get("data") if isinstance(plan_cursors, dict) else None
    if not isinstance(plan_cursor_data, dict):
        plan_cursor_data = {}

    cull_error: BaseException | None = None
    if apply:
        # archive_terminal_review_findings — bounded MOVE of terminal rows
        cull_error = _run_terminal_review_findings_archive()

    result = _envelope(
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
            "plan_cursors": plan_cursor_data,
        },
    )
    if cull_error is not None:
        _surface_side_effect_failure(
            result,
            effect="archive_terminal_review_findings",
            error_class=_side_effect_error_class(cull_error),
            detail=(
                "archive_terminal_review_findings failed during reap_tasks: "
                f"{cull_error}; terminal rows remain on the hot table until the next reap"
            ),
        )
    return result


# internal: plan_cursors dispatched >72h reclaimer.
DEFAULT_PLAN_CURSOR_STALE_HOURS = 72
DEFAULT_PLAN_CURSOR_REAP_BATCH = 50
_PLAN_CURSOR_EXPIRED = "expired"
_PLAN_CURSOR_DISPATCHED = "dispatched"
# Lane statuses that still imply possible live work — never auto-expire.
_LANE_LIVE_STATUSES = LIVE_LANE_STATUSES


def _plan_cursor_entry(row: sqlite3.Row | Mapping[str, object], *, reason: str) -> dict[str, object]:
    return {
        "id": int(cast(int, row["id"])),
        "task_ref": str(row["task_ref"]),
        "plan_item_id": str(row["plan_item_id"]),
        "lane_id": row["lane_id"],
        "state": str(row["state"]),
        "dispatched_at": row["dispatched_at"],
        "updated_at": row["updated_at"],
        "reason": reason,
    }


def _probe_plan_cursor_liveness(
    conn: sqlite3.Connection,
    row: sqlite3.Row | Mapping[str, object],
) -> tuple[str, str]:
    """Classify a stale dispatched cursor as dead, alive, or ambiguous.

    Conclusive-dead requires proof the work is gone (lane terminal and/or
    worktree path missing). Probe failure or missing signals → ambiguous
    (never silent-expire).
    """
    try:
        raw_lane_id = row["lane_id"]
    except (KeyError, IndexError, TypeError):
        raw_lane_id = None
    lane_id = _normalize_optional_text(raw_lane_id)
    task_ref = str(row["task_ref"])
    if lane_id is None:
        return "ambiguous", "no lane_id; cannot probe liveness"

    try:
        lane = conn.execute(
            """
            SELECT lane_id, status, worktree_path, branch
            FROM worktree_lanes
            WHERE task_ref = ? AND lane_id = ?
            """,
            (task_ref, lane_id),
        ).fetchone()
    except sqlite3.Error as exc:
        return "ambiguous", f"lane probe unavailable: {exc}"

    if lane is None:
        # No lane row: cannot locate worktree; conservative triage, not expire.
        return "ambiguous", "lane row missing; cannot prove dead"

    status = str(lane["status"] or "")
    worktree_path = _normalize_optional_text(lane["worktree_path"])
    path_exists: bool | None
    if worktree_path is None:
        path_exists = False
    else:
        try:
            path_exists = Path(worktree_path).exists()
        except OSError as exc:
            return "ambiguous", f"worktree probe failed: {exc}"

    if path_exists is True and status in _LANE_LIVE_STATUSES:
        return "alive", f"lane {lane_id} status={status} with live worktree"

    if path_exists is False and status in TERMINAL_LANE_STATUSES:
        return "dead", f"lane {lane_id} terminal ({status}) and worktree missing"

    if path_exists is False and status in _LANE_LIVE_STATUSES:
        # Live status but worktree gone — could be mid-move; do not auto-expire.
        return "ambiguous", f"lane {lane_id} status={status} but worktree missing"

    if path_exists is True and status in TERMINAL_LANE_STATUSES:
        return "ambiguous", f"lane {lane_id} terminal ({status}) but worktree still present"

    return "ambiguous", f"lane {lane_id} inconclusive (status={status or 'unknown'})"


def _expire_plan_cursor_cas(
    conn: sqlite3.Connection,
    *,
    cursor_id: int,
    probed_updated_at: str | None,
    summary: str,
    note: str,
    terminal_identity: Mapping[str, object] | None = None,
) -> bool:
    """CAS-expire a cursor: only if still dispatched with probed updated_at."""
    new_summary = f"{summary} [{note}]" if note not in summary else summary
    identity_guard = ""
    identity_args: tuple[object, ...] = ()
    if terminal_identity is not None:
        # The terminal operation adds identity and lane-status guards. Existing
        # age-reaper callers keep their original CAS and conservative policy.
        identity_guard = """
          AND task_ref = ? AND plan_item_id = ? AND lane_id IS ?
          AND dispatched_at IS ? AND created_at IS ?
          AND EXISTS (SELECT 1 FROM worktree_lanes AS lane
                      WHERE lane.task_ref = plan_cursors.task_ref
                        AND lane.lane_id = plan_cursors.lane_id
                        AND lane.status IN ('merged', 'closed'))
        """
        identity_args = tuple(
            terminal_identity[key] for key in ("task_ref", "plan_item_id", "lane_id", "dispatched_at", "created_at")
        )
    cur = conn.execute(
        """
        UPDATE plan_cursors
        SET state = ?,
            summary = ?,
            completed_at = datetime('now'),
            updated_at = datetime('now')
        WHERE id = ?
          AND state = ?
          AND ((updated_at IS NULL AND ? IS NULL) OR updated_at = ?)
        """
        + identity_guard,
        (
            _PLAN_CURSOR_EXPIRED,
            new_summary,
            cursor_id,
            _PLAN_CURSOR_DISPATCHED,
            probed_updated_at,
            probed_updated_at,
        )
        + identity_args,
    )
    return int(cur.rowcount or 0) == 1


def expire_terminal_plan_cursors(
    *,
    task_ref: str,
    apply: bool = False,
    max_batch: int = DEFAULT_PLAN_CURSOR_REAP_BATCH,
) -> dict:
    """Dispose only this task's dispatched cursors on merged or closed lanes.

    No age criterion or worktree probe. Default is a dry run. Apply re-reads
    cursor identity and lane status in the writer transaction, then CAS-expires
    that exact identity. A race is a successful no-op, safe to retry.
    """
    data: dict[str, object] = {
        "task_ref": task_ref,
        "applied": apply,
        "would_expire": [],
        "expired": [],
        "raced": [],
        "failed": [],
    }
    would_expire: list[dict] = []
    expired: list[dict] = []
    raced: list[dict] = []
    failed: list[dict] = []
    data.update(would_expire=would_expire, expired=expired, raced=raced, failed=failed)
    try:
        if not isinstance(task_ref, str) or not task_ref.strip():
            raise ValueError("task_ref is required")
        batch = min(500, max(1, int(max_batch)))
        data["max_batch"] = batch
        connection = (
            _get_db_connection(begin_immediate=True)
            if apply
            else closing(_open_bounded_roster_read_connection(get_runtime_config().db_path, busy_timeout_ms=1000))
        )
        with connection as conn:
            rows = conn.execute(
                """SELECT cursor.* FROM plan_cursors AS cursor
                   WHERE cursor.task_ref = ? AND cursor.state = 'dispatched'
                     AND EXISTS (SELECT 1 FROM worktree_lanes AS lane
                                 WHERE lane.task_ref = cursor.task_ref
                                   AND lane.lane_id = cursor.lane_id
                                   AND lane.status IN ('merged', 'closed'))
                   ORDER BY cursor.id LIMIT ?""",
                (task_ref, batch),
            ).fetchall()
            for row in rows:
                entry = _plan_cursor_entry(row, reason="lane is merged or closed")
                would_expire.append(entry)
                if not apply:
                    continue
                try:
                    current = conn.execute(
                        """SELECT cursor.*, lane.status AS lane_status
                           FROM plan_cursors AS cursor
                           JOIN worktree_lanes AS lane
                             ON lane.task_ref = cursor.task_ref AND lane.lane_id = cursor.lane_id
                           WHERE cursor.id = ?""",
                        (row["id"],),
                    ).fetchone()
                    identity = (
                        "id",
                        "task_ref",
                        "plan_item_id",
                        "lane_id",
                        "state",
                        "updated_at",
                        "dispatched_at",
                        "created_at",
                    )
                    if (
                        current is None
                        or any(current[key] != row[key] for key in identity)
                        or current["state"] != "dispatched"
                        or current["lane_status"] not in {"merged", "closed"}
                    ):
                        raced.append({**entry, "reason": "cursor identity or lane status changed before expiry"})
                        continue
                    reason = f"lane {current['lane_id']} is {current['lane_status']}"
                    entry = {**entry, "reason": reason}
                    changed = _expire_plan_cursor_cas(
                        conn,
                        cursor_id=int(current["id"]),
                        probed_updated_at=current["updated_at"],
                        summary=str(current["summary"] or ""),
                        note=f"expired by terminal plan-cursor disposition: {reason}",
                        terminal_identity=dict(current),
                    )
                    if changed:
                        expired.append({**entry, "state": _PLAN_CURSOR_EXPIRED})
                    else:
                        raced.append({**entry, "reason": "CAS miss: cursor identity or lane status changed"})
                except sqlite3.Error as exc:
                    failed.append({**entry, "reason": "cursor expiry failed", "error": str(exc)[:500]})
    except Exception as exc:
        # An operation/commit failure cannot claim that its rolled-back writes succeeded.
        expired.clear()
        data["error"] = f"{type(exc).__name__}: {exc}"[:500]
        return _envelope(ok=False, tool="expire_terminal_plan_cursors", task_ref=task_ref, data=data)
    return _envelope(ok=not failed, tool="expire_terminal_plan_cursors", task_ref=task_ref, data=data)


def reap_stale_plan_cursors(
    *,
    apply: bool = False,
    stale_after_hours: int = DEFAULT_PLAN_CURSOR_STALE_HOURS,
    max_batch: int = DEFAULT_PLAN_CURSOR_REAP_BATCH,
) -> dict:
    """Sweep ``plan_cursors`` stuck in ``dispatched`` longer than 72h.

    Liveness probe (lane status + worktree existence):
    - conclusive-dead → state ``expired`` with a reaper note (when apply=True)
    - alive → report as active, leave untouched
    - ambiguous / probe unavailable → triage line, never expire

    Dry-run by default (``apply=False``). Never raises.
    """
    try:
        hours = max(1, int(stale_after_hours))
        batch = max(1, int(max_batch))
    except (TypeError, ValueError):
        hours = DEFAULT_PLAN_CURSOR_STALE_HOURS
        batch = DEFAULT_PLAN_CURSOR_REAP_BATCH

    expired: list[dict[str, object]] = []
    would_expire: list[dict[str, object]] = []
    active: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    triage: list[str] = []
    failed: list[dict[str, object]] = []

    try:
        with _get_db_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, task_ref, plan_item_id, state, lane_id, summary,
                       dispatched_at, completed_at, created_at, updated_at
                FROM plan_cursors
                WHERE state = ?
                  AND datetime(COALESCE(dispatched_at, updated_at, created_at))
                      < datetime('now', ?)
                ORDER BY datetime(COALESCE(dispatched_at, updated_at, created_at)) ASC, id ASC
                LIMIT ?
                """,
                (_PLAN_CURSOR_DISPATCHED, f"-{hours} hours", batch),
            ).fetchall()

            for row in rows:
                try:
                    verdict, reason = _probe_plan_cursor_liveness(conn, row)
                except Exception as exc:  # noqa: BLE001 — per-row degrade, never silent-expire
                    verdict, reason = "ambiguous", f"probe raised: {exc}"
                entry = _plan_cursor_entry(row, reason=reason)
                if verdict == "alive":
                    active.append(entry)
                    continue
                if verdict != "dead":
                    ambiguous.append(entry)
                    triage.append(
                        f"plan_cursor id={entry['id']} task={entry['task_ref']} item={entry['plan_item_id']}: {reason}"
                    )
                    continue

                note = f"expired by plan-cursor reaper: {reason}"
                entry = {**entry, "note": note}
                would_expire.append(entry)
                if not apply:
                    continue
                try:
                    ok = _expire_plan_cursor_cas(
                        conn,
                        cursor_id=int(row["id"]),
                        probed_updated_at=row["updated_at"],
                        summary=str(row["summary"] or ""),
                        note=note,
                    )
                except sqlite3.Error as exc:
                    failed.append({**entry, "stage": "expire", "error": str(exc)})
                    continue
                if ok:
                    expired.append({**entry, "state": _PLAN_CURSOR_EXPIRED})
                else:
                    # CAS miss: row moved under us — defer, never overwrite [CON-11].
                    ambiguous.append(
                        {
                            **entry,
                            "reason": "CAS miss: row changed since probe",
                        }
                    )
                    triage.append(
                        f"plan_cursor id={entry['id']} task={entry['task_ref']} "
                        f"item={entry['plan_item_id']}: CAS miss; re-probe next tick"
                    )
    except Exception as exc:  # noqa: BLE001 — never-raise reaper [RES-07]/[AGT-10]
        triage.append(f"plan-cursor sweep failed: {exc}")
        return _envelope(
            ok=True,
            tool="reap_stale_plan_cursors",
            data={
                "applied": apply,
                "stale_after_hours": hours,
                "max_batch": batch,
                "error": str(exc),
                "would_expire": would_expire,
                "expired": expired,
                "active": active,
                "ambiguous": ambiguous,
                "triage": triage,
                "failed": failed,
            },
        )

    return _envelope(
        ok=True,
        tool="reap_stale_plan_cursors",
        data={
            "applied": apply,
            "stale_after_hours": hours,
            "max_batch": batch,
            "would_expire": would_expire,
            "expired": expired,
            "active": active,
            "ambiguous": ambiguous,
            "triage": triage,
            "failed": failed,
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


def _classify_done_nonscratch_handoff_rows(
    conn: sqlite3.Connection,
    *,
    task_ref: str | None = None,
) -> dict[str, list[dict[str, object]]]:
    """Partition done non-scratch rows into closeable/active/ambiguous buckets.

    When ``task_ref`` is set, non-matching refs are skipped before bucketing so
    every reported bucket is scoped (mirrors ``_classify_live_rows``).
    """
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
        if task_ref is not None and ref != task_ref:
            continue
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
        elif not target_branch:
            # Null/empty target_branch is missing merge evidence, not proof of merge.
            # clear_worktree_pointer_for_close may null the pointer when a worktree is
            # missing with no merge check; do not auto-reap (OBS-08 / NAME-03).
            ambiguous.append(
                {
                    **entry,
                    "reason": "done row with no branch pointer; merge status unprovable",
                }
            )
        elif _is_integration_target_branch(target_branch):
            hold = _integration_target_landing_hold(conn, ref) or task_completion_hold_reason(
                task_ref=ref,
                status="done",
                target_branch=target_branch,
                target_worktree_path=row["target_worktree_path"],
                workspace_root=get_runtime_config().git_workspace_root,
            )
            if hold:
                ambiguous.append({**entry, "reason": hold})
            else:
                closeable.append({**entry, "reason": "done integration-target row with no open findings"})
        elif _branch_is_merged(target_branch):
            # Selection uses the archive-time predicate itself (one seam).
            hold = _arch_c_archive_hold(conn, row, "done_reap")
            if hold:
                ambiguous.append({**entry, "reason": hold})
            else:
                closeable.append({**entry, "reason": "done merged branch with no open findings"})
        else:
            ambiguous.append({**entry, "reason": "done row but branch not merged"})
    return {"closeable": closeable, "active": active, "ambiguous": ambiguous}


def _integration_target_landing_hold(conn: sqlite3.Connection, task_ref: str) -> str | None:
    """``landing_unproven:<lane>`` for an integration-target row whose lane tip is unlanded.

    Integration-target rows skip :func:`_final_source_hold`, which proves only
    non-integration targets; the lane portion is the shared landing predicate.
    """
    unlanded = unlanded_lane_tips(conn, task_ref)
    return f"landing_unproven:{unlanded[0]['lane_id']}" if unlanded else None


def reap_done_nonscratch_handoff_rows(
    *,
    apply: bool = False,
    task_ref: str | None = None,
) -> dict:
    """Archive live non-scratch handoff rows already marked done with no open findings.

    When ``task_ref`` is set, classification and reaping are scoped to that row
    (all reported buckets). When ``None`` (default), behaviour is repo-wide so
    unscoped callers keep their sweep.
    """
    with _get_db_connection() as conn:
        selected = {
            str(row["task_ref"]): dict(row)
            for row in conn.execute("SELECT * FROM handoff_state WHERE status='done'").fetchall()
            if task_ref is None or row["task_ref"] == task_ref
        }
        buckets = _classify_done_nonscratch_handoff_rows(conn, task_ref=task_ref)

    reaped: list[str] = []
    failed: list[dict[str, object]] = []
    if apply:
        for entry in buckets["closeable"]:
            ref = str(entry["task_ref"])
            if ref not in selected:
                failed.append({"task_ref": ref, "stage": "guard", "error": "task changed during selection"})
                continue
            archive_result = archive_task_state(
                task_ref=ref,
                cascade_maint_review=False,
                expected_state=selected[ref],
            )
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
    Archives with registrations absent from the live roster, or unreadable
    registration history, remain available to issuer resolution.
    """
    if older_than_days < 1:
        return _envelope(
            ok=False,
            tool="archives_retention_gc",
            data={"error": "older_than_days must be >= 1"},
        )

    would_prune: list[dict[str, object]] = []
    pruned: list[str] = []

    with _get_db_connection(begin_immediate=apply) as conn:
        if not apply:
            conn.execute("BEGIN")
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
            # GC cannot discard the only copy of a registration (or turn an
            # unreadable inventory into an apparently complete one).
            (encoded,) = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref=?", (ref,)).fetchone()
            try:
                snapshot = _registration_json(encoded)
                archived = _archive_registrations(ref, snapshot, require_lanes=False)
            except (TypeError, ValueError):
                continue
            if supersession.archive_has_custody(snapshot.get("task_supersession")):
                continue
            if archived:
                live = dict(conn.execute("SELECT id, lane_id FROM worktree_lanes WHERE task_ref=?", (ref,)))
                if any(live.get(key) != lane["lane_id"] for key, lane in archived.items()):
                    continue
            if apply and "worktree_lanes" not in snapshot:
                # Old task-only snapshots never asserted an empty roster. GC
                # may retire the task while preserving that missing evidence.
                _retain_registration_snapshot(conn, ref, {})
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


_REF_ABSENT = "ABSENT"
_REF_UNREADABLE = "UNREADABLE"
_HEX_OID = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class _RefUnreadable(Exception):
    """A qualified ref cannot be read from ref files; the fence holds [SECD-05]."""


def _read_ref_files(git_dir: Path, common_dir: Path, name: str, depth: int = 0) -> str:
    """Resolve one ref from loose and packed ref files only, with no subprocess.

    It is safe under the writer lock, where the subprocess audit barrier
    forbids Git. Returns an object id, ``ref:<target>=<value>`` for a symbolic
    ref, or ``ABSENT``. Anything else raises :class:`_RefUnreadable`.
    """
    if depth > 5 or (common_dir / "reftable").exists():
        raise _RefUnreadable(name)
    base = git_dir if name == "HEAD" else common_dir
    try:
        text: str | None = (base / name).read_text(encoding="utf-8").strip()
    except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
        text = None
    except (OSError, UnicodeDecodeError) as exc:
        raise _RefUnreadable(name) from exc
    if text is not None:
        if text.startswith("ref:") and text[4:].strip():
            target = text[4:].strip()
            return f"ref:{target}={_read_ref_files(git_dir, common_dir, target, depth + 1)}"
        if _HEX_OID.match(text.lower()):
            return text.lower()
        raise _RefUnreadable(name)
    if name == "HEAD":
        return _REF_ABSENT
    try:
        packed = (common_dir / "packed-refs").read_text(encoding="utf-8")
    except FileNotFoundError:
        return _REF_ABSENT
    except (OSError, UnicodeDecodeError) as exc:
        raise _RefUnreadable(name) from exc
    for line in packed.splitlines():
        oid, _, ref = line.partition(" ")
        if ref.strip() == name and not line.startswith(("#", "^")):
            if not _HEX_OID.match(oid.lower()):
                raise _RefUnreadable(name)
            return oid.lower()
    return _REF_ABSENT


def _read_worktree_head(path: str) -> str:
    """Identity of a registered worktree: its git dir and HEAD, or ``ABSENT``."""
    dot_git = Path(path) / ".git"
    try:
        if dot_git.is_dir():
            git_dir = dot_git
        else:
            text = dot_git.read_text(encoding="utf-8").strip()
            if not text.startswith("gitdir:") or not text[7:].strip():
                raise _RefUnreadable(f"{path}/.git")
            git_dir = (Path(path) / text[7:].strip()).resolve()
        try:
            common = git_dir / (git_dir / "commondir").read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            common = git_dir
    except (FileNotFoundError, NotADirectoryError):
        return _REF_ABSENT
    except (OSError, UnicodeDecodeError) as exc:
        raise _RefUnreadable(f"{path}/.git") from exc
    return f"{git_dir}:{_read_ref_files(git_dir, common.resolve(), 'HEAD')}"


# Lane columns the reap qualifier reads (_final_source_hold, _lane_final_receipt).
_LANE_FENCE_COLUMNS = (
    "id",
    "lane_id",
    "status",
    "branch",
    "worktree_path",
    "branch_tip_sha",
    "branch_tip_source",
    "branch_tip_observed_at",
    "landing_commit_sha",
    "cycle_epoch",
    "updated_at",
)


def _lane_fence_rows(lanes: Iterable[Mapping[str, object]]) -> tuple[tuple[object, ...], ...]:
    """The task's lane rows, reduced to the columns the qualifier reads."""
    rows = [tuple(lane.get(column) for column in _LANE_FENCE_COLUMNS) for lane in lanes]
    return tuple(sorted(rows, key=repr))


def _lane_fence_value(rows: tuple[tuple[object, ...], ...]) -> str:
    digest = hashlib.sha256(repr(rows).encode("utf-8", "backslashreplace")).hexdigest()[:12]
    lanes = ",".join(f"{row[1]}={row[2]}" for row in rows)
    return f"[{lanes}]#{digest}"


def _read_worktree_registrations(common_dir: Path, names: frozenset[str]) -> str:
    """Every worktree whose HEAD is registered on one of ``names``, from admin files only.

    Covers the main worktree (``<common>/HEAD``) and each linked worktree
    (``<common>/worktrees/*/HEAD``), including unborn branches, which no ref
    file reveals. Same match rule as the qualifier's roster read.
    """
    heads = [(".", common_dir / "HEAD")]
    try:
        admin = sorted((common_dir / "worktrees").iterdir())
    except (FileNotFoundError, NotADirectoryError):
        admin = []
    except OSError as exc:
        raise _RefUnreadable(f"{common_dir}/worktrees") from exc
    heads.extend((entry.name, entry / "HEAD") for entry in admin)
    registered: list[str] = []
    for label, head in heads:
        try:
            text = os.fsdecode(head.read_bytes()).strip()
        except (FileNotFoundError, NotADirectoryError):
            continue
        except OSError as exc:
            raise _RefUnreadable(str(head)) from exc
        if text.startswith("ref:") and text[4:].strip().removeprefix("refs/heads/") in names:
            registered.append(f"{label}={text[4:].strip()}")
    return ",".join(registered) or _REF_ABSENT


@dataclasses.dataclass(frozen=True)
class _QualifiedRefIdentities:
    """Identities a completion qualification proved, for a locked compare-and-swap [DATA-05].

    The source refs and roster names come from :func:`_reap_source_names`, the
    same rule the qualifier proves, so the fence watches exactly the refs the
    qualification relied on: every spelling of the target and every lane
    branch of the task. It also fences the worktree registrations on those
    names, the stored worktree's HEAD, the integration ref, and the task's
    lane rows (SQL), which must still all be terminal under the lock.
    """

    git_dir: Path
    common_dir: Path
    task_ref: str
    refs: tuple[str, ...]
    names: frozenset[str]
    worktree: str | None
    lanes: tuple[tuple[object, ...], ...]
    proved: tuple[tuple[str, str], ...]

    @classmethod
    def capture(
        cls, conn: sqlite3.Connection, row: Mapping[str, object], integration_ref: str
    ) -> _QualifiedRefIdentities:
        root = get_runtime_config().git_workspace_root
        dirs = (
            _git(Path(root), "rev-parse", "--path-format=absolute", "--git-dir", "--git-common-dir") if root else None
        )
        lines = dirs.splitlines() if dirs else []
        if len(lines) != 2:
            raise _RefUnreadable("git-common-dir")
        task_ref = str(row.get("task_ref"))
        task_lanes = _task_lanes(conn, task_ref)
        branch = _normalize_optional_text(row.get("target_branch"))
        names = _reap_source_names(task_lanes, branch, integration_ref) if branch else set()
        refs = sorted(_reap_source_refs(names))
        ref = integration_ref.strip()
        if ref.startswith("refs/"):
            refs.append(ref)
        elif ref and not _HEX_OID.match(ref.lower()):
            # Git's own DWIM order for a short name (gitrevisions(7)).
            refs.extend([ref] if re.fullmatch(r"[A-Z_]+", ref) else [])
            refs.extend(f"refs/{p}{ref}" for p in ("", "tags/", "heads/", "remotes/"))
            refs.append(f"refs/remotes/{ref}/HEAD")
        worktree = _normalize_optional_text(row.get("target_worktree_path"))
        fence = cls(
            Path(lines[0]),
            Path(lines[1]),
            task_ref,
            tuple(dict.fromkeys(refs)),
            frozenset(names),
            worktree,
            _lane_fence_rows(task_lanes),
            (),
        )
        return dataclasses.replace(fence, proved=fence.read())

    def read(self) -> tuple[tuple[str, str], ...]:
        values = [(ref, _read_ref_files(self.git_dir, self.common_dir, ref)) for ref in self.refs]
        if self.worktree is not None:
            values.append((f"worktree:{self.worktree}", _read_worktree_head(self.worktree)))
        values.append(("worktree-registrations", _read_worktree_registrations(self.common_dir, self.names)))
        return tuple(values)

    def changed(self, conn: sqlite3.Connection) -> tuple[str, str, str] | None:
        """First proved identity that differs now; an unreadable ref counts as changed.

        Run under the writer lock: ref and admin files only (no Git), then SQL.
        """
        try:
            now = dict(self.read())
        except _RefUnreadable as exc:
            now = {name: _REF_UNREADABLE for name, _ in self.proved}
            _log.warning("qualified ref unreadable at the locked recheck: %s", exc)
        for name, value in self.proved:
            if now.get(name) != value:
                return name, value, now.get(name, _REF_UNREADABLE)
        # SQL only, allowed under RESERVED.
        lanes = _lane_fence_rows(_task_lanes(conn, self.task_ref))
        if lanes != self.lanes:
            return f"lanes:{self.task_ref}", _lane_fence_value(self.lanes), _lane_fence_value(lanes)
        for lane in lanes:
            if lane[2] not in TERMINAL_LANE_STATUSES:
                return f"lane:{lane[1]}", "terminal", str(lane[2])
        return None


def _classify_with_ref_identities(
    conn: sqlite3.Connection, row: Mapping[str, object], integration_ref: str
) -> tuple[str, str, _QualifiedRefIdentities | None]:
    """Final pre-lock qualification, returning the Git identities it proved.

    Identities are read before classification, so any change from here to
    the locked recheck, including one during classification, differs.
    """
    try:
        identities: _QualifiedRefIdentities | None = _QualifiedRefIdentities.capture(conn, row, integration_ref)
        unreadable = None
    except _RefUnreadable as exc:
        identities, unreadable = None, f"source_identity_unreadable: {exc}"
    bucket, reason = _classify_live_row(conn, row, integration_ref)
    if bucket == "closeable" and unreadable is not None:
        return "ambiguous", unreadable, None
    return bucket, reason, identities


def _done_reap_with_ref_identities(
    conn: sqlite3.Connection, row: Mapping[str, object]
) -> tuple[str | None, _QualifiedRefIdentities | None]:
    """Done-row archive qualification plus the identities it proved.

    The fence covers rows whose qualification ran the final-source predicate
    (non-integration targets), with the same capture-before-classify order as
    :func:`_classify_with_ref_identities`, so the locked recheck rolls back on
    any change to the refs, registrations or lane rows the sweep relied on.
    """
    identities: _QualifiedRefIdentities | None = None
    unreadable = None
    branch = _normalize_optional_text(row.get("target_branch"))
    if branch and not _is_integration_target_branch(branch):
        try:
            identities = _QualifiedRefIdentities.capture(conn, row, "main")
        except _RefUnreadable as exc:
            unreadable = f"source_identity_unreadable: {exc}"
    hold = _arch_c_archive_hold(conn, row, "done_reap")
    if hold is None and unreadable is not None:
        return unreadable, None
    return hold, identities


def _source_changed_hold(task_ref: str, moved: tuple[str, str, str]) -> dict:
    ref, qualified, observed = moved
    return _envelope(
        ok=False,
        tool="archive_task_state",
        task_ref=task_ref,
        data={
            "error": (
                f"source_changed_after_qualification: {ref} was {qualified} at qualification, "
                f"now {observed}; retry from selection."
            ),
            "error_code": "completion_held",
            "hold": "source_changed_after_qualification",
            "ref": ref,
            "qualified": qualified,
            "observed": observed,
        },
    )


def _restore_completion_source_status(
    conn: sqlite3.Connection,
    held: dict,
    *,
    task_ref: str,
    completion_source: Mapping[str, object],
    expected_state: Mapping[str, object],
    write: Callable[[sqlite3.Connection], dict],
) -> dict:
    """Undo the caller's done flip when this archive holds; return ``held`` with the outcome.

    ``completion_source`` is the generation reap qualified and ``expected_state``
    the done row it wrote from it. A reap that does not archive leaves the live
    row's status as it was before reap [DBG-10]: in the archive's own writer
    transaction, compare-and-swap the complete row reap wrote and write the
    pre-reap status back. A row another writer changed after the flip keeps that
    writer's state [SECD-05]. SQL only (the actor and binding were resolved
    before the lock). The revision still advances; it never moves back.
    """
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    data = held.setdefault("data", {})
    if not _arch_c_matches(conn, expected_state):
        conn.rollback()
        data.update(status_restored=False, status_restore_error="task changed after status")
        return held
    restored = write(conn)
    if not restored.get("ok"):
        conn.rollback()
        data.update(status_restored=False, status_restore_error=_reap_error(restored))
        return held
    conn.commit()
    data["status_restored"] = True
    from .current_task_rendering import _write_per_task_projection  # noqa: PLC0415

    # Derived files observe the committed authority, outside its writer lock.
    try:
        with _get_db_connection() as projection_conn:
            _write_workspace_current_task_json(projection_conn, task_ref)
        _write_per_task_projection(task_ref)
    except Exception:  # noqa: BLE001 -- the restore committed; projections refresh on the next write
        _log.exception("projection refresh failed after restoring %s from %s", task_ref, completion_source["status"])
    return held


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
    expected_state: Mapping[str, object] | None = None,
    completion_source: Mapping[str, object] | None = None,
    integration_ref: str = "main",
    completion_hold: Callable[[Mapping[str, object]], str | None] | None = None,
    _task_supersession: supersession.ArchiveBinding | None = None,
) -> dict:
    cascade_archived: list[str] = []
    exact_proof = None
    with ExitStack() as custody:
        if _task_supersession is not None:
            _task_supersession.authority.check()
            proof = _task_supersession.proof
            custody.enter_context(
                supersession.native_reservation(
                    proof["selection"]["repository"],
                    task_ref=proof["selection"]["task_ref"],
                    operation="supersession_handoff_archive",
                )
            )
            exact_proof = {**proof, "native_intent": supersession.require_prepared_intent(proof)}
        conn = custody.enter_context(
            _get_db_connection(existing_only=True) if _task_supersession is not None else _get_db_connection()
        )
        if _task_supersession is not None:
            _task_supersession.authority.check(conn)
            # Native cleanup can follow this commit. Keep the archive durable
            # across a power loss even when the ordinary writer uses WAL NORMAL.
            conn.execute("PRAGMA synchronous=FULL")
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        selected_row = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (resolved_task_ref,)).fetchone()
        qualified = dict(selected_row) if selected_row is not None else None
        if expected_state is not None and qualified != dict(expected_state):
            return _arch_c_changed("archive_task_state", resolved_task_ref)
        if completion_source is not None and (
            expected_state is None
            or any(
                completion_source.get(key) != expected_state.get(key)
                for key in ("task_ref", "target_branch", "target_worktree_path")
            )
            or int(expected_state["revision"]) != int(completion_source["revision"]) + 1
        ):
            return _arch_c_changed("archive_task_state", resolved_task_ref)
        archive_actor = build_write_actor(agent=archive_by, branch=archive_branch, commit_sha=archive_commit_sha)
        # Resolve actor BEFORE clear/DML so git work never runs under RESERVED
        # and the archive stays one transaction (DURREV-HARM-01 / [CON-18]).
        # Off-canonical close may have a deleted worktree: allow the missing-
        # worktree fallback so resolve does not raise before the clear below.
        # Do not mid-commit the clear — that made target_branch permanently
        # NULL when a later step rolled back.
        ctx = _resolve_write_actor(
            conn,
            archive_actor,
            task_ref=resolved_task_ref,
            allow_missing_worktree_fallback=True,
        )
        # Precompute worktree path while still deferred/unlocked so the
        # status-flip path can recheck pure-SQL branch enforcement under the
        # write without re-shelling git (DURREV-HARM-04 / [ARCH-13]).
        precomputed_target_worktree_path: str | None = None
        branch_row = conn.execute(
            "SELECT * FROM handoff_state WHERE task_ref = ?",
            (resolved_task_ref,),
        ).fetchone()
        target_branch_for_ctx = (
            _normalize_optional_text(branch_row["target_branch"])
            if branch_row is not None and branch_row["target_branch"]
            else None
        )
        if _task_supersession is not None:
            precomputed_target_worktree_path = _task_supersession.proof["selection"]["checkout"]["path"]
        elif target_branch_for_ctx is not None and _worktree_derivation_enabled():
            try:
                canonical_fn = _resolve_core_override("_canonical_worktree_for_task", _canonical_worktree_for_task)
                precomputed_target_worktree_path = canonical_fn(target_branch_for_ctx)
            except WorktreeNotFoundError:
                precomputed_target_worktree_path = None
        producer_source_row = dict(branch_row) if branch_row is not None else None
        producer_binding = _prepare_task_resource_binding(ctx, producer_source_row)
        # Envelope-level warnings only — do NOT pass them into
        # _set_handoff_state_with_conn (that skips the in-lock recheck).
        warnings = collect_target_context_warnings(
            conn,
            ctx,
            task_ref=resolved_task_ref,
            target_worktree_path=precomputed_target_worktree_path,
        )
        # An archive (and its auto-flip to done) must not strand an unlanded
        # lane tip: observe the landing proof before the writer lock.
        landing_probe = (
            probe_task_landing(conn, resolved_task_ref)
            if _task_supersession is None and qualified is not None
            else None
        )

        def held_after_reap_flip(held: dict) -> dict:
            # Reap flipped expected_state to done from completion_source; this
            # archive will not happen, so put the pre-reap status back.
            if completion_source is None or expected_state is None:
                return held
            source, wrote = completion_source, expected_state
            from .handoff_state import _set_handoff_state_with_conn  # noqa: PLC0415

            return _restore_completion_source_status(
                conn,
                held,
                task_ref=resolved_task_ref,
                completion_source=source,
                expected_state=wrote,
                write=lambda locked: _set_handoff_state_with_conn(
                    locked,
                    task_ref=resolved_task_ref,
                    status=str(source["status"]),
                    expected_revision=int(cast(int, wrote["revision"])),
                    actor=archive_actor,
                    resolved_ctx=ctx,
                    precomputed_target_worktree_path=precomputed_target_worktree_path,
                    producer_binding=_recheck_task_resource_binding(
                        locked, resolved_task_ref, producer_source_row, producer_binding
                    ),
                ),
            )

        # Refresh bounded external evidence before the writer lock. A changed
        # row must not inherit this source qualification, even at the same revision.
        ref_fence: _QualifiedRefIdentities | None = None
        if expected_state is not None and _task_supersession is None:
            if completion_hold is not None:
                # Explicit finalization: the caller's qualifier refreshes the
                # actual target for this exact row; findings use archive triage.
                hold = completion_hold(expected_state)
            elif completion_source is None:
                hold, ref_fence = _done_reap_with_ref_identities(conn, expected_state)
            else:
                # The Done write advanced updated_at. Revalidate the original
                # qualified generation, rather than treating the landing as old
                # relative to our own status transition (deleted-source recovery).
                bucket, reason, ref_fence = _classify_with_ref_identities(conn, completion_source, integration_ref)
                hold = None if bucket == "closeable" else reason
            if hold:
                return held_after_reap_flip(
                    _envelope(
                        ok=False,
                        tool="archive_task_state",
                        task_ref=resolved_task_ref,
                        data={"error": hold, "error_code": "completion_held"},
                    )
                )
        conn.execute("BEGIN IMMEDIATE")
        if qualified is not None and not _arch_c_matches(conn, qualified):
            return _arch_c_changed("archive_task_state", resolved_task_ref)
        # The SQL recheck cannot see Git. Compare-and-swap what the final
        # qualification proved: its source refs, worktree registrations and
        # integration ref from ref/admin files only (no Git under RESERVED),
        # then the task's lane rows in SQL, which must all still be terminal.
        if ref_fence is not None and (moved := ref_fence.changed(conn)) is not None:
            # Nothing is written yet; keep the lock for the restore.
            return held_after_reap_flip(_source_changed_hold(resolved_task_ref, moved))
        if _task_supersession is not None:
            _task_supersession.check(conn, resolved_task_ref)
        elif qualified is None and supersession.archived_custody_snapshot(conn, resolved_task_ref) is not None:
            return _envelope(
                ok=False,
                tool="archive_task_state",
                task_ref=resolved_task_ref,
                data={"error": "supersession_archive_already_has_custody", "error_code": "completion_held"},
            )
        elif expected_state is not None and completion_hold is None and _count_open_findings(conn, resolved_task_ref):
            return held_after_reap_flip(_arch_c_changed("archive_task_state", resolved_task_ref))
        producer_binding = _recheck_task_resource_binding(
            conn, resolved_task_ref, producer_source_row, producer_binding
        )
        pre_clear_pointer = None
        archive_notes = notes or f"Archived {resolved_task_ref}"
        status_flipped = False
        # internal: severity-aware gate — block BEFORE any archive
        # mutation when open high findings remain (blocked-with-remedy).
        if tombstone_findings:
            pre_triage = _triage_open_findings_for_archive(
                conn,
                resolved_task_ref,
                apply=False,
                agent=ctx.agent,
            )
            if pre_triage.get("blocked"):
                return held_after_reap_flip(
                    _envelope(
                        ok=False,
                        tool="archive_task_state",
                        data={
                            "error": pre_triage.get("error") or "open_high_findings_block_archive",
                            "remedy": pre_triage.get("remedy") or _HIGH_ARCHIVE_REMEDY,
                            "high_finding_ids": pre_triage.get("high_finding_ids") or [],
                            "open_high_count": pre_triage.get("open_high_count") or 0,
                            "open_low_medium_count": pre_triage.get("open_low_medium_count") or 0,
                        },
                        task_ref=resolved_task_ref,
                    )
                )
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
        # SQL-only fence on the pre-lock probe: a moved roster is a typed,
        # retryable refusal, never a Git re-probe under RESERVED [CON-21][GRPH-27].
        landing_fenced = fenced_unlanded(conn, resolved_task_ref, landing_probe) if landing_probe is not None else []
        if isinstance(landing_fenced, StaleLandingProbe):
            return held_after_reap_flip(
                _envelope(
                    ok=False,
                    tool="archive_task_state",
                    task_ref=resolved_task_ref,
                    data=landing_probe_stale_refusal("archive_task_state", resolved_task_ref),
                )
            )
        if landing_fenced:
            return held_after_reap_flip(
                _envelope(
                    ok=False,
                    tool="archive_task_state",
                    task_ref=resolved_task_ref,
                    data=unlanded_lanes_refusal("archive_task_state", resolved_task_ref, landing_fenced),
                )
            )
        # All blockers above precede mutation. Reuse the pre-lock missing-target
        # probe; never run Git while holding the writer lock.
        if (
            _task_supersession is None
            and target_branch_for_ctx is not None
            and _worktree_derivation_enabled()
            and precomputed_target_worktree_path is None
        ):
            clear_row = conn.execute(
                "SELECT task_ref,target_branch,target_worktree_path FROM handoff_state WHERE task_ref=?",
                (resolved_task_ref,),
            ).fetchone()
            if clear_row is not None and _task_source_identity(clear_row) == _task_source_identity(producer_source_row):
                pre_clear_pointer = dict(clear_row)
                conn.execute(
                    "UPDATE handoff_state SET target_branch=NULL,target_worktree_path=NULL WHERE task_ref=?",
                    (resolved_task_ref,),
                )
        active_row = conn.execute(
            "SELECT status, revision FROM handoff_state WHERE task_ref = ?",
            (resolved_task_ref,),
        ).fetchone()
        lifecycle = {"knowledge": "unknown", "reason": "active_task_unavailable"}
        if active_row is not None:
            from .handoff_state import _set_handoff_state_with_conn

            flip_result = _set_handoff_state_with_conn(
                conn,
                task_ref=resolved_task_ref,
                status="done",
                expected_revision=int(active_row["revision"]),
                actor=archive_actor,
                resolved_ctx=ctx,
                precomputed_target_worktree_path=precomputed_target_worktree_path,
                producer_binding=producer_binding,
                landing_probe=landing_probe,
            )
            if not flip_result.get("ok"):
                conn.rollback()
                flip_data = flip_result.get("data", {}) or {}
                return _envelope(
                    ok=False,
                    tool="archive_task_state",
                    data={
                        "error": flip_data.get("error") or "Failed to flip task status to done before archive.",
                        "status_flip_error": flip_data.get("error"),
                        **{
                            key: flip_data[key]
                            for key in ("error_kind", "error_code", "retryable", "lanes")
                            if key in flip_data
                        },
                    },
                    task_ref=resolved_task_ref,
                )
            lifecycle = flip_result.get("data", {}).get("lifecycle_resource", lifecycle)
            status_flipped = str(active_row["status"]) != "done"
            # The flip is an operational note, not a user/forensic archive note:
            # surface it in warnings so `notes` stays the caller's value (or the
            # default), which forensic readers and the regression tests expect.
            if status_flipped:
                warnings = [*warnings, "archive auto-flipped non-done status to done before snapshot"]
        warnings.extend(_resource_warnings(lifecycle))
        snapshot = _snapshot_with_test_traces(conn, _collect_task_snapshot(conn, resolved_task_ref))
        if exact_proof is not None:
            snapshot["task_supersession"] = exact_proof
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
        # internal: replace severity-blind supersede with migrate.
        tombstoned_findings = 0
        migrated_findings = 0
        backlog_task_ref: str | None = None
        if tombstone_findings:
            triage = _triage_open_findings_for_archive(
                conn,
                resolved_task_ref,
                apply=True,
                agent=ctx.agent,
            )
            # High was gated pre-mutation. If high appears mid-archive (race),
            # raise so ``_get_db_connection`` rolls back the partial snapshot.
            if triage.get("blocked"):
                raise RuntimeError(
                    "open_high_findings_block_archive: high findings appeared mid-archive; transaction rolled back"
                )
            migrated_findings = int(cast(int, triage.get("migrated_findings") or 0))
            backlog_raw = triage.get("backlog_task_ref")
            backlog_task_ref = str(backlog_raw) if backlog_raw else None
            tombstoned_findings = int(cast(int, triage.get("tombstoned_findings") or 0))
        # Steady-state reclaimer (internal S1): the snapshot persisted
        # above already captured lane_messages, so the archive always retires the
        # task's live brief/inbox rows — their only other deletion path is the
        # opt-in destructive prune below.
        conn.execute("DELETE FROM lane_messages WHERE task_ref = ?", (resolved_task_ref,))
        _reap_task_tool_roster_after_archive(conn, resolved_task_ref)
        active_cleared = False
        if clear_active_if_matches:
            deleted = conn.execute("DELETE FROM handoff_state WHERE task_ref = ?", (resolved_task_ref,))
            active_cleared = deleted.rowcount > 0
        pruned = False
        if prune_working_rows:
            from .coordination_declaration import immutable_fact_condition  # noqa: PLC0415

            # Immutable coordination facts outlive the task: they carry remote
            # obligations that a later declaration/cleanup must still see.
            conn.execute(
                f"DELETE FROM decisions WHERE task_ref = ? AND NOT {immutable_fact_condition()}",
                (resolved_task_ref,),
            )
            # A task-finish annotation can replace the task archive after this
            # commit. Keep the state at removal independently of that snapshot.
            _retain_displaced_lanes(conn, resolved_task_ref, [])
            for table in (
                "blockers",
                "next_actions",
                "test_traces",
                "verified_tests",
                "review_findings",
                "worker_reports",
                "lane_messages",
                "task_tool_roster",
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
    # archive_terminal_review_findings — bounded MOVE of terminal rows
    cull_error = _run_terminal_review_findings_archive()
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
    result = _envelope(
        ok=True,
        tool="archive_task_state",
        data={
            "active_cleared": active_cleared,
            "pruned_working_rows": pruned,
            "allow_destructive_clear": allow_destructive_clear,
            "cascade_archived": cascade_archived,
            "status_flipped_to_done": status_flipped,
            "tombstoned_findings": tombstoned_findings,
            "migrated_findings": migrated_findings,
            "backlog_task_ref": backlog_task_ref,
            "lifecycle_resource": lifecycle,
            **({"task_supersession": exact_proof} if exact_proof is not None else {}),
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
    if cull_error is not None:
        _surface_side_effect_failure(
            result,
            effect="archive_terminal_review_findings",
            error_class=_side_effect_error_class(cull_error),
            detail=(
                "archive_terminal_review_findings failed after archive_task_state: "
                f"{cull_error}; terminal rows remain on the hot table until the next reap"
            ),
        )
    return result


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


def _reactivation_refusal(tool: str, task_ref: str, reason: str) -> dict:
    return _envelope(ok=False, tool=tool, task_ref=task_ref, data={"error": reason, "error_code": reason})


def update_task_status(
    task_ref: str,
    status: str,
    expected_revision: int | None = None,
    actor: WriteActor | None = None,
    expected_state: Mapping[str, object] | None = None,
    integration_ref: str = "main",
    completion_hold: Callable[[Mapping[str, object]], str | None] | None = None,
) -> dict:
    """Update task status for the active task or an archived/inactive task snapshot.

    Library function, not a served MCP tool. Envelopes stamp
    ``tool="set_handoff_state"`` so in-process callers, the public package
    alias, and ``set_handoff_state(..., status_only=True)`` share one
    served identity. The retired name is not a producer tag.
    """
    if status not in HANDOFF_ACTIVE_STATUSES:
        return _envelope(
            ok=False,
            tool="set_handoff_state",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(HANDOFF_ACTIVE_STATUSES))}"},
            task_ref=task_ref,
        )

    if expected_state is not None and expected_state.get("task_ref") != task_ref:
        return _arch_c_changed("set_handoff_state", task_ref)

    active_path_envelope: dict[str, object] | None = None
    active_path_succeeded = False
    # Live status-only updates retain their retries. Done consumes completion
    # authority once: never adopt another revision after a terminal conflict.
    from .core import MAX_OPTIMISTIC_REVISION_ATTEMPTS  # noqa: PLC0415

    working_revision = expected_revision
    revision_retries = 0
    landing_probe_retries = 0
    for attempt in range(MAX_OPTIMISTIC_REVISION_ATTEMPTS):
        with _get_db_connection() as conn:
            # Resolve actor first so git work is outside any write lock
            # ([CON-18], [CON-11], [internal]). Off-canonical status=done
            # may lack a worktree; allow missing fallback. BEGIN IMMEDIATE is
            # acquired below (after worktree precompute, before the clear) so
            # the stale-pointer clear, revision read, and delegated status
            # write share one exclusive transaction — do not mid-commit the
            # clear ([RES-01], REV-S3-05 / REV-S1-01).
            ctx = _resolve_write_actor(
                conn,
                actor,
                task_ref=task_ref,
                allow_missing_worktree_fallback=(status == "done"),
            )
            # Precompute worktree path before any DML / BEGIN IMMEDIATE so the
            # delegated write can recheck pure-SQL branch enforcement without
            # re-shelling git under RESERVED (DURREV-HARM-04 / [ARCH-13]).
            # The same pre-lock porcelain also decides the status=done stale-
            # pointer clear: clear_worktree_pointer_for_close shells git, so
            # calling it after BEGIN IMMEDIATE trips the hold-path barrier
            # (WriteLockHeldAcrossBlockingWorkError). Reorder only — probe
            # outside, pure-SQL clear inside; no mid-function commit
            # (DURREV-HARM-02 / CON-18 / [CON-21] hold-and-wait).
            precomputed_target_worktree_path: str | None = None
            worktree_probe_attempted = False
            branch_row = conn.execute(
                "SELECT * FROM handoff_state WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            target_branch_for_ctx = (
                _normalize_optional_text(branch_row["target_branch"])
                if branch_row is not None and branch_row["target_branch"]
                else None
            )
            if target_branch_for_ctx is not None and _worktree_derivation_enabled():
                worktree_probe_attempted = True
                try:
                    canonical_fn = _resolve_core_override("_canonical_worktree_for_task", _canonical_worktree_for_task)
                    precomputed_target_worktree_path = canonical_fn(target_branch_for_ctx)
                except WorktreeNotFoundError:
                    precomputed_target_worktree_path = None
            producer_source_row = dict(branch_row) if branch_row is not None else None
            producer_binding = _prepare_task_resource_binding(ctx, producer_source_row)
            # Landing proof runs Git: observe it here, fence it under the lock.
            landing_probe = (
                probe_task_landing(conn, task_ref)
                if status == "done" and branch_row is not None and str(branch_row["status"]) != "done"
                else None
            )
            # Exclusive lock dominates every write on this connection for the
            # status=done close path: clear + revision read + delegated write.
            # Hoisting BEGIN IMMEDIATE above the clear removes the need for a
            # mid-commit dodge that previously made the clear durable alone
            # when a later step rolled back (sibling scar: archive_task_state).
            if expected_state is not None:
                if not _arch_c_matches(conn, expected_state):
                    return _arch_c_changed("set_handoff_state", task_ref)
                if completion_hold is not None:
                    reason = completion_hold(expected_state)
                    bucket = "closeable" if reason is None else "held"
                else:
                    bucket, reason = _classify_live_row(conn, expected_state, integration_ref)
                if bucket != "closeable":
                    return _envelope(
                        ok=False,
                        tool="set_handoff_state",
                        task_ref=task_ref,
                        data={"error": reason, "error_code": "completion_held"},
                    )
            conn.execute("BEGIN IMMEDIATE")
            if expected_state is not None and (
                not _arch_c_matches(conn, expected_state)
                or (completion_hold is None and _count_open_findings(conn, task_ref))
            ):
                return _arch_c_changed("set_handoff_state", task_ref)
            producer_binding = _recheck_task_resource_binding(conn, task_ref, producer_source_row, producer_binding)
            if status == "done":
                # Pure-SQL mirror of clear_worktree_pointer_for_close using the
                # pre-lock porcelain result. Re-probe under RESERVED is forbidden;
                # when the pre-lock probe found a live worktree, leave the pointer.
                if expected_state is None and worktree_probe_attempted and precomputed_target_worktree_path is None:
                    clear_row = conn.execute(
                        "SELECT task_ref,target_branch,target_worktree_path FROM handoff_state WHERE task_ref = ?",
                        (task_ref,),
                    ).fetchone()
                    if clear_row is not None and _task_source_identity(clear_row) == _task_source_identity(
                        producer_source_row
                    ):
                        conn.execute(
                            "UPDATE handoff_state SET target_branch = NULL, "
                            "target_worktree_path = NULL WHERE task_ref = ?",
                            (task_ref,),
                        )
            active_row = conn.execute(
                "SELECT * FROM handoff_state WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()

            if active_row is None:
                # Fall through to archived-snapshot path below.
                break

            from .handoff_state import _set_handoff_state_with_conn  # noqa: PLC0415

            inferred_revision = working_revision
            if working_revision is None and status == "done":
                # internal: elide expected_revision for status='done'.
                # status='done' is an end-of-lifecycle transition where forcing
                # callers to pre-fetch the revision was pure cold-start friction.
                # Other statuses are mid-lifecycle and still require explicit
                # stale-write protection (rejected below by set_handoff_state).
                # Exclusive lock already held (BEGIN IMMEDIATE above the clear).
                try:
                    current = conn.execute(
                        "SELECT revision FROM handoff_state WHERE task_ref = ?",
                        (task_ref,),
                    ).fetchone()
                    if current is None:
                        conn.execute("ROLLBACK")
                        return _envelope(
                            ok=False,
                            tool="set_handoff_state",
                            data={
                                "error": "Active row vanished between resolve and update.",
                                "revision_retries": revision_retries,
                            },
                            task_ref=task_ref,
                        )
                    inferred_revision = int(current["revision"])
                except Exception:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise

            # Pass pre-resolved ctx and precomputed worktree path only — do
            # NOT pass a precomputed warnings list; that skips in-lock pure-SQL
            # branch recheck against committed state (DURREV-HARM-04).
            delegated = _set_handoff_state_with_conn(
                conn,
                task_ref=task_ref,
                status=status,
                expected_revision=inferred_revision,
                actor=actor,
                resolved_ctx=ctx,
                precomputed_target_worktree_path=precomputed_target_worktree_path,
                producer_binding=producer_binding,
                landing_probe=landing_probe,
            )
            if not delegated.get("ok"):
                state_data = delegated.get("data", {}) or {}
                if (
                    state_data.get("error_kind") == LANDING_PROBE_STALE
                    and attempt < MAX_OPTIMISTIC_REVISION_ATTEMPTS - 1
                ):
                    # The lane roster moved after the pre-lock landing probe.
                    # Nothing was written: roll back the exclusive close txn and
                    # loop, so the probe re-runs before BEGIN IMMEDIATE (never
                    # Git under RESERVED, [CON-21]). The last attempt returns
                    # the typed retryable refusal below [GRPH-27].
                    conn.execute("ROLLBACK")
                    landing_probe_retries += 1
                    continue
                if state_data.get("error") == "Revision conflict.":
                    latest = state_data.get("current_revision")
                    if (
                        expected_state is not None
                        or status == "done"
                        or attempt >= MAX_OPTIMISTIC_REVISION_ATTEMPTS - 1
                    ):
                        # Non-exception exit commits via CM; roll back the
                        # exclusive close txn so the clear is not durable alone
                        # (REV-UPD-1). Guard: only this path opened BEGIN.
                        if status == "done":
                            conn.execute("ROLLBACK")
                        conflict_data = dict(state_data)
                        conflict_data["revision_retries"] = revision_retries
                        return _envelope(
                            ok=False,
                            tool="set_handoff_state",
                            data=conflict_data,
                            task_ref=task_ref,
                        )
                    if latest is not None:
                        working_revision = int(latest)
                    revision_retries += 1
                    # continue is a normal CM exit → commit; roll back first.
                    if status == "done":
                        conn.execute("ROLLBACK")
                    continue
                # Soft / returned failure (not Revision conflict.): same
                # normal-exit commit hazard as the conflict returns above.
                if status == "done":
                    conn.execute("ROLLBACK")
                fail_data = dict(state_data)
                fail_data["revision_retries"] = revision_retries
                if landing_probe_retries:
                    fail_data["landing_probe_retries"] = landing_probe_retries
                return _envelope(
                    ok=False,
                    tool="set_handoff_state",
                    data=fail_data,
                    task_ref=task_ref,
                )
            active = delegated.get("data", {}).get("active", {}) or {}
            data: dict[str, object] = {
                "status": status,
                "updated_scope": "active",
                "active": active,
                "revision_retries": revision_retries,
                "lifecycle_resource": delegated.get("data", {}).get("lifecycle_resource"),
            }
            if landing_probe_retries:
                data["landing_probe_retries"] = landing_probe_retries
            active_path_envelope = _envelope(
                ok=True,
                tool="set_handoff_state",
                data=data,
                task_ref=task_ref,
                mutation={
                    "entity": "handoff_state",
                    "operation": "update_status",
                    "affected_ids": [task_ref],
                    "task_revision": active.get("revision"),
                },
                warnings=delegated.get("warnings"),
            )
            active_path_succeeded = True
            break

    if active_path_succeeded:
        # internal sub-implementation note.3: refresh the per-task projection so a
        # separate connection sees the post-commit row. Done outside
        # the ``with`` block so the writer's own connection observes
        # the committed data.
        from .current_task_rendering import (  # noqa: PLC0415
            _write_per_task_projection,
            _write_workspace_summary_current_task_json,
        )

        # Derived files observe the committed authority, outside its writer lock.
        assert active_path_envelope is not None
        try:
            with _get_db_connection() as projection_conn:
                _write_workspace_current_task_json(projection_conn, task_ref)
            active_path_envelope["data"]["current_task_md_regen"] = "ok"
            active_path_envelope["artifacts"] = [{"type": "file", "path": "CURRENT_TASK.json"}]
        except Exception as exc:  # noqa: BLE001 -- primary write committed
            active_path_envelope["data"].update(current_task_md_regen="failed", current_task_md_regen_error=str(exc))
        _write_per_task_projection(task_ref)
        # internal: terminal-status transitions flush the
        # workspace summary unconditionally. Gated on LIVE_ACTIVE_STATUSES
        # complement (per shared_primitives.LIVE_ACTIVE_STATUSES) so any
        # future expansion of the terminal vocabulary is honored without
        # touching this branch. Mirrors decisions.py:758 (close_check)
        # and the archive flush above. Live-to-live transitions stay
        # routine-gated on current_task_auto_regen via the existing
        # _write_workspace_current_task_json call in the active path.
        if status not in LIVE_ACTIVE_STATUSES:
            _write_workspace_summary_current_task_json(unconditional=True)
        assert active_path_envelope is not None
        return active_path_envelope

    with ExitStack() as custody, _get_db_connection() as conn:
        ctx = _resolve_write_actor(conn, actor, task_ref=task_ref)
        warnings = collect_target_context_warnings(conn, ctx, task_ref=task_ref)
        archive_row = conn.execute(
            "SELECT snapshot_json,notes FROM task_archives WHERE task_ref=?",
            (task_ref,),
        ).fetchone()
        if archive_row is None:
            return _envelope(
                ok=False,
                tool="set_handoff_state",
                task_ref=task_ref,
                data={
                    "error": "Task is neither active nor archived; switch to it or archive it before updating its inactive status.",
                },
            )
        archive_json = archive_row["snapshot_json"]
        try:
            snapshot = json.loads(archive_json)
            if not isinstance(snapshot, dict):
                raise ValueError("snapshot must be an object")
        except (TypeError, ValueError):
            return _envelope(
                ok=False,
                tool="set_handoff_state",
                task_ref=task_ref,
                data={"error": "Archived snapshot is invalid JSON."},
            )
        active_block = snapshot.get("active")
        if not isinstance(active_block, dict):
            active_block = {"task_ref": task_ref}
            snapshot["active"] = active_block
        if active_block.get("task_ref", task_ref) != task_ref:
            return _envelope(
                ok=False,
                tool="set_handoff_state",
                task_ref=task_ref,
                data={"error": "Archived task identity mismatch.", "error_code": "task_identity_mismatch"},
            )
        binding = _prepare_task_resource_binding(ctx, active_block)
        archive_notes = _append_resolution_note(archive_row["notes"], f"Updated archived status to {status}")
        active_block.update(
            task_ref=task_ref,
            status=status,
            updated_by=ctx.agent,
            updated_branch=ctx.branch,
            updated_commit_sha=ctx.commit_sha,
        )
        proof = snapshot.get("task_supersession")
        try:
            reserved = custody.enter_context(supersession.reactivation_reservation(proof, task_ref=task_ref))
        except supersession.SupersessionRefusal as exc:
            return _reactivation_refusal("set_handoff_state", task_ref, str(exc))
        if reserved:
            conn.execute("PRAGMA synchronous=FULL")
        if status != "done" and supersession.archive_has_custody(proof):
            proof["custody_phase"] = supersession.REACTIVATED
        # Archive status is intentionally revisionless in the public API. Fence
        # the exact observed snapshot and active-row absence under the write lock.
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref=?", (task_ref,)).fetchone()
        if (
            current is None
            or current["snapshot_json"] != archive_json
            or conn.execute("SELECT 1 FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
        ):
            return _envelope(
                ok=False,
                tool="set_handoff_state",
                task_ref=task_ref,
                data={"error": "Archived task changed during preparation; retry.", "error_code": "task_changed"},
            )

        def mutate_archive(connection):
            _persist_task_archive_snapshot(
                connection, task_ref=task_ref, snapshot=snapshot, ctx=ctx, notes=archive_notes
            )

        lifecycle = stage_change(
            conn,
            binding=binding,
            task_ref=task_ref,
            ctx=ctx,
            snapshot=active_block,
            event_id=None,
            state_change=mutate_archive,
        )
    warnings.extend(_resource_warnings(lifecycle))
    # Regenerate only after commit, reading the authority from a fresh connection.
    regen_result = "skipped"
    with _get_db_connection() as conn:
        try:
            active_task_row = _resolve_workspace_handoff_row(conn)
        except ValueError:
            active_task_row = None
        if active_task_row is not None:
            try:
                _write_workspace_current_task_json(conn, str(active_task_row["task_ref"]))
                regen_result = "ok"
            except Exception as exc:  # noqa: BLE001 -- primary transaction committed
                regen_result = str(exc)
    data_archived: dict[str, object] = {
        "status": status,
        "updated_scope": "archived",
        "current_task_md_regen": regen_result,
        "lifecycle_resource": lifecycle,
    }
    if regen_result not in {"ok", "skipped"}:
        data_archived.update(current_task_md_regen="failed", current_task_md_regen_error=regen_result)
    return _envelope(
        ok=True,
        tool="set_handoff_state",
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
    status: str | None = None,
    actor: WriteActor | None = None,
    target_branch: str | None = None,
) -> dict:
    """Select/create/restore a named task, preserving its canonical identity.

    Omitted fields retain stored task progress, including archived plan/focus
    and status. New tasks default to in_progress. The actor remains provenance;
    it never supplies an implicit replacement target. No sibling is evicted.
    """
    if status is not None and status not in HANDOFF_ACTIVE_STATUSES:
        return _envelope(
            ok=False,
            tool="switch_task",
            data={"error": f"Invalid status. Valid: {', '.join(sorted(HANDOFF_ACTIVE_STATUSES))}"},
            task_ref=task_ref,
        )
    with _get_db_connection() as conn:
        ctx = _resolve_write_actor(conn, actor, task_ref=task_ref)
        existing = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
        existing = dict(existing) if existing is not None else None
        archive_row = conn.execute("SELECT snapshot_json FROM task_archives WHERE task_ref=?", (task_ref,)).fetchone()
        archive_json = archive_row["snapshot_json"] if archive_row is not None else None
        stored = existing or {}
        proof = None
        if existing is None and archive_json is not None:
            try:
                snapshot = json.loads(archive_json)
                stored = snapshot.get("active") or {}
                proof = snapshot.get("task_supersession")
            except (ValueError, TypeError):
                stored = {}
            if not isinstance(stored, dict) or stored.get("task_ref", task_ref) != task_ref:
                return _envelope(
                    ok=False,
                    tool="switch_task",
                    task_ref=task_ref,
                    data={"error": "Archived task identity mismatch.", "error_code": "task_identity_mismatch"},
                )
        resolved_objective = objective if objective is not None else stored.get("objective")
        if resolved_objective is None:
            return _envelope(
                ok=False,
                tool="switch_task",
                task_ref=task_ref,
                data={
                    "error": "Cannot determine objective for the target task. Pass --objective explicitly or archive the current task first."
                },
            )
        branch = target_branch if target_branch is not None else stored.get("target_branch")
        worktree = stored.get("target_worktree_path")
        # An explicit branch change must not keep the old branch's checkout.
        # A legacy unset pointer can be recovered only from this repository's
        # registered worktree, never from actor cwd.
        if branch and (not worktree or branch != stored.get("target_branch")):
            try:
                worktree = _canonical_worktree_for_task(branch)
            except WorktreeNotFoundError:
                worktree = None
        intended = {
            "task_ref": task_ref,
            "objective": resolved_objective,
            "focus": focus if focus is not None else stored.get("focus"),
            "status": status if status is not None else stored.get("status") or "in_progress",
            "target_branch": branch,
            "target_worktree_path": worktree,
            "task_plan_path": stored.get("task_plan_path"),
            "revision": int(existing["revision"]) + 1 if existing else 0,
            "updated_by": ctx.agent,
            "updated_branch": ctx.branch,
            "updated_commit_sha": ctx.commit_sha,
        }
        binding = _prepare_task_resource_binding(ctx, intended)
        if binding.reason == "task_target_mismatch":
            return _envelope(
                ok=False,
                tool="switch_task",
                task_ref=task_ref,
                data={
                    "error": "Task target does not belong to the canonical branch/workspace.",
                    "error_code": "task_target_mismatch",
                },
            )
        warning_task = task_ref if existing is not None or archive_json is not None else None
        try:
            warnings = collect_target_context_warnings(
                conn,
                ctx,
                task_ref=warning_task,
                target_worktree_path=worktree,
                enforce_branch=False,
            )
        except ValueError:
            if warning_task is not None:
                raise
            # A new task cannot borrow an unrelated ambiguous workspace row.
            warnings = []
    with ExitStack() as custody:
        try:
            reserved = custody.enter_context(supersession.reactivation_reservation(proof, task_ref=task_ref))
        except supersession.SupersessionRefusal as exc:
            return _reactivation_refusal("switch_task", task_ref, str(exc))
        conn = custody.enter_context(_get_db_connection())
        if reserved:
            conn.execute("PRAGMA synchronous=FULL")
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
        current_archive = conn.execute(
            "SELECT snapshot_json FROM task_archives WHERE task_ref=?", (task_ref,)
        ).fetchone()
        if (dict(current) if current is not None else None) != existing or (
            existing is None
            and (current_archive["snapshot_json"] if current_archive is not None else None) != archive_json
        ):
            return _envelope(
                ok=False,
                tool="switch_task",
                task_ref=task_ref,
                data={"error": "Task changed during switch preparation; retry.", "error_code": "task_changed"},
            )

        def mutate(connection):
            if existing is not None:
                changed = connection.execute(
                    "UPDATE handoff_state SET objective=?,focus=?,status=?,target_branch=?,target_worktree_path=?,"
                    "task_plan_path=?,revision=revision+1,updated_at=datetime('now'),updated_by=?,updated_branch=?,"
                    "updated_commit_sha=? WHERE task_ref=? AND revision=?",
                    (
                        intended["objective"],
                        intended["focus"],
                        intended["status"],
                        branch,
                        worktree,
                        intended["task_plan_path"],
                        ctx.agent,
                        ctx.branch,
                        ctx.commit_sha,
                        task_ref,
                        existing["revision"],
                    ),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("task switch lost its revision fence")
            else:
                connection.execute(
                    "INSERT INTO handoff_state (id,task_ref,objective,focus,status,target_branch,target_worktree_path,"
                    "task_plan_path,revision,updated_at,updated_by,updated_branch,updated_commit_sha) "
                    "VALUES (NULL,?,?,?,?,?,?,?,0,datetime('now'),?,?,?)",
                    (
                        task_ref,
                        intended["objective"],
                        intended["focus"],
                        intended["status"],
                        branch,
                        worktree,
                        intended["task_plan_path"],
                        ctx.agent,
                        ctx.branch,
                        ctx.commit_sha,
                    ),
                )

        lifecycle = stage_change(
            conn,
            binding=binding,
            task_ref=task_ref,
            ctx=ctx,
            snapshot=intended,
            event_id=None,
            state_change=mutate,
        )
        active = _row_to_dict(conn.execute("SELECT * FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone())
    warnings.extend(_resource_warnings(lifecycle))
    regen_error = None
    try:
        from .current_task_rendering import _write_per_task_projection, _write_workspace_summary_current_task_json

        with _get_db_connection() as conn:
            _write_workspace_current_task_json(conn, task_ref)
        _write_per_task_projection(task_ref)
        _write_workspace_summary_current_task_json(unconditional=True)
    except Exception as exc:  # noqa: BLE001 -- primary transaction already committed
        regen_error = str(exc)
    data = {
        "active": active,
        "lifecycle_resource": lifecycle,
        "current_task_md_regen": "failed" if regen_error else "ok",
    }
    if existing is not None:
        data["already_active"] = True
    else:
        data.update(switched=True, archived_previous=False, previous_task_ref=None)
    if regen_error is not None:
        data["current_task_md_regen_error"] = regen_error
    return _envelope(
        ok=True,
        tool="switch_task",
        data=data,
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


_COMPLETION_PORT_TOOL = "complete_task_conditionally"


def _completion_port_envelope(task_ref: str, outcome: str, error_code: str, error: str, **extra: object) -> dict:
    return _envelope(
        ok=False,
        tool=_COMPLETION_PORT_TOOL,
        task_ref=task_ref,
        data={"outcome": outcome, "error_code": error_code, "error": error, **extra},
    )


def _completion_port_outcome(task_ref: str, result: dict, *, lookup_current: bool = True) -> dict:
    """Re-stamp a checked writer's refusal with the port's outcome vocabulary."""
    data = dict(result.get("data") or {})
    code = data.get("error_code")
    if code == "task_changed" or data.get("error") == "Revision conflict.":
        outcome = "conflict"
        data["error_code"] = "task_changed"
        data.setdefault("error", "Task changed after completion qualification; retry from selection.")
        if lookup_current:
            with _get_db_connection() as conn:
                row = conn.execute("SELECT revision,status FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
            data["current_revision"] = int(row["revision"]) if row is not None else None
            data["current_status"] = str(row["status"]) if row is not None else None
    else:
        outcome = "held"
        data["error_code"] = "completion_held"
    data["outcome"] = outcome
    return {**result, "ok": False, "tool": _COMPLETION_PORT_TOOL, "data": data}


def _complete_exact_supersession(
    *,
    operation: str,
    expected: dict,
    db_path: str | Path | None,
    db_identity: Mapping[str, object] | None,
    generation: str | None,
    clear_active_if_matches: bool,
    notes: str | None,
    actor: WriteActor | None,
) -> dict:
    task_ref = expected["task_ref"]
    if not (
        operation in {"select", "archive", "resume"}
        and expected["status"] in {"in_progress", "done"}
        and all(
            isinstance(expected[key], str) and re.fullmatch(r"[0-9a-f]{40}", expected[key])
            for key in ("receipt", "tip")
        )
        and expected["target_branch"]
        and expected["target_worktree_path"]
        and expected["integration_ref"]
        and expected["revision"] >= 0
        and clear_active_if_matches
        and isinstance(db_path, (str, Path))
        and Path(db_path).is_absolute()
        and isinstance(db_identity, Mapping)
        and db_identity.keys() == {"st_dev", "st_ino"}
        and all(type(value) is int and value >= 0 for value in db_identity.values())
        and (generation is None if operation == "select" else supersession.valid_generation(generation))
    ):
        return _completion_port_envelope(
            task_ref,
            "invalid",
            "invalid_request",
            "Exact supersession requires select, atomic archive or resume, a linked target, receipt, tip and selected Handoff DB identity. Archive and resume require the issued task generation.",
        )

    authority = supersession.HandoffAuthority(Path(db_path), cast(dict[str, int], dict(db_identity)))
    branch = expected["target_branch"].removeprefix("refs/heads/")
    path = expected["target_worktree_path"]
    expected_fields = ("task_ref", "revision", "status", "target_branch", "target_worktree_path")
    try:
        authority.check()
        repository = supersession.repository_identity()
        with closing(connect_handoff_db(authority.path, read_only=True)) as conn:
            authority.check(conn)
            conn.execute("BEGIN")
            row = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
            if operation == "archive" and row is not None:
                store_id = supersession.generation_store(conn)
                nonce = supersession.live_nonce(conn, task_ref) if store_id is not None else None
            else:
                store_id, nonce = None, None
        if operation == "resume" or (operation == "archive" and row is None):
            with _get_db_connection(begin_immediate=True, existing_only=True) as conn:
                proof = supersession.resume_proof(
                    conn,
                    expected=expected,
                    authority=authority,
                    repository=repository,
                    generation=cast(str, generation),
                )
            return _envelope(
                ok=True,
                tool=_COMPLETION_PORT_TOOL,
                task_ref=task_ref,
                data={
                    "outcome": "already_archived",
                    "operation": operation,
                    "task_supersession": proof,
                    "active_cleared": True,
                    "archived_revision": expected["revision"],
                },
            )
        if row is None:
            return _completion_port_envelope(task_ref, "not_found", "task_not_live", "No live task to select.")
        observed = dict(row)
        if any(observed[key] != expected[key] for key in expected_fields):
            return _completion_port_outcome(
                task_ref, _arch_c_changed(_COMPLETION_PORT_TOOL, task_ref), lookup_current=False
            )
        if operation == "archive" and nonce is None:
            raise supersession.SupersessionRefusal("supersession_task_generation_changed")

        checkout = supersession.registered_checkout(path, branch=branch, tip=expected["tip"])
        if checkout["git_common_dir"] != repository["git_common_dir"]:
            raise supersession.SupersessionRefusal("supersession_repository_mismatch")
        hold = task_completion_hold_reason(
            task_ref=task_ref,
            status=expected["status"],
            target_branch=branch,
            target_worktree_path=path,
            workspace_root=repository["root"],
            integration_ref=expected["integration_ref"],
            explicit_completion=True,
            supersession_receipt=expected["receipt"],
            supersession_tip=expected["tip"],
        )
        if hold:
            raise supersession.SupersessionRefusal(hold)
        selection = {
            **expected,
            "repository": repository,
            "checkout": checkout,
            "owner": {
                "kind": "handoff_task",
                "task_ref": task_ref,
            },
        }
        if operation == "select":
            with _get_db_connection(existing_only=True) as conn:
                authority.check(conn)
                # Native prepare may persist this generation as soon as it is issued.
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                if not _arch_c_matches(conn, observed):
                    return _completion_port_outcome(
                        task_ref,
                        _arch_c_changed(_COMPLETION_PORT_TOOL, task_ref),
                        lookup_current=False,
                    )
                supersession.check_owners(conn, selection, repository)
                supersession.check_checkout_identity(selection)
                store_id = supersession.generation_store(conn, create=True)
                nonce = supersession.live_nonce(conn, task_ref, issue=True)

        selection.update(handoff=supersession.handoff_identity(authority, cast(int, store_id)), task_nonce=nonce)
        issued_generation = supersession.task_generation(selection)
        proof = {
            "schema_version": supersession.SCHEMA_VERSION,
            "custody_phase": "selected" if operation == "select" else supersession.ARCHIVED,
            "task_generation": issued_generation,
            "selection": selection,
        }
        if operation == "select":
            return _envelope(
                ok=True,
                tool=_COMPLETION_PORT_TOOL,
                task_ref=task_ref,
                data={
                    "outcome": "selected",
                    "operation": operation,
                    "task_supersession": proof,
                },
            )
        if generation != issued_generation:
            raise supersession.SupersessionRefusal("supersession_task_generation_changed")
        result = archive_task_state(
            task_ref=task_ref,
            notes=notes,
            expected_state=observed,
            archive_by=(actor or {}).get("agent"),
            archive_branch=(actor or {}).get("branch"),
            archive_commit_sha=(actor or {}).get("commit_sha"),
            _task_supersession=supersession.ArchiveBinding(authority, proof),
        )
        if not result.get("ok"):
            return _completion_port_outcome(task_ref, result, lookup_current=False)
        data = dict(result.get("data") or {})
        data.update(outcome="archived", operation=operation, archived_revision=expected["revision"])
        return {**result, "tool": _COMPLETION_PORT_TOOL, "data": data}
    except supersession.SupersessionRefusal as exc:
        return _completion_port_envelope(task_ref, "held", "completion_held", str(exc))
    except (
        ImportError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        sqlite3.Error,
        subprocess.TimeoutExpired,
    ) as exc:
        _log.warning("Exact supersession qualification failed for %s", task_ref, exc_info=True)
        return _completion_port_envelope(
            task_ref,
            "held",
            "completion_held",
            f"supersession_authority_unavailable: {type(exc).__name__}",
        )


def complete_task_conditionally(
    *,
    task_ref: str,
    operation: str,
    expected_revision: int,
    expected_status: str,
    expected_target_branch: str | None,
    expected_target_worktree_path: str | None,
    integration_ref: str = "main",
    notes: str | None = None,
    actor: WriteActor | None = None,
    clear_active_if_matches: bool = True,
    supersession_receipt: str | None = None,
    supersession_tip: str | None = None,
    expected_handoff_db_path: str | Path | None = None,
    expected_handoff_db_identity: Mapping[str, object] | None = None,
    expected_task_generation: str | None = None,
) -> dict:
    """Apply explicit completion only to the exact generation the caller selected.

    Ordinary ``operation="done"`` writes status ``done`` at exactly ``expected_revision``.
    ``operation="archive"`` archives an exactly selected ``done`` row. Each call
    refreshes the bounded explicit-completion target proof for the stored row
    before the writer lock, then fences the complete row under BEGIN IMMEDIATE
    (SQL only). There is no revision elision, retry, or adoption of a newer
    generation; a done write never archives and an archive never changes status.
    ``clear_active_if_matches`` controls only archive's live-row removal;
    False keeps the selected Done row after persisting its archive snapshot.
    The external filesystem can still change after the probe: the proof is
    bound to the row generation, not a filesystem snapshot.
    Exact supersession uses select, atomic archive, and idempotent resume.
    Select issues a generation bound to the current row, authority and checkout.
    Archive consumes a durable native prepare with the Done transition. Resume
    validates the same Handoff archive and immutable native journal binding.

    Canon v0.26.2: DATA-14 (Designing Data-Intensive Applications, ch. 11),
    RES-19 (Operating Systems: Three Easy Pieces, ch. 42), GRPH-27 (Models of
    Computation, §DFA), and CARD-02 / Principle 12 (correction at the source).
    """
    if expected_status not in HANDOFF_ACTIVE_STATUSES:
        return _completion_port_envelope(task_ref, "invalid", "invalid_request", "expected_status is not a task status")
    if isinstance(expected_revision, bool) or not isinstance(expected_revision, int):
        return _completion_port_envelope(task_ref, "invalid", "invalid_request", "expected_revision must be an integer")
    if operation in {"select", "resume"} or any(
        value is not None
        for value in (
            supersession_receipt,
            supersession_tip,
            expected_handoff_db_path,
            expected_handoff_db_identity,
            expected_task_generation,
        )
    ):
        return _complete_exact_supersession(
            operation=operation,
            expected={
                "task_ref": task_ref,
                "revision": expected_revision,
                "status": expected_status,
                "target_branch": _normalize_optional_text(expected_target_branch),
                "target_worktree_path": _normalize_optional_text(expected_target_worktree_path),
                "receipt": supersession_receipt,
                "tip": supersession_tip,
                "integration_ref": integration_ref.removeprefix("refs/heads/"),
            },
            db_path=expected_handoff_db_path,
            db_identity=expected_handoff_db_identity,
            generation=expected_task_generation,
            clear_active_if_matches=clear_active_if_matches,
            notes=notes,
            actor=actor,
        )
    if operation not in {"done", "archive"}:
        return _completion_port_envelope(
            task_ref, "invalid", "invalid_request", "operation must be 'done' or 'archive'"
        )
    if operation == "archive" and expected_status != "done":
        return _completion_port_envelope(
            task_ref, "invalid", "invalid_request", "archive requires a selected done row (expected_status='done')"
        )
    selected = (
        task_ref,
        expected_revision,
        expected_status,
        _normalize_optional_text(expected_target_branch),
        _normalize_optional_text(expected_target_worktree_path),
    )
    with _get_db_connection() as conn:
        row = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    if row is None:
        return _completion_port_envelope(
            task_ref, "not_found", "task_not_live", "No live task row; archived snapshots are not completed here."
        )
    observed = dict(row)
    current = (
        str(observed["task_ref"]),
        int(observed["revision"]),
        str(observed["status"]),
        _normalize_optional_text(observed["target_branch"]),
        _normalize_optional_text(observed["target_worktree_path"]),
    )
    if current != selected:
        return _completion_port_envelope(
            task_ref,
            "conflict",
            "task_changed",
            "Task changed after completion qualification; retry from selection.",
            current_revision=current[1],
            current_status=current[2],
        )
    from .config import _resolve_primary_worktree_root  # noqa: PLC0415

    configured_root = Path(get_runtime_config().git_workspace_root)
    qualification_root = _resolve_primary_worktree_root(configured_root) or configured_root

    def explicit_hold(fenced: Mapping[str, object]) -> str | None:
        # Runs before BEGIN IMMEDIATE on the exact row the writer then fences.
        return task_completion_hold_reason(
            task_ref=str(fenced["task_ref"]),
            status=str(fenced["status"]),
            target_branch=_normalize_optional_text(fenced["target_branch"]),
            target_worktree_path=_normalize_optional_text(fenced["target_worktree_path"]),
            workspace_root=qualification_root,
            integration_ref=integration_ref,
            explicit_completion=True,
        )

    if operation == "done":
        result = update_task_status(
            task_ref=task_ref,
            status="done",
            expected_revision=expected_revision,
            actor=actor,
            expected_state=observed,
            integration_ref=integration_ref,
            completion_hold=explicit_hold,
        )
        if not result.get("ok"):
            return _completion_port_outcome(task_ref, result)
        data = dict(result.get("data") or {})
        active = data.get("active") or {}
        data.update(
            outcome="done",
            operation="done",
            previous_revision=expected_revision,
            revision=active.get("revision"),
            status="done",
            target_branch=active.get("target_branch"),
            target_worktree_path=active.get("target_worktree_path"),
        )
        return {**result, "tool": _COMPLETION_PORT_TOOL, "data": data}

    result = archive_task_state(
        task_ref=task_ref,
        notes=notes,
        clear_active_if_matches=clear_active_if_matches,
        expected_state=observed,
        integration_ref=integration_ref,
        completion_hold=explicit_hold,
    )
    if not result.get("ok"):
        return _completion_port_outcome(task_ref, result)
    data = dict(result.get("data") or {})
    data.update(outcome="archived", operation="archive", archived_revision=expected_revision)
    return {**result, "tool": _COMPLETION_PORT_TOOL, "data": data}


def _arch_c_archive_hold(conn: sqlite3.Connection, row: Mapping[str, object], kind: str) -> str | None:
    """Refresh source evidence before BEGIN; the subsequent SQL fence binds it."""
    if row["status"] != "done":
        return "selected task is no longer done"
    branch = _normalize_optional_text(row["target_branch"])
    if kind == "done_reap":
        if _count_open_findings(conn, str(row["task_ref"])):
            return "done row with open findings"
        if not branch or not (_is_integration_target_branch(branch) or _branch_is_merged(branch)):
            return "done row without current integration evidence"
        if not _is_integration_target_branch(branch):
            # The same final-source predicate reap uses (every lane terminal,
            # sibling and remote-tracking refs in integration, roster custody),
            # so a row reap held is not archived by the done-row sweep [GRPH-14].
            hold = _final_source_hold(conn, row, "main", deleted_source_proof=False)
            if hold is not None:
                return hold
        elif hold := _integration_target_landing_hold(conn, str(row["task_ref"])):
            return hold
    return task_completion_hold_reason(
        task_ref=str(row["task_ref"]),
        status=str(row["status"]),
        target_branch=branch,
        target_worktree_path=_normalize_optional_text(row["target_worktree_path"]),
        task_updated_at=_normalize_optional_text(row["updated_at"]),
        workspace_root=get_runtime_config().git_workspace_root,
    )


def _arch_c_changed(tool: str, task_ref: str) -> dict:
    return _envelope(
        ok=False,
        tool=tool,
        task_ref=task_ref,
        data={
            "error": "Task changed after completion qualification; retry from selection.",
            "error_code": "task_changed",
        },
    )


def _arch_c_matches(conn: sqlite3.Connection, expected: Mapping[str, object] | None) -> bool:
    """Compare the complete observed generation using SQL only, under the writer lock."""
    if expected is None:
        return False
    current = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (expected["task_ref"],)).fetchone()
    return current is not None and dict(current) == dict(expected)
