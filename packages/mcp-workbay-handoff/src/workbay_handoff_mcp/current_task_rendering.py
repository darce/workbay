"""Generated handoff surface rendering cluster for workbay_handoff_mcp.

Extracted from _shared.py (implementation note of internal). Contains:
  - snapshot collection (_collect_task_snapshot)
  - state assembly (_build_current_task_state_from_snapshot)
  - write path (_write_current_task_md_for_task, _write_current_task_md_from_state)
  - related-findings helpers (_fetch_related_open_findings_impl, _fetch_related_open_findings)
  - rendering sub-helpers (_format_token_suffix, _render_lanes_section,
    _render_findings_section, _render_coverage_section, _render_token_summary_section)
    - human dashboard render (_render_current_task_md)
    - machine-readable current-task render (_render_current_task_json)

All symbols are re-exported from _shared.py for backward compatibility.

Imports from _shared are done at function level (late imports) to avoid a circular
module dependency: _shared.py re-exports from this module at its end, so module-level
imports in this file would create a deadlock when this module is loaded first.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import NotRequired, TypedDict, cast

from .enums import ActionStatus, BlockerStatus, FindingStatus, HandoffStatus, MessageStatus
from .runtime import get_runtime_config
from .shared_primitives import (
    LIVE_ACTIVE_STATUSES,
    _decode_lane_message_row_dict,
    _decode_turn_metric_row_dict,
    _row_to_dict,
)
from .shared_schema import _get_db_connection

# ---------------------------------------------------------------------------
# Typed containers
# ---------------------------------------------------------------------------


class TaskSnapshot(TypedDict):
    """Raw data collected from the DB for a single task reference."""

    task_ref: str
    active: dict | None
    blockers: list[dict]
    next_actions: list[dict]
    decisions: list[dict]
    verified_tests: list[dict]
    review_findings: list[dict]
    worktree_lanes: list[dict]
    worker_reports: list[dict]
    lane_messages: list[dict]
    plan_cursors: list[dict]
    turn_metrics: list[dict]
    repo_instances: list[dict]
    terminal_guard_events: list[dict]


class ReviewCoverageSummary(TypedDict, total=False):
    """Coverage summary returned by get_review_coverage and stored in render state."""

    ok: bool
    run_count: int
    latest_verdict: str | None
    latest_review_run_id: str | None
    open_findings_by_severity: dict[str, int]
    reopened_findings_count: int


class DashboardTaskRow(TypedDict):
    """Cross-task dashboard row rendered at the top of CURRENT_TASK.json."""

    task_ref: str
    status: str
    last_activity: str | None
    open_blockers: int
    pending_actions: int
    open_findings: int
    archived_at: str | None


class CurrentTaskRenderState(TypedDict):
    """Filtered, render-ready view of a task's handoff state."""

    task_ref: str | None
    active: dict | None
    blockers_open: list[dict]
    actions_pending: list[dict]
    decisions_recent: list[dict]
    tests_recent: list[dict]
    findings_open: list[dict]
    findings_deferred: list[dict]
    findings_resolved: list[dict]
    worktree_lanes: list[dict]
    worker_reports_recent: list[dict]
    lane_messages_open: list[dict]
    dashboard_tasks: NotRequired[list[DashboardTaskRow]]
    review_coverage: NotRequired[ReviewCoverageSummary | None]
    related_findings_open: NotRequired[dict[str, list[dict]]]
    related_findings_deferred: NotRequired[dict[str, list[dict]]]
    cold_start_compaction: NotRequired[str | None]
    compaction_advisory: NotRequired[dict | None]


def _normalize_current_task_json_for_compare(serialized: str) -> str:
    """Normalize the machine-readable CURRENT_TASK.json payload for sync checks."""

    try:
        payload = json.loads(serialized)
    except json.JSONDecodeError:
        return serialized.strip()
    return json.dumps(payload, indent=2, sort_keys=True)


def _infer_epic_ref(task_ref: str | None) -> str | None:
    """Infer an epic short id from task refs like ``internal``.

    Keep this intentionally conservative; only emit an epic when the task ref
    clearly follows the `<EpicShortID>-<N>` task-plan convention.
    """

    normalized = (task_ref or "").strip()
    if not normalized:
        return None
    prefix, separator, remainder = normalized.partition("-")
    if not separator or not remainder:
        return None
    if not prefix.startswith("E") or not prefix[1:].isdigit():
        return None
    if not remainder[0].isdigit():
        return None
    return prefix


# ---------------------------------------------------------------------------
# Snapshot collection
# ---------------------------------------------------------------------------


def _collect_dashboard_rows(
    conn: sqlite3.Connection, limit: int = 20, include_archived: bool = True
) -> list[DashboardTaskRow]:
    """Collect compact cross-task dashboard rows for CURRENT_TASK.json and dashboard view."""

    limit = max(1, limit)
    archive_union = (
        "UNION ALL SELECT task_ref, archived_at AS updated_at FROM task_archives" if include_archived else ""
    )
    archived_filter = "" if include_archived else "WHERE archived.archived_at IS NULL"
    rows = conn.execute(
        """
        WITH activity AS (
            -- Include handoff_state.updated_at for the active task so it always has a
            -- recent-activity anchor even when it has no decisions, actions, or other
            -- ledger entries. Intentional behavioral difference from the prior
            -- _get_handoff_dashboard_view, which excluded this source.
            SELECT task_ref, updated_at FROM handoff_state
            UNION ALL
            SELECT task_ref, created_at AS updated_at FROM decisions
            UNION ALL
            SELECT task_ref, created_at AS updated_at FROM blockers
            UNION ALL
            SELECT task_ref, updated_at FROM next_actions
            UNION ALL
            SELECT task_ref, verified_at AS updated_at FROM verified_tests
            UNION ALL
            SELECT task_ref, COALESCE(updated_at, resolved_at, created_at) AS updated_at FROM review_findings
            UNION ALL
            SELECT task_ref, updated_at FROM worktree_lanes
            UNION ALL
            SELECT task_ref, created_at AS updated_at FROM worker_reports
            UNION ALL
            SELECT task_ref, updated_at FROM lane_messages
        """
        + archive_union
        + """
        ),
        candidates AS (
            SELECT task_ref, MAX(updated_at) AS last_activity
            FROM activity
            GROUP BY task_ref
            ORDER BY MAX(updated_at) DESC
            LIMIT ?
        ),
        blocker_counts AS (
            SELECT task_ref, COUNT(*) AS open_blockers
            FROM blockers
            WHERE status = 'open'
            GROUP BY task_ref
        ),
        action_counts AS (
            SELECT task_ref, COUNT(*) AS pending_actions
            FROM next_actions
            WHERE status = 'pending'
            GROUP BY task_ref
        ),
        finding_counts AS (
            SELECT task_ref, COUNT(*) AS open_findings
            FROM review_findings
            WHERE status = 'open'
            GROUP BY task_ref
        ),
        archived AS (
            SELECT task_ref, archived_at, snapshot_json
            FROM task_archives
        ),
        active_state AS (
            SELECT task_ref, status
            FROM handoff_state
        )
        SELECT
            candidates.task_ref,
            candidates.last_activity,
            COALESCE(blocker_counts.open_blockers, 0) AS open_blockers,
            COALESCE(action_counts.pending_actions, 0) AS pending_actions,
            COALESCE(finding_counts.open_findings, 0) AS open_findings,
            archived.archived_at,
            archived.snapshot_json,
            active_state.status AS active_status
        FROM candidates
        LEFT JOIN blocker_counts ON blocker_counts.task_ref = candidates.task_ref
        LEFT JOIN action_counts ON action_counts.task_ref = candidates.task_ref
        LEFT JOIN finding_counts ON finding_counts.task_ref = candidates.task_ref
        LEFT JOIN archived ON archived.task_ref = candidates.task_ref
        LEFT JOIN active_state ON active_state.task_ref = candidates.task_ref
        """
        + archived_filter
        + """
        ORDER BY candidates.last_activity DESC
        """,
        (limit,),
    ).fetchall()

    dashboard_rows: list[DashboardTaskRow] = []
    for row in rows:
        task_ref = str(row["task_ref"])
        archived_at = row["archived_at"]
        status = row["active_status"] or ("archived" if archived_at else "active")
        snapshot_json = row["snapshot_json"]
        # If a task was archived in the past and later reactivated, the live
        # handoff_state row is the operator-facing source of truth. Only fall
        # back to archived snapshot status when there is no live active status.
        if row["active_status"] is None and archived_at and snapshot_json:
            try:
                archived_snapshot = json.loads(snapshot_json)
                archived_active = archived_snapshot.get("active") or {}
                status = archived_active.get("status") or status
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        dashboard_rows.append(
            {
                "task_ref": task_ref,
                "status": str(status),
                "last_activity": row["last_activity"],
                "open_blockers": int(row["open_blockers"] or 0),
                "pending_actions": int(row["pending_actions"] or 0),
                "open_findings": int(row["open_findings"] or 0),
                "archived_at": archived_at,
            }
        )
    return dashboard_rows


def _sqlite_supports_window_functions() -> bool:
    parts = sqlite3.sqlite_version.split(".")
    try:
        major, minor = int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return False
    return (major, minor) >= (3, 25)


def _qualify_order_by(order_by: str, alias: str) -> str:
    return (
        order_by.replace("severity", f"{alias}.severity")
        .replace("created_at", f"{alias}.created_at")
        .replace("updated_at", f"{alias}.updated_at")
        .replace("status", f"{alias}.status")
    )


def _fetch_findings_grouped(
    conn: sqlite3.Connection,
    *,
    statuses: tuple[str, ...],
    active_task_ref: str | None,
    max_per_task: int,
    order_by: str | None = None,
) -> dict[str, list[dict]]:
    if len(statuses) == 1:
        status_where = "status = ?"
        status_params: tuple[object, ...] = (statuses[0],)
    else:
        placeholders = ",".join(["?"] * len(statuses))
        status_where = f"status IN ({placeholders})"
        status_params = statuses

    if active_task_ref:
        where = f"{status_where} AND task_ref != ?"
        base_params: tuple[object, ...] = (*status_params, active_task_ref)
        if len(statuses) == 1:
            outer_where = "outer_rf.status = ? AND outer_rf.task_ref != ?"
        else:
            placeholders = ",".join(["?"] * len(statuses))
            outer_where = f"outer_rf.status IN ({placeholders}) AND outer_rf.task_ref != ?"
    else:
        where = status_where
        base_params = status_params
        if len(statuses) == 1:
            outer_where = "outer_rf.status = ?"
        else:
            placeholders = ",".join(["?"] * len(statuses))
            outer_where = f"outer_rf.status IN ({placeholders})"

    order_by = order_by or (
        "CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, created_at DESC"
    )

    if _sqlite_supports_window_functions():
        sql = f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY task_ref ORDER BY {order_by}
                ) AS _rn
                FROM review_findings
                WHERE {where}
            )
            WHERE _rn <= ?
            ORDER BY task_ref, {order_by}
        """
        rows = conn.execute(sql, (*base_params, max_per_task)).fetchall()
    else:
        inner_alias = "rf_inner"
        if len(statuses) == 1:
            inner_status = f"{inner_alias}.status = ?"
        else:
            placeholders = ",".join(["?"] * len(statuses))
            inner_status = f"{inner_alias}.status IN ({placeholders})"
        inner_order = _qualify_order_by(order_by, inner_alias)
        outer_order = _qualify_order_by(order_by, "outer_rf")
        sql = f"""
            SELECT outer_rf.* FROM review_findings AS outer_rf
            WHERE {outer_where}
              AND outer_rf.id IN (
                SELECT {inner_alias}.id FROM review_findings AS {inner_alias}
                WHERE {inner_alias}.task_ref = outer_rf.task_ref
                  AND {inner_status}
                ORDER BY {inner_order}
                LIMIT ?
              )
            ORDER BY outer_rf.task_ref, {outer_order}
        """
        rows = conn.execute(sql, (*base_params, *status_params, max_per_task)).fetchall()

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        d = dict(row)
        d.pop("_rn", None)
        ref = d["task_ref"]
        grouped.setdefault(ref, []).append(d)
    return grouped


def _collect_all_open_findings(
    conn: sqlite3.Connection,
    active_task_ref: str | None = None,
    max_per_task: int = 5,
    include_stale: bool = False,
) -> dict[str, list[dict]]:
    """Collect open review findings across all tasks, grouped by task_ref.

    Excludes active_task_ref whose findings are already in findings_open.
    By default excludes refs without a live non-done handoff row.
    Returns at most *max_per_task* findings per task_ref group.
    """
    grouped = _fetch_findings_grouped(
        conn,
        statuses=(FindingStatus.OPEN.value,),
        active_task_ref=active_task_ref,
        max_per_task=max_per_task,
    )
    if include_stale:
        return grouped
    live_refs = {
        str(row["task_ref"])
        for row in conn.execute(
            "SELECT task_ref FROM handoff_state WHERE status <> ?",
            (HandoffStatus.DONE.value,),
        ).fetchall()
    }
    return {task_ref: rows for task_ref, rows in grouped.items() if task_ref in live_refs}


def _collect_all_deferred_findings(
    conn: sqlite3.Connection,
    active_task_ref: str | None = None,
    max_per_task: int = 5,
) -> dict[str, list[dict]]:
    """Collect deferred/wontfix review findings across all tasks, grouped by task_ref.

    Excludes active_task_ref whose findings are already in findings_deferred.
    Returns at most *max_per_task* findings per task_ref group.
    """
    return _fetch_findings_grouped(
        conn,
        statuses=(FindingStatus.DEFERRED.value, FindingStatus.WONTFIX.value),
        active_task_ref=active_task_ref,
        max_per_task=max_per_task,
    )


def _collect_all_resolved_findings(
    conn: sqlite3.Connection,
    max_per_task: int = 100,
) -> dict[str, list[dict]]:
    """Collect resolved review findings (``fixed`` / ``resolved_on_branch`` /
    ``integrated``) across all tasks, grouped by ``task_ref``.

    internal: the dashboard uses this collector to project the
    two-state finding lifecycle into a ``RESOLVED FINDINGS`` section so
    reviewers can see, at a glance, how many findings per task are fixed
    on-branch (pending integration) versus already integrated.
    """
    resolved_order = (
        "CASE status WHEN 'integrated' THEN 0 WHEN 'resolved_on_branch' THEN 1 ELSE 2 END, "
        "COALESCE(updated_at, created_at) DESC"
    )
    return _fetch_findings_grouped(
        conn,
        statuses=(
            FindingStatus.FIXED.value,
            FindingStatus.RESOLVED_ON_BRANCH.value,
            FindingStatus.INTEGRATED.value,
        ),
        active_task_ref=None,
        max_per_task=max_per_task,
        order_by=resolved_order,
    )


def _collect_task_snapshot(conn: sqlite3.Connection, task_ref: str) -> TaskSnapshot:
    active_row = conn.execute("SELECT * FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    active = _row_to_dict(active_row) if active_row is not None else None

    def _rows(query: str) -> list[dict]:
        rows = [dict(row) for row in conn.execute(query, (task_ref,)).fetchall()]
        if "lane_messages" in query:
            return [_decode_lane_message_row_dict(row) for row in rows]
        if "turn_metrics" in query:
            return [_decode_turn_metric_row_dict(row) for row in rows]
        return rows

    terminal_guard_events = _rows(
        "SELECT * FROM terminal_guard_events WHERE task_ref = ? ORDER BY created_at DESC, event_key DESC"
    )
    repo_instances = [
        dict(row)
        for row in conn.execute(
            """
            SELECT repo_instances.*
            FROM repo_instances
            JOIN (
                SELECT DISTINCT repo_instance_id
                FROM terminal_guard_events
                WHERE task_ref = ?
            ) AS telemetry USING (repo_instance_id)
            ORDER BY repo_instances.created_at DESC, repo_instances.repo_instance_id DESC
            """,
            (task_ref,),
        ).fetchall()
    ]

    return {
        "task_ref": task_ref,
        "active": active,
        "blockers": _rows("SELECT * FROM blockers WHERE task_ref = ? ORDER BY created_at DESC"),
        "next_actions": _rows("SELECT * FROM next_actions WHERE task_ref = ? ORDER BY priority ASC, created_at ASC"),
        "decisions": _rows("SELECT * FROM decisions WHERE task_ref = ? ORDER BY created_at DESC"),
        "verified_tests": _rows("SELECT * FROM verified_tests WHERE task_ref = ? ORDER BY verified_at DESC"),
        "review_findings": _rows(
            "SELECT * FROM review_findings WHERE task_ref = ? ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, COALESCE(updated_at, created_at) DESC"
        ),
        "worktree_lanes": _rows("SELECT * FROM worktree_lanes WHERE task_ref = ? ORDER BY updated_at DESC, id DESC"),
        "worker_reports": _rows("SELECT * FROM worker_reports WHERE task_ref = ? ORDER BY created_at DESC, id DESC"),
        "lane_messages": _rows("SELECT * FROM lane_messages WHERE task_ref = ? ORDER BY updated_at DESC, id DESC"),
        "plan_cursors": _rows("SELECT * FROM plan_cursors WHERE task_ref = ? ORDER BY updated_at DESC, id DESC"),
        "turn_metrics": _rows("SELECT * FROM turn_metrics WHERE task_ref = ? ORDER BY created_at DESC, id DESC"),
        "repo_instances": repo_instances,
        "terminal_guard_events": terminal_guard_events,
    }


# ---------------------------------------------------------------------------
# State assembly
# ---------------------------------------------------------------------------


def _build_current_task_state_from_snapshot(snapshot: TaskSnapshot) -> CurrentTaskRenderState:
    return {
        "task_ref": snapshot.get("task_ref"),
        "active": snapshot["active"],
        "blockers_open": [row for row in snapshot["blockers"] if row.get("status") == BlockerStatus.OPEN],
        "actions_pending": [row for row in snapshot["next_actions"] if row.get("status") == ActionStatus.PENDING],
        "decisions_recent": snapshot["decisions"],
        "tests_recent": snapshot["verified_tests"],
        "findings_open": [row for row in snapshot["review_findings"] if row.get("status") == FindingStatus.OPEN],
        "findings_deferred": [
            row
            for row in snapshot["review_findings"]
            if row.get("status") in (FindingStatus.DEFERRED, FindingStatus.WONTFIX)
        ],
        "findings_resolved": [
            row
            for row in snapshot["review_findings"]
            if row.get("status") in (FindingStatus.FIXED, FindingStatus.RESOLVED_ON_BRANCH, FindingStatus.INTEGRATED)
        ],
        "worktree_lanes": snapshot.get("worktree_lanes", []),
        "worker_reports_recent": snapshot.get("worker_reports", []),
        "lane_messages_open": [
            row for row in snapshot.get("lane_messages", []) if row.get("status") == MessageStatus.OPEN
        ],
    }


def _build_current_task_render_state(
    conn: sqlite3.Connection,
    task_ref: str,
) -> CurrentTaskRenderState:
    """Assemble the CURRENT_TASK render state from the canonical task snapshot path.

    Contains only active-task data.  Cross-task sections (All Tasks table,
    open/deferred findings from other tasks) are rendered by DASHBOARD.txt via
    dashboard_rendering.generate_dashboard_md().
    """
    snapshot = _collect_task_snapshot(conn, task_ref)
    state = _build_current_task_state_from_snapshot(snapshot)
    try:
        from .review_findings import (
            _collect_review_coverage,  # noqa: PLC0415 – late import to break circular
        )

        state["review_coverage"] = cast(ReviewCoverageSummary, _collect_review_coverage(conn, task_ref=task_ref))
    except Exception:
        pass
    try:
        from .compaction import render_cold_start_compaction  # noqa: PLC0415 – late import to break circular

        cold_start_block = render_cold_start_compaction(task_ref)
        if cold_start_block is not None:
            state["cold_start_compaction"] = cold_start_block
    except Exception:
        # Cold-start render is additive; never block CURRENT_TASK on its failure.
        pass
    try:
        from .compaction import compute_compaction_advisory  # noqa: PLC0415 – late import to break circular
        from .runtime import get_runtime_config  # noqa: PLC0415

        workspace_root = get_runtime_config().compaction_config_root
        advisory = compute_compaction_advisory(workspace_root=workspace_root, task_ref=task_ref)
        state["compaction_advisory"] = advisory
    except Exception:
        # Advisory is additive; never block CURRENT_TASK on its failure.
        pass
    return state


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


def _write_current_task_md_for_task(
    conn: sqlite3.Connection,
    task_ref: str,
    *,
    unconditional: bool = False,
) -> bool:
    """Write the workspace-summary CURRENT_TASK.json (internal).

    The live file is the v2 workspace summary derived from per-task projection
    files; the ``conn`` and ``task_ref`` arguments are advisory and preserved
    for caller compatibility.

    By default this is a routine auto-write — it no-ops when the runtime has
    ``current_task_auto_regen=False``, since DASHBOARD.txt is the always-current
    operator surface. Pass ``unconditional=True`` for explicit export paths
    (e.g. ``render_handoff(kind='current_task')`` or import/export round-trips)
    that must always render regardless of the flag.
    """
    return _write_workspace_summary_current_task_json(unconditional=unconditional)


def _write_current_task_md_from_state(task_ref: str) -> bool:
    """Write the workspace-summary CURRENT_TASK.json from the internal write path.

    Routine auto-write; respects ``current_task_auto_regen``. ``task_ref`` is
    advisory — the v2 workspace summary is derived from per-task projection
    files, not from a single task's state.
    """
    return _write_workspace_summary_current_task_json()


# ---------------------------------------------------------------------------
# Related-findings helpers
# ---------------------------------------------------------------------------


def _fetch_related_open_findings_impl(conn: sqlite3.Connection, task_refs: list[str]) -> dict[str, list[dict]]:
    """Query open review findings for multiple task_refs, grouped by task_ref.

    Takes a connection so callers can use their own connection context.
    """
    if not task_refs:
        return {}
    placeholders = ",".join("?" for _ in task_refs)
    rows = conn.execute(
        f"SELECT * FROM review_findings WHERE task_ref IN ({placeholders}) AND status = 'open' "
        "ORDER BY task_ref, CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, created_at DESC",
        tuple(task_refs),
    ).fetchall()
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        d = dict(row)
        grouped.setdefault(d["task_ref"], []).append(d)
    return grouped


def _fetch_related_open_findings(task_refs: list[str]) -> dict[str, list[dict]]:
    """Query open review findings for multiple task_refs, grouped by task_ref."""
    if not task_refs:
        return {}
    with _get_db_connection() as conn:
        return _fetch_related_open_findings_impl(conn, task_refs)


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _format_token_suffix(item: dict) -> str:
    total = item.get("total_tokens")
    if total is None:
        return ""
    if total >= 1000:
        return f" [{total / 1000:.1f}K tok]"
    return f" [{total} tok]"


def _render_lanes_section(state: CurrentTaskRenderState) -> list[str]:
    lines: list[str] = ["", "## Worktree Lanes"]
    lanes = state.get("worktree_lanes", [])
    if lanes:
        for lane in lanes:
            lines.append(
                f"- `{lane.get('lane_id')}` [{lane.get('status')}] {lane.get('branch')} @ {lane.get('worktree_path')}"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Lane Dispatches"])
    lane_message_rows = state["lane_messages_open"]
    lane_messages = [message for message in lane_message_rows if message.get("direction") == "orchestrator_to_worker"]
    if lane_messages:
        for message in lane_messages:
            lane_id = message.get("lane_id", "?")
            subject = message.get("subject", "")
            body = message.get("message", "")
            lines.append(f"- `{lane_id}` [{message.get('id')}] {subject} -- {body}")
    else:
        lines.append("- None")
    return lines


def _render_findings_section(state: CurrentTaskRenderState) -> list[str]:
    def _finding_line(finding: dict, show_status: bool = False) -> str:
        location = (
            f"{finding.get('file_path')}:{finding.get('line_start')}"
            if finding.get("line_start")
            else finding.get("file_path")
        )
        status_prefix = f"[{finding.get('status', '').upper()}] " if show_status else ""
        return f"- {status_prefix}[{finding.get('severity', '').upper()}] {finding.get('finding_id')}: {location} -- {finding.get('description')}"

    def _resolved_line(finding: dict) -> str:
        base = _finding_line(finding)
        status = finding.get("status")
        if status == FindingStatus.INTEGRATED and finding.get("integrated_at_commit"):
            sha7 = str(finding["integrated_at_commit"])[:7]
            ref = finding.get("integrated_at_ref") or "main"
            return f"{base} :: integrated to {ref}@{sha7}"
        if status == FindingStatus.RESOLVED_ON_BRANCH and finding.get("resolved_on_branch_at_commit"):
            sha7 = str(finding["resolved_on_branch_at_commit"])[:7]
            branch = finding.get("resolved_on_branch_ref") or finding.get("branch") or "unknown"
            return f"{base} :: fixed on {branch}@{sha7}, pending integration to main"
        # Legacy ``fixed`` rows fall through to the pre-internal UX text using
        # the actor branch + commit columns.
        legacy_branch = finding.get("branch") or "unknown"
        legacy_sha = finding.get("commit_sha")
        if legacy_sha:
            return f"{base} :: fixed on {legacy_branch}@{str(legacy_sha)[:7]} (legacy)"
        return f"{base} :: fixed (legacy)"

    # --- Open findings (active task only) ---
    lines: list[str] = ["", "## Open Review Findings"]
    active_findings = state.get("findings_open", [])

    if not active_findings:
        lines.append("- None")
    else:
        lines.extend(_finding_line(f) for f in active_findings)

    # --- Resolved findings (internal two-state lifecycle + legacy fixed). ---
    resolved = state.get("findings_resolved", [])
    if resolved:
        lines.extend(["", "## Resolved Findings"])
        lines.extend(_resolved_line(f) for f in resolved)

    # --- Deferred / wontfix findings (active task only) ---
    active_deferred = state.get("findings_deferred", [])
    if active_deferred:
        lines.extend(["", "## Deferred / Won't Fix Findings"])
        lines.extend(_finding_line(f, show_status=True) for f in active_deferred)

    return lines


def _render_coverage_section(state: CurrentTaskRenderState) -> list[str]:
    """Render a `## Review Coverage` section when coverage data is in state."""
    coverage = state.get("review_coverage")
    if not coverage or not coverage.get("ok"):
        return []
    lines: list[str] = ["", "## Review Coverage"]
    lines.append(f"- review runs: {coverage.get('run_count', 0)}")
    latest_verdict = coverage.get("latest_verdict")
    latest_run_id = coverage.get("latest_review_run_id")
    if latest_verdict or latest_run_id:
        verdict_str = latest_verdict or "no verdict"
        run_str = f" (run: {latest_run_id})" if latest_run_id else ""
        lines.append(f"- latest verdict: {verdict_str}{run_str}")
    else:
        lines.append("- latest verdict: none")
    sev = coverage.get("open_findings_by_severity", {})
    lines.append(f"- open findings: high={sev.get('high', 0)} medium={sev.get('medium', 0)} low={sev.get('low', 0)}")
    lines.append(f"- reopened findings: {coverage.get('reopened_findings_count', 0)}")
    return lines


def _format_dashboard_last_activity(last_activity: str | None) -> str:
    if not last_activity:
        return "-"
    try:
        timestamp = datetime.fromisoformat(last_activity.replace(" ", "T"))
    except ValueError:
        return last_activity
    # Unconditional absolute format: a today-relative rendering (%H:%M when the
    # activity is "today") flips at midnight with no underlying data change,
    # which breaks prefix-stability of the dashboard body. Always render the
    # full timestamp so the cell depends only on the ledger, not the clock.
    return timestamp.strftime("%Y-%m-%d %H:%M")


def _format_dashboard_task_ref(task_ref: str, width: int) -> str:
    if len(task_ref) <= width:
        return task_ref
    if width <= 3:
        return task_ref[:width]
    return f"{task_ref[: width - 3]}..."


def _render_dashboard_section(tasks: list[DashboardTaskRow], active_task_ref: str | None) -> list[str]:
    # Fixed-width ASCII table — avoids layout shifts on refresh.
    col_task = 44
    col_status = 13
    col_find = 4
    col_block = 5
    col_act = 3
    col_last = 16

    header = (
        f"  {'Task':<{col_task}}  {'Status':<{col_status}}  {'Find':>{col_find}}"
        f"  {'Block':>{col_block}}  {'Act':>{col_act}}  {'Last':<{col_last}}"
    )
    sep = "\u2500"
    separator = (
        f"  {sep * col_task}  {sep * col_status}  {sep * col_find}"
        f"  {sep * col_block}  {sep * col_act}  {sep * col_last}"
    )

    lines: list[str] = ["", "ALL TASKS", "-" * 9, "", header, separator]
    if not tasks:
        lines.append(
            f"  {'(no tasks)':<{col_task}}  {'-':<{col_status}}  {'0':>{col_find}}  {'0':>{col_block}}  {'0':>{col_act}}  {'-':<{col_last}}"
        )
        return lines
    for task in tasks:
        task_ref = task.get("task_ref", "")
        is_active = active_task_ref and task_ref == active_task_ref
        marker = "> " if is_active else "  "
        task_cell = _format_dashboard_task_ref(task_ref, col_task)
        status = task.get("status") or ("archived" if task.get("archived_at") else "active")
        last = _format_dashboard_last_activity(task.get("last_activity"))
        lines.append(
            f"{marker}{task_cell:<{col_task}}  {status:<{col_status}}  {task.get('open_findings', 0):>{col_find}}"
            f"  {task.get('open_blockers', 0):>{col_block}}  {task.get('pending_actions', 0):>{col_act}}  {last:<{col_last}}"
        )
    return lines


def _render_token_summary_section(decisions: list[dict]) -> list[str]:
    token_decisions = [d for d in decisions if d.get("total_tokens") is not None]
    if not token_decisions:
        return []
    total_tok = sum(d.get("total_tokens", 0) for d in token_decisions)
    total_in = sum(d.get("input_tokens", 0) for d in token_decisions if d.get("input_tokens") is not None)
    total_out = sum(d.get("output_tokens", 0) for d in token_decisions if d.get("output_tokens") is not None)
    by_agent: dict[str, int] = {}
    for d in token_decisions:
        agent_key = d.get("agent") or "unknown"
        by_agent[agent_key] = by_agent.get(agent_key, 0) + (d.get("total_tokens") or 0)

    def _fmt_tok(n: int) -> str:
        return f"{n / 1000:.1f}K" if n >= 1000 else str(n)

    lines: list[str] = ["", "## Token Summary"]
    lines.append(
        f"- Decisions with tokens: {len(token_decisions)} ; Total: {_fmt_tok(total_tok)} (in: {_fmt_tok(total_in)}, out: {_fmt_tok(total_out)})"
    )
    agent_parts = " ; ".join(f"{a}: {_fmt_tok(t)}" for a, t in sorted(by_agent.items(), key=lambda x: -x[1]))
    lines.append(f"- By agent: {agent_parts}")
    return lines


def _render_current_task_md(state: CurrentTaskRenderState) -> str:
    active = state.get("active")
    _generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    decisions = state.get("decisions_recent", [])
    latest_decision = decisions[0] if decisions else None

    def _task_identity_lines(task_ref: str | None) -> list[str]:
        identity_lines: list[str] = []
        epic_ref = _infer_epic_ref(task_ref)
        if epic_ref:
            identity_lines.append(f"- epic_ref: `{epic_ref}`")
        if task_ref:
            identity_lines.append(f"- task_ref: `{task_ref}`")
        return identity_lines

    def _decision_line(item: dict) -> str:
        parts = f"- [#{item.get('id')}] {item.get('decision')}"
        if item.get("agent"):
            parts += f" ({item.get('agent')})"
        parts += _format_token_suffix(item)
        return parts

    def _truncate_command(cmd: str, max_len: int = 120) -> str:
        if not cmd:
            return ""
        single_line = cmd.replace("\n", " \u21a9 ").strip()
        if len(single_line) > max_len:
            return single_line[:max_len] + "\u2026"
        return single_line

    # Wall-clock stamp lives on a single trailing line, not in the header, so
    # everything above it is prefix-stable across renders of an unchanged
    # ledger (only this last line varies by clock).
    header_lines = [
        "# DASHBOARD",
        "",
        "_DO NOT EDIT: generated from .task-state/handoff.db._",
        "",
    ]
    trailing_stamp = f"_Last generated: {_generated_at}_"

    if not active:
        has_data = any(
            state.get(key) for key in ("decisions_recent", "findings_open", "blockers_open", "actions_pending")
        )
        if not has_data:
            return "\n".join(header_lines + ["No active handoff state found.", "", trailing_stamp])
        task_ref_display = state.get("task_ref", "unknown")
        lines: list[str] = header_lines + [
            "## Task Context",
            *_task_identity_lines(task_ref_display),
            "",
            "> **Note**: No active `handoff_state` row for this task. Context assembled from available decisions, findings, blockers, and actions.",
            "",
            "## Latest Decision",
        ]
    else:
        lines = header_lines + [
            "## Objective",
            f"{active.get('objective', '')}",
            "",
        ]
        focus_val = active.get("focus")
        if focus_val:
            lines.extend(["## Current Focus", f"{focus_val}", ""])
        lines.extend(
            [
                "## Active Status",
                *_task_identity_lines(str(active.get("task_ref", ""))),
                f"- status: `{active.get('status', '')}`",
                f"- revision: `{active.get('revision', 0)}`",
                f"- updated_at: `{active.get('updated_at', '')}`",
                *([f"- target_branch: `{active['target_branch']}`"] if active.get("target_branch") else []),
                "",
                "## Latest Decision",
            ]
        )
    if latest_decision:
        lines.append(_decision_line(latest_decision))
    else:
        lines.append("- None")
    lines.extend(["", "## Open Blockers"])
    for section, empty_text, formatter in [
        ("blockers_open", "- None", lambda item: f"- [#{item.get('id')}] {item.get('description')}"),
        (
            "actions_pending",
            "- None",
            lambda item: f"- (P{item.get('priority')}) [#{item.get('id')}] {item.get('action')}",
        ),
        ("decisions_recent", "- None", _decision_line),
        (
            "tests_recent",
            "- None",
            lambda item: (
                f"- [#{item.get('id')}] `{_truncate_command(item.get('command', ''))}` -> `{'pass' if item.get('passed') else 'fail'}`"
            ),
        ),
    ]:
        raw_items = state.get(section, [])
        items = raw_items if isinstance(raw_items, list) else []
        if section == "actions_pending":
            lines.extend(["", "## Pending Next Actions"])
        elif section == "decisions_recent":
            lines.extend(["", "## Recent Decisions"])
        elif section == "tests_recent":
            lines.extend(["", "## Latest Verified Tests"])
        if items:
            lines.extend(formatter(item) for item in items)
        else:
            lines.append(empty_text)
    lines.extend(_render_lanes_section(state))
    lines.extend(_render_coverage_section(state))
    lines.extend(_render_findings_section(state))
    lines.extend(_render_token_summary_section(decisions))
    lines.append("")
    lines.append(trailing_stamp)
    # Trailing empty element => output ends with a single final newline.
    lines.append("")
    return "\n".join(lines)


def _render_current_task_json() -> str:
    """Serialize the v2 workspace summary as the on-disk CURRENT_TASK.json string.

    internal: replaces the legacy ``schema_version: 1`` per-task
    payload with the derive-on-read workspace summary built from per-task
    projection files. Always re-derives — no mtime cache, no revision-token
    cache (CTP-internal).
    """
    return json.dumps(_render_workspace_summary_from_per_task_files(), indent=2, sort_keys=True)


# ---------------------------------------------------------------------------
# Per-task projection writer (internal)
# ---------------------------------------------------------------------------


PER_TASK_PROJECTION_SCHEMA_VERSION = 1


def _build_per_task_projection_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "task_projection_schema_version": PER_TASK_PROJECTION_SCHEMA_VERSION,
        "task_ref": row["task_ref"],
        "status": row["status"],
        "objective": row["objective"],
        "focus": row["focus"],
        "target_branch": row["target_branch"],
        "target_worktree_path": row["target_worktree_path"],
        "task_plan_path": row["task_plan_path"],
        "revision": row["revision"],
        "updated_at": row["updated_at"],
    }


def _write_per_task_projection(task_ref: str) -> Path:
    """Write the per-task projection file for ``task_ref`` atomically.

    Reads the live ``handoff_state`` row, builds the
    ``task_projection_schema_version=1`` payload documented in the
    internal plan, and writes it to
    ``<RuntimeConfig.per_task_projection_dir>/<task_ref>.json`` via
    ``tempfile.NamedTemporaryFile`` + ``os.replace`` so concurrent
    readers never observe a partial file.

    Raises ``KeyError`` when ``task_ref`` has no live ``handoff_state``
    row — callers wiring this into write paths already hold the row.
    """
    runtime = get_runtime_config()
    with _get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT task_ref, objective, focus, status, target_branch,
                   target_worktree_path, task_plan_path, revision, updated_at
              FROM handoff_state
             WHERE task_ref = ?
            """,
            (task_ref,),
        ).fetchone()
    if row is None:
        raise KeyError(f"no live handoff_state row for task_ref={task_ref!r}")

    target_dir = runtime.per_task_projection_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{task_ref}.json"

    payload = _build_per_task_projection_payload(row)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"

    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f".{task_ref}.", suffix=".json.tmp", dir=str(target_dir))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.replace(tmp_path, target_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    return target_path


def _remove_per_task_projection(task_ref: str) -> bool:
    """Reap the per-task projection file for ``task_ref`` if present.

    Used by the ``archive`` write path so the per-task projection
    directory does not retain orphan files for tasks whose
    ``handoff_state`` row has been cleared.

    Returns True when a file was removed, False when no file existed.
    Missing parent directories are tolerated.
    """
    runtime = get_runtime_config()
    target_path = runtime.per_task_projection_dir / f"{task_ref}.json"
    try:
        target_path.unlink()
        return True
    except FileNotFoundError:
        return False


# ---------------------------------------------------------------------------
# Workspace summary derive-on-read (internal)
# ---------------------------------------------------------------------------


WORKSPACE_SUMMARY_SCHEMA_VERSION = 2
WORKSPACE_SUMMARY_STALENESS_NOTE = "May lag; authoritative state via load_session."


def _workspace_summary_base() -> dict[str, object]:
    return {
        "schema_version": WORKSPACE_SUMMARY_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "staleness_note": WORKSPACE_SUMMARY_STALENESS_NOTE,
    }


def _render_workspace_summary_from_per_task_files() -> dict[str, object]:
    """Derive the workspace-summary CURRENT_TASK.json shape from
    per-task projection files cross-referenced against live ``handoff_state`` rows.

    Returns one of three shapes (always with ``schema_version=2``):

    - ``single``: exactly one live task →
      ``{"shape": "single", "task_ref": ..., "active": <per-task payload>}``
    - ``workspace_ambiguous``: 2+ live tasks →
      ``{"shape": "workspace_ambiguous", "tasks": [<per-task payload>, ...]}``
      sorted by ``task_ref``.
    - ``none``: 0 live tasks → ``{"shape": "none"}``.

    Per-task files whose ``handoff_state`` row was deleted out-of-band
    (orphans) are filtered out (CTP-internal). The directory is bounded
    by active-task count; this read is cheap and always re-derives — no
    mtime cache, no revision-token cache (CTP-internal).
    """
    runtime = get_runtime_config()
    target_dir = runtime.per_task_projection_dir

    candidate_payloads: list[dict[str, object]] = []
    if target_dir.exists():
        for entry in sorted(target_dir.iterdir()):
            if not entry.is_file() or entry.suffix != ".json":
                continue
            try:
                payload = json.loads(entry.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            task_ref = payload.get("task_ref")
            if not isinstance(task_ref, str) or not task_ref:
                continue
            candidate_payloads.append(payload)

    if not candidate_payloads:
        return {**_workspace_summary_base(), "shape": "none"}

    candidate_refs = [str(p["task_ref"]) for p in candidate_payloads]
    placeholders = ",".join("?" for _ in candidate_refs)
    status_placeholders = ",".join("?" for _ in LIVE_ACTIVE_STATUSES)
    with _get_db_connection() as conn:
        rows = conn.execute(
            f"SELECT task_ref FROM handoff_state "  # noqa: S608
            f"WHERE task_ref IN ({placeholders}) "
            f"AND status IN ({status_placeholders})",
            [*candidate_refs, *LIVE_ACTIVE_STATUSES],
        ).fetchall()
    live_refs = {row[0] for row in rows}
    live_payloads = sorted(
        (p for p in candidate_payloads if p["task_ref"] in live_refs),
        key=lambda p: str(p["task_ref"]),
    )

    if not live_payloads:
        return {**_workspace_summary_base(), "shape": "none"}

    enriched_payloads = [_overlay_compaction_advisory(p) for p in live_payloads]

    if len(enriched_payloads) == 1:
        only = enriched_payloads[0]
        return {
            **_workspace_summary_base(),
            "shape": "single",
            "task_ref": only["task_ref"],
            "active": only,
        }
    return {
        **_workspace_summary_base(),
        "shape": "workspace_ambiguous",
        "tasks": enriched_payloads,
    }


def _overlay_compaction_advisory(payload: dict[str, object]) -> dict[str, object]:
    """Additively annotate a per-task projection payload with compaction_advisory.

    The advisory is derived from the live runtime — never persisted into the
    projection file — so the internal projection schema stays frozen at v1
    while CURRENT_TASK.json's workspace-summary view exposes the canonical
    advisory documented in internal for cold-start consumers.
    """
    task_ref = payload.get("task_ref")
    if not isinstance(task_ref, str) or not task_ref:
        return payload
    try:
        from .compaction import compute_compaction_advisory  # noqa: PLC0415 – late import to break circular

        workspace_root = get_runtime_config().compaction_config_root
        advisory = compute_compaction_advisory(workspace_root=workspace_root, task_ref=task_ref)
    except Exception:
        return payload
    enriched = dict(payload)
    enriched["compaction_advisory"] = advisory
    return enriched


def _write_workspace_summary_current_task_json(*, unconditional: bool = False) -> bool:
    """Atomically write the v2 workspace-summary CURRENT_TASK.json.

    Always re-derives via ``_render_workspace_summary_from_per_task_files()``.
    Writes via tmpfile + ``os.replace`` so concurrent readers never observe a
    partial file.

    Returns ``True`` when the file was rewritten and ``False`` when the
    routine auto-write was skipped because ``current_task_auto_regen`` is
    ``False``. Callers that must always materialize the summary pass
    ``unconditional=True``.
    """
    runtime = get_runtime_config()
    if not unconditional and not runtime.current_task_auto_regen:
        return False
    target_path = runtime.current_task_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = _render_current_task_json()
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=".CURRENT_TASK.", suffix=".json.tmp", dir=str(target_path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.replace(tmp_path, target_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    return True
