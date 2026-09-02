"""Orchestrator dashboard extension for DASHBOARD.txt.

Registers a ``DashboardExtension`` callback (per rg-014: late-binding imports)
that appends sections to the DASHBOARD.txt human observatory view:

- Lane Health (order=50): active/blocked/review lanes from the most recent
  ``worktree_lanes`` snapshot passed in ``DashboardContext``. Blocked lanes
  include internal aging fields (age + task_ref + last blocker).
- Blocked Lane Aging (order=55): dedicated aging report for blocked lanes.
- Worker Status (order=60): recent submitted worker reports.

Registration is triggered by importing this module (called from
``workbay_orchestrator_mcp.api`` at module load via
``_register_dashboard_extensions()``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from workbay_handoff_mcp.dashboard_rendering import DashboardContext, DashboardSection


def lane_worker_extension(ctx: DashboardContext) -> list[DashboardSection]:
    """Extension callback: produce Lane Health, aging, and Worker Status sections.

    Uses late-binding imports per rg-014 so this module can be imported
    without triggering an early circular import of workbay_handoff_mcp symbols.
    """
    from workbay_handoff_mcp.dashboard_rendering import DashboardSection  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
        collect_blocked_lane_aging_entries,
        format_blocked_lane_aging_line,
        format_lane_age_label,
    )

    sections: list[DashboardSection] = []

    lanes: list[dict] = ctx.get("worktree_lanes", [])
    reports: list[dict] = ctx.get("worker_reports", [])
    visible_statuses = {"active", "blocked", "review"}
    visible_lanes = [ln for ln in lanes if ln.get("status") in visible_statuses]

    # Best-effort blockers lookup for aging lines (DashboardContext has no blockers).
    # Degrades to worker_reports / notes when the DB is unavailable.
    aging_conn = None
    aging_conn_cm = None
    try:
        from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

        aging_conn_cm = _get_db_connection()
        aging_conn = aging_conn_cm.__enter__()
    except Exception:  # noqa: BLE001 — dashboard must never fail-closed
        aging_conn = None
        aging_conn_cm = None

    try:
        blocked_entries = collect_blocked_lane_aging_entries(
            lanes,
            reports=reports,
            conn=aging_conn,
        )
        blocked_by_lane = {str(e.get("lane_id")): e for e in blocked_entries}

        # ------------------------------------------------------------------
        # Lane Health (order=50)
        # ------------------------------------------------------------------
        if visible_lanes:
            lane_lines: list[str] = []
            for ln in visible_lanes:
                status = ln.get("status", "")
                lane_id = ln.get("lane_id", ln.get("id", "?"))
                title = ln.get("title") or ln.get("objective") or ""
                branch = ln.get("branch", "")
                icon = {"active": "◎", "blocked": "⚠", "review": "↑"}.get(status, "·")
                if status == "blocked":
                    # internal: age + task_ref + last blocker on DASHBOARD.
                    aging = blocked_by_lane.get(str(lane_id))
                    if aging is None:
                        ones = collect_blocked_lane_aging_entries([ln], reports=reports, conn=aging_conn)
                        aging = ones[0] if ones else None
                    if aging is not None:
                        lane_lines.append(format_blocked_lane_aging_line(aging))
                    else:
                        age = format_lane_age_label(ln.get("updated_at"), ln.get("created_at"))
                        task_ref = ln.get("task_ref") or "?"
                        lane_lines.append(f"  {icon} {lane_id}  task={task_ref}  age={age}  blocker: (no blocker text)")
                else:
                    lane_lines.append(f"  {icon} {lane_id:<20}  [{status}]  {title}  ({branch})")
            sections.append(
                DashboardSection(
                    heading="Lane Health",
                    content="\n".join(lane_lines),
                    order=50,
                )
            )
        else:
            sections.append(
                DashboardSection(
                    heading="Lane Health",
                    content="- No active, blocked, or review lanes.",
                    order=50,
                )
            )

        # ------------------------------------------------------------------
        # Blocked Lane Aging (order=55)
        # ------------------------------------------------------------------
        if blocked_entries:
            aging_lines = [format_blocked_lane_aging_line(e) for e in blocked_entries]
            sections.append(
                DashboardSection(
                    heading="Blocked Lane Aging",
                    content="\n".join(aging_lines),
                    order=55,
                )
            )
        else:
            sections.append(
                DashboardSection(
                    heading="Blocked Lane Aging",
                    content="- No blocked lanes.",
                    order=55,
                )
            )
    finally:
        if aging_conn_cm is not None:
            try:
                aging_conn_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001 — never fail dashboard on cleanup
                pass

    # ------------------------------------------------------------------
    # Worker Status (order=60)
    # ------------------------------------------------------------------
    submitted = [r for r in reports if r.get("status") == "submitted"]

    if submitted:
        report_lines: list[str] = []
        for r in submitted[:5]:  # show at most 5
            lane_id = r.get("lane_id", "?")
            summary = (r.get("summary") or "")[:80]
            merge_ready = r.get("merge_ready", 0)
            ready_flag = " [merge-ready]" if merge_ready else ""
            report_lines.append(f"  • lane={lane_id}  {summary}{ready_flag}")
        sections.append(
            DashboardSection(
                heading="Worker Status",
                content="\n".join(report_lines),
                order=60,
            )
        )
    else:
        sections.append(
            DashboardSection(
                heading="Worker Status",
                content="- No submitted worker reports.",
                order=60,
            )
        )

    return sections
