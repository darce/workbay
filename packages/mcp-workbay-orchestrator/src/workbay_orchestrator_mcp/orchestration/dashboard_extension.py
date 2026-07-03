"""Orchestrator dashboard extension for DASHBOARD.txt.

Registers a ``DashboardExtension`` callback (per rg-014: late-binding imports)
that appends two sections to the DASHBOARD.txt human observatory view:

- Lane Health (order=50): active/blocked/review lanes from the most recent
  ``worktree_lanes`` snapshot passed in ``DashboardContext``.
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
    """Extension callback: produce Lane Health and Worker Status sections.

    Uses late-binding imports per rg-014 so this module can be imported
    without triggering an early circular import of workbay_handoff_mcp symbols.
    """
    from workbay_handoff_mcp.dashboard_rendering import DashboardSection  # noqa: PLC0415

    sections: list[DashboardSection] = []

    # ------------------------------------------------------------------
    # Lane Health (order=50)
    # ------------------------------------------------------------------
    lanes: list[dict] = ctx.get("worktree_lanes", [])
    visible_statuses = {"active", "blocked", "review"}
    visible_lanes = [ln for ln in lanes if ln.get("status") in visible_statuses]

    if visible_lanes:
        lane_lines: list[str] = []
        for ln in visible_lanes:
            status = ln.get("status", "")
            lane_id = ln.get("lane_id", ln.get("id", "?"))
            title = ln.get("title") or ln.get("objective") or ""
            branch = ln.get("branch", "")
            icon = {"active": "◎", "blocked": "⚠", "review": "↑"}.get(status, "·")
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
    # Worker Status (order=60)
    # ------------------------------------------------------------------
    reports: list[dict] = ctx.get("worker_reports", [])
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
