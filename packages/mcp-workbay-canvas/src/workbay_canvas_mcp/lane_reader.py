"""Production lane reader for ``render_lanes_as_canvas`` (internal)."""

from __future__ import annotations

from typing import Any

from workbay_orchestrator_mcp.api import get_lane_activity, manage_worktree_lane  # type: ignore[import-untyped]


def default_lane_reader(task_ref: str, lane_id: str | None = None) -> dict[str, Any]:
    """Read orchestrator lane state and adapt it to the pinned snapshot shape."""

    if lane_id is None:
        listed = manage_worktree_lane(operation="list", task_ref=task_ref)
        if not listed.get("ok"):
            return {"task_ref": task_ref, "lanes": []}
        lane_ids = [str(row["lane_id"]) for row in list(listed.get("lanes", []) or []) if row.get("lane_id")]
    else:
        lane_ids = [lane_id]

    lanes: list[dict[str, Any]] = []
    for current_lane_id in lane_ids:
        activity = get_lane_activity(
            task_ref=task_ref,
            lane_id=current_lane_id,
            sections="identity,lane,reports,messages,actions,findings,decisions",
            detail="summary",
        )
        if not activity.get("ok"):
            continue

        lane = activity.get("lane") or {}
        reports = list(activity.get("reports", []) or [])
        decisions = list(activity.get("decisions", []) or [])
        workers: list[dict[str, str | None]] = []
        for idx, report in enumerate(reports):
            agent = report.get("agent")
            worker_id = str(report.get("worker_id") or agent or f"{current_lane_id}-{idx}")
            workers.append(
                {
                    "worker_id": worker_id,
                    "agent": None if agent is None else str(agent),
                    "report_text": None if report.get("summary") is None else str(report.get("summary")),
                }
            )

        lanes.append(
            {
                "lane_id": str(lane.get("lane_id") or current_lane_id),
                "status": str(lane.get("status") or "unknown"),
                "title": None if lane.get("title") is None else str(lane.get("title")),
                "objective": None if lane.get("objective") is None else str(lane.get("objective")),
                "workers": workers,
                "decision_ids": [str(row["decision"]) for row in decisions if row.get("decision")],
            }
        )

    return {"task_ref": task_ref, "lanes": lanes}
