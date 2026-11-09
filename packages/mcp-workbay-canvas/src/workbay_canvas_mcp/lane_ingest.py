"""Map a lane snapshot to a :class:`Canvas` (internal)."""

from __future__ import annotations

from typing import Any, Callable

from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

LaneReader = Callable[[str, str | None], dict[str, Any]]


def build_canvas_from_lanes(*, reader: LaneReader, task_ref: str, lane_id: str | None = None) -> Canvas:
    """Render the lane snapshot for ``task_ref`` as a :class:`Canvas`."""

    snapshot = reader(task_ref, lane_id)
    lanes: list[dict[str, Any]] = list(snapshot.get("lanes", []) or [])

    task_node = Node(
        id=f"task:{task_ref}",
        kind=NodeKind.TASK,
        label=task_ref,
        task_ref=task_ref,
    )

    nodes: list[Node] = [task_node]
    edges: list[Edge] = []
    decision_nodes: dict[str, Node] = {}

    for lane in lanes:
        current_lane_id = str(lane["lane_id"])
        lane_node = Node(
            id=f"lane:{current_lane_id}",
            kind=NodeKind.LANE,
            label=current_lane_id,
            task_ref=task_ref,
            lane_id=current_lane_id,
        )
        nodes.append(lane_node)

        for worker in list(lane.get("workers", []) or []):
            worker_id = str(worker["worker_id"])
            worker_node = Node(
                id=f"worker:{current_lane_id}:{worker_id}",
                kind=NodeKind.WORKER,
                label=worker_id,
                task_ref=task_ref,
            )
            nodes.append(worker_node)
            edges.append(
                Edge(
                    id=f"assigned:{worker_node.id}->{lane_node.id}",
                    kind=EdgeKind.ASSIGNED_TO,
                    source=worker_node.id,
                    target=lane_node.id,
                )
            )

            report_text = worker.get("report_text")
            if isinstance(report_text, str) and report_text:
                report_node = Node(
                    id=f"report:{current_lane_id}:{worker_id}",
                    kind=NodeKind.FREEFORM,
                    label=report_text,
                    task_ref=task_ref,
                )
                nodes.append(report_node)
                edges.append(
                    Edge(
                        id=f"assigned:{report_node.id}->{worker_node.id}",
                        kind=EdgeKind.ASSIGNED_TO,
                        source=report_node.id,
                        target=worker_node.id,
                    )
                )

        for decision_id_raw in list(lane.get("decision_ids", []) or []):
            decision_id = str(decision_id_raw)
            decision_node = decision_nodes.get(decision_id)
            if decision_node is None:
                decision_node = Node(
                    id=f"decision:{decision_id}",
                    kind=NodeKind.DECISION,
                    label=decision_id,
                    task_ref=task_ref,
                    decision_id=decision_id,
                )
                decision_nodes[decision_id] = decision_node
                nodes.append(decision_node)
            edges.append(
                Edge(
                    id=f"produced:{lane_node.id}->{decision_node.id}",
                    kind=EdgeKind.PRODUCED,
                    source=lane_node.id,
                    target=decision_node.id,
                )
            )

    canvas_ref = f"lanes:{task_ref}" if lane_id is None else f"lane:{task_ref}:{lane_id}"
    return Canvas(canvas_ref=canvas_ref, nodes=nodes, edges=edges)
