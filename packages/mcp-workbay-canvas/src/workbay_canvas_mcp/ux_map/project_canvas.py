"""Project a UX map flow onto a FREEFORM-only Canvas (derived, not dual SSOT)."""

from __future__ import annotations

from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind
from workbay_canvas_mcp.ux_map.models import UxMap


class ProjectError(ValueError):
    """Raised when a flow cannot be projected."""


def project_flow_to_canvas(map_: UxMap, flow_id: str) -> Canvas:
    """Build a FREEFORM Canvas for ``flow_id`` (pure; no I/O).

    - One FREEFORM node per flow step (label = screen title when known)
    - FREEFORM edges between consecutive steps (label = *source* step
      ``branch_label``, matching Mermaid)
    - Durable identity: ``canvas_ref = f"uxmap:{map_ref}:flow:{flow_id}"``
    - ``metadata["uxmap"]`` is **call-scoped only** (CanvasStore does not
      persist ``metadata``); re-read via ``get_canvas`` keeps nodes/edges
      but not metadata — parse ``canvas_ref`` for map/flow ids
    - Never writes ``.uxmap.json``; never reverse-syncs canvas → map ([DATA-14])
    """

    flow = next((f for f in map_.flows if f.id == flow_id), None)
    if flow is None:
        known = [f.id for f in map_.flows]
        raise ProjectError(f"unknown flow {flow_id!r}; known={known}")

    map_ref = map_.map_ref
    nodes: list[Node] = []
    edges: list[Edge] = []
    node_ids: list[str] = []

    for index, step in enumerate(flow.steps):
        screen = map_.screen_by_id(step.screen_id)
        label = screen.title if screen is not None else step.screen_id
        # Unique node id even if the same screen appears twice in a flow
        node_id = f"uxmap:{map_ref}:flow:{flow_id}:step:{index}:{step.screen_id}"
        nodes.append(
            Node(
                id=node_id,
                kind=NodeKind.FREEFORM,
                label=label,
            )
        )
        node_ids.append(node_id)
        if index > 0:
            # Match Mermaid: edge label comes from the *source* step's branch_label
            prev_step = flow.steps[index - 1]
            prev = node_ids[index - 1]
            edges.append(
                Edge(
                    id=f"uxmap:{map_ref}:flow:{flow_id}:edge:{index - 1}-{index}",
                    kind=EdgeKind.FREEFORM,
                    source=prev,
                    target=node_id,
                    label=prev_step.branch_label,
                )
            )

    return Canvas(
        canvas_ref=f"uxmap:{map_ref}:flow:{flow_id}",
        nodes=nodes,
        edges=edges,
        metadata={
            "uxmap": {
                "map_ref": map_ref,
                "flow_id": flow_id,
                "job": flow.job,
                "label": flow.label,
                "source": "project_flow_to_canvas",
            }
        },
    )
