"""The ``lane-dependency`` canvas template (internal).

``instantiate`` is a pure ``(LaneDependencyInput) -> Canvas``
transformation. It encodes a lane-dependency skeleton so an agent can
sketch how orchestrator lanes relate from one MCP call:

- one ``task`` node addressed as ``task:{task_ref}``;
- one ``lane`` node per (deduped, order-preserving) ``lane_id``,
  addressed as ``lane:{lane_id}`` — the same id ``lane_ingest.py`` /
  ``render_lanes_as_canvas`` use, so a templated canvas and a rendered
  lane canvas address the same nodes;
- an ``assigned_to`` edge from each lane back to the task;
- a ``depends_on`` edge for each caller-supplied lane-to-lane dependency.

Edges that reference a lane outside ``lane_ids`` fail model validation,
which :func:`build_template_canvas` surfaces as
``TemplateError(reason="invalid_input")``. Nodes are positionless
(``x``/``y`` ``None``); internal's ``layout_canvas`` populates
coordinates on demand. The default ``canvas_ref`` is
``lane-deps:{task_ref}`` so the template never clobbers the
``lanes:{task_ref}`` canvas that ``render_lanes_as_canvas`` owns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, model_validator

from workbay_canvas_mcp.component_roster import ComponentSpec
from workbay_canvas_mcp.design_document import DesignDocument, append_decision, apply_patch
from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

if TYPE_CHECKING:
    from workbay_canvas_mcp.store import CanvasStore

TEMPLATE_ID = "lane-dependency"
DESCRIPTION = (
    "Starter canvas for lane dependencies: one task node, one lane node "
    "per lane_id, and depends_on edges between lanes. Lane node ids match "
    "render_lanes_as_canvas so the views overlay."
)

# Component id of the reusable lane-node spec the seed hook upserts.
SEED_COMPONENT_ID = "lane-node"


class LaneDependencyEdge(BaseModel):
    """A directed dependency: ``source`` lane depends on ``target`` lane."""

    source: str
    target: str
    label: str | None = None


class LaneDependencyInput(BaseModel):
    """Typed input for the ``lane-dependency`` template.

    ``lane_ids`` may repeat; :func:`instantiate` dedupes them
    order-preservingly. Every ``edges`` endpoint must appear in
    ``lane_ids`` or validation fails.
    """

    task_ref: str
    lane_ids: list[str] = Field(default_factory=list)
    edges: list[LaneDependencyEdge] = Field(default_factory=list)
    title: str | None = None

    @model_validator(mode="after")
    def _edges_reference_known_lanes(self) -> LaneDependencyInput:
        known = set(self.lane_ids)
        for edge in self.edges:
            unknown = {edge.source, edge.target} - known
            if unknown:
                raise ValueError(f"edge references unknown lane(s): {sorted(unknown)}")
        return self


def _dedupe(lane_ids: list[str]) -> list[str]:
    """Return ``lane_ids`` with duplicates removed, order preserved."""

    seen: set[str] = set()
    deduped: list[str] = []
    for lane_id in lane_ids:
        if lane_id not in seen:
            seen.add(lane_id)
            deduped.append(lane_id)
    return deduped


def instantiate(input: LaneDependencyInput) -> Canvas:
    task_ref = input.task_ref
    task_node = Node(
        id=f"task:{task_ref}",
        kind=NodeKind.TASK,
        label=input.title or task_ref,
        task_ref=task_ref,
    )

    lane_ids = _dedupe(input.lane_ids)

    lane_nodes = [
        Node(
            id=f"lane:{lane_id}",
            kind=NodeKind.LANE,
            label=lane_id,
            task_ref=task_ref,
        )
        for lane_id in lane_ids
    ]

    edges = [
        Edge(
            id=f"assigned:lane:{lane_id}->{task_node.id}",
            kind=EdgeKind.ASSIGNED_TO,
            source=f"lane:{lane_id}",
            target=task_node.id,
        )
        for lane_id in lane_ids
    ]
    edges.extend(
        Edge(
            id=f"depends:lane:{edge.source}->lane:{edge.target}",
            kind=EdgeKind.DEPENDS_ON,
            source=f"lane:{edge.source}",
            target=f"lane:{edge.target}",
            label=edge.label,
        )
        for edge in input.edges
    )

    return Canvas(
        canvas_ref=f"lane-deps:{task_ref}",
        nodes=[task_node, *lane_nodes],
        edges=edges,
    )


def seed(input: LaneDependencyInput, store: CanvasStore, canvas: Canvas) -> None:
    """Opt-in seed of durable design-system state for a lane-dependency canvas.

    Invoked by ``instantiate_template`` only when the caller passes
    ``seed=True``. Two writes, both idempotent and non-destructive so a
    repeat instantiate does not duplicate or clobber:

    - upsert one reusable ``lane-node`` :class:`ComponentSpec` into the
      roster under ``design:{task_ref}`` (idempotent on ``component_id``),
      provenance-linked to the stored canvas and its lane node ids;
    - add ``lane-node`` to the design document's ``component_refs`` and
      append one "Lane dependency layout" decision — but only if not
      already present — so the design record gains memory of the layout
      without rewriting any existing field.
    """

    task_ref = input.task_ref
    design_ref = f"design:{task_ref}"
    lane_node_ids = [node.id for node in canvas.nodes if node.kind == NodeKind.LANE]

    store.upsert_component(
        design_ref=design_ref,
        component=ComponentSpec(
            component_id=SEED_COMPONENT_ID,
            name="Lane Node",
            description="Reusable node for one orchestrator lane in a lane-dependency canvas.",
            source_canvas_ref=canvas.canvas_ref,
            source_node_ids=lane_node_ids,
        ),
    )

    document = store.get_design_document(design_ref=design_ref) or DesignDocument(design_ref=design_ref)
    if SEED_COMPONENT_ID not in document.component_refs:
        document = apply_patch(
            document,
            {"component_refs": [*document.component_refs, SEED_COMPONENT_ID]},
        )

    summary = f"Lane dependency layout for {task_ref}"
    if not any(decision.summary == summary for decision in document.layout_decisions):
        lane_ids = [node_id.removeprefix("lane:") for node_id in lane_node_ids]
        lanes_text = ", ".join(lane_ids) if lane_ids else "none"
        document = append_decision(
            document,
            summary=summary,
            rationale=f"{len(lane_ids)} lane(s): {lanes_text}; {len(input.edges)} dependency edge(s).",
        )

    store.upsert_design_document(document)
