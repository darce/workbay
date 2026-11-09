"""Typed Canvas / Node / Edge models for canvas-mcp.

Slice-2 surface. Nodes and edges are typed via :class:`NodeKind` and
:class:`EdgeKind`. ``task_ref``, ``decision_id``, and ``finding_id`` on a
node are not foreign-key-enforced against ``mcp-workbay-handoff``;
dangling references are tolerated by design (the canvas survives a
handoff archive or rename).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


class NodeKind(str, Enum):
    TASK = "task"
    SLICE = "slice"
    DECISION = "decision"
    FINDING = "finding"
    LANE = "lane"
    WORKER = "worker"
    FREEFORM = "freeform"


class EdgeKind(str, Enum):
    DEPENDS_ON = "depends_on"
    BLOCKS = "blocks"
    DERIVES_FROM = "derives_from"
    ASSIGNED_TO = "assigned_to"
    PRODUCED = "produced"
    FREEFORM = "freeform"


class Node(BaseModel):
    """A typed canvas node.

    ``id`` is unique within a Canvas. ``task_ref`` / ``decision_id`` /
    ``finding_id`` reference handoff identifiers without foreign-key
    enforcement. ``x`` / ``y`` are optional layout coordinates; absent
    coordinates mean "no fixed position yet".
    """

    id: str
    kind: NodeKind
    label: str
    task_ref: str | None = None
    decision_id: str | None = None
    finding_id: str | None = None
    lane_id: str | None = None
    x: float | None = None
    y: float | None = None


class Edge(BaseModel):
    """A typed edge between two node ids within the same Canvas."""

    id: str
    kind: EdgeKind
    source: str
    target: str
    label: str | None = None


class Canvas(BaseModel):
    """A canvas document keyed by ``canvas_ref``.

    ``canvas_ref`` is the stable string identifier callers use to
    address this canvas (for example ``"task:internal"`` for the
    canonical task-derived canvas).

    Coherence invariant: every edge's ``source`` and ``target`` must
    appear in the node id set. ``upsert_canvas`` relies on this so the
    store never persists dangling edges.
    """

    canvas_ref: str
    nodes: list[Node] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)
    updated_at: str | None = None
    """Last persisted update timestamp, populated by :class:`CanvasStore`.

    Browser clients use this as a lightweight stale-draft guard before
    submitting full-replace edits through ``upsert_canvas``.
    """
    metadata: dict[str, Any] = Field(default_factory=dict)
    """Transient, call-scoped annotations (e.g. layout warnings).

    Not persisted: :class:`CanvasStore` ignores this field on write and
    always reconstructs it empty on read. Tools such as ``layout_canvas``
    populate ``metadata["layout"]`` on the returned (in-memory) Canvas so
    callers can inspect what the call did without a schema change to the
    persisted ``canvases``/``nodes``/``edges`` tables.
    """

    @model_validator(mode="after")
    def _edges_must_reference_existing_nodes(self) -> Self:
        node_ids = {n.id for n in self.nodes}
        dangling: list[str] = []
        for edge in self.edges:
            if edge.source not in node_ids:
                dangling.append(f"edge {edge.id!r} source={edge.source!r}")
            if edge.target not in node_ids:
                dangling.append(f"edge {edge.id!r} target={edge.target!r}")
        if dangling:
            raise ValueError("Canvas edges reference unknown node ids: " + "; ".join(dangling))
        return self


class CanvasSpec(BaseModel):
    """Lightweight per-canvas summary for the canvas-index / list view.

    Returned by ``list_canvases`` so a UI can render the set of stored
    canvases without fetching every full :class:`Canvas`. The ``Canvas``
    model has no title, so consumers use ``canvas_ref`` as the display
    label. ``created_at`` / ``updated_at`` are the verbatim TEXT
    timestamps stored on the ``canvases`` row.
    """

    canvas_ref: str
    node_count: int
    edge_count: int
    created_at: str
    updated_at: str
