"""Reusable component roster models (internal).

The roster is the durable design-system layer: a set of reusable
component definitions scoped by ``design_ref`` so successive canvas
iterations can say "this layout reuses the same ``SidebarShell``
component" instead of treating every diagram as an isolated picture.

Models borrow Pencil's reusable-component vocabulary — a component owns a
props schema, named :class:`SlotSpec` insertion points, and design-token
references — but stay tool-neutral so the same spec can later scaffold
static HTML, Markdown, React, or a ``.pen``-like export. Nothing here
performs I/O; :class:`~workbay_canvas_mcp.store.CanvasStore` owns
persistence and the ``manage_component_roster`` MCP tool owns the
operation surface.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SlotSpec(BaseModel):
    """A named insertion point within a component.

    ``allowed_component_ids`` lists the component ids that may fill the
    slot (Pencil's "suggested components" idea — guidance for both human
    and AI designers); an empty list means "unconstrained". ``guidance``
    is optional free text shown alongside the slot.
    """

    slot_id: str
    allowed_component_ids: list[str] = Field(default_factory=list)
    guidance: str | None = None


class ComponentSpec(BaseModel):
    """A reusable component definition in the roster.

    ``component_id`` is unique within a ``design_ref``. ``status`` is
    ``"active"`` for live components and ``"archived"`` once retired;
    archived rows are retained so historical canvases keep resolving
    their component references.
    """

    component_id: str
    name: str
    description: str | None = None
    props_schema: dict[str, Any] = Field(default_factory=dict)
    slots: list[SlotSpec] = Field(default_factory=list)
    token_refs: list[str] = Field(default_factory=list)
    source_canvas_ref: str | None = None
    source_node_ids: list[str] = Field(default_factory=list)
    status: Literal["active", "archived"] = "active"


class ComponentRosterReceipt(BaseModel):
    """Result of a ``manage_component_roster`` call.

    ``components`` always reflects the full roster for ``design_ref``
    after the operation, so a caller sees the post-mutation state without
    a follow-up ``list``. ``component_id`` echoes the entry an ``upsert``
    or ``archive`` touched (``None`` for ``list``).
    """

    operation: str
    design_ref: str
    component_id: str | None = None
    components: list[ComponentSpec] = Field(default_factory=list)
