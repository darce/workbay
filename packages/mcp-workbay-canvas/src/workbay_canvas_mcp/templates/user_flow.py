"""The ``user-flow`` canvas template — pure FREEFORM projection wrapper.

Symmetry with ``instantiate_template`` only. The map file remains SSOT;
this template never persists a dual map into the canvas DB beyond the
FREEFORM flow projection produced by :func:`project_flow_to_canvas`.

**Purity ([REF-18]):** ``instantiate`` is pure ``(UserFlowInput) -> Canvas``.
It requires an inline ``document`` dict. Filesystem load is **not** done here
— use MCP ``project_ux_flow`` / CLI ``ux-map project`` (or load_map then
project_flow_to_canvas) for path/map_ref resolution.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

from workbay_canvas_mcp.models import Canvas
from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.project_canvas import project_flow_to_canvas

TEMPLATE_ID = "user-flow"
DESCRIPTION = (
    "Project an inline UX map document flow onto a FREEFORM canvas "
    "(canvas_ref=uxmap:{map_ref}:flow:{flow_id}). Pass document= for purity; "
    "use project_ux_flow MCP/CLI for filesystem maps."
)


class UserFlowInput(BaseModel):
    """Typed input for the ``user-flow`` template (pure; no filesystem)."""

    flow_id: str
    document: dict[str, Any] = Field(
        ...,
        description="Full UxMap JSON document (required for pure instantiate).",
    )
    canvas_ref_override: str | None = Field(
        default=None,
        description="Optional; normally derived as uxmap:{map_ref}:flow:{flow_id}.",
    )

    @model_validator(mode="after")
    def _document_required(self) -> UserFlowInput:
        if not self.document:
            raise ValueError(
                "user-flow requires document={...} UxMap JSON; "
                "load filesystem maps via project_ux_flow / ux-map project"
            )
        return self


def instantiate(input: UserFlowInput) -> Canvas:
    """Pure ``(UserFlowInput) -> Canvas`` — validates document and projects."""
    map_ = UxMap.model_validate(input.document)
    canvas = project_flow_to_canvas(map_, flow_id=input.flow_id)
    if input.canvas_ref_override is not None:
        canvas = canvas.model_copy(update={"canvas_ref": input.canvas_ref_override})
    return canvas
