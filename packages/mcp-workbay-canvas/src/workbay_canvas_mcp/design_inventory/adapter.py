"""Map declared components onto inventory rows (WBC-03 S8, no I/O)."""

from __future__ import annotations

from workbay_canvas_mcp.design_inventory.config import DeclaredComponent
from workbay_canvas_mcp.design_inventory.models import ExportRef, InventoryComponent


def component_from_declared(
    declared: DeclaredComponent,
    *,
    export: ExportRef,
    known_tokens: set[str] | None = None,
    declared_props: dict[str, str] | None = None,
) -> InventoryComponent:
    """Copy roster-shaped fields from a declared component plus collector export."""
    known = known_tokens or set()
    token_refs: list[str] = []
    unresolved_token_refs: list[str] = []
    for ref in declared.token_refs:
        if ref in known:
            token_refs.append(ref)
        else:
            unresolved_token_refs.append(ref)
    return InventoryComponent(
        id=declared.id,
        name=declared.export_name,
        description=declared.description,
        export_ref=export,
        declared_props=dict(declared_props or {}),
        slots=list(declared.slots),
        token_refs=token_refs,
        unresolved_token_refs=unresolved_token_refs,
        example_refs=[],
    )
