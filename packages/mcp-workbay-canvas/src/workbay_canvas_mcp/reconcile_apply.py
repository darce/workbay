"""Mutation logic for ``apply_reconciliation`` (internal).

:func:`apply_reconciliation_pure` applies an operator-confirmed
:class:`ReconciliationPlan` to a :class:`Canvas` and returns a *new*
Canvas with rename / remove / keep actions applied. The input Canvas is
not mutated — the server-side tool calls
:meth:`workbay_canvas_mcp.store.CanvasStore.upsert_canvas` to persist the
returned value.

Rename clears the named ref field on the named node; remove deletes the
node and any edges that reference it (so the resulting Canvas still
satisfies the ``edges → existing-node-ids`` invariant on
:class:`workbay_canvas_mcp.models.Canvas`). Keep is a no-op recorded so
the plan stays a complete record of operator intent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from workbay_canvas_mcp.models import Canvas, Node
from workbay_canvas_mcp.reconcile import RefKind

ApplyAction = Literal["keep", "rename", "remove"]


class ApplyReconciliationError(Exception):
    """Structured error raised when an action cannot be applied.

    ``reason`` is a stable token (one of ``rename_missing_new_value``,
    ``new_value_not_allowed``, ``node_not_found``, ``canvas_ref_mismatch``)
    so callers can branch on the failure class without parsing free-form
    text. ``ApplyReconciliationError`` is intentionally an ``Exception``
    (not ``ValueError``), so pydantic ``model_validator`` raises propagate
    it directly rather than wrapping it in a ``ValidationError``.
    """

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class ReconciliationAction(BaseModel):
    """One operator-confirmed action against a single (node, ref kind) pair.

    ``new_value`` is required for ``rename`` and must be omitted for
    ``keep`` / ``remove`` — enforced by the model validator so an
    invalid plan is rejected at the boundary rather than mid-apply.
    """

    node_id: str
    ref_kind: RefKind
    action: ApplyAction
    new_value: str | None = None

    @model_validator(mode="after")
    def _enforce_new_value_pairing(self) -> ReconciliationAction:
        if self.action == "rename" and not self.new_value:
            raise ApplyReconciliationError(
                "rename_missing_new_value",
                f"rename action on node {self.node_id!r} requires a non-empty new_value",
            )
        if self.action != "rename" and self.new_value is not None:
            raise ApplyReconciliationError(
                "new_value_not_allowed",
                f"{self.action} action on node {self.node_id!r} must omit new_value",
            )
        return self


class ReconciliationPlan(BaseModel):
    """An operator-confirmed batch of actions for one Canvas."""

    canvas_ref: str
    actions: list[ReconciliationAction] = Field(default_factory=list)


_REF_FIELD: dict[RefKind, str] = {
    "task_ref": "task_ref",
    "decision_id": "decision_id",
    "finding_id": "finding_id",
    "lane_id": "lane_id",
}


def apply_reconciliation_pure(canvas: Canvas, plan: ReconciliationPlan) -> Canvas:
    """Apply ``plan`` to ``canvas`` and return the resulting Canvas.

    Pure: the input Canvas is not mutated. Removed nodes have their
    incident edges dropped so the returned Canvas still satisfies
    Canvas's edge-references-existing-node invariant.
    """

    if plan.canvas_ref != canvas.canvas_ref:
        raise ApplyReconciliationError(
            "canvas_ref_mismatch",
            f"plan.canvas_ref={plan.canvas_ref!r} does not match canvas.canvas_ref={canvas.canvas_ref!r}",
        )

    nodes_by_id: dict[str, Node] = {n.id: n.model_copy() for n in canvas.nodes}
    removed_ids: set[str] = set()

    for action in plan.actions:
        if action.action == "keep":
            continue
        if action.node_id not in nodes_by_id:
            raise ApplyReconciliationError(
                "node_not_found",
                f"action references unknown node_id {action.node_id!r}",
            )
        if action.action == "remove":
            removed_ids.add(action.node_id)
            nodes_by_id.pop(action.node_id, None)
            continue
        # rename
        field = _REF_FIELD[action.ref_kind]
        setattr(nodes_by_id[action.node_id], field, action.new_value)

    surviving_nodes = [nodes_by_id[n.id] for n in canvas.nodes if n.id in nodes_by_id]
    surviving_edges = [e for e in canvas.edges if e.source not in removed_ids and e.target not in removed_ids]

    return Canvas(
        canvas_ref=canvas.canvas_ref,
        nodes=surviving_nodes,
        edges=surviving_edges,
    )


__all__ = [
    "ApplyAction",
    "ApplyReconciliationError",
    "ReconciliationAction",
    "ReconciliationPlan",
    "apply_reconciliation_pure",
]
