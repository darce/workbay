"""The ``scope-intake`` canvas template (internal).

``instantiate`` is a pure ``(ScopeIntakeInput) -> Canvas`` transformation.
It encodes the recurring scope-intake skeleton so an agent gets an
editable starter canvas from one MCP call instead of an empty document:

- one ``task`` node addressed as ``task:{task_ref}``;
- four ``decision`` placeholder nodes, one per canonical intake decision
  (primary consumer, data model, mvp signal, not-doing);
- one ``freeform`` "open questions" node;
- a ``derives_from`` edge from each decision node back to the task node.

Nodes are positionless (``x``/``y`` ``None``); internal's ``layout_canvas``
populates coordinates on demand. The default ``canvas_ref`` is
``scope:{task_ref}`` so a scope-intake starter never clobbers the
``task:{task_ref}`` canvas that ``render_task_as_canvas`` owns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from workbay_canvas_mcp.design_document import DesignDocument, apply_patch
from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

if TYPE_CHECKING:
    from workbay_canvas_mcp.store import CanvasStore

TEMPLATE_ID = "scope-intake"
DESCRIPTION = (
    "Starter canvas for a scope intake: one task node, four canonical "
    "decision placeholders (primary consumer, data model, mvp signal, "
    "not-doing), and an open-questions note."
)

# (decision key, human-readable label) in stable render order.
_INTAKE_DECISIONS: tuple[tuple[str, str], ...] = (
    ("primary_consumer", "Primary consumer"),
    ("data_model", "Data model"),
    ("mvp_signal", "MVP signal"),
    ("not_doing", "Not doing"),
)


class ScopeIntakeInput(BaseModel):
    """Typed input for the ``scope-intake`` template.

    ``task_ref`` is the only required field. ``title`` overrides the task
    node label when the caller wants something friendlier than the raw
    ``task_ref``.

    ``goals``, ``open_questions``, and ``not_doing`` carry no weight in
    the pure :func:`instantiate` canvas (its decision/open-questions nodes
    stay empty placeholders); they are seed-only content the opt-in
    :func:`seed` hook folds into the durable design document.
    """

    task_ref: str
    title: str | None = None
    goals: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    not_doing: list[str] = Field(default_factory=list)


def instantiate(input: ScopeIntakeInput) -> Canvas:
    task_ref = input.task_ref
    task_node = Node(
        id=f"task:{task_ref}",
        kind=NodeKind.TASK,
        label=input.title or task_ref,
        task_ref=task_ref,
    )

    decision_nodes = [
        Node(
            id=f"decision:{task_ref}:{key}",
            kind=NodeKind.DECISION,
            label=label,
            task_ref=task_ref,
        )
        for key, label in _INTAKE_DECISIONS
    ]

    open_questions = Node(
        id=f"freeform:{task_ref}:open_questions",
        kind=NodeKind.FREEFORM,
        label="Open questions",
        task_ref=task_ref,
    )

    edges = [
        Edge(
            id=f"derives:{node.id}",
            kind=EdgeKind.DERIVES_FROM,
            source=node.id,
            target=task_node.id,
        )
        for node in decision_nodes
    ]

    return Canvas(
        canvas_ref=f"scope:{task_ref}",
        nodes=[task_node, *decision_nodes, open_questions],
        edges=edges,
    )


def _merge(existing: list[str], incoming: list[str]) -> list[str]:
    """Append ``incoming`` items not already in ``existing``, order-preserving."""

    merged = list(existing)
    for item in incoming:
        if item not in merged:
            merged.append(item)
    return merged


def seed(input: ScopeIntakeInput, store: CanvasStore, canvas: Canvas) -> None:
    """Opt-in seed of the durable design document for a scope intake.

    Invoked by ``instantiate_template`` only when the caller passes
    ``seed=True``. Folds the caller's ``goals``, ``open_questions``, and
    ``not_doing`` into the ``design:{task_ref}`` document, merging by
    union so a re-run neither duplicates nor clobbers: existing entries
    are kept, only genuinely new ones append. An existing ``title`` is
    never overwritten (a missing one is filled from ``input.title``).
    Honors the task constraint that template instantiation may seed a
    design document but does not silently rewrite an existing one.
    """

    design_ref = f"design:{input.task_ref}"
    document = store.get_design_document(design_ref=design_ref) or DesignDocument(design_ref=design_ref)

    patch: dict[str, object] = {}
    if input.title is not None and document.title is None:
        patch["title"] = input.title

    for field, incoming in (
        ("goals", input.goals),
        ("open_questions", input.open_questions),
        ("not_doing", input.not_doing),
    ):
        merged = _merge(getattr(document, field), incoming)
        if merged != getattr(document, field):
            patch[field] = merged

    if patch:
        document = apply_patch(document, patch)
    store.upsert_design_document(document)
