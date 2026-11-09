"""Typed annotation model for canvas-mcp.

Slice-1 surface (internal): an :class:`Annotation` is an append-only,
per-`(canvas_ref, node_id)` prose note. ``parent_annotation_id`` is
reserved for implementation note's threading; implementation note accepts only ``None``.

Annotation rows live in the same SQLite store as Canvas data
(see :class:`workbay_canvas_mcp.store.CanvasStore`). They are keyed on
``(canvas_ref, node_id)`` rather than a Node primary key so that they
survive Canvas re-renders that drop the original node; such rows are
called "detached" and are still listable by canvas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from workbay_canvas_mcp.store import CanvasStore


class Annotation(BaseModel):
    """A typed annotation on a canvas node.

    ``annotation_id`` is unique within the store. ``created_at`` is an
    ISO-8601 UTC timestamp assigned at append time. ``body`` is opaque
    UTF-8 (Markdown by convention; not parsed server-side).
    """

    annotation_id: str
    canvas_ref: str
    node_id: str
    author: str
    body: str
    created_at: str
    parent_annotation_id: str | None = None


class AnnotationError(Exception):
    """Raised for typed annotation failures.

    ``reason`` is a stable machine-readable tag (e.g. ``"parent_missing"``,
    ``"annotation_missing"``) so callers can switch on it without parsing
    the message.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class ThreadEntry(BaseModel):
    """A single position in a reconstructed annotation thread.

    ``depth`` is the distance from the reconstruction root: 0 for the
    starting annotation, 1 for its direct replies, and so on.
    """

    annotation: Annotation
    depth: int


def reconstruct_thread(store: CanvasStore, annotation_id: str) -> list[ThreadEntry]:
    """Return ``annotation_id`` and its descendants in DFS preorder.

    Children are visited in ``(created_at, annotation_id)`` order, sorted
    inside this helper so the traversal order does not depend on the
    implicit ``ORDER BY`` of :meth:`CanvasStore.list_annotations`.
    Raises :class:`AnnotationError` with ``reason="annotation_missing"``
    if ``annotation_id`` does not exist.
    """

    root = store.get_annotation(annotation_id)
    if root is None:
        raise AnnotationError("annotation_missing", f"annotation {annotation_id!r} does not exist")
    all_for_canvas = store.list_annotations(canvas_ref=root.canvas_ref)
    children_by_parent: dict[str | None, list[Annotation]] = {}
    for a in all_for_canvas:
        children_by_parent.setdefault(a.parent_annotation_id, []).append(a)
    for siblings in children_by_parent.values():
        siblings.sort(key=lambda a: (a.created_at, a.annotation_id))

    out: list[ThreadEntry] = []
    stack: list[tuple[Annotation, int]] = [(root, 0)]
    while stack:
        node, depth = stack.pop()
        out.append(ThreadEntry(annotation=node, depth=depth))
        for child in reversed(children_by_parent.get(node.annotation_id, [])):
            stack.append((child, depth + 1))
    return out
