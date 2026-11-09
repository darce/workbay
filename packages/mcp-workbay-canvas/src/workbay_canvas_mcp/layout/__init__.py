"""Deterministic, pure-Python layout for canvas-mcp (internal).

Public surface:

- :class:`LayoutAlgorithm` — enum selecting a layout algorithm.
- :class:`CyclePolicy` — how hierarchical layout reacts to cycles.
- :class:`LayoutOptions` — tunables + safety budget, with documented defaults.
- :class:`LayoutError` — structured failure (``reason`` in ``too_large`` |
  ``timeout`` | ``cycle_policy_required``).
- :func:`layout_canvas_pure` — dispatch entry point. Pure: it reads a
  :class:`~workbay_canvas_mcp.models.Canvas` and returns a new Canvas with
  populated ``(x, y)``; it does not touch persistence.

Determinism contract: identical Canvas + algorithm + options (+ seed for
force-directed) produce identical positions. Algorithms sort by node id
for every tie-break and never depend on input ordering.

implementation note ships ``hierarchical`` (Sugiyama); implementation note ships
``force_directed`` (Fruchterman-Reingold).
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

from workbay_canvas_mcp.models import Canvas


class LayoutAlgorithm(str, Enum):
    HIERARCHICAL = "hierarchical"
    FORCE_DIRECTED = "force_directed"


class CyclePolicy(str, Enum):
    """Hierarchical-layout reaction to a cyclic graph.

    ``BREAK`` (default) reverses the lowest-id edge of each cycle for
    layout purposes only — the persisted Canvas edges are never mutated —
    and records a warning in the returned Canvas metadata. ``STRICT``
    refuses to lay out a cyclic graph and raises
    ``LayoutError(reason="cycle_policy_required")``.
    """

    BREAK = "break"
    STRICT = "strict"


class LayoutOptions(BaseModel):
    """Layout tunables and safety budget.

    Defaults are a constrained spacing scale rather than arbitrary tuning;
    consumers (SVG/PNG export, browser UI) scale these logical units at
    render time. ``seed``/``iterations`` are consumed by force-directed
    layout; hierarchical ignores them.
    """

    seed: int | None = None
    iterations: int = Field(default=100, ge=0)
    node_spacing: float = Field(default=120.0, gt=0.0)
    rank_spacing: float = Field(default=100.0, gt=0.0)
    respect_existing_positions: bool = False
    cycle_policy: CyclePolicy = CyclePolicy.BREAK
    # Safety budget — enforced before/within layout so a pathological
    # graph returns a structured error instead of blocking the MCP server.
    max_nodes: int = Field(default=1000, ge=0)
    max_edges: int = Field(default=4000, ge=0)
    max_seconds: float = Field(default=5.0, gt=0.0)


class LayoutError(Exception):
    """Structured layout failure.

    ``reason`` is one of ``too_large`` (budget exceeded), ``timeout``
    (elapsed-time guard tripped), or ``cycle_policy_required`` (cyclic
    graph under ``CyclePolicy.STRICT``).
    """

    _REASONS = frozenset({"too_large", "timeout", "cycle_policy_required"})

    def __init__(self, reason: str, message: str) -> None:
        if reason not in self._REASONS:
            raise ValueError(f"unknown LayoutError reason: {reason!r}")
        super().__init__(message)
        self.reason = reason
        self.message = message


class LayoutWarning(BaseModel):
    """A non-fatal event emitted during layout (e.g. a broken cycle edge)."""

    kind: str
    edge_id: str | None = None
    detail: str = ""


def layout_canvas_pure(
    canvas: Canvas,
    algorithm: LayoutAlgorithm,
    options: LayoutOptions | None = None,
) -> Canvas:
    """Lay out ``canvas`` with ``algorithm`` and return a new Canvas.

    Pure read-modify-return: the input Canvas is not mutated and nothing
    is persisted. The returned Canvas carries populated ``(x, y)`` and a
    ``metadata["layout"]`` summary. Raises :class:`LayoutError` on budget
    or cycle-policy violations.
    """

    opts = options or LayoutOptions()
    if algorithm is LayoutAlgorithm.HIERARCHICAL:
        from workbay_canvas_mcp.layout.hierarchical import layout_hierarchical

        return layout_hierarchical(canvas, opts)
    if algorithm is LayoutAlgorithm.FORCE_DIRECTED:
        from workbay_canvas_mcp.layout.force_directed import layout_force_directed

        return layout_force_directed(canvas, opts)
    raise ValueError(f"unsupported layout algorithm: {algorithm!r}")


__all__ = [
    "CyclePolicy",
    "LayoutAlgorithm",
    "LayoutError",
    "LayoutOptions",
    "LayoutWarning",
    "layout_canvas_pure",
]
