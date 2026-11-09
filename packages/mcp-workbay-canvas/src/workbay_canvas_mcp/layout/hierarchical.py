"""Hierarchical (Sugiyama-style) layout for canvas-mcp DAGs.

Top-down layered layout. Canvas edges point child -> parent
(``DERIVES_FROM`` source=slice/decision target=task; ``DEPENDS_ON``
newer -> older), so the layout ranks along *reversed* edges: an edge's
target is the parent and is drawn one rank above its source. ``y`` grows
downward with rank; ``x`` grows with a node's order index inside its rank.

Pipeline: reverse edges -> break/refuse cycles -> longest-path rank from
roots -> within-rank ordering by the median of parent x-indices -> grid
coordinates. Every tie-break sorts by node id, so identical input yields
identical output.
"""

from __future__ import annotations

import time
from bisect import insort
from collections import defaultdict

from workbay_canvas_mcp.layout import (
    CyclePolicy,
    LayoutError,
    LayoutOptions,
    LayoutWarning,
)
from workbay_canvas_mcp.layout.geometry import median
from workbay_canvas_mcp.models import Canvas, Node

# DFS coloring states for cycle detection.
_WHITE, _GRAY, _BLACK = 0, 1, 2


def layout_hierarchical(canvas: Canvas, options: LayoutOptions) -> Canvas:
    """Return a new Canvas with hierarchical ``(x, y)`` positions populated."""

    if len(canvas.nodes) > options.max_nodes:
        raise LayoutError(
            "too_large",
            f"{len(canvas.nodes)} nodes exceeds max_nodes={options.max_nodes}",
        )
    if len(canvas.edges) > options.max_edges:
        raise LayoutError(
            "too_large",
            f"{len(canvas.edges)} edges exceeds max_edges={options.max_edges}",
        )

    started = time.monotonic()
    node_ids = sorted(n.id for n in canvas.nodes)

    children, display_edge_ids = _build_display_graph(canvas)

    warnings: list[LayoutWarning] = []
    if _find_cycle_edges(children, node_ids) is not None:
        if options.cycle_policy is CyclePolicy.STRICT:
            raise LayoutError(
                "cycle_policy_required",
                "canvas contains a cycle; set cycle_policy=break to lay it out",
            )
        warnings = _break_cycles(children, display_edge_ids, node_ids)

    rank = _rank_nodes(children, node_ids)
    order_index = _order_within_ranks(children, rank, node_ids)

    if time.monotonic() - started > options.max_seconds:  # pragma: no cover - guard
        raise LayoutError("timeout", "hierarchical layout exceeded max_seconds")

    pinned: dict[str, tuple[float, float]] = {}
    if options.respect_existing_positions:
        pinned = {n.id: (n.x, n.y) for n in canvas.nodes if n.x is not None and n.y is not None}

    positioned: list[Node] = []
    for node in canvas.nodes:
        if node.id in pinned:
            x, y = pinned[node.id]
        else:
            x = order_index[node.id] * options.node_spacing
            y = rank[node.id] * options.rank_spacing
        positioned.append(node.model_copy(update={"x": x, "y": y}))

    return Canvas(
        canvas_ref=canvas.canvas_ref,
        nodes=positioned,
        edges=list(canvas.edges),
        metadata={
            "layout": {
                "algorithm": "hierarchical",
                "warnings": [w.model_dump() for w in warnings],
            }
        },
    )


def _build_display_graph(
    canvas: Canvas,
) -> tuple[dict[str, set[str]], dict[tuple[str, str], list[str]]]:
    """Reverse canvas edges into a parent -> children display adjacency.

    Returns ``(children, edge_ids)`` where ``children[parent]`` is the set
    of child node ids and ``edge_ids[(parent, child)]`` lists the canvas
    edge ids that produced that display edge (so cycle-breaking can name
    the lowest-id edge it reversed).
    """

    children: dict[str, set[str]] = defaultdict(set)
    edge_ids: dict[tuple[str, str], list[str]] = defaultdict(list)
    for edge in canvas.edges:
        parent, child = edge.target, edge.source  # reversed
        if parent == child:
            continue  # self-loop carries no hierarchy
        children[parent].add(child)
        edge_ids[(parent, child)].append(edge.id)
    return children, edge_ids


def _find_cycle_edges(children: dict[str, set[str]], node_ids: list[str]) -> list[tuple[str, str]] | None:
    """Return one cycle's display edges via DFS, or ``None`` if acyclic.

    Deterministic: roots and neighbours are visited in sorted id order, so
    the same graph always surfaces the same cycle first.
    """

    color = {n: _WHITE for n in node_ids}
    for root in node_ids:
        if color[root] != _WHITE:
            continue
        stack: list[tuple[str, list[str]]] = [(root, sorted(children.get(root, ())))]
        color[root] = _GRAY
        while stack:
            parent, pending = stack[-1]
            if not pending:
                color[parent] = _BLACK
                stack.pop()
                continue
            child = pending.pop(0)
            if color[child] == _GRAY:
                path = [node for node, _ in stack]
                cycle_start = path.index(child)
                cycle_nodes = path[cycle_start:] + [child]
                return [(cycle_nodes[i], cycle_nodes[i + 1]) for i in range(len(cycle_nodes) - 1)]
            if color[child] == _WHITE:
                color[child] = _GRAY
                stack.append((child, sorted(children.get(child, ()))))
    return None


def _break_cycles(
    children: dict[str, set[str]],
    edge_ids: dict[tuple[str, str], list[str]],
    node_ids: list[str],
) -> list[LayoutWarning]:
    """Remove back edges until the display graph is acyclic.

    Each removal reverses one display edge for layout purposes only (the
    persisted Canvas is untouched) and records the lowest canvas edge id
    that produced it.
    """

    warnings: list[LayoutWarning] = []
    while True:
        cycle = _find_cycle_edges(children, node_ids)
        if cycle is None:
            return warnings
        parent, child = _lowest_cycle_edge(cycle, edge_ids)
        children[parent].discard(child)
        canvas_edge_id = min(edge_ids.get((parent, child), [""]))
        warnings.append(
            LayoutWarning(
                kind="cycle_broken",
                edge_id=canvas_edge_id or None,
                detail=f"reversed edge {child} -> {parent} for layout",
            )
        )


def _lowest_cycle_edge(
    cycle_edges: list[tuple[str, str]],
    edge_ids: dict[tuple[str, str], list[str]],
) -> tuple[str, str]:
    """Choose the cycle edge backed by the lowest Canvas edge id."""

    return min(
        cycle_edges,
        key=lambda edge: (
            min(edge_ids.get(edge, [""])),
            edge[0],
            edge[1],
        ),
    )


def _rank_nodes(children: dict[str, set[str]], node_ids: list[str]) -> dict[str, int]:
    """Longest-path rank from roots via Kahn's algorithm (sorted, stable)."""

    indegree = {n: 0 for n in node_ids}
    for parent in node_ids:
        for child in children.get(parent, ()):
            indegree[child] += 1

    rank = {n: 0 for n in node_ids}
    ready: list[str] = sorted(n for n in node_ids if indegree[n] == 0)
    while ready:
        node = ready.pop(0)
        for child in sorted(children.get(node, ())):
            if rank[node] + 1 > rank[child]:
                rank[child] = rank[node] + 1
            indegree[child] -= 1
            if indegree[child] == 0:
                insort(ready, child)
    return rank


def _order_within_ranks(
    children: dict[str, set[str]],
    rank: dict[str, int],
    node_ids: list[str],
) -> dict[str, int]:
    """Assign each node its order index inside its rank.

    Rank 0 is ordered by node id. Deeper ranks order by the median of
    their parents' already-assigned indices (a single ascending sweep —
    parents always sit in a strictly lower rank), tie-broken by node id.
    """

    parents: dict[str, list[str]] = defaultdict(list)
    for parent in node_ids:
        for child in children.get(parent, ()):
            parents[child].append(parent)

    by_rank: dict[int, list[str]] = defaultdict(list)
    for node in node_ids:
        by_rank[rank[node]].append(node)

    order_index: dict[str, int] = {}
    for r in sorted(by_rank):
        nodes = by_rank[r]
        if r == 0:
            ordered = sorted(nodes)
        else:
            ordered = sorted(
                nodes,
                key=lambda n: (median([order_index[p] for p in parents[n]]), n),
            )
        for index, node in enumerate(ordered):
            order_index[node] = index
    return order_index
