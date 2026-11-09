"""Force-directed (Fruchterman-Reingold) layout for canvas-mcp.

Pure-Python FR. Nodes repel each other (``f_r = k^2 / d``) while edges
pull their endpoints together (``f_a = d^2 / k``); a per-iteration
temperature caps how far any node may move and cools linearly to ~0, so
the layout stops moving. Cycles need no special handling — the forces
are symmetric — so ``cycle_policy`` is ignored.

Determinism contract: initial placement draws from a local
``random.Random(seed)`` (``seed`` defaults to 0 so omitting it stays
reproducible), every node/edge sweep iterates in sorted-id order, and
coincident nodes separate along a fixed id-ordered offset. Identical
Canvas + options therefore yield identical positions on a given platform.
``x``/``y`` are unit-less floats inside a square frame sized from
``node_spacing`` and the node count.
"""

from __future__ import annotations

import math
import random
import time

from workbay_canvas_mcp.layout import LayoutError, LayoutOptions
from workbay_canvas_mcp.models import Canvas, Node

# Distance below which two nodes are treated as coincident and nudged
# apart along a deterministic id-ordered offset (avoids divide-by-zero).
_EPSILON = 1e-9
_SEPARATION = 1e-3


def layout_force_directed(canvas: Canvas, options: LayoutOptions) -> Canvas:
    """Return a new Canvas with force-directed ``(x, y)`` positions populated."""

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

    node_ids = sorted(n.id for n in canvas.nodes)
    n = len(node_ids)
    seed = options.seed if options.seed is not None else 0
    iterations = max(options.iterations, 0)

    if n == 0:
        return _positioned_canvas(canvas, {}, seed=seed, iterations=0, max_disp=0.0)

    rng = random.Random(seed)
    side = options.node_spacing * math.sqrt(n)
    k = options.node_spacing  # ideal edge length

    pinned: set[str] = set()
    pos: dict[str, list[float]] = {}
    for node_id in node_ids:  # sorted order keeps rng draws stable
        existing = _existing_position(canvas, node_id)
        if options.respect_existing_positions and existing is not None:
            pos[node_id] = [existing[0], existing[1]]
            pinned.add(node_id)
        else:
            pos[node_id] = [rng.uniform(0.0, side), rng.uniform(0.0, side)]

    edges = sorted((e.source, e.target) for e in canvas.edges if e.source != e.target)

    t0 = side / 10.0 if side > 0 else 1.0
    started = time.monotonic()
    max_disp = 0.0
    for i in range(iterations):
        if time.monotonic() - started > options.max_seconds:
            raise LayoutError("timeout", "force-directed layout exceeded max_seconds")

        disp: dict[str, list[float]] = {node_id: [0.0, 0.0] for node_id in node_ids}

        for a in node_ids:
            ax, ay = pos[a]
            for b in node_ids:
                if a == b:
                    continue
                dx, dy, dist = _delta(ax, ay, pos[b][0], pos[b][1], a, b)
                force = (k * k) / dist
                disp[a][0] += (dx / dist) * force
                disp[a][1] += (dy / dist) * force

        for u, v in edges:
            dx, dy, dist = _delta(pos[u][0], pos[u][1], pos[v][0], pos[v][1], u, v)
            force = (dist * dist) / k
            ox = (dx / dist) * force
            oy = (dy / dist) * force
            disp[u][0] -= ox
            disp[u][1] -= oy
            disp[v][0] += ox
            disp[v][1] += oy

        t = t0 * (1.0 - i / iterations)  # linear cool: full at i=0, ~0 at the end
        max_disp = 0.0
        for node_id in node_ids:
            if node_id in pinned:
                continue
            dx, dy = disp[node_id]
            d = math.hypot(dx, dy)
            if d > _EPSILON:
                step = min(d, t)
                pos[node_id][0] += (dx / d) * step
                pos[node_id][1] += (dy / d) * step
                max_disp = max(max_disp, step)
            pos[node_id][0] = min(side, max(0.0, pos[node_id][0]))
            pos[node_id][1] = min(side, max(0.0, pos[node_id][1]))

    return _positioned_canvas(canvas, pos, seed=seed, iterations=iterations, max_disp=max_disp)


def _existing_position(canvas: Canvas, node_id: str) -> tuple[float, float] | None:
    for node in canvas.nodes:
        if node.id == node_id and node.x is not None and node.y is not None:
            return (node.x, node.y)
    return None


def _delta(ax: float, ay: float, bx: float, by: float, a_id: str, b_id: str) -> tuple[float, float, float]:
    """Vector a->b's components and length, with deterministic anti-overlap.

    Coincident nodes (distance below ``_EPSILON``) are pushed apart along
    a fixed offset whose sign follows the nodes' id order, so the result
    is identical on every run.
    """

    dx = ax - bx
    dy = ay - by
    dist = math.hypot(dx, dy)
    if dist < _EPSILON:
        dx = _SEPARATION if a_id > b_id else -_SEPARATION
        dy = _SEPARATION
        dist = math.hypot(dx, dy)
    return dx, dy, dist


def _positioned_canvas(
    canvas: Canvas,
    pos: dict[str, list[float]],
    *,
    seed: int,
    iterations: int,
    max_disp: float,
) -> Canvas:
    positioned: list[Node] = []
    for node in canvas.nodes:
        if node.id in pos:
            x, y = pos[node.id][0], pos[node.id][1]
            positioned.append(node.model_copy(update={"x": x, "y": y}))
        else:
            positioned.append(node.model_copy())

    return Canvas(
        canvas_ref=canvas.canvas_ref,
        nodes=positioned,
        edges=list(canvas.edges),
        metadata={
            "layout": {
                "algorithm": "force_directed",
                "seed": seed,
                "iterations": iterations,
                "max_displacement": max_disp,
            }
        },
    )
