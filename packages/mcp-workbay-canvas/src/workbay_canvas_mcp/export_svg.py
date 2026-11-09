"""First-party SVG export for canvas-mcp.

internal deliverable. The Mermaid arm (implementation note) emits text;
this slice emits SVG markup directly from positioned Canvas nodes, with
no external rendering dependency. PNG (implementation note) reuses the same emitter
and converts the SVG bytes through an optional backend.

Layout contract: every :class:`~workbay_canvas_mcp.models.Node` must carry
non-None ``x`` and ``y``. When internal (deterministic-position source)
lands, ``render_task_as_canvas`` / ``render_lanes_as_canvas`` will fill
these in for callers. Until then, callers that have not assigned
positions get a structured
:class:`~workbay_canvas_mcp.export.ExportError(reason="layout_required")`
instead of an unstable, position-less rendering.

Determinism: nodes/edges are rendered in stable Canvas id order; numeric
coordinates are formatted via a single ``_fmt`` helper that drops
redundant decimals. The same Canvas always yields byte-identical SVG.

Per-NodeKind shape map (kept narrow so the visual language stays
learnable; mirrors the Mermaid implementation note totality guard):

- ``TASK`` → ``<rect>``, ``stroke-width=3`` (emphasised)
- ``SLICE`` → ``<rect>``, ``stroke-width=2``
- ``DECISION`` → ``<polygon>`` (diamond)
- ``FINDING`` → ``<polygon>`` (right-pointing pennant)
- ``LANE`` → ``<ellipse>``
- ``WORKER`` → stadium ``<rect>`` (``rx = height/2``)
- ``FREEFORM`` → ``<rect>`` with ``stroke-dasharray``
"""

from __future__ import annotations

from math import isfinite
from typing import Final

from workbay_canvas_mcp.export import ExportError
from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

SVG_CONTENT_TYPE: Final = "image/svg+xml"

# Neutral design tokens. Kept small per the Refactoring UI principle of
# reducing repeated minor choices through systems. Per-kind variation is
# expressed through shape and stroke-width, not bespoke colors.
_TOKENS: Final[dict[str, str]] = {
    "background": "#ffffff",
    "surface": "#f7f7f7",
    "ink": "#1a1a1a",
    "muted": "#666666",
    "stroke": "#cccccc",
}

_RADIUS: Final = 8
_NODE_PADDING: Final = 12
_FONT_SIZE: Final = 12
_EDGE_FONT_SIZE: Final = 10
_CHAR_PX: Final = 7  # rough sans-serif average at 12px font
_NODE_HEIGHT: Final = 36
_NODE_MIN_WIDTH: Final = 80
_VIEWBOX_MARGIN: Final = 20
_TASK_STROKE_WIDTH: Final = 3
_DEFAULT_STROKE_WIDTH: Final = 2
_EDGE_STROKE_WIDTH = "1.5"

# Stable English phrasing for labelled edge kinds. FREEFORM has no
# default phrase; an unlabelled FREEFORM edge renders only as the line
# with no <text> sibling.
_EDGE_PHRASES: Final[dict[EdgeKind, str]] = {
    EdgeKind.DEPENDS_ON: "depends on",
    EdgeKind.BLOCKS: "blocks",
    EdgeKind.DERIVES_FROM: "derives from",
    EdgeKind.ASSIGNED_TO: "assigned to",
    EdgeKind.PRODUCED: "produced",
}


def _fmt(value: float) -> str:
    """Format a coordinate compactly and deterministically.

    Integer-valued floats render without a decimal point so most outputs
    stay short and stable. Non-integer values render with up to 2 decimal
    places, trailing zeros stripped.
    """

    if value == int(value):
        return str(int(value))
    formatted = f"{value:.2f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _xml_escape(value: str) -> str:
    """Escape XML metacharacters for both text nodes and attribute values."""

    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _node_box(node: Node) -> tuple[float, float, float, float]:
    """Return ``(x_top_left, y_top_left, width, height)`` for the node.

    Width adapts to label length so longer labels do not overflow; the
    minimum keeps short labels visually consistent. Caller has already
    ensured ``node.x`` / ``node.y`` are non-None.
    """

    label_w = len(node.label) * _CHAR_PX
    width = max(_NODE_MIN_WIDTH, label_w + 2 * _NODE_PADDING)
    height = _NODE_HEIGHT
    assert node.x is not None and node.y is not None
    return (node.x - width / 2, node.y - height / 2, width, height)


def _compute_viewbox(canvas: Canvas) -> tuple[float, float, float, float]:
    if not canvas.nodes:
        return (0.0, 0.0, 0.0, 0.0)
    xs: list[float] = []
    ys: list[float] = []
    for node in canvas.nodes:
        x_tl, y_tl, w, h = _node_box(node)
        xs.extend([x_tl, x_tl + w])
        ys.extend([y_tl, y_tl + h])
    min_x = min(xs) - _VIEWBOX_MARGIN
    min_y = min(ys) - _VIEWBOX_MARGIN
    max_x = max(xs) + _VIEWBOX_MARGIN
    max_y = max(ys) + _VIEWBOX_MARGIN
    return (min_x, min_y, max_x - min_x, max_y - min_y)


def _has_renderable_layout(node: Node) -> bool:
    if node.x is None or node.y is None:
        return False
    return isfinite(node.x) and isfinite(node.y)


def _unrenderable_layout_node_ids(canvas: Canvas) -> list[str]:
    return sorted(n.id for n in canvas.nodes if not _has_renderable_layout(n))


def _text_element(*, x: float, y: float, label: str, font_size: int, fill: str) -> str:
    return (
        f'<text x="{_fmt(x)}" y="{_fmt(y)}" '
        f'text-anchor="middle" dominant-baseline="central" '
        f'font-family="sans-serif" font-size="{font_size}" '
        f'fill="{fill}">{_xml_escape(label)}</text>'
    )


def _render_node(node: Node) -> str:
    x_tl, y_tl, w, h = _node_box(node)
    assert node.x is not None and node.y is not None
    cx, cy = node.x, node.y
    klass = f"node node-{node.kind.value}"
    fill = _TOKENS["surface"]
    stroke = _TOKENS["stroke"]

    if node.kind is NodeKind.LANE:
        shape = (
            f'<ellipse class="{klass}" '
            f'cx="{_fmt(cx)}" cy="{_fmt(cy)}" '
            f'rx="{_fmt(w / 2)}" ry="{_fmt(h / 2)}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{_DEFAULT_STROKE_WIDTH}" />'
        )
    elif node.kind is NodeKind.DECISION:
        points = " ".join(
            f"{_fmt(px)},{_fmt(py)}"
            for px, py in (
                (cx, cy - h / 2),
                (cx + w / 2, cy),
                (cx, cy + h / 2),
                (cx - w / 2, cy),
            )
        )
        shape = (
            f'<polygon class="{klass}" points="{points}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{_DEFAULT_STROKE_WIDTH}" />'
        )
    elif node.kind is NodeKind.FINDING:
        # Right-pointing pentagon (pennant): rectangle with a triangular tip
        # on the right edge. The notch depth equals h/2 so the tip sits on
        # the node center line.
        notch = h / 2
        points = " ".join(
            f"{_fmt(px)},{_fmt(py)}"
            for px, py in (
                (x_tl, y_tl),
                (x_tl + w - notch, y_tl),
                (x_tl + w, cy),
                (x_tl + w - notch, y_tl + h),
                (x_tl, y_tl + h),
            )
        )
        shape = (
            f'<polygon class="{klass}" points="{points}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{_DEFAULT_STROKE_WIDTH}" />'
        )
    else:
        # Rect-family: TASK, SLICE, WORKER, FREEFORM.
        if node.kind is NodeKind.WORKER:
            rx = h / 2  # stadium silhouette
        else:
            rx = _RADIUS
        if node.kind is NodeKind.TASK:
            stroke_width = _TASK_STROKE_WIDTH
        else:
            stroke_width = _DEFAULT_STROKE_WIDTH
        extra = ' stroke-dasharray="4,2"' if node.kind is NodeKind.FREEFORM else ""
        shape = (
            f'<rect class="{klass}" '
            f'x="{_fmt(x_tl)}" y="{_fmt(y_tl)}" '
            f'width="{_fmt(w)}" height="{_fmt(h)}" rx="{_fmt(rx)}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"'
            f"{extra} />"
        )

    text = _text_element(x=cx, y=cy, label=node.label, font_size=_FONT_SIZE, fill=_TOKENS["ink"])
    return f"  {shape}\n  {text}"


def _render_edge(edge: Edge, node_map: dict[str, Node]) -> str:
    src = node_map[edge.source]
    dst = node_map[edge.target]
    assert src.x is not None and src.y is not None
    assert dst.x is not None and dst.y is not None
    label = edge.label if edge.label is not None else _EDGE_PHRASES.get(edge.kind)
    klass = f"edge edge-{edge.kind.value}"
    line = (
        f'<line class="{klass}" '
        f'x1="{_fmt(src.x)}" y1="{_fmt(src.y)}" '
        f'x2="{_fmt(dst.x)}" y2="{_fmt(dst.y)}" '
        f'stroke="{_TOKENS["muted"]}" stroke-width="{_EDGE_STROKE_WIDTH}" />'
    )
    if label is None:
        return f"  {line}"
    mid_x = (src.x + dst.x) / 2
    mid_y = (src.y + dst.y) / 2
    label_text = (
        f'<text class="edge-label edge-label-{edge.kind.value}" '
        f'x="{_fmt(mid_x)}" y="{_fmt(mid_y)}" '
        f'text-anchor="middle" dominant-baseline="central" '
        f'font-family="sans-serif" font-size="{_EDGE_FONT_SIZE}" '
        f'fill="{_TOKENS["muted"]}">{_xml_escape(label)}</text>'
    )
    return f"  {line}\n  {label_text}"


def to_svg(canvas: Canvas) -> str:
    """Render a Canvas as deterministic first-party SVG markup.

    All nodes must carry ``(x, y)`` coordinates. Any missing position
    raises :class:`ExportError` with ``reason="layout_required"`` and a
    detail string naming the offending node ids — callers fix the
    upstream canvas (or wait for the internal layout source) rather than
    consuming a position-less rendering.

    Output order: SVG root, background rect, edges (so nodes overdraw
    them), then nodes. Within each group, items are sorted by their
    stable Canvas id so the same Canvas always yields byte-identical
    output.
    """

    unplaced = _unrenderable_layout_node_ids(canvas)
    if unplaced:
        raise ExportError(
            reason="layout_required",
            detail=f"nodes missing or invalid (x, y): {', '.join(unplaced)}",
        )

    min_x, min_y, w, h = _compute_viewbox(canvas)
    lines: list[str] = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="{_fmt(min_x)} {_fmt(min_y)} {_fmt(w)} {_fmt(h)}" '
            f'width="{_fmt(w)}" height="{_fmt(h)}">'
        ),
        (
            f'  <rect class="canvas-background" '
            f'x="{_fmt(min_x)}" y="{_fmt(min_y)}" '
            f'width="{_fmt(w)}" height="{_fmt(h)}" '
            f'fill="{_TOKENS["background"]}" />'
        ),
    ]

    node_map = {n.id: n for n in canvas.nodes}
    for edge in sorted(canvas.edges, key=lambda e: e.id):
        lines.append(_render_edge(edge, node_map))
    for node in sorted(canvas.nodes, key=lambda n: n.id):
        lines.append(_render_node(node))

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


__all__ = [
    "SVG_CONTENT_TYPE",
    "to_svg",
]
