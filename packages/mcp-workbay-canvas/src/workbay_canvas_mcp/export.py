"""Diagram export for canvas-mcp.

internal (Mermaid) deliverable. SVG and PNG arms land in
Slices 2 and 3; the surface (``ExportFormat``, ``ExportResult``,
``ExportError``) is final so later slices only add emitters, never
re-shape the contract.

Public surface:

- :class:`ExportFormat` — enum of supported wire formats. Values are
  lowercase strings so MCP callers can pass either the enum or the raw
  string.
- :class:`ExportResult` — typed envelope returned by the ``export_canvas``
  tool. Exactly one of ``content_text`` / ``content_bytes`` is populated
  per format.
- :class:`ExportError` — structured error with a stable ``reason`` field
  so callers can branch on the failure mode without parsing prose.
- :func:`to_mermaid` — pure ``(canvas) -> str`` emitter that produces a
  deterministic Mermaid ``flowchart TD`` document.

Deterministic emission keeps the export safe for snapshot testing and
golden files: nodes and edges are sorted by canvas id before rendering,
so insertion order in the Canvas does not affect the output.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from workbay_canvas_mcp.models import Canvas, Edge, EdgeKind, Node, NodeKind

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")


class ExportFormat(str, Enum):
    """Supported export wire formats."""

    MERMAID = "mermaid"
    SVG = "svg"
    PNG = "png"


class ExportResult(BaseModel):
    """Envelope returned by the ``export_canvas`` tool.

    Exactly one of ``content_text`` / ``content_bytes`` is populated:
    Mermaid and SVG use ``content_text``; PNG uses ``content_bytes``.
    The ``content_type`` is the canonical MIME type for the format so
    HTTP / MCP transports can echo it without re-deriving from the enum.
    """

    # Bytes payloads (PNG, future image formats) are not valid UTF-8, so the
    # default Pydantic JSON encoder (ser_json_bytes='utf8') would raise on
    # serialisation through FastMCP's transport. Switching to base64 keeps
    # the wire format MCP-safe while round-tripping through `bytes` in-process.
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")

    format: ExportFormat
    content_text: str | None = None
    content_bytes: bytes | None = None
    content_type: str

    @model_validator(mode="after")
    def _exactly_one_payload(self) -> Self:
        has_text = self.content_text is not None
        has_bytes = self.content_bytes is not None
        if has_text == has_bytes:
            raise ValueError(
                "ExportResult must populate exactly one of content_text or content_bytes; "
                f"got content_text={'set' if has_text else 'unset'}, "
                f"content_bytes={'set' if has_bytes else 'unset'}."
            )
        return self


def coerce_export_format(format: ExportFormat | str) -> ExportFormat:
    if isinstance(format, ExportFormat):
        return format
    try:
        return ExportFormat(format)
    except ValueError as exc:
        raise ExportError(reason="unsupported_format", detail=str(format)) from exc


class ExportError(Exception):
    """Structured export failure.

    ``reason`` is a stable, machine-readable token (for example
    ``"canvas_not_found"``, ``"layout_required"``, ``"missing_dependency"``,
    ``"unsupported_format"``) that MCP callers can branch on.
    ``detail`` is an optional human-readable supplement.
    """

    def __init__(self, *, reason: str, detail: str | None = None) -> None:
        self.reason = reason
        self.detail = detail
        message = reason if detail is None else f"{reason}: {detail}"
        super().__init__(message)


# ---------------------------------------------------------------------------
# Mermaid emitter
# ---------------------------------------------------------------------------

_MERMAID_CONTENT_TYPE = "text/vnd.mermaid"

# Shape map. Each entry is (open, close) bracket pair wrapping a quoted
# label. NodeKind values without an explicit entry fall back to the
# rectangle shape and the canvas remains valid (PR-internal default).
_NODE_SHAPES: dict[NodeKind, tuple[str, str]] = {
    NodeKind.TASK: ("[[", "]]"),
    NodeKind.SLICE: ("[", "]"),
    NodeKind.DECISION: ("{", "}"),
    NodeKind.FINDING: (">", "]"),
    NodeKind.LANE: ("((", "))"),
    NodeKind.WORKER: ("[(", ")]"),
    NodeKind.FREEFORM: ("(", ")"),
}

_DEFAULT_SHAPE: tuple[str, str] = ("[", "]")

# Stable English phrasing for labelled edge kinds. FREEFORM has no
# default phrase — an unlabelled FREEFORM edge renders as a plain
# ``-->`` arrow.
_EDGE_PHRASES: dict[EdgeKind, str] = {
    EdgeKind.DEPENDS_ON: "depends on",
    EdgeKind.BLOCKS: "blocks",
    EdgeKind.DERIVES_FROM: "derives from",
    EdgeKind.ASSIGNED_TO: "assigned to",
    EdgeKind.PRODUCED: "produced",
}


def _sanitize_id(raw_id: str) -> str:
    """Turn an arbitrary Canvas node id into a Mermaid-safe identifier.

    Mermaid identifiers must match ``[A-Za-z0-9_]+`` and cannot collide
    with reserved tokens (``graph``, ``end``, …). Any non-alphanumeric
    character becomes ``_``; the result is prefixed with ``n_`` so a
    numeric-leading or reserved-word id still renders cleanly and so
    sanitized ids never overlap with literal Mermaid keywords.
    """

    sanitized = "".join(ch if ch.isascii() and ch.isalnum() else "_" for ch in raw_id)
    return f"n_{sanitized}"


def _build_mermaid_id_map(nodes: list[Node]) -> dict[str, str]:
    """Build collision-free Mermaid ids keyed by raw Canvas node id."""

    mapping: dict[str, str] = {}
    used: set[str] = set()
    for node in sorted(nodes, key=lambda n: n.id):
        base = _sanitize_id(node.id)
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        mapping[node.id] = candidate
        used.add(candidate)
    return mapping


def _escape_label(label: str) -> str:
    """Escape a node/edge label so Mermaid keeps it as one quoted token.

    Mermaid's recommended escape for embedded ``"`` inside a quoted
    label is the HTML entity ``&quot;``. Brackets and braces are safe
    inside the quoted wrapper, so we do not touch them.

    CR/LF/tab and other C0 control characters are collapsed to a single
    space first so a newline cannot close the quoted label and inject
    graph syntax.
    """

    label = _CONTROL_RE.sub(" ", label)
    return label.replace("&", "&amp;").replace('"', "&quot;").replace("|", "&#124;")


def _render_node(node: Node, id_map: dict[str, str]) -> str:
    open_, close = _NODE_SHAPES.get(node.kind, _DEFAULT_SHAPE)
    return f'    {id_map[node.id]}{open_}"{_escape_label(node.label)}"{close}'


def _render_edge(edge: Edge, id_map: dict[str, str]) -> str:
    src = id_map[edge.source]
    dst = id_map[edge.target]
    label = edge.label if edge.label is not None else _EDGE_PHRASES.get(edge.kind)
    if label is None:
        return f"    {src} --> {dst}"
    return f"    {src} -->|{_escape_label(label)}| {dst}"


def to_mermaid(canvas: Canvas) -> str:
    """Render a Canvas as a deterministic Mermaid ``flowchart TD`` string.

    Output order: header, then nodes sorted by ``Node.id``, then edges
    sorted by ``Edge.id``. Sorting by stable canvas-level ids (not the
    Mermaid-sanitized form) keeps the golden file stable even if the
    sanitization rules evolve.
    """

    lines: list[str] = ["flowchart TD"]
    id_map = _build_mermaid_id_map(canvas.nodes)
    for node in sorted(canvas.nodes, key=lambda n: n.id):
        lines.append(_render_node(node, id_map))
    for edge in sorted(canvas.edges, key=lambda e: e.id):
        lines.append(_render_edge(edge, id_map))
    if len(lines) == 1:
        # Empty canvas: drop the trailing newline so the strip()-equality
        # check in the test reads as the bare header.
        return lines[0] + "\n"
    return "\n".join(lines) + "\n"


__all__ = [
    "ExportError",
    "ExportFormat",
    "ExportResult",
    "to_mermaid",
]
