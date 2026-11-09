"""canvas-mcp — shared visual/spatial planning surface (MCP server).

Slice-2 surface: typed :class:`Canvas` / :class:`Node` / :class:`Edge`
models (with :class:`NodeKind` / :class:`EdgeKind` enums), a SQLite-backed
:class:`CanvasStore`, and a :func:`build_server` factory that registers
``upsert_canvas`` and ``get_canvas`` MCP tools. Handoff ingest
(``render_task_as_canvas``) arrives in implementation note.
"""

from __future__ import annotations

from workbay_canvas_mcp.annotations import (
    Annotation,
    AnnotationError,
    ThreadEntry,
    reconstruct_thread,
)
from workbay_canvas_mcp.component_roster import (
    ComponentRosterReceipt,
    ComponentSpec,
    SlotSpec,
)
from workbay_canvas_mcp.design_document import (
    DesignDecision,
    DesignDocument,
    DesignDocumentReceipt,
    append_decision,
    apply_patch,
    render_json,
    render_markdown,
)
from workbay_canvas_mcp.export import (
    ExportError,
    ExportFormat,
    ExportResult,
    to_mermaid,
)
from workbay_canvas_mcp.export_png import to_png
from workbay_canvas_mcp.export_svg import to_svg
from workbay_canvas_mcp.models import Canvas, CanvasSpec, Edge, EdgeKind, Node, NodeKind
from workbay_canvas_mcp.server import (
    DB_ENV_VAR,
    WORKSPACE_DB_RELPATH,
    build_server,
    resolve_db_path,
)
from workbay_canvas_mcp.store import CanvasStore
from workbay_canvas_mcp.templates import (
    TemplateError,
    TemplateSpec,
    build_template_canvas,
    list_templates,
)

__all__ = [
    "DB_ENV_VAR",
    "WORKSPACE_DB_RELPATH",
    "Annotation",
    "AnnotationError",
    "Canvas",
    "CanvasSpec",
    "CanvasStore",
    "ComponentRosterReceipt",
    "ComponentSpec",
    "DesignDecision",
    "DesignDocument",
    "DesignDocumentReceipt",
    "Edge",
    "EdgeKind",
    "ExportError",
    "ExportFormat",
    "ExportResult",
    "Node",
    "NodeKind",
    "SlotSpec",
    "TemplateError",
    "TemplateSpec",
    "ThreadEntry",
    "append_decision",
    "apply_patch",
    "build_server",
    "build_template_canvas",
    "list_templates",
    "reconstruct_thread",
    "render_json",
    "render_markdown",
    "resolve_db_path",
    "to_mermaid",
    "to_png",
    "to_svg",
]
