"""Shared canvas operations for MCP tools and the browser HTTP API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workbay_canvas_mcp.annotations import Annotation
from workbay_canvas_mcp.component_roster import ComponentRosterReceipt, ComponentSpec
from workbay_canvas_mcp.design_document import (
    DesignDocument,
    apply_patch,
)
from workbay_canvas_mcp.export import (
    ExportError,
    ExportFormat,
    ExportResult,
    coerce_export_format,
    to_mermaid,
)
from workbay_canvas_mcp.export_png import PNG_CONTENT_TYPE, SvgToPngConverter, to_png
from workbay_canvas_mcp.export_svg import SVG_CONTENT_TYPE, to_svg
from workbay_canvas_mcp.handoff_ingest import HandoffReader, build_canvas_from_handoff
from workbay_canvas_mcp.lane_ingest import LaneReader, build_canvas_from_lanes
from workbay_canvas_mcp.layout import LayoutAlgorithm, LayoutOptions, layout_canvas_pure
from workbay_canvas_mcp.models import Canvas, CanvasSpec
from workbay_canvas_mcp.reconcile import (
    LaneLookupReader,
    ReconciliationReport,
    RefLookupReader,
    TaskRefReader,
    reconcile_canvas_pure,
)
from workbay_canvas_mcp.reconcile_apply import ReconciliationPlan, apply_reconciliation_pure
from workbay_canvas_mcp.store import CanvasStore
from workbay_canvas_mcp.templates import (
    TemplateSpec,
    build_template_canvas,
    run_template_seed,
)
from workbay_canvas_mcp.templates import (
    list_templates as registry_list_templates,
)

if TYPE_CHECKING:
    from workbay_canvas_mcp.review_service import ReviewService

ReconcileReaders = tuple[TaskRefReader, RefLookupReader, RefLookupReader, LaneLookupReader]


class StaleDraftError(Exception):
    """Raised when expected_updated_at does not match the stored canvas."""

    def __init__(self, *, expected: str | None, actual: str | None) -> None:
        super().__init__(f"stale draft: expected {expected!r}, actual {actual!r}")
        self.expected = expected
        self.actual = actual


@dataclass(frozen=True)
class WorkspaceInfo:
    workspace_root: str
    db_path: str
    status: str


@dataclass
class CanvasService:
    store: CanvasStore
    workspace_root: Path
    db_path: Path
    handoff_reader: HandoffReader | None = None
    lane_reader: LaneReader | None = None
    reconcile_readers: ReconcileReaders | None = None
    png_converter: SvgToPngConverter | None = None
    review_service: ReviewService | None = None

    @classmethod
    def build(
        cls,
        *,
        db_path: str | Path | None = None,
        workspace_root: str | Path | None = None,
        handoff_reader: HandoffReader | None = None,
        lane_reader: LaneReader | None = None,
        reconcile_readers: ReconcileReaders | None = None,
        png_converter: SvgToPngConverter | None = None,
    ) -> CanvasService:
        from workbay_canvas_mcp.server import resolve_db_path

        resolved_root = Path(workspace_root) if workspace_root is not None else Path.cwd()
        resolved_db = resolve_db_path(db_path=db_path, workspace_root=resolved_root)
        return cls(
            store=CanvasStore(resolved_db),
            workspace_root=resolved_root.resolve(),
            db_path=resolved_db.resolve(),
            handoff_reader=handoff_reader,
            lane_reader=lane_reader,
            reconcile_readers=reconcile_readers,
            png_converter=png_converter,
        )

    def workspace_info(self) -> WorkspaceInfo:
        parent = self.db_path.parent
        if not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                status = "unavailable"
            else:
                status = "missing" if not self.db_path.exists() else "ok"
        elif not parent.is_dir() or not os.access(parent, os.W_OK):
            status = "unavailable"
        elif not self.db_path.exists():
            status = "missing"
        else:
            status = "ok"
        return WorkspaceInfo(
            workspace_root=str(self.workspace_root),
            db_path=str(self.db_path),
            status=status,
        )

    def list_canvases(self) -> list[CanvasSpec]:
        return self.store.list_canvas_specs()

    def get_canvas(self, canvas_ref: str) -> Canvas | None:
        return self.store.get_canvas(canvas_ref)

    def upsert_canvas(self, canvas: Canvas, *, expected_updated_at: str | None = None) -> Canvas:
        return self.store.upsert_canvas(canvas, expected_updated_at=expected_updated_at)

    def append_annotation(
        self,
        *,
        canvas_ref: str,
        node_id: str,
        author: str,
        body: str,
        parent_annotation_id: str | None = None,
    ) -> Annotation:
        return self.store.append_annotation(
            canvas_ref=canvas_ref,
            node_id=node_id,
            author=author,
            body=body,
            parent_annotation_id=parent_annotation_id,
        )

    def list_annotations(
        self,
        *,
        canvas_ref: str,
        node_id: str | None = None,
        include_detached: bool = True,
    ) -> list[Annotation]:
        return self.store.list_annotations(
            canvas_ref=canvas_ref,
            node_id=node_id,
            include_detached=include_detached,
        )

    def reconcile_canvas(self, canvas_ref: str) -> ReconciliationReport:
        if self.reconcile_readers is None:
            raise ValueError("reconcile_canvas requires reconcile_readers")
        canvas = self.store.get_canvas(canvas_ref)
        if canvas is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        task_ref_reader, decision_reader, finding_reader, lane_lookup = self.reconcile_readers
        return reconcile_canvas_pure(
            canvas,
            task_ref_reader=task_ref_reader,
            decision_reader=decision_reader,
            finding_reader=finding_reader,
            lane_reader=lane_lookup,
        )

    def apply_reconciliation(self, canvas_ref: str, plan: ReconciliationPlan) -> Canvas:
        canvas = self.store.get_canvas(canvas_ref)
        if canvas is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        new_canvas = apply_reconciliation_pure(canvas, plan)
        return self.store.upsert_canvas(new_canvas)

    def get_design_document(self, design_ref: str) -> DesignDocument:
        return self.store.get_design_document(design_ref=design_ref) or DesignDocument(design_ref=design_ref)

    def patch_design_document(self, design_ref: str, patch: dict[str, Any]) -> DesignDocument:
        document = self.get_design_document(design_ref)
        updated = apply_patch(document, patch)
        self.store.upsert_design_document(updated)
        return updated

    def list_components(self, design_ref: str) -> list[ComponentSpec]:
        return self.store.list_components(design_ref=design_ref)

    def upsert_component(self, design_ref: str, component: dict[str, Any]) -> ComponentRosterReceipt:
        spec = ComponentSpec.model_validate(component)
        self.store.upsert_component(design_ref=design_ref, component=spec)
        return ComponentRosterReceipt(
            operation="upsert",
            design_ref=design_ref,
            component_id=spec.component_id,
            components=self.store.list_components(design_ref=design_ref),
        )

    def archive_component(self, design_ref: str, component_id: str) -> ComponentRosterReceipt:
        self.store.archive_component(design_ref=design_ref, component_id=component_id)
        return ComponentRosterReceipt(
            operation="archive",
            design_ref=design_ref,
            component_id=component_id,
            components=self.store.list_components(design_ref=design_ref),
        )

    def export_canvas(self, canvas_ref: str, format: ExportFormat | str) -> ExportResult:
        canvas = self.store.get_canvas(canvas_ref)
        if canvas is None:
            raise ExportError(reason="canvas_not_found", detail=canvas_ref)
        selected_format = coerce_export_format(format)
        if selected_format is ExportFormat.MERMAID:
            return ExportResult(
                format=ExportFormat.MERMAID,
                content_text=to_mermaid(canvas),
                content_bytes=None,
                content_type="text/vnd.mermaid",
            )
        if selected_format is ExportFormat.SVG:
            return ExportResult(
                format=ExportFormat.SVG,
                content_text=to_svg(canvas),
                content_bytes=None,
                content_type=SVG_CONTENT_TYPE,
            )
        if selected_format is ExportFormat.PNG:
            return ExportResult(
                format=ExportFormat.PNG,
                content_text=None,
                content_bytes=to_png(canvas, converter=self.png_converter),
                content_type=PNG_CONTENT_TYPE,
            )
        raise ExportError(reason="unsupported_format", detail=str(format))

    # --- MCP-only tools (kept on service for parity, not exposed on browser API) ---

    def instantiate_template(
        self,
        template_id: str,
        input: dict[str, Any],
        canvas_ref_override: str | None = None,
        seed: bool = False,
    ) -> Canvas:
        canvas = build_template_canvas(template_id, input, canvas_ref_override)
        persisted = self.store.upsert_canvas(canvas)
        if seed:
            run_template_seed(template_id, input, self.store, persisted)
        return persisted

    def list_templates(self) -> list[TemplateSpec]:
        return registry_list_templates()

    def render_task_as_canvas(self, task_ref: str) -> Canvas:
        reader = self.handoff_reader or _empty_handoff_reader
        canvas = build_canvas_from_handoff(reader=reader, task_ref=task_ref)
        return self.store.upsert_canvas(canvas)

    def render_lanes_as_canvas(self, task_ref: str, lane_id: str | None = None) -> Canvas:
        reader = self.lane_reader or _empty_lane_reader
        canvas = build_canvas_from_lanes(reader=reader, task_ref=task_ref, lane_id=lane_id)
        return self.store.upsert_canvas(canvas)

    def layout_canvas(
        self,
        canvas_ref: str,
        algorithm: LayoutAlgorithm,
        options: LayoutOptions | None = None,
    ) -> Canvas:
        current = self.store.get_canvas(canvas_ref)
        if current is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        laid_out = layout_canvas_pure(current, algorithm, options)
        self.store.upsert_canvas(laid_out)
        return laid_out


def _empty_handoff_reader(task_ref: str) -> dict[str, object]:
    return {"task_ref": task_ref, "decisions": []}


def _empty_lane_reader(task_ref: str, lane_id: str | None = None) -> dict[str, object]:
    return {"task_ref": task_ref, "lanes": []}
