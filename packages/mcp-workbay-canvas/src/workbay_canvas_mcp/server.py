"""FastMCP server registration for the canvas MCP server.

Registers the public tool surface backed by a :class:`CanvasStore`:

- ``upsert_canvas(canvas)`` — full-replace write of a Canvas keyed by
  ``canvas_ref``. Returns the persisted Canvas.
- ``get_canvas(canvas_ref)`` — read a Canvas by ref, ``None`` if absent.
- ``list_canvases()`` — read lightweight per-canvas summaries.
- ``instantiate_template(template_id, input, canvas_ref_override=None, seed=False)`` —
  render a registered template to a Canvas, persist it, and optionally
  seed durable design-system state.
- ``list_templates()`` — list the registered template specs.
- ``render_task_as_canvas(task_ref)`` — read handoff state via the
  injected reader, render it via :func:`build_canvas_from_handoff`,
  persist the result at ``canvas_ref=f"task:{task_ref}"``, and return
  the Canvas.
- ``render_lanes_as_canvas(task_ref, lane_id=None)`` — read orchestrator
    lane state via the injected reader, render it via
    :func:`build_canvas_from_lanes`, persist it under
    ``lanes:{task_ref}`` or ``lane:{task_ref}:{lane_id}``, and return it.
- ``layout_canvas(canvas_ref, algorithm, options=None)`` — populate
    deterministic ``(x, y)`` positions on a stored Canvas and persist them.
- ``append_annotation(...)`` / ``list_annotations(...)`` — append-only
    per-node annotations with optional threading, persisted alongside the
    Canvas and surviving re-renders.
- ``export_canvas(canvas_ref, format)`` — render a stored Canvas as
    Mermaid, SVG, or PNG.

The handoff reader is injected so tests can supply an in-process stub.
The production MCP-client reader (implementation note) is wired through
:func:`build_server`'s ``handoff_reader`` parameter at launch time.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from workbay_canvas_mcp import lab_experiments
from workbay_canvas_mcp.annotations import Annotation
from workbay_canvas_mcp.canvas_service import CanvasService
from workbay_canvas_mcp.component_roster import ComponentRosterReceipt, ComponentSpec
from workbay_canvas_mcp.design_document import (
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
    coerce_export_format,
    to_mermaid,
)
from workbay_canvas_mcp.export_png import PNG_CONTENT_TYPE, SvgToPngConverter, to_png
from workbay_canvas_mcp.export_svg import SVG_CONTENT_TYPE, to_svg
from workbay_canvas_mcp.handoff_ingest import HandoffReader, build_canvas_from_handoff
from workbay_canvas_mcp.http_api import register_browser_api
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
from workbay_canvas_mcp.reconcile_apply import (
    ReconciliationPlan,
    apply_reconciliation_pure,
)
from workbay_canvas_mcp.review_errors import ReviewServiceError
from workbay_canvas_mcp.review_service import ReviewService
from workbay_canvas_mcp.store import CanvasStore
from workbay_canvas_mcp.templates import (
    TemplateSpec,
    build_template_canvas,
    run_template_seed,
)
from workbay_canvas_mcp.templates import (
    list_templates as registry_list_templates,
)
from workbay_canvas_mcp.ux_map.critique import CritiqueFinding, critique_map
from workbay_canvas_mcp.ux_map.fs_store import load_map, save_map
from workbay_canvas_mcp.ux_map.handoff_emit import (
    HandoffFindingWriter,
    emit_critique_to_handoff,
)
from workbay_canvas_mcp.ux_map.models import UxMap
from workbay_canvas_mcp.ux_map.paths import resolve_map_path
from workbay_canvas_mcp.ux_map.project_canvas import project_flow_to_canvas
from workbay_canvas_mcp.ux_map.render_ascii import render_ascii_layout
from workbay_canvas_mcp.ux_map.render_markdown import render_markdown_bundle
from workbay_canvas_mcp.ux_map.render_mermaid import render_flow_mermaid

ReconcileReaders = tuple[TaskRefReader, RefLookupReader, RefLookupReader, LaneLookupReader]

_LOG = logging.getLogger(__name__)
_REVIEW_INTERNAL_ERROR: dict[str, Any] = {
    "error": {"code": "internal_error", "message": "internal error", "details": {}},
}

# NOTE (implementation note §0.5 D1): this server's bare distribution/package identity is
# intentionally KEPT PRIVATE / unrenamed. Canvas is non-distributed
# (implementation note §9-D); only the two distributed servers (handoff, orchestrator)
# were cut over to WorkBay-native identities in Slice B. Any canvas rename
# is a separate, separately-owned follow-up. The shipped privacy gate forbids
# both spellings of the bare name, so source/docstrings here must avoid the
# literal token entirely rather than just picking the "correct" word order.
SERVER_NAME = "Design Canvas MCP"

DB_ENV_VAR = "DESIGN_CANVAS_MCP_DB"
WORKSPACE_DB_RELPATH = Path(".task-state") / "canvas-mcp" / "canvas.db"


def resolve_db_path(
    *,
    db_path: str | Path | None = None,
    workspace_root: str | Path | None = None,
) -> Path:
    """Resolve the SQLite path per the internal Persistence Contract.

    Precedence (highest first):

    1. Explicit ``db_path`` (used by tests and embedders).
    2. ``$DESIGN_CANVAS_MCP_DB`` environment variable.
    3. Workspace default ``<workspace>/.task-state/canvas-mcp/canvas.db``,
       where ``<workspace>`` is ``workspace_root`` if given, else
       :func:`Path.cwd`. Each clone or worktree carries its own canvas
       state — this mirrors the ``mcp-workbay-handoff`` convention.
    """

    if db_path is not None:
        return Path(db_path)
    env_override = os.environ.get(DB_ENV_VAR)
    if env_override:
        return Path(env_override)
    root = Path(workspace_root) if workspace_root is not None else Path.cwd()
    return root / WORKSPACE_DB_RELPATH


def _review_error_envelope(exc: ReviewServiceError) -> dict[str, Any]:
    return {
        "error": {
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
        }
    }


def build_server(
    *,
    db_path: str | Path | None = None,
    workspace_root: str | Path | None = None,
    handoff_reader: HandoffReader | None = None,
    lane_reader: LaneReader | None = None,
    reconcile_readers: ReconcileReaders | None = None,
    png_converter: SvgToPngConverter | None = None,
    handoff_finding_writer: HandoffFindingWriter | None = None,
) -> FastMCP:
    """Build the canvas MCP server.

    ``db_path`` and ``workspace_root`` flow through :func:`resolve_db_path`;
    see that function for the resolver precedence.

    ``handoff_reader`` is the callable that :func:`render_task_as_canvas`
    consults to obtain a handoff snapshot for a given ``task_ref``. Tests
    pass an in-process stub; the production reader (implementation note) adapts the
    public ``workbay-handoff-mcp`` surface. When ``None``, the tool returns
    a Canvas with only the task node (degenerates to an empty-handoff
    render) — useful for early launch before the reader is wired.

    ``lane_reader`` is the callable that :func:`render_lanes_as_canvas`
    consults to obtain the pinned lane snapshot for a task or a single lane.
    When ``None``, the tool returns an empty-lanes Canvas containing only
    the task node.
    """

    resolved_root = Path(workspace_root) if workspace_root is not None else Path.cwd()
    resolved_db = resolve_db_path(db_path=db_path, workspace_root=workspace_root)
    store = CanvasStore(resolved_db)
    review_service = ReviewService(
        workspace_root=resolved_root.resolve(),
        handoff_writer=handoff_finding_writer,
    )

    server: FastMCP = FastMCP(
        SERVER_NAME,
        instructions=(
            "This server provides a shared visual/spatial planning surface: canvases "
            "rendered from handoff and lane state, starter templates, per-node annotations, "
            "component rosters and design documents, text-first UX maps, and review bundles. "
            "Canvas state persists in SQLite; UX maps persist as .uxmap.json files."
        ),
    )

    def upsert_canvas(canvas: Canvas) -> Canvas:
        """Persist ``canvas`` under its ``canvas_ref``, replacing every stored node and edge."""
        return store.upsert_canvas(canvas)

    def get_canvas(canvas_ref: str) -> Canvas | None:
        """Return the stored Canvas for ``canvas_ref``, or ``None`` when it does not exist."""
        return store.get_canvas(canvas_ref)

    def list_canvases() -> list[CanvasSpec]:
        """Return a lightweight summary of every stored canvas.

        Backs the SPA canvas-index view: one :class:`CanvasSpec` per
        stored canvas (ref + node/edge counts + timestamps), sorted by
        ``canvas_ref``, without materializing each full Canvas.
        """
        return store.list_canvas_specs()

    def instantiate_template(
        template_id: str,
        input: dict[str, Any],
        canvas_ref_override: str | None = None,
        seed: bool = False,
    ) -> Canvas:
        """Render a registered template and persist the produced Canvas.

        Looks up ``template_id`` in the registry, validates ``input``
        against the template's model, builds the Canvas, and upserts it
        under ``canvas_ref_override`` (if given) or the template's default
        ``canvas_ref``. Unknown ids and invalid input raise
        ``TemplateError``.

        When ``seed=True`` and the template exposes a seed hook, durable
        design-system state (roster components, a design-document entry)
        is seeded under ``design:<task_ref>`` after the Canvas is
        persisted. Seeding is opt-in, idempotent, and never rewrites an
        existing design document; templates without a seed hook ignore the
        flag.
        """

        canvas = build_template_canvas(template_id, input, canvas_ref_override)
        persisted = store.upsert_canvas(canvas)
        if seed:
            run_template_seed(template_id, input, store, persisted)
        return persisted

    def list_templates() -> list[TemplateSpec]:
        """Return every registered template spec, sorted by ``template_id``."""
        return registry_list_templates()

    def render_task_as_canvas(task_ref: str) -> Canvas:
        """Render the handoff decisions for ``task_ref`` and persist them at ``task:<task_ref>``."""
        reader: HandoffReader = handoff_reader or _empty_handoff_reader
        canvas = build_canvas_from_handoff(reader=reader, task_ref=task_ref)
        return store.upsert_canvas(canvas)

    def render_lanes_as_canvas(task_ref: str, lane_id: str | None = None) -> Canvas:
        """Render orchestrator lane state and persist it.

        The Canvas is stored at ``lanes:<task_ref>``, or at
        ``lane:<task_ref>:<lane_id>`` when ``lane_id`` selects one lane.
        """
        reader: LaneReader = lane_reader or _empty_lane_reader
        canvas = build_canvas_from_lanes(reader=reader, task_ref=task_ref, lane_id=lane_id)
        return store.upsert_canvas(canvas)

    def layout_canvas(
        canvas_ref: str,
        algorithm: LayoutAlgorithm,
        options: LayoutOptions | None = None,
    ) -> Canvas:
        """Populate deterministic ``(x, y)`` on a stored Canvas and persist it.

        Reads the current Canvas, computes positions with the selected
        pure algorithm, upserts the same ``canvas_ref`` with populated
        coordinates (last-write-wins), and returns the laid-out Canvas.
        The returned Canvas carries a ``metadata["layout"]`` summary
        (e.g. cycle-break warnings); that metadata is call-scoped and not
        persisted.
        """

        current = store.get_canvas(canvas_ref)
        if current is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        laid_out = layout_canvas_pure(current, algorithm, options)
        store.upsert_canvas(laid_out)
        return laid_out

    def reconcile_canvas(canvas_ref: str) -> ReconciliationReport:
        """Detect drift between a stored Canvas and live handoff/orchestrator state.

        Reads the Canvas, then classifies every node's ``task_ref``,
        ``decision_id``, ``finding_id``, and ``lane_id`` against the
        injected readers. Returns a :class:`ReconciliationReport`; the
        Canvas itself is not mutated. When ``reconcile_readers`` is None
        at build time the tool raises — the production wiring in
        :mod:`workbay_canvas_mcp.cli` always supplies them.
        """

        if reconcile_readers is None:
            raise ValueError("reconcile_canvas requires reconcile_readers; build_server was not wired")
        canvas = store.get_canvas(canvas_ref)
        if canvas is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        task_ref_reader, decision_reader, finding_reader, lane_lookup = reconcile_readers
        return reconcile_canvas_pure(
            canvas,
            task_ref_reader=task_ref_reader,
            decision_reader=decision_reader,
            finding_reader=finding_reader,
            lane_reader=lane_lookup,
        )

    def apply_reconciliation(canvas_ref: str, plan: ReconciliationPlan) -> Canvas:
        """Apply an operator-confirmed :class:`ReconciliationPlan` and persist.

        Reads the Canvas, applies the plan via
        :func:`apply_reconciliation_pure`, upserts the result, and
        returns the persisted Canvas. Raises ``ValueError`` if the
        canvas is unknown; :class:`ApplyReconciliationError` for plan
        / canvas mismatches surfaces directly to the caller.
        """

        canvas = store.get_canvas(canvas_ref)
        if canvas is None:
            raise ValueError(f"unknown canvas_ref: {canvas_ref!r}")
        new_canvas = apply_reconciliation_pure(canvas, plan)
        return store.upsert_canvas(new_canvas)

    def append_annotation(
        canvas_ref: str,
        node_id: str,
        author: str,
        body: str,
        parent_annotation_id: str | None = None,
    ) -> Annotation:
        """Attach a note to ``node_id`` on ``canvas_ref``.

        ``parent_annotation_id`` makes the note a reply; the parent must exist
        on the same canvas.
        """
        return store.append_annotation(
            canvas_ref=canvas_ref,
            node_id=node_id,
            author=author,
            body=body,
            parent_annotation_id=parent_annotation_id,
        )

    def list_annotations(
        canvas_ref: str,
        node_id: str | None = None,
        include_detached: bool = True,
    ) -> list[Annotation]:
        """List annotations on ``canvas_ref``, oldest first, optionally for one ``node_id``.

        Pass ``include_detached=False`` to drop notes whose node is no longer
        on the canvas.
        """
        return store.list_annotations(
            canvas_ref=canvas_ref,
            node_id=node_id,
            include_detached=include_detached,
        )

    def manage_component_roster(
        operation: str,
        design_ref: str,
        component: dict[str, Any] | None = None,
        component_id: str | None = None,
    ) -> ComponentRosterReceipt:
        """List, upsert, or archive reusable component specs for a design.

        ``upsert`` requires ``component`` (validated against
        :class:`ComponentSpec`); ``archive`` requires ``component_id``.
        The receipt always carries the full post-operation roster for
        ``design_ref``.
        """

        affected_id: str | None = None
        if operation == "upsert":
            if component is None:
                raise ValueError("component is required for operation='upsert'")
            spec = ComponentSpec.model_validate(component)
            store.upsert_component(design_ref=design_ref, component=spec)
            affected_id = spec.component_id
        elif operation == "archive":
            if component_id is None:
                raise ValueError("component_id is required for operation='archive'")
            store.archive_component(design_ref=design_ref, component_id=component_id)
            affected_id = component_id
        elif operation != "list":
            raise ValueError(f"unknown operation: {operation!r}")

        return ComponentRosterReceipt(
            operation=operation,
            design_ref=design_ref,
            component_id=affected_id,
            components=store.list_components(design_ref=design_ref),
        )

    def manage_design_document(
        operation: str,
        design_ref: str,
        patch: dict[str, Any] | None = None,
        format: str | None = None,
    ) -> DesignDocumentReceipt:
        """Get, upsert, append a decision to, or render a design document.

        ``upsert`` and ``append_decision`` require ``patch`` (``upsert``
        merges patchable fields; ``append_decision`` needs a ``summary``
        and an optional ``rationale``). ``format`` is meaningful only for
        ``render`` (``"json"`` or ``"markdown"``, default ``"markdown"``)
        and is ignored elsewhere. ``get`` on an unknown ``design_ref``
        returns an empty document rather than raising.
        """

        document = store.get_design_document(design_ref=design_ref) or DesignDocument(design_ref=design_ref)

        if operation == "get":
            return DesignDocumentReceipt(operation=operation, design_ref=design_ref, document=document)
        if operation == "upsert":
            if patch is None:
                raise ValueError("patch is required for operation='upsert'")
            document = apply_patch(document, patch)
            store.upsert_design_document(document)
            return DesignDocumentReceipt(operation=operation, design_ref=design_ref, document=document)
        if operation == "append_decision":
            if patch is None:
                raise ValueError("patch is required for operation='append_decision'")
            if "summary" not in patch:
                raise ValueError("patch must include 'summary' for operation='append_decision'")
            document = append_decision(
                document,
                summary=patch["summary"],
                rationale=patch.get("rationale"),
                decision_id=patch.get("decision_id"),
            )
            store.upsert_design_document(document)
            return DesignDocumentReceipt(operation=operation, design_ref=design_ref, document=document)
        if operation == "render":
            selected = format or "markdown"
            if selected == "markdown":
                rendered = render_markdown(document)
            elif selected == "json":
                rendered = render_json(document)
            else:
                raise ValueError(f"unknown render format: {selected!r}")
            return DesignDocumentReceipt(
                operation=operation,
                design_ref=design_ref,
                document=document,
                rendered=rendered,
                format=selected,
            )
        raise ValueError(f"unknown operation: {operation!r}")

    def export_canvas(canvas_ref: str, format: ExportFormat | str) -> ExportResult:
        """Export a stored Canvas as ``mermaid`` or ``svg`` text, or ``png`` bytes."""
        canvas = store.get_canvas(canvas_ref)
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
                content_bytes=to_png(canvas, converter=png_converter),
                content_type=PNG_CONTENT_TYPE,
            )
        raise ExportError(reason="unsupported_format", detail=str(format))

    def _resolve_ux_map(
        *,
        path: str | None,
        map_ref: str | None,
        maps_dir: str | None,
        workspace_root: str | None,
    ) -> tuple[UxMap, Path]:
        """Load a map from path or map_ref under maps-dir."""
        root = Path(workspace_root) if workspace_root is not None else resolved_root
        resolved = resolve_map_path(
            path=path,
            map_ref=map_ref,
            maps_dir=maps_dir,
            workspace_root=root,
        )
        return load_map(resolved), resolved

    def upsert_ux_map(
        document: dict[str, Any],
        path: str | None = None,
        map_ref: str | None = None,
        maps_dir: str | None = None,
        workspace_root: str | None = None,
    ) -> UxMap:
        """Validate and write a UX map to the filesystem (map SSOT).

        Provide ``path`` or ``map_ref`` (+ optional ``maps_dir`` /
        ``workspace_root``). When only ``map_ref`` is set, the file is
        written as ``{maps_dir}/{map_ref}.uxmap.json``.

        Upsert validates structure only — it does **not** run the heuristic
        critique pack. Call ``critique_ux_map`` after write before planning UI
        slices ([INT-07] and peers are critique-owned, not model-owned).
        """
        map_ = UxMap.model_validate(document)
        root = Path(workspace_root) if workspace_root is not None else resolved_root
        # Prefer explicit path; else derive from document.map_ref / map_ref arg
        target_ref = map_ref or map_.map_ref
        resolved = resolve_map_path(
            path=path,
            map_ref=None if path else target_ref,
            maps_dir=maps_dir,
            workspace_root=root,
        )
        # Keep map_ref in document as authority when writing by path alone
        if map_.map_ref != target_ref and path is None:
            map_ = map_.model_copy(update={"map_ref": target_ref})
        save_map(map_, resolved)
        return map_

    def get_ux_map(
        path: str | None = None,
        map_ref: str | None = None,
        maps_dir: str | None = None,
        workspace_root: str | None = None,
    ) -> UxMap:
        """Load a UX map from path or map_ref under maps-dir."""
        map_, _ = _resolve_ux_map(
            path=path,
            map_ref=map_ref,
            maps_dir=maps_dir,
            workspace_root=workspace_root,
        )
        return map_

    def render_ux_map(
        path: str | None = None,
        map_ref: str | None = None,
        maps_dir: str | None = None,
        workspace_root: str | None = None,
        formats: list[str] | None = None,
        flow_id: str | None = None,
        screen_id: str | None = None,
    ) -> dict[str, str]:
        """Render a UX map as ascii / mermaid / markdown strings.

        ``formats`` defaults to all three. ASCII uses ``screen_id`` or every
        screen joined. Mermaid uses ``flow_id`` or the first flow on the map.
        """
        map_, _ = _resolve_ux_map(
            path=path,
            map_ref=map_ref,
            maps_dir=maps_dir,
            workspace_root=workspace_root,
        )
        selected = formats or ["ascii", "mermaid", "markdown"]
        out: dict[str, str] = {}
        for fmt in selected:
            key = fmt.lower()
            if key == "ascii":
                if screen_id is not None:
                    out["ascii"] = render_ascii_layout(map_, screen_id=screen_id)
                elif map_.screens:
                    out["ascii"] = "\n\n".join(render_ascii_layout(map_, screen_id=s.id) for s in map_.screens)
                else:
                    out["ascii"] = ""
            elif key == "mermaid":
                fid = flow_id or (map_.flows[0].id if map_.flows else None)
                if fid is None:
                    out["mermaid"] = 'flowchart TD\n  empty["no flows"]\n'
                else:
                    out["mermaid"] = render_flow_mermaid(map_, flow_id=fid)
            elif key == "markdown":
                out["markdown"] = render_markdown_bundle(map_)
            else:
                raise ValueError(f"unknown render format: {fmt!r}")
        return out

    def critique_ux_map(
        path: str | None = None,
        map_ref: str | None = None,
        maps_dir: str | None = None,
        workspace_root: str | None = None,
        task_ref: str | None = None,
        session: str | None = None,
        emit_to_handoff: bool = False,
    ) -> dict[str, Any]:
        """Run the heuristic critique pack (advisory).

        Returns ``{"findings": [...], "handoff_emit": receipt|null}``.
        When ``emit_to_handoff`` is true and ``task_ref`` is set, findings
        are batch-recorded under that task via the injected handoff writer
        (or the default ``workbay_handoff_mcp.review_findings`` writer).
        Emit failures are captured in the receipt and do not raise — critique
        remains advisory.
        """
        map_, resolved_path = _resolve_ux_map(
            path=path,
            map_ref=map_ref,
            maps_dir=maps_dir,
            workspace_root=workspace_root,
        )
        findings: list[CritiqueFinding] = critique_map(map_)
        rows = [
            {
                "rule_id": f.rule_id,
                "severity": f.severity,
                "pointer": f.pointer,
                "fix": f.fix,
                "detail": f.detail,
            }
            for f in findings
        ]
        receipt: dict[str, Any] | None = None
        if emit_to_handoff:
            if not task_ref:
                receipt = {
                    "ok": False,
                    "written": 0,
                    "finding_ids": [],
                    "error": "emit_to_handoff requires task_ref",
                }
            else:
                try:
                    receipt = emit_critique_to_handoff(
                        findings,
                        task_ref=task_ref,
                        session=session or f"ux-map-critique-{map_.map_ref}",
                        map_ref=map_.map_ref,
                        map_path=str(resolved_path),
                        workspace_root=str(Path(workspace_root) if workspace_root is not None else resolved_root),
                        writer=handoff_finding_writer,
                    )
                except Exception as exc:  # noqa: BLE001 — advisory path
                    receipt = {
                        "ok": False,
                        "written": 0,
                        "finding_ids": [],
                        "error": f"{type(exc).__name__}: {exc}",
                    }
        return {"findings": rows, "handoff_emit": receipt}

    def list_review_bundles() -> dict[str, Any]:
        """Return a summary of every configured review bundle.

        Each entry carries ``map_ref``, ``digest``, ``freshness``, and the
        generated ``.review-bundle.json`` path. Service errors return an
        ``error`` envelope instead of raising.
        """
        try:
            return {"bundles": [asdict(summary) for summary in review_service.list_bundles()]}
        except ReviewServiceError as exc:
            return _review_error_envelope(exc)
        except Exception:
            _LOG.warning("list_review_bundles failed", exc_info=True)
            return _REVIEW_INTERNAL_ERROR

    def get_review_bundle(map_ref: str) -> str | dict[str, Any]:
        """Return the stored review-bundle JSON text for ``map_ref``.

        Reads the generated ``.review-bundle.json`` for that map. An unknown
        or malformed ``map_ref`` returns an ``error`` envelope. If the
        configured map no longer parses but a bundle was generated earlier,
        the last-good bundle is returned; ``refresh_review_bundle`` reports
        the ``map_invalid`` error for that map.
        """
        try:
            return review_service.get_bundle(map_ref)
        except ReviewServiceError as exc:
            return _review_error_envelope(exc)
        except Exception:
            _LOG.warning("get_review_bundle failed", exc_info=True)
            return _REVIEW_INTERNAL_ERROR

    def refresh_review_bundle(
        map_ref: str,
        emit_handoff: bool = False,
        task_ref: str | None = None,
        session: str | None = None,
    ) -> dict[str, Any]:
        """Rebuild the review bundle for ``map_ref`` and persist it.

        Returns ``status``, ``digest``, the parsed ``bundle``, and
        ``handoff_emit``. When ``emit_handoff`` is true, findings are also
        recorded under ``task_ref``. Service errors return an ``error``
        envelope instead of raising.
        """
        try:
            result = review_service.refresh_bundle(
                map_ref,
                emit_handoff=emit_handoff,
                task_ref=task_ref,
                session=session,
            )
        except ReviewServiceError as exc:
            return _review_error_envelope(exc)
        except Exception:
            _LOG.warning("refresh_review_bundle failed", exc_info=True)
            return _REVIEW_INTERNAL_ERROR
        return {
            "status": "ok",
            "digest": result.digest,
            "bundle": json.loads(result.bundle_text),
            "handoff_emit": result.handoff_emit,
        }

    def project_ux_flow(
        flow_id: str,
        path: str | None = None,
        map_ref: str | None = None,
        maps_dir: str | None = None,
        workspace_root: str | None = None,
    ) -> Canvas:
        """Project a named flow onto a FREEFORM Canvas and persist it.

        ``canvas_ref`` is ``uxmap:{map_ref}:flow:{flow_id}``. Map file
        remains the SSOT; this is a one-way projection (no reverse-sync).
        """
        map_, _ = _resolve_ux_map(
            path=path,
            map_ref=map_ref,
            maps_dir=maps_dir,
            workspace_root=workspace_root,
        )
        canvas = project_flow_to_canvas(map_, flow_id=flow_id)
        return store.upsert_canvas(canvas)

    def save_lab_experiment(slug: str, document: dict[str, Any], if_match: str | None = None, create: bool = False) -> dict[str, Any]:
        """Save a git-owned variant with raw-file CAS or exclusive create.

        MCP has already decoded arguments; duplicate keys cannot be observed
        here. The resulting file always uses the canonical experiment encoder.
        """
        try:
            return asdict(lab_experiments.save_experiment(resolved_root, slug, document, if_match=if_match, create=create))
        except lab_experiments.ExperimentConflict as exc:
            return exc.as_dict()
        except lab_experiments.ExperimentValidationError as exc:
            return {"error": exc.code, "pointer": exc.pointer, "message": str(exc)}

    def get_lab_experiment(slug: str) -> dict[str, Any]:
        """Read a variant and its exact-file SHA256; never rebase it."""
        try:
            loaded = lab_experiments.read_experiment(resolved_root, slug)
            return {"document": loaded.document, "sha256": loaded.sha256}
        except FileNotFoundError:
            return {"error": "not_found"}
        except lab_experiments.ExperimentValidationError as exc:
            return {"error": exc.code, "pointer": exc.pointer, "message": str(exc)}

    server.tool(save_lab_experiment)
    server.tool(get_lab_experiment)

    server.tool(upsert_canvas)
    server.tool(get_canvas)
    server.tool(list_canvases)
    server.tool(instantiate_template)
    server.tool(list_templates)
    server.tool(render_task_as_canvas)
    server.tool(render_lanes_as_canvas)
    server.tool(layout_canvas)
    server.tool(reconcile_canvas)
    server.tool(apply_reconciliation)
    server.tool(append_annotation)
    server.tool(list_annotations)
    server.tool(manage_component_roster)
    server.tool(manage_design_document)
    server.tool(export_canvas)
    server.tool(upsert_ux_map)
    server.tool(get_ux_map)
    server.tool(render_ux_map)
    server.tool(critique_ux_map)
    server.tool(list_review_bundles)
    server.tool(get_review_bundle)
    server.tool(refresh_review_bundle)
    server.tool(project_ux_flow)

    service = CanvasService(
        store=store,
        workspace_root=resolved_root.resolve(),
        db_path=resolved_db.resolve(),
        handoff_reader=handoff_reader,
        lane_reader=lane_reader,
        reconcile_readers=reconcile_readers,
        png_converter=png_converter,
        review_service=review_service,
    )
    register_browser_api(server, service)
    return server


def _empty_handoff_reader(task_ref: str) -> dict[str, object]:
    """Fallback reader used when ``build_server(handoff_reader=None)``.

    Returns an empty-decisions snapshot so the render tool still produces
    a valid lone-task-node Canvas. The production reader replaces this
    in implementation note when the launch surface wires through to
    ``workbay-handoff-mcp``.
    """

    return {"task_ref": task_ref, "decisions": []}


def _empty_lane_reader(task_ref: str, lane_id: str | None = None) -> dict[str, object]:
    """Fallback lane reader used when ``build_server(lane_reader=None)``."""

    return {"task_ref": task_ref, "lanes": []}
