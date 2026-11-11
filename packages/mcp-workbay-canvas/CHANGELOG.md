# mcp-workbay-canvas

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.1.20] — 2026-11-11

### Changed
- Align handoff and orchestrator dependency floors with the WorkBay v0.1.90 release.

## [0.1.19] — 2026-11-09

### Changed
- Align handoff and orchestrator dependency floors with the WorkBay v0.1.89 release.

## [0.1.18] — 2026-11-09

### Changed
- Align handoff and orchestrator dependency floors with the WorkBay v0.1.89 release.

## [0.1.17] — 2026-11-09

- Qualify installed Studio, Lab, and Fleet surfaces with the WorkBay v0.1.88 component set.

### Added
- Ship the Canvas Studio, Lab and Fleet surfaces from the integrated public release, including the installed Studio front door and packaged web and collector assets built by the existing producer.

### Changed
- Include Canvas exactly once in the eight-member public release catalog and consumer gate.

## [0.1.15] — 2026-10-28

- Update the orchestrator dependency floor for the terminal-history recovery release.

## [0.1.14] — 2026-10-28

### Changed
- Refresh coordinated dependency floors; this private component remains excluded from the public Git mirror.

## [0.1.13] — 2026-10-27

### Changed
- Refresh coordinated dependency floors; this private component remains excluded from the public Git mirror.

## [0.1.12] — 2026-10-27

### Changed
- Refresh the orchestrator dependency floor for the coordinated stack update.

## [0.1.11] — 2026-10-23

### Changed
- Refresh dependency floors to the finalized MCP family; this private package remains excluded from the public mirror.

## [0.1.10] — 2026-10-22

### Changed
- **Follows the coordinated v0.1.74 floor cascade.** This release requires `mcp-workbay-handoff>=0.3.14,<0.4.0` and `mcp-workbay-orchestrator>=0.3.17,<0.4.0`.

## [0.1.9] — 2026-10-19

### Changed
- Payload drift since 0.1.8 (landed main commits); version bump only, no consumer-facing API change.

## [0.1.8] — 2026-10-18

### Changed
- Keep dependency compatibility with mcp-workbay-orchestrator 0.3.15.

## [0.1.7] — 2026-09-27

### Changed
- **Canvas errors no longer expose private paths or collector stderr.** Unexpected `/api` failures return a fixed `internal_error` envelope, collector failures preserve typed reason codes without echoing raw stderr, and launch or collector hints name public remedies.
- **Canvas MCP instructions describe the server and its review tools.** Server instructions now state the product purpose, every tool has a description, and `get_review_bundle` explains how it returns the stored bundle and last-good copy.

## [0.1.6] — 2026-09-23

### Added
- **WBC-06 local launch.** `workbay-canvas-mcp local` serves the packaged SPA plus `/api` on one loopback origin (the IPv4 loopback address on port 8765 by default, `--port 0` for a free port, one stdout `{"url": ...}` line).
- **WBC-05 local review loop.** Review bundles can be listed, read, checked and refreshed from the CLI (`workbay-canvas-mcp ux-map bundle list|get|status|refresh`), over MCP (`list_review_bundles`, `get_review_bundle`, `refresh_review_bundle`) and over four `/api/review-bundles` HTTP routes, all backed by one `ReviewService`.
- **WBC-04 readable Library.** The review shell links Map and Library; hash deep links restore the selected map screen or flow, Library collection or item, while unknown paths and ids open a not-found view with links back to Map and Library.
- **WBC-03 living design inventory.** A versioned `docs/design/design-sources.json` config (`config_version` 1) declares a product's UX maps, components, CSS inputs, asset roots, compiled-CSS provenance, examples and attributions, with hard bounds on counts and bytes read.
- **WBC-02 versioned UX-map contract.** Maps now carry `schema_version` 1: a missing version reads as 1, and any other value is refused as `unknown_version`.
- **WBC-02 flow ASCII render and source check.** `render_flow_ascii` (CLI: `ux-map render --format ascii-flow --flow-id ID`) prints a flow as numbered steps with screen titles, repeat-visit markers and labelled edges.
- **WBC-02 critique provenance.** Every critique finding now carries `pack_revision` (`uxmap-critique/1`), `canon_revision`, `evidence_kind` (`structural` for `AGT-02`, `heuristic` otherwise), a `next_check` question and a `limitation` stating what the map alone cannot prove.
- **WBC-01 review bundle.** `build_review_bundle(map_)` derives a deterministic review bundle (`bundle_version` 1) from a UX map: per-screen ASCII layouts, per-flow Mermaid and ASCII renders, the Markdown render, a source-check report, the critique findings, renderer versions, the rule-pack and canon revisions, and a `sha256:` digest of the normalized map, plus the design inventory when one is supplied.
- **internal: text-first UX map domain.** New `workbay_canvas_mcp.ux_map` package: `UxMap` model + committed JSON Schema, pure ASCII/Mermaid/Markdown renders, heuristic critique pack (lexicon IDs `RLSE-04`…`UI-06`), filesystem load/save, FREEFORM-only `project_flow_to_canvas`, MCP tools `upsert_ux_map` / `get_ux_map` / `render_ux_map` / `critique_ux_map` / `project_ux_flow`, CLI `workbay-canvas-mcp ux-map render|critique|project`, optional handoff emit (`emit_to_handoff`), pure `user-flow` template (`document=` required), fixtures `workbench-operator-loop` + `roster-people`, package doc `docs/ux-map.md`, workbay-system skill + portable `/ux-map`.
- **internal graphical web frontend (implementation note: drag-to-position + inline label edit).** The Canvas web app's canvas view is now editable.
- **internal graphical web frontend (implementation note: read-only view).** New `list_canvases()` MCP tool returns a lightweight `CanvasSpec` per stored canvas (`canvas_ref`, `node_count`, `edge_count`, `created_at`, `updated_at`), ordered by `canvas_ref`, so a client can render an index without loading every canvas.
- **internal: opt-in template seeding (`seed=True`).** `instantiate_template` gains a `seed=False` parameter.
- **internal: `lane-dependency` template.** A second built-in template, registered as `lane-dependency`.
- **internal: design document.** New durable, per-`design_ref` design record (`DesignDocument`: goals, audience, design tokens, component-roster links, layout decisions, scaffold targets, open questions) backed by an additive `design_documents` SQLite table.
- **internal: component roster.** New reusable-component roster scoped by `design_ref`, backed by an additive `component_roster` SQLite table.
- **internal: canvas templates + `scope-intake`.** New template registry (`TemplateSpec`, `TEMPLATE_REGISTRY`, `list_templates`, `build_template_canvas`) plus the first built-in `scope-intake` template.
- **internal: `apply_reconciliation` repair.** New `apply_reconciliation(canvas_ref, plan) -> Canvas` MCP tool applies an operator-confirmed `ReconciliationPlan` to a stored Canvas (full-replace upsert, last-write-wins).
- **internal: `reconcile_canvas` drift detection.** New read-only `reconcile_canvas(canvas_ref) -> ReconciliationReport` MCP tool classifies every node's `task_ref`, `decision_id`, `finding_id`, and `lane_id` against live handoff/orchestrator state and emits a deterministic `ReconciliationReport` with per-entry `DriftKind` (`present | archived | renamed | missing | ambiguous`) plus an aggregate `SourceSnapshot` of which readers ran and whether any failed.
- **internal auto-layout (implementation note: force-directed).** `layout_canvas(..., algorithm="force_directed", options=LayoutOptions(seed=42))` ships a pure-Python Fruchterman–Reingold spring layout: nodes repel (`k²/d`), edges attract (`d²/k`), and a per-iteration temperature cools linearly to ~0 so the layout settles.
- **internal auto-layout (implementation note: hierarchical).** New `layout_canvas(canvas_ref, algorithm, options=None)` MCP tool that populates deterministic `(x, y)` positions on a stored Canvas and persists it (last-write-wins).
- **internal: PNG diagram export.** `export_canvas(canvas_ref, format=ExportFormat.PNG)` now returns PNG bytes derived from the same slice-2 SVG emitter, with `content_type="image/png"`.
- **internal: SVG diagram export.** `export_canvas(canvas_ref, format=ExportFormat.SVG)` now returns first-party SVG markup with `content_type="image/svg+xml"`.
- **internal: Mermaid diagram export.** New `export_canvas(canvas_ref, format)` MCP tool that loads a persisted Canvas and emits a deterministic `flowchart TD` document for `format=ExportFormat.MERMAID`.
- **internal lane-ops live view.** New `render_lanes_as_canvas(task_ref, lane_id=None)` MCP tool that ingests `mcp-workbay-orchestrator` lane state for a task and persists it as a Canvas.
- **Lane-ingest graph kinds.** `NodeKind.LANE`, `NodeKind.WORKER`, `EdgeKind.ASSIGNED_TO`, and `EdgeKind.PRODUCED` enum values cover the lane-ingest graph surface (additive — existing canvases are unaffected).
- **internal: annotation threading.** `append_annotation(..., parent_annotation_id=...)` now enforces parent existence on the same canvas (raises `AnnotationError(reason="parent_missing")` otherwise).
- **internal: per-node annotation surface.** New `Annotation` model, `annotations` SQLite table (additive, idempotent), and `append_annotation` / `list_annotations` MCP tools.
- **Optional `export-png` extra.** Added optional extra `export-png = ["cairosvg>=2.7.0,<3.0.0"]` for the PNG conversion backend.
- **Runtime dependency on `mcp-workbay-orchestrator`.** Added `mcp-workbay-orchestrator>=0.3.10,<0.4.0` as a runtime dependency for the production `default_lane_reader`.
- **Initial package scaffold (internal).** FastMCP server registration with one trivial `ping` round-trip tool and a minimal `Canvas` model.

### Changed
- **`serve --transport http` is now guarded and API-only.** The Host/Origin guard applies on every path including `/mcp`, no static SPA routes are served, and remote bind is refused (loopback literals and `localhost` only; no override flag).
- **ASCII and Markdown renders changed shape.** A screen row longer than its box now wraps onto the next line after a visible trailing `\`, with a two-space indent, instead of being cut off with `…`.
- **`load_map` raises `MapSchemaError` on schema problems.** `fs_store.load_map` now validates through `parse_map` and raises `MapSchemaError` (a `ValueError`) instead of pydantic's `ValidationError`, and saved maps now include `schema_version`.
- **`critique_ux_map` returns an object, not a list.** It returns `{findings: list[dict], handoff_emit: dict|null}` instead of a bare finding list, which is breaking relative to the early internal S4 draft.

## [0.1.5] — 2026-09-21

### Changed
- Requires mcp-workbay-handoff 0.3.6 and mcp-workbay-orchestrator 0.3.9.

## [0.1.4] — 2026-09-20

### Changed
- Requires mcp-workbay-handoff 0.3.5 and mcp-workbay-orchestrator 0.3.8.

## [0.1.3] — 2026-09-18

### Changed
- Requires mcp-workbay-orchestrator 0.3.6.

## [0.1.1] — 2026-09-16

### Changed
- Version sync with the v0.1.64 package family; no functional changes.

## [0.1.2] — 2026-09-18

### Changed
- `ux-map critique --emit-handoff` configures the handoff runtime before emitting.
