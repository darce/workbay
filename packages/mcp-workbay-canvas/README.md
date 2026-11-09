# workbay-canvas-mcp

Portable MCP server providing a shared visual/spatial planning surface for agents and humans.

Sibling to [`mcp-workbay-handoff`](../mcp-workbay-handoff/) and [`mcp-workbay-orchestrator`](../mcp-workbay-orchestrator/). Canvas storage is independent; handoff and orchestrator ids are foreign-key-like references that are not enforced — a Canvas may outlive any task, lane, or decision it references.

## Tools

- `upsert_canvas(canvas)` / `get_canvas(canvas_ref)` — direct read/write of a typed `Canvas` (nodes, edges, optional `(x, y)` layout hints).
- `list_canvases()` — return a lightweight `CanvasSpec` per stored canvas (`canvas_ref`, `node_count`, `edge_count`, `created_at`, `updated_at`), sorted by `canvas_ref`, without materializing each full `Canvas`. Backs an index/list view; the `Canvas` model has no title, so consumers use `canvas_ref` as the display label.
- `instantiate_template(template_id, input, canvas_ref_override=None, seed=False)` / `list_templates()` — render a registered starter template to a Canvas and persist it. With `seed=True`, a template that exposes a seed hook also augments durable design-system state (roster components, a design document). See [Templates](#templates).
- `manage_component_roster(operation, design_ref, component=None, component_id=None)` — `list` / `upsert` / `archive` reusable component specs scoped by `design_ref`. See [Component roster](#component-roster).
- `manage_design_document(operation, design_ref, patch=None, format=None)` — `get` / `upsert` / `append_decision` / `render` the durable per-design design record. See [Design document](#design-document).
- `render_task_as_canvas(task_ref)` — ingest handoff state for `task_ref` via `mcp-workbay-handoff` and persist it under `canvas_ref="task:<task_ref>"`. Renders the task plus recent slice-complete and other decisions.
- `render_lanes_as_canvas(task_ref, lane_id=None)` — ingest orchestrator lane state for `task_ref` via `mcp-workbay-orchestrator` and persist it as a Canvas:
  - `lane_id=None` (default): enumerates every lane on the task and writes under `canvas_ref="lanes:<task_ref>"`.
  - explicit `lane_id`: skips the lane-listing call and writes under `canvas_ref="lane:<task_ref>:<lane_id>"`.
  - If the underlying orchestrator listing fails, returns a task-only Canvas rather than raising.
- `layout_canvas(canvas_ref, algorithm, options=None)` — populate deterministic `(x, y)` positions on a stored Canvas and persist it (last-write-wins). `algorithm="hierarchical"` is a top-down Sugiyama-style layered layout; `algorithm="force_directed"` is a Fruchterman–Reingold spring layout. Identical Canvas + algorithm + options always yield identical positions. See [Layout](#layout).
- `reconcile_canvas(canvas_ref)` — read-only drift report. Walks every node and classifies its `task_ref`, `decision_id`, `finding_id`, and `lane_id` against live handoff/orchestrator state, returning a `ReconciliationReport(canvas_ref, entries, source_snapshot)`. Each `ReconciliationEntry` carries a `DriftKind` of `present`, `archived`, `renamed` (single prefix candidate, surfaced in `suggested`), `missing`, or `ambiguous` (multiple candidates, surfaced in `candidates`). Reader faults degrade to `ambiguous` and surface in `source_snapshot.readers[*].ok=False`; the Canvas is never mutated. See [Reconciliation](#reconciliation).
- `apply_reconciliation(canvas_ref, plan)` — apply an operator-confirmed `ReconciliationPlan` to a stored Canvas and persist the result (full-replace upsert, last-write-wins). The plan is a list of `ReconciliationAction(node_id, ref_kind, action, new_value=None)` with `action` ∈ `keep | rename | remove`. Removed nodes have their incident edges dropped so the Canvas still satisfies its edges->existing-node-ids invariant. Errors surface as `ApplyReconciliationError(reason, detail)` with stable tokens (`rename_missing_new_value`, `node_not_found`, `canvas_ref_mismatch`). See [Reconciliation](#reconciliation).
- `export_canvas(canvas_ref, format)` — load a persisted Canvas and emit a diagram.
  - `format=ExportFormat.MERMAID` (implementation note) returns a deterministic `flowchart TD` document in `ExportResult.content_text` with `content_type="text/vnd.mermaid"`.
  - `format=ExportFormat.SVG` (implementation note) returns first-party deterministic SVG markup in `ExportResult.content_text` with `content_type="image/svg+xml"`. Every node must carry `(x, y)` coordinates; if any node is unplaced the tool raises `ExportError(reason="layout_required", detail=...)` naming the offending ids.
  - `format=ExportFormat.PNG` (implementation note) returns PNG bytes in `ExportResult.content_bytes` with `content_type="image/png"`. PNG is derived from the slice-2 SVG via an optional `export-png` backend (`cairosvg`). When the backend is not installed, the tool raises `ExportError(reason="missing_dependency", detail=...)` with installation guidance instead of a raw `ImportError`. The `layout_required` check applies before the converter is invoked. For PNG support, install `cairosvg>=2.7,<3` in the same environment as the public installation and install native `libcairo`.
  - Missing canvases raise `ExportError(reason="canvas_not_found")`; unsupported formats raise `ExportError(reason="unsupported_format")`. Nodes and edges are sorted by id before emission so output is snapshot-safe.

### Layout

`layout_canvas` reads the stored Canvas, computes positions with the selected pure algorithm, and upserts the same `canvas_ref` with coordinates filled in. The returned (in-memory) Canvas carries a call-scoped `metadata["layout"]` summary (algorithm name plus any warnings such as broken cycle edges); that metadata is not persisted.

**`hierarchical`** ranks along reversed canvas edges (canvas edges point child -> parent, so a `DERIVES_FROM`/`DEPENDS_ON` target is drawn one rank above its source): longest-path layering from roots, within-rank ordering by the median of parent positions, every tie-break by node id.

**`force_directed`** is Fruchterman–Reingold: nodes repel each other while edges pull their endpoints together, with a per-iteration temperature that caps movement and cools to ~0 so the layout settles. It handles cycles natively and ignores `cycle_policy`. Determinism comes from a local `random.Random(seed)` for initial placement (`seed` defaults to `0` when omitted) plus sorted-id force sweeps. The returned `metadata["layout"]` carries `seed`, `iterations`, and the final-iteration `max_displacement`.

`LayoutOptions` tunables: `node_spacing` / `rank_spacing` (logical units), `respect_existing_positions` (pin nodes that already have `(x, y)` — they stay fixed while the rest lay out around them), `cycle_policy` (`"break"` — default — reverses the lowest-id edge of each cycle *for layout only* and records a warning; `"strict"` raises `LayoutError(reason="cycle_policy_required")`; force-directed ignores it), `seed` / `iterations` (force-directed only), and a safety budget (`max_nodes`, `max_edges`, `max_seconds`) that raises `LayoutError(reason="too_large"|"timeout")` instead of blocking the server.

### Reconciliation

`reconcile_canvas(canvas_ref)` (internal) is read-only: it loads the persisted Canvas, looks every reference up against `mcp-workbay-handoff` (`list_active_tasks`, `archive(operation="get")`, `search_handoff`, `list_review_findings`) and `mcp-workbay-orchestrator` (`manage_worktree_lane(operation="list", ...)`), and returns a `ReconciliationReport`. Detection rules per `ref_kind`:

- `task_ref`: `present` if the ref appears in the live handoff_state set; otherwise `archive(operation="get", task_ref=ref)` decides between `archived` (envelope `ok=True`) and `missing`.
- `decision_id` / `finding_id`: `present` on exact id match; `renamed` when exactly one prefix-match candidate exists (id surfaced in both `suggested` and `candidates`); `ambiguous` when multiple candidates exist (all listed in `candidates`); `missing` otherwise.
- `lane_id`: `present` if `manage_worktree_lane(operation="list", task_ref=node.task_ref)` returns the lane; `missing` otherwise. Pre-internal LANE nodes that lack `Node.lane_id` resolve via the legacy `lane:<id>` id prefix.

Reader failures degrade gracefully — the affected entry becomes `ambiguous` and the per-reader row in `source_snapshot.readers` records `ok=False` with the originating exception, so a partial outage still surfaces actionable verdicts for the ref kinds that did resolve. The Canvas itself is never mutated.

`apply_reconciliation(canvas_ref, plan)` (internal) is the opt-in repair side. The caller hands in a `ReconciliationPlan(canvas_ref, actions=[...])` after operator confirmation; the tool reads the Canvas, runs `apply_reconciliation_pure(canvas, plan)`, and persists the result. Each `ReconciliationAction` names one node and ref kind plus an `action`:

- `keep` — recorded no-op so the plan stays a complete record of operator intent.
- `rename` — set the named ref field on the node to `new_value` (required for `rename`; pydantic rejects an empty `new_value` at the boundary as `ApplyReconciliationError(reason="rename_missing_new_value")`).
- `remove` — delete the node and drop every incident edge so the resulting Canvas still validates.

Mismatched `plan.canvas_ref` raises `ApplyReconciliationError(reason="canvas_ref_mismatch")`; an action targeting an unknown node raises `reason="node_not_found"`. The pure mutator does not touch the input Canvas — callers receive a new Canvas object.

### `canvas_ref` namespaces

| Prefix    | Source tool                  | Example                       |
| --------- | ---------------------------- | ----------------------------- |
| `task:`   | `render_task_as_canvas`      | `task:internal`               |
| `lanes:`  | `render_lanes_as_canvas`     | `lanes:internal`              |
| `lane:`   | `render_lanes_as_canvas` (single-lane) | `lane:internal:lane-a` |
| `scope:`  | `instantiate_template("scope-intake")` | `scope:internal`     |
| `lane-deps:` | `instantiate_template("lane-dependency")` | `lane-deps:internal` |

`upsert_canvas` accepts any string `canvas_ref`; the prefixes above are conventions used by the render tools and by clients that need to disambiguate the source view.

### Lane snapshot shape

`render_lanes_as_canvas` consumes a pinned snapshot from its injected reader:

```python
{
    "task_ref": str,
    "lanes": [
        {
            "lane_id": str,
            "status": str,
            "title": str | None,
            "objective": str | None,
            "workers": [
                {"worker_id": str, "agent": str | None, "report_text": str | None},
            ],
            "decision_ids": [str, ...],
        },
    ],
}
```

The production `default_lane_reader` adapts `manage_worktree_lane(operation="list")` plus `get_lane_activity(...)` from `mcp-workbay-orchestrator` into this shape. Tests inject in-process stub readers instead.

## Templates

A template is a pure `(typed input) -> Canvas` function plus a `TemplateSpec` registry entry. `instantiate_template` validates the raw `input` against the template's Pydantic model, renders the Canvas, and persists it under the template's default `canvas_ref` (or `canvas_ref_override`). Unknown ids raise `TemplateError(reason="unknown_template")`; invalid input raises `TemplateError(reason="invalid_input")` carrying the pydantic error list. Templates are positionless — run `layout_canvas` to place nodes.

Ship-with templates:

- **`scope-intake`** (`input: {task_ref, title?, goals?, open_questions?, not_doing?}`) — a scope-intake starter under `scope:<task_ref>`: one `task` node, four `decision` placeholders (primary consumer, data model, MVP signal, not-doing), an "open questions" `freeform` note, and a `derives_from` edge from each decision back to the task. The `goals` / `open_questions` / `not_doing` lists are seed-only — they never alter the canvas; they feed the seed hook (below).
- **`lane-dependency`** (`input: {task_ref, lane_ids, edges?, title?}`) — a lane-dependency canvas under `lane-deps:<task_ref>`: one `task` node, one `lane:<lane_id>` node per (deduped) lane id, an `assigned_to` edge from each lane to the task, and a `depends_on` edge per `{source, target}` entry in `edges`. Lane node ids match `render_lanes_as_canvas`, so a templated canvas and a rendered lane canvas address the same nodes. Edges referencing a lane outside `lane_ids` raise `TemplateError(reason="invalid_input")`.

### Optional seeding (`seed=True`)

A template may expose an opt-in seed hook that augments durable design-system state after the Canvas is persisted. The pure `instantiate` path never touches I/O; the hook runs only when the caller passes `seed=True` to `instantiate_template` (templates without a hook ignore the flag). Seeding is idempotent and non-destructive — it augments by union and never rewrites an existing design document.

- **`scope-intake`** seeds the `design:<task_ref>` design document, folding the input's `goals` / `open_questions` / `not_doing` lists in by order-preserving union. An existing `title` is kept; a missing one is filled from `input.title`.
- **`lane-dependency`** seeds the `design:<task_ref>` document and upserts a reusable `lane-node` `ComponentSpec` into the `design:<task_ref>` roster, so the lane view's node shape is captured as a roster component.

## Component roster

The roster is the durable design-system layer: reusable `ComponentSpec` entries scoped by `design_ref` so successive canvas iterations can reuse the same component instead of treating every diagram as an isolated picture. A `ComponentSpec` carries a `component_id`, `name`, `props_schema`, named `SlotSpec` insertion points (each with `allowed_component_ids` guidance), `token_refs`, source canvas/node provenance, and a `status` (`active` / `archived`). The vocabulary borrows Pencil's reusable components, slots, and variables but stays tool-neutral so the same spec can later scaffold static HTML, Markdown, React, or a `.pen`-like export.

`manage_component_roster(operation, design_ref, component=None, component_id=None)` returns a `ComponentRosterReceipt` carrying the full post-operation roster:

- `operation="list"` — return every component for `design_ref`.
- `operation="upsert"` — insert or replace `component` (idempotent on `component_id`).
- `operation="archive"` — flip `component_id` to `status="archived"` (the row is retained so historical canvases keep resolving their references); an unknown id raises `ValueError`.

## Design document

The design document is the durable, per-`design_ref` design record that gives successive iterations memory: `goals`, `audience`, design `tokens`, `component_refs` (links into the roster), `layout_decisions`, `scaffold_targets`, `open_questions`, and `not_doing` (explicit out-of-scope items). It is the artifact a future step renders to Markdown for docs or reads to scaffold static content — without turning the canvas into a code generator.

`manage_design_document(operation, design_ref, patch=None, format=None)`:

- `operation="get"` — return the current document; an unseeded `design_ref` yields an empty document rather than an error.
- `operation="upsert"` — merge `patch` into the document per field (last-write-wins; absent fields untouched). A patch key outside the patchable set (e.g. `design_ref` or `layout_decisions`) raises `ValueError`.
- `operation="append_decision"` — append one `DesignDecision` from `patch` (`{summary, rationale?}`); the id and timestamp are generated. Order is preserved.
- `operation="render"` — emit the document as text. `format="markdown"` (default) renders stable, section-omitting Markdown (the title falls back to `design_ref` when untitled); `format="json"` renders indented JSON. `format` is ignored for the other operations.

## Persistence

State is SQLite-backed under the workspace by default:

- DB: `<workspace>/.task-state/workbay-canvas-mcp/canvas.db`

Override precedence (high -> low):

1. `build_server(db_path=...)` argument.
2. `DESIGN_CANVAS_MCP_DB` environment variable.
3. Workspace default above.

Canvases written by any tool — including both render paths — persist through the same store and survive server restarts. The component roster (`component_roster`) and design documents (`design_documents`) live in additive tables in the same database, scoped by `design_ref`, and persist the same way.

## Annotations

internal (implementation note) adds an append-only annotation surface keyed on
`(canvas_ref, node_id)`. Annotations carry contextual prose against a
canvas node without mutating the underlying handoff state.

```python
store.append_annotation(
    canvas_ref="task:internal",
    node_id="n1",
    author="actor:claude",
    body="markdown-ish note",
)
store.list_annotations(canvas_ref="task:internal", node_id="n1")
```

The same calls are exposed as MCP tools `append_annotation` and
`list_annotations`. Annotations survive Canvas re-renders: if a node id
disappears from the canvas, its annotations remain listable and are
flagged as detached via `list_annotations(..., include_detached=True)`
(default).

internal (implementation note) adds threading via an optional
`parent_annotation_id`. The parent must already exist on the same
canvas — otherwise `append_annotation` raises
`AnnotationError(reason="parent_missing")`. Readers can reconstruct
any chain with `reconstruct_thread(store, annotation_id)`, which
returns a DFS preorder list of `ThreadEntry(annotation, depth)` rooted
at the requested annotation. Unknown ids raise
`AnnotationError(reason="annotation_missing")`.

## UX maps (text-first IA)

Filesystem SSOT for product screens, zones, states, flows, and actions — independent of the canvas SPA. Use `.uxmap.json` + ASCII/Mermaid/Markdown renders + heuristic critique; optional FREEFORM flow projection onto canvas (`uxmap:{map_ref}:flow:{flow_id}`).

- Guide: [`docs/ux-map.md`](docs/ux-map.md)
- CLI: `workbay-canvas-mcp ux-map render|critique|project`
- MCP: `upsert_ux_map`, `get_ux_map`, `render_ux_map`, `critique_ux_map`, `project_ux_flow`
- Skill: `packages/workbay-system/workbay_system/payload/skills/ux-map/`
- Golden fixture: `tests/fixtures/ux_maps/workbench-operator-loop.uxmap.json`

## Install and local launch

A package built with staged web assets and an inventory collector includes those
files as package data. If launch reports `web_assets_missing`, install a package
built with staged web assets, or pass `--web-dist` to a built web directory that
contains `index.html`. After install, launch the SPA and `/api` on one loopback origin:

```bash
workbay-canvas-mcp local [--workspace-root PATH] [--db-path PATH] [--host 127.0.0.1] [--port 8765] [--allow-origin URL]... [--web-dist PATH]
```

`--port 0` binds a free port. The process prints one stdout JSON line `{"url": ...}`. Remote bind is refused (loopback literals and `localhost` only).

`workbay canvas` is the front door: when this package is installed, `workbay` dispatches `canvas` to the same CLI (`local` when argv is empty or flags-only).

Vite dev pairing (API-only, guarded):

```bash
workbay-canvas-mcp serve --transport http --allow-origin http://127.0.0.1:5173 --port 8765
```

Node is needed only for Refresh; viewing needs no Node.

## Use Canvas in another repository

Design Canvas provides Map review for a product's UX map and a read-only Library
of its tokens, registered components and assets. A review bundle is generated
JSON that combines the map, critique, source checks and design inventory for
these views. The consumer repository owns the source files and configuration.

### Install the public distribution

See [the public installation recipe](../../docs/workbay/canvas-install.md) for the full local-path installation of all shipped members, Studio and Fleet. The browser app and collector are included as checked package data.

### Declare the consumer's existing design sources

For a repository with design assets, author
`<repo>/docs/design/design-sources.json`. The default path and every source path
in the file are relative to `--workspace-root`, including paths to maps.
Use canonical paths without `..`, URLs or absolute paths; resolved paths must
stay inside that workspace. The headless `bundle` and `inventory` commands accept
`--config` to override the workspace-relative config path. The local browser
launch uses the default config path and has no `--config` flag.

For example, if the consumer already has a map at
`docs/ux-maps/product.uxmap.json` and tokens in `src/styles.css`:

```json
{
  "config_version": 1,
  "maps": ["docs/ux-maps/product.uxmap.json"],
  "components": [],
  "css_inputs": ["src/styles.css"],
  "asset_roots": []
}
```

The required top-level fields are `config_version`, `maps`, `components`,
`css_inputs` and `asset_roots`; empty lists are allowed. Optional lists default
to `[]`. Unknown top-level fields are rejected.

| Field | Type and meaning |
| --- | --- |
| `config_version` | Number equal to `1`; booleans are rejected. |
| `maps` | List of strings: workspace-relative `.uxmap.json` paths for Map review. |
| `components` | List of objects declaring the components to inspect; each needs the fields below. Component `id`s must be unique. |
| `components[].id`, `.export_name`, `.module_path`, `.description` | Required strings: stable component identifier, exported symbol, workspace-relative module path, and description. |
| `components[].slots` | Required list of objects: `slot_id` (string), optional `allowed_component_ids` (list of strings, default `[]` for unconstrained), optional `guidance` (string or null, default null). A slot is a named insertion point. |
| `components[].example_props` | Required object mapping prop names to JSON values for the declared component. |
| `components[].token_refs` | Optional list of token-name strings, at most 50, default `[]`. |
| `css_inputs` | List of strings: workspace-relative CSS files from which to collect custom properties. |
| `asset_roots` | List of strings: workspace-relative directories or files to scan for assets. |
| `attributions` | Optional list of objects with required string `path` (workspace-relative asset path), `license` and `credit`. |
| `compiled_css` | Optional list of objects: required string `output` (workspace-relative CSS output path) and required `provenance` object. Include an output in `css_inputs` if its custom properties should be collected. |
| `compiled_css[].provenance` | Required `inputs` list of objects with string `path` and `sha256`, and string `output_digest`. Input hashes accept hex or `sha256:`-prefixed hex; the output digest uses `sha256:`-prefixed hex. Records which source bytes produced the CSS output for freshness checks. |
| `examples` | Optional list of objects with required string `component_id` and workspace-relative string `path` linking an example to a component. |
| `generated_dir` | Optional string: workspace-relative output directory, default `docs/design/generated`. Keep it separate from source inputs. |

Inventory coverage declares four limitations: `css_custom_properties_only`
(CSS custom properties only), `registered_components_only` (only components
listed in the config), `no_usage_inference` (no inference of where tokens,
components or assets are used), and `compiled_css_requires_provenance`
(compiled CSS needs the input hashes and output digest above).

### Launch, Refresh and read the Library

With the package and the `workbay` front door installed, launch from the consumer
repository with `workbay canvas`, or pass its root explicitly:

```bash
workbay canvas --workspace-root /path/to/consumer-repo
workbay-canvas-mcp local --workspace-root /path/to/consumer-repo --port 0
```

Each command launches the same local app and prints one stdout JSON line
`{"url": ...}`. Open that URL, select the configured map, choose Refresh, then
read Map review and the Library. Binding is loopback-only (loopback IP literals
or `localhost`); remote bind is refused.

Refresh reads the configured sources and writes `inventory.json`, `inventory.md`,
`digest-manifest.json` and `<map_ref>.review-bundle.json` under
`docs/design/generated/` (or `generated_dir`). `map_ref` is the identifier inside
the map, such as `product`, not a filesystem path; bundle commands use that
identifier to find the map among the configured `maps` paths. The Library is
read-only: it has no token editor or CSS writer and does not edit production
CSS or TSX. Refresh writes generated review artifacts.

### Review headlessly with the agent CLI

Agents can use the dedicated `ux-map` console script without opening the app:

```bash
ux-map bundle list --workspace-root /path/to/consumer-repo
ux-map bundle refresh --workspace-root /path/to/consumer-repo --map-ref product
ux-map bundle get --workspace-root /path/to/consumer-repo --map-ref product
ux-map bundle status --workspace-root /path/to/consumer-repo --map-ref product
ux-map inventory collect --workspace-root /path/to/consumer-repo --json
ux-map inventory check --workspace-root /path/to/consumer-repo
```

All six commands accept `--config PATH` and `--json`; bundle output is JSON even
without `--json`. `get`, `status` and `refresh` require `--map-ref ID`.
`inventory collect|check` also accept `--out-dir PATH` (workspace-relative) and
`--collector PATH` (a `collect-design-inventory.mjs` path resolved from the
current directory). Use a collector outside the workspace being inspected.
Collection writes
the three inventory files; `check` recollects without writing, exits 0 when
they match and 1 when they differ. Use `bundle refresh` to also generate the
review bundle.

### Resolve launch and collection errors

Launch errors use `error.code`. Review-bundle errors use the same JSON envelope;
collection failures have `error.code = collector_failed` and the specific
reason in `error.details.reason`. The table quotes review-service messages for
those reasons; the inventory CLI prints collector diagnostics on stderr.

| Code or collection reason | What the user sees | Fix |
| --- | --- | --- |
| `web_assets_missing` | "Web assets are missing. Reinstall the Canvas package, or pass --web-dist to a directory that contains index.html." | Install a package built with staged web assets, or pass a built web directory containing `index.html` with `--web-dist`. |
| `config_invalid` | "invalid design-sources config"; `details.pointer` locates the field and `details.code` gives the config error. | An empty pointer usually means the file is missing or unreadable. Check the workspace root and file existence/access before debugging the schema; malformed JSON can also have an empty pointer. Missing-file stat failures currently use `details.code = invalid`. |
| `collector_not_found` | "inventory collector not found: reinstall the Canvas package, or set WORKBAY_INVENTORY_COLLECTOR to a collect-design-inventory.mjs file" | Reinstall with staged collector data, or set `WORKBAY_INVENTORY_COLLECTOR` to that file. |
| `node_not_found` | "node not found: install Node.js >=20 or set WORKBAY_NODE; viewing needs no Node, only Refresh collects sources" | Install Node.js >=20 or set `WORKBAY_NODE` to an executable. |
| `deps_missing` | "install typescript and postcss in the workspace (`npm install -D typescript postcss`) or run from a checkout" | Make `typescript>=5` and `postcss>=8` resolvable beside the collector or in the consumer workspace. |
| `remote_bind_refused` | "Refusing to bind to a non-loopback host." | Use `--host 127.0.0.1`, another loopback IP literal or `localhost`; remote bind is unsupported. |
| `port_in_use` | "Port is already in use." | Select another port or use `--port 0` for a free port. |

### Use text-first maps when the repository has no design assets

Do not author a speculative `design-sources.json` for a repository with no design
assets. Use `.uxmap.json` authoring, render and critique through the text-first
UX map CLI or MCP tools described above. These workflows do not require a design
inventory config. Consumer design guidance belongs with the consumer's real
sources.

## Running the server

The `workbay-canvas-mcp` console script runs the FastMCP server. The transport defaults to **stdio** (for local MCP clients):

```bash
workbay-canvas-mcp                       # stdio (default)
```

`serve --transport http` is guarded and API-only: Host/Origin checks apply on every path including `/mcp`, remote bind is refused, and static SPA routes are not served. Pair it with Vite using `--allow-origin http://127.0.0.1:5173`. For the packaged SPA plus `/api` on one origin, use `local` (see [Install and local launch](#install-and-local-launch)). `--host`/`--port` bind the HTTP listener (default `127.0.0.1:8765`):

```bash
workbay-canvas-mcp serve --transport http --port 8765
```

## Command help

```bash
workbay studio --help
workbay-canvas-mcp --help
ux-map --help
```
