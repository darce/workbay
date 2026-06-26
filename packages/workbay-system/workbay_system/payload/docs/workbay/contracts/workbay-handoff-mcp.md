---
boundary_owner: agentic-tooling
---

# WorkBay Handoff MCP Contract

## Purpose

`workbay-handoff-mcp` is the portable MCP server for agent coordination state. After the internal event, review, next-action, and artifact-domain consolidation plus profile-removal stretch work, internal verified-test search/read support, internal observatory dashboard split, the internal slice-complete decision-id preflight surface, and the internal discriminated-operation flattening pass, it exposes a single **21-tool** MCP surface for task state, review findings, verification evidence, artifacts, touch ledger, integrity checks (working-tree / post-merge / close), session compaction, export/import, decision-id and write preflight, and DASHBOARD.txt generation. Orchestration, daemon lifecycle, lane management, and turn metrics are served by [`workbay-orchestrator-mcp`](workbay-orchestrator-mcp.md).

## Runtime Configuration

CLI args take precedence over env vars.

Supported config inputs:

- `--workspace-root` or `WORKBAY_HANDOFF_WORKSPACE_ROOT`
- `--state-dir` or `WORKBAY_HANDOFF_STATE_DIR`
- `--current-task-path` or `WORKBAY_HANDOFF_CURRENT_TASK_PATH`
- `--exports-dir` or `WORKBAY_HANDOFF_EXPORTS_DIR`
- `--tool-profile` or `WORKBAY_HANDOFF_TOOL_PROFILE` — legacy compatibility input. `all`, `core`, and `extended` are accepted, but all launches now expose the same 21-tool surface.
- `WORKBAY_HANDOFF_DEFAULT_AGENT`
- `WORKBAY_HANDOFF_DEFAULT_BRANCH`
- `WORKBAY_HANDOFF_DEFAULT_COMMIT_SHA`

Default workspace-owned state:

- DB: `.task-state/handoff.db`
- artifact DB: `.task-state/mcp-artifacts.db`
- exports: `.task-state/exports/`
- generated machine-readable snapshot: `CURRENT_TASK.json` (v2 workspace summary)
- generated human-readable dashboard: `DASHBOARD.txt` (pure ASCII, human-scoped observatory view)

`CURRENT_TASK.json` is a deterministic v2 workspace-summary payload (`schema_version: 2`) describing which live task — if any — owns the current workspace. It exposes one of three shapes:

- `shape="single"`: exactly one live task owns the workspace. The payload carries `task_ref` and a compact `active` block with the identity fields a reader needs to route to the right task (`task_ref`, `objective`, `status`, `target_branch`, `target_worktree_path`, `task_plan_path`, `revision`, and update provenance).
- `shape="workspace_ambiguous"`: more than one live task is resident on the same workspace path. The payload carries a `tasks[]` list of compact per-task descriptors so the caller can disambiguate before writing.
- `shape="none"`: no live task is resolvable for the workspace.

The workspace summary intentionally does **not** embed the detailed per-task sections (full decision/test/finding/blocker/action/lane history). Those live behind the task-state and dashboard read surfaces: use `get_handoff_state(task_ref=...)` or `load_session(task_ref=...)` for the structured per-task ledger, and `render_handoff(kind='dashboard')` / `DASHBOARD.txt` for the cross-task observatory. `DASHBOARD.txt` is the human-readable ASCII observatory: Needs Attention summary, All Tasks table, cross-task open findings, deferred/wontfix findings, and registered extension sections (e.g. Lane Health from workbay-orchestrator-mcp). Use `render_handoff(kind='current_task')` to refresh the JSON workspace summary and `render_handoff(kind='dashboard')` to refresh the ASCII dashboard.

The monorepo now consumes `workbay-handoff-mcp` from the private git+ssh source for `darce/mcp-workbay-handoff`; the installed binary shape stays the same.

Runtime bootstrap:

```bash
cd "${REPO_ROOT:-$PWD}"

# Core ledger server
uv tool install "mcp-workbay-handoff @ git+https://github.com/darce/workbay.git@mcp-workbay-handoff-vX.Y.Z#subdirectory=packages/mcp-workbay-handoff"

# Orchestration server (daemons, workers, lanes, metrics)
uv tool install ./packages/mcp-workbay-orchestrator

# Codex subagent bridge for BACKEND=codex-subagent
python3 -m pip install -e packages/workbay-codex-bridge

# Validate runtime wiring, writable state dirs, and FTS5 support
mcp-workbay-handoff --workspace-root "$(pwd)" doctor
mcp-workbay-orchestrator --workspace-root "$(pwd)" doctor
```

Notes:

- `doctor` hard-fails when the local SQLite build lacks FTS5; artifact indexing depends on it.
- `dashboard-live` does not require optional UI packages. `dashboard-tui` uses Textual when installed, then `rich.live`, then plain text.

## MCP Tool Surface

Surface classes:

- `action`: mutates state, filesystem state, lane runtime, or daemon runtime. Do not retry blindly.
- `query`: read-only inspection of canonical state. Safe to retry when transport/runtime is healthy.
- `generator`: derives a report, search result, reconciliation result, or rendered artifact from current state. Usually safe to retry unless the tool also writes a file by default.

| Tool | Surface class | Idempotent | Notes |
| --- | --- | --- | --- |
| `set_handoff_state` | action | no | Updates active task state with optimistic revision guard. Pass `status_only=True` to update only the task status (active row or archived snapshot) without touching objective/focus or recording a slice decision. The status-only path preserves the four-case concurrency contract: active `done` elides `expected_revision` (revision-inference under `BEGIN IMMEDIATE`), active mid-lifecycle transitions (`in_progress`/`blocked`/`review`) require `expected_revision`, archived-snapshot status updates remain revisionless. Replaces the legacy `update_task_status` tool. |
| `get_handoff_state` | query | yes | Canonical task-state read. `sections` accepts a comma-separated subset of task-state sections; `active` and `limits` remain always included. `detail` accepts `full` (default) or `summary` to truncate long rationale and verification fields without changing the default payload shape. internal layered read controls: `read_profile` selects a stable named shape (`identity`, `hot_summary`, `review_packet`, `open_items`, `full_debug`); explicit `sections`/`detail`/`top_n_*` override profile defaults. `response_budget_bytes` enables the server-side budget planner (paired with `budget_policy="warn"|"auto_summary"|"fail"`; default `warn`, or `auto_summary` when a budget is set without an explicit policy). Responses surface `data.read_shape.applied_profile` and `data.read_budget = {requested_bytes, policy, estimated_initial_bytes, estimated_after_bytes, applied_reductions[], omitted_sections[], over_budget_after, retry_with?}` when shaping was applied. |
| `record_event` | action | no | Appends decision/test-result/blocker state through a typed `event` payload. `event.event_kind` selects the variant and required fields. |
| `next_actions` | action | no | Typed next-actions domain surface. `action.operation` selects `list`, `add`, `update`, `complete`, or `skip`. |
| `review_findings` | action | no | Typed review-findings domain surface. `review.operation` selects `record`, `batch_record`, `update`, `resolve`, `repair_provenance`, `merge`, or `list`. `resolve` is the commit-backed reconciliation path: it classifies selected open findings against the current workspace commit, marks only eligible findings `fixed`, reports `pending_uncommitted` when the fix is not yet commit-backed, and refuses ambiguous root-workspace writes instead of guessing. Any fixed-finding mutation through this path refreshes `DASHBOARD.txt` in the same action. `merge` is coordinator-centric and additive: it copies findings from `source_task_refs` under `target_task_ref` with `merged_from` provenance while **preserving each source row's `status` and resolution metadata** (plain `batch_record` still reopens to `open`). |
| `review_runs` | action | no | Typed review-runs domain surface. `review.operation` selects `record`, `list`, or `coverage`. |
| `integrity_check` | generator | yes | Typed integrity-check domain surface. `payload.kind` selects `working_tree` (workspace-vs-HEAD diff against an allowlist), `post_merge` (post-fast-forward diff against `merged_sha` and an explicit allowed-changes list), or `close` (handoff close-readiness verdict). Replaces the legacy `working_tree_integrity_check`, `post_merge_integrity_check`, and `handoff_close_check` tools. |
| `render_handoff` | generator | no | Compound renderer. `kind="current_task"` writes the v2 workspace-summary JSON to `CURRENT_TASK.json` by default. The payload carries `schema_version: 2` and one of three shapes — `single` (one live task owns the workspace; `task_ref` + compact `active` identity block), `workspace_ambiguous` (multiple live tasks on the same workspace path; `tasks[]` descriptors), or `none` (no live task resolvable). Detailed per-task sections (full decisions, tests, findings, blockers, actions, lanes) are **not** embedded — read them via `get_handoff_state` / `load_session` for task-state, or `render_handoff(kind='dashboard')` / `DASHBOARD.txt` for the cross-task observatory. `kind="dashboard"` renders the human observatory view and writes pure-ASCII `DASHBOARD.txt` by default (All Tasks table, Needs Attention, Open Findings, Deferred/Won't Fix, and registered extension sections such as Lane Health / Worker Status from `workbay-orchestrator-mcp`). Pass `write_file=False` for the **canonical pure-read path** (no DB mutation, no file mtime change): the rendered v2 workspace-summary payload is returned in the envelope's `current_task_json` field. Client-side lifecycle handlers (`task_start.py`, `context.py`, `task_finish.py`, `shell_out.py` in `workbay-system`) consume this entry point via `mcp-workbay-handoff render-handoff --kind=current_task --no-write` rather than reading `CURRENT_TASK.json` directly — keeps readers consistent with the internal derive-on-read contract. |
| `export_handoff_state` | generator | yes | Produces portable snapshot output. |
| `import_handoff_state` | action | no | Imports snapshot into local DB; destructive in replace modes. |
| `archive` | action | no | Typed archive domain surface. `payload.operation` selects `archive` (snapshot active state into archive storage), `gc` (garbage-collect tombstoned/expired archive rows; pass `apply=True` to commit), or `get` (fetch an archived snapshot, optionally without the embedded snapshot when `include_snapshot=False`). Replaces the legacy `archive_task_state`, `tasks_gc`, and `get_archived_task` tools. |
| `get_verified_tests` | query | yes | Lists verified test rows with optional task, lane, branch, commit, and pass/fail filters. |
| `load_session` | query | yes | **Compound**: calls `get_handoff_state` + `review_findings(review={"operation":"list","status":"open"})` + the `touched_files(payload={"operation":"list",...})` ledger read in one invocation. Use at session start to minimise round trips. `sections` is passed through only to the nested `state` payload from `get_handoff_state`; `detail` is passed through to both nested state and findings; `top_n_touched_files` (default 20, max 200) bounds the additive `touched_files` list. Defaults preserve the pre-parameterization full payload behavior. internal: accepts the same `read_profile`/`response_budget_bytes`/`budget_policy` parameters as `get_handoff_state`. The compound budget is split across state, open_findings, and touched_files so a single round trip lands within the requested ceiling; the response carries the same `data.read_shape` and `data.read_budget` blocks. |
| `close_slice` | action | no | **Compound**: records a slice-complete decision, re-applies the active task as `in_progress`, and regenerates `CURRENT_TASK.json` plus `DASHBOARD.txt`. Requires `expected_revision` when the target task is currently active. Accepts the same optional `changed_files` list as the decision variant of `record_event` and passes it through to the nested decision write. |
| `audit_decision_ids` | query | yes | Audits recent decision IDs for grammar conformance. Returns canonical/malformed/freeform classifications per ID. |
| `list_handoff_rows` | query | yes | Operator/debug listing of raw handoff ledger rows by status filter. CLI mirror: `mcp-workbay-handoff handoff-rows --status-filter <filter>`. |
| `validate` | query | yes | Side-effect-free preflight validator. `payload.kind="decision_id"` validates a slice-complete decision id against the server-published registry; `payload.kind="write"` validates an arbitrary write payload against the registry's required-fields and field-grammar rules. Replaces the legacy `validate_decision_id` and `validate_write` tools. |
| `artifacts` | action | no | Typed artifacts domain surface. `payload.operation` selects `record`, `search`, `get`, or `purge` for indexed artifact rows. Search mode supports both ranked hits and source-list mode when `queries` is omitted or empty; get mode supports `include_terms=true`. |
| `touched_files` | action | no | Typed touched-files ledger surface. `payload.operation` selects `record` (append a touch row with `file_path` + `change_kind`) or `list` (paginated read scoped to the active task by default). `change_kind` is a closed enum announced in the input schema: `edit` (modified content / git `M`), `add` (new file / git `A`), `delete` (removed / git `D`) — use `edit` for any in-place modification (there is no `modified` value). Replaces the legacy `record_file_touch` and `get_touched_files` tools. |
| `compaction` | action | no | Typed session-compaction surface. `payload.operation` selects `record` (compact a transcript file for a `task_ref` + `harness` + `session_id`), `get` (dereference a stable `compaction_id`), or `get_latest` (return the latest compaction summary for `task_ref`). Replaces the legacy `compact_session`, `get_compaction`, and `get_latest_compaction` tools. |
| `search_handoff` | generator | yes | Returns ranked snippets over handoff FTS tables, including verified test evidence. `detail` accepts `full` (default) or `summary`, and `fields` accepts a comma-separated per-result projection. |

Cross-task and review-summary tools (`switch_task`, `get_latest_slice_review_packet`, `get_review_findings_summary`, `reconcile_review_findings`) are registered on `workbay-orchestrator-mcp`. See [`workbay-orchestrator-mcp.md`](workbay-orchestrator-mcp.md).

`reconcile_review_findings` remains the orchestrator-owned integrity/dedup summary surface. It does not close findings. Use `review_findings(review={"operation": "resolve", ...})` when the goal is commit-backed finding disposition.

Preferred review-intake path when orchestrator is loaded:

1. `get_latest_slice_review_packet`
2. `get_review_findings_summary` or `review_findings(review={"operation":"list","status":"open"})` as needed

Agent investigation rule:

- Use the MCP review/intake surfaces above before inspecting `.task-state/handoff.db` directly.
- Preferred escalation order for open-issue lookup is: `get_latest_slice_review_packet` -> `get_review_findings_summary` -> `review_findings(review={"operation":"list","status":"open"})` -> `load_session` / `search_handoff`.
- Drop to raw SQLite or filesystem inspection only when the host session does not expose the required MCP tool, or when debugging an MCP transport/serialization failure.

Handoff-only fallback:

1. `load_session`
2. `search_handoff(queries=["slice_complete"], record_types=["decision"], limit=1)`
3. `get_verified_tests(task_ref=..., commit_sha=...)`
4. `review_findings(review={"operation":"list","status":"open"})`

This is a degraded multi-call fallback for sessions where orchestrator is unavailable. `workbay-handoff-mcp` does not expose a parallel compound `get_review_packet` surface.

## Typed Input Conventions: Announce Closed Value Sets In-Schema

Extends the internal typed-tool-surface ADR (`packages/mcp-workbay-handoff/docs/tasks/internal-typed-tool-surface-consolidation-adr-task-plan.md`). The typed ops already give every tool one Pydantic-validated input surface; this convention governs how a tool's **closed value sets** are surfaced to agents.

- **Announce closed value sets in the input schema via `Literal`.** Any parameter whose accepted values are a fixed, closed set (typically backed by a `StrEnum` in `src/workbay_handoff_mcp/`) is typed `Literal[...]`, not bare `str`, so the generated tool input schema carries a JSON-schema `enum`. An agent introspecting the schema sees the valid values without invoking the tool. Bare `str` is reserved for genuinely open text (sessions, ids, paths, free-form labels, content).
- **Runtime envelope errors are recovery, not the discovery channel.** A domain helper may still coerce/validate the value at runtime (e.g. `ChangeKind(value)` → error envelope) as defense-in-depth for the CLI and direct-Python callers, where Pydantic schema validation does not run. That guard is a fallback after a failed call, not the place values are first learned — the schema `enum` is the primary, pre-call discovery surface.
- **Name the canonical↔colloquial mapping in the description when they diverge.** When the canonical value differs from the term a caller will reflexively reach for, the parameter description states the mapping so the caller routes to the canonical value without guessing.
- **Keep CLI choices and the schema `enum` in parity.** A CLI `ArgSpec(..., choices=[...])` and the MCP `Literal` express the same set; a regression test binds the announced schema `enum` to the `StrEnum` source of truth so the two cannot drift.

Worked example — `touched_files` `change_kind` (internal): the field is `Literal["edit","add","delete"]` (announced as a schema `enum`), backed by `ChangeKind` (`touched_files.py`). The description maps git vocabulary — `edit` = modified content (git `M` / "modified"), `add` = new file (git `A`), `delete` = removed (git `D`) — because agents reflexively reach for git's "modified". The `record_file_touch` runtime guard still rejects unknown values for the CLI/direct path, and `test_change_kind_schema_enum_bound_to_changekind_source_of_truth` binds the schema `enum` to `CHANGE_KIND_VALUES` so adding a `ChangeKind` member without extending the announced `Literal` fails CI. As of internal, `change_kind` was the only closed-enum input field still typed as bare `str`; the other closed enums (`status`, `severity`, `verdict`, `review_mode`, `subject_kind`, terminal-guard `decision`, and the `operation`/`kind` discriminators) already use `Literal`.

## Live vs Archived Statuses

`HANDOFF_ACTIVE_STATUSES` is the union of valid `handoff_state.status` values
(`in_progress`, `review`, `blocked`, `done`). It is the *write-validation*
contract: any status persisted to a row must belong to this set.

`LIVE_ACTIVE_STATUSES` is the narrower *resolver/renderer* contract — the
subset (`in_progress`, `review`, `blocked`) that counts as "live" for active-task
selection. The workspace-path resolver, `render_handoff(kind='current_task')`,
the `plans-list` surface, and the `make task-start` lifecycle handler all
enumerate candidates through this set so that:

- a `status=done` row is never promoted into `CURRENT_TASK.json.active`, even
  when no live row exists for the workspace;
- after `archive(payload={"operation": "archive", "active_cleared": True})`, the next renderer pass
  writes `task_ref=null` instead of re-promoting another stale `status=done`
  row;
- `make task-start TASK=<NEW>` succeeds against a workspace whose only
  surviving rows are `status=done`.

Use `list_handoff_rows(status_filter=...)` (CLI: `mcp-workbay-handoff handoff-rows
--status in_progress review blocked`) when tooling needs to enumerate live
rows. Pass no filter to return every non-archived row for diagnostic use.
Archived rows live in `task_archives` and are excluded from this surface;
read them via `archive(payload={"operation": "get", "task_ref": ...})`.

## Status Update Ergonomics

`set_handoff_state(status_only=True, ...)` requires `expected_revision`
for mid-lifecycle transitions (`in_progress`, `review`, `blocked`) so
concurrent writers cannot silently flip status backwards under a stale
view of the row.

The single carve-out is `status="done"`. As an end-of-lifecycle
transition, it elides the `expected_revision` requirement: the server
opens `BEGIN IMMEDIATE`, reads the current revision under the write
lock, and uses that value as the optimistic-concurrency guard. Callers
may still pass `expected_revision` explicitly to keep the strict
stale-write check; an explicit stale revision still rejects.

This asymmetry exists so `make task-finish` and other cold-start
ceremonies do not need to chain `get_handoff_state(sections="identity")`
before every status update.

## Cascade Archive of internal Rows

`archive(payload={"operation": "archive", "task_ref": "<internal>", "cascade_maint_review": False})`
is non-cascading by default. Pass `cascade_maint_review=True` (CLI:
`mcp-workbay-handoff archive --operation archive --cascade-maint-review`)
to also archive every non-archived
`internal-*` row whose `objective` or `task_plan_path`
references the parent `task_ref`. The cascade and a single
`cascade_archive` decision (session=`archive_cascade`) are written in
the same transaction as the parent archive. The decision rationale
lists every cascade-archived `task_ref` for audit. The response payload
exposes the list under `cascade_archived`.

Lifecycle close-sequence handlers and the `make tasks-gc` janitor pass
`cascade_maint_review=True` explicitly when they want planning-review
cleanup; direct callers preserve legacy non-cascading behavior unless
they opt in.

## Review-Finding Resolution

Use `review_findings(review={"operation":"resolve", ...})` or CLI parity through `mcp-workbay-handoff review-findings --operation resolve` when a code change should close existing review findings.

Rules:

- Pass explicit `finding_ids` or `--resolve-finding-id` values, or set `all_open=true` / `--all-open` to reconcile all open findings for the resolved task.
- Root workspaces with multiple live tasks do not guess. The action fails with the existing ambiguity error until the caller supplies `task_ref` or runs from the registered target worktree.
- A finding moves to `fixed` only when the current workspace commit satisfies the existing commit ancestry guard. Clean same-commit and descendant-commit fixes are eligible.
- Uncommitted fixes are reported as `pending_uncommitted` and the finding remains open.
- Failed or blocked resolution attempts do not require a substitute blocker row or no-op verified-test row. The resolution receipt is the durable explanation surface.

Example outcomes:

- `fixed`: the current committed fix is the finding commit or a valid descendant; the finding is updated with `verified_commit_sha` and `DASHBOARD.txt` is refreshed.
- `pending_uncommitted`: the workspace appears to contain the fix, but tracked changes are still uncommitted; the finding stays open.
- `blocked_by_context`: the current commit is not the finding commit or a valid descendant, or the required verification commit is missing.
- ambiguous-root failure: when multiple live tasks are active and no unique task can be resolved, the action refuses to mutate anything.

Retry guidance:

- Retry `query` and pure `generator` surfaces when the failure is transport-level, timeout-based, or due to a transient read lock.
- Do not auto-retry `action` surfaces unless the caller can prove the operation is safe to repeat.
- Treat `render_handoff` and `close_slice` as write-affecting surfaces even though they derive output from current state.

## MCP Troubleshooting Ladder

### 1. Startup Failure

Symptoms:

- `workbay-handoff-mcp` binary not found
- import or launcher failure
- wrong `--workspace-root` / `--state-dir`
- missing `.task-state` or unwritable `CURRENT_TASK.json` / `DASHBOARD.txt`

Checks:

```bash
mcp-workbay-handoff --workspace-root /path/to/repo doctor
python3 -m workbay_handoff_mcp --workspace-root /path/to/repo doctor
ls -ld /path/to/repo/.task-state /path/to/repo/.task-state/exports
```

Recovery:

- fix the executable or `PYTHONPATH`
- point the client at the real workspace root
- create or repair the workspace-owned state directories

### 2. Capability Discovery Failure

Symptoms:

- tool appears in docs but not in the client
- wrapper or skill references stale tool names
- adapter launches the wrong server entrypoint

Checks:

```python
from workbay_handoff_mcp.api import TOOL_DESCRIPTIONS
print(len(TOOL_DESCRIPTIONS))
print(sorted(TOOL_DESCRIPTIONS))
```

Recovery:

- treat the installed `workbay-handoff-mcp` package and this contract as the live source of truth for the ledger surface in this monorepo
- update stale docs, skills, or wrappers in the same slice
- prefer minimal valid payloads when a write bounces on signature drift

### 3. Runtime Execution Failure

Symptoms:

- optimistic revision mismatch
- SQLite lock or FTS5 errors

Checks:

```bash
mcp-workbay-handoff --workspace-root /path/to/repo doctor
mcp-workbay-handoff --workspace-root /path/to/repo state
```

Recovery:

- refresh the expected revision before retrying write operations
- treat FTS5 errors as environment/runtime issues first, not search-contract bugs

### 4. Evidence-Write Failure

Symptoms:

- a decision, finding, or test write is described in prose but not persisted
- `CURRENT_TASK.json` is out of sync with handoff state
- review close checks fail because fresh verification evidence is missing

Checks:

```bash
mcp-workbay-handoff --workspace-root /path/to/repo doctor
mcp-workbay-handoff --workspace-root /path/to/repo state
mcp-workbay-handoff --workspace-root /path/to/repo review-findings --operation list
```

Recovery:

- reissue the write with the live signature and minimal valid payload
- record verification with `record_event(event={event_kind=\"test_result\", ...})` instead of prose-only rationale
- regenerate `CURRENT_TASK.json` after decision writes when the workflow requires it

## Artifact Read Shaping

The artifact read surfaces now support the same additive compact-read pattern used by the handoff state and review-finding reads:

```python
artifacts(
    artifact={
        "operation": "search" | "get",
        ...
    }
) -> str
```

- `detail="summary"` truncates long artifact text fields (`summary`, `source_summary`, `snippet`, and `metadata_json`) without changing the default full-detail behavior.
- `artifacts(operation="get", detail="summary")` also returns only the first three chunk previews while preserving `chunk_count` for the full source.
- `fields` is a comma-separated projection over the per-row payload. `artifacts(operation="search")` interprets it against the active mode:
  - search mode: hit fields such as `source_id`, `source_label`, `title`, `snippet`
  - source-list mode: source fields such as `id`, `task_ref`, `source_label`, `summary`
  - artifact fetch: source fields such as `source_label`, `chunk_count`, `chunks`
- Invalid field names are stripped. If none remain, the tool falls back to a compact identity shape instead of failing.

## Structured Handoff Search (`search_handoff`)

`search_handoff` provides BM25/FTS5 full-text search over the five canonical handoff record
tables (decisions, review findings, blockers, next actions, and verified tests) stored in `handoff.db`.

### FTS5 Shadow Tables

Five FTS5 virtual tables are maintained in `handoff.db` alongside the canonical tables:

| FTS table       | Source table      | Indexed body                                     | Status column |
| --------------- | ----------------- | ------------------------------------------------ | ------------- |
| `decisions_fts` | `decisions`       | `decision \|\| ' ' \|\| COALESCE(rationale, '')` | no            |
| `findings_fts`  | `review_findings` | `description \|\| ' ' \|\| COALESCE(fix, '')`    | yes           |
| `blockers_fts`  | `blockers`        | `description`                                    | yes           |
| `actions_fts`   | `next_actions`    | `action`                                         | yes           |
| `verified_tests_fts` | `verified_tests` | `command \|\| ' ' \|\| COALESCE(result, '')` | no            |

All tables use `tokenize='porter unicode61'`, `record_id UNINDEXED`, `task_ref UNINDEXED`, and
`lane_id UNINDEXED` so that scope filters (`task_ref`, `lane_id`) are fast equality lookups
without touching FTS ranking.

### Trigger Maintenance

Fifteen SQL triggers (INSERT / UPDATE / DELETE for each source table) keep FTS tables in sync
automatically. UPDATE triggers follow the DELETE-then-INSERT pattern to prevent stale rows. All
triggers use `CREATE TRIGGER IF NOT EXISTS` so they are schema-idempotent.

`_ensure_handoff_fts(conn)` is called on every `_get_db_connection()` call. It:

1. Probes FTS5 availability (CREATE/DROP `_fts5_handoff_probe`); silently returns on failure.
2. Creates the five FTS5 virtual tables if not already present.
3. Creates the fifteen triggers if not already present.
4. Runs `_backfill_handoff_fts(conn)`: for each source/FTS pair, if source has rows but FTS is
   empty, bulk-inserts all source rows into the FTS table (handles cold-start upgrades).

FTS5 unavailability degrades silently so existing handoff operations are never blocked. Call
`workbay-handoff-mcp doctor` to verify FTS5 is available.

### Tool Signature

```python
search_handoff(
    queries: list[str],
    task_ref: str | None = None,
    lane_id: str | None = None,
    record_types: list[str] | None = None,  # subset of ["decision", "finding", "blocker", "action", "verified_test"]
    limit: int = 20,                         # max 200
    detail: str = "full",
    fields: str | None = None,
    decision_fields: list[str] | None = None,
) -> str:
```

- **queries**: One or more search terms. Multiple terms are OR-joined. Multi-word terms are
  automatically phrase-quoted (`"term with spaces"`) for precise adjacency matching.
- **record_types**: Defaults to all five types when omitted.
- **limit**: Clamped to [1, 200]. Results across all searched types are merged and re-ranked.
- **detail**: `full` preserves the compact FTS snippet returned by SQLite. `summary` truncates that snippet further for startup-friendly reads.
- **fields**: Optional comma-separated projection over result rows, for example `record_type,snippet`.
  Validated against the global allowlist `{record_type, record_id, task_ref, lane_id, status, snippet}`;
  values outside the allowlist are silently dropped.
- **decision_fields**: Optional **decision-scoped** projection. When provided, the named columns
  from the `decisions` table are merged onto result rows whose `record_type == "decision"`. Allowed
  values: `decision`, `rationale`, `branch`, `commit_sha`, `lane_id`, `created_at`, `agent`,
  `model`, `model_label`, `reasoning_level`, `changed_files_json`. Decision-only fields are **not**
  reachable through the global `fields` parameter; non-decision rows in mixed searches retain the
  global projection unchanged. Use this for decision discovery when callers need provenance
  (branch, commit, lane) on the result row, not just the FTS snippet.

### Response Shape

```json
{
  "ok": true,
  "results": [
    {
      "record_type": "decision",
      "record_id": 42,
      "task_ref": "my-task",
      "lane_id": "api",
      "status": null,
      "snippet": "...exponential backoff retry policy..."
    }
  ],
  "total": 1,
  "query": "\"exponential backoff\"",
  "record_types_searched": ["action", "blocker", "decision", "finding", "verified_test"]
}
```

- `status` is `null` for decisions (no status column); `open` / `fixed` / etc. for others.
- `snippet` uses FTS5 `snippet()` with a 12-token window; result is compact, not full body.
- Results are sorted by BM25 rank (best match first); ties break by insertion order.
- Invalid `fields` values are stripped. If none remain, the result rows fall back to `record_type`, `record_id`, `task_ref`, and `snippet`.

### CLI Subcommand

```bash
mcp-workbay-handoff --workspace-root <repo> handoff-search \
    --query "retry policy" \
    --query "circuit breaker" \
    --task-ref my-task \
    --lane-id api \
    --record-types decision finding \
    --limit 10
```

`--query` is repeatable; multiple `--query` flags are OR-joined.

### Error Cases

- `queries` is `None` or all strings are blank: returns `{"ok": false, "error": "..."}`.
- Any `record_types` entry is not in `["decision", "finding", "blocker", "action", "verified_test"]`: returns error.
- FTS5 tables not initialized (FTS5 unavailable): returns `{"ok": false, "error": "..."}`. Run
  `doctor` to diagnose.

## Decision Read Surface

The decision ledger is reachable through two complementary tools that together cover the full operator workflow: **discover** decisions by content with `search_handoff`, then **exact-read** them by id/branch/lane with `get_handoff_state`. Both halves share the same allowlist of decision-table columns so a discovery hit can be re-fetched by exact id without re-deriving the projection.

Allowlist (used by both `search_handoff.decision_fields` and `get_handoff_state.decision_fields`):
`decision`, `rationale`, `branch`, `commit_sha`, `lane_id`, `created_at`, `agent`, `model`, `model_label`, `reasoning_level`, `changed_files_json`.

### Discovery (`search_handoff.decision_fields`)

FTS-ranked discovery across the decisions ledger. Use when the operator knows what they want by topic but not by id — for example, "find the slice-complete decision that mentions the retry-policy refactor."

```python
search_handoff(
    queries=["retry policy"],
    record_types=["decision"],
    decision_fields=["decision", "branch", "commit_sha", "lane_id"],
    limit=10,
)
```

Decision rows in the result envelope gain the named columns alongside the FTS `snippet`. Non-decision rows in mixed searches retain the global projection unchanged. Decision-only fields are not reachable through the global `fields` parameter — they must come through `decision_fields`. See [Structured Handoff Search](#structured-handoff-search-search_handoff) for the full signature.

### Exact Read (`get_handoff_state.decision_*`)

Bounded exact-read of `decisions_recent` by id, branch, commit, or lane — no FTS, no ranking. Use when the operator already knows which decision row(s) they want and needs the full provenance fields without paging through search results.

```python
get_handoff_state(
    task_ref="internal",
    sections="decisions_recent",
    decision_id_prefix="codex_slice_complete_internal_36_",
    decision_fields=["decision", "branch", "commit_sha", "rationale"],
    detail="summary",
)
```

```python
get_handoff_state(
    task_ref="internal",
    sections="decisions_recent",
    decision_branch="feature/internal-36-decision-read-surface-parameterization",
    decision_lane_id="internal-36",
)
```

`decision_id_prefix` matches literally — SQL `LIKE` wildcards (`_`, `%`, `\\`) in the prefix are escaped before query, so a prefix of `codex_slice_complete_internal_36_` does not match unrelated ids that happen to share the underscore positions. Passing any `decision_*` parameter when `sections` excludes `decisions_recent` returns `ok=false` with a parameter error. Default callers (none of the five `decision_*` parameters set) receive the unchanged `decisions_recent` shape; `_apply_detail` still runs even when `decision_fields` narrows the row, so `detail="summary"` continues to truncate `rationale`. See [Request Shape Notes](#request-shape-notes) for the full parameter list.

## Verified Test Read Surface (`get_verified_tests`)

`get_verified_tests` returns verified test rows from the handoff ledger without requiring a broader dashboard read.

```python
get_verified_tests(
  task_ref: str | None = None,
  lane_id: str | None = None,
  branch: str | None = None,
  commit_sha: str | None = None,
  passed: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> str
```

- Results are ordered by `verified_at DESC, id DESC` for deterministic newest-first reads.
- Filters are additive; combine `branch`, `commit_sha`, and `passed` to inspect the exact verification rows tied to a merge candidate.
- The envelope includes `total_matching`, `returned`, `has_more`, and `tests`.

### Response Shape

```json
{
  "ok": true,
  "total_matching": 1,
  "returned": 1,
  "has_more": false,
  "tests": [
    {
      "id": 42,
      "task_ref": "my-task",
      "lane_id": "api",
      "branch": "feature/my-task",
      "commit_sha": "0123456789abcdef0123456789abcdef01234567",
      "command": "python -m pytest packages/mcp-workbay-handoff/tests/test_schema_migrations.py -q",
      "passed": true,
      "verified_at": "2026-04-10 03:20:23"
    }
  ]
}
```

- `tests` entries return the stored verification row data rather than FTS snippets.
- Filter combinations narrow the result set without changing the envelope shape.


## Request Shape Notes

- Most write tools accept optional `task_ref`. When omitted, they target the active task as a fallback.
- In concurrent or multi-task workflows, pass `task_ref` explicitly on writes instead of relying on ambient active-state routing.
- Live MCP tool signatures are authoritative over examples, templates, or prior-session memory. Prefer the minimal valid payload for write operations unless a richer payload is required by the current signature.
- If a write call fails validation, treat it as signature drift. Retry once with the minimal payload accepted by the live signature, then update the stale contract/rule/template in the same slice so the bounce does not recur.
- Slice-completion decisions must follow the server-owned registry published at `get_handoff_state(sections="identity").data.limits.write.slice_complete_decision_id` and the side-effect-free `validate(payload={"kind": "decision_id", "decision": ..., "decision_kind": "slice_complete"})` preflight. The registry is authoritative for the canonical form, regex, segment rules, and examples; prose here intentionally points at that data instead of restating the regex. Valid: `cdx_slice_complete_plan0004_contract_pinning_and_docs`. Invalid: `cdx_slice_complete_plan0004_contract-pinning-and-docs`. The legacy `slice_complete_<short_label>` format remains read-compatible for historical rows only. Structured rationales still require the four headings `## Changes`, `## Verification`, `## Schema / Contract Changes`, and `## Open Threads`.
- `record_event(event={event_kind="decision", ...})` requires a `session` string in the nested decision variant (MCP path) or `--session` flag on the `event --event-kind decision` CLI path. Use a stable, human-readable identifier such as `"<agent>-<task-slug>"` or `"<agent>-<short-description>"`. The field is NOT auto-populated from context; omitting it causes validation failure.
- The `decision` variant of `record_event(...)` rejects slice-complete writes at write time when the rationale is missing those headings or any section is empty. This is enforced before the row is inserted.
- The `decision` variant of `record_event` accepts optional `changed_files` (list of monorepo-relative paths touched by this slice). Stored as `changed_files_json` on the decision row. When present, the slice-review packet uses this list directly instead of parsing file paths from the rationale text. Pass this parameter on every slice-completion decision to give reviewers an explicit, structured scope.
- Historical decision rows that predate the prefixed naming scheme are grandfathered. MCP read paths (close-check, slice-review packet, handoff search) recognize both formats. Do not plan retroactive renames of historical rows.
- The structured rationale is mandatory even for docs-only slices. Use `- none.` for empty sections rather than omitting headings.
- Handoff consumers should treat prose-only completion decisions as malformed process output that must be corrected before the slice is considered fully handed off.
- To switch between tasks, use `switch_task(task_ref)` on `workbay-orchestrator-mcp`. It auto-archives the outgoing task (full snapshot) and activates the target, restoring the objective from its archive when not provided. Idempotent if the target is already active.
- For in-place updates to the _current_ task (status, objective change, focus update), use `set_handoff_state(...)` directly.
- `set_handoff_state` requires `expected_revision` for updates. Accepts optional `focus` for mutable per-slice working context. `objective` is optional on updates (preserved when omitted). `focus` is preserved when omitted on updates; pass an empty string to clear it explicitly.
- `close_slice` is a slice-completion helper, not a task-closure helper. It keeps the target task `in_progress` and now preflights the active-task revision guard before recording a decision.
- Use `set_handoff_state(task_ref=..., status=..., expected_revision=..., status_only=True)` when you need to mark a task `done` or otherwise correct status without writing a slice-completion decision. Archived-task updates do not require `expected_revision` because they update the archived snapshot rather than the live singleton row.
- The shared actor shape may include `model`, `model_label`, `reasoning_level`, and `lane_id` in addition to `agent`, `branch`, and `commit_sha`. Only decisions persist the granular model fields today; other write surfaces continue to persist `agent` plus git provenance.
- `build_write_actor(agent=None, model=None, model_label=None, reasoning_level=None, branch=None, commit_sha=None, lane_id=None) -> WriteActor` is the public helper for constructing that normalized actor payload before passing it into write tools.
- `build_write_actor` derives the canonical `agent` display identity from model provenance when available: `"{model_label} {reasoning_level}"` when both are present, `model_label` when only the label is known, and the caller-provided `agent` only as a legacy fallback.
- Known model labels are normalized for common backends (`claude-opus-4-0520` -> `Opus 4.6`, `claude-sonnet-4-20250514` -> `Sonnet 4`); unknown models pass through unchanged.
- Decision rows now persist nullable `model`, `model_label`, and `reasoning_level` columns alongside `agent`. Treat the turn-metrics ledger on `workbay-orchestrator-mcp` as the canonical source for token consumption; decision rows carry model provenance only and do not duplicate per-turn token columns.
- `record_event` and `next_actions` accept optional `task_ref`, matching the existing cross-task targeting pattern. For `record_event`, `task_ref` lives inside the typed `event` payload.
- Write responses for `record_event` and `next_actions` echo the resolved `task_ref`. Treat that field as the authoritative write target in multi-agent flows.
- `review_findings(review={"operation":"record", ...})` accepts optional `details={ line_start?, line_end?, fix? }`.
- `review_findings(review={"operation":"record", ...})` also accepts optional `review_mode` with values `branch`, `release_audit`, or `planning`.
- `review_findings(review={"operation":"record", ...})` accepts `task_ref="__repo__"` to record a repo-scoped finding that is not owned by any one implementation task. Task-scoped listing queries exclude `__repo__` rows unless repo scope is explicitly included.
- `review_findings(review={"operation":"batch_record", ...})` accepts `session`, `findings` (list of `BatchFindingItem`), optional `actor`, and optional `task_ref`. Each `BatchFindingItem` requires `finding_id`, `severity`, `file_path`, and `description`; `review_mode` and `details` are optional. Maximum 100 items per call; larger batches return `ok: false` without writing. All items are pre-validated (severity, review_mode, required fields) before the transaction opens — a single invalid item rejects the entire batch. Returns `{ ok, task_ref, written, results: [{ finding_id, action, reopened? }] }`. Use this operation instead of repeated single-record writes when logging 3 or more findings in a single review pass.
- `review_findings(review={"operation":"merge", ...})` accepts `source_task_refs` (non-empty list), `target_task_ref`, and optional `session`. Copies every finding row from the sources under the coordinator `target_task_ref`, stamps `merged_from` provenance, and **preserves each source row's `status` plus resolution/lifecycle metadata** (`resolved_at`, `resolution_notes`, `verification_evidence`, `resolved_on_branch_*`, `integrated_at_*`). Source rows remain intact; re-merge is an idempotent upsert that does not reopen non-open findings and never clobbers a coordinator-local disposition: when the existing coordinator copy already carries a non-open status (e.g. the coordinator marked it `fixed` or `deferred`), the coordinator's status and resolution metadata are preserved over the source row's values. Reopen history (`reopen_count`, `last_reopen_reason`, `last_reopened_at`) is carried from source rows onto the coordinator copy. Returns `{ ok, task_ref, session, source_task_refs, written, results }`. Empty sources or missing findings return `ok: false` with a validated error.
- `get_handoff_state` accepts optional `sections` and `detail` on task views. `sections` is a comma-separated subset of task-state sections; invalid names are silently dropped, and if no valid names remain the response contains only identity data (`active` + `limits`, no data sections). The reserved token `sections="identity"` explicitly requests the same identity-only shape; when present it takes precedence over any other section names. `active` and `limits` are always included and are not selectable or suppressible. Pass `sections=None` (the default) to receive the full task payload. `detail="summary"` truncates long rationale, command, result, and finding text fields while keeping the default `detail="full"` response backward-compatible.
- `get_handoff_state` and `load_session` accept the internal layered read controls. **Layer 1 — read profiles:** `read_profile` selects a stable bundled shape under one of `identity` (active task + limits only), `hot_summary` (summary detail, `top_n_*=3/5/5/3/3`), `review_packet` (summary detail, `top_n_*=20/20/20/5/5`, additive add-ons capped at 20), `open_items` (full detail, `blockers_open`/`actions_pending`/`findings_open` required and never omitted by `auto_summary`), or `full_debug` (legacy full shape; the only profile the budget planner may fully reshape). Explicit `sections`/`detail`/`top_n_*` parameters override profile defaults. Responses surface `data.read_shape = {applied_profile, sections, detail, top_n_*}` so callers can verify the materialized shape. **Layer 2 — response budget planner:** `response_budget_bytes: int | None` pairs with `budget_policy: "warn" | "auto_summary" | "fail" | None`. The effective default policy is `warn` when no budget is supplied and `auto_summary` when a budget is supplied without an explicit policy. `auto_summary` reduces detail to `summary`, halves `top_n_*` (capped at the requested profile's per-section minimums; required sections of `open_items` are never omitted), and finally drops optional sections in priority order until the estimated payload fits — all before heavy rows materialize. `fail` returns `ok=false` with `data.read_budget.retry_with` (a suggested `read_profile` + `response_budget_bytes` retry pair) instead of materializing an over-budget payload. The `data.read_budget = {requested_bytes, policy, estimated_initial_bytes, estimated_after_bytes, applied_reductions[], omitted_sections[], over_budget_after, retry_with?}` block is attached whenever a budget was supplied or the planner applied reductions. Default callers (no `read_profile`, no `response_budget_bytes`) receive the legacy unbounded response.
- `get_handoff_state` accepts five flat decision-scoped parameters that shape only the `decisions_recent` section: `decision_fields: list[str] | None`, `decision_branch: str | None`, `decision_commit_sha: str | None`, `decision_lane_id: str | None`, and `decision_id_prefix: str | None`. The four filter parameters apply equality (or literal prefix matching for `decision_id_prefix`, with SQL LIKE wildcards escaped) constraints to `decisions_recent` rows. `decision_fields` narrows each returned decision row to only the listed columns and is validated against the same allowlist used by `search_handoff.decision_fields` (`decision`, `rationale`, `branch`, `commit_sha`, `lane_id`, `created_at`, `agent`, `model`, `model_label`, `reasoning_level`, `changed_files_json`); invalid names return `ok=false`. Passing any of the five parameters when `decisions_recent` is excluded from `sections` returns `ok=false` with a parameter error. Default `get_handoff_state` callers (none of these parameters set) receive the unchanged response shape.
- `review_findings(review={"operation":"update", ...})` accepts exactly one of `finding_id` or `finding_db_id`.
- `review_findings(review={"operation":"update", ...})` requires `resolution_notes` for `wontfix` and `deferred`.
- `review_findings(review={"operation":"update", ...})` requires `reopen_reason` when changing a non-open finding back to `open`.
- `review_findings(review={"operation":"update", ...})`: when `task_ref` is omitted and `finding_id` or `finding_db_id` is provided, the lookup is global. If exactly one row matches, the update is applied to that row regardless of active task. If multiple rows share the same `finding_id`, an explicit ambiguity error listing the candidate scopes is returned.
- `review_findings(review={"operation":"list", ...})`: when `finding_id` or `finding_db_id` is provided and `task_ref` is omitted, the lookup is global — the active-task fallback is skipped. If more than one row shares the same `finding_id` across different task scopes, an explicit ambiguity error is returned. To scope the lookup to a specific task, pass `task_ref` explicitly.
- `finding_id` naming convention: prefix with the owning task-ref or review scope to minimize cross-scope collisions (e.g., `internal`, `REVIEW-COVERAGE-internal`). Global uniqueness is not schema-enforced; ambiguity errors serve as the collision safety net. Repo-scoped findings should use the `__repo__` task-ref prefix or the review subject path as the prefix.
- `review_findings(review={"operation":"list", ...})` accepts optional `review_mode`; `branch` includes rows where `review_mode IS NULL` for backward compatibility.
- `review_findings(review={"operation":"list", ...})` accepts optional `detail="full"|"summary"`. Summary mode truncates long `description`, `fix`, `resolution_notes`, and `verification_evidence` fields while preserving the same filters, counts, and lookup rules.
- `load_session` accepts optional `sections`, `detail`, and `top_n_touched_files`. `sections` is passed only to the nested `state` payload returned by `get_handoff_state`; `detail` is passed to both `get_handoff_state` and `review_findings(review={"operation":"list","status":"open"})` so the combined response can be trimmed without changing default compatibility behavior. `top_n_touched_files` (default 20, max 200) bounds the additive `touched_files` list returned alongside `state`, `open_findings`, and `open_findings_count`.
- `load_session` accepts the same internal layered controls as `get_handoff_state` (`read_profile`, `response_budget_bytes`, `budget_policy`). The budget planner treats the compound payload (state + open_findings + touched_files) as a single ceiling: open-findings caps and `top_n_touched_files` are reduced alongside the nested state's `top_n_*` so the final response fits within the requested byte budget in one round trip. `data.read_shape` and `data.read_budget` describe the materialized shape of the compound response.
- `load_session(include_context_refresh=True)` adds additive `data.context_refresh` with the same top-level keys as before (`available`, `reason`, `dedupe_key`, `packet`). When a new packet is available, `packet.semantic_reinjection` carries the typed readable semantic selection result (status, skip_reason, selected snippets metadata, chars_used/budget) built from the task's objective, focus, pending actions, and `WORKBAY_REINJECT_SEMANTIC_REFRESH_BUDGET_CHARS`. Legacy `packet.rendered_cold_start` and dedupe behavior are unchanged.
- `review_runs(review={"operation":"record", ...})` requires `review_run_id` (must be globally unique in the ledger), `session`, and `subject_path`. `subject_kind` defaults to `task_plan`; valid values are `task_plan`, `epic`, `branch`, `adr`, `roadmap`, `other`. `review_mode` defaults to `planning`; valid values are `branch`, `release_audit`, `planning`. `verdict` is optional; valid values are `pass`, `pass_with_findings`, `fail`, `conditional_pass`. `verdict_decision` is optional and should hold the stable decision string from the decision variant of `record_event` (not the integer id). `task_ref` is optional and links the run to a task scope.
- `review_runs(review={"operation":"record", ...})` returns the numeric row id in `data.review_run.id` and in `mutation.affected_ids`; the human-stable `review_run_id` is returned as `mutation.affected_keys`. Agents must print the numeric id in handoff receipts (for example `review_run id 414`) because context compaction restores provenance by row id.
- `review_runs(review={"operation":"list", ...})` is unscoped by default (returns all runs). Pass `task_ref` to scope to a task, `subject_path` to scope to an artifact, `review_mode` to filter by review type, or `verdict` to filter by outcome. Max 100 per page.
- `review_runs(review={"operation":"coverage", ...})` requires at least one of `task_ref` or `subject_path`. When `task_ref` is given, finding counts come from the `task_ref` column on `review_findings`. When only `subject_path` is given, finding counts are derived via the `review_run_id` link from matching runs. Returns: `run_count`, `latest_review_run_id`, `latest_verdict`, `recent_run_ids` (last 5), `open_findings_by_severity` (dict of high/medium/low counts), `reopened_findings_count`.
- `review_runs(review={"operation":"coverage","task_ref":"REVIEW-COVERAGE"})` is a supported backward-compatible query pattern; the returned counts will be zero until repo-scoped findings are migrated from the pseudo-task bucket.
- Use `review_runs(review={"operation":"record", ...})` at the end of each planning or branch review to record the verdict and link it to the reviewed artifact. Then pass `review_run_id` on each `review_findings(review={"operation":"record", ...})` call to link findings to their run.
- When `current_commit_sha` is provided, `integrity_check(payload={"kind":"close",...})` also verifies that at least one structured `slice_complete_*` decision exists for that commit. Treat missing current-commit slice summaries as a close/review gate failure, including for docs-only slices.
- The `test_result` variant's `result` field on `record_event` is a concise verification-summary field, not a full log sink. Keep short proof lines such as `55 passed in 7.02s`, `diff-check clean`, or `REVIEW READY: READY`; store longer output in artifacts/files instead of the `verified_tests` table.
- `import_handoff_state(mode="replace_task")` rejects destructive clears unless `allow_destructive_clear=true`.
- `close_slice` requires a `session` string (same as the decision variant of `record_event`). Pass `task_ref` explicitly in multi-task flows. `focus` updates the active-task working context after the decision is recorded. `changed_files` passes through to the decision variant of `record_event` for structured review scope. The success response includes `decision` (full row) and `task_revision` (int) so callers can confirm state without a follow-up read.
- Any final user-facing response after MCP writes must include a compact write receipt using row ids from the tool responses. Format: `MCP writes: <summary>; test_result id <id>; verdict decision id <id> (<decision_key>); review_run id <id>. DASHBOARD.txt refreshed. Handoff updated: decision <decision_key> recorded.` Omit clauses that do not apply, but never replace row ids with prose-only "recorded" claims.
- `export_handoff_state` defaults to `include_markdown=False`. Pass `include_markdown=True` explicitly to embed CURRENT_TASK.json markdown in the export.
- `render_handoff(kind="current_task")` renders the v2 workspace-summary payload described in the §Default workspace-owned state intro (shapes `single`/`workspace_ambiguous`/`none`); detailed per-task sections live on `get_handoff_state` / `load_session` and cross-task observatory sections are produced by `render_handoff(kind="dashboard")`. The "All Review Findings History" section has been removed from the default render; historical findings are available via `review_findings(review={"operation":"list","status":"all"})`.
- `render_handoff(kind="dashboard")` accepts `write_file` (default `True`) to control whether `DASHBOARD.txt` is written to disk. Pass `write_file=False` to get the text without writing a file (useful in tests and CI diff checks). Extension sections (Lane Health, Worker Status) are contributed by `workbay-orchestrator-mcp` via `register_dashboard_extension`.
- `set_handoff_state` accepts an optional `target_branch` parameter. When provided, it sets the task's intended work branch. When omitted on subsequent calls, the existing value is preserved. The field appears in `get_handoff_state` responses and in the CURRENT_TASK.json Active Status section.
- `set_handoff_state` accepts an optional `target_worktree_path` parameter (introduced in the lane-orchestration improvements slice). It records the absolute filesystem path of the linked worktree where the task should be implemented. Used by `make context` and write-side context-drift warnings to fail-fast when an agent runs from the wrong directory in a multi-agent / multi-worktree workflow. When omitted on subsequent calls, the existing value is preserved.
- `set_handoff_state` accepts an optional `task_plan_path` parameter. This is the canonical write surface for root-visible task-plan discovery; `focus` is not a fallback source. When omitted on update, the existing `task_plan_path` value is preserved. Pass an empty string to clear it.
- `get_handoff_state` active rows always include the task-plan metadata quartet: `task_plan_path`, `task_plan_abs_path`, `task_plan_exists`, and `task_plan_resolution`. `task_plan_abs_path` resolves relative paths against `target_worktree_path` when present, otherwise against the configured workspace root. `task_plan_resolution` is `worktree`, `workspace`, `absolute`, `unresolved` (when a relative plan path is present but workspace-root resolution fails), or `null` when no plan path is set.
- Routine writes keep `CURRENT_TASK.json` on-demand by default. With `current_task_auto_regen=False` (the runtime default), routine mutation surfaces do not rewrite the file; legacy consumers can opt back in with `WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN=1`. Explicit export/render paths such as `render_handoff(kind="current_task")` and `export_handoff_state(..., include_markdown=True)` remain unconditional. Terminal transitions also flush unconditionally (internal): `archive(operation="archive", ...)` and `set_handoff_state(status_only=True, ...)` where the resolved status is outside `LIVE_ACTIVE_STATUSES` (i.e. `done`, or any future terminal vocabulary in `shared_primitives.LIVE_ACTIVE_STATUSES`) force the on-disk workspace summary to match the post-write derive — mirroring the `decisions.py:758` close-check precedent — so legacy file consumers never observe a stale archived/terminated task.
- `DASHBOARD.txt` is server-maintained. Every public mutating MCP tool exported from `workbay_handoff_mcp/api.py` regenerates `DASHBOARD.txt` once per outer call (coalesced via a per-call `ContextVar` so batch tools render at most once) after the underlying transaction commits. Each successful mutation envelope carries an additive `dashboard_md_regen` field with one of `"ok" | "skipped" | "failed"` and an optional `dashboard_md_regen_error` string when the post-commit render fails. Render failure is reported but never rolls back the underlying mutation. Read-only tools (`get_handoff_state`, `load_session`, `*.list`, `search_handoff`, `render_handoff`, `audit_decision_ids`, integrity checks) are explicitly excluded from the auto-regen wrap and pay zero render cost. Auto-regen defaults to enabled (`dashboard_auto_regen=True`); legacy / CI consumers managing the file out-of-band can opt out with `WORKBAY_HANDOFF_DASHBOARD_AUTO_REGEN=0` (or `RuntimeConfig(dashboard_auto_regen=False)`) and continue to invoke `render_handoff(kind="dashboard")` explicitly. The render path is bounded by a 50 ms wall-clock budget enforced by a CI benchmark on a representative-load fixture.
- `render_handoff(kind="dashboard")` includes an `ACTIVE TASK PLANS` section that lists each active task's task ref, target branch, declared `task_plan_path`, resolved absolute path, and an existence marker (`✓` for present, `✗` for missing). When no active rows exist it renders `(no active tasks)`; when active rows exist but none have `task_plan_path`, it renders `(no active tasks have task_plan_path set)` plus the `set_handoff_state(task_plan_path='docs/tasks/...')` hint; active tasks without a plan path are summarized in a `(no task_plan_path set for: ...)` footer.
- Write surfaces that resolve actor context (`set_handoff_state`, `record_event`, `next_actions`, `review_findings`, and `review_runs`) emit `context_drift` warnings when the resolved actor branch differs from the active task's `target_branch`, or when the current process working directory differs from the active task's `target_worktree_path`.
- Branch drift is warning-only by default. When `WORKBAY_HANDOFF_ENFORCE_BRANCH` is truthy and `WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT` is not, writes targeting enforceable branches fail before mutation with `BranchMismatchError` for direct Python callers.
- MCP clients do not receive a transport exception for this case. The MCP wrapper converts `BranchMismatchError` into the normal v2 envelope with `ok=false` and `data.error`, `data.task_ref`, `data.expected_branch`, and `data.actual_branch` populated so agents can handle the failure as structured tool output.
- `switch_task` (registered on `workbay-orchestrator-mcp`) also accepts `target_branch`, set at task init time.

### v2 Response Envelope (OC-004)

All public MCP tool responses use the v2 envelope as of package version `0.2.0`. Previous tool-specific top-level fields are nested under `data`.

As of internal, responses use compact serialization: no indentation, null/empty fields stripped, and no legacy field mirroring. MCP tool handlers return `dict` to FastMCP (which serializes once), eliminating double-serialization escapes in MCP responses. Core functions continue returning `str` for orchestrator in-process callers. The `data` block is the canonical payload; callers should read fields from `data`, not from top-level mirrors. In-process Python callers that need flat access should use `_flatten_v2()`.

```json
{"ok":true,"schema_version":2,"tool":"get_handoff_state","scope":{"task_ref":"internal"},"data":{"active":{...},"limits":{...},...},"task_ref":"internal"}
```

| Key | Type | Notes |
|-----|------|-------|
| `ok` | bool | Always present |
| `schema_version` | int | Always `2` for v2 responses |
| `tool` | string | Tool name that produced this response |
| `scope.task_ref` | string/null | Resolved task reference |
| `data` | object | Tool-specific payload (canonical v2 shape) |
| `task_ref` | string | Present when task_ref is non-null |
| `mutation` | object | Present on write responses only: `{ entity, operation, affected_ids, task_revision }`. Omitted when null. |
| `artifacts` | array | Present only when non-empty |
| `warnings` | array | Present only when non-empty |

Internal utility functions (`get_review_findings_summary`, `reconcile_review_findings`) may still use the v1 shape. All public MCP-registered tools return the v2 envelope. Check `schema_version == 2` to confirm.

## CLI Fallback

Primary entrypoints:

- `mcp-workbay-handoff --workspace-root <repo> serve-stdio`
- `mcp-workbay-handoff --workspace-root <repo> serve-http`
- `mcp-workbay-handoff --workspace-root <repo> doctor`

Fallback subcommands:

- `state`
- `dashboard`
- `set`
- `decision` — requires `--session` and `--decision`; `--rationale` is optional but mandatory for `slice_complete_*` decisions:

  ```bash
  mcp-workbay-handoff --workspace-root <repo> event \
    --event-kind decision \
    --session "<agent>-<task-slug>" \
    --decision "cdx_slice_complete_<work_ref>_<slug>" \
    --rationale "## Changes\n..."
  ```

- `validate` — side-effect-free preflight router (replaces the legacy `validate-decision-id` and `validate-write` subcommands). Pass `--kind decision_id` to preflight a decision id (returns `category`, `error`, and `suggested` when applicable); pass `--kind write` to preflight an arbitrary write payload against the registry:

  ```bash
  mcp-workbay-handoff --workspace-root <repo> validate \
    --kind decision_id \
    --decision "codex_slice_complete_plan0004_contract-pinning-and-docs" \
    --decision-kind slice_complete
  ```

- `next-actions`
- `blocker`
- `test`
- `review-findings`
- `review-runs`
- `integrity-check` (`--kind working_tree|post_merge|close`; replaces the legacy `working-tree-integrity-check`, `post-merge-integrity-check`, and `handoff-close-check` subcommands)
- `export`
- `import`
- `archive` (`--operation archive|gc|get`; replaces the legacy `archive-task-state`, `tasks-gc`, and `get-archived-task` subcommands)
- `audit-decisions`
- `artifacts`
- `touched-files` (`--operation record|list`; replaces the legacy `record-file-touch` and `get-touched-files` subcommands)
- `compaction` (`--operation record|get|get_latest`; replaces the legacy `compact-session`, `get-compaction`, and `get-latest-compaction` subcommands)
- `handoff-search`
- `handoff-rows`
- `get-verified-tests`

Orchestration subcommands (`orchestrator-start`, `worker-start`, `dispatch`, `orchestrator-cycle`, `worker-events`, `list-backends`, `metrics`, etc.) are served exclusively by `workbay-orchestrator-mcp`. See [`workbay-orchestrator-mcp.md`](workbay-orchestrator-mcp.md).

**CLI surface note:** `workbay-handoff-mcp` CLI is ledger-only. It exposes `serve-stdio`, `serve-http`, `doctor`, `dashboard`, and the 19 ledger MCP tools that have CLI wrappers (`load_session` and `close_slice` remain MCP-only), plus two CLI-only artifact variants (`artifact-list`, `artifact-terms`). All orchestration and lane-management commands are exclusively on `workbay-orchestrator-mcp`.

## HTTP Transport

`serve-http` starts the same MCP server over FastMCP's `streamable-http` transport instead of stdio.

Current runtime behavior:

- host: `127.0.0.1`
- port: `8000`
- endpoint path: `/mcp`
- log level: FastMCP default unless overridden by its settings

Example:

```bash
mcp-workbay-handoff --workspace-root /path/to/repo serve-http
```

## Portable Hook Semantics

The orchestration tooling implements a set of named automation events with stable expected behavior regardless of the host agent. Each semantic has a trigger condition, allowed side effects, a required durable output, and an operator visibility path.

Host-specific integrations (e.g. Codex skill wrappers, VS Code callbacks) trigger these semantics, but the semantics themselves and their durable outputs live in this contract and the repo tooling layer. A hook semantic must never depend on a single host product's lifecycle model.

---

### `after_review_findings_recorded`

**Trigger:** The `ace-detect` PostToolUse hook runs after host-supported
`review_findings(operation="record"|"batch_record")` tool calls. The worker
daemon review pipeline does not perform a second ACE capture pass.

**Side effects allowed:** ACE reflection detection. The hook scans finding
descriptions for `[sr-NNN]` or `[rg-NNN]` rule references and appends pending
evidence entries to `.task-state/ace_reflect_log.jsonl`.

**Required durable output:** An entry in `.task-state/ace_reflect_log.jsonl`
naming the finding ID, the matched rule reference, the support/contradiction
classification, and a timestamp. The entry is written only when the finding
description contains rule references.

**Operator visibility:** Inspect `.task-state/ace_reflect_log.jsonl` directly,
or run
`make ace-reflect WORKBAY_ACE_PLAYBOOK_FILES=<paths> ACE_ARGS=--dry-run` to
preview pending counter updates. Run
`make ace-reflect WORKBAY_ACE_PLAYBOOK_FILES=<paths>` to apply pending updates
to the declared playbook file(s).

---

### `before_close_check`

**Trigger:** `integrity_check(payload={"kind":"close","task_ref":...})` is invoked. (Replaces the legacy `handoff_close_check` MCP tool; the close-readiness verdict logic is unchanged.)

**Side effects allowed:** None. The `close` kind of `integrity_check` is a pure `generator` surface and must not mutate state.

**Required durable output:** A structured readiness verdict (JSON envelope) including open blockers, open high-severity findings, unresolved test failures, and missing slice decisions. The verdict must be inspectable without re-running the tool. Individual failure reasons must reference MCP record IDs (finding IDs, blocker IDs) so the operator can navigate to the source.

**Operator visibility:** The verdict is returned synchronously in the tool response. No additional state query is required to understand what blocked the close check.

---

### `after_worker_turn`

**Trigger:** A worker execution turn completes, regardless of whether review passed or was skipped due to a scope violation.

**Side effects allowed:** Observability logging. The worker daemon appends a structured JSONL event to `logs/worker-daemon/worker-<lane>.jsonl` recording token usage, scope result, context pressure, and turn outcome.

**Required durable output:** A JSONL event with at minimum: `event_type`, `task_ref`, `lane_id`, `turn_timestamp`, `scope_result` (`pass` or `violation`), and `turn_outcome` (`review_submitted` or `skipped`). Token fields (`input_tokens`, `output_tokens`, `total_tokens`) are included when the backend provides exact usage.

**Operator visibility:** `worker_event_history(task_ref, lane_id, limit=20)` via MCP, or `make worker-daemon-tail` from the worker worktree.

---

### `after_task_switch`

**Trigger:** `switch_task(task_ref=...)` completes (archives the prior task, activates the new task).

**Side effects allowed:** `CURRENT_TASK.json` and `DASHBOARD.txt` regeneration. `render_handoff(kind="current_task")` runs for the new active task so the machine-readable snapshot and human-readable mirror reflect the switch immediately.

**Required durable output:** Updated machine-readable `CURRENT_TASK.json` plus human-readable `DASHBOARD.txt` for the new task. If regeneration fails, the failure must be surfaced in the `switch_task` response, not silently swallowed.

**Operator visibility:** `DASHBOARD.txt` must be current after `switch_task` returns, and `CURRENT_TASK.json` must remain parseable machine state. If either file appears stale, run `render_handoff(kind="current_task", task_ref=<new-task>)` explicitly.

---

### `before_review_prompt_build`

**Trigger:** The orchestrator daemon or `review_runner.py` prepares a review prompt for a completed worker turn.

**Side effects allowed:** Slice-packet composition. ACE guidance, slice decisions, the review checklist, and a scope diff are assembled into the prompt envelope. Composition metadata (section sizes, attribution flags) should be captured for the turn-metrics ledger when available.

**Required durable output:** A scope-violation precondition check runs before prompt assembly. If scope validation failed, prompt build is skipped entirely and a `scope_violation` JSONL event is emitted instead of a review prompt. No review prompt is produced for a scope-violating turn.

**Operator visibility:** Prompt composition metadata (section label, character count, attribution flags) is included in `worker_event_history` output when the worker daemon captures it. The `scope_violation` event is inspectable via `worker_event_history` without re-running the review cycle.

---

### Hook Adapter Guidance

A host-specific integration that triggers a hook semantic must:

1. Invoke the tooling mechanism that produces the required durable output (write to DB, append to JSONL, regenerate a file).
2. Not substitute informal prose or chat messages for the required durable output.
3. Surface failures explicitly rather than silently continuing as if the hook ran.
4. Not assume a specific agent product manages the hook lifecycle; the trigger condition and durable output must be achievable from any MCP-capable runtime or shell.

## Semantic Compaction Reinjection (implementation note)

After a compaction, the SessionStart reinjection hook (implementation note) re-surfaces handoff state to the model. implementation note adds an **opt-in semantic selection path**: instead of recency/ID ordering plus the first-4096-char residual, the hook can rank stored handoff concepts by embedding cosine similarity to a composed anchor and emit the top-K. The feature is **off by default and byte-identical to the prior reinjection output when off, when the model artifact is absent, or when the optional `embeddings` extra is not installed** — it never reaches the network on any code path. Operator setup, backfill, and the int8/fp16 choice are documented in `packages/mcp-workbay-handoff/docs/guides/semantic-reinjection.md`.

### Storage (schema v15)

- `concept_embeddings(entity_kind, entity_id, task_ref, text_hash, dim, vector BLOB, model_id, created_at)`, keyed `(entity_kind, entity_id)`. `vector` is canonical little-endian float32 bytes (`dim * 4`); stored vectors are L2-normalized so cosine equals a dot product. Re-embed is gated on `(text_hash, model_id)` — unchanged text embedded by the same model is never re-embedded.
- `session_compactions.anchor_vector` (nullable BLOB): the transcript-derived anchor embedded at compaction (Stop-hook) time, since SessionStart does not hold the live transcript. NULL leaves the read path degrading to the prior selection.
- Embeddable **concept** kinds are a fixed enumeration (`entity.field`): `decision.rationale`, `finding.description`, `finding.fix`, `finding.resolution_notes`, `blocker.description`, `handoff_state.objective`, `handoff_state.focus`, `compaction.prose_residual`.
- Migration is additive and forward-only (`HANDOFF_SCHEMA_VERSION` 14 → 15); older readers ignore the table. Embed-on-write runs best-effort **after** the concept row commits (never inside the write transaction), and any gap is reconciled by the resumable backfill — so provider absence or inference failure leaves the write path unchanged.

### Embedding artifact (offline, hash-pinned, opt-in)

The model is `Alibaba-NLP/gte-base-en-v1.5` (int8 ONNX, 768-d, CLS pooling + L2-norm, no query/passage prefix). The ~147 MB artifact is **not** bundled in the wheel/sdist — it ships as a separate optional artifact resolved from an explicit, hash-pinned local path. The published-bytes gate (implementation note) and `check_shipped_privacy` therefore operate on the core package only; the binary model lives outside the scrubbed source surface and needs no privacy-gate carve-out. Configuration is env-only and all four must be set for the provider to resolve (else clean degrade):

| Env var | Meaning |
|---|---|
| `WORKBAY_HANDOFF_EMBEDDING_MODEL` | Local path to the ONNX model artifact. |
| `WORKBAY_HANDOFF_EMBEDDING_TOKENIZER` | Local path to `tokenizer.json`. |
| `WORKBAY_HANDOFF_EMBEDDING_MODEL_SHA256` | Pinned SHA-256 of the model; verified at load and fails closed on mismatch. |
| `WORKBAY_HANDOFF_EMBEDDING_TOKENIZER_SHA256` | Pinned SHA-256 of the tokenizer; verified at load. |

### Reinjection selection knobs

Env wins over the `harness-protocol.yaml` `reinjection:` contract default.

| Env var | Default | Meaning |
|---|---|---|
| `WORKBAY_REINJECT_SEMANTIC` | off | When truthy, append a `relevant:` line of semantically top-ranked concepts. Off ⇒ prior selection (byte-identical). |
| `WORKBAY_REINJECT_SEMANTIC_TOP_K` | `8` | Top-K concept count for the semantic line. |
| `WORKBAY_REINJECT_AB` | off | When `1`, assign a deterministic selection arm from `session_id` hash parity (supersedes the implementation note emit/suppress window): `treatment` = arm B (semantic top-K), `control` = arm A (current selection). Both arms emit; the arm overrides `WORKBAY_REINJECT_SEMANTIC`. Recorded on `session_reinjections.arm`. |
| `WORKBAY_REINJECT_BUDGET_CHARS` | `1500` | Total stdout budget for the reinjection block; the semantic line is bounded by it. |

`session_reinjections` rows record each SessionStart reinject emission. Core columns: `reinjection_id`, `session_id`, `harness`, `task_ref`, optional `compaction_id`, `source` (`compact` or `resume`), `emitted_chars`, optional `arm` (A/B assignment), and optional `semantic_detail_json` — canonical compact JSON (`sort_keys=true`, `,`/`:` separators) of the typed semantic reinjection telemetry packet when semantic selection runs. Null `semantic_detail_json` means no structured detail was persisted for that row.

The SessionStart anchor is the mean of the persisted transcript anchor plus freshly embedded live components — currently the task `objective`, `focus`, and pending-action texts; missing components are dropped. (Open-finding ids are emitted as a separate block line, not folded into the anchor in the shipped hook.) A dimension/model mismatch drops the offending vector rather than crashing. Per-component weights are uniform (a tuning surface, not yet a knob).

### A/B efficacy gate (implementation note)

The pre-registered rule (`embeddings/eval_recall.py::apply_recall_gate`) adopts arm B only if `recall@K` improves at equal-or-lower reinjected tokens. On the offline labeled fixture semantic recall@3 = 1.0 vs recency 0.0 at equal token cost (decision: adopt); live multi-session token medians remain operator-owned. A cross-encoder reranker is an explicit deferred follow-up (plan non-goal), recorded as a implementation note finding.

### Doctor visibility

`doctor` surfaces two informational checks (neither gates overall `ok`, since the feature is opt-in):

- `checks.embedding_provider`: `{ok, configured, model_id, artifacts_verified?, runtime_available?, warning?, note?, error?, remediation?}`. Unconfigured ⇒ `ok=true, configured=false`. A configured provider has its pinned model + tokenizer SHA-256 verified via hash-only checks (no session load, no network); a corrupt/mismatched artifact reports `ok=false` with remediation before it reaches the embed path. When artifacts verify but the optional `[embeddings]` extra is absent or broken, `runtime_available=false` and a `warning` surface while `ok` stays true — semantic reinjection degrades at embed time.
- `checks.embedding_backfill_coverage`: `{ok, embedded_concepts, model_ids, anchor_vectors, note?}`. A pre-v15 DB or unused feature degrades to `ok=true` with a note and zero counts.
