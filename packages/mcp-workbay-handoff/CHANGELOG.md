# mcp-workbay-handoff

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.2.6] — 2026-07-12

### Changed
- `agent_errors` pre-rebrand DB retrofit: register the `workbay_release` column in the warm-start net (schema v27→v28) so an already-stamped DB retrofits it and error telemetry stops going dark.
- Classify backend-internal faults (max-tokens truncation, rate-limit, 5xx, overloaded) as `backend_transient` on the error outcome so a coordinator can re-dispatch mechanically.
- Brand: inline-exempt legacy `workstate_release` column-name references in `shared_schema`.
- Embeddings actionability: guard the `semantic_reinjection_packet` import and emit remedy hints; resolve `pending_uncommitted` scope to finding file paths.
- Fix stale `EXPECTED_HANDOFF_TOOL_COUNT` (25→26); `finalize-plan` now emits `untickable_acceptance_boxes` as a first-class receipt field.

## [0.2.5] — 2026-07-11

### Changed
- Version bump for the coordinated release; no tool contract or field-shape changes.

## [0.2.4] — 2026-07-10

### Changed
- Closure preflight: structured rejection envelopes, requirements-on-read, auto-evidence from verified tests, and a rejection-rate freshness metric.
- Finding-backlog GC: classify/triage/reanchor apply path plus a severity-aware archive gate and debt digest.
- Harness identity-marker write attribution (0107) as a fallback when no explicit actor is supplied.
- Hygiene-residue reaping, terminal-guard telemetry drain, and mypy fixes for sqlite3.Row/dict typing.

## [0.2.3] — 2026-07-08

### Changed
- Boot-time embedding availability probe + embeddings provisioning for semantic reinjection (implementation note): typed availability signal so lane prompts degrade cleanly when the ONNX provider is absent instead of silently regressing.
- Register the `worktree_lanes.test_cmd` column (structured self-verify command for offload lanes, implementation note) and bump `HANDOFF_SCHEMA_VERSION` 24 → 25 so the add-column migration re-runs on stamped-current DBs; `test_cmd` is also registered in `_HANDOFF_REQUIRED_COLUMNS` as the warm-start net (regression coverage in `test_schema_migrations.py`).

## [0.2.2] — 2026-07-07

### Changed
- Lane-message durability: fail-fast on empty inbox, `lane_messages` reclaimer, and atomic `dispatch_id` idempotency for offload sync.
- Register `lane_messages.dispatch_id`/`payload_json` columns and bump `HANDOFF_SCHEMA_VERSION` 23 → 24 so the migrator stays reachable on stamped-current DBs (with regression + drift-guard coverage).
- Full dashboard render now surfaces stale-task open findings.

## [0.2.1] — 2026-07-04

### Changed
- Bump the protocol dependency floor to `workbay-protocol>=0.2.1` and ship the accumulated handoff close-check, plan-checklist, and projection hardening.

### Added
- implementation note (internal): `semantic_reinjection_packet` MCP tool wrapping `build_semantic_reinjection_packet` — inputs `task_ref`, `anchor_texts` (slice changed files / rationale), optional budget override; returns bounded `relevant:` lines with `status` / `skip_reason` and clean `provider_unavailable` degradation so review-parallel never blocks on embeddings.
- Close-check verification commands (E7 / internal): optional `close_check_required_commands` on `RuntimeConfig` (env: `WORKBAY_HANDOFF_CLOSE_CHECK_REQUIRED_COMMANDS` as JSON array or newline-separated list).

### Fixed
- Close-check verification commands (E7) review hardening (internal): on **timeout** the whole process group is now `SIGKILL`-ed (commands launch under `start_new_session=True`), so multi-process commands such as `make check-all` no longer orphan sub-make/compiler/pytest grandchildren past the timeout bound.

### Changed
- internal: harness launch via `mcp_launch.py` shim (workspace `.venv` or bootstrap-provisioned `uv tool` binary); PyPI `uvx …@` retired.

## [0.2.0] — 2026-06-26

### Changed
- Idempotent decisions (dedup migration + unique index), CAS-guarded `review_findings` status transitions, atomic `close_slice` via a `BEGIN IMMEDIATE` compound txn, provenance attribution + worktree-repoint diagnostic, auto-reap of stale MAINT-on-main rows, case-insensitive `task_ref` lookup fallback, and a hermetic embedding-provider env (Plans 0062/0063/0066).

## [0.1.0] — 2026-06-22

### Changed
- First release under the WorkBay name; the version line was reset to `0.1.0` for the new PyPI project (greenfield, single-maintainer).

## [0.13.3] — 2026-06-19

### Fixed
- Same-version handoff DB schema skew self-heal (implementation note): a DB stamped at the current `HANDOFF_SCHEMA_VERSION` (17, unchanged) but missing load-bearing migration columns (e.g.

## [0.13.2] — 2026-06-18

### Changed
- Optional `embeddings` extra for semantic reinjection (`numpy`, `onnxruntime`, `tokenizers`); test/dev groups now include `numpy` for embedding eval coverage.
- Bumped canonical MCP uvx pin to `0.13.2`.

## [0.13.1] — 2026-06-13

### Changed
- Maintenance: ruff/isort import-ordering sweep across `api`, `compaction`, `compaction_contract`, `shared_primitives`, and `shared_schema` (no behavior change).

## [0.13.0] — 2026-06-11

### Added
- Schema v14 (`HANDOFF_SCHEMA_VERSION` 13→14): `session_reinjections` table for durable reinject firing telemetry (internal).
- Migration doctor deadline budget + uv-build timeout so a slow build fails fast instead of hanging bootstrap (internal).

### Changed
- Atomic bootstrap transaction: a lock failure now leaves the DB unstamped so the next boot self-heals rather than half-applying schema state (internal).
- `review_findings` merge preserves each finding's existing status instead of reopening fixed/deferred rows on re-record (implementation note).
- `_detect_git_write_context()` short-circuits `git rev-parse` when both branch and commit hints are already resolved from `WORKBAY_HANDOFF_DEFAULT_*` or paired `GITHUB_*` / `CI_*` fallbacks (implementation note).

## [0.12.9] — 2026-06-10

### Changed
- Build: migrate sdist build backend setuptools→hatchling with at-build privacy scrub (implementation note sdist-privacy sweep).
- Fix: v13 column-add migrations are now idempotent under concurrent/skewed ALTER (internal).

## [0.12.8] — 2026-06-08

### Changed
- Privacy: internal project ids scrubbed from shipped source.

## [0.12.6] — 2026-06-07

### Changed
- Re-cut of the unpublished 0.12.5 with the runtime `__version__` string synced to the package version; `workbay-protocol` floor moved to 0.2.4.

## [0.12.5] — 2026-06-07

### Changed
- Compaction harness literals (`CompactionHarness`/`CompactionHarnessInput`/ CLI choices) gain `grok` for internal parity with the canonical harness list.
- `agent_errors`: bind-narrowed optional text normalization and hoisted harness normalization for dedup insert-or-bump (mypy parity, no behavior change).

## [0.12.4] — 2026-06-06

### Added
- Agent error telemetry (implementation note): `agent_errors` ledger (schema v12), `record_event` error kind with redaction, MCP server self-capture of rejected writes, `capture-agent-errors` PostToolUse hook + `errors-record`/`errors-replay-spool` CLI, and `errors-report`/`errors-export` harvest clustering by (error_class, package_name) with version ranges.
- Compaction surfaces the latest compaction id for session-start reinjection (internal).
- Grok harness parity via canonical plugin emission (internal).

## [0.12.3] — 2026-06-04

### Changed
- Re-cut of the unreleased 0.12.2 with the runtime `__version__` string synced to the package version.

## [0.12.2] — 2026-06-04

### Changed
- Dependency-pin-only release: `workbay-protocol>=0.2.0,<0.3.0` so installs alongside `workbay-bootstrap 0.8.0` resolve.

## [0.12.0] — 2026-05-30

### Changed
- **MCP server identity cutover to `workbay-handoff-mcp` (implementation note Slice B).** The canonical registered server name is `workbay-handoff-mcp`; bootstrap collapses any duplicate registration so only one canonical entry survives (fixes the duplicate-`.mcp.json` "servers not loading" symptom).
- **Harness-contract path resolves through `workbay_protocol`.** `compaction_contract` now imports `HARNESS_CONTRACT_RELPATH` from `workbay-protocol` (>=0.1.6), so the contract is read from `docs/workbay/contracts/harness-protocol.yaml` after the implementation note Slice D path rename.

### Removed (Migration)
- **Tier-4 env-var alias shim retired (implementation note).** `resolve_env_alias` is now keyword-only and reads the canonical `WORKBAY_*` names only.

### Added
- **Compaction receipt fields on operator-visible outputs (internal).** Successful `compact-session` Stop-hook runs and `make compact-now` / `workbay_handoff_mcp.compaction_cli` output now preserve `compaction_id=<id>` as the stable first line, followed by `tokens_saved_estimate=<n>`, `input_chars=<n>`, `summary_chars=<n>`, and `prose_residual_chars=<n>` on separate lines.

## [0.11.5] — 2026-05-20

### Fixed
- **ACTIVE TASK PLANS rows now keep `task_plan_path` repo-relative.** `DASHBOARD.txt` once again renders the declared `task_plan_path` verbatim under `plan:` and moves the branch/readability cue into the read hint, so root-workspace task-plan discovery matches the published contract and the consumer acceptance test.

### Added
- **Adaptive read profiles + response budgeting (internal).** `get_handoff_state` and `load_session` now accept three layered read controls:
- **Layer 1 — `read_profile`** selects a stable bundled shape under one of `identity` (active task + limits only), `hot_summary` (summary detail, `top_n_*=3/5/5/3/3`), `review_packet` (summary detail, `top_n_*=20/20/20/5/5`, additive add-ons capped at 20), `open_items` (full detail with `blockers_open`/`actions_pending`/`findings_open` required and never omitted by `auto_summary`), or `full_debug` (the legacy full shape; the only profile the budget planner may fully reshape).
- **Layer 2 — `response_budget_bytes` / `budget_policy`** drives the server-side budget planner.
- The compaction-style envelope advisory now names `read_profile=` and `response_budget_bytes=` as the bounded-read levers.
- **Migration:** routine consumers (orchestrator helpers, skills, hooks) should prefer `read_profile=` over hand-rolled `sections=`/`top_n_*`.
- **Compaction-vs-host explainer + cold-start hook test alignment (internal).** New `docs/explainers/compaction-vs-default-harness-compaction.md` covers where each mechanism runs (in-conversation vs.
- **Typed compaction record receipt (internal).** The `compact_session` implementation (now exposed only as the typed callable `workbay_handoff_mcp.compaction.compact_session`; the `api.py` bare-string wrapper was deleted after the internal caller audit confirmed no external caller depended on it) returns a Pydantic `CompactionRecordReceipt` with `{compaction_id, summary: StructuredSummary, input_chars, summary_chars, prose_residual_chars, tokens_saved_estimate, db_row_id}`.
- **Unified compaction runtime-disable surface (internal).** A single resolver, `resolve_compaction_disabled(env, conn, task_ref)`, now silences both internal compaction surfaces with the same precedence chain: `AGENT_HANDOFF_COMPACTION_DISABLED` env (legacy `internal_COMPACTION_DISABLED` alias) → task-scoped row in `compaction_settings` → workspace-default row → enabled.

### Changed
- **Compaction advisory default thresholds lowered to 70,000 tokens / 280,000 chars** in `packages/workbay-system/docs/workbay/contracts/harness-protocol.yaml` (was 120,000 / 500,000).

### Added
- **Per-deployment compaction threshold overrides (internal).** Operators can now tune the compaction advisory's token and char gates without editing `harness-protocol.yaml`.
- Env vars: `AGENT_HANDOFF_COMPACTION_THRESHOLD_TOKENS`, `AGENT_HANDOFF_COMPACTION_THRESHOLD_CHARS`.
- Overlay JSON: `.agentic-overlay.json -> compaction.thresholds.{tokens,chars}` (sibling of the existing `surfaces` key).
- Advisory envelope gains additive `thresholds_source: {tokens, chars}` field reporting which layer (`"env"` / `"overlay"` / `"contract"`) supplied each effective threshold.
- Invalid overrides (non-int, negative) append a `compaction_threshold_override_invalid: <source>=<key>=<value>` warning to the advisory `warnings` list and fall through to the next layer; no exception.
- **Canonical compaction advisory at `get_handoff_state(sections="identity")` (internal / internal).** A new `compute_compaction_advisory()` evaluator reads `docs/workbay/contracts/harness-protocol.yaml`, the configured transcript path (env var first, fallback glob second), and the running `turn_metrics` token total, and returns a stable 7-key envelope: `{recommended, thresholds, observed, harness, transcript, latest_compaction_id, warnings}`.

## [0.11.4] — 2026-05-13

### Changed
- **`handoff_close_check` materializes `CURRENT_TASK.json` on demand.** The close check is now the single materialization point for the workspace summary file: it calls `_write_workspace_summary_current_task_json(unconditional=True)` before reading the on-disk copy for its `current_task_sync` comparison.

## [0.11.3] — 2026-05-11

### Added
- **`docs/guides/production-readiness.md`**: new operator guide covering response-size controls (`HandoffReadLimits`, oversize warning), archive/GC behavior, artifact purge, subprocess timeouts, and failure modes for ambiguous workspace and task context.

### Changed
- **`HandoffReadLimits` read-limit policy**: `get_handoff_state` and `load_session` now route all `top_n_*` parameters through a typed `HandoffReadLimits` dataclass.
- **Named migration helpers**: `_apply_handoff_migrations` now delegates to four explicitly named, idempotent functions (`_migrate_add_audit_tables`, `_migrate_add_column_extensions`, `_migrate_handoff_state_schema`, `_migrate_add_turn_metrics`).
- **Named doctor-check helpers**: `run_doctor` delegates to `_check_state_dir_writable`, `_check_fts_index_health`, `_check_fts5_available`, `_check_cli_startup`, and `_check_stdio_startup`.
- **Typed request objects**: `_parse_import_snapshot` (returns `SnapshotImportData`) and `_parse_provenance_repair_request` (returns `ProvenanceRepairRequest`) extract validation from their callers.
- **Tool-registry group builders**: `_build_tool_registry` delegates to `_task_state_tool_entries`, `_review_tool_entries`, `_lifecycle_tool_entries`, and `_artifact_tool_entries`.

### Fixed
- **`write_contracts` grammar alignment**: `record_event.decision` regex relaxed from `^[A-Za-z][A-Za-z0-9_]*$` to `^[A-Za-z][A-Za-z0-9_-]*$` and `close_slice.work_ref` relaxed from `^[a-z0-9][a-z0-9_]*$` to `^[A-Za-z0-9][A-Za-z0-9_-]*$`.

## [0.11.2] — 2026-05-11

### Fixed
- **File-descriptor leak in `_get_db_connection`**: the prior factory returned a bare `sqlite3.Connection` that callers wrapped with `with ...

## [0.11.1] — 2026-05-10

### Fixed
- **internal BR-01/02/03 fixes** on the multi-active CURRENT_TASK projection landed in 0.11.0:
- **BR-01** — multi-active dashboard rendering no longer collapses distinct active tasks into one row when their `task_ref`s collide only by case.
- **BR-02** — `import_handoff_state` now rejects malformed v2 `single` and `workspace_ambiguous` payloads with a typed validation error instead of partially mutating the workspace.
- **BR-03** — `import_handoff_state` preserves `target_branch`, `target_worktree_path`, and `plan_path` on imported rows rather than silently zeroing them when the import payload omits the keys.
- **internal compaction env-var consolidation**: the `compact-session` hook now resolves every compaction-tuning value through `CompactionSettings.from_env()` so the canonical `AGENT_HANDOFF_COMPACTION_*` prefix and its deprecated `internal_*` alias share one parse/validate/warn path.

### Added
- **`doctor` reports package `version` (internal)**: `run_doctor` now includes a `"version"` key at the **top level** of its returned dict, equal to `workbay_handoff_mcp.__version__`.
- **Package `__version__` and CLI `--version` flag (internal)**: `workbay_handoff_mcp.__version__` is now exposed at module load (sourced from `importlib.metadata.version("mcp-workbay-handoff")` with a source-checkout fallback that mirrors `pyproject.toml:[project].version`).

## [0.11.0] — 2026-05-08

### Changed
- **Tool-surface flattening: 30 → 21 (internal)**: the workbay-handoff-mcp surface contracts via the discriminated-operation pattern established by ADR-005.
- `mcp-workbay-handoff validate-decision-id ...` / `mcp-workbay-handoff validate-write ...` → `mcp-workbay-handoff validate --kind decision_id|write ...`
- `mcp-workbay-handoff compact-session ...` / `mcp-workbay-handoff get-compaction ...` / `mcp-workbay-handoff get-latest-compaction ...` → `mcp-workbay-handoff compaction --operation record|get|get_latest ...`
- `mcp-workbay-handoff record-file-touch ...` / `mcp-workbay-handoff get-touched-files ...` → `mcp-workbay-handoff touched-files --operation record|list ...`
- `mcp-workbay-handoff working-tree-integrity-check ...` / `mcp-workbay-handoff post-merge-integrity-check ...` / `mcp-workbay-handoff handoff-close-check ...` → `mcp-workbay-handoff integrity-check --kind working_tree|post_merge|close ...`
- `mcp-workbay-handoff archive-task-state ...` / `mcp-workbay-handoff tasks-gc ...` / `mcp-workbay-handoff get-archived-task ...` → `mcp-workbay-handoff archive --operation archive|gc|get ...`
- `mcp-workbay-handoff task-status ...` → `mcp-workbay-handoff set --status-only --task-ref X --status Y [--expected-revision N] ...`

### Migration
- **In-process Python callers**: no action required.
- **MCP wire callers** (stdio/HTTP/CLI): switch to the consolidated tools per the table above.

## [0.10.0] — 2026-05-08

### Added
- **Side-effect-free preflight validators** (internal): `validate_review_ready` and `validate_finding_resolution` let callers preflight gate-readiness and finding-resolution requests without bouncing off the mutating path.
- **Dashboard fragment renderer** (internal): the production dashboard render path now splits rendered markdown into per-section fragment files under `.task-state/DASHBOARD.d/` with a manifest (`dashboard_fragments.manifest.json`) so prompt-cache invalidation is scoped to the section that actually changed.
- **Write-contract registry** (internal): the registry that describes per-tool required fields and field grammars is now exported through `limits.write.tools`, and `validate_write` is registered as a side-effect-free MCP tool (peer of `validate_decision_id`) so callers can preflight an arbitrary write payload against the registry.
- **Distribution-name alias package** `workbay_handoff_mcp` re-exports the full `workbay_handoff_mcp` public surface (via `__all__` plus `__getattr__` submodule forwarding).

### Changed
- `EXPECTED_HANDOFF_TOOL_COUNT` bumped from 29 to 30 to cover the new `validate_write` tool.

## [0.9.1] — 2026-05-08

### Changed
- **Write-context attribution rollout completed** (internal).

### Migration
- No migration needed.

## [0.9.0] — 2026-05-07

### Added
- **Commit-backed review-finding reconciliation** (internal).
- **Working-tree integrity helpers** (internal + working-tree assessment items E/G).
- **Compaction CLI** (`workbay_handoff_mcp.compaction_cli`) — bounded read surface for compacting session state from the command line; `make compact-now` driver target wraps it.
- **Cascade archive + tasks-gc janitor**: archiving an internal parent now cascades to its `internal-*` rows; new janitor pass bulk archives status=done MAINT rows whose parent is already archived.

### Changed
- `handoff_close_check` now consults `working_tree_integrity_check` so the pre-merge gate refuses to pass when the tree has drifted from HEAD on paths outside the effective allowlist.

### Migration
- Consumers reading the working-tree integrity envelope should expect the new `allowlist` and `allowlist_source` fields and a non-empty default allowlist of `{CURRENT_TASK.json, DASHBOARD.txt}`.
- Reconciliation callers that relied on silent provenance rewrites must now use `repair_provenance` with explicit `expected_branch` / `expected_commit_sha` guards.

## [0.8.0] — 2026-05-04

### Added
- **MCP-resolved task plan paths** (implementation note / internal).
- `workbay_handoff_mcp.PlanLocation`, `PlanPathNotRegistered`, `resolve_plan_location`, `list_active_task_locations`, `plan_show_command` — programmatic plan-path resolution.
- `workbay_handoff_mcp.plan_cli` — CLI driver with `{show, edit, list, register}` subcommands invoked by the `make plan-show / plan-edit / plans-list / plan-register` recipes.
- `workbay_handoff_mcp.scripts.backfill_plan_paths` — one-shot enumerator + writer that populates `task_plan_path` on existing in-progress tasks via frontmatter discovery.
- `set_handoff_state` now accepts and persists `task_plan_path`.

### Added (continued — pre-0.8 work that ships in this release)
- **Re-export `workbay_protocol.branch_naming` symbols** (internal).

### Changed
- **Raise `workbay-protocol` lower bound to `>=0.1.2,<0.2.0`** (implementation note BR-01 fix).

## [0.7.0] — 2026-05-02

### Breaking
- **Minimum Python is now 3.12.** `requires-python` is bumped from `>=3.11` to `>=3.12`.

### Changed
- **Active-task plan metadata is now a pinned consumer contract.** `set_handoff_state(..., task_plan_path=...)` is the explicit write path for task-plan discovery, and `get_handoff_state` now always returns the full task-plan field set on active rows: `task_plan_path`, `task_plan_abs_path`, `task_plan_exists`, and `task_plan_resolution`.
- **Routine writes keep `CURRENT_TASK.json` on-demand by default.** The default runtime remains `current_task_auto_regen=False`; routine mutation paths such as `close_slice`, review-finding record/update, and reconcile only rewrite `CURRENT_TASK.json` when legacy consumers opt back in with `AGENT_HANDOFF_CURRENT_TASK_AUTO_REGEN=1`.
- **`DASHBOARD.txt` now exposes a stable `ACTIVE TASK PLANS` operator section.** The section renders each active task's task ref, target branch, declared `task_plan_path`, resolved absolute path, and an existence marker (`✓` or `✗`), followed by a footer listing active tasks that have not set `task_plan_path`.
- **Review-run CLI subject-kind validation now mirrors the persisted contract.** `review-runs --subject-kind branch_diff` is rejected by argparse as an invalid choice; branch-diff reviews should persist as `subject_kind="branch"` with `subject_path="<base>...<head>"`.
- **Dashboard task-plan enrichment now validates against the full `ActiveTask` shape.** The renderer includes `objective` when enriching active rows for `ACTIVE TASK PLANS`, avoiding validation warnings during dashboard generation.
- **Review-run mutations now expose numeric row ids.** The `review_runs(record)` response returns `data.review_run.id` in `mutation.affected_ids` and keeps the human-stable `review_run_id` in `mutation.affected_keys`, so agents can print compact MCP write receipts for context compaction.
- **Dashboard recent-decision rows now preserve handoff ids for all active scopes.** Non-epic active tasks get their own `RECENT DECISIONS (<task_ref>)` section, and decision lines prefer `model_label reasoning_level` in the suffix when available.
- **`DASHBOARD.txt` is now server-owned and auto-regenerated on every state-mutating MCP call.** Each public write tool exported from `workbay_handoff_mcp/api.py` rewrites `DASHBOARD.txt` once per outer call after the underlying transaction commits.
- **`regenerate-task-views` harness hook contract removed.** The `regenerate-task-views` PostToolUse row has been dropped from `harness-protocol.yaml`, and bootstrap no longer materializes any Claude / VS Code / Codex hook wiring that invokes it.
- **Slice-complete decision-id grammar is now published in the identity envelope.** `get_handoff_state(sections="identity")` (and `load_session`) now expose `data.limits.write.slice_complete_decision_id` with `canonical_form`, `regex` (the validator constant by reference), per-segment `segment_rules`, `valid_examples`, and a legacy-write note.
- **New `validate_decision_id` preflight surface.** Exposed via the Python API, the MCP tool registry (`validate_decision_id`), and the CLI (`mcp-workbay-handoff validate-decision-id`).

### Migration
- Consumers that were running `make render-dashboard` or invoking `regenerate-task-views.sh` from a harness PostToolUse hook can remove that step; `DASHBOARD.txt` now refreshes inside the server.
- Cold-start agents should read the slice-complete decision-id grammar from `get_handoff_state(sections="identity")` rather than copying the regex; pass semantic `(author_tag, work_ref, slug)` to `close_slice` to let the server compose the id.
- Legacy `decision`-only callers of `close_slice` and direct callers of `record_decision` are unchanged.
- To expose a task plan from the repo root, set `task_plan_path="docs/plans/..."` on the active task and read the resolved fields from `get_handoff_state` or the `ACTIVE TASK PLANS` section in `DASHBOARD.txt`.
- Consumers that still expect routine writes to refresh `CURRENT_TASK.json` must opt in explicitly with `AGENT_HANDOFF_CURRENT_TASK_AUTO_REGEN=1`.
- No schema migration is required.

## [0.5.1] — 2026-04-28

### Changed
- **Package-local runtime and review-intake guidance is now explicit for distributed clients.** The package README/specs now tell operators and agents to prefer MCP review surfaces (`get_latest_slice_review_packet`, `get_review_findings_summary`, `load_session`, `search_handoff`, `get_verified_tests`) before inspecting `.task-state/handoff.db` directly, and they point package-local test runs at the package root / Makefile flow instead of assuming a workspace-level Python interpreter already has `pytest` installed.

### Migration
- No code changes required.

## [0.5.0] — 2026-04-26

### Breaking
- **Distribution published as `mcp-workbay-handoff`.** The console script (`mcp-workbay-handoff`) and importable Python module (`workbay_handoff_mcp`) align with it, following the broader MCP ecosystem convention (`mcp-server-*`).

### Packaging
- **Hoist WorkBay System MVP packaging metadata.** `pyproject.toml` declares a `[tool.hoisted]` table for harness scripts that need to resolve the install surface via `git+https://github.com/darce/workbay.git@mcp-workbay-handoff-v{version}#subdirectory=packages/mcp-workbay-handoff`.

## [0.4.3] — 2026-04-24

### Breaking
- **Console script is `mcp-workbay-handoff`**, matching the `mcp-*` prefix naming convention shared with sibling MCP servers (`mcp-workbay-orchestrator`, etc.).

### Migration
- Update the `command` field wherever the server is launched:

## [0.4.1] — 2026-04-22

### Fixed
- **`run_doctor` no longer hard-fails on transient stdio handshake errors in fresh consumer venvs.** The stdio + CLI startup probes are now best-effort by default: if either probe raises (e.g.

### Migration
- Consumer setup scripts that parsed `payload["ok"]` as the only health signal still work — `ok` now reflects whether at least one of the two probes succeeded.
- Programmatic readers of `checks.stdio_startup` and `checks.cli_fallback_startup` should expect an optional `error` key on each block, present only when that probe failed.

### Versioning realignment
- The standalone `darce/mcp-workbay-handoff` v0.1.0 tag (the original packaging cut) is retired in favour of the in-source `pyproject.toml` version line.

## [0.4.0] — 2026-04-07

### Added
- **Oversize-response advisory warning.** The response envelope built by `_envelope()` now appends an `oversize_response: ~<bytes> bytes (~<tokens> tokens) ...` warning to `payload["warnings"]` whenever the serialised payload exceeds `RESPONSE_OVERSIZE_WARN_BYTES` (default 20,000 bytes, ~5,000 tokens).
- `sections="identity"` for routine identity-only checks (returns just `active` + `limits`).
- `sections="<comma-separated>"` to fetch only the sections you need.
- `detail="summary"` to truncate long-form rationale, fix, and verification fields to 200 chars.
- Lower `top_n_blockers`, `top_n_actions`, `top_n_decisions`, `top_n_tests`, `top_n_findings` to reduce row counts.
- `fields=...` (where supported) to project specific columns.

### Migration — what callers must do
- **Nothing required.** This is an additive change.
- If you were already filtering `payload["warnings"]` for `context_drift:` prefixes, add `oversize_response:` to your filter list to surface the new advisory.
- Treat the advisory as a soft signal: **the next call** should be narrowed, not the current one.

## [0.3.0] — 2026-04-08

### Changed (BREAKING — wire format)
- **MCP tool responses are now native JSON objects, not JSON strings.** Every handoff-mcp tool handler is annotated `-> dict` and returns a real Python dict via `_envelope()` / `_json_response()`.

### Migration — what callers must do
- **Use the canonical access pattern** (this was always documented in the README and `docs/guides/token-efficient-usage.md`, but is now mandatory):
- **Stop wrapping handler results in `json.loads(...)`.** Pre-0.3.0 callers did `parsed = json.loads(handoff_tool(...))`.
- **External tools that read `result.content[0].text` and then `json.loads()` the inner string** must instead read `result.structured_content` directly.
- **Tests** that build a flat-access dict via a local helper like `_parse_response(raw)` should accept both `str` (CLI stdout capture) and `dict` (in-process handler call) input.

### Removed
- The `_make_dict_wrapper` shim in `packages/mcp-workbay-handoff/src/workbay_handoff_mcp/api.py` is gone.
- The `_flatten_v2` helper in `core.py` is gone.
- The legacy top-level mirror introduced by internal (where every `data` field was duplicated at the envelope root) was removed by internal; internal finishes the cleanup by deleting all the bridging shims that depended on it.
