# workbay-bootstrap

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.3.2] — 2026-06-30

### Changed
- Raise the `workbay-system` dependency floor to `>=0.3.1` to pick up the overlay tooling parameterization (internal).

## [0.3.1] — 2026-06-30

### Fixed
- Git-only install regression (internal): `build_uv_tool_install_argv` now passes `uv tool install --no-sources`.

## [0.3.0] — 2026-06-29

### Changed
- Widen `workbay-system` requirement to `>=0.3.0,<0.4.0` for the coordinated 0.3.0 anchor cut.

### Added
- internal: install-time embeddings consent prompt (default yes; `--no-embeddings` / non-interactive opt-out); `embeddings` `--status|--enable|--disable` SSOT gate in `.workbay/embedding.env`.
- internal: `gitonly_closure` builds tag-pinned `uv tool install --with …` argv for MCP members; `install` / `install_plan` run one-time git-sourced MCP tool provisioning and record `gitonly_mcp_tools` on the receipt; `mcp_launch.py` prefers `.venv` then `uv tool` bins (loud serve failure without either).
- internal (C1): idempotent embedding model auto-provision on `install` / `repair` (SHA-256 verified HF fetch, shared `~/.cache/workbay/models/<digest>/` cache, per-worktree `.workbay/embedding.env`); `provision-embeddings` verb; `--no-embeddings` opt-out; `doctor` `embedding_artifact_*` / `embedding_extra_absent` delivery-state.
- internal (Plan B): convergent overlay `install` / `repair` (receipt-driven re-materialization from `.workbay-bootstrap.json`); atomic clone↔package mode-switch; confirm-then-act `clean` / `gc` filesystem reclaimer; `rules` and `guides` `component_kind` on the plugin override seam; `overrides relock` digest refresh; generator ledger surfaces (`scripts/generate_agent_workflows.py`, `config/agent-workflows/portable_commands.json`, `skills/`) recorded in the install manifest; portable Stop-hook adapter forms accepted by `doctor` / `repair` (`{{consumer_root}}`, `$CLAUDE_PROJECT_DIR`); explicit tracked-vs-overlay boundary checks (`tracked_overlay_boundary` findings).

## [0.2.1] — 2026-06-26

### Changed
- Packaging flatten: the stack version anchor is now the `workbay` front door (not `workbay-stack`).

## [0.2.0] — 2026-06-26

### Changed
- Regenerated managed MCP-server uvx pins (`mcp-workbay-handoff@0.2.0`, `mcp-workbay-orchestrator[bridge]@0.2.0`) and workspace-aware interpreter selection in the hook launchers (implementation note).

## [0.1.0] — 2026-06-22

### Changed
- First release under the WorkBay name; the version line was reset to `0.1.0` for the new PyPI project (greenfield, single-maintainer).

## [0.11.0] — 2026-06-19

### Changed
- Raise `workbay-system` dependency floor to `>=0.5.0,<0.6.0` for ACE overlay 0.5.0 compatibility (implementation note stack pin alignment).
- Bump the generated default MCP launch pin to `mcp-workbay-handoff@0.13.3`, ensuring the published bootstrap wheel carries the handoff schema-skew self-heal release.

## [0.10.0] — 2026-06-12

### Added
- Cursor harness adapter (internal): pin-surface emission, adopt/install plumbing, and MCP sync for the Cursor harness.

## [0.9.0] — 2026-06-11

### Changed
- Compositional Claude settings hooks: per-install overrides aggregate over the managed base (internal / internal); obsolete shared Claude hook flags removed from install plumbing with canonical-key migration dedup.
- Bump managed MCP-server uvx pins: `mcp-workbay-handoff@0.13.0`, `mcp-workbay-orchestrator[bridge]@0.7.0`.

## [0.8.10] — 2026-06-10

### Changed
- Build: migrate sdist build backend setuptools→hatchling with at-build privacy scrub (implementation note sdist-privacy sweep).
- Fix: bound the `run_doctor` stdio probe with `asyncio.wait_for` so a hung MCP server no longer stalls doctor (internal).
- MCP launch pins moved to `mcp-workbay-handoff@0.12.9` / `mcp-workbay-orchestrator[bridge]@0.6.6`.

### Removed
- implementation note: removed `--install-claude-stop-hook`, `--install-claude-reinject-hook`, `--install-claude-error-hook`, and `--install-claude-error-hook-local`.

## [0.8.9] — 2026-06-08

### Changed
- Privacy: internal project ids scrubbed from shipped source.
- MCP launch pins moved to `mcp-workbay-handoff@0.12.8` / `mcp-workbay-orchestrator[bridge]@0.6.5`.

## [0.8.8] — 2026-06-07

### Fixed
- Grok plugin activation is now idempotent: a re-run where `grok plugin install` exits non-zero with "already installed" (same content hash already registered) is treated as success and reports the distinct `already_present` action instead of `failed`, so repeated `make dogfood` deploys no longer report a spurious grok activation failure.

## [0.8.7] — 2026-06-07

### Added
- internal: `--install-claude-error-hook` and `--install-claude-error-hook-local` opt-in flags write the bootstrap-managed PostToolUse(Bash) `capture-agent-errors` adapter into `.claude/settings.json` / `.claude/settings.local.json`.

## [0.8.6] — 2026-06-07

### Changed
- Re-cut of the unpublished 0.8.5 with the runtime `__version__` string synced to the package version; managed MCP pins moved to `mcp-workbay-handoff@0.12.6` + `mcp-workbay-orchestrator[bridge]@0.6.3`.

## [0.8.5] — 2026-06-07

### Changed
- implementation note: MCP registration ownership is single-sourced from the canonical `mcp_servers.yaml` `registration` table.
- Plugin-tree integrity is self-describing: `plugin.json` must declare `mcpServers` exactly when a sibling `.mcp.json` exists; doctor flags both the declares-but-missing and stray-`.mcp.json` (dual registration) cases.

## [0.8.4] — 2026-06-06

### Added
- Canonical `HARNESS_PLUGIN_DELIVERY` registry as the single source of truth for per-harness plugin delivery, with `.get`-guarded registry access and a harness-key namespace comment.

### Fixed
- Reconcile stale managed gitignore blocks instead of leaving drift behind (REV17A2E2E-B-01); `adopt --check` now detects drift via leak-based probing.
- Ignore the materialized grok plugin tree; CONSUMER.md fence cleanup plus sentinel note.

## [0.8.3] — 2026-06-06

### Added
- Hook coherence gating on install/update with fail-open `_run_guard.py` wrapper, render injection + coherence self-check, and self-host mount repoint to live payload (internal).
- Reinject-context SessionStart adapters with `supported_harnesses` schema, `--install-claude-reinject-hook[-local]` opt-ins preserved across update, and per-family `hook_adapters` doctor facet (internal / internal).

### Changed
- MCP launch pins bumped: `mcp-workbay-handoff@0.12.4`, `mcp-workbay-orchestrator[bridge]@0.6.1`.

## [0.8.2] — 2026-06-04

### Added
- **Doctor visibility for `source=local` managed surfaces (implementation note):** doctor now diffs local content against the payload and classifies `local_redundant` (identical to current payload), `local_stale` (identical to an older payload revision — the update-starvation signature), or `local_override` (consumer-authored, informational only).
- **Install/update skip reporting:** every surface kept under local precedence is named (`skipped (local precedence): <path>`) plus an aggregate count, so a receipt that bumps `remote_ref` while local copies starve updates can no longer report silently.
- **Opt-in adoption — `repair --adopt-stale-local SURFACE`:** backs the local copy up under `.workbay/backup/<ts>/`, re-materializes the managed surface per source kind (symlink for git overlay, copy for package source; carved surfaces rebuild install's per-child form so carve-excluded children stay absent), and flips the receipt back to `source=shared`.

### Fixed
- Carved surfaces (`Makefile.d`, `scripts/workbay`) classify correctly: carve-excluded children are stripped from both comparison sides instead of every carved surface falling through to `local_override`.

## [0.8.1] — 2026-06-04

### Changed
- Re-cut of the unreleased 0.8.0 with the runtime `__version__` string synced to the package version; managed MCP server pin moved to `mcp-workbay-handoff@0.12.3`.

## [0.8.0] — 2026-06-04

### Added
- **Durable consumer recipe overrides (internal):** always-effective plugin composition — marketplace pins now always target the `.workbay/generated/plugins/<plugin>/effective/` tree (recomposed as a passthrough copy of base when no overrides exist); skill patch mode via `git merge-file` 3-way merge; new `overrides` CLI surface (`status` / `accept-upstream`) with override lock bookkeeping; consumer migration on `install`/`update`.
- Primary override root resolution from adopted linked worktrees.

### Changed
- Dependency floors: `workbay-protocol>=0.2.0`, `workbay-system>=0.2.0`; managed MCP server pins moved to `mcp-workbay-handoff@0.12.3` and `mcp-workbay-orchestrator@0.6.0`.

## [0.7.2] — 2026-06-02

### Added
- **Linked-worktree overlay self-heal (implementation note):** `adopt-worktree` re-runs the install materializer against a linked worktree with `clone=<primary>/.workbay/remote`; worktree-aware `doctor`/`repair` short-circuit emits a single `unadopted_worktree` finding; a managed sentinel-delimited `.gitignore` block keeps an adopted worktree's `git status` clean.

### Changed
- **Apply and `--check` share one surface enumeration (implementation note S1, `revB-install-private-symbol-coupling`):** new `iter_expected_surface_targets` is the single source of the surface/carve/exclusion rule consumed by both the materializer and `adopt._compute_drift`, so the drift guard can no longer desync from apply; the `_materialize_surfaces_copy` (package-install) path is single-sourced through the same helper.
- **Overlay-root resolver prefers a materialized overlay (implementation note S3, `revA-overlay-root-unbounded-walk`):** the upward walk skips an unmaterialized stray ancestor marker, falling back to the nearest marker so a genuinely un-materialized primary still fails loudly.
- **Relocation repoint (implementation note S3b, `revB-relocation-dangling-symlink-no-repoint`):** a dangling bootstrap-owned surface link (e.g.

### Notes
- `.claude-plugin/marketplace.json` continues to resolve via the tracked file + the adopted `.workbay/generated` symlink; `adopt` does not materialize it (implementation note S2 locks this contract).

## [0.7.1] — 2026-06-02

### Fixed
- **Default managed orchestrator pin realigned to `mcp-workbay-orchestrator@0.5.1`.** The 0.7.0 coordinated release published orchestrator 0.5.1 but left `DEFAULT_MCP_SERVERS` pinned at `@0.5.0`, so package-source / default-server installs launched the superseded 0.5.0 wheel via `uvx`.

## [0.7.0] — 2026-06-01

### Added
- **Package-source overlay delivery (internal).** `install` / `update` can resolve the harness overlay from an installed `workbay-system` distribution (`source_kind="package"`) instead of a git clone, writing a `package`-kind `BootstrapManifest`.

### Changed
- **MCP launch decoupled from resolution (implementation note Theme A, internal).** Generated serve commands now use `uv run --no-sync` and the server venvs are pre-built at install time (`_presync_local_mcp_envs`), eliminating the cold-start re-sync race that overran the harness's 30s MCP connection timeout and registered zero tools.

### Removed
- **Runtime-path migration shim retired (implementation note Slice D cutover complete).** The legacy-runtime-tree migration helpers (`migrate_runtime_paths()`, `plan_runtime_path_migration()`) and the `legacy_runtime_path` `doctor` check are removed.

## [0.6.0] — 2026-05-30

### Changed
- **MCP server identity cutover (implementation note Slice B).** Default managed servers register under the canonical `workbay-handoff-mcp` / `workbay-orchestrator-mcp` names.
- **Default managed server pins bumped** to `mcp-workbay-handoff@0.12.0` and `mcp-workbay-orchestrator@0.5.0`.

### Added
- **Runtime path migration `.workbay/` → `.workbay/` and `docs/workbay/` → `docs/workbay/` (implementation note Slice D).** `migrate_runtime_paths()` / `plan_runtime_path_migration()` run on `install`/`update`: idempotent, archive-backed (a both-present collision moves the legacy tree aside rather than overwriting), dry-run-capable, and routed through the shared `workbay_protocol.paths` constants.

## [0.5.2] — 2026-05-20

### Changed
- **Bump the default managed handoff server pin** to `mcp-workbay-handoff@0.11.5` so fresh `workbay-bootstrap install` dogfood installs pick up the `ACTIVE TASK PLANS` task-plan-path fix by default.

## [0.5.1] — 2026-05-11

### Fixed
- **internal — stale shared-surface symlinks now repointed on rerun.** When a consumer was installed pre-v0.2.0 (legacy root layout `<clone>/<surface>`) and the layout subsequently moved into `<clone>/packages/workbay-system/<surface>`, the target-side `scripts/hooks -> ../.workbay/remote/scripts/hooks` symlink survived every subsequent `workbay-bootstrap install` because the pre-fix `points_into_clone` check still passed lexically for the broken resolved path.

## [0.5.0] — 2026-05-10

### Changed
- **internal — cross-harness install manifest is now the single source of truth for hook adapter wiring.** `config/agent-workflows/portable_commands.json` schema v2 introduces a top-level `hooks[]` array; install dispatches adapter rows through a manifest-driven walker (closed-set operation table) instead of bespoke per-harness writers.
- **CLI default profile flips back to `all`** so a no-argument `workbay-bootstrap install` materializes the full surface set out of the box — per-agent generated surfaces (`.claude/skills`, `.claude/commands`, `.github/prompts`, `.codex/skills`), shared overlay symlinks (`scripts/hooks`, `Makefile.d`, `scripts/workbay`, …), and the lifecycle hoist (`Makefile.d/lifecycle.mk` plus the sentinel-bracketed `-include` block).
- **The install manifest (`.workbay-bootstrap.json`) now records the active profile** under `manifest["profile"]` so downstream tools (`sync`, `doctor`, rehearsals) can reason about what the consumer installed without re-inferring it from the surface set.
- **Stop-hook wiring is fully opt-in across every harness shipped by the manifest.** The legacy `--harness-hook-scope` flag is replaced by four boolean flags: `--install-claude-stop-hook` (shared, checked-in `.claude/settings.json`), `--install-claude-stop-hook-local` (user-owned, gitignored `.claude/settings.local.json`), `--install-codex-stop-hook` (`.codex/hooks/stop.json`), and `--install-vscode-stop-hook` (`.vscode/agentic-stop-hooks.json`).

## [0.4.2] — 2026-05-10

### Changed
- **Bump default managed MCP server pins** to `mcp-workbay-handoff@0.11.1` and `mcp-workbay-orchestrator@0.4.5` so consumer repos pick up the internal (multi-active CURRENT_TASK projection, import/export malformed-payload rejection, target_branch/worktree_path/plan_path preservation) and internal (compaction env-var namespace consolidation under `AGENT_HANDOFF_COMPACTION_*` with `internal_*` kept as a deprecated alias) fixes by default.

## [0.4.1] — 2026-05-09

### Fixed
- **`--profile all` now performs the lifecycle hoist** (internal).

### Changed
- **Default managed MCP servers are now version-pinned.** The built-in `--mcp-servers default` map writes `uvx mcp-workbay-handoff@0.11.0` and `uvx mcp-workbay-orchestrator@0.4.4` (rather than unpinned `uvx mcp-workbay-handoff` / `uvx mcp-workbay-orchestrator`) so consumer repos do not silently drift when PyPI advances independently of the overlay tag they bootstrapped against.
- **Raise `workbay-protocol` lower bound to `>=0.1.4,<0.2.0`** to match the floor pinned by `mcp-workbay-handoff` 0.11.0 and `mcp-workbay-orchestrator` 0.4.4 — the two packages bootstrap launches via `uvx` — so the bootstrap venv cannot resolve a protocol release older than what those servers import at startup.

## [0.4.0] — 2026-05-04

### Added
- **Install profile contract: `--profile {minimal,lifecycle,all}`** (implementation note / internal.5.a).
- **Hoist `Makefile.d/plans.mk` + `git-plan-cat.sh` stub** (implementation note / internal).

### Changed
- Bootstrap installs and rehearsals are validated against `mcp-workbay-handoff>=0.8.0` (the version that ships the `plan_resolve` / `plan_cli` surface targeted by the new make recipes).

## [0.3.1] — 2026-05-03

- **implementation note BR-01 — raise `workbay-protocol` lower bound to `>=0.1.2,<0.2.0`.** Bootstrap's default install path invokes `uvx mcp-workbay-handoff`, which imports `workbay_protocol.branch_naming` at startup; the previous `>=0.1.0` floor let `uvx` resolve a protocol release missing the module, crashing init-state on a fresh install.
- **internal — install rehearsal pins six-hook surface + helper materialization.** `SHARED_GIT_HOOK_NAMES` now includes `pre-commit` (in addition to `post-checkout`, `post-commit`, `post-merge`, `post-rewrite`, `pre-push`); the install rehearsal test asserts that `core.hooksPath` resolves to the directory carrying all six executable hook scripts AND that the Python helper `scripts/hooks/check_branch_naming.py` (the delegate the post-checkout / pre-commit / pre-push hooks `exec`) is materialized alongside them.
- **Manifest renamed: `.workbay-overlay.json` → `.workbay-bootstrap.json` (schema_version bumped to 2).** Resolves a name collision with consumer repos that also use `.workbay-overlay.json` for unrelated config.
- **Managed MCP defaults now launch stdio servers.** The built-in `--mcp-servers default` map writes `uvx mcp-workbay-handoff --workspace-root .
- **`regenerate-task-views` harness hook contract dropped (implementation note).** Bootstrap no longer materializes any Claude / VS Code / Codex hook wiring that invokes `regenerate-task-views`; `DASHBOARD.txt` is now auto-regenerated server-side inside `mcp-workbay-handoff` on every state-mutating MCP call.

## 0.3.0 — 2026-04-28

- **Install-time state provisioning (implementation note).** `install` now runs the handoff server's `init-state` after surface/config materialization but before `core.hooksPath` is set, so a fresh install ends with a schema-current `.task-state/handoff.db` and `.task-state/exports/` ready for the first MCP call.
- **Required-surfaces refusal runs before the generator and init-state.** A failing required-surface check now leaves no `.task-state/`, no generated artifacts, and no manifest behind on disk, so refused installs no longer half-write the target.
- **`status` reports handoff state.** When the install registered MCP servers, `status` invokes `init-state --check` and appends the resolved `state_dir` / `db_path` / `exports_dir` / `schema_version` / `initialized` to the summary.
- **`doctor` flags missing `.task-state/handoff.db` as `state_drift`,** gated on `.mcp.json` being present in the manifest's `configs` array so config-only installs (`--no-mcp-servers`) do not produce false-positive drift.
- **`switch_task` cold-start fix.** internal (in `mcp-workbay-handoff` 0.5.0+) drops `BranchMismatchError` from `switch_task`; the cold-start cycle (register task → `switch_task` → first content write) now completes from any branch.

## 0.2.1 — 2026-04-26

- Add `pyyaml>=6` to runtime dependencies.

## 0.2.0 — 2026-04-26

- Resolve shared overlay surfaces and the agent-workflow generator under `packages/workbay-system/` (the workbay monorepo layout) with fallback to the clone root for legacy hoisted overlays.
- Rehearsal fixture (`fake_remote_with_generator`) now mirrors the real monorepo layout so this regression cannot return silently.
