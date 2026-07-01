# workbay-system

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.3.3] — 2026-07-01

### Changed
- Compaction contract single-sourcing: the payload `docs/workbay/contracts/harness-protocol.yaml` `compaction.threshold_tokens` is the sole source for the Stop-hook token gate (root contract stays a byte-identical materialized mirror).

## [0.3.2] — 2026-06-30

### Fixed
- Git-only init-state regression (internal): the `scripts/hooks/mcp_launch.py` `uv run --no-sync --project` fallback for a git_overlay clone now passes `--no-sources`, so the cloned package's `[tool.uv.sources] { workspace = true }` pins no longer abort `uv run` ("references a workspace ...

## [0.3.1] — 2026-06-30

### Changed
- Overlay tooling parameterization (internal): uniform `--target` / `--packages-root` CLI across `check_harness_sync`, `check_skills`, and `lint_hoisted_paths`; added `ResolutionContext` and threaded `packages_root` through the fixture sites so the checks resolve against an explicit overlay root instead of an implicit cwd.

## [0.3.0] — 2026-06-29

### Changed
- internal: git-only delivery — `mcp_servers.yaml` registers `scripts/hooks/mcp_launch.py` (no `uvx …@` pins); `release_public.py` exports/tags only; MCP harness contracts document shim resolution and bootstrap one-time `uv tool install`.

### Added
- internal: portable `/workbay` guide command and `workbay` skill (Claude `AskUserQuestion` embeddings menu; positional `/workbay embeddings <on|off|status>` elsewhere) delegating to `workbay-bootstrap embeddings`.
- implementation note (internal): `/review-parallel` slice-scoped redesign — default fan-out partitions by completed slice packets (one reviewer per slice under coordinator `…-REV-…-S<n>-…` refs), per-slice merge with source retirement, a whole-branch harmonization pass for cross-slice integration lenses, and an explicit whole-branch fallback when no `slice_complete_*` decisions exist; adversarial record-only reviewer contract; optional slice-anchored semantic context in the reviewer flow.
- `workbay_system.overlay_tooling` port module plus `workbay-overlay-tooling` CLI (`check-harness-sync`, `check-skills`, `lint-hoisted-paths`, `generate-agent-workflows`); legacy `scripts/*.py` paths are thin shims.
- Portable `/refactor` command materialized in `portable_commands.json` with regenerated router blocks (`CLAUDE.md`, `docs/workbay/instructions.md`).

### Changed
- Refactoring lens wiring across `incremental-implementation`, `branch-review`, `plan-analyze`, and `review-parallel/default.md` (produce-first shape gate, See Also cross-refs, lens definitions, and heuristics anchors).
- Rot-guard: `make check-heuristics-wiring` validates prompt/skill wiring.

## [0.2.1] — 2026-06-26

### Changed
- Packaging flatten: `make workbay-update` / `update.sh` now upgrade the `workbay` front-door anchor (which carries the exact member pins) instead of the deprecated `workbay-stack` shim.

## [0.2.0] — 2026-06-26

### Changed
- Lifecycle features: `--plan-intent` plan-id procurement + branch/worktree/row auto-relink, doctor interpreter-skew and stack-update probes, workspace `.venv` provisioning over a uv workspace, and a close-check ambiguous-`task_ref` guard (Plans 0064/0066/0067).

## [0.1.0] — 2026-06-22

### Changed
- First release under the WorkBay name; the version line was reset to `0.1.0` for the new PyPI project (greenfield, single-maintainer).

## [0.5.0] — 2026-06-19

### Added
- `Makefile.d/ace.mk` exposes `ace-reflect`, `ace-curation-report`, `ace-metrics`, `ace-metrics-json`, and `ace-trends` via the managed orchestrator launcher.

### Changed
- Reconcile ACE capture docs to PostToolUse hook semantics and require explicit playbook declarations at the Make surface.
- Bump the canonical orchestrator uvx pin to `mcp-workbay-orchestrator[bridge]@0.8.0`.
- MCP pin bump to `mcp-workbay-handoff@0.13.3` (handoff schema-skew self-heal, implementation note).

## [0.4.2] — 2026-06-18

### Changed
- Lifecycle payload rollout: review-parallel workflow, plan-done archive gate, tasks GC/resolver coverage, hook/contract/guide updates, and MCP pin bump to `mcp-workbay-handoff@0.13.2`.

## [0.4.1] — 2026-06-13

### Changed
- Cursor plugin tree now emits a skills-only `.native-skills-only` marker instead of per-command markdown (`_CURSOR_COMMANDS_SKILLS_ONLY_MARKER`).
- Handoff/branch-review skill bodies teach export/import, `list_handoff_rows`, `next_actions`, and the `expected_revision`/commit-backed resolve recipes; added the in-session skills index and README session-start hydration.
- Restored the `ensure-agent-surfaces` SessionStart hook and fixed overlay drift in payload hook/contract surfaces.
- Bumped the canonical `mcp-workbay-handoff` uvx pin to `0.13.1`.

## [0.4.0] — 2026-06-12

### Added
- Cursor harness payload surfaces (internal) — workflow/MCP config emitted for the Cursor harness alongside Claude/Codex/Grok/VS Code.

## [0.3.0] — 2026-06-11

### Added
- Published-bytes release gate: A1-bit reproducible build with per-package SDE signoff + sha256 dispatch parity, fail-closed; `validate-gate` subcommand dispatches a `dry_run=true` gate-without-publish workflow (implementation note).

### Changed
- Plan-id branch/worktree naming surfaced through the workflow payload (internal); compaction reinject activation (internal); `review_findings` merge status-preservation documented in contract + guide (implementation note).
- Bump canonical MCP-server pins: `mcp-workbay-handoff@0.13.0`, `mcp-workbay-orchestrator[bridge]@0.7.0`.

## [0.2.11] — 2026-06-10

### Changed
- Build: migrate sdist build backend setuptools→hatchling with at-build privacy scrub (implementation note sdist-privacy sweep).
- Feature: wholesale ownership of `.claude/settings.json` with managed-hook reconciliation and migration (plans 0035/0036).
- Feature: compaction-feedback notify envelope plus tokens-saved telemetry (implementation note).
- Fix: stop-hook compaction evaluation now proceeds past the 50k-token gate (internal); advisory threshold aligned to the 50k/200k canonical default.
- MCP launch pins moved to `mcp-workbay-handoff@0.12.9` / `mcp-workbay-orchestrator[bridge]@0.6.6`.

### Removed
- implementation note: removed shared Claude opt-in hook adapters from `portable_commands.json` (`--install-claude-stop-hook`, `--install-claude-reinject-hook`, `--install-claude-error-hook[-local]`).

## [0.2.10] — 2026-06-08

### Changed
- Privacy: internal project ids scrubbed from the shipped payload.

## [0.2.9] — 2026-06-08

### Fixed
- Privacy: removed internal planning-artifact references from shipped payload surfaces; the public-export gate now fails closed on them.

## [0.2.8] — 2026-06-08

### Fixed
- Privacy: removed personal identifiers from shipped and exported surfaces; the public-export gate now fails closed on them.

## [0.2.7] — 2026-06-08

### Added
- internal (implementation note): `LIFECYCLE_WORKTREE_BOOTSTRAP` hook — after a worktree is provisioned and adopted, `make task-start` runs an optional consumer-declared shell command (e.g.

## [0.2.6] — 2026-06-07

### Added
- internal: `capture-agent-errors` hook family in `portable_commands.json` (claude-code PostToolUse bridge) plus dogfood install docs for `--install-claude-error-hook`.
- internal: `grok_command` on `capture-agent-errors` contract entry; `check_harness_sync` contract-field gate; regression test that grok plugin `hooks.json` stays PreToolUse-only.
- implementation note review fix (REV-E-010): `_resolve_harness` in `capture-agent-errors.py` / `compact-session.py` now attributes grok via `GROK_WORKSPACE_ROOT` when `WORKBAY_HANDOFF_HARNESS` is unset — the compat-loaded `.claude/settings.json` delivery path carries no inline env export (internal), so grok-originated rows previously mislabeled as claude-code.

## [0.2.5] — 2026-06-07

### Fixed
- Re-cut of 0.2.4, whose published wheel was corrupted by the public-export scrub: the case-insensitive inline prefix regex matched `internal` inside `fnmatch.fnmatchcase` in the payload hooks `_harness_protocol.py` and `guard-task-plan-findings.py`, breaking surface-pattern matching at import time for consumers.

## [0.2.4] — 2026-06-07

### Changed
- Re-cut of the unpublished 0.2.3 with payload `mcp_servers.yaml` pins moved to `mcp-workbay-handoff@0.12.6` + `mcp-workbay-orchestrator[bridge]@0.6.3`.

## [0.2.3] — 2026-06-07

### Changed
- implementation note: `mcp_servers.yaml` gains a per-harness `registration` ownership table (root vs plugin) as the single source for every MCP registration surface; `vscode` joins the canonical server `harnesses` lists.
- Plugin emission is registration-aware: `plugin.json` declares `mcpServers` (with a sibling `.mcp.json`) only under plugin ownership; `registration: plugin` is rejected for harnesses without an emitted plugin tree, and all-root composition refuses unconsumed `components.mcp_servers` overrides with a per-server consumption gate.

## [0.2.2] — 2026-06-06

### Added
- Ship referenced rules docs canonical in payload rules (planning-review-guide + 4 companion docs) with payload-wide link-resolution and byte-equality drift guards (internal).

### Changed
- Payload `mcp_servers.yaml` pins bumped: `mcp-workbay-handoff@0.12.4`, `mcp-workbay-orchestrator[bridge]@0.6.1`.
- Render coherence + harness sync gates; reinject budget floor and fence sanitization (internal review fixes).

## [0.2.1] — 2026-06-04

### Fixed
- Bash main-branch guard (implementation note, BR-17): separator-aware effective-cwd tracking (`cd` propagates through `&&`/`;` only — `|`/`||`/`&` degrade to unknown fail-closed), so the prescribed cd-into-worktree fallback pattern is no longer false-blocked; `git -C <dir>` global options parsed before subcommand detection (closes an invisible-stage bypass); Python `-c` inline write targets resolved against the stage's effective cwd.

### Changed
- Bypass env var renamed to `WORKBAY_ALLOW_BASH_MAIN_WRITE` (legacy `ALT_*` honored with a deprecation warning); inline leading-assignment bypass now parsed (first stage only); every bypass writes a jsonl audit record (env|inline source).

## [0.2.0] — 2026-06-04

### Added
- Payload `generate_agent_workflows.py` composes the always-effective plugin tree (`effective/{claude,codex}`) from base plus consumer overrides, and propagates manifest `global_instructions` to every harness surface (Claude, VS Code, Codex adapters).
- `plugins.mk` targets for effective-tree composition and pin checks.

### Changed
- Plugin distribution docs and managed MCP server pins updated (`mcp-workbay-handoff@0.12.3`, `mcp-workbay-orchestrator@0.6.0`); marketplace pins documented against the effective tree.

## [0.1.4] — 2026-06-03

### Fixed
- **Claude Stop-hook adapters now emit Claude-valid entries.** The claude-code compact-session adapters in `portable_commands.json` used a flat `{"_managed_by", "command"}` entry that Claude Code silently ignores; they now emit the required nested `{"matcher": "", "hooks": [{"type": "command", ...}]}` shape, and the canonical command is `python3 "$CLAUDE_PROJECT_DIR/scripts/hooks/compact-session.py"` so fresh installs match (and no longer report drift against) the form working consumers already carry.

## [0.1.3] — 2026-06-03

### Changed
- implementation note S4 release-pipeline hardening: preflight distinguishes unverifiable from missing artifacts, the publish gate locks the accepted release-state set, and `pypi_without_tag` reconciliation is provenance-verified with bidirectional byte-parity.
- Release publishing moved from local `twine upload` to `gh workflow run` (PyPI Trusted Publishing).
- `make dogfood` gained `DOGFOOD_SOURCE=package` to rehearse the package-source install path; managed MCP-server pins bumped for the post-Plan-0020-S4 releases.

## [0.1.2] — 2026-06-02

### Added
- `task-start` now adopts the bootstrap overlay into a freshly created linked worktree (implementation note S3 durable self-heal trigger), and its gate walks upward for a *materialized* overlay so nested-source layouts self-heal (implementation note S4, `revC-nested-source-marker-gate-mismatch`); the monorepo self-host (tracked marker, no clone) still skips without spawning a doomed subprocess.

### Changed
- implementation note upstream asks D/E/G: `lifecycle.mk tasks-gc` → `archive --operation gc`; `check-agent-workflows` now validates the Codex router block; git hooks resolve guards via `GUARD_DIR` rather than `$REPO_ROOT`.
- Regenerated the per-harness workflow adapters so the Claude/Codex/VS Code surfaces match the manifest (review-parallel prompt drift).

## [0.1.1] — 2026-06-02

### Fixed
- **Plugin-emission orchestrator pin realigned to `mcp-workbay-orchestrator@0.5.1`.** `config/agent-workflows/mcp_servers.yaml` — the hand-maintained source for the emitted Claude/Codex plugin `.mcp.json` server maps — still pinned `@0.5.0` after the coordinated release published orchestrator 0.5.1, so the emitted plugin trees launched the superseded wheel.

### Added
- **implementation note — feature-branch naming enforcement.** Four-layer gate uniformly classifies every branch as protected (`main`/`master`/`release/*`/`hotfix/*`), conforming (matches `workbay_protocol.branch_naming.TASK_REF_RE`), or non-conforming.

### Changed
- `$branch-review` now records branch-diff review runs with `subject_kind="branch"` and explicitly forbids direct writes to `.task-state/handoff.db`; when MCP tools are unavailable, agents should use the `mcp-workbay-handoff` CLI wrapper or stop with a blocker.
- Handoff and review skills now require final responses to print MCP write receipts with row ids (`decision id`, `test_result id`, `review_run id`, finding counts, and dashboard refresh status) so context compaction can recover exact handoff provenance.
- Public skill and review-rule guidance now uses repo-local manifests, placeholder paths, and stack-level commands instead of consumer installation paths or lane names.

### Removed
- Removed the final consumer-specific conflict/sync contract from the hoisted contract surface; the current packaged contract set is the six agent/agentic contracts only.

## [0.2.0] — 2026-04-22

### Added
- Eleven previously empty skill folders are now populated with their full `SKILL.md` (commit2git, daemon-lifecycle, document-sync, investigate, refactor, rescue-lane, review, security-audit, subfeature-committer, worktree-orchestrator, worktree-worker).
- `.claude/commands/` and `.github/prompts/` are now hoisted, providing the eleven managed portable-command adapters (`auto-fix`, `branch-lifecycle`, `branch-review`, `handoff-lifecycle`, `incremental-implementation`, `plan-analyze`, `planning-review`, `review-parallel`, `review`, `scope`, `tdd`).
- `config/lane-orchestration/` lane configuration surface.
- `docs/workbay/templates/` shared planning/review/decision templates.
- `scripts/lint_hoisted_paths.py` portability linter plus its `scripts/overlay_resolver.py` dependency.
- Root `.gitignore` excluding `__pycache__/`, `*.pyc`, and `.DS_Store`.

### Changed
- `scripts/hooks/filter-test-output.py` and its test refresh.
- `.claude/skills/plan-analyze/SKILL.md` and `.claude/skills/planning-review/SKILL.md` updated to reference `docs/workbay/templates/TASK_PLAN.template.md` (now hoisted).
- `docs/workbay/contracts/repo-intel-mcp-candidates.md` content refresh.

### Removed (contract split)
- Eight alt-context-monorepo-specific contracts moved out of the hoisted surface so `workbay-system` carries only agent/agentic contracts:
- `cluster-delta-api.md`
- `cluster-snapshot-api.md`
- `clustering-api.md`
- `curation-sync-api.md`
- `recognition-clustering.md`
- `recognition-media-xmp-mapping.md`
- `security.md`
- `suggestion-extensions-api.md`

### Verification
- `python3 scripts/lint_hoisted_paths.py --repo-root .` → `lint-hoisted-paths: PASS`.
- `find .claude/skills -maxdepth 2 -name SKILL.md | wc -l` → `21`.
- `ls docs/workbay/contracts | wc -l` → `7`.

## [0.1.0 (pre-rename)] — 2026-04-21

### Added
- Initial extraction from `upstream-harness-repo` HEAD `841b8fb2e080f54e5e47b99cd911c254fb61c248` on branch `feature/e17-10`.
