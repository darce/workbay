# mcp-workbay-orchestrator

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.2.8] — 2026-07-13

### Changed
- Host-memory admission governance (internal): `host_resources` probe + policy loader, admission gates on `offload_preflight` / worker-start / `run_offload_pass`, global heavy-suite bulkhead, pre-turn re-check park, post-crash breaker, admission telemetry, and the `workbay-hostgov` CLI.
- Engine self-integrity: new `server_stale_restart_required` typed outcome when the pass engine's on-disk source vanished since import (concurrent env flip) instead of crashing later.
- Close-time per-package smoke at slice closure; `backend_transient` fault classification on the error outcome payload.

## [0.2.7] — 2026-07-12

### Changed
- Offload guardrails (0127): `dispatch_lane_work` enforces a single-active-brief invariant (supersedes prior open dispatch briefs); a no-brief re-dispatch naming an open brief returns `continuation_armed` instead of a bare `params_only`.
- Engine self-integrity: per-pass source-integrity check refuses with typed `server_stale_restart_required` when the engine's own on-disk source vanished since import.
- Close-time package smoke: run each touched package's test dir at slice close (wall-clock-capped, degrades to `smoke_skipped_too_slow`) so a slice that breaks its own package fails at closure, not the merge gate.
- Preflight worktree-env readiness probe + pointer-drift co-signal; hermetic `build_lane_test_cmd` builder and brief-hygiene warnings (unscoped full-suite / full-rebaseline).

## [0.2.6] — 2026-07-11

### Changed
- Version bump for the coordinated release; no tool contract or field-shape changes.

## [0.2.5] — 2026-07-10

### Changed
- Grok session-token observability: session-token reader, per-turn context-delta token persistence, and a usage-source SSOT for the token summary.
- Grok-4.5 model pins with composer-attestation gating (green → handoff_ready).
- Offload orchestration hardening: typed-outcome fidelity, env-hygiene + model-pin verifier, and lane-branch payload-rules freshness preflight.
- Hybrid slice-brief packet (semantic + codemap structural) and blocked-lane aging.

## [0.2.4] — 2026-07-08

### Changed
- Grok offload review topology: two-tier review flow, per-slice dispatch guard, and malformed-handoff salvage (implementation note).

## [0.2.3] — 2026-07-08

### Changed
- Offload worker self-verify gate + Composer quarantine + max-turns checkpoint (implementation note): workers run a structured self-verify command before reporting a terminal outcome; the self-verify timeout decodes captured bytes safely.
- Inject the semantic-reinjection packet into `lane_prompt` worker prompts and persist the reinjection cache across `lane_prompt` subprocesses (internal).

## [0.2.2] — 2026-07-07

### Changed
- Capability-aware offload token governance: preflight now emits a token-governance decision and refuses silent caps (TB-002/004).
- Offload dispatch/worker composition hardening: outcome honesty (`needs_guidance`, timeout contract, dry-run/enum), lane-message durability, and `dispatch_id` idempotency.
- Flush the exclude append before releasing the flock.

## [0.2.1] — 2026-07-04

### Changed
- Add cross-harness `/offload` dispatch support for explicit `--agent` and `--effort` selection, including codex-subagent profiles, token-budget preflight, reviewer pins, and preferred-model manifest handling.

### Added
- **Lane-data CLI subcommands (internal).** `mcp-workbay-orchestrator` now exposes bash-callable `lane-upsert`, `lane-list`, `lane-activity`, `lane-message{,-list,-update}`, and `lane-report{,-list}` adapters over `lanes.py`.

### Changed
- internal: harness launch via `mcp_launch.py` shim; session serve uses workspace `.venv` or bootstrap `uv tool` binary — no per-session PyPI/`uvx` resolve.

## [0.2.0] — 2026-06-26

### Changed
- Projection-spool durability hardening: bounded/idempotent replay, flock-serialized drain, poison-entry dead-lettering past the retry budget, and breaker trips on signal-killed handoff CLI.

## [0.1.0] — 2026-06-22

### Changed
- First release under the WorkBay name; the version line was reset to `0.1.0` for the new PyPI project (greenfield, single-maintainer).

## [0.8.0] — 2026-06-19

### Added
- `workbay_orchestrator_mcp.orchestration.ace_reflect` owns ACE parser/apply, journal recovery, dry-run, and advisory curation helpers.
- Public CLI subcommands: `ace-reflect`, `ace-curation-report`, `ace-metrics`, and `ace-trends`.

### Changed
- `ace-metrics` requires repeatable `--playbook-file` declarations and shares the orchestrator playbook parser with reflection.

## [0.7.0] — 2026-06-11

### Changed
- Bump member pins to the 0.1.24 stack (protocol 0.3.0, handoff 0.13.0); raise the `[bridge]` extra floor to `workbay-codex-bridge>=0.2.0,<0.3.0` so the managed uvx pin resolves the current bridge wheel.

## [0.6.6] — 2026-06-10

### Changed
- Build: migrate sdist build backend setuptools→hatchling with at-build privacy scrub (implementation note sdist-privacy sweep).

## [0.6.5] — 2026-06-08

### Changed
- Privacy: internal project ids scrubbed from shipped source.

## [0.6.3] — 2026-06-07

### Changed
- Re-cut of the unpublished 0.6.2: dependency floors moved to `workbay-protocol>=0.2.4`, `mcp-workbay-handoff>=0.12.6`.

## [0.6.2] — 2026-06-07

### Changed
- Dependency floors raised: `workbay-protocol>=0.2.3`, `mcp-workbay-handoff>=0.12.5` (internal grok harness parity release).

## [0.6.1] — 2026-06-06

### Changed
- `run_structured_turn` kind-branched dispatch: in-process backends route via the adapter runner seam (verbatim schema pass-through, single timeout layer, recursion guard); `probe_availability` annotates downstream prerequisites and `list_available_backends` passes them through (internal).

## [0.6.0] — 2026-06-04

### Changed
- **Breaking default:** `list_available_backends` now probes live availability by default (`probe=true`), so MCP callers and skills can distinguish "declared" from "actually reachable" before dispatching.
- Dependency floor: `workbay-protocol>=0.2.0`.

## [0.5.2] — 2026-06-03

### Added
- **Probed availability for `list_available_backends`.** The tool now accepts an optional `probe: bool = True` argument (CLI: `list-backends --probe`).
- **Optional `bridge` extra.** `workbay-codex-bridge` is now an installable optional-dependency (`mcp-workbay-orchestrator[bridge]`), resolved locally from the sibling source.

## [0.5.1] — 2026-06-01

### Changed
- **Drop the stale `"duplicate altcontext"` orchestrator-guidance string.** Final cleanup of the purged legacy `altcontext-*` naming so generated guidance no longer references a name that no longer exists.

## [0.5.0] — 2026-05-30

### Changed
- **MCP server identity cutover — `workbay-orchestrator-mcp` → `workbay-orchestrator-mcp` (implementation note Slice B).** Canonical registered server name updated; bootstrap collapses any stale duplicate registration to the single canonical name.
- **Doc paths resolve through `workbay_protocol` (implementation note Slice D).** `api` now imports `HARNESS_CONTRACT_RELPATH` and `INSTRUCTIONS_RELPATH` from `workbay-protocol` (>=0.1.6), reading from the renamed `docs/workbay/` mirror.

### Notes
- Coordinated rebrand release with `workbay-protocol` 0.1.6, `mcp-workbay-handoff` 0.12.0, and `workbay-bootstrap` 0.6.0.

## [0.4.7] — 2026-05-13

### Changed
- **`evaluate_review_ready` trusts `current_task_sync.is_violation` explicitly.** Removed the `not current_task_in_sync` fallback that silently re-introduced `CURRENT_TASK.json is out of sync with handoff state` as a hard blocking reason whenever an older `mcp-workbay-handoff` envelope omitted the `is_violation` key.

## [0.4.6] — 2026-05-11

### Changed
- **Track `mcp-workbay-handoff` 0.11.2 contextmanager change**: the local re-exporter `lanes._get_db_connection` now declares its return type as `AbstractContextManager[sqlite3.Connection]` to match the upstream factory, which is now a generator-based context manager that closes the underlying connection on exit.

## [0.4.5] — 2026-05-10

### Changed
- **Bump `mcp-workbay-handoff` floor to `>=0.11.0,<0.12.0`** to pick up internal BR-01/02/03 fixes (multi-active dashboard projection, malformed import-payload rejection, target_branch/worktree_path/plan_path preservation) and internal compaction env-var namespace consolidation.

## [0.4.4] — 2026-05-08

### Changed
- Bump `mcp-workbay-handoff` floor to `>=0.10.0,<0.11.0` so the orchestrator picks up the implementation note surface: side-effect-free preflight validators, dashboard fragment renderer in the production render path, write-contract registry exposed via `limits.write.tools` + `validate_write` tool, and the `mcp_agent_handoff` distribution- name alias.

## [0.4.3] — 2026-05-08

### Changed
- Refresh bundled `_assets/rules/branch-review-guide.md` asset to include the revision-history guidance block.

## [0.4.2] — 2026-05-07

### Changed
- Bump `mcp-workbay-handoff` floor to `>=0.9.0,<0.10.0` so the orchestrator picks up commit-backed review-finding reconciliation (internal) and the working-tree integrity helpers (internal).
- `lane_exec` prefers bash for lane preflight invocations.

## [0.4.1] — 2026-05-04

### Changed
- Bump `mcp-workbay-handoff` floor to `>=0.8.0,<0.9.0` so the orchestrator picks up the new `plan_resolve` / `plan_cli` surface (implementation note / internal) for plan-path resolution.
- Identity-response baseline rebaselined (1159 → 1551 bytes) per internal / internal docs reorganization.

## [0.4.0] — 2026-05-02

### Breaking
- **Minimum Python is now 3.12.** `requires-python` is bumped from `>=3.11` to `>=3.12`, mirroring the same bump in the sibling `mcp-workbay-handoff` package.

### Changed
- **Sibling dependency repinned to `mcp-workbay-handoff>=0.7.0,<0.8.0`.** Tracks the 0.7.0 release of `mcp-workbay-handoff`, which carries the matching `requires-python` floor.

## [0.2.0] — 2026-04-26

### Breaking
- **Distribution published as `mcp-workbay-orchestrator`.** An earlier PyPI name was squatted by an unrelated party; the canonical name aligns with the binary name (`mcp-workbay-orchestrator`) and the sibling `mcp-workbay-handoff`.

### Changed
- Sibling dependency repinned: the previous `workbay-handoff-mcp @ git+ssh://...@v0.4.3` line is replaced with `mcp-workbay-handoff>=0.5.0,<0.6.0` from PyPI.

## [0.1.4] — 2026-04-24

### Breaking
- **Console script is `mcp-workbay-orchestrator`**, matching the `mcp-*` prefix naming convention shared with sibling MCP servers (`mcp-workbay-handoff`, etc.).

### Changed
- `mcp-workbay-handoff` dependency advanced from `v0.4.2` to `v0.4.3` to pick up the paired console-script name (`mcp-workbay-handoff`).
- `run_doctor` and `run_tools_snapshot` now return `{"server": "mcp-workbay-orchestrator"}` to match the new CLI name.
- `argparse` `prog=` and the `doctor` fallback default were updated to `mcp-workbay-orchestrator`.

### Migration
- Update the `command` field wherever the server is launched:
- Consumers parsing the `server` field of `doctor` / `tools-snapshot` output should expect `mcp-workbay-orchestrator` instead of `workbay-orchestrator-mcp`.

## [0.1.1] — 2026-04-22

### Added
- `SliceReviewPacket.external_changed_files` field.

### Changed
- `workbay-handoff-mcp` dependency advanced from `v0.1.0` to `v0.4.1` to pick up the `run_doctor` soft-fail patch and align with the current published consumer install URL.

## [0.1.0 (pre-rename)] — 2026-04-19

### Added
- **Hoist Agentic System MVP packaging metadata.** `pyproject.toml` now declares a `[tool.hoisted]` table for the standalone install surface: `git+ssh://git@github.com/darce/mcp-workbay-orchestrator.git@v{version}`.
