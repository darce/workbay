# mcp-workbay-orchestrator

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.2.21] — 2026-08-24

### Changed
- Offload timeout caps now have a single source of truth (`orchestration/offload_timeout_ssot.py`); `resolve_adapter_timeout_cap` reads the profile table for every backend and the cursor-only bypass is gone.
- `review_runner` salvages a review whose output cannot be parsed instead of dropping it, and no longer records findings from a degraded parse.
- `lanes` was split into `lanes_support`; the import-acyclicity guard resolves every relative and absolute import spelling.
- Plan-cursor updates are compare-and-set guarded; revival from an expired cursor keeps the reaper note as provenance.
- Restored reviewer and worker-gate pins (measured 20260727 snapshots, live off-box rank pin) and removed dead code from the registry split.

## [0.2.20] — 2026-08-23

### Added
- `AuthPort` credential ports declared on off-box backend rows; remote auth probes (status argv, transport-vs-credential states) are rendered from the declared port (implementation note S1–S2).
- `0xalpha-remote` backend row with a key-info credential probe, invocation recipe, offload profile, and lane `spend_bound`; only advertised efforts are shipped (implementation note S4).
- Budget alert at the preflight seam, wired into the spawn edge; backend telemetry pin (implementation note S5).

### Fixed
- codex-remote `status_argv`, narrowed `AuthPort.env_file` typing, hardened key-info probe with a pre-source health check and finite output.

## [0.2.19] — 2026-08-22

### Added
- **Doctor `mcp_protocol` facet.** Orchestrator `doctor` reports the same `{ok, sdk_version, latest_protocol_version, fastmcp_version, declared_fastmcp, note?}` shape as handoff.

### Fixed
- **Own-package fastmcp pin drift now surfaces in the doctor facet (CL0816-MCPSPEC-R3REV-claude-03).** `run_doctor` forwarded handoff's `mcp_protocol` facet verbatim; it never read orchestrator's own declared `fastmcp` pin, so a pin drift between the two packages' `pyproject.toml` files went undetected.
- **Doctor facet tests no longer only exercise self-injected values (round-3 claude-06).** Every prior test in `test_doctor_mcp_protocol_facet.py` monkeypatched `importlib.metadata.requires` to return the exact pyproject string it then asserted against, and the checkout-walk fallback was only ever exercised by patching it to `None`.

## [0.2.18] — 2026-08-17

### Added
- `REMOTE_OFFLOAD_BACKENDS` is derived from `dispatchable_off_box`, and `codex-remote` and `cursor-remote` are registered as offload profiles (implementation note S1/S2).
- Curated `cursor-remote` model allow-list covering kimi-k3 and grok-4.6, plus per-profile adapter-timeout ceilings.

### Fixed
- Remote-lane `token_usage` is populated from the agent debug log; harvested counts are bounded and harvest misses are observable rather than silent.
- Token harvest: a recoverable tail record is no longer discarded, `modelCalls` is pinned as the cumulative-usage discriminator, and the harvested record is legible to the promoter and hardened against an untrusted log.
- Lane sandbox quarantine: the hook-helper surface is quarantined, the harvest sanitizer is tightened, each lane gets distinct `writable_roots`, and the hunk latch no longer accepts a folded `.git` path.
- `.git` is allowed again for codex lanes, and the retired grok pin moves to 4.6.

## [0.2.17] — 2026-08-10

### Fixed
- **Exit-8 envelope honesty.** When a remote apply failed, the stderr that explained why did not reach the consumer.
- **Exit-3 stderr reaches `blockers[0]`.** The same class of defect on the apply-blocked path: the diagnosis existed and was dropped before the consumer.
- **Single-owner stop-reason registry with a fail-closed reject arm.** Worker stop reasons were spelled in several places; there is now one owner, pinned by import identity, and an unknown reason is rejected rather than silently admitted.
- **Dual-axis engine outcome.** A lane's *work* status and its *ceremony* status are now separate fields (`work_status` / `ceremony_status`, plus `ceremony_failed`), so a lane that did the work but failed a wrap-up step no longer reads as a failed lane.
- **Strict `.draining` claim grammar and `.checkpoint` sidecar liveness.** A leading-zero pid in a claim file could reach `os.kill`; the grammar now refuses it.
- **Lane `test_cmd` convergence routes through the shared connection factory.** `_converge_stored_lane_test_cmd` no longer opens its own bare `sqlite3` connection.
- **The extracted `remote_agent` fragments are runnable standalone**, and the VM/host phase-timing envelope reports two-sided completeness bounds rather than a single optimistic number.

### Migration
- Consumers reading a lane's outcome should read `work_status` and `ceremony_status`.

## [0.2.16] — 2026-08-02

### Added
- **Lane results spool durably under `.task-state`**, so a pass result survives a harness restart instead of living only in the returned payload.

### Fixed
- **`harvest_verdict` is honest about a failed harvest.** It is now gated on `review_product_harvest`, so a review lane that produced no parseable findings block reports that rather than reading as a clean pass.
- **Commit-subject byline screen: credit escapes closed.** A long series of bypasses in the authorship-trailer screen — wrapped and type-prefixed trailer objects, hyphen-prefixed `verb-By` forms, mid-line bare-object credit, trailing adjuncts and compound modifiers — no longer slip past the scan.

## [0.2.15] — 2026-07-28

### Fixed
- **`worktree-lane` no longer lets CLI stderr corrupt the JSON it parses.** `_run_cli_json` captured the command with `2>&1`, so any diagnostic line the CLI wrote to stderr was prepended to the payload, and the validator swallowed the resulting parse error as a success.
- A missing lane manifest no longer aborts the whole daemon cycle.
- The exit-75 producer enumerator no longer counts `exit 75` occurring inside a quoted shell string, which had misread two admission-refusal `exit 2` sites as unmapped exit-75 producers.

## [0.2.14] — 2026-07-28

### Added
- **Plan decomposition and wave scheduling (implementation note).** `plan_decomposer` turns a plan into lanes; `lane_ready_set` computes the critical path over the resulting dependency graph; a flock-backed `conflict_gate` colours conflict edges so file-overlapping lanes cannot be dispatched into the same wave.
- **`self_verify_outcome` on the offload path (implementation note).** The remote script stamps a real self-verify outcome enum onto `raw_payload` and the engine consumes it, replacing inference from exit codes.

### Changed
- **The codemap freshness gate now claims only what it measured (implementation note S6–S9).** Prior wording asserted agreement it had not established.
- Adapter timeouts reject non-positive and non-integral values, and the invariant is enforced past construction rather than only at the boundary (implementation note) — a remote turn can no longer be dispatched unbounded.
- `WORKBAY_DISABLE_PYTEST_PATH_GUARD` is single-sourced into the hermetic self-verify environment instead of being re-derived per call site.

### Fixed
- The package's own type gate was dead, not passing — same `python_version` / `requires-python` skew described in the `mcp-workbay-handoff` entry.

## [0.2.13] — 2026-07-22

### Added
- **`cursor-cli` execution backend (internal).** Registers the Cursor CLI (`cursor-agent`) as a local parallel-lane dispatch target, driving the model named by `WORKBAY_CURSOR_MODEL` (default `cursor-grok-4.5-high-fast`).
- **No structured-output flag.** Unlike grok's `--json-schema` and codex's `--output-schema`, cursor has neither, so the schema rides in the prompt and the `BackendResult` is recovered by the shared prose extractor.
- **Binary-name collision.** Both the Cursor and grok CLIs install an `agent` binary with incompatible flags.
- **No turn bound.** `cursor-agent` has no `--max-turns`, so the cycle is bounded by wall-clock only, enforced by a process-group kill.

### Changed
- `offload_preflight` now resolves single-cycle bounds by the profile's declared bound **kind** (`derive_single_cycle_bounds`) rather than comparing against the `grok_derived` literal, so a backend bounded by wall-clock alone is recognised as governed instead of being refused as an ungoverned pass.
- **New capability `supports_adapter_timeout_bounds` + predicate `backend_derives_cycle_bounds`.** `supports_token_budget_cycle_bounds` was overloaded: it meant both "derive cycle bounds" and "construct with grok ctor kwargs".
- **Breaking (S1 semantics, implementation note):** `WORKBAY_OFFLOAD_SEMANTIC_REINJECTION=1` no longer loads the in-process embedding model in `lane_prompt` / `slice_brief_packet`.
- Lane-prep footprint telemetry attribution field renamed to `offload_semantic_reinjection_flag` (records the env flag, not reinjection success).
- `list_turn_metrics` default listing excludes `phase='lane_prep'` rows; pass `phase='lane_prep'` to inspect prep telemetry.

### Added
- `record_turn_metric(..., replace_prior_same_phase=True)` deletes prior same-phase rows for the lane in the same connection/transaction as the insert, giving per-lane latest-wins prep telemetry with no DELETE/INSERT race.

## [0.2.12] — 2026-07-17

### Fixed
- Remote offload transport: `stdin=DEVNULL` on the remote runner so step-1 `git push` no longer inherits the never-EOF MCP stdio pipe and hangs (0149).
- Offload pass: stop the `ARTIFACT_INDEXED` log emit double-binding `lane_id`, which crashed the whole pass (`TypeError`) when a large review `details` was compressed to an artifact.

### Changed
- Host-memory admission (internal): add a fully off-box `COST_REMOTE` cost class.

## [0.2.11] — 2026-07-16

### Added
- `grok-remote` backend (implementation note): runs each grok worker turn in a hardened, history-stripped sandbox on the remote gate VM via `scripts/remote_agent.sh`, returning the work as a patch committed locally under the engine identity.

### Changed
- Grok cycle-governance sites are capability-gated (`backend_supports_token_budget_cycle_bounds`) instead of naming `grok-cli` literally; `remote_agent.sh` knob precedence is env-over-config-file for every `WORKBAY_*` knob.

## [0.2.10] — 2026-07-14

### Fixed
- Host-memory admission: `derive_width` now applies a **per-class OS reserve** instead of subtracting the full heavy `os_reserve_gib` for every cost class.

## [0.2.9] — 2026-07-14

### Changed
- Secure grok offload: grok now runs inside a shallow, history-stripped clone of the lane worktree (no git history to bundle), fail-closed and re-verified after the worker's turn, so an offloaded grok worker cannot export the repository's full object database; commits are replayed onto the lane branch.
- Host-memory admission gained a remote-API cost class (small-local-footprint CLI-driver backends) with its own elastic width, threaded through the preflight and worker-slot gates; the policy echo now flags the `WORKBAY_HOSTGOV_DISABLE` kill switch; the standalone probe accepts the new class.
- Lane hygiene: a reaper closes lanes orphaned by task archival (present in the archive, absent from live state), and reports when a sweep is truncated by its batch cap.

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
