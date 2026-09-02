# workbay-protocol

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.2.6] — 2026-09-02

### Changed
- Regenerated `tool_serving_index`: the header now names the real regeneration command (`python3 scripts/tool_serving_index.py sync`) instead of a make target that no longer exists, and `SHARED_TOOL_NAMES` is documented as the verified intersection of the two server tool-name sets.
- Expanded the tool-serving-index tests to pin the generated surface against both server tool-name sets rather than sampling it.

### Added
- **`tool_serving_index` — generated `SLUG_MCP_TOOLS`, `HANDOFF_TOOL_NAMES`, `ORCHESTRATOR_TOOL_NAMES` (implementation note S0).**
- **`convergence` — terminal-convergence contracts.** `WorkerOutcomeV2`, `ReviewAttemptOutcomeV2`, `CandidateDisposition`, `CandidateControlState`, `CandidateEvent`, `MergeCapability`, `LaneContextPacket`, `ShipStage`, `ShipCleanupPostcondition`, `admit_candidate_transition`, `TransitionAdmission`, and `TransitionRefusedError` are the shared exact-SHA types and state-first transition function for implementation note S1a.

### Changed
- **Tool-serving index readers fail closed.** Skill manifests must be top-level mappings with a valid `mcp_tools` list, and server entries must use a direct `ToolEntry("<literal>", ...)` call.
- **Tool-serving drift diagnostics are explicit.** `check` distinguishes `CORRUPT`, `ABSENT`, and `DRIFT` generated-index states, while generated `SHARED_TOOL_NAMES` records the verified overlap between both server rosters.
- **Convergence schema/runtime parity.** `MergeCapability.consumed_at` and `LaneContextPacket.server_started_at` share the aware RFC3339 contract with review-attempt timestamps.

## [0.2.4] — 2026-08-10

### Added
- **`branch_naming` — plan-segment branch and lane identity.** A new `workbay_protocol.branch_naming` module is the single owner of the `plan<NNNN>-<id>` mint and its already-prefixed guard, so a plan-bound task yields the same branch and lane id everywhere instead of each caller re-deriving it.

### Changed
- **Handoff-state paths refuse to mint a DB outside a git checkout.** `paths` now steers handoff state away from `.workbay` and warns when a database would be minted from a non-git location, which is the shape that produced the 0-byte decoy DB.

### Removed
- **`PYPI_URL` and the doctor public-index query path.** The public index is retired as a distribution channel; the constant and the code that queried it are gone rather than left dead.

## [0.2.3] — 2026-07-22

### Changed
- Version bump for the coordinated release; no contract surface changes.

## [0.2.2] — 2026-07-11

### Changed
- Version bump for the coordinated release; no contract surface changes.

## [0.2.1] — 2026-07-04

### Changed
- Refresh the git-only contract payload metadata for the current WorkBay stack and publish the distribution contract in the 0.2.1 tag.
- internal: delivery is git-only from `darce/workbay` tags; no PyPI publish path for stack members.

## [0.2.0] — 2026-06-26

### Changed
- Coordinated 0.2.0 stack release.

## [0.1.0] — 2026-06-22

### Changed
- First release under the WorkBay name; the version line was reset to `0.1.0` for the new PyPI project (greenfield, single-maintainer).

## [0.3.0] — 2026-06-11

### Changed
- Branch/worktree naming contract embeds the implementing plan id in feature branch and worktree names; lifecycle ref resolution recognizes the plan-id suffix (internal).

## [0.2.8] — 2026-06-10

### Changed
- Build: migrate sdist build backend setuptools→hatchling with at-build privacy scrub (implementation note sdist-privacy sweep).

## [0.2.7] — 2026-06-08

### Changed
- Privacy: internal project ids scrubbed from shipped source.

## [0.2.5] — 2026-06-07

### Fixed
- Re-cut of 0.2.4, whose published wheel was corrupted by the public-export scrub: the case-insensitive inline prefix regex matched `internal` inside identifiers and renamed `BranchClassification` to `BranWORKSTATElassification` in `branch_naming`.

## [0.2.4] — 2026-06-07

### Changed
- Re-cut of the unpublished 0.2.3 with the runtime `__version__` string synced to the package version.

## [0.2.3] — 2026-06-07

### Changed
- `StructuredSummary.harness` literal gains `grok` (internal harness parity with the canonical compaction-contract harness list).

## [0.2.2] — 2026-06-06

### Added
- `BootstrapManifest` stack provenance fields (`stack_distribution`, `stack_version`, `stack_members`) and a package-source update path with `--remote-ref` optional (validated post-manifest-load).

## [0.2.1] — 2026-06-04

### Changed
- Re-cut of the unreleased 0.2.0 with the runtime `__version__` string synced to the package version.

## [0.2.0] — 2026-06-04

### Added
- **Durable consumer recipe overrides (internal):** new `plugin-override-manifest.json` and `plugin-override-lock.json` schemas plus expanded `bootstrap-manifest.json` fields covering the override root, effective plugin tree, and `global_instructions` propagation.
- `bootstrap.py` helpers for resolving and validating override manifests/locks used by `workbay-bootstrap` composition and `overrides` subcommands.

## [0.1.7] — 2026-06-01

### Added
- **`BootstrapManifest` gains a `source_kind` discriminator (internal).** New `source_kind: "git_overlay" | "package"` field (default `"git_overlay"`) plus `package_version`, with `remote_url` / `remote_ref` / `remote_sha` now optional.

### Removed
- **Legacy runtime-path symbols retired (implementation note Slice D cutover complete).** `LEGACY_RUNTIME_ROOT_DIRNAME`, `LEGACY_DOCS_MIRROR_DIR`, and `RUNTIME_PATH_RENAMES` are removed from the public surface.

## [0.1.6] — 2026-05-30

### Added
- **`workbay_protocol.paths` — single source of truth for the runtime root and docs mirror (implementation note Slice D).** New module exporting `RUNTIME_ROOT_DIRNAME` (`.workbay`), `DOCS_MIRROR_DIR` (`docs/workbay`), their `LEGACY_*` counterparts (`.agentic` / `docs/workbay`), `RUNTIME_PATH_RENAMES`, `CONTRACTS_DIR`, `RULES_DIR`, `HARNESS_CONTRACT_RELPATH`, `INSTRUCTIONS_RELPATH`, and the `docs_mirror_path()` / `runtime_root_path()` helpers.

### Notes
- Coordinated rebrand release with `mcp-workbay-handoff` 0.12.0, `mcp-workbay-orchestrator` 0.5.0, and `workbay-bootstrap` 0.6.0.

## [0.1.5] — 2026-05-10

### Changed
- **Coordinated release with `mcp-workbay-handoff` 0.11.1, `mcp-workbay-orchestrator` 0.4.5, and `workbay-bootstrap` 0.4.2** to ship internal (multi-active CURRENT_TASK projection, malformed import-payload rejection, target_branch/worktree_path/plan_path preservation) and internal (compaction env-var namespace consolidation under `AGENT_HANDOFF_COMPACTION_*`, with `internal_*` retained as a deprecated alias).

## [0.1.4] — 2026-05-08

### Added
- **Branch-grammar registry** (internal) under `workbay_protocol.branch_naming`.

## [0.1.3] — 2026-05-04

### Changed
- Documentation refresh and minor packaging maintenance to support the internal / internal / internal release wave.

## [0.1.2] — 2026-05-03

### Added
- **`workbay_protocol.branch_naming` is now a documented public module** (internal).
- `TASK_REF_RE` — canonical regex describing the conforming feature-branch grammar (`feature/<task-ref>-<slug>`, lowercase, must contain at least one digit).
- `derive_task_ref_candidates(branch_name)` — yields every digit-bearing prefix from longest to shortest (used by the "did you mean" suggestion in the post-checkout warn).
- `format_suggested_branch_name(task_ref)` — render a conforming branch name from a registered task ref.
- The README declares `branch_naming` as a ✅ v0.1.0 schema row so external consumers can pin against the published surface.
