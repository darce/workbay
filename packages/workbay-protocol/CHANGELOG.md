# workbay-protocol

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

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
