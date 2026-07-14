# workbay

Condensed public changelog — internal references removed, one headline
per change. Auto-generated from the project's release notes.

## [0.3.19] — 2026-07-14

### Changed
- Bump member pins: `mcp-workbay-handoff` 0.2.8, `mcp-workbay-orchestrator` 0.2.9, `workbay-bootstrap` 0.3.15, `workbay-system` 0.3.13.

## [0.3.18] — 2026-07-13

### Changed
- Bump member pins: `mcp-workbay-handoff` 0.2.7, `mcp-workbay-orchestrator` 0.2.8, `workbay-bootstrap` 0.3.14, `workbay-system` 0.3.12.

## [0.3.17] — 2026-07-12

### Changed
- Re-sync stack member pins for this release: `mcp-workbay-handoff` 0.2.6, `mcp-workbay-orchestrator` 0.2.7, `workbay-bootstrap` 0.3.13, `workbay-system` 0.3.11.

## [0.3.16] — 2026-07-11

### Changed
- Sync stack member pins for the coordinated release (bootstrap, system, protocol, MCP servers).

## [0.3.15] — 2026-07-10

### Changed
- Add an embeddings CLI facade over the `embedding_provision` gate and restore the SSOT operator docs for it.
- Roll up the semantic-reinjection heuristics fixes (sticky dedupe on selected status, delivery-proof, lane cache) and re-sync pins to the current handoff/orchestrator/bootstrap/system family.

## [0.3.14] — 2026-07-08

### Changed
- Re-sync anchor pin to workbay-bootstrap 0.3.10 (implementation note floor cascade).

## [0.3.13] — 2026-07-08

### Changed
- Re-sync anchor pins to mcp-workbay-orchestrator 0.2.4 and workbay-system 0.3.8 (implementation note coordinated bump).

## [0.3.12] — 2026-07-08

### Changed
- Re-synced anchor pins to workbay-bootstrap 0.3.9 for the coordinated release (handoff/orchestrator 0.2.3, system 0.3.7, bootstrap 0.3.9).

## [0.3.11] — 2026-07-08

### Changed
- Re-synced anchor pins to the coordinated release: mcp-workbay-handoff 0.2.3, mcp-workbay-orchestrator 0.2.3, workbay-system 0.3.7.

## [0.3.10] — 2026-07-08

### Changed
- Re-synced anchor pins to workbay-bootstrap 0.3.8 and workbay-system 0.3.6.

## [0.3.9] — 2026-07-07

### Changed
- Re-anchor pins to the coordinated release: mcp-workbay-handoff 0.2.2, mcp-workbay-orchestrator 0.2.2, workbay-bootstrap 0.3.7, workbay-system 0.3.5.

## [0.3.8] — 2026-07-04

### Changed
- Re-cut the front-door anchor after synchronizing the generated stack pins to the 2026-07-04 member versions (`workbay-protocol==0.2.1`, `mcp-workbay-handoff==0.2.1`, `mcp-workbay-orchestrator==0.2.1`, `workbay-bootstrap==0.3.6`, `workbay-system==0.3.4`).

## [0.3.7] — 2026-07-04

### Changed
- Re-pin the front-door stack to `workbay-bootstrap==0.3.6` and `workbay-system==0.3.4` for the refreshed git-only release.

## [0.3.6] — 2026-07-02

### Changed
- Re-cut the anchor release from current `main` to refresh the public mirror, shipping the internal release-tooling hardening (stale-tag audit/guard/prune + RELEASING playbook).

## [0.3.5] — 2026-07-01

### Changed
- Re-pin the anchor stack to `workbay-bootstrap==0.3.5` (dogfood self-target install fix).

## [0.3.4] — 2026-07-01

### Changed
- Re-pin the anchor stack to `workbay-system==0.3.3` and `workbay-bootstrap==0.3.4` (workbay-system 0.3.3 compaction single-source release).

## [0.3.3] — 2026-06-30

### Changed
- Bump anchor stack pins to `workbay-system==0.3.2` and `workbay-bootstrap==0.3.3`, shipping the git-only clone `--no-sources` fix (internal).

## [0.3.2] — 2026-06-30

### Changed
- Bump anchor stack pins to `workbay-system==0.3.1` and `workbay-bootstrap==0.3.2`, shipping the overlay tooling parameterization (internal).
