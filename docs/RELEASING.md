# Releasing the workbay

This document describes how to cut a **git-only** release. It is the
operator playbook for exporting the public subset to `darce/workbay`,
syncing the consumer tags, and verifying the smoke install. There is
**one delivery channel: the public git mirror.** A release is an export
+ push + tag-sync; nothing is uploaded to a package index. Consumers
install with `workbay-bootstrap install --remote-ref vX.Y.Z` from
[`darce/workbay`](https://github.com/darce/workbay); they do not read
this doc.

## Repository identity policy (canonical)

implementation note §0.5 D2 — the canonical statement of which repo is which:

- **The private implementation monorepo** keeps its historical name; the git
  remote is **not** renamed (implementation note D2). It holds the full development
  history (plans, assessments, reviews) and is never the surface consumers
  install from.
- **`darce/workbay` (public)** is the **sole consumer-facing identity**. It
  is produced by the `export-public` filter (fresh history, operational subset
  only) and carries the branded package tags plus the consumer-facing
  `vX.Y.Z` monorepo tags. Every consumer install command points here
  (`git+https://github.com/darce/workbay@vX.Y.Z`).

No release or bootstrap doc should reference the private repo as a consumer
install source. When in doubt, consumers use `darce/workbay`; the private
monorepo is an implementation detail.

## Quickstart

Two layers, one source of truth. Most days you only touch the top one:

```bash
make help                            # list every distribution target
make preflight                       # pre-release checklist only
make release-status                  # show package/tag state + suggested next consumer tag
make release-public                  # PRIMARY path (dry-run): export + push + tag-sync plan
make release-public FLAGS=--execute  # export -> push darce/workbay -> force-sync tags
make smoke                           # git-install the latest consumer tag into /tmp
```

**`make release-public` is the only supported release path** (git mirror
only). Pipeline: `export` → `push` → `tag-sync` → `status` (see
`scripts/release_public.py`). The legacy `release-pending` /
`release-all` / `release-package` Make targets are **retired**: they
still shell out to the publish runway that Dist-1 removed, so they no
longer do anything useful — do not run them. Use `release-public`.

The git-mirror package catalog comes from `config/release/packages.json`.
Inspect it with
`python scripts/release_manifest.py list --release-only --field name`.

| Layer                       | When to use                                                |
|-----------------------------|------------------------------------------------------------|
| `make <target>`             | Day-to-day driver. Tab-completes; safest.                  |
| `scripts/release_public.py` | When you need flags the Makefile doesn't expose, or in CI. |
| Manual `git` tag/push       | Debugging a script failure, or re-syncing a tag by hand.   |

See **[Common deployment tasks](#common-deployment-tasks)** below for the
playbook keyed by intent ("ship a patch", "back out a bad release",
"coordinate a multi-package release").

## Two tag families

The monorepo carries two families of tags. Only one of them is
load-bearing for consumers:

| Tag                          | Audience               | Purpose                                                                                          |
|------------------------------|------------------------|--------------------------------------------------------------------------------------------------|
| `vX.Y.Z`                     | external consumers     | The single ref that `workbay-bootstrap install --remote-ref vX.Y.Z` pins. One bump per release.  |
| `<package-name>-vX.Y.Z`      | informational          | Marks the commit that produced a given package version. Useful for `git log`; not consumed directly. |

Rule: **every monorepo `vX.Y.Z` is preceded by all the per-package tags
it contains, in the same commit chain**, in dependency order:

```text
workbay-protocol → mcp-workbay-handoff → mcp-workbay-orchestrator → workbay-bootstrap → workbay-codex-bridge → vX.Y.Z (monorepo)
```

The dependency order still matters: each downstream package's
`pyproject.toml` pins an upstream version *range*, and the export ships
the whole family on a single fresh-history commit so every pin resolves
against the same mirror snapshot. `release-public` emits all the
`<pkg>-vX.Y.Z` tags plus the monorepo `vX.Y.Z` tag on that one commit
(see the release flow below).

## The release flow: export → push → tag-sync → status

Dist-1 made the public git mirror the **only** consumer channel. A
release is four steps, orchestrated by `make release-public`
(`scripts/release_public.py`):

1. **Export** the operational subset to a throwaway fresh-history tree
   (`scripts/export_public.py --out <tmp> --force`) — a single scrubbed
   commit, with plans/assessments/reviews stripped.
2. **Push** that commit force to the public `main` on
   `git@github.com:darce/workbay.git`. The export rewrites history every
   run, so a force replacement is the intended, convergent behavior.
3. **Tag-sync** the per-package `<pkg>-vX.Y.Z` family plus the
   consumer-facing `vX.Y.Z` monorepo tag onto the exported commit, and
   force-push them.
4. **Status** — read-only; confirm the tags and print the suggested next
   consumer bump.

```bash
make release-public                  # dry-run plan (no network mutation)
make release-public FLAGS=--execute  # maintainer-run: export + push + tag-sync
```

`release-public` is **dry-run by default**; it mutates remote state only
under `--execute`, and even then it prompts for confirmation — type
`release` to proceed (operator automation can pass `--assume-yes`). The
two mutating steps are `push` and `tag-sync`; `export` and `status` never
touch the remote. Pass `FLAGS=--json` for a machine-readable plan/report.

Consumers then install from the synced tag:

```bash
uv tool install "git+https://github.com/darce/workbay@vX.Y.Z#subdirectory=packages/workbay-bootstrap"
workbay-bootstrap install --target <repo> --remote-ref vX.Y.Z
```

There is no package-index upload at any step — the git tags on
`darce/workbay` are the entire release artifact.

### Stale-higher tags (audit, guard, prune)

`sort -V | tail -1` semver-latest resolution is only correct while no
package family carries a tag *above* its current line. Pre-rebrand tags
(published above the rebrand baseline) poison that resolution, so the
pipeline defends it in three places:

- **Audit (read-only):** `python scripts/release_public.py audit-tags`
  lists every tag whose version exceeds its family's declared/current-line
  version *and* whose commit predates the rebrand baseline, and exits
  nonzero when any exist. It resolves the baseline commit and enumerates
  local tags, so it needs a full-history checkout (`fetch-depth: 0`), never
  a shallow clone. It is wired into the `workbay-system` CI job as an
  **advisory, non-blocking** step (`continue-on-error`): it surfaces
  offenders without failing the build, because the only remediation is the
  manual operator-gated prune below.
- **Guard (automatic):** `release-public --execute` aborts before the tag
  push if the mirror already carries a higher tag than the version being
  released for **active release families + the monorepo `v*` line + retired
  families** (e.g. `workbay-stack`, seeded with the `removed` sentinel so any
  surviving mirror tag outranks it) — a version-only comparison against the
  mirror's tag names — unless you pass `--allow-higher-existing-tags` (the
  override is logged).
- **Prune (operator-gated):** `python scripts/prune_stale_tags.py` is
  **dry-run by default** — it prints the exact `git push <remote> --delete`
  and `git tag -d` commands for the audited offenders. `--execute` runs
  them (after a typed confirmation; `--assume-yes` skips it). Deleting mirror
  tags is destructive and public, so it is never wired to an auto-running
  target; the maintainer runs `--execute` by hand. It enumerates **local**
  tags with the same date-gated detector as `audit-tags`, whereas the
  `release-public` guard reads the **mirror** version-only; if they ever
  diverge (a mirror-only or post-baseline higher tag), prune that tag by hand.

> **Consumer note:** deleting tags is convergent, but a consumer that
> hand-runs `git fetch` keeps a dangling local ref until it re-fetches
> with `--prune`. Overlay installs already fetch `--tags --prune --force`
> (`workbay_bootstrap/install_plan.py`), so they self-heal on the next run;
> a hand-run fetch should add `--prune`.

### Agent / permission note

An AI coding agent **cannot** run `release-public --execute`: the harness
blocks the private-monorepo → public-repo push as a data-exfiltration class
action. The maintainer runs the push step directly. Everything up to the push
(bump, CHANGELOG, local tests, committing + pushing `main`) is agent-safe.

## Pre-release checklist

Before `make release-public FLAGS=--execute` (or run `make preflight`,
which automates the gate-able subset):

1. **Working tree is clean and on `main`.**
   `git status` empty; `git rev-parse --abbrev-ref HEAD` returns `main`.
2. **Every git-mirror package's test suite passes.**

   ```bash
   for pkg in $(python scripts/release_manifest.py list --release-only --field name); do
       (cd packages/$pkg && python -m pytest -q) || { echo "FAIL: $pkg"; exit 1; }
   done
   ```

3. **The cross-package contract test passes.**
   `make test-contract` (or
   `cd packages/mcp-workbay-orchestrator && python -m pytest tests/test_protocol_contract.py -q`).
4. **The bootstrap install rehearsal passes.**
   `cd packages/workbay-bootstrap && python -m pytest tests/test_bootstrap_install_rehearsal.py -q`
5. **Generated client surfaces carry the release fixes.**
   The rehearsal must show `.mcp.json`, `.vscode/mcp.json`, and
   `.codex/config.toml` registering both managed MCP servers via the
   harness-agnostic `mcp_launch.py` stdio shim
   (`python3 scripts/hooks/mcp_launch.py <server-id>`). Also run
   `cd packages/workbay-system && python -m pytest tests/test_generator_round_trip.py -q`
   so `.claude/skills` and `.codex/skills` carry the current
   `$branch-review` persistence guidance.
6. **Each `pyproject.toml` version matches the tag you are about to cut.**
   A mismatch means the tag family points at the wrong version string.
7. **No private `git+ssh://` cross-package pins remain in any
   `pyproject.toml`.** Public consumers cannot resolve the private remote
   and the pin would leak it; downstream packages must pin upstream via a
   version *range* (e.g. `mcp-workbay-handoff>=A.B.C,<A+1.0.0`).
8. **Each package CHANGELOG.md has an entry at the new version**
   (one line per shipped change; this is for `git log` readers).

## Smoke test

```bash
make smoke
```

Picks the highest `v*` tag, creates `/tmp/workbay-smoke-…`, `git init`s
it, and runs the full
`uvx --from "git+https://github.com/darce/workbay@<tag>#subdirectory=packages/workbay-bootstrap" workbay-bootstrap install --target <dir> --remote-ref <tag>`.
The install must:

- exit 0,
- write `.mcp.json`, `.vscode/mcp.json`, `.codex/config.toml` referencing
  both managed servers,
- materialize the plugin tree under
  `.workbay/generated/plugins/workbay-system/base/{claude,codex}/`
  (skills + commands) and register it via `.claude-plugin/marketplace.json`
  and `.agents/plugins/marketplace.json`. The per-agent surfaces
  (`.claude/skills`, `.claude/commands`, `.github/prompts`, `.codex/skills`)
  are no longer populated directly in the overlay clone — they are
  delivered through the plugin marketplace, so their absence from
  `.claude/` is expected,
- set `core.hooksPath` to `scripts/hooks` and resolve to a populated
  hooks directory.

If any of those fail, treat the release as broken and follow
[Backing out a bad release](#backing-out-a-bad-release).

## Backing out a bad release

The only artifacts a release produces are git tags on `darce/workbay`,
so backing out is purely a tag operation — there is no index to yank.
Recovery is **bump-and-fix**: don't quietly re-point a published tag at
different bytes.

1. **Delete the bad tag locally and on the public mirror.** If a single
   package shipped wrong, drop its `<pkg>-vX.Y.Z` tag; if the consumer
   surface is affected, also drop the monorepo `vX.Y.Z`:

   ```bash
   # local
   git tag -d <pkg>-vX.Y.Z vX.Y.Z
   # public mirror
   git push --delete git@github.com:darce/workbay.git <pkg>-vX.Y.Z vX.Y.Z
   ```

   Consumers who already pinned `--remote-ref vX.Y.Z` keep working off
   their local checkout; new installs of that ref fail until it is
   re-pushed at the corrected commit.
2. **Bump the patch version**, fix the issue, add a CHANGELOG entry, and
   commit to `main`.
3. **Re-run the release flow** (`make release-public FLAGS=--execute`) to
   re-export, re-push, and re-sync the tag family at the corrected commit.
   Because the export rewrites history and `release-public` force-syncs
   tags, the corrected `vX.Y.Z` lands cleanly.
4. **Smoke-test** the re-cut tag (`make smoke`).

A same-version re-push at a corrected commit is technically possible
(tag-sync force-pushes), but prefer bump-and-fix so consumers always see
a monotonic version rather than a moved tag.

If the backout discovers a contract drift caught only by the smoke test,
add a regression test in
`packages/workbay-bootstrap/tests/test_bootstrap_install_rehearsal.py`
or the relevant package's contract suite as part of the bump-and-fix
commit.

## Common deployment tasks

A flat playbook keyed by intent. Every task assumes a clean working tree
on `main` synced with `origin/main`.

### Task: confirm what is currently shipped

```bash
make versions   # local pyproject versions, package by package
make tags       # release tags on origin (per-package + monorepo vX.Y.Z)
make release-status
```

`make versions` shows what each package's `pyproject.toml` claims;
`make tags` shows what has been pushed. A version under `make versions`
with no matching `<pkg>-vX.Y.Z` under `make tags` is unreleased.
`make release-status` prints the suggested next consumer-facing monorepo
tag.

### Task: pre-flight before any release

```bash
make preflight
```

Runs the gate-able subset of the
[pre-release checklist](#pre-release-checklist) (clean tree, package test
suites, the cross-package contract test, and the bootstrap install
rehearsal). Fast-path with `make preflight FLAGS=--skip-tests` when you've
just run `make test` and only need the contract + rehearsal + tree checks.

### Task: ship a patch to a single package

There is one release action regardless of how many packages changed —
`release-public` always re-exports the whole tree and syncs the full tag
family on one commit. For a single-package patch:

```bash
# 1. Bump packages/<pkg>/pyproject.toml version + add a CHANGELOG entry.
$EDITOR packages/<pkg>/pyproject.toml packages/<pkg>/CHANGELOG.md

# 2. Commit and push to main.
git commit -am "<pkg>: <one-line summary>"
git push origin main

# 3. Re-export, push the mirror, and sync the tag family.
make release-public FLAGS=--execute

# 4. Smoke-test from a fresh shell against the new monorepo tag.
make smoke
```

### Task: ship a coordinated multi-package release

When upstream changes ripple downstream (e.g. `workbay-protocol` adds a
field, `mcp-workbay-handoff` adopts it, the orchestrator adopts the new
handoff version, bootstrap pins the new orchestrator):

```bash
# 1. Bump every affected pyproject.toml + CHANGELOG. Raise each downstream
#    pin's lower bound to the new upstream version; leave the upper bound
#    at the next major (e.g. <0.6.0).
# 2. One commit per package, in dependency order, then push main.
# 3. Release the whole family atomically:
make release-public FLAGS=--execute
make smoke
```

Because the export ships every package on a single fresh-history commit
and tag-sync emits the entire `<pkg>-vX.Y.Z` family plus the monorepo
`vX.Y.Z` together, there is no half-released state to resume from — the
release is all-or-nothing per `--execute` run.

### Task: preview a release without taking action

```bash
make release-public                 # dry-run plan (the default)
make release-public FLAGS=--json    # same plan, machine-readable
```

Dry-run prints the ordered pipeline (`export`/`push`/`tag-sync`/`status`),
flags which steps mutate the remote, and lists the git-mirror packages and
the suggested monorepo tag — without touching the network. It is also the
rehearsal path before a high-stakes coordinated release: it exercises the
plan and gate without pushing or tagging.

### Task: smoke-test the latest tag end-to-end

`make smoke` — see [Smoke test](#smoke-test) for what the install must
produce.

### Task: back out a bad release

See [Backing out a bad release](#backing-out-a-bad-release). In short:
delete the bad tag on `darce/workbay`, bump-and-fix, and re-run
`make release-public FLAGS=--execute`.

## Who runs the release

While the monorepo is private, only the maintainer with **push access to
`darce/workbay`** runs releases. CI does not auto-release —
`.github/workflows/test.yml` is a gate, not a release driver; there is no
publish workflow.

When the repo flips public, the same playbook applies; consumers install
from the public tags and need no `darce/*` access.
