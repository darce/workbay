# Releasing the workbay

This document describes how to cut a release. It is the operator
playbook for the maintainer cutting tags and triggering Trusted
Publishing (local `twine` upload is only the fallback). Consumers
(`uvx workbay-bootstrap install --remote-ref vX.Y.Z`) do not need to
read it.

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

Three layers, one source of truth. Most days you only touch the top one:

```bash
make help                       # list every distribution target
make preflight                  # checklist only
make release-status             # show package/tag/PyPI state + suggested next monorepo tag
make release-public             # PRIMARY publish path (dry-run): export + tag-sync + Trusted-Publishing plan
make release-public FLAGS=--execute  # ...execute it (maintainer-run — see "Trusted Publishing" below)
make smoke                      # install the latest tag into /tmp
```

**Trusted Publishing via `release-public` is the canonical publish path** (PyPI
OIDC, no local token). The `release-pending` / `release-all` / `release-package`
targets below are the local-`twine` *fallback*; see
**[Trusted Publishing (primary path)](#trusted-publishing-primary-path)**.

The package catalog and release order come from `config/release/packages.json`.
Inspect the publishable set with `python scripts/release_manifest.py list --release-only --field name`.

The `Makefile` wraps `scripts/release.sh`, which in turn implements the
manual sequence documented in this file. All three layers stay in sync
— change one, change the others.

| Layer                  | When to use                                                 |
|------------------------|-------------------------------------------------------------|
| `make <target>`        | Day-to-day driver. Tab-completes; safest.                   |
| `scripts/release.sh`   | When you need flags the Makefile doesn't expose, or in CI.  |
| Manual `git`/`twine`   | Debugging a script failure, or releasing out of band.       |

See **[Common deployment tasks](#common-deployment-tasks)** below for the
full playbook keyed by intent ("ship a patch", "back out a bad wheel",
"coordinate a multi-package release", etc.).

## Scripted vs. manual

**The script** (`scripts/release.sh`) is the default — it implements
the same checklist + per-step sequence documented below, with safety
rails (refuses dirty tree, refuses to re-publish an existing version,
refuses to upload `.dev0`/`aN`/`bN`/`rcN` without `--allow-pre`, scopes
the upload glob to the bumped version, verifies the wheel reaches PyPI
before tagging, asserts per-package tags are ancestors of the monorepo
tag).

```bash
scripts/release.sh preflight                 # run the checklist only
scripts/release.sh status                    # show package/tag/PyPI state + suggested next monorepo tag
scripts/release.sh pending [vX.Y.Z]          # release unpublished package versions + cut a monorepo tag
scripts/release.sh package workbay-protocol  # release one package at its pyproject version
scripts/release.sh all                       # release all publishable manifest packages in dep order
scripts/release.sh monorepo v0.1.0           # cut the consumer-facing tag (after package tags)
scripts/release.sh --dry-run all             # show what would happen, take no action
```

For repos where the only dirty path is the generated `DASHBOARD.txt`, add
`--auto-stash-dashboard` to temporarily stash and restore that file during the
release gate instead of clearing it by hand.

**The manual sequence** below is what the script automates. Read it
when debugging a script failure, or when releasing a single package
out of band. The three layers (Makefile, script, manual) are kept
consistent; if you change one, change the others.

## Two tag families

The monorepo carries two families of tags. Only one of them is
load-bearing for consumers:

| Tag                          | Audience               | Purpose                                                                                          |
|------------------------------|------------------------|--------------------------------------------------------------------------------------------------|
| `vX.Y.Z`                     | external consumers     | The single ref that `workbay-bootstrap install --remote-ref vX.Y.Z` pins. One bump per release.  |
| `<package-name>-vX.Y.Z`      | informational          | Marks the commit that produced a given PyPI wheel. Useful for `git log`; not consumed directly.  |

Rule: **every monorepo `vX.Y.Z` is preceded by all the per-package tags
it contains, in the same commit chain**, and tagged in dependency
order:

```text
workbay-protocol → mcp-workbay-handoff → mcp-workbay-orchestrator → workbay-bootstrap → workbay-codex-bridge → vX.Y.Z (monorepo)
```

The dependency order is not optional: each downstream package's
`pyproject.toml` may pin the upstream version that just shipped, and
`twine upload` of a downstream wheel that resolves an unreleased
upstream from PyPI will fail at install time on the consumer.

## Tooling prerequisites

The release commands below invoke `build` and `twine` via `uvx`, so
no per-environment install of those tools is required. The only
hard prerequisites on the maintainer's machine are:

- `uv` / `uvx` on `PATH` (already required for everyday consumer use).
- `git` with push access to `origin` **and `darce/workbay`** (the primary
  Trusted-Publishing path pushes the export there) and tag-create permission.
- PyPI publish credentials are **not needed** — `release.sh package` now
  triggers Trusted Publishing via `gh workflow run -R darce/workbay` rather
  than uploading locally. PyPI auth happens via GitHub OIDC inside
  `release-publish.yml`. The `gh` CLI must be installed and authenticated.
  Local-`twine` is still documented as an out-of-band fallback below.

If you previously ran releases against a venv with `pip install build
twine`, that still works — just substitute `python -m build` for
`uvx --from build pyproject-build` and `python -m twine ...` for
`uvx twine ...` in every command below.

## Pre-release checklist

Before any `twine upload` or `git push --tags`:

1. **Working tree is clean and on `main`.**
   `git status` empty; `git rev-parse --abbrev-ref HEAD` returns `main`.
2. **All publishable package test suites pass.**

   ```bash
   for pkg in $(python scripts/release_manifest.py list --release-only --field name); do
       (cd packages/$pkg && python -m pytest -q) || { echo "FAIL: $pkg"; exit 1; }
   done
   ```

3. **The cross-package contract test passes.**
   `cd packages/mcp-workbay-orchestrator && python -m pytest tests/test_protocol_contract.py -q`
4. **The bootstrap install rehearsal passes.**
   `cd packages/workbay-bootstrap && python -m pytest tests/test_bootstrap_install_rehearsal.py -q`
5. **Generated client surfaces contain the release fixes.**
   The rehearsal must show `.mcp.json`, `.vscode/mcp.json`, and
   `.codex/config.toml` registering both managed MCP servers with
   `--workspace-root . serve-stdio`. Also run
   `cd packages/workbay-system && python -m pytest tests/test_generator_round_trip.py -q`
   so `.claude/skills` and `.codex/skills` carry the current
   `$branch-review` persistence guidance.
6. **Each `pyproject.toml` version matches the tag you are about to
   cut.** Mismatch here means the wheel uploaded to PyPI will carry
   the wrong version string.
7. **No private `git+ssh://` cross-package pins remain in any
   `pyproject.toml` you are about to publish.** The orchestrator's
   former `git+ssh://...` pin was replaced by
   `mcp-workbay-handoff>=A.B.C,<A+1.0.0` in the rename commit; verify
   the line is still a PyPI version range — `pypi` rejects direct VCS
   dependencies on upload.
8. **Each package CHANGELOG.md has an entry at the new version**
   (one line per shipped change is fine; this is for `git log`
   readers, not for marketing).

## The release sequence

Publishable packages release in manifest dependency order. Each step is
independent — if a later step fails, earlier package uploads are still good
and need not be backed out.

The scripted sequence below is what `scripts/release.sh package` automates.
Run it only when debugging a script failure; otherwise use `make release-package`.

**Published-bytes dispatch contract (implementation note):** when `dry_run=false`, the
workflow rejects dispatch unless `expected_sha256_wheel`, `expected_sha256_sdist`,
and `source_date_epoch` are all set. `scripts/release.sh package` computes these
from the locally gate-validated dist (with `SOURCE_DATE_EPOCH` pinned to the tag
commit). Manual `gh workflow run … -f dry_run=false` without those inputs fails
closed — use `release.sh package` / `make release-package` for real publishes.

### 1. `workbay-protocol`

```bash
cd packages/workbay-protocol
# Confirm version in pyproject.toml matches the next tag.
rm -rf dist/
uvx --from build pyproject-build                # produces dist/*.whl + dist/*.tar.gz
uvx twine check dist/*                          # format validation only — no upload
cd ../..
git tag workbay-protocol-vX.Y.Z
git push origin workbay-protocol-vX.Y.Z       # private origin
scripts/release.sh package workbay-protocol
```

Verify the wheel is reachable: `pip index versions workbay-protocol`
must list the new version (PyPI's index can lag a few seconds).

### 2. `mcp-workbay-handoff`

Pins `workbay-protocol>=A.B.C,<A+1.0.0` from step 1. Run only after
that line resolves on PyPI.

```bash
cd packages/mcp-workbay-handoff
rm -rf dist/
uvx --from build pyproject-build
uvx twine check dist/*
cd ../..
git tag mcp-workbay-handoff-vX.Y.Z
git push origin mcp-workbay-handoff-vX.Y.Z
scripts/release.sh package mcp-workbay-handoff
```

Verify: `uvx mcp-workbay-handoff --help` from a clean venv exits 0.

### 3. `mcp-workbay-orchestrator`

```bash
cd packages/mcp-workbay-orchestrator
rm -rf dist/
uvx --from build pyproject-build
uvx twine check dist/*
cd ../..
git tag mcp-workbay-orchestrator-vX.Y.Z
git push origin mcp-workbay-orchestrator-vX.Y.Z
scripts/release.sh package mcp-workbay-orchestrator
```

Verify: `uvx mcp-workbay-orchestrator --help` from a clean venv exits 0.

If `twine check` fails with "direct VCS deps are not allowed", a
`git+ssh://` or `git+https://` line crept back in — fix before re-running.

### 4. `workbay-bootstrap`

```bash
cd packages/workbay-bootstrap
rm -rf dist/
uvx --from build pyproject-build
uvx twine check dist/*
cd ../..
git tag workbay-bootstrap-vX.Y.Z
git push origin workbay-bootstrap-vX.Y.Z
scripts/release.sh package workbay-bootstrap
```

Verify: `uvx workbay-bootstrap --help` from a clean venv exits 0.

### 5. `workbay-codex-bridge`

```bash
cd packages/workbay-codex-bridge
rm -rf dist/
uvx --from build pyproject-build
uvx twine check dist/*
cd ../..
git tag workbay-codex-bridge-vX.Y.Z
git push origin workbay-codex-bridge-vX.Y.Z
scripts/release.sh package workbay-codex-bridge
```

Verify: `uvx workbay-codex-bridge --help` from a clean venv exits 0.

### 6. The monorepo distribution tag

After every publishable package tag is pushed, cut the consumer-facing tag
on the same commit chain:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

Smoke-test against a throwaway target. Note the `mkdir -p` — bootstrap
refuses to install into a path that doesn't already exist, so the
target must be created first:

```bash
SRC="git+https://github.com/darce/workbay@vX.Y.Z#subdirectory=packages/workbay-bootstrap"
TARGET="/tmp/release-smoke-$(date +%s)"
mkdir -p "$TARGET"
uvx --from "$SRC" workbay-bootstrap install --target "$TARGET"
```

The install must succeed without manual fixes, write `.mcp.json` and
`.vscode/mcp.json` referencing both managed servers, and materialize the
plugin tree under
`.workbay/generated/plugins/workbay-system/base/{claude,codex}/skills`,
registered via `.claude-plugin/marketplace.json` (and
`.agents/plugins/marketplace.json` for Codex). Per-agent surfaces such as
`.claude/skills` are no longer written into the overlay clone directly —
they ship through the plugin marketplace, so a bare `.claude/` carrying
only `settings.json` is expected, not a failure. If any of those fail,
treat the release as bad and back out (next section).

## Common upload failures

- **`HTTPError: 400 Bad Request from https://upload.pypi.org/legacy/`
  on the *first* file in `dist/`.** Almost always means that filename
  is already on PyPI — either a previous release of the same version,
  or an old wheel from before you bumped that's still sitting in
  `dist/`. PyPI rejects re-uploads of an existing filename. Fix:
  `rm -rf dist/`, rebuild, and use the version-scoped upload glob
  (`dist/<pkg_underscored>-X.Y.Z*`) so only the wheel + sdist for the
  *new* version are uploaded. PyPI normalises the project name to
  hyphens but the file artifacts use underscores
   (`workbay_protocol-0.1.1-...`, not `workbay-protocol-0.1.1-...`).

- **`SetuptoolsDeprecationWarning: project.license as a TOML table
  is deprecated`.** Warning only; safe to ship before 2027-Feb-18.
  Track the migration to a SPDX-string license + `project.license-files`
   separately — applies uniformly to package `pyproject.toml` files.

- **`twine check` fails with "direct VCS deps are not allowed".** A
  `git+ssh://` or `git+https://` line crept back into the package
  being uploaded. Replace with a `>=A.B.C,<NEXT_MAJOR` PyPI range
  before re-running build + upload.

- **`uvx twine upload` rejects credentials silently.** Confirm
  `~/.pypirc` has a `[pypi]` section with `username = __token__` and
  `password = pypi-...`, or export `TWINE_USERNAME=__token__` and
  `TWINE_PASSWORD=pypi-...` for the shell.

## Backing out a bad release

PyPI does not allow re-uploading a yanked version with the same
number. Recovery is always **bump-and-fix**, never "re-publish the
same version".

1. **Yank the bad wheel** (does not delete it; it tells `pip` to skip
   it for new installs). PyPI exposes yank via the project page only:

   ```bash
   open "https://pypi.org/manage/project/<pkg>/release/<version>/"
   ```

   Use the "Yank" button and supply a one-line reason. Yanking is
   reversible; deletion is not, so prefer yank.
2. **Delete the bad git tag locally and on the remote**:

   ```bash
   git tag -d <pkg>-vX.Y.Z
   git push --delete origin <pkg>-vX.Y.Z
   ```

   If the bad release was the monorepo `vX.Y.Z` tag, also delete
   that. Consumers who already pulled it keep working; new
   `--remote-ref vX.Y.Z` invocations will fail until the tag is
   re-pushed at the corrected commit.
3. **Bump the patch version**, fix the issue, and run the full
   release sequence again from step 1. Do not skip steps that
   succeeded the first time — `mcp-workbay-handoff` may need a no-op
   patch bump just to repin against a freshly-yanked
   `workbay-protocol`.

If the backout discovers a contract drift caught only by the monorepo
smoke test, add a regression test in
`packages/workbay-bootstrap/tests/test_bootstrap_install_rehearsal.py`
or the relevant package's contract suite as part of the bump-and-fix
commit.

## Common deployment tasks

A flat playbook keyed by intent. Every task assumes a clean working
tree on `main` synced with `origin/main` (the script enforces this; the
Makefile delegates to it).

### Task: confirm what is currently shipped

```bash
make versions   # local pyproject versions, package by package
make tags       # release tags on origin (per-package + monorepo vX.Y.Z)
make release-status
```

The output tells you (a) what each package's `pyproject.toml` claims
and (b) what's been pushed. A version listed under `make versions`
that has no matching `<pkg>-vX.Y.Z` under `make tags` is unreleased.
`make release-status` folds in the PyPI probe and prints the suggested next
consumer-facing monorepo tag.

### Task: pre-flight before any release

```bash
make preflight
```

Runs the full pre-release checklist (clean tree, every package's
pytest suite, the cross-package contract test, the bootstrap install
rehearsal, and a scan for direct-VCS deps in any `pyproject.toml`).
Fast-path with `make preflight FLAGS=--skip-tests` when you've just
run `make test` and only need the contract + rehearsal + tree checks.

### Task: ship a patch to a single package

The packages release in dependency order, but a true single-package
patch (no upstream change in the same release cycle) only needs that
one package re-published.

```bash
# 1. Bump packages/<pkg>/pyproject.toml version + add CHANGELOG entry.
$EDITOR packages/<pkg>/pyproject.toml packages/<pkg>/CHANGELOG.md

# 2. Commit and push to main.
git commit -am "<pkg>: <one-line summary>"
git push origin main

# 3. Release just this package.
make release-package PKG=<pkg>

# 4. If consumers pin <pkg> via the monorepo tag (i.e. the bootstrap
#    or the SHARED surfaces moved), cut a new monorepo tag too:
make release-monorepo TAG=v<X.Y.Z>

# 5. Smoke-test from a fresh shell against the new monorepo tag.
make smoke
```

Skip step 4 when the patched package is purely a library consumed via
PyPI (e.g. `workbay-protocol`) and no consumer-visible surface in the
overlay clone changed. The per-package tag (`<pkg>-v<X.Y.Z>`) is
sufficient in that case.

### Task: ship a coordinated multi-package release

When upstream changes ripple downstream (e.g. `workbay-protocol` adds
a field, `mcp-workbay-handoff` adopts it, orchestrator adopts the new
handoff version, bootstrap pins the new orchestrator):

```bash
# 1. Bump every affected pyproject.toml + CHANGELOG.
#    Bump downstream pin ranges to match the new upstream lower bound.
#    Leave the upper bound at the next major (e.g. <0.6.0).

# 2. Single commit per package, in dependency order — keeps git log
#    parseable and lets you stop midway without orphan tags.

# 3. Push to main, then:
make release-all                  # releases publishable manifest packages in dep order, halts on first failure
make release-monorepo TAG=v<X.Y.Z>
make smoke
```

If `release-all` halts after an earlier package succeeds, that package is
published and tagged. Fix the issue, push the fix, and **re-run** `release-all` —
the script's `ensure_no_published_version` guard skips packages whose
pyproject version is already on PyPI, so it resumes from the failure
point rather than re-uploading.

### Task: ship only the versions that are still pending

When a release cycle stalls halfway through, or when only a subset of the
package versions in `pyproject.toml` have not been published yet, use the
pending-release path instead of manually deciding which package to run next.

```bash
make release-status
make release-pending                    # auto-chooses the next vX.Y.Z tag

# If the only dirty file is the generated dashboard:
make release-pending FLAGS=--auto-stash-dashboard

# If you want to override the suggested monorepo tag:
make release-pending TAG=v<X.Y.Z>
```

The script releases only packages whose current `pyproject.toml` version has
no matching `<pkg>-vX.Y.Z` tag and is not already on PyPI, then cuts the
consumer-facing monorepo tag (default: next patch tag after the latest `v*`).
It refuses the unsafe case where a version appears on PyPI but no matching
package tag exists.

### Task: preview a release without taking action

```bash
make dry-run-all                      # publishable package + tag commands, no uploads
make dry-run-monorepo TAG=v<X.Y.Z>    # monorepo tag flow only
```

Dry-run prints every command it would execute, prefixed `[dry-run]`.
The preflight gate (clean tree, tests, contract, rehearsal) still runs
under dry-run — that's a feature, not a bug; you want to know whether
the *real* run would clear preflight before you commit to it.

### Task: ship a pre-release (.devN, aN, bN, rcN)

The script refuses pre-release versions by default to prevent slipping
a `.dev0` to PyPI by accident. Opt in explicitly:

```bash
make release-package PKG=<pkg> FLAGS=--allow-pre
```

Test installers must opt into pre-releases too (`pip install --pre <pkg>`,
`uv add --prerelease=allow <pkg>`); a stable consumer tag will not
resolve a pre-release. Cut a monorepo tag for a pre-release only when
you actually want consumers using `--remote-ref` to receive it.

### Task: smoke-test the latest tag end-to-end

```bash
make smoke
```

Picks the highest `v*` tag on `origin`, mkdirs `/tmp/agentic-smoke-...`,
`git init`s it, and runs the full `uvx --from "git+ssh://...@<tag>..."`
install. The install must:

- exit 0,
- write `.mcp.json`, `.vscode/mcp.json`, `.codex/config.toml` referencing
  both managed servers,
- materialize the plugin tree under
  `.workbay/generated/plugins/workbay-system/base/{claude,codex}/`
  (skills + commands) and register it via `.claude-plugin/marketplace.json`
  and `.agents/plugins/marketplace.json`. The per-agent surfaces
  (`.claude/skills`, `.claude/commands`, `.github/prompts`, `.codex/skills`)
  are no longer populated directly in the overlay clone — they are delivered
  through the plugin marketplace, so their absence from `.claude/` is
  expected,
- set `core.hooksPath` to `scripts/hooks` and resolve to a populated
  hooks directory.

If any of those fail, treat the release as broken and follow
**[Backing out a bad release](#backing-out-a-bad-release)** above.

### Task: clean stale build artifacts

```bash
make clean    # removes packages/*/dist/
```

Always safe; the build tooling regenerates `dist/` on the next release.
Useful when a previous failed build left artifacts that would trip
`twine`'s version-scoped upload glob.

### Task: back out a bad release

The compact form of the [back-out section](#backing-out-a-bad-release)
above:

```bash
# 1. (optional) Yank the bad PyPI wheel via the project page UI.
open "https://pypi.org/manage/project/<pkg>/release/<X.Y.Z>/"

# 2. Delete the bad git tag locally and on origin.
git tag -d <pkg>-v<X.Y.Z>
git push --delete origin <pkg>-v<X.Y.Z>
# Drop the monorepo tag too if the bad release reached the consumer surface:
git tag -d v<X.Y.Z> && git push --delete origin v<X.Y.Z>

# 3. Bump-and-fix.
$EDITOR packages/<pkg>/pyproject.toml packages/<pkg>/CHANGELOG.md
git commit -am "<pkg>: fix <issue> (bumps to <X.Y.Z+1>)"
git push origin main
make release-package PKG=<pkg>
make release-monorepo TAG=v<X'.Y'.Z'>   # next monorepo number
make smoke
```

PyPI never permits re-uploading a yanked or deleted version under the
same number, so the answer to *every* "release went out broken" is
**bump-and-fix**, never "redo with the same number".

### Task: rotate the gh CLI auth token

The primary publish path uses `gh workflow run -R darce/workbay`, so it
needs a `gh` session with `workflow` scope on `darce/workbay`:

```bash
gh auth status                            # confirm you are authenticated
gh auth refresh -h github.com -s workflow # add the workflow scope if missing
```

The local PyPI token in `~/.pypirc` is no longer required for normal releases.
Keep it only if you still use the out-of-band twine fallback (see below).

### Task: rehearse the release flow without touching origin or PyPI

Useful before a high-stakes coordinated release, or when training a new
maintainer:

```bash
git checkout -b throwaway/release-rehearsal
# Bump pyproject + CHANGELOG to a fake version on this branch.
make dry-run-all
make dry-run-monorepo TAG=v99.99.99
git checkout main && git branch -D throwaway/release-rehearsal
```

Dry-run never pushes tags, never uploads, and never mutates remote
state — but exercises every preflight check and every code path the
real run would hit.

## Who runs the release

While the monorepo is private, only the maintainer with `darce/workbay`
push access and a `gh` session with `workflow` scope runs releases.
CI does not auto-publish — `.github/workflows/test.yml` is a gate, not a
release driver.

When the repo flips public, the same playbook applies; consumers no longer
need `darce/*` SSH access.

## Trusted Publishing (primary path)

PyPI Trusted Publishers are now configured for every package, bound to
publisher `darce/workbay` / workflow `release-publish.yml` / environment
`pypi`. **This is the canonical publish path** — it uses GitHub OIDC, so **no
local PyPI token is required** and no secret ever leaves the maintainer's
machine. Local `twine` (below) is now the *fallback*.

### How to publish (Trusted Publishing)

```bash
make release-public                 # dry-run: preflight + export + tag-sync plan + publisher check
make release-public FLAGS=--execute # export -> push to darce/workbay -> tag-sync -> Trusted Publishing
```

`release-public` exports the operational subset, force-pushes it to
`git@github.com:darce/workbay.git`, and force-syncs the per-package tag family
plus the consumer `vX.Y.Z` tag. **It does not upload to PyPI itself** (its
docstring: *"PyPI upload is intentionally out of scope"*). The actual upload is a
second, explicit step: `release-publish.yml` on the public repo is
**`workflow_dispatch`-only** (one package per run; real publishes require
`package`, `dry_run=false`, `expected_sha256_wheel`, `expected_sha256_sdist`, and
`source_date_epoch`), so after the tags land run `release.sh package` per
`pending_upload` package:

```bash
scripts/release.sh package workbay-system
scripts/release.sh package workbay-bootstrap
```

Its publish job — the only place with `id-token: write` — computes
`scripts/release.sh plan --json`, uploads it as an artifact, and publishes only
when the package's version is absent from PyPI — i.e. state `pending_upload`
**or** `remote_tag_without_pypi` (the latter is the normal state in the tag-first
`release-public` flow, where the package tag is force-synced before CI runs);
`released` and other states fail-closed. Upload is via OIDC, gated by the GitHub
`pypi` environment approval if configured. No local token involved.

### Preflight publisher check is fail-closed — use `--publishers-verified`

`release-public`'s Trusted-Publisher preflight probes
`https://pypi.org/pypi/<pkg>/json`, but **PyPI's public JSON API never exposes
Trusted-Publisher metadata**, so the live probe *always* reports every package
as `missing` and aborts under `--execute` — even when the publishers are
correctly configured. This is intentional fail-closed behavior, not a config
error. Confirm the publishers exist on each project's *Manage → Publishing* page,
then pass the operator-confirm escape hatch:

```bash
make release-public FLAGS="--execute --publishers-verified"
```

`--publishers-verified` skips the unreliable probe (release-public performs no
upload anyway — the dispatch step above does).

> **Agent/permission note.** An AI coding agent **cannot** run
> `release-public --execute`: the harness auto-mode classifier blocks the
> private-monorepo → public-repo push as a data-exfiltration class action, and
> user intent alone does not clear it. The maintainer runs the publish step
> directly, or adds an explicit `Bash` permission rule for it in
> `.claude/settings.json`. Everything *up to* the push (bump, CHANGELOG, build,
> `twine check`, local-wheel dogfood, committing + pushing `main`) is agent-safe.

### Fallback: local `twine` upload

Use this only when `gh workflow run` is unavailable (e.g. `darce/workbay`
is unreachable, or the Trusted Publisher is misconfigured):

```bash
uvx --with keyring twine upload packages/<pkg>/dist/<prefix>-X.Y.Z*
```

Requires a PyPI token reachable non-interactively: OS keyring
(`keyring set https://upload.pypi.org/legacy/ __token__`), `~/.pypirc`
(`[pypi]` `username = __token__`, `password = pypi-…`), or
`TWINE_USERNAME=__token__` + `TWINE_PASSWORD=pypi-…`.
`make release-package` **no longer calls twine upload** — it uses
`gh workflow run` instead; the above is a manual out-of-band escape hatch only.
