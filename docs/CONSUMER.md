# Consuming the workbay

This is the entry point for target repos that want the workbay-system
overlay (skills, hooks, MCP-server configs) installed into their tree.
Delivery is **GitHub-only**: consumers install from tagged refs on
[`darce/workbay`](https://github.com/darce/workbay) — no PyPI.

## Quick start (primary path)

In Claude Code, register the public plugin marketplace once per checkout:

```text
/plugin marketplace add darce/workbay
```

Install the bootstrap CLI from a consumer tag, then hoist the overlay:

```bash
REF=v0.1.35
R="git+https://github.com/darce/workbay.git@$REF"
# --no-sources is required (each member pyproject carries a workspace source
# uv rejects on a #subdirectory= install); the closure deps are git-sourced
# via --with so nothing resolves from PyPI.
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay-bootstrap" \
  workbay-bootstrap
workbay-bootstrap install --target /path/to/your/repo --remote-ref "$REF"
```

(`--remote-url` defaults to `git@github.com:darce/workbay.git`; HTTPS
works too. `--source git_overlay` is the default — the installer clones
the monorepo into `<target>/.workbay/remote/` at the pinned ref.)

Optional front-door wrapper (same git-sourced closure, delegates to
`workbay-bootstrap`):

```bash
REF=v0.1.35
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/mcp-workbay-handoff" \
  --with "$R#subdirectory=packages/mcp-workbay-orchestrator" \
  --with "$R#subdirectory=packages/workbay-bootstrap" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay" \
  workbay
workbay install --target /path/to/your/repo --remote-ref "$REF"
```

For repeated `status` / `doctor` runs, keep the tool on `$PATH`:

```bash
workbay-bootstrap status --target /path/to/your/repo
workbay-bootstrap doctor --target /path/to/your/repo
uv tool upgrade workbay-bootstrap   # pull a newer git-sourced tool closure
```

> If `uv tool install` prints `Failed to hardlink files; falling back
> to full copy`, your `uv` cache and tool dir are on different
> filesystems. The install still succeeds — silence the warning with
> `export UV_LINK_MODE=copy` in your shell profile.

That install:

- clones `darce/workbay` at `--remote-ref` into `<target>/.workbay/remote/`,
- symlinks or carves the SHARED surfaces (`scripts/hooks`, `.github/hooks`,
  `docs/workbay/contracts`, `docs/workbay/rules`, `Makefile.d`, and
  `scripts/workbay`) into the target,
- runs the workflow generator to populate the Copilot prompt surface
  (`.github/prompts`) and the Claude/Codex/Cursor/grok plugin trees under
  `.workbay/generated/plugins/workbay-system/`,
- runs a one-time `uv tool install` for the git-sourced MCP server closure
  (handoff + orchestrator) at install time,
- writes `.mcp.json`, `.vscode/mcp.json`, and `.codex/config.toml`
  registering both managed MCP servers through `scripts/hooks/mcp_launch.py`,
- runs the handoff server's `init-state` to provision
  `<target>/.task-state/` with `handoff.db` and `exports/` (skipped under
  `--no-mcp-servers`),
- sets `core.hooksPath` so harness hooks fire only after `init-state`
  succeeds,
- writes the install ledger at `<target>/.workbay-bootstrap.json`
  (legacy `.workbay-overlay.json` is auto-migrated on upgrade).

No hand-edits required.

### State-ready install contract

After `workbay-bootstrap install`, the cold-start workflow `register
task -> switch_task -> first record_event` completes from any branch
without `BranchMismatchError`. The handoff `switch_task` operation no
longer enforces branch parity (it is the operation that *resolves* a
branch-mismatch pointer), but content writes (`record_event`,
`close_slice`, `set_handoff_state`, `record_review_finding`,
`record_verified_test`, etc.) keep their branch-isolation checks. The
context-drift warning still surfaces in the `switch_task` response
envelope.

`workbay-bootstrap status` reports the resolved `state_dir` /
`db_path` / `exports_dir` / `schema_version` after a managed install
(via `init-state --check`), so you can confirm the state contract was
satisfied without booting a server. `workbay-bootstrap doctor` flags
a missing `.task-state/handoff.db` as `state_drift` *only* when the
install registered `.mcp.json`; `--no-mcp-servers` installs suppress
that check so config-only installs do not look broken.

`.task-state/` is gitignored (see [`.gitignore` policy](#gitignore-policy-for-bootstrap-managed-surfaces)
below). Each fresh checkout regenerates the DB through bootstrap; this
is the same code path human developers run.

## Semantic embeddings (consent + toggle)

Interactive `workbay-bootstrap install` and `repair` ask **Enable semantic embeddings? [Y/n]**
(default **yes**). A **no** answer (or `--no-embeddings`) skips model provisioning and
persists the off gate in `.workbay/embedding.env`. Non-interactive installs default to
**enabled** (opt-out via `--no-embeddings`).

The SSOT toggle is the CLI verb (not hand-edited harness settings). Run from the worktree root:

```bash
workbay embeddings --status
workbay embeddings --enable
workbay embeddings --disable
```

`workbay-bootstrap embeddings --target <path> …` remains a **deprecated** alias.

Each harness also exposes a generated **`/workbay`** portable command over the same gate:
Claude Code uses an `AskUserQuestion` menu; Codex, Cursor, and Grok use
`/workbay embeddings <on|off|status>`, which delegates to `workbay embeddings`.


## MCP-server registration

Default behavior (omitting `--mcp-servers`, or passing the literal
`--mcp-servers default`) registers the two MCP servers shipped by this
monorepo:

| Server                     | Launch form |
| -------------------------- | ----------- |
| `workbay-handoff-mcp`      | `python3 scripts/hooks/mcp_launch.py workbay-handoff-mcp` |
| `workbay-orchestrator-mcp` | `python3 scripts/hooks/mcp_launch.py workbay-orchestrator-mcp` |

An **optional** third managed server (`codebase-graph-mcp`) launches a
prebuilt `codebase-memory-mcp` binary from `PATH`. It is excluded by
default so consumers without the binary never inherit a dead entry. Opt
in at install:

```bash
workbay install --target . --with-codebase-graph
```

Re-materialize managed surfaces later with the same flag:

```bash
workbay mcp-sync --target . --with-codebase-graph --apply
```

The shim resolves workspace `.venv` consoles, then `uv tool` binaries
installed during bootstrap setup, and fails loud if neither exists (no
per-session PyPI/`uvx` resolve on serve). Provisioning commands such as
`init-state` may fall back to `uv run --no-sync` against the cloned
overlay when in-tree packages are present.

The canonical pin source is
`packages/workbay-system/workbay_system/payload/config/agent-workflows/mcp_servers.yaml`;
the installer's `DEFAULT_MCP_SERVERS` constant is generated from it via
`make mcp-pins-sync`. `make check-mcp-pins` fails when the generated
copy or this table drifts from the manifest.

Override with a JSON file when you need a non-default mapping:

```bash
workbay-bootstrap install --target . --mcp-servers ./my-mcp.json
```

The file accepts either `{"mcpServers": {...}}` or a flat mapping.

Opt out entirely with `--no-mcp-servers` (the install still writes
SHARED surfaces, generated prompts/plugin trees, lifecycle hoists, and
`core.hooksPath`):

```bash
workbay-bootstrap install --target . --no-mcp-servers
```

## Offloading a slice to a junior backend (`/offload`)

`/offload` hands one self-contained implementation slice to a bounded
grok Composer-2.5 lane, then presents the result behind a review gate, so
an expensive orchestrator model does not spend tokens on work a junior
backend can do. It ships with the orchestrator server (no extra install)
and adds two MCP tools, `offload_preflight` and
`materialize_offload_lane_manifest`.

Prerequisites and discipline:

- The `grok-cli` binary must be on `PATH`; the pinned model is
  `grok-composer-2.5-fast`. Pre-flight fails fast (zero dispatch) if the
  binary is missing, the model pin is wrong, the worktree is dirty, or
  `token_budget` is unset.
- Run it only from an active `feature/<task-ref>` branch, never `main`.
- `token_budget` (required) is a cross-cycle circuit breaker: a
  non-converging lane stops at the next cycle boundary after cumulative
  spend crosses the cap, keeping the worktree diff and recording a
  `token_budget_exceeded` blocker. The lane is not silently killed, and it
  does not fall back to Claude or retry.
- The lane's own review runs on `grok-cli`; the offload worker never
  auto-merges. Review the handoff diff (e.g. `/branch-review`) before
  merging.

```text
/offload "implement the CSV export helper" token-budget=120000
```

The offload governor enforces the reviewer-backend pin, the `token-budget`
ceiling, and the no-auto-merge guardrail described above.

## Upgrade

Bump the consumer tag and re-run `update`:

```bash
workbay-bootstrap update --target . --remote-ref v0.1.35
```

(Repos that consume the overlay through the generated Make surface can
run `make workbay-update`, which performs the git fetch + reinstall.)

`update` re-runs the generator, refreshes SHARED surfaces, re-syncs the
git-sourced MCP tool closure when needed, and (when `--mcp-servers` is
supplied) refreshes the three config files. Local edits to GENERATED
surfaces are preserved unless `doctor` reports drift; see "Drift" below.

Pin `v0.1.27` or later; earlier consumer tags are broken (missing
surfaces, generator deps). See [Git ref notes](#git-ref-notes) for
`#subdirectory=` install URLs and fork remotes.

## Migrating from legacy `agentic-system`

If `workbay-bootstrap install` refuses with **legacy agentic-system overlay
detected**, the target still carries the old distribution layout. Remove these
artifacts before re-installing from `workbay.git`:

- `.agentic-overlay.json` (legacy manifest)
- `.agentic/` (embedded clone directory, including `.agentic/remote`)
- Stale symlinks that pointed into `.agentic/remote`
- Any `core.hooksPath` value under `.agentic/` (reset after cleanup)

Then run a fresh install against a current `workbay` tag (see
[Git ref notes](#git-ref-notes)). Pin
`v0.1.27` or later for the D-class git_overlay consumer-install fixes.

## Git ref notes

The primary install above **is** the git-overlay flow. Use these forms when
you need a one-shot CLI without persisting the tool, or when scripting from
a fork:

```bash
REF=v0.1.35
R="git+https://github.com/darce/workbay.git@$REF"
uvx --no-sources \
    --with "$R#subdirectory=packages/workbay-protocol" \
    --with "$R#subdirectory=packages/workbay-system" \
    --from "$R#subdirectory=packages/workbay-bootstrap" \
    workbay-bootstrap install --target /path/to/your/repo --remote-ref "$REF"
```

To upgrade, bump `--remote-ref` and re-run `update` (see [Upgrade](#upgrade)).

> **Note:** Pin to `v0.1.2` or later. Earlier tags are broken:
>
> - `v0.1.0` — bootstrap looks for shared surfaces at the clone root and
>   fails with `required surface 'scripts/hooks' was not materialized`.
> - `v0.1.1` — bootstrap is missing the PyYAML runtime dep; the
>   generator subprocess exits with `PyYAML is required to read skill.yaml`.

The monorepo root has no `pyproject.toml` (each package owns its own
under `packages/<name>/`). Always use the `#subdirectory=` URL fragment
when installing a package straight from git.


## Refresh MCP servers

`mcp-sync` is a config-only refresh of the three managed MCP-server
surfaces:

- `.mcp.json` (Claude Code)
- `.vscode/mcp.json` (VS Code)
- `.codex/config.toml` (Codex CLI)

It also rewrites the `mcp_servers` provenance block in
`.workbay-bootstrap.json` so the next run can prune removed managed
launchers without touching third-party entries.

```bash
workbay-bootstrap mcp-sync --target . --mcp-servers default --check    # exit 1 on drift
workbay-bootstrap mcp-sync --target . --mcp-servers default --apply    # write
```

`--mcp-servers` accepts the literal `default` (resolves to the bundled
`DEFAULT_MCP_SERVERS` constant) or a path to a JSON file
holding either a flat ``{name: spec, ...}`` mapping or
``{"mcpServers": {...}}``. Add `--prune-removed-managed` to drop names that previously
appeared in the ledger's `mcp_servers` block but are no longer in the
resolved map; third-party launchers (names absent from the ledger) are
never pruned. Add `--surfaces claude` (or `vscode`, `codex`) to limit
the write to a subset. Add `--json` for machine-readable output that
includes per-surface drift, action, preserved third-party names, and
the post-write ledger state.

`mcp-sync` does NOT fetch the remote, regenerate skills, or run
`init-state`. Use `update` for those. Exit codes: `0` clean reconcile,
`1` drift detected with `--check`, `2` resolution failure (e.g.
unparseable `--mcp-servers`).

## Drift detection and repair

Two subcommands keep the overlay honest after the install:

```bash
workbay-bootstrap doctor --target .   # exit 1 when drift found
workbay-bootstrap repair --target .   # restore drifted surfaces
```

`doctor` covers SHARED (broken or moved symlinks), GENERATED (the
generator's `--check` mode), and — when `--mcp-servers` is supplied —
the three config files. `repair` re-runs the generator for any
GENERATED drift, restores SHARED symlinks, and (with `--mcp-servers`)
rewrites managed config entries. Run with `--force-dirty` to overwrite
SHARED surfaces that contain real local content.

## Overriding individual skills

The Claude, Codex, Cursor, and grok skill surfaces are generated plugin trees. To
override a skill, add an override component under
`workbay-overrides/workbay-system/` and rerun install/update so the
effective plugin tree is regenerated. Copilot prompts remain generated
as real files in the repo and can be edited directly when you accept
the resulting drift:

```text
.github/prompts/<slug>.prompt.md
.workbay/generated/plugins/workbay-system/effective/claude/skills/<slug>/SKILL.md
.workbay/generated/plugins/workbay-system/effective/codex/skills/<slug>/SKILL.md
.workbay/generated/plugins/workbay-system/effective/cursor/skills/<slug>/SKILL.md
.workbay/generated/plugins/workbay-system/effective/grok/skills/<slug>/SKILL.md
```

`doctor` will flag direct edits to generated outputs as drift on the
next run; keep durable overrides in the override tree so update/repair
can compose them repeatedly.

To override a hook or shared script, replace the surface with a real
local directory before running `install` (or `repair`). The bootstrap
respects an existing real directory and records `source: "local"` in
the manifest.

## Optional `git plan-cat` alias

`workbay-bootstrap` hoists `scripts/workbay/git-plan-cat.sh` as a
shell wrapper around `make plan-show`'s underlying CLI. It is **not**
installed as a `git` alias automatically — the Make targets
(`make plan-show`, `make plan-edit`, `make plans-list`) remain the
canonical entrypoint. Opt in by adding the snippet below to your
`.gitconfig` (user-level or repo-level):

```gitconfig
[alias]
    plan-cat = "!sh scripts/workbay/git-plan-cat.sh"
```

Then `git plan-cat` prints the active task's plan, and
`git plan-cat internal` resolves a specific task. Both forms produce
byte-for-byte the same output as `make plan-show` because both shell
through `workbay_handoff_mcp.plan_cli show` — there is no second copy of
the resolver to drift.

Override the launcher by exporting `WORKBAY_HANDOFF_PLAN_CLI` (e.g. when
the consumer manages its own venv); the default is the same `uvx`
invocation `Makefile.d/plans.mk` uses.

## `current_task_auto_regen` migration note

`mcp-workbay-handoff` flipped the default for `current_task_auto_regen`
to **off** in v0.5.0. If your tooling reads
`<target>/CURRENT_TASK.json` (e.g. dashboards, oncall scripts), opt
back in explicitly:

```bash
# in the target repo, before booting the handoff server
export WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN=1
```

If you have never read `CURRENT_TASK.json`, no action is required —
the file is no longer regenerated automatically.

## What lives where

The canonical source of truth for bootstrap-managed surfaces is the
installer itself: `SHARED_SURFACES` and `GENERATED_SURFACES` in
`packages/workbay-bootstrap/src/workbay_bootstrap/install.py`.
The table below is documentation of that contract, not an independent
surface registry.

| Surface                               | Source     | Layer       |
| ------------------------------------- | ---------- | ----------- |
| `scripts/hooks/`                      | shared     | symlink     |
| `.github/hooks/`                      | shared     | symlink     |
| `docs/workbay/contracts/`             | shared     | symlink     |
| `docs/workbay/rules/`                 | shared     | symlink     |
| `Makefile.d/` non-excluded children   | shared     | carved dir  |
| `scripts/workbay/` non-excluded children | shared  | carved dir  |
| `.github/prompts/`                    | generated  | real dir    |
| `.workbay/generated/plugins/workbay-system/base/` | generated | real dir |
| `.workbay/generated/plugins/workbay-system/effective/` | generated | real dir |
| `.mcp.json`                           | generated  | real file   |
| `.vscode/mcp.json`                    | generated  | real file   |
| `.codex/config.toml`                  | generated  | real file   |
| `core.hooksPath` git config           | generated  | git config  |
| `.workbay/remote/`                    | bootstrap  | git clone   |
| `.workbay-bootstrap.json`             | bootstrap  | manifest    |

All bootstrap-managed paths are listed in `<target>/.workbay-bootstrap.json`
(legacy `.workbay-overlay.json` is auto-renamed on the next install)
with their `source` discriminator (`shared` | `local` | `generated`).

## `.gitignore` policy for bootstrap-managed surfaces

The single rule: **commit the install ledger
(`.workbay-bootstrap.json`); regenerate everything else via
`workbay-bootstrap install` after `git clone`.** Add the block below to
the consumer repo's `.gitignore`.

This policy derives from the installer's owned-surface lists in
`packages/workbay-bootstrap/src/workbay_bootstrap/install.py`
(`SHARED_SURFACES` + `GENERATED_SURFACES` + the materialized trees in
`HARNESS_PLUGIN_DELIVERY`) plus the config writers.
Only ignore paths the installer actually owns. Harness marketplace
pointers (`.claude-plugin/marketplace.json`, `.agents/plugins/marketplace.json`)
stay tracked; harnesses without marketplace indirection (Grok) get their
full plugin tree re-materialized on every install/update, so that tree is
ignored like the generated surfaces.

```gitignore
# --- workbay-bootstrap-managed surfaces ---------------------------------
# Regenerate via `workbay-bootstrap install` from the pinned `remote_sha`
# in `.workbay-bootstrap.json` (which IS tracked — it's the install ledger).
#  - SHARED entries are symlinks into `.workbay/remote/`; they break on a
#    fresh clone until bootstrap recreates the cache.
#  - GENERATED entries are deterministic outputs of the workflow generator
#    and the MCP-config writer; committing them produces drift on every
#    `bootstrap update`.

.workbay/                  # disposable remote-clone cache

/scripts/hooks             # SHARED symlinks
/.github/hooks
/docs/workbay/contracts
/docs/workbay/rules
/Makefile.d
/scripts/workbay

# GENERATED workflow outputs + the Grok plugin tree (no marketplace
# indirection; re-materialized from effective/grok on every install/update)
/.github/prompts/
/.grok/plugins/workbay-system

/.mcp.json                 # GENERATED MCP-server configs
/.vscode/mcp.json
/.codex/config.toml

.task-state/               # local handoff SQLite (per checkout)
```

You do not have to hand-author this: when any managed surface would leak
into `git status`, `workbay-bootstrap install`/`adopt` append (and on
later runs reconcile) an equivalent block delimited by
`# >>> WORKBAY_BOOTSTRAP OVERLAY IGNORE >>>` /
`# <<< WORKBAY_BOOTSTRAP OVERLAY IGNORE <<<` sentinels. The fence above
is the hand-authored equivalent for repos that prefer to own their
`.gitignore` outright.

Dogfood exception: this monorepo has authored root content adjacent to
bootstrap-owned paths. Do not widen these rules to blanket-ignore
entire roots like `.claude/` or `.codex/`, and do not add non-owned
paths such as unrelated Make fragments or `docs/workbay/generated/`
unless the installer surface lists change first.

CI implications: `git clone` alone yields a checkout with no hooks, no
generated prompts/plugin trees, no MCP wiring. CI must run
`workbay-bootstrap install --target .` before any workbay-system
surface is used — package-source manifests re-install from the pinned
`workbay-system` wheel recorded in the committed
`.workbay-bootstrap.json`; git-overlay manifests use its `remote_ref`
+ `remote_sha`.
This is the same flow human developers run, so it forces install
reproducibility through the same code path consumers ship.

Why not commit the symlinks and generated dirs? Two failure modes:

1. **Symlinks point into `.workbay/remote/` which is gitignored.** If
   you commit them, a freshly-cloned checkout has dangling symlinks
   until bootstrap recreates the cache. You still need bootstrap; the
   commit just hides the dependency.
2. **Generated content drifts on every `workbay-bootstrap update`.**
   Committing generated prompt or plugin outputs means each bump
   produces a noisy diff that's not the consumer's authorship. `doctor`
   already detects this as drift; gitignoring the surface eliminates
   the diff entirely.

External consumer repos can usually adopt the block as-is. Dogfood
installs in this monorepo should treat the installer-owned path list as
the boundary and keep authored repo content reviewable in git.

## Install timeouts and step receipts

`workbay-bootstrap install` records per-step outcomes in
`.workbay-bootstrap.json` under `install_steps` (status
`ok|failed|deferred|skipped`, optional `reason`, `failure_class` of
`system` or `application`). Best-effort phases also persist
`presync_projects`, `prewarm_refs`, and `offline_latch` when relevant.

External subprocess calls route through a shared gateway with per-class
defaults. Override any class with
`WORKBAY_TIMEOUT_<CLASS>` (seconds), where `<CLASS>` is one of
`GIT`, `GENERATOR`, `UV_SYNC`, `GITONLY_MCP_TOOLS`, `GROK_CLI`, or
`HANDOFF_CLI`.

`workbay-bootstrap doctor` reads receipt fields before re-probing disk.
`workbay-bootstrap repair` retries deferred install steps (for example
`prewarm_uvx_mcp` after connectivity returns) and inherits managed MCP
registration from the ledger when `--mcp-servers` is omitted.

## See also

- [`../README.md`](../README.md) — what WorkBay is, the command surface,
  and which package to install.
- [`RELEASING.md`](RELEASING.md) — maintainer release playbook (cutting
  and publishing the front door + stack).
