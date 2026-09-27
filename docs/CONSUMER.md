# Consuming the workbay

This is the entry point for target repos that want the workbay-system
overlay (skills, hooks, MCP-server configs) installed into their tree.
Delivery is **GitHub-only**: consumers install from tagged refs on
[`darce/workbay`](https://github.com/darce/workbay) — no PyPI.

## Quick start (primary path)

### Claude Code plugin

Register the public plugin marketplace once per checkout:

```text
/plugin marketplace add darce/workbay
```

### CLI front door (recommended for non-Claude harnesses)

Install the `workbay` CLI from a consumer tag using the helper script
(it expands the multi-package git closure so you do not list
`#subdirectory=` members by hand), then hoist the overlay:

```bash
REF=v0.1.55   # tag must include scripts/install-workbay-cli.sh (shipped from this change forward)
curl -fsSL "https://raw.githubusercontent.com/darce/workbay/${REF}/scripts/install-workbay-cli.sh" \
  | bash -s -- "$REF"
workbay install --target /path/to/your/repo --remote-ref "$REF"
```

High-assurance hosts: download the script, read it, then run
`bash install-workbay-cli.sh "$REF"` instead of piping curl to bash.

From a checkout of the same tag (monorepo or public mirror):

```bash
./scripts/install-workbay-cli.sh v0.1.55
workbay install --target /path/to/your/repo --remote-ref v0.1.55
```

(`--remote-url` defaults to `git@github.com:darce/workbay.git`; HTTPS
works too. `workbay install` defaults `--source package` — the published
`workbay-system` payload. Pass `--source git_overlay` to clone the
monorepo into `<target>/.workbay/remote/` at the pinned ref.)

From a consumer repo root, `doctor` / `status` / `repair` / `apply-hooks`
default to cwd (no `--target` required; `--target` still overrides).
Install still requires `--target`. Install and `apply-hooks` accept
`--hooks none|repo|all` (`none` is the install default;
`apply-hooks --hooks none` is a no-op). Re-apply ledger adapters
with `workbay repair --hooks recorded`. `recorded` is repair only —
`update` already reapplies recorded adapters. The eight
`--install-*-hook` flags remain aliases.

```bash
workbay status
workbay doctor
workbay repair
workbay apply-hooks --install-codex-stop-hook
# re-run install-workbay-cli.sh with a newer tag to upgrade the CLI
```

> If `uv tool install` prints `Failed to hardlink files; falling back
> to full copy`, your `uv` cache and tool dir are on different
> filesystems. The install still succeeds — silence the warning with
> `export UV_LINK_MODE=copy` in your shell profile.

### Manual `uv` closure (escape hatch)

Only if you cannot run the helper script. Member packages declare
workspace sources for monorepo development; a consumer install needs
`--no-sources` and an explicit `--with` for each runtime sibling (kept
in sync with `packages/workbay-bootstrap/.../gitonly_closure.py`):

```bash
REF=v0.1.55
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources --force \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/mcp-workbay-handoff" \
  --with "$R#subdirectory=packages/mcp-workbay-orchestrator" \
  --with "$R#subdirectory=packages/workbay-bootstrap" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay" \
  workbay
workbay install --target /path/to/your/repo --remote-ref "$REF"
```

Bootstrap-only CLI (narrower tool). `workbay-bootstrap install` still
defaults `--source git_overlay` unless you pass `--source package`:

```bash
REF=v0.1.55
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources --force \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay-bootstrap" \
  workbay-bootstrap
workbay-bootstrap install --target /path/to/your/repo --remote-ref "$REF"
```

A `git_overlay` install:

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

`--with-embeddings` is the *verified* opt-in, not a synonym for the default. It
hard-verifies at install time that the `[embeddings]` extra is importable and the
model digest pin is readable, then records `embeddings_mode=verified` in the
bootstrap ledger. When verification fails the install **fails** and names the
repair command, instead of leaving a repo that looks provisioned and silently
degrades at first use. The three ledger values are `verified` (the flag was
passed and verification succeeded), `disabled` (`--no-embeddings`), and
`unspecified` (neither flag). Semantic features gate hard reliance on
embeddings on `verified`; `unspecified` is read as "may or may not work".
`--with-embeddings` and `--no-embeddings` are mutually exclusive and the parser
rejects both together.

```bash
workbay install --target . --with-embeddings     # fail the install if the extra is not usable
workbay repair  --target . --no-embeddings       # skip model re-download during this repair run
```

Turning embeddings policy off is `workbay embeddings --disable` (or install-time `--no-embeddings`).
The SSOT toggle is the CLI verb (not hand-edited harness settings). Run from the worktree root:

```bash
workbay embeddings --status
workbay embeddings --enable
workbay embeddings --disable
```

`workbay-bootstrap embeddings --target <path> …` remains a **deprecated** alias.

Provisioning the model is its own verb, so a host can fetch and verify it ahead
of the install (or re-verify a shared cache) without reinstalling anything:

```bash
workbay-bootstrap provision-embeddings --target .   # download + digest-verify the pinned model
```

Coverage after the fact is reported by `doctor`, not assumed. The
`embedding_backfill_gap` facet counts embeddable rows against embedded ones per
kind and names the shortfall; `embedding_backfill_unmeasurable` is the distinct
finding for a probe that could not measure at all (missing extra, unreadable
DB), so an unmeasurable index never reads as a clean one. Backfilling is opt-in
and never runs on sync:

```bash
python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings --dry-run
python -m workbay_handoff_mcp.scripts.backfill_concept_embeddings --limit 500
```

(In a monorepo checkout the same run is `make backfill-handoff-embeddings
BACKFILL_ARGS="--dry-run"`; that target is not part of the consumer overlay.)

Each harness also exposes a generated **`/wb-harness`** portable command over the same gate:
Claude Code uses an `AskUserQuestion` menu; Codex, Cursor, and Grok use
`/wb-harness embeddings <on|off|status>`, which delegates to `workbay embeddings`.


## MCP-server registration

Default behavior (omitting `--mcp-servers`, or passing the literal
`--mcp-servers default`) registers the two MCP servers shipped by this
monorepo:

| Server                     | Launch form |
| -------------------------- | ----------- |
| `workbay-handoff-mcp`      | `python3 scripts/hooks/mcp_launch.py workbay-handoff-mcp` |
| `workbay-orchestrator-mcp` | `python3 scripts/hooks/mcp_launch.py workbay-orchestrator-mcp` |

`WORKBAY_TOOL_ROSTER_POLICY` controls the session-start advertised roster.
The default, `all`, exposes the pinned full surface (29 handoff tools). The
opt-in `skill` policy exposes, per server, catalog tools plus `ALWAYS_SERVE`
members, the resolved task's skill-declared tools (or its status-default skill
union), and persisted activated-domain tools, intersected with that server's
closed tool-name set. Resolution failures take the documented `in_progress`
bootstrap floor rather than preventing server startup.

An **optional** third managed server (`codebase-graph-mcp`) launches a
prebuilt `codebase-memory-mcp` binary from `PATH`. It is excluded by
default so consumers without the binary never inherit a dead entry. Opt
in at install:

```bash
workbay install --target . --with-codebase-graph
```

Re-materialize managed surfaces later with the same flag:

```bash
workbay mcp-sync --target . --mcp-servers default --with-codebase-graph --apply
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

## Remote execution policy (`--with-remote`)

`--with-remote` verifies at install time that the remote offload gate is
reachable and records `execution_mode: remote_only` in the bootstrap ledger.
The probe reads the gate host from `WORKBAY_REMOTE_GATE_HOST` (env first),
then the `REMOTE_GATE_HOST` key in `.workbay/remote-gate.env`, and SSHes to it.

```bash
workbay install --target . --with-remote
```

Install **fails** when the probe fails, naming the probe state and detail. That
is deliberate: a repo whose ledger claims `remote_only` while the host is
unreachable would refuse every local backend and have nowhere to send the work.
For deferred setup, install without the flag and flip the ledger later:

```bash
workbay repair --target . --with-remote     # probe, then flip to remote_only
workbay repair --target . --no-remote       # flip back to local_ok (no probe)
```

Any ledger value other than the literal `remote_only` reads as `local_ok`, so a
missing manifest or field is never mistaken for a remote install.

What `remote_only` changes at dispatch time: `/wb-offload` resolves an omitted
backend to `grok-remote`, and an explicitly named local backend is refused with
the typed `remote_required` outcome instead of being silently substituted.
Dropping the policy is an operator act (`repair --no-remote`), not something a
lane or coordinator may do to get unblocked.

The gate host is also what `make check-remote` uses to run suites remotely.

## Offloading a slice to a junior backend (`/wb-offload`)

`/wb-offload` hands one self-contained implementation slice to a bounded junior
lane, then presents the result behind a review gate, so an expensive
orchestrator model does not spend tokens on work a cheaper backend can do. It
ships with the orchestrator server (no extra install). The skill declares eight
MCP tools (`offload_preflight`, `materialize_offload_lane_manifest`,
`manage_worktree_lane`, `dispatch_lane_work`, `run_offload_pass`,
`await_offload_pass`, `list_available_backends`, `turn_metrics`); the first two
are the offload-specific additions beyond the shared lane surface.

Backend and reasoning effort are always explicit; there is no `--agent auto`
and no fallback to a second backend when the first fails. Pass `--agent` for
operator intent; the engine resolution default is `grok-remote` under
`remote_only` and `grok-cli` under `local_ok`. `/wb-offload` resolves `--agent`
against these profiles:

| `--agent` | Where it runs | Model pin |
| --- | --- | --- |
| `grok-remote` | Provisioned gate host | `grok-4.6` |
| `grok-cli` | Local `grok` binary | `grok-4.6` |
| `cursor-cli` | Local `cursor-agent` binary | `cursor-grok-4.5-high-fast` |
| `codex-subagent` | Codex app-server bridge | optional |

Local profiles are only selectable under a `local_ok` ledger. Under a
`--with-remote` install the bootstrap ledger (`.workbay-bootstrap.json`)
records `execution_mode: remote_only`; `/wb-offload` may select only
`grok-remote`, and every other explicit agent id yields a typed
`remote_required` outcome with no silent backend substitution.
`WORKBAY_GROK_MODEL` overrides the grok pin; it is read at import time, so set
it before the orchestrator server starts. The environment still outranks
`SHIPPED_GROK_MODEL` at import. A retired override logs a warning naming the
retired slug, and every dispatch path (`GrokCliAdapter`, `RemoteExecAdapter`,
and `build_agent_spec`/CLI) refuses it (`RetiredModelError` / `RuntimeError`).
The operator must unset or update `WORKBAY_GROK_MODEL` to a live slug before
the next dispatch.

Lane dispatch (`dispatch_lane_work`) reaches three remote invocation backends.
`codex-remote` and `cursor-remote` are not `/wb-offload` `--agent` values; only
the four profiles above are:

| Lane-dispatch invocation backend | Default model |
| --- | --- |
| `grok-remote` | `grok-4.6` |
| `codex-remote` | `gpt-5.6-sol` |
| `cursor-remote` | `cursor-grok-4.5-high-fast` |

Both `codex-remote` and `cursor-remote` require `WORKBAY_REMOTE_GATE_HOST` and
a provisioned host.

Prerequisites and discipline:

- Pre-flight fails fast, with zero dispatch spend, when the backend probes
  unavailable, the effort level is unsupported for that profile, the model pin
  is wrong, the worktree is dirty, or `token_budget` is unset.
- Run it only from an active `feature/<task-ref>` branch, never `main`.
- `token_budget` (required) is a cross-cycle circuit breaker: a non-converging
  lane stops at the next cycle boundary after cumulative spend crosses the cap,
  keeping the worktree diff and recording a `token_budget_exceeded` blocker.
  The lane is not silently killed and does not retry elsewhere.
- Single-cycle bounds are profile-specific (grok: derived turns+timeout;
  `codex-subagent`: bridge timeout; `cursor-cli`: timeout only).
  `token_budget` stays cross-cycle.
- The lane commits its own self-verified work and submits a handoff. It never
  auto-merges. Review the diff (`/wb-review-code` or `/wb-review-slice`) first.
- Every pass returns a typed outcome (`handoff_ready`, `needs_guidance`,
  `self_verify_failed`, `timeout`, `token_budget_exceeded`, `remote_required`,
  `admission_deferred`, `composer_violation_quarantined`, and the other
  declared pass outcomes) plus `commit_landed` and `failed_stage`, so the
  caller branches on structured result rather than log tails.
  On `remote_required`, recover with `workbay repair --no-remote` or switch
  to `grok-remote`.

```text
/wb-offload --agent grok-remote --effort high --token-budget 120000 "implement the CSV export helper"
```

## Upgrade

Bump the consumer tag and re-run `update`:

```bash
workbay-bootstrap update --target . --remote-ref v0.1.55
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

`workbay install` defaults `--source package`. The forms below are the
optional clone-backed (`--source git_overlay`) escape hatch — one-shot CLI
without persisting the tool, or scripting from a fork:

```bash
REF=v0.1.55
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

Reclaiming what an install left behind is a third verb. `clean` (aliased as
`gc`) removes orphaned overlay clones and stale package directories. It lists
and does not delete until you say so — `--dry-run` is the default and `--yes`
is the only thing that removes anything:

```bash
workbay-bootstrap gc --target .          # dry run: list what would be reclaimed
workbay-bootstrap gc --target . --yes    # apply removals
```

`doctor` covers SHARED (broken or moved symlinks), GENERATED (the
generator's `--check` mode), and — when `--mcp-servers` is supplied —
the three config files. It also reports an `mcp_protocol` facet per
managed server (installed SDK version, latest protocol version, resolved and
declared `fastmcp`), which is informational: a `false` facet names a pin
disagreement without failing the run. `repair` re-runs the generator for any
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
