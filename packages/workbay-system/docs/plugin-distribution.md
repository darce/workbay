# Plugin Distribution

Operator-facing guide for emitting and installing the workbay-system
Claude Code, Codex, Cursor, and grok plugin trees. The architectural
decision is recorded in
[ADR-001: Agentic Plugin Distribution](workbay/adrs/ADR-001-agentic-plugin-distribution.md)
and
[ADR-003: Plugin Consumer Overrides](workbay/adrs/ADR-003-plugin-consumer-overrides.md);
this page covers the day-to-day flow.

## Audience

- Maintainers of `workbay` (APM) who emit and ship the
  plugin trees.
- Consumer-repo operators installing the plugin into Claude Code,
  Codex, Cursor, or grok.

VS Code Copilot does not consume these plugin trees. Its
`.github/prompts/<command>.prompt.md` files stay on the legacy generator
output, but those prompts are rendered from the same canonical manifest
and skill bodies.

## Canonical Inputs

The plugin emission reads three files from `packages/workbay-system/`:

| Input                                                      | Role                                                                                     |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `skills/<slug>/{skill.yaml, body.md}`                      | Canonical cross-harness skill bodies. SKILL.md is rendered identically for both harnesses. |
| `config/agent-workflows/portable_commands.json`            | Portable command manifest; selects which skills are emitted into the plugin tree.        |
| `config/agent-workflows/mcp_servers.yaml`                  | Canonical MCP registration manifest (implementation note): per-harness `registration` ownership table, server launch specs, and the plugin version. Also generates bootstrap's `_mcp_pins.py` via `make mcp-pins-sync`. |

## Emitted Layout

`make plugins-build` writes one tree per supported harness under
`.workbay/generated/plugins/workbay-system/base/{claude,codex,cursor,grok}`.
That is the same bootstrap-generated base tree that the checked-in
marketplace pins reference. Paths in the layout sketch below are
relative to the repo root:

```text
.workbay/generated/plugins/workbay-system/base/claude/
  .claude-plugin/plugin.json   # metadata only: name, version, skills
  skills/<slug>/SKILL.md       # one per portable_commands.json skill

.workbay/generated/plugins/workbay-system/base/codex/
  .codex-plugin/plugin.json    # same body as claude/.claude-plugin/plugin.json
  skills/<slug>/SKILL.md       # byte-identical to base/claude/skills/<slug>/SKILL.md

.workbay/generated/plugins/workbay-system/base/cursor/
  .cursor-plugin/plugin.json   # same body as claude/.claude-plugin/plugin.json
  commands/<id>.md             # native Cursor command wrappers
  hooks.json                   # Cursor hook projection from harness-protocol.yaml
  skills/<slug>/SKILL.md       # byte-identical to base/claude/skills/<slug>/SKILL.md

.workbay/generated/plugins/workbay-system/base/grok/
  .grok-plugin/plugin.json     # same body as claude/.claude-plugin/plugin.json
  hooks/hooks.json             # PreToolUse guards projected from harness-protocol.yaml
  skills/<slug>/SKILL.md       # byte-identical to base/claude/skills/<slug>/SKILL.md
```

Plugin trees are **skills-only**: the
`registration` table in `mcp_servers.yaml` declares `root` ownership for
every harness, so MCP servers register through the bootstrap-written root
surfaces (`.mcp.json`, `.vscode/mcp.json`, `.codex/config.toml`; grok
reads the repo-root `.mcp.json` compat surface) and the plugin manifests
omit `mcpServers`. Dual registration — the pre-plan-0028 state where
Claude loaded the same servers from both the root `.mcp.json` and the
plugin tree — is a bug, and `workbay-bootstrap doctor` flags a stray
plugin-tree `.mcp.json` as drift.

`plugin.json` is metadata-only: it carries `name=workbay-system`, a
`version` pulled from `mcp_servers.yaml plugin_version`, a short
`description` (constant in the generator), and the sibling-path
reference `skills: "./skills/"`. Slash commands and skills are
discovered by each harness from that sibling directory — there is no
inline `slashCommands` array, and under `root` ownership there is no
`mcpServers` key.

When a harness is flipped to `registration: plugin` in
`mcp_servers.yaml`, its manifest regains `mcpServers: "./.mcp.json"` and
the generator emits the sibling `.mcp.json`. The blobs include
`"type": "stdio"` and the same server entries, but their top-level shape
differs by harness. Claude uses the camelCase wrapper from the Claude
plugin schema; live Codex CLI 0.131.0 rejects wrapped plugin MCP config,
so Codex uses a bare server map:

Claude:

```json
{
  "mcpServers": {
    "workbay-handoff-mcp": {
      "type": "stdio",
      "command": "python3",
      "args": ["scripts/hooks/mcp_launch.py", "workbay-handoff-mcp"]
    },
    "workbay-orchestrator-mcp": {
      "type": "stdio",
      "command": "python3",
      "args": ["scripts/hooks/mcp_launch.py", "workbay-orchestrator-mcp"]
    }
  }
}
```

Codex:

```json
{
  "workbay-handoff-mcp": {
    "type": "stdio",
    "command": "python3",
    "args": ["scripts/hooks/mcp_launch.py", "workbay-handoff-mcp"]
  },
  "workbay-orchestrator-mcp": {
    "type": "stdio",
    "command": "python3",
    "args": ["scripts/hooks/mcp_launch.py", "workbay-orchestrator-mcp"]
  }
}
```

## Operator Commands

Run both from the repo root:

```bash
make plugins-build   # emit .workbay/generated/plugins/workbay-system/base/{claude,codex,cursor,grok}/
make plugins-check   # re-emit with --check; fails on any hand-mutation
```

`plugins-build` is destination-idempotent: re-running it produces a
byte-identical tree. `plugins-check` is the gate to wire into CI for the
generated plugin tree that marketplace pins load.

Override the destination by passing `PLUGINS_DIST_ROOT=/some/path`.

## Consumer Install

Consumer install is **project-scoped via local plugin marketplace files**
that live in the consumer repo. Each harness reads a different file, but
both pin the same `workbay-system` plugin from the same generated plugin
tree. The pin files are committed in the consumer repo so every clone
opens the project in the same plugin state.

The three pin files (paths are repo-root-relative):

| File                                  | Harness | Purpose                                                                                            |
| ------------------------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `.claude-plugin/marketplace.json`     | Claude  | Declares the local marketplace and lists the `workbay-system` plugin with its generated source.    |
| `.claude/settings.json`               | Claude  | WorkBay-owned project settings: marketplace pin, `enabledPlugins`, contract-rendered guard hooks (`_managed_by: workbay-bootstrap`). |
| `.claude/settings.local.json`         | Claude  | Gitignored individual overrides; bootstrap migrates non-managed project keys here on install/update. |
| `.agents/plugins/marketplace.json`    | Codex   | Codex's project-scoped marketplace; same plugin name, points at the generated Codex plugin root.   |

The canonical name is `workbay-system@workbay-marketplace`.

Marketplace pins always target
`.workbay/generated/plugins/workbay-system/effective/{claude,codex}`
(internal always-effective model). Bootstrap composes the effective tree on
every install/update/repair run: with zero overrides it is a byte-identical
passthrough of `base/` plus a passthrough `plugin-lock.json` receipt, and
with overrides it is the composed result. There is no pin-flipping logic —
a pin still pointing at `base/` is reported by doctor as
`pin_target_drift` with a repair remediation.

`.claude/settings.json` is owned wholesale by workbay-bootstrap for the
managed subtree (`_managed_by`, marketplace pin, `enabledPlugins`, and
contract-rendered hooks). Team-shared Claude customizations that must stay
committed belong in `workbay-overrides/workbay-system/` via
`components.claude_settings_hooks` (`mode: add` merges hook arrays with
consumer precedence on matcher collision; `mode: override` replaces whole
stages). Individual or machine-local hooks belong in gitignored
`.claude/settings.local.json`. Claude Code **aggregates** hooks across
Project + Local settings files (see
[Claude Code hooks](https://code.claude.com/docs/en/hooks)); workbay relies
on that aggregation premise so migrated user hooks keep firing from Local
after wholesale project ownership.

**Manual integration check (not unit-testable):** on a consumer checkout,
confirm a project-rendered guard hook and a local user hook both appear in
Claude's `/hooks` menu (Project + Local sources) and both fire on a
representative tool event.

The committed pin uses Claude's `extraKnownMarketplaces` schema, whose inner
`source.source` discriminator is **`directory`** for a repo-local
marketplace tree (Claude Code 2.1.144+). This is **not** the same as
`.agents/plugins/marketplace.json`'s inner source object, which uses Codex's
`local` discriminator. Pasting one schema into the other file silently breaks
session start. The managed project file carries marketplace pins plus the
rendered hook block:

```json
{
  "_managed_by": "workbay-bootstrap",
  "extraKnownMarketplaces": {
    "workbay-marketplace": {
      "source": { "source": "directory", "path": "." }
    }
  },
  "enabledPlugins": {
    "workbay-system@workbay-marketplace": true
  },
  "hooks": { "...": "contract-rendered guard hooks" }
}
```

`scripts/validate_claude_settings_pin.py` enforces this shape; it runs
under `make check-claude-settings-pin` and is wired into
`make check-agent-workflows`.

### Claude Code

1. Run `workbay-bootstrap install` or `workbay-bootstrap update` from
   the consumer repo so the always-effective plugin tree exists at
   `.workbay/generated/plugins/workbay-system/effective/claude/`.
   In the APM source repo, `make plugins-build` remains the base-tree
   build/check path.
2. Commit `.claude-plugin/marketplace.json` and `.claude/settings.json`
   alongside the project. Both files are tiny and human-readable; do not
   edit the generated plugin tree by hand.
3. **Fresh clone, one-time per checkout:** register the local
   marketplace from the repo root before any `claude plugin install`
   invocation:

   ```bash
   claude plugin marketplace add ./ --scope project
   ```

   This binds the project's `workbay-marketplace` name (declared
   in `.claude-plugin/marketplace.json` and referenced by the
   `enabledPlugins` pin in `.claude/settings.json`) to the committed
   marketplace, whose plugin source points at the local
   `.workbay/generated/plugins/workbay-system/effective/claude/` tree.
   Skipping this step makes
   `claude plugin install workbay-system@workbay-marketplace`
   fail with `Plugin "workbay-system" not found in marketplace
   "workbay-marketplace"`, because the CLI does not auto-discover
   marketplaces from `.claude-plugin/marketplace.json` outside of an
   interactive Claude Code session.

   The Codex pin discussed below does not need an equivalent step — Codex
   auto-discovers project-scoped marketplaces from
   `.agents/plugins/marketplace.json` on session start.

   > **Caveat:** `claude plugin marketplace add ./ --scope project`
   > rewrites `.claude/settings.json`'s
   > `extraKnownMarketplaces.<name>.source.path` to an absolute,
   > machine-local path. The committed pin keeps `path: "."` so it stays
   > portable across clones; if you re-run the CLI step, restore
   > `path: "."` (and confirm `source.source: "directory"`) before
   > committing. `make check-claude-settings-pin` fails on either drift.

4. Open the project in Claude Code. Claude resolves the marketplace
   declared by `.claude-plugin/marketplace.json`, sees the plugin pinned
   on in `.claude/settings.json`, and discovers skills from the generated
   plugin tree; MCP servers come from the bootstrap-written root
   `.mcp.json` (implementation note `root` ownership). (Interactive sessions also accept the
   `marketplace add` step at runtime, but committing the pin files plus
   the one-time CLI registration keeps the install reproducible across
   clones.)

The equivalent CLI form (writes the same `enabledPlugins` entry, and
must follow the `marketplace add` step above) is:

```bash
claude plugin install workbay-system@workbay-marketplace --scope project
```

> **Distribution status:** the plugin is exercised against local generated
> trees only: `.workbay/generated/plugins/workbay-system/base/{claude,codex}`
> for source-tree build checks and
> `effective/{claude,codex}` for consumer marketplace pins. It is not yet
> published to an external marketplace. Every install path documented here is
> project-scoped against the committed `.claude-plugin/` and `.agents/plugins/`
> files.

#### Claude Delivery Proof

Claude docs checked on 2026-05-22:

- https://code.claude.com/docs/en/plugin-marketplaces — local marketplace flow uses `.claude-plugin/marketplace.json`, relative plugin `source` values rooted at the marketplace root, `claude plugin validate .`, `claude plugin marketplace add <source> --scope project`, and project-scoped install.
- https://code.claude.com/docs/en/plugins-reference — plugin manifests live at `.claude-plugin/plugin.json`, skills load from `skills/<slug>/SKILL.md`, `.mcp.json` can supply MCP servers, `claude plugin list --json` reports installed plugins, and `claude plugin details <name>` reports component inventory.

Sandbox transcript summary, using `claude` 2.1.146 with a `mktemp -d` project, disposable `HOME`, and disposable `CLAUDE_CODE_PLUGIN_CACHE_DIR`:

```bash
claude plugin validate .
claude plugin marketplace add ./ --scope project
claude plugin install workbay-system@workbay-marketplace --scope project
claude plugin list --json
claude plugin details workbay-system@workbay-marketplace
```

Claude delivery proof result: pass (recorded pre-plan-0028, when the plugin tree still carried the MCP pins: `claude plugin list --json` showed the plugin enabled with the workbay MCP pins and `claude plugin details` listed 10 skills plus the two MCP servers). Post-plan-0028 expectation: `claude plugin details` lists the 10 skills only; the two MCP servers register once, from the root `.mcp.json` — a session listing a `plugin:workbay-system:` duplicate of either server is the dual-registration bug this plan removed. Re-proof pending the next sandbox pass.

### Codex

1. Run `workbay-bootstrap install` or `workbay-bootstrap update` so
   `.workbay/generated/plugins/workbay-system/effective/codex/` exists.
2. Commit `.agents/plugins/marketplace.json` — the committed Codex pin that
   Codex auto-discovers from the project on session start. `.codex/config.toml`
   is **not** committed: it is gitignored and developer-local (`.gitignore`
   ignores `/.codex/config.toml`, the same surface that carries the per-machine
   MCP-server block). `workbay-bootstrap install` (and `repair`) regenerate its
   Codex activation tables on each checkout:

   ```toml
   [marketplaces.workbay-marketplace]
   source_type = "local"
   source = "."

   [plugins."workbay-system@workbay-marketplace"]
   enabled = true
   ```

   The marketplace source stays relative to the project root (`source = "."`),
   so bootstrap regenerates this activation locally on every checkout without
   ever writing `~/.codex/config.toml` or the user's plugin cache. Activation
   travels with the repo through the committed marketplace pin plus this
   bootstrap-regenerated config, not by committing `.codex/config.toml` itself.
3. Open the project with Codex. The repo-local activation points Codex at
   the local marketplace, and the marketplace's `local` source resolves the
   generated plugin tree. Skills come from `skills/<slug>/SKILL.md`; MCP
   servers come from the bootstrap-written `[mcp_servers.*]` tables in
   `.codex/config.toml` (implementation note `root` ownership — the plugin tree no
   longer ships a `.mcp.json`).

The user-global CLI install form remains a compatibility fallback, not the
bootstrap contract:

```bash
codex plugin marketplace add ./
codex plugin add workbay-system@workbay-marketplace
```

#### Codex Delivery Proof

Codex docs checked on 2026-05-22 and rechecked for the internal `.mcp.json`
shape fix on 2026-05-30:

- https://developers.openai.com/codex/plugins/build — repo-scoped marketplaces live at `$REPO_ROOT/.agents/plugins/marketplace.json`; local entries use `source: local` with a `./`-prefixed `source.path` relative to the marketplace root; `codex plugin marketplace add <source>` registers a local marketplace; `codex plugin list --marketplace <name>` shows available plugins; `codex plugin add <plugin>@<marketplace>` installs into the Codex plugin cache. Bootstrap does not use that cache path by default; it writes repo-local activation into `.codex/config.toml`.

Sandbox transcript summary, using `codex-cli` 0.131.0 with a `mktemp -d` project and disposable `HOME`:

```bash
codex plugin marketplace add ./
codex plugin marketplace list
codex plugin list --marketplace workbay-marketplace
codex plugin add workbay-system@workbay-marketplace
codex debug prompt-input
codex exec --skip-git-repo-check --ephemeral --dangerously-bypass-approvals-and-sandbox "Use workbay-handoff-mcp/load_session once."
```

Codex delivery proof result: pass. The marketplace registered from the isolated repo root, `codex plugin marketplace list` showed `workbay-marketplace`, `codex plugin list --marketplace workbay-marketplace` showed `workbay-system@workbay-marketplace` available, and `codex plugin add workbay-system@workbay-marketplace` installed the plugin into `~/.codex/plugins/cache/workbay-marketplace/workbay-system/<plugin_version>/`. The installed cache contained `.codex-plugin/plugin.json`, `.mcp.json` (pre-plan-0028 tree; post-0028 trees are skills-only and Codex MCP registration moves to `.codex/config.toml`), and the `skills/` directory. Codex may print non-blocking remote-plugin or icon warnings in disposable homes; plugin marketplace discovery and install still completed successfully.

internal loader-shape follow-up result: pass for plugin parsing and tool
resolution. The installed Codex `.mcp.json` was a bare server map,
`codex debug prompt-input` listed the `workbay-system:*` plugin skills, and
`codex exec` no longer emitted the prior `invalid transport` warning that
occurred when the file used a `mcp_servers` wrapper. A disposable
`CODEX_HOME` seeded with existing auth completed a noninteractive
`workbay-handoff-mcp/load_session` call; read-only non-bypass mode registered
the server but cancelled the MCP call before return, so the final proof used
Codex's explicit noninteractive bypass flag in the disposable home.

Repo-local activation proof: `workbay-bootstrap install` now writes the same
marketplace source and enabled plugin selector into `.codex/config.toml` with
`source = "."`, preserving unrelated Codex config and an explicit local
`enabled = false` override. This is the preferred bootstrap path because it is
project-scoped and avoids user-global config/cache writes.

### Grok

1. Run `make dogfood` (or `workbay-bootstrap install`) from the consumer
   repo so bootstrap composes the effective plugin tree, materializes
   `.grok/plugins/workbay-system/` from
   `.workbay/generated/plugins/workbay-system/effective/grok/`, and
   **activates** the plugin via the Grok CLI when `grok` is on `PATH`:
   `grok plugin install .grok/plugins/workbay-system --trust` followed by
   `grok plugin enable workbay-system` (bare-name selector only — bootstrap
   never writes unstable `project/<hash>/workbay-system` selectors).
   When `grok` is absent the install still succeeds and records a
   `configs[]` entry with `kind: grok_plugin_activation` and
   `action: skipped_no_cli` in `.workbay-bootstrap.json`;
   `workbay-bootstrap doctor` surfaces the gap with remediation text.
   The materialized tree is **not** committed — Grok has no marketplace
   indirection, so the full tree is regenerated on every install/update
   and is gitignored like the other generated surfaces (the managed
   `.gitignore` block covers `/.grok/plugins/workbay-system`).
2. Restart Grok from the consumer repo root so project-scoped plugin
   discovery reloads.
3. Run `/hooks-trust` in the Grok session so plugin-delivered PreToolUse
   guards are trusted. MCP servers come from repo-root `.mcp.json` (compat
   load) — `registration: root` in `mcp_servers.yaml` (implementation note; formerly
   a grok-only special case) keeps `mcpServers` out of the plugin manifest
   so `grok inspect` lists each server exactly once.
4. Optional: opt in to the managed Stop adapter with
   `make dogfood DOGFOOD_INSTALL_FLAGS=--install-grok-stop-hook` (or pass
   the same flag to `workbay-bootstrap install`). Bootstrap writes
   `.grok/hooks/stop.json` and records the opt-in in
   `.workbay-bootstrap.json`.

#### Grok worktree adoption

Linked worktrees share the primary's `.workbay/generated` symlink but not
gitignored plugin surfaces. `workbay-bootstrap adopt` (also run by
`make task-start`) symlinks `.grok/plugins/workbay-system` to the
worktree's effective grok tree and runs the same activation seam. `adopt
--check` reports drift when that surface is missing or stale.

A worktree's checked-out copy of the git-tracked `.workbay-bootstrap.json`
carries **primary-scoped** install claims. The per-invocation adopt receipt
(`configs` / `surfaces` on the adopt return dict) is the worktree-local truth
surface for grok materialization and activation outcomes.

#### Grok Acceptance Checklist

```bash
make dogfood
grok inspect
grok plugin validate .workbay/generated/plugins/workbay-system/base/grok
workbay-bootstrap doctor
```

Expected after restart + `/hooks-trust`:

- `grok inspect` lists project plugin `workbay-system` **enabled** with
  eleven portable-command skills and two MCP servers (each server listed once
  — no compat + plugin double registration).
- Slash autocomplete includes `/plan-analyze`, `/review-parallel`, `/tdd`,
  and every other `portable_commands.json` `command_id`.
- PreToolUse guards from the plugin `hooks/hooks.json` fire once per
  matching tool call (main-branch bash, worktree drift, MCP param
  validation, rationale size).
- `workbay-bootstrap doctor` reports no `grok_activation_drift` on a
  healthy install; stale `project/<hash>/workbay-system` selectors in
  `~/.grok/config.toml` surface as warnings with remediation text.

### Smoke-Only Fallback: Local Plugin Directory

Claude's historical local-tree install form is still useful for one-off
smoke tests, but it is **smoke-only** — it does not produce a committed,
reproducible install:

```bash
# Smoke-test only; not part of the consumer-install contract.
claude --plugin-dir path/to/repo/.workbay/generated/plugins/workbay-system/base/claude /skills
```

Use this form to verify a freshly generated Claude plugin tree before
committing a pin update. Current Codex CLI builds no longer expose a
top-level `--plugin-dir`; Codex smoke tests should use the repo-local
marketplace/config path above or the user-global CLI fallback in a
disposable `HOME`.

### Consumer Overrides

internal adds one explicit customization path for consumers: put
repo-owned overrides under `workbay-overrides/workbay-system/` and let
bootstrap or the composition flow generate the effective plugin tree at
`.workbay/generated/plugins/workbay-system/effective/{claude,codex}/`.

**Consumer recipe overrides vs personal skills.** The override root is for
durable, repo-owned edits to *shipped* workbay recipes — review
checklists, slice conventions, repo-specific reviewer guidance — that must
survive upstream updates and travel with the consumer repo (it is tracked
source). A skill that is not a customization of shipped behavior — a
personal helper, a one-off experiment, a team-private workflow — belongs in
the harness's own personal-skill location (e.g. `.claude/skills/`), not in
the override root. Rule of thumb: if deleting upstream would orphan it, it
is a personal skill; if upstream updates should merge into it, it is a
recipe override.

Operator rules:

- do not edit `.workbay/generated/plugins/workbay-system/base/...`,
  harness plugin caches, or
  `.workbay/generated/plugins/workbay-system/effective/...` by hand.
  Those are generated outputs and will be replaced on the next
  install/update/repair cycle.
- Do not rely on undeclared same-name shadowing. The override root is
  the supported contract.
- Normal install/update preserves override files. Destructive cleanup
  must use the explicit `--reset-overrides` flow instead of a generic
  "clean install" shortcut.

The generated effective tree is allowed to stay gitignored because it is
bootstrap-managed output, not source. The tracked source of truth is the
override root plus its lock/provenance files.

The override root carries tracked inputs such as `overrides.yaml`,
`overrides.lock.json`, `skills/<slug>/SKILL.md`, and structured MCP patch
files under `tools/`. The generated effective tree emits its own
`plugin-lock.json` receipt so operators can distinguish tracked override
intent from generated output provenance.

#### Walkthrough: replace a shipped skill

1. Create the override file under
   `workbay-overrides/workbay-system/skills/<slug>/SKILL.md`.
2. Declare it in `workbay-overrides/workbay-system/overrides.yaml` with
   `mode: replace`, the relative file path, and the recorded
   `upstream_digest`.
3. Run:

   ```bash
   workbay-bootstrap install --plugin-overrides workbay-overrides/workbay-system
   ```

4. Confirm the generated body now lives under
   `.workbay/generated/plugins/workbay-system/effective/{claude,codex}/skills/<slug>/SKILL.md`
   and that `overrides.lock.json` records the tracked replacement.

#### Walkthrough: patch a shipped skill (fork → edit → update → conflict → accept-upstream)

`mode: patch` keeps consumer edits AND upstream improvements via a 3-way
`git merge-file` merge at composition time. This is the recommended mode
for recipe customizations that should track upstream.

1. **Fork.** Copy the current effective skill body twice into the override
   root:

   ```bash
   SLUG=branch-review
   SRC=.workbay/generated/plugins/workbay-system/effective/claude/skills/$SLUG/SKILL.md
   DST=workbay-overrides/workbay-system/skills/$SLUG
   mkdir -p "$DST"
   cp "$SRC" "$DST/SKILL.base.md"   # fork-time upstream snapshot (merge base)
   cp "$SRC" "$DST/SKILL.md"        # your editable copy
   ```

2. **Edit** `SKILL.md` with the repo-specific change. Declare the component
   in `overrides.yaml` with `mode: patch`, both paths, and the fork-time
   digest:

   ```yaml
   components:
     skills:
       branch-review:
         mode: patch
         path: skills/branch-review/SKILL.md
         base_path: skills/branch-review/SKILL.base.md
         upstream_digest: sha256:<digest of SKILL.base.md>
         on_upstream_change: warn
   ```

3. **Update.** Re-run install/update/repair. Composition merges
   (consumer edit) vs (fork base) vs (current upstream); a clean merge
   carries both your edit and any upstream change into the effective tree.
   `workbay-bootstrap overrides status --target .` shows
   `mode=patch status=clean`.

4. **Conflict.** When upstream rewrites the same lines you edited, the
   merge conflicts: the effective tree falls back to your consumer copy
   verbatim, `plugin-lock.json` records `status=merge_conflict`, doctor
   reports `override_merge_conflict`, and `--check` exits non-zero. The
   conflict is visible, never silent.

5. **Accept upstream.** To re-baseline after reviewing the upstream change:

   ```bash
   workbay-bootstrap overrides accept-upstream --target . branch-review
   ```

   This refreshes `SKILL.base.md` and the recorded `upstream_digest` to
   current upstream (refusing on a dirty override root unless `--force`),
   leaving your `SKILL.md` for you to reconcile; re-run update to
   recompose.

#### Walkthrough: add a repo-specific skill

1. Create the skill source under
   `workbay-overrides/workbay-system/skills/<slug>/SKILL.md` with the
   normal `SKILL.md` frontmatter, for example `name` and `description`,
   followed by the skill body.
2. Declare it in `workbay-overrides/workbay-system/overrides.yaml` with
   `mode: add` and the relative file path. Added skills are repo-owned;
   they do not need an upstream digest.
3. Run:

   ```bash
   workbay-bootstrap install --plugin-overrides workbay-overrides/workbay-system
   ```

4. Confirm the generated body now lives under
   `.workbay/generated/plugins/workbay-system/effective/{claude,codex}/skills/<slug>/SKILL.md`
   and that `plugin-lock.json` records the component with `mode: add`.

#### Walkthrough: disable a shipped skill

Set the component entry in `overrides.yaml` to `mode: disable`, then run:

```bash
workbay-bootstrap update --plugin-overrides workbay-overrides/workbay-system
```

The next effective tree omits that skill while the generated base tree
stays untouched.

#### Walkthrough: patch MCP server args

> **Requires a plugin-owned MCP surface.** MCP-server overrides only take
> effect through an emitted plugin `.mcp.json`, i.e. when at least one
> harness in `mcp_servers.yaml` declares `registration: plugin`. Under the
> shipped all-root ownership table the generator refuses
> `components.mcp_servers` overrides (the composed servers would be locked
> but never consumed); change launch specs in the canonical
> `mcp_servers.yaml` + `make mcp-pins-sync` instead.

1. Add a patch file such as
   `workbay-overrides/workbay-system/tools/mcp_servers.patch.yaml`.
2. Declare the server under `components.mcp_servers` in `overrides.yaml`
   with `mode: patch`, `patch_path`, and `requires_trust_ack: true`.
3. Run install or update with `--plugin-overrides`.
4. Review `overrides.lock.json`, generated `plugin-lock.json`, and
   `workbay-bootstrap doctor` output to confirm the command, args, or env
   mutation was recorded explicitly.

#### Walkthrough: update upstream and inspect a stale override

After pulling a newer upstream plugin version, run:

```bash
workbay-bootstrap update --plugin-overrides workbay-overrides/workbay-system
workbay-bootstrap doctor
```

If the canonical base digest changed beneath a warn-mode replacement,
`workbay-bootstrap doctor` reports `stale_override` and leaves the local override
in place. Review the upstream skill or MCP change, update the override if
needed, then rerun update so the new effective tree and receipts reflect
the reconciled state.

Fresh clones with override-aware marketplace pins should also use
`workbay-bootstrap doctor` when the effective tree has not yet
been materialized; that path reports the missing generated target and the
install/update remediation instead of silently failing.

#### Walkthrough: reset overrides safely

Normal install/update never removes consumer-owned override files. The
only destructive path is the explicit reset flow:

```bash
workbay-bootstrap update \
  --plugin-overrides workbay-overrides/workbay-system \
  --reset-overrides \
  --backup
```

`--reset-overrides` refuses to run on a dirty worktree unless `--backup`
is also supplied. With backup enabled, bootstrap archives the override
root under `.workbay/override-backups/<timestamp>/` before removal,
recomposes the base-only effective tree, and prints the backup path.

## Pin Updates

When a shipped plugin surface changes, including the MCP launch wiring, skill
bodies, generated prompts, plugin metadata, or MCP server maps:

1. Update the changed canonical inputs in
   `packages/workbay-system/config/agent-workflows/mcp_servers.yaml`. Under
   git-only delivery (internal) the MCP `args` are the fixed shim form
   `["scripts/hooks/mcp_launch.py", "<server>"]` and carry **no** version pin, so
   editing `args` is for shim-command / server-map changes only — **not** runtime
   version bumps. The MCP **runtime version** is selected by the git tag / remote
   ref (`uv tool install …@<ref>` driven by `workbay-bootstrap install
   --remote-ref <ref>`), not by `mcp_servers.yaml`.
2. Bump `plugin_version` in the same file (it flows into both
   `plugin.json` files via the generator). This is the cache-busting key Codex
   uses under `~/.codex/plugins/cache/.../<plugin_version>/`; changing a skill
   body without bumping the plugin version can leave consumers running stale
   skill instructions even though the source tree is fixed.
3. Run `make plugins-build && make plugins-check` to refresh the tree.
4. When the shim command or server map changed, run `make mcp-pins-sync` to
   regenerate `workbay-bootstrap`'s `DEFAULT_MCP_SERVERS` from `mcp_servers.yaml`
   (`make check-mcp-pins` guards the copy), then refresh the
   developer-local `.mcp.json` (gitignored) via `workbay-bootstrap
   mcp-sync`. `make mcp-pins-sync` keeps the shim-command / `DEFAULT_MCP_SERVERS`
   parity — it does **not** bump the MCP runtime version (that is the git tag /
   remote ref in step 1).

The bootstrap install path remains the source-of-truth for the live
developer config; the plugin distribution path is the source-of-truth
for what consumers receive when they install the plugin.

> **Note on developer-local MCP duplication.** The repo-root `.mcp.json`
> is gitignored (`.gitignore:51`) — it is the maintainer's live config,
> not part of the consumer-install contract. When the plugin is also
> installed via the pin, `claude doctor` will report the plugin's
> `mcpServers` as "skipped because identical servers are already
> configured elsewhere." That elsewhere is your local `.mcp.json`,
> taking precedence over the plugin tree. Consumers who install only
> via the pin see the plugin's MCP block as the sole source.

## Related

- `config/agent-workflows/mcp_servers.yaml`
- `scripts/generate_agent_workflows.py`
- `Makefile.d/plugins.mk`
