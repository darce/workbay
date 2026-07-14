# Upgrading from the four-repo era to `workbay`

If your target repo previously consumed any of these legacy private
repos directly, this guide walks the cutover:

- `darce/mcp-workbay-handoff`
- `darce/mcp-workbay-orchestrator`
- `darce/workbay-system`
- `darce/workbay-bootstrap`

All four now ship from `darce/workbay` under
`packages/<name>/`. The legacy repos stay reachable until active
consumers migrate; afterwards they will be archived (read-only) with
pinned issues redirecting to the monorepo.

## What changes for consumers

| Before                                                                | After                                                              |
| --------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `workbay-bootstrap install --remote-url darce/workbay-system@vX`      | `workbay-bootstrap install` (default remote = monorepo `main`)     |
| Four separate `vX.Y.Z` tags to track                                  | One canonical `vX.Y.Z` monorepo tag (per-package tags exist alongside) |
| Hand-supply `--mcp-servers <json>` for each install                   | Default registers both managed MCP servers via the `mcp_launch.py` shim |
| Orchestrator pinned a private handoff checkout via `git+ssh://...@v0.4.3` | Orchestrator's `mcp-workbay-handoff` dependency is git-sourced from the same monorepo tag — no private checkout, no PyPI |
| Skill content under `.claude/skills/<slug>/SKILL.md` (Claude-only)    | Neutral source `skills/<slug>/{skill.yaml,body.md}` generates per-agent plugin/prompt surfaces |

## Cutover steps

1. **Update `--remote-url` and `--remote-ref`.** The `workbay-bootstrap`
   `0.3.0` release ships a new default `DEFAULT_REMOTE_URL`
   (`git@github.com:darce/workbay.git`) and
   `DEFAULT_REMOTE_REF` (`main`). If your install was pinned at the
   legacy `darce/workbay-system` URL, repoint it:

   ```bash
   workbay-bootstrap update \
       --target . \
       --remote-url git@github.com:darce/workbay.git \
       --remote-ref v0.1.35
   ```

   Subsequent upgrades only need `--remote-ref`.

2. **Drop hand-supplied `--mcp-servers` files** unless you carry
   non-default servers. The default map registers
   `mcp-workbay-handoff` and `mcp-workbay-orchestrator` via the stdlib
   `scripts/hooks/mcp_launch.py` shim with
   `--workspace-root . serve-stdio`; that is the shipped cross-client
   startup contract for Claude Code, Codex, Cursor, grok, and VS Code
   Copilot. To keep a custom
   map, continue passing `--mcp-servers <path>`, but include an MCP
   transport subcommand yourself. To skip MCP-config writes entirely,
   pass `--no-mcp-servers`.

3. **Re-run `workbay-bootstrap install`** (or `update`) with the new
   remote. Verify with `workbay-bootstrap doctor --target .` — exit
   `0` is clean. Investigate any drift before committing.

4. **Refresh per-agent surfaces.** The neutral skill layout
   (`skills/<slug>/{skill.yaml,body.md}`) regenerates harness-native
   plugin or prompt surfaces for Claude Code, Codex, Cursor, grok, and
   VS Code Copilot on every install. If your target had hand-edited any
   generated output, expect drift on the first
   `doctor` run; re-apply your overrides on top of the regenerated
   content. The `$branch-review` skill guidance is part of this
   regenerated surface and must be consumed from the release tag, not
   copied by hand.

5. **Mind the `current_task_auto_regen` default flip.** As of
   `mcp-workbay-handoff 0.5.0`, the default is **off**. If your tooling
   reads `CURRENT_TASK.json`, opt back in explicitly with
   `WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN=1`.

## Routine upgrades: the coherent upgrade flow

Once you are on the monorepo remote, every subsequent upgrade follows one
documented flow. Do **not** hand-edit managed overlay files or hand-repair
dangling links; the installer owns those writes. Managed and generated
surfaces (the overlay symlinks/copies, `.mcp.json`, the generated agent
workflow adapters, the plugin trees) are *derived artifacts* — rebuild them
from the payload with the commands below, never by hand. Bootstrap itself
writes them during `update`; the `guard-main-branch` block on hand-edits is
correct, and `repair --materialize-managed` is the sanctioned rebuild path.

Run these steps as one sequence per upgrade:

1. **Stash or branch first.** Upgrade a clean tree. If you carry
   uncommitted work, stash it (`git stash`) or upgrade on a throwaway
   branch, then resolve any regenerated-surface drift before restoring —
   generated files always rewrite in full, so a dirty tree hides real
   drift under your own edits.

2. **Update to the new tag.** Only `--remote-ref` normally changes:

   ```bash
   uv run --project packages/workbay-bootstrap \
     workbay-bootstrap update --target . --remote-ref vX.Y.Z
   ```

3. **Member verification is enforced (package mode only) — expect a loud
   failure on skew.** As of `internal`, a **package-mode**
   `install`/`update` (`--source package`, the `workbay install` front door)
   refuses to write a **mixed** package stack: if any installed stack member
   (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`, `workbay-protocol`,
   `workbay-bootstrap`, `workbay-system`) diverges from the `workbay`
   anchor pins, the command exits **non-zero** and names the stale member,
   e.g.:

   ```
   refusing to write a mixed workbay package stack; upgrade/converge the
   workbay package set and re-run `make workbay-update` (or
   `workbay-bootstrap update`).
   - workbay-bootstrap: installed 0.3.6 != stack-required 0.3.8
   ```

   This check does **not** run for a `git_overlay` update (the
   `--remote-ref` command in step 2): the member gate keys off the
   package-mode manifest and is a no-op for git-overlay consumers.

   Converge the package set (reinstall the full `workbay` stack at the
   target tag) and re-run `update`. This is a **behavior change from the
   pre-`internal` contract**, where `update` always exited
   `0` on write success — CI or scripts that call `update` must now expect
   this failure mode on a skewed host. For an *intentional* development
   skew (a local checkout of one member), pass `--allow-member-skew` to
   bypass the check. `doctor` reports the same skew (`stack_drift`) between
   upgrades, so a member bumped outside workbay (e.g. a bare
   `uv tool upgrade`) is still caught on the next run.

4. **Audit MCP tool sources.** `doctor` now records where each git-only
   MCP server was installed from and flags any that came from a **local
   path** instead of the canonical pinned git ref
   (`git+<url>@<tag>#subdirectory=packages/<pkg>`):

   ```bash
   uv run --project packages/workbay-bootstrap \
     workbay-bootstrap doctor --target .
   # warning gitonly_mcp_tool_source: mcp-workbay-handoff was installed from a
   #   local path source '/…/your-repo/packages/mcp-workbay-handoff',
   #   not a pinned git ref; reinstall from the pinned ref: uv tool install …
   ```

   The finding prints the exact `uv tool install --no-sources --force
   --from git+…` command; reinstall from the pinned ref so the tool is
   reproducible.

5. **Repair dangling managed links.** A tracked symlink left pointing at a
   legacy `.workstate/remote/` or a now-absent `.workbay/remote/` path (a
   half-migrated overlay pointer) shows up as `managed_link_dangling` in
   `doctor`. Materialize the real payload files with the installer-owned
   verb — it backs up and replaces only managed paths and **skips
   `source=local` overrides**, so your intentional local files are left
   untouched:

   ```bash
   uv run --project packages/workbay-bootstrap \
     workbay-bootstrap repair --target . --materialize-managed
   ```

   Re-run `doctor` to confirm exit `0`. `repair --materialize-managed` is
   idempotent — a second run is a no-op on byte-identical files.

## Pin the cross-package versions

WorkBay ships from the git mirror only — there are no PyPI releases to pin
against. The `workbay-bootstrap` install pins the monorepo tag (`--remote-ref`)
end-to-end, so you normally do **not** enumerate package versions individually.

If your own project depends on a member directly (for example the typed
contracts), git-source it at the same monorepo tag rather than a PyPI version
range:

```toml
# pyproject.toml — git-sourced, pinned to the monorepo tag
[tool.uv.sources]
workbay-protocol = { git = "https://github.com/darce/workbay.git", tag = "v0.1.35", subdirectory = "packages/workbay-protocol" }
```

## Worktree bootstrap hook (`LIFECYCLE_WORKTREE_BOOTSTRAP`)

As of `workbay-system` implementation note, consumers can declare a
post-provision shell command that `make task-start` runs automatically
after Python sync and overlay adopt — for example `npm install` for a
Node-backed app directory. Set it in your root `Makefile`:

```makefile
LIFECYCLE_WORKTREE_BOOTSTRAP = cd apps/my-app && npm install
```

The recipe forwards this to `WORKBAY_WORKTREE_BOOTSTRAP_CMD` for the
lifecycle runner. Empty default = no-op. Bootstrap failures are
best-effort (they do not roll back the worktree). See
`packages/workbay-system/workbay_system/payload/docs/workbay/rules/development-workflow.md`
(§ Post-provision worktree bootstrap) for semantics, timeout, and trigger
coverage. Requires an overlay refresh of `.workbay/remote` after the
payload release lands.

## Self-host worktree surface bootstrap (implementation note)

The workbay monorepo **self-hosts** the overlay: it ships the in-tree
payload (`packages/workbay-system/workbay_system/payload`) and has a
tracked `.workbay-bootstrap.json` marker but **no** `.workbay/remote`
clone. Consumer `adopt-worktree` is therefore skipped
(`no_overlay_clone`), so as of `workbay-system` implementation note the lifecycle
runner instead emits the generated agent surfaces **locally** into the
new worktree — the base + effective plugin trees, root
`.github/prompts`, and Cursor/grok native wiring — via `workbay-bootstrap
bootstrap-surfaces`. This runs automatically in `make task-start`
(worktree and claim modes) after overlay adopt and before the optional
`LIFECYCLE_WORKTREE_BOOTSTRAP` hook above.

- Best-effort: a generator failure, timeout, or missing bootstrap is
  non-fatal — the worktree stays healable. Tune the timeout with
  `WORKBAY_BOOTSTRAP_SURFACES_TIMEOUT` (default 300s); override the
  command with `WORKBAY_BOOTSTRAP_SURFACES_CMD`.
- Doctor/repair: `workbay-bootstrap doctor` flags a starved self-host
  worktree as `selfhost_worktree_missing_surfaces`, and `repair` heals it
  by re-running `bootstrap-surfaces` (local emission), not
  `adopt-worktree`.
- Manual `make plugins-build` + `make generate-agent-workflows
  WORKFLOW_TARGET_ROOT="$PWD"` remains the operator fallback for a
  worktree created outside `make task-start`.

## Removing legacy pins

After the migration is complete, search the target repo for any
remaining references to the four legacy remotes and remove them:

```bash
rg --hidden 'darce/(mcp-workbay-handoff|mcp-workbay-orchestrator|workbay-system|workbay-bootstrap)\.git'
```

Once these are clean and `workbay-bootstrap doctor --target .` is
green, the upgrade is complete.
