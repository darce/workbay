# workbay-bootstrap

Pip-installable CLI that hoists the shared workbay-system surface
(typed protocol, two MCP servers, hooks, skills, generated agent
workflows) into consumer repositories. Lives inside
[`darce/workbay`](https://github.com/darce/workbay).

Consumers run `workbay-bootstrap install --target <path>` once; it
clones the monorepo, materializes the overlay, and registers both
managed MCP servers (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`)
across `.mcp.json`, `.vscode/mcp.json`, and `.codex/config.toml`. No
hand-edits required.

## Install

Git-only delivery: `workbay-bootstrap` is installed from the public git mirror
[`darce/workbay`](https://github.com/darce/workbay), not PyPI. Pin a released
monorepo tag for reproducibility.

### From the git mirror (consumers)

One-shot (no install — fetches each invocation):

```bash
REF=v0.1.35  # a released tag from https://github.com/darce/workbay/tags
R="git+https://github.com/darce/workbay.git@$REF"
# --no-sources is required (the pyproject carries a workspace source); the deps
# (workbay-protocol + workbay-system) are git-sourced via --with, never PyPI.
uvx --no-sources \
    --with "$R#subdirectory=packages/workbay-protocol" \
    --with "$R#subdirectory=packages/workbay-system" \
    --from "$R#subdirectory=packages/workbay-bootstrap" \
    workbay-bootstrap install \
    --target /path/to/your/repo
```

Persistent (installs `workbay-bootstrap` onto `$PATH`):

```bash
REF=v0.1.35
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay-bootstrap" \
  workbay-bootstrap
# then:
workbay-bootstrap status --target /path/to/your/repo
workbay-bootstrap doctor --target /path/to/your/repo
# upgrade later:
uv tool upgrade workbay-bootstrap
```

### From the monorepo source tree (development)

```bash
cd packages/workbay-bootstrap
python -m pip install -e ".[dev]"
```

> **Hardlink warning on first install?** If you see
> `Failed to hardlink files; falling back to full copy`, your `uv`
> cache and tool dir live on different filesystems. The install still
> succeeds; silence the warning with `export UV_LINK_MODE=copy` in
> your shell profile.


### Embedding model auto-provision (C1)

By default, `install` and `repair` provision the pinned `gte-base-en-v1.5` int8
ONNX model: SHA-256 verified download into `~/.cache/workbay/models/<digest>/`,
then `.workbay/embedding.env` (artifact paths + digests +
`WORKBAY_REINJECT_SEMANTIC=1`). Hooks and the MCP launcher load that file
set-if-unset. Interactive `install` / `repair` prompt **Enable semantic embeddings? [Y/n]** (default yes); `--no-embeddings` or a **no** answer opts out. Non-interactive runs default to enabled unless `--no-embeddings` is set.

Post-install, use `workbay embeddings --status|--enable|--disable` from the worktree root as the SSOT toggle (implicit cwd; idempotent writes to `.workbay/embedding.env`). `workbay-bootstrap embeddings --target <path> …` remains a deprecated alias. Harnesses also ship `/workbay` (Claude -> `AskUserQuestion` menu; other harnesses -> `/workbay embeddings <on|off|status>`).

Pass `--no-embeddings` or set `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED=1` to skip provisioning. `doctor` reports `embedding_artifact_*` delivery-state.

Semantic reinjection readiness has **two halves**: (1) model
artifacts — `workbay-bootstrap provision-embeddings --target <repo>` downloads
the pinned model into the shared cache and writes `.workbay/embedding.env` for
the exact `.venv` interpreter the SessionStart reinject hook heals into; and
(2) the runtime extra — `uv pip install 'mcp-workbay-handoff[embeddings]'` (or
`uv sync --extra embeddings`) into that same `.venv`, which provisioning does
NOT install (deliberately opt-in; never added to the hook's required deps).
With `WORKBAY_REINJECT_SEMANTIC=1` armed, `doctor` (embedding-state on) emits a
`reinject_readiness_unavailable` warning naming whichever half is missing, and
an info finding when semantic is armed but the hook is not wired.

## Subcommands

```text
workbay install --target <path> [--with-codebase-graph]   # front door (delegates here)
workbay-bootstrap install --target <path> [--remote-ref <tag>] [--mcp-servers <default|path>] [--no-mcp-servers] [--with-codebase-graph] [--no-embeddings]
workbay-bootstrap update  --target <path> --remote-ref <tag>
workbay-bootstrap status  --target <path>
workbay-bootstrap clean     --target <path> [--dry-run] [--yes]
workbay-bootstrap gc        --target <path> [--dry-run] [--yes]
workbay-bootstrap doctor  --target <path> [--mcp-servers <default|path>]
workbay-bootstrap provision-embeddings --target <path>
workbay embeddings --status|--enable|--disable   # preferred (cwd = worktree root)
workbay-bootstrap embeddings --target <path> --status|--enable|--disable  # deprecated alias
workbay-bootstrap repair  --target <path> [--force-dirty] [--no-embeddings] [--mcp-servers <default|path>]
workbay-bootstrap overrides status --target <path> [--plugin-overrides <path>] [--json]
workbay-bootstrap overrides relock  --target <path> [--plugin-overrides <path>] [--force]
workbay-bootstrap adopt-worktree [--target <linked-worktree>] [--primary <root>] [--check] [--json]
```

- `install`: Clone the monorepo, materialize SHARED + GENERATED
  surfaces, write the three MCP-config files, run `init-state` to
  provision `<target>/.task-state/handoff.db` (skipped under
  `--no-mcp-servers`), set `core.hooksPath`, and write the overlay
  manifest. Pass `--with-codebase-graph` (or `workbay install
  --with-codebase-graph`) to also materialize the optional
  `codebase-graph-mcp` server.
- `update`: Re-run install at a new `--remote-ref`; refresh GENERATED
  surfaces and, optionally, configs.
- `status`: Print a summary of the installed overlay manifest. When
  the install registered MCP servers, also reports the resolved
  `state_dir` / `db_path` / `exports_dir` / `schema_version` via
  `init-state --check`.
- `doctor`: Detect drift in SHARED, GENERATED, config, and
  initialized-state surfaces. Flags missing `.task-state/handoff.db`
  as `state_drift` only when the manifest recorded `.mcp.json`. Exit
  `1` when drift exists.
- `repair`: Restore drifted surfaces flagged by `doctor`. For an
  unadopted linked worktree this routes to `adopt-worktree` (below).
- `adopt-worktree`: Materialize the overlay into a **linked git
  worktree** by redirecting its surfaces at the primary's
  `.workbay/remote` clone (one hop, relative links). `--target`
  defaults to the current directory; the primary is resolved by the
  `.workbay-bootstrap.json` marker unless `--primary` is given.
  `--check` reports drift without writing and exits `1` when the
  worktree is unadopted. A no-op on the primary worktree.
- `clean` / `gc`: Confirm-then-act reclaim of **non-load-bearing**
  overlay debris (orphaned clone directories, stale package trees).
  Default is dry-run (`--dry-run`); pass `--yes` to apply removals.
  Paths still resolved through live overlay symlinks are refused.
  `gc` is an alias for `clean`.
- `overrides status`: Summarize plugin override components from
  `overrides.yaml` / `overrides.lock.json` (skills, MCP servers,
  portable commands, **rules**, **guides**).
- `overrides relock`: Recompute upstream digests in `overrides.yaml`
  from on-disk patch bases and the generated base tree, then refresh
  `overrides.lock.json`. Use after editing patch bases or upgrading
  the installed remote ref; `--force` bypasses a dirty override root.

### Plugin override component kinds

`overrides.yaml` declares digest-pinned passthrough components. Besides
`skill`, `mcp_server`, and `portable_command`, bootstrap accepts
`rules` and `guides` entries under `components.rules` /
`components.guides`. Tracked consumer enrichments live under
`workbay-overrides/workbay-system/rules/` and
`.../guides/` (or the manifest-recorded `--plugin-overrides` root).
Re-materialization via `install` / `update` / `repair` preserves those
files; `overrides relock` keeps lock digests aligned with the installed
base.

### Linked worktrees

A linked worktree (`git worktree add`, or an IDE/agent auto-worktree)
shares the primary's `.git` but **not** gitignored files, so the
overlay starts absent — the plugin is enabled (tracked
`.claude/settings.json`) but unresolvable. Self-heal works as follows:

- **`make task-start`** (the supported flow) adopts the overlay into the
  new worktree automatically, so it works out of the box. In source
  checkouts, the lifecycle uses the freshly provisioned worktree `.venv`
  `workbay-bootstrap` command when available, then falls back to `uvx`.
  Set `WORKBAY_ADOPT_CMD=""` to disable auto-adopt, or set it to a custom
  command to override that default.
- **Post-provision bootstrap** — after adopt, `make task-start` can also run
  a consumer-declared shell command (for example `npm install`) via
  `LIFECYCLE_WORKTREE_BOOTSTRAP` in the root `Makefile`. Best-effort,
  worktree-rooted, `sh -c` semantics; see the development-workflow rule doc.
- **Raw `git worktree add` / auto-worktrees** are healed on demand:

  ```bash
  # adopt-worktree heals a raw `git worktree add` in a repo that already carries
  # the overlay, so `workbay-bootstrap` is already on PATH — call it directly
  # (a bare uvx of the dist name would resolve from PyPI, which does not exist).
  workbay-bootstrap adopt-worktree --target <worktree>
  # or, as a steady-state guard (exit 1 on drift):
  workbay-bootstrap adopt-worktree --target <worktree> --check
  ```

`.task-state/`, `DASHBOARD.txt`, and `CURRENT_TASK.json` are **never**
adopted — they stay per-worktree (the handoff DB is primary-rooted).

See [`docs/CONSUMER.md`](https://github.com/darce/workbay/blob/main/docs/CONSUMER.md)
for the consumer-facing walkthrough (upgrade, drift handling, skill
overrides, the `current_task_auto_regen` migration note).

## Surfaces written by `install`

The canonical source of truth for bootstrap-managed surfaces is the
installer implementation in
`src/workbay_bootstrap/install.py` (`SHARED_SURFACES` and
`GENERATED_SURFACES`). Keep this table aligned with those constants.

| Surface                              | Source     | Layer       |
| ------------------------------------ | ---------- | ----------- |
| `scripts/hooks/`                     | shared     | symlink     |
| `.github/hooks/`                     | shared     | symlink     |
| `docs/workbay/contracts/`            | shared     | symlink     |
| `docs/workbay/rules/`                | shared     | symlink     |
| `Makefile.d/` non-excluded children  | shared     | carved dir  |
| `scripts/workbay/` non-excluded children | shared | carved dir  |
| `.github/prompts/`                   | generated  | real dir    |
| `.workbay/generated/plugins/workbay-system/base/` | generated | real dir |
| `.workbay/generated/plugins/workbay-system/effective/` | generated | real dir |
| `.mcp.json`                          | generated  | real file   |
| `.vscode/mcp.json`                   | generated  | real file   |
| `.codex/config.toml`                 | generated  | real file   |
| `core.hooksPath` git config          | generated  | git config  |
| `.task-state/handoff.db`             | runtime    | sqlite      |
| `.task-state/exports/`               | runtime    | dir         |
| `.workbay/remote/`                   | bootstrap  | git clone   |
| `.workbay-bootstrap.json`              | bootstrap  | manifest    |

`.task-state/` is provisioned by the handoff server's `init-state`
subcommand at install time and is gitignored — each fresh checkout
regenerates it through `workbay-bootstrap install`.

## Defaults

- `--profile` defaults to `all`, which materializes the full surface
  set: generated Copilot prompts, Claude/Codex plugin trees, shared
  overlay surfaces, and the lifecycle hoist
  (`Makefile.d/lifecycle.mk` plus the sentinel-bracketed `-include`
  block in the consumer `Makefile`). Pass `--profile minimal` for a
  clone-only install with no surfaces, or `--profile lifecycle` for
  just the lifecycle runner and Makefile fragment. The active profile
  is recorded in `.workbay-bootstrap.json` under `"profile"`.
- `--remote-url` defaults to `git@github.com:darce/workbay.git`.
- `--remote-ref` defaults to `main` (override with a release tag like `v0.1.0`).
- `--mcp-servers` defaults to the built-in managed map registering
  `mcp-workbay-handoff` and `mcp-workbay-orchestrator` via `uvx` with
  `--workspace-root . serve-stdio`, so Codex, VS Code, and Claude
  clients start real MCP stdio servers from the generated config.
  Pass a JSON file path to override; pass `--no-mcp-servers` to skip
  the three config writers entirely.
- Plugin overrides are auto-discovered at
  `workbay-overrides/workbay-system/` when that root contains an
  `overrides.yaml` manifest. Use `--plugin-overrides <path>` on
  `install`, `update`, `doctor`, or `repair` for a non-default root;
  bootstrap records that path so later update/doctor/repair runs reuse
  it. Override-aware installs generate effective plugin trees under
  `.workbay/generated/plugins/workbay-system/effective/{claude,codex}`
  and point marketplace pins at those generated trees.
- `install` and `update` preserve plugin override files by default.
  `--reset-overrides` is the explicit destructive path; it removes only
  the resolved override root, refuses dirty git worktrees unless
  `--backup` is supplied, and archives backups under
  `.workbay/override-backups/<timestamp>/` before removal.

## Development

Tests live under `tests/`. From the monorepo root:

```bash
cd packages/workbay-bootstrap
PYTHONPATH=.:src:../workbay-protocol/src pytest tests -q
```
