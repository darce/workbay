# workbay

WorkBay gives stateless coding agents a durable, shared workflow state.
Task lifecycle, slice decisions, review findings, test evidence, and
blockers live in a SQLite database on your machine, exposed over MCP, so
a session that ends in Claude Code can be picked up by Codex, Cursor,
grok, or VS Code Copilot without losing the thread. Git hooks enforce
the workflow at the repository layer, where an agent cannot talk its way
around them.

A coding agent starts every session cold: it does not remember what it
shipped yesterday, who reviewed it, or why a previous attempt was
rejected. Most memory tools answer this by saving conversation
summaries. WorkBay instead persists the workflow itself — the active
task, its branch and worktree, the slices that closed and at which
commit, and the review findings still open against it.

## Quick start

Register the public plugin marketplace (Claude Code — one-time per
checkout), then install the overlay from a tagged `darce/workbay` ref — no
PyPI:

```text
/plugin marketplace add darce/workbay
```

```bash
uv tool install "git+https://github.com/darce/workbay@v0.2.1#subdirectory=packages/workbay-bootstrap"
workbay-bootstrap install --target /path/to/your/repo --remote-ref v0.2.1
```

One install materializes skill and hook surfaces, registers the two MCP
servers (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`) via the
stdlib `mcp_launch.py` shim, provisions `.task-state/handoff.db`, and
sets `core.hooksPath` so enforcement hooks fire throughout the git
lifecycle. Restart your agent so it picks up the new surfaces, then:

```bash
make context        # reload active task state at any point
make task-start TASK=PROJ-1 OBJECTIVE="add rate limiting"
```

or start from a vague idea inside the agent session:

```text
/scope  we should probably add rate limiting somewhere
```

`workbay doctor --target .` detects drift after upgrades; `status`,
`update`, and `repair` round out the lifecycle. See
[`docs/CONSUMER.md`](docs/CONSUMER.md) for the upgrade workflow and for
overriding MCP servers or skills.

### Developer / pre-release install

Track an unreleased `git` ref or hack on the installer from a monorepo
checkout:

```bash
uv tool install "git+https://github.com/darce/workbay@main#subdirectory=packages/workbay-bootstrap"
workbay-bootstrap install --target /path/to/your/repo --remote-ref main
```

This is the clone-backed git-overlay flow (`--source git_overlay` is the
default). See [`docs/CONSUMER.md`](docs/CONSUMER.md) for upgrades and
[`docs/RELEASING.md`](docs/RELEASING.md) for cutting a release.

## The command surface

Eleven portable commands cover the lifecycle. Each resolves to the same
skill on every supported agent, so the workflow does not change when the
agent does:

```text
DEFINE        PLAN               BUILD                         VERIFY            SHIP
/scope        /plan-analyze      /branch-lifecycle             /branch-review    /branch-lifecycle
              /planning-review   /tdd                          /review-parallel    (task-finish)
                                 /incremental-implementation   /auto-fix
                                 /investigate
```

| Command | What it does | Use when |
| --- | --- | --- |
| `/scope` | Turns "we should probably add X" into a scoped, written plan | An idea is still vague |
| `/plan-analyze` | Triage pass over a draft plan for ambiguity and gaps | Before formal review |
| `/planning-review` | Formal plan review with verdicts and persisted findings | A plan needs sign-off |
| `/branch-lifecycle` | Opens, advances, and finishes the task branch and worktree | Starting or closing a task |
| `/tdd` | First failing test for a slice, then drive to green | Starting a slice test-first |
| `/incremental-implementation` | Slice-by-slice implementation with recorded close decisions | Working through an approved plan |
| `/investigate` | Root-cause a defect and preserve the investigation trail | A bug needs diagnosis before a fix |
| `/auto-fix` | Bounded test-driven fix loop | A failing test has a known scope |
| `/branch-review` | Pre-merge review; findings persist in handoff state | A branch claims to be done |
| `/review-parallel` | N independent reviewers over the same diff | One reviewer pass is not enough |
| `/handoff-lifecycle` | Resume, switch, or end a session against stored state | Picking work back up |

A typical task, end to end:

```text
/scope             "we should probably add X"
/plan-analyze      triage the draft plan
/planning-review   formal review, findings recorded
make task-start    feature branch + linked worktree
/tdd               first failing test for implementation note
/incremental-implementation   drive slices to green
/branch-review     pre-merge review, findings persisted
make task-finish   close, archive, tear down
```

## What persists, exactly

State lives in `.task-state/handoff.db`, a versioned SQLite schema
(currently v14, migrated in place) owned by the handoff MCP server.
The load-bearing tables:

| Table | Holds |
| --- | --- |
| `handoff_state` | Active tasks: objective, status, branch, worktree, plan path |
| `decisions` | Recorded decisions, stamped with branch, commit SHA, and session |
| `review_findings` | Findings with severity, status, and two-anchor provenance (the commit that fixed it on-branch, the commit that integrated it) |
| `review_runs` | Structured review records with verdict semantics |
| `verified_tests` | Test results with commands and exit codes |
| `touched_files` | Per-slice file-touch ledger |
| `blockers`, `next_actions` | Open blockers and prioritized follow-ups |
| `task_archives` | Snapshots of completed tasks |
| `session_compactions`, `session_reinjections` | Compaction and context re-feed receipts for long-running sessions |

Everything is full-text searchable (`search_handoff`), renderable as a
human dashboard (`DASHBOARD.txt`) and machine snapshot
(`CURRENT_TASK.json`), and portable as JSON via `export_handoff_state` /
`import_handoff_state`. Findings anchored to commit SHAs mean "this was
fixed" is a claim you can check against git history.

These rows are enforced, not advisory: a `review_findings` row blocks
`make review-ready` until it is closed, and `make handoff-close-check`
refuses to pass if any slice lacks a recorded decision.
[`docs/COMPARISON.md`](docs/COMPARISON.md) maps this against mem0,
Contynu, engram, beads, and the other persistence tools you may already
know.

## One state, any agent

WorkBay builds each harness's surface from one manifest, so the
commands and MCP tools are the same whichever agent you run:

| Harness | Generated surface |
| --- | --- |
| Claude Code | Plugin with skills and hooks |
| Codex | Plugin plus `.codex/config.toml` activation |
| Cursor | `.cursor/skills/` |
| grok | `.grok/plugins/workbay-system/` |
| VS Code Copilot | `.github/prompts/` |

A Claude Code session and a Codex session pointed at the same workspace
read the same task rows, open findings, and dashboard.
Each session opens by calling `load_session`, which returns a ranked
context packet — active task, open findings, recent decisions, touched
files — so the agent resumes from the load-bearing state rather than a
cold prompt, whichever vendor it is.
Switching vendors mid-task costs nothing, which also means no single
vendor's session format owns your project history. The same property
covers a single long session: compaction records and session-start
reinjection hooks can rebuild the working context from MCP instead of
leaning on the transcript summary alone.

## Review artifacts

Different agents can review each other's work:

1. The authoring agent records intent via `set_handoff_state` and
   `record_event`, then opens a branch.
2. A reviewing agent, typically a different model family, runs
   `/plan-analyze`, `/planning-review`, or `/branch-review`. Each is a
   reviewer-side script with explicit verdict semantics.
3. Findings land in `review_findings` with severity, status, and a
   stable id, tied to the task row. They survive the reviewer's session.
4. The authoring agent receives the findings on its next `load_session`
   and must close them before `make review-ready` passes.
5. `make handoff-close-check` runs on the final HEAD as the
   merge-readiness gate.

Because author and reviewer are decoupled through the database, two
adversarial passes from independent models compose without either agent
trusting the other's transcript. For larger work, the orchestrator
server runs several worktree lanes behind worker daemons; lane messaging
and plan cursors coordinate them, and per-turn token metrics give
visibility.

## What the hooks enforce

Skills are suggestions; hooks are not. Installed through `core.hooksPath`
and the harness hook configs, they apply no matter which agent is
running:

- Edits on `main` are refused outside explicitly permitted surfaces.
- Branch names must match the grammar: `feature/<task-ref>` with a
  lowercase, hyphenated, digit-bearing task ref (plus `maint/`,
  `hotfix/`, `release/`, and `revert/` kinds).
- `make review-ready` fails while findings are open.
- Close-check refuses a finish when any slice lacks a recorded decision.
- File touches are recorded per slice for provenance.

## Install integrity

The bootstrap installer writes a ledger at `.workbay-bootstrap.json`
recording where the install came from — its source kind and pinned ref
or package version — alongside what it produced: the generated surfaces,
managed MCP servers, and install steps. `workbay-bootstrap doctor`
checks that ledger against the files on disk; `repair` restores drifted
surfaces, `update` moves the overlay forward, and durable
`workbay-overrides/workbay-system/` entries are composed into the
effective plugin tree instead of being overwritten on the next install.

## Packages

WorkBay ships from one public git mirror — [`darce/workbay`](https://github.com/darce/workbay).
Seven runtime packages are tagged together (`workbay-protocol`,
`mcp-workbay-handoff`, `mcp-workbay-orchestrator`, `workbay-bootstrap`,
`workbay-system`, `workbay-codex-bridge`, `workbay`). Consumers install from
those git tags with `uv tool install` and `workbay-bootstrap install
--remote-ref vX.Y.Z`; PyPI is retired. Cross-cutting changes land
atomically from this monorepo; building straight from source is for
development.

| Package | Role |
| --- | --- |
| `workbay-protocol` | Typed contracts (Pydantic v2 + JSON Schema) |
| `mcp-workbay-handoff` | MCP server: task state, reviews, evidence |
| `mcp-workbay-orchestrator` | MCP server: lanes, workers, dispatch |
| `workbay-bootstrap` | Consumer install/update/doctor CLI |
| `workbay-system` | Shared skills, hooks, generators |
| `workbay-codex-bridge` | Codex subagent backend for the orchestrator |
| `workbay` | Front door: one-command install + runtime version anchor |

```text
workbay/
├── Makefile                  # `make help` lists every target
├── docs/
│   ├── CONSUMER.md           # install, upgrade, drift workflow
│   ├── UPGRADING.md          # standalone-repo era cutover
│   └── RELEASING.md          # maintainer release playbook
└── packages/                 # the eight packages above
```

## Which package do I install?

Install from **`darce/workbay`** at a consumer tag. `uv tool install`
from the `packages/workbay` or `packages/workbay-bootstrap` subdirectory
pulls the git-sourced runtime closure and runs the bootstrap installer —
you do not name component packages one at a time.

| You want to… | Command |
| --- | --- |
| Install or upgrade the overlay in a repo | `workbay-bootstrap install --remote-ref v0.2.1` |
| Front-door wrapper over the same installer | `uv tool install` `packages/workbay` @ tag, then `workbay install` |
| Track `main` or a fork ref | `workbay-bootstrap install --remote-ref <ref>` |

The component trees (`workbay-protocol`, `mcp-workbay-handoff`,
`mcp-workbay-orchestrator`, `workbay-system`, `workbay-codex-bridge`) ship
in the same git mirror; bootstrap installs their MCP binaries once via
`uv tool install` during overlay setup.
[`docs/CONSUMER.md`](docs/CONSUMER.md) walks through install, upgrade,
and drift repair.

## Developing in this repo

Agent surfaces are generated into gitignored paths, so a fresh clone has
the sources but not the built output. Opening the repo in Claude Code
builds everything automatically via a `SessionStart` hook and prints a
one-time restart prompt. From any other entry point:

```bash
workbay-bootstrap install --target .                     # Codex activation + all surfaces
make plugins-build                                         # Claude + Codex + Cursor + grok plugin trees
make generate-agent-workflows WORKFLOW_TARGET_ROOT="$PWD"  # VS Code Copilot prompts
```

Agents read these surfaces only at startup, so restart after the first
build. Release mechanics live in [`docs/RELEASING.md`](docs/RELEASING.md).

## Status

This monorepo is the canonical WorkBay source. The earlier standalone
repositories (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`,
`workbay-system`, `workbay-bootstrap`) remain reachable while
consumers migrate; see [`docs/UPGRADING.md`](docs/UPGRADING.md) for the
cutover.
