# workbay

A coding agent starts every session cold. It does not remember what it
shipped yesterday, who reviewed it, or why the last attempt was rejected.
Most tools answer that by saving conversation summaries. WorkBay saves the
workflow instead: the active task, its branch and worktree, which slices
closed and at which commit, the review findings still open against it, and
the test evidence behind them.

That state lives in a SQLite database in your repo and is exposed over MCP,
so a session that ends in Claude Code can be picked up by Codex, Cursor,
grok, or VS Code Copilot without losing the thread. Git hooks block the
default paths while they are installed and `core.hooksPath` is intact;
unrestricted shell can still disable or skip them.

WorkBay is for git-backed coding agents that install hooks into a repository.
It is not a memory plugin and not an orchestration engine; those neighboring
layers are covered under [Adjacent tool layers](#adjacent-tool-layers).

## What WorkBay enforces that skills alone cannot

Skills are advice a model can decline. These six hold whether or not it
cooperates:

| Claim | Mechanism |
| --- | --- |
| Closed findings are checkable | Every review finding carries two commit anchors: the commit that fixed it on the branch, and the commit that merged it. An agent report that a finding is closed is verifiable against git history. |
| Enforcement survives the agent | Hooks installed through `core.hooksPath` block the default paths while that configuration is intact. Edits on `main` are refused, branch names must match the grammar, and the close gate will not pass while a slice has no recorded decision. A harness you install next month inherits all of it with no reconfiguration. Unrestricted shell can still retarget hooks, use `--no-verify` where applicable, or trip documented env bypasses. |
| No vendor owns project history | The task ledger is harness-neutral; switching model families mid-task still costs tool, auth, and capability re-entry, because the task state was never in the transcript. The same property saves a single long session: compaction receipts and a session-start reinjection hook rebuild working context from the database instead of from a summary of a summary. |
| Author and reviewer are decoupled | A reviewing agent, usually a different model family, writes findings into the database. The authoring agent reads them on its next `load_session` and cannot pass the review gate while open high or medium findings remain; deferred, wontfix, and superseded are allowed exits. Neither one reads the other's transcript; the second pass leaves durable findings and a gate the author must clear. |
| Bookkeeping does not spend tokens | Creating a worktree, closing a slice, running the merge gate, archiving a task: these run as make targets, not model turns. `make wb VERB=start` and its siblings do that work with no model in the loop at all. |
| Cheap work can go to a cheap backend | `/offload` hands one self-contained slice to a bounded lane under a hard token cap, with a fail-fast preflight and no silent substitution of a different backend when the chosen one is unavailable. The lane never merges its own work. |

## Pick a command

Fifteen portable commands are generated from one manifest into harness-native
surfaces; semantics are shared, activation paths differ.

### You have an idea but not a plan

| Command | Use when |
| --- | --- |
| `/scope` | "We should probably add X" and nothing is bounded yet |
| `/plan-analyze` | A draft plan exists; triage it for ambiguity and gaps |
| `/planning-review` | The plan needs formal sign-off with recorded verdicts |

### You are writing code

| Command | Use when |
| --- | --- |
| `/branch-lifecycle` | Opening, advancing, or closing the task branch and worktree |
| `/tdd` | Starting a slice test-first, red before green |
| `/incremental-implementation` | Working through an approved plan slice by slice |
| `/investigate` | A defect needs root-causing before anyone writes a fix |
| `/auto-fix` | A failing test has a known, bounded scope |
| `/refactor` | Auditing code health or sequencing tech-debt work |
| `/offload` | A mechanical slice can go to a cheaper backend under a cost cap |

### You are checking work

| Command | Use when |
| --- | --- |
| `/branch-review` | A branch claims to be done |
| `/review-parallel` | One reviewer pass is not enough for the diff |

### You are managing the session or the install

| Command | Use when |
| --- | --- |
| `/handoff-lifecycle` | Resuming, switching tasks, or ending a session |
| `/workbay` | Turning semantic embeddings on or off, or checking their state |
| `/ux-map` | Authoring or critiquing a text-first UX inventory |

A typical task, end to end:

```text
/scope             "we should probably add X"
/plan-analyze      triage the draft plan
/planning-review   formal review, findings recorded
make task-start    feature branch + linked worktree
/tdd               first failing test for implementation note
/incremental-implementation   drive slices to green
/branch-review     pre-merge review, findings persisted
make close-check   merge-readiness gate on the final HEAD
make task-finish   close, archive, tear down
```

`make help` lists every target. `make wb VERB=<start|status|slice|close|gate|ship|stop|accept|doctor>`
is the short form of the same lifecycle.

## Install

Delivery is **GitHub-only** from tagged refs on
[`darce/workbay`](https://github.com/darce/workbay).

### 1. Claude Code (fastest)

One-time marketplace registration, then install the plugin for that checkout:

```text
/plugin marketplace add darce/workbay
```

### 2. CLI (any harness)

Install the `workbay` front-door CLI from a tag, then hoist the overlay into
your repo. The helper script expands the multi-package git closure so you do
not list member subdirectories by hand:

```bash
REF=v0.1.55   # needs a mirror tag that ships scripts/install-workbay-cli.sh
curl -fsSL "https://raw.githubusercontent.com/darce/workbay/${REF}/scripts/install-workbay-cli.sh" \
  | bash -s -- "$REF"
workbay install --target /path/to/your/repo --remote-ref "$REF"
```

From a monorepo or mirror checkout of the same tag:

```bash
./scripts/install-workbay-cli.sh v0.1.55
workbay install --target /path/to/your/repo --remote-ref v0.1.55
```

On locked-down hosts that need the full runtime package list written out,
use the explicit `uv tool install --no-sources` closure in
[`docs/CONSUMER.md`](docs/CONSUMER.md) or the
[Developer / pre-release install](#developer--pre-release-install) section
below.

One install materializes skill and hook surfaces, registers the two MCP
servers (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`) via the stdlib
`mcp_launch.py` shim, provisions `.task-state/handoff.db`, and sets
`core.hooksPath` so enforcement hooks fire throughout the git lifecycle.

Restart your agent so it picks up the new surfaces, then:

```bash
make context        # reload active task state at any point
make task-start TASK=PROJ-1 OBJECTIVE="add rate limiting"
```

or start from a vague idea inside the agent session:

```text
/scope  we should probably add rate limiting somewhere
```

### Optional install flags

| Flag | What it turns on | If verification fails |
| --- | --- | --- |
| `--with-codebase-graph` | Registers the optional `codebase-graph-mcp` server. Needs the `codebase-memory-mcp` binary on `PATH`. | Nothing at install time. The launch shim probes at start and soft-degrades. |
| `--with-remote` | Probes the remote gate over SSH using `WORKBAY_REMOTE_GATE_HOST` and records `execution_mode: remote_only` in the bootstrap ledger, so offload defaults to a remote lane. | Install fails and names the repair command. Re-run without the flag, then `workbay repair --with-remote` once the host is reachable. |
| `--with-embeddings` | Hard-verifies at install that the embeddings extra imports and the model digest pin is readable, then records `embeddings_mode=verified`. | Install fails and names the repair command. |
| `--no-embeddings` | Skips model provisioning and records `embeddings_mode=disabled`. Also honored via `WORKBAY_HANDOFF_EMBEDDINGS_DISABLED=1`. | n/a |

`--with-codebase-graph` is excluded by default so consumers without the
binary never inherit a dead MCP entry. Embeddings default to enabled;
interactive installs ask for consent, and the gate is reversible at any time
with `workbay embeddings --enable|--disable|--status`. Remote policy can be
flipped after the fact with `workbay repair`
(`--with-remote` / `--no-remote`).

`workbay doctor` (cwd; `--target` still overrides) detects drift after
upgrades; `status`, `update`, and `repair` round out the lifecycle. See
[`docs/CONSUMER.md`](docs/CONSUMER.md) for the upgrade workflow and for
overriding MCP servers or skills.

## One state, any agent

WorkBay builds each harness's surface from one manifest, so the commands and
MCP tools are the same whichever agent you run:

| Harness | Generated surface |
| --- | --- |
| Claude Code | Plugin with skills and hooks |
| Codex | Plugin plus `.codex/config.toml` activation |
| Cursor | `.cursor/skills/` |
| grok | `.grok/plugins/workbay-system/` |
| VS Code Copilot | `.github/prompts/` |

A Claude Code session and a Codex session pointed at the same workspace read
the same task rows, open findings, and dashboard. Each session opens by
calling `load_session`, which returns a ranked context packet (active task,
open findings, recent decisions, touched files) so the agent resumes from
load-bearing state rather than a cold prompt.

## Remote subagent fan-out

Parallel work without shared obligation either serializes (slow) or collides
(same files, same mistakes, same unproven claims). WorkBay's answer is
**lanes**: isolated worktrees, explicit backends, budgets, and a review gate
that keeps merge authority outside the worker.

### What `/offload` and lane dispatch do

`/offload` hands one self-contained implementation slice to a bounded junior
lane, then presents the result behind a review gate, so an expensive
orchestrator model does not spend tokens on work a cheaper backend can do.
It ships with the orchestrator server and needs no extra install.

Backends are chosen explicitly, never inferred. `/offload` accepts
`grok-remote` (the remote lane), plus `grok-cli`, `cursor-cli`, and
`codex-subagent` locally. Under a `--with-remote` install the ledger records
`execution_mode: remote_only`, an omitted backend resolves to `grok-remote`,
and an explicit local backend is refused with a typed `remote_required`
outcome (no silent substitution onto another backend).

The orchestrator's lane dispatch (`dispatch_lane_work`) reaches three remote
invocation backends. `codex-remote` and `cursor-remote` are not `/offload`
`--agent` values; only the four profiles above are:

| Lane-dispatch invocation backend | Default model |
| --- | --- |
| `grok-remote` | `grok-4.6` |
| `codex-remote` | `gpt-5.6-sol` |
| `cursor-remote` | `cursor-grok-4.5-high-fast` |

Fan-out scales when the work is independent: many files to review, many
mechanical slices under one plan, or N adversarial reviewers on the same diff
(`/review-parallel`). Each lane owns a worktree (or remote workspace), a
token budget, and a typed pass outcome. The parent does not re-run the
worker's tools from a prose summary; it inspects the commit and the structured
handoff.

### Guardrails

- Preflight fails fast, before any dispatch spend, if the backend is
  unavailable, the effort level is unsupported, the model pin is wrong, the
  worktree is dirty, or `token_budget` is unset.
- `token_budget` is a cross-cycle circuit breaker. A non-converging lane
  stops at the next cycle boundary after cumulative spend crosses the cap,
  keeps its worktree diff, and records a `token_budget_exceeded` blocker. It
  is not killed silently and does not retry on another backend.
- The lane commits its own verified work and submits a handoff. It never
  merges. Review the diff (`/branch-review` or `/review-parallel`) first.
- Author and reviewer do not share a transcript. That is intentional: a
  second model family that only sees the patch and the rules can still force
  findings into the database that block `review-ready`.

Every pass returns a typed outcome (`handoff_ready`, `needs_guidance`,
`self_verify_failed`, `timeout`, `token_budget_exceeded`, and so on) plus
`commit_landed` and `failed_stage`, so the coordinator branches on structured
result instead of reading log tails.

### How this differs from common “multi-agent” tools

| Shape | Typical product | WorkBay lane fan-out |
| --- | --- | --- |
| Chat of agents | Multi-agent frameworks / company rooms | Isolated git worktrees + MCP ledger, not a shared conversation |
| Memory of what was said | mem0, claude-mem, wiki vaults | Obligation of what still must pass gates |
| Cloud company OS | Multiplayer harnesses with Slack/crons | Local (or SSH remote gate) process control plane for *this* repo |
| Orchestration graph | LangGraph / Temporal / n8n | Agent/operator chooses next step; WorkBay records and enforces |

Use lanes when breadth is real. Do not fan out when the second agent needs
most of the first agent’s raw evidence — that is handoff amnesia dressed as
collaboration. Prefer passing **artifacts** (diffs, failing test output,
schemas) over summaries.

## Semantic embeddings (workflow recall, not a second memory product)

Optional embeddings sit **on top of** the structured handoff database. They
do not replace tasks, findings, or gates.

**What they solve.** After compaction or a cold start, keyword search alone
misses related decisions and findings phrased differently from the query.
Embeddings power semantic retrieval over workflow rows (decisions, findings,
tests, blockers) so `load_session` / reinjection can rank **obligation-shaped**
context, not chat snippets.

**How they differ from memory tools.**

| | Typical memory tool | WorkBay embeddings |
| --- | --- | --- |
| Indexed content | Free-text “facts” or transcript chunks | Rows already in `handoff.db` (decisions, findings, tests, …) |
| Success metric | “Something relevant was recalled” | “The right open obligations re-entered context” |
| Authority | Advisory | Still subordinate to hooks and disposition state |
| Toggle | Product default on | Consent at install; `/workbay` or `workbay embeddings` on/off/status |

**When to enable.** Multi-session tasks, large decision history, or
cross-slice continuity where reinjection quality matters. **When to skip.**
Greenfield repos with almost no handoff history, air-gapped hosts that
cannot provision the model, or operators who want a minimal footprint
(`--no-embeddings`).

Embeddings default to enabled with interactive consent; `--with-embeddings`
hard-verifies the stack at install time; `--no-embeddings` disables
provisioning. Policy is reversible without reinstall.

## What the hooks enforce

Installed through `core.hooksPath` and the harness hook configs, these apply
no matter which agent is running:

- Edits on `main` are refused outside explicitly permitted surfaces.
- Branch names must match the grammar: `feature/<task-ref>` with a
  lowercase, hyphenated, digit-bearing task ref (plus `maint/`, `hotfix/`,
  `release/`, and `revert/` kinds).
- `make review-ready` fails while findings are open.
- `make close-check` refuses a finish when any slice lacks a recorded
  decision, and `make handoff-close-check` runs the same gate against the
  final branch HEAD.
- File touches are recorded per slice for provenance.

## What persists, exactly

State lives in `.task-state/handoff.db`, a versioned SQLite schema
(currently v33, migrated in place) owned by the handoff MCP server. The
load-bearing tables:

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

Everything is full-text searchable (`search_handoff`), renderable as a human
dashboard (`DASHBOARD.txt`) and machine snapshot (`CURRENT_TASK.json`), and
portable as JSON via `export_handoff_state` / `import_handoff_state`.

## Delegating a slice

`/offload` hands one self-contained implementation slice to a bounded junior
lane under a hard token cap. Backends, preflight guardrails, remote-only
policy, and the token-budget circuit breaker are covered under
[Remote subagent fan-out](#remote-subagent-fan-out) and in
[`docs/CONSUMER.md`](docs/CONSUMER.md#offloading-a-slice-to-a-junior-backend-offload).

## How review works

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

For larger work, the orchestrator server runs several worktree lanes behind
worker daemons; lane messaging and plan cursors coordinate them, and
per-turn token metrics give visibility.

## Adjacent tool layers

WorkBay sits next to three neighboring product shapes. Each owns a different
layer.

| Neighbor | Owns | WorkBay owns |
| --- | --- | --- |
| **Memory / persistence tools** | What was said or learned | What was done and still owed, with gates |
| **Multiplayer company harnesses** | Humans + agents across org rooms, crons, cloud scopes | Coding process control inside a git repo |
| **Orchestration engines** | What runs next (graphs, durable workflows, DAGs) | Obligation, evidence, and merge refusal |

They compose: memory for fuzzy recall, a multiplayer front door for humans if
you need one, an orchestrator if you already have workflow graphs — and
WorkBay as the repo-local ledger that still refuses a dirty merge.

Product-by-product tables (including memory tools and multiplayer harnesses
such as YC QM), when-to-use guidance, and composition notes live in
[`docs/COMPARISON.md`](docs/COMPARISON.md).

## Install integrity

Run `workbay doctor` after an upgrade, and `workbay repair` when it reports
drift. The bootstrap installer writes a ledger at `.workbay-bootstrap.json`
recording where the install came from (its source kind and pinned ref or
package version) alongside what it produced: the generated surfaces, managed
MCP servers, install steps, and the embedding and execution-mode policy.
`workbay doctor` checks that ledger against the files on disk; `repair`
restores drifted surfaces, `update` moves the overlay forward, and durable
`workbay-overrides/workbay-system/` entries are composed into the effective
plugin tree instead of being overwritten on the next install.

Optional install flags (`--with-codebase-graph`, `--with-remote`,
`--with-embeddings`, `--no-embeddings`) are documented under
[Optional install flags](#optional-install-flags).

## Packages

WorkBay ships from one public git mirror,
[`darce/workbay`](https://github.com/darce/workbay). Seven runtime packages
are tagged together. Consumers install from those git tags via the plugin
marketplace or `scripts/install-workbay-cli.sh`, then
`workbay install --remote-ref vX.Y.Z`. Cross-cutting changes land atomically
from this monorepo; building straight from source is for development.

| Package | Role |
| --- | --- |
| `workbay-protocol` | Typed contracts (Pydantic v2 and JSON Schema) |
| `mcp-workbay-handoff` | MCP server: task state, reviews, evidence |
| `mcp-workbay-orchestrator` | MCP server: lanes, workers, dispatch |
| `workbay-bootstrap` | Consumer install/update/doctor CLI |
| `workbay-system` | Shared skills, hooks, generators |
| `workbay-codex-bridge` | Codex subagent backend for the orchestrator |
| `workbay` | Front door: one-command install and runtime version anchor |

```text
workbay/
├── Makefile                  # `make help` lists every target
├── docs/
│   ├── CONSUMER.md           # install, upgrade, drift workflow
│   ├── UPGRADING.md          # standalone-repo era cutover
│   └── RELEASING.md          # maintainer release playbook
└── packages/                 # the seven packages above
```

The private canvas member is built in this monorepo but is not part of the public
mirror.

### Which package do I install?

Install from `darce/workbay` at a consumer tag. Prefer the front door (`workbay`)
or the Claude plugin path. You do not name component packages one at a time.

| You want to… | Command |
| --- | --- |
| Install the CLI from a tag | `./scripts/install-workbay-cli.sh v0.1.55` (or the curl one-liner above) |
| Install or upgrade the overlay in a repo | `workbay install --target . --remote-ref v0.1.55` |
| Install via bootstrap CLI | `workbay-bootstrap install --target . --remote-ref v0.1.55` |
| Track `main` or a fork ref | `workbay install --target . --remote-ref <ref>` |

The component trees (`workbay-protocol`, `mcp-workbay-handoff`,
`mcp-workbay-orchestrator`, `workbay-system`, `workbay-codex-bridge`) ship in
the same git mirror; bootstrap installs their MCP binaries once during overlay
setup. [`docs/CONSUMER.md`](docs/CONSUMER.md) walks through install, upgrade,
and drift repair.

## Developing in this repo

Agent surfaces are generated into gitignored paths, so a fresh clone has the
sources but not the built output. Opening the repo in Claude Code builds
everything automatically via a `SessionStart` hook and prints a one-time
restart prompt. From any other entry point:

```bash
workbay install --target .                                 # Codex activation + all surfaces
make plugins-build                                         # Claude + Codex + Cursor + grok plugin trees
make generate-agent-workflows WORKFLOW_TARGET_ROOT="$PWD"  # VS Code Copilot prompts
```

Agents read these surfaces only at startup, so restart after the first build.
Release mechanics live in [`docs/RELEASING.md`](docs/RELEASING.md).

### Developer / pre-release install

Track an unreleased `git` ref or hack on the installer from a monorepo
checkout:

```bash
R="git+https://github.com/darce/workbay.git@main"
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/mcp-workbay-handoff" \
  --with "$R#subdirectory=packages/mcp-workbay-orchestrator" \
  --with "$R#subdirectory=packages/workbay-bootstrap" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay" \
  workbay
workbay install --target /path/to/your/repo --remote-ref main
```

`workbay install` defaults `--source package`. Pass `--source git_overlay`
for the clone-backed overlay. See [`docs/CONSUMER.md`](docs/CONSUMER.md)
for upgrades and [`docs/RELEASING.md`](docs/RELEASING.md) for cutting a
release.

## Status

This monorepo is the canonical WorkBay source. The earlier standalone
repositories (`mcp-workbay-handoff`, `mcp-workbay-orchestrator`,
`workbay-system`, `workbay-bootstrap`) remain reachable while consumers
migrate; see [`docs/UPGRADING.md`](docs/UPGRADING.md) for the cutover.
