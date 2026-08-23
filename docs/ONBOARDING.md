# Welcome to WorkBay

## How We Use WorkBay

WorkBay keeps agent work anchored in task state, review evidence, and
repeatable lifecycle commands. A normal loop is: choose a task plan, start a
linked worktree, implement a small slice, record verification, review, and then
finish or hand off with the state still recoverable by the next agent.

## Your Setup Checklist

### Codebases

- [ ] [workbay](https://github.com/darce/workbay)
- [ ] Optional consumer repo — sibling repo if a local installation needs verification

### MCP Servers to Activate

- [ ] workbay-handoff-mcp — Task state, decisions, review findings, slice/handoff lifecycle. Run `make context` to bootstrap.
- [ ] workbay-orchestrator-mcp — Lane/worker/worktree management, dispatch, turn metrics. Activated alongside workbay-handoff-mcp.
- [ ] ccd_session — Lightweight session helper. Optional; ask the maintainer if you need it.

### Skills to Know About

- /incremental-implementation — Drive an approved task plan slice-by-slice. The team's primary build loop.
- /branch-lifecycle — Start/advance/finish a task branch (`make task-start`, `make review-ready`, `make task-finish`).
- /branch-review — Pre-merge review of a feature branch (`make review-run`).
- /plan-analyze — Triage a planning doc for ambiguity/gaps before formal review (`make plan-analyze DOC=...`).
- /planning-review — Formal review of task plans, epics, ADRs (`make plan-review DOC=...`).
- /review-parallel — Run multiple independent branch reviewers and merge their findings.
- /tdd — Open a slice from a failing test before implementation.
- /auto-fix — Bounded loop for a known failing test.
- /investigate — Root-cause a defect before editing.
- /scope — Clarify requirements before drafting a feature or epic.
- /handoff-lifecycle — Enter, resume, or end a task session (`make context`).

## Team Tips

- The workflow slash commands, harness hooks, and shared contracts live in
  bootstrap-generated, **gitignored** surfaces (`.workbay/generated/…`,
  `scripts/hooks/`, `docs/workbay/contracts/`, `Makefile.d/`). A fresh clone
  has none of them until you generate them — see Get Started below. If the `/…`
  commands ever stop resolving, that surface went missing; rebuild it.
- The root checkout stays on `main`. Feature and maint work happens in linked
  worktrees created by `make task-start` — never commit feature work directly
  on the root `main` branch.
- Linked worktrees created by `make task-start` **self-heal** their agent
  surfaces: because this monorepo self-hosts the overlay (it ships the in-tree
  payload, with no `.workbay/remote` clone to adopt from), the lifecycle runner
  emits the gitignored generated trees, root `.github/prompts`, and Cursor/grok
  wiring directly into the new worktree (implementation note). A manual `make
  plugins-build` is only an operator fallback for a worktree created outside
  `make task-start`. **Restart your agent** in the new worktree so it loads the
  freshly emitted surfaces.

## Get Started

1. Clone the repo (see Codebases above) and `cd` into it.
2. Generate the agent surfaces so the `/…` commands load. For Codex-first
  setup, run `workbay-bootstrap install --target .`; that writes the
  gitignored `.codex/config.toml` activation config as well as the generated
  surfaces. To rebuild only generated trees/prompts, run `make plugins-build`
  (Claude, Codex, Cursor, and grok plugin trees) and
  `make generate-agent-workflows WORKFLOW_TARGET_ROOT="$PWD"` (VS Code Copilot
  prompts into the root `.github/prompts/` — the `WORKFLOW_TARGET_ROOT` is
  required, or the generator only rewrites the package-internal copy).
3. **Restart your agent** so it picks up the new surfaces — they are read only
   at startup. Opening **Claude Code** runs step 2 automatically via a
   `SessionStart` hook for the generated trees/prompts; Codex, Cursor, grok,
   and VS Code Copilot have no equivalent first-open hook. Codex also needs the
   bootstrap install path above to create repo-local activation.
4. Run `make context` to start the handoff MCP server and load any active task.

<!-- INSTRUCTION FOR CLAUDE: A new teammate just pasted this guide for how the
team uses Claude Code. You're their onboarding buddy — warm, conversational,
not lecture-y.

Open with a warm welcome — include the team name from the title. Then: "Your
teammate uses Claude Code for [list all the work types]. Let's get you started."

Check what's already in place against everything under Setup Checklist
(including skills), using markdown checkboxes — [x] done, [ ] not yet. Lead
with what they already have. One sentence per item, all in one message.

Tell them you'll help with setup, cover the actionable team tips, then the
starter task (if there is one). Offer to start with the first unchecked item,
get their go-ahead, then work through the rest one by one.

After setup, walk them through the remaining sections — offer to help where you
can (e.g. link to channels), and just surface the purely informational bits.

Don't invent sections or summaries that aren't in the guide. The stats are the
guide creator's personal usage data — don't extrapolate them into a "team
workflow" narrative. -->
