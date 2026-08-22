# How WorkBay compares to adjacent tools

Evaluators usually arrive with one of three questions: "is this another
memory plugin?", "doesn't multiplayer harness X already do this?", or
"isn't this just LangGraph?" This page answers against products that
were publicly available as of mid-2026. Product capabilities change;
treat tables as dated surveys, not permanent scorecards.

The monorepo [`README.md`](../README.md) keeps a short generic framing
only. Detailed product rows live here.

## The short answer

| Layer | Typical tools | WorkBay |
| --- | --- | --- |
| Memory | mem0, claude-mem, engram, Contynu | Optional embeddings over **workflow rows**, not chat |
| Obligation | Issue trackers, PR review, task masters | Tasks, slices, findings, tests, blockers in SQLite |
| Multiplayer org OS | Company harnesses (e.g. YC QM) | Repo-local process control; attaches via MCP |
| Orchestration | LangGraph, Temporal, n8n, Airflow | Does **not** own “what runs next”; records and gates |

Memory tools persist what an agent *said and learned*. WorkBay
persists what an agent *did and still owes*: the active task and its
branch, slice-complete decisions anchored to commit SHAs, review
findings with disposition lifecycles (open, fixed, deferred, wontfix,
resolved_on_branch, integrated, superseded),
test results with commands and exit codes, and close gates that refuse
a merge while any of that is unresolved. The categories are
complementary.

## Survey

| Product | What it persists | Storage | Harness reach | Structured workflow state |
| --- | --- | --- | --- | --- |
| [mem0](https://github.com/mem0ai/mem0) | Extracted natural-language memories at user/session/agent scope | Vector stores (Qdrant, Chroma, PGVector, ...) | Any MCP client; SDKs | None |
| [Contynu](https://contynu.com/) | Freeform memories in six kinds (facts, decisions, todos, ...) with importance ranking | Local SQLite | Claude Code, Codex, Gemini CLI, OpenClaw | None; "decisions" and "todos" are memory text, not lifecycle rows |
| [engram](https://github.com/Gentleman-Programming/engram) | Tagged observations (architecture, decision, bugfix) | Single SQLite file + FTS5 | Any MCP client | None; type tags only |
| [lcm](https://lossless-claude.com/) | Every message losslessly, plus a DAG of compacted summaries | SQLite + FTS5 | Claude Code hooks; 22 connectors via MCP | None; transcript memory |
| [claude-mem](https://github.com/thedotmack/claude-mem) | AI-compressed per-tool-call observations and session summaries | SQLite + Chroma | Claude Code plus ~8 harnesses | None |
| [ai-memory](https://github.com/akitaonrails/ai-memory) | Session logs compiled into markdown wiki pages and handoff narratives | SQLite + git-versioned markdown | ~8 harnesses via hooks and MCP | None |
| [Letta](https://github.com/letta-ai/letta) | Agent memory blocks, recall and archival memory, full agent state | PostgreSQL + pgvector | Its own runtime; generic MCP/API from others | None |
| [beads](https://github.com/gastownhall/beads) | Issue graph: hash IDs, status, priority, typed dependencies, audit trail | Dolt (version-controlled SQL); JSONL export | CLI + MCP; Claude Code, Codex, Cursor, Factory, Mux | Partial: durable issue lifecycle and dependency provenance; no review findings, commit anchoring, test evidence, or gates |
| [Task Master](https://github.com/eyaltoledano/claude-task-master), [Shrimp](https://github.com/cjo4m06/mcp-shrimp-task-manager), [Backlog.md](https://github.com/MrLesk/Backlog.md) | Task lists with dependencies and status | JSON / markdown / SQLite | MCP across major harnesses | Partial: task state only; verification is self-assessed text where it exists |
| [OpenSpec](https://github.com/Fission-AI/OpenSpec), [spec-kit](https://github.com/github/spec-kit) | Spec and plan artifacts with checklists | Markdown in-repo | 20+ assistants via slash commands | Partial: planning artifacts, no runtime state |
| [vibe-kanban](https://github.com/BloopAI/vibe-kanban) | Tasks bound to agent workspaces, branch per task, inline diff comments | Local DB | 10+ harnesses (community-maintained since Bloop shut down) | Partial: lifecycle orchestration; review comments are ephemeral steering, no dispositions or gates |
| [SonarQube MCP](https://github.com/SonarSource/sonarqube-mcp-server) | Static-analysis issues with status transitions, quality gates | SonarQube server | Copilot, Claude, Gemini via MCP | Partial: real dispositions and gates, but analysis findings only — no reviewer verdicts, task lifecycle, or test ledger |
| GitHub PRs + [CodeRabbit](https://docs.coderabbit.ai/) / [Greptile](https://greptile.com/) | SHA-anchored review comments, approve/request-changes verdicts, check runs, branch protection | GitHub platform | Agent-queryable via `gh` / MCP | Partial: the closest platform analog, but PR-granular and platform-bound |

Categories deliberately left out: agent messaging (agmsg), orchestration
UIs that keep no durable state (Conductor, claude-squad), and
platform-bound memory (Devin Knowledge, Cursor Memories, Factory
droids), which are single-vendor by construction.

## What the survey shows

Against the four columns above, every peer leaves at least one empty:

- Memory tools (mem0, Contynu, engram, lcm, claude-mem, ai-memory, Letta)
  fill freeform or extracted recall; **Structured workflow state** is
  empty for each.
- Issue/task trackers (beads, Task Master / Shrimp / Backlog.md) fill
  durable task or issue lifecycle; they do not fill disposition-tracked
  review findings, commit provenance for fixes, test evidence ledgers, or
  enforced merge gates.
- Planning surfaces (OpenSpec, spec-kit) fill planning artifacts only.
- Orchestrators with partial workflow state (vibe-kanban, SonarQube MCP,
  GitHub PRs + AI reviewers) each fill a subset: workspace/branch binding,
  static-analysis dispositions and gates, or platform-bound SHA-anchored
  review — none of those rows fills all four pieces WorkBay records
  (disposition-tracked findings, commit provenance, test evidence, and
  enforced gates) together with generated multi-harness surfaces.

WorkBay's row is that combination plus enforcement: the agent cannot
merge until the recorded findings, decisions, and tests are clean.

Two design choices follow from that position rather than from the
memory-tool playbook:

- Findings carry two provenance anchors — the commit that resolved them
  on the branch and the commit that integrated them into main — so
  "fixed" is checkable against git history, including after rebases and
  worktree moves.
- Gates run at the git layer via `core.hooksPath`, so they hold for
  every harness identically, including ones WorkBay has never heard
  of, and an agent cannot disable them by editing its own settings.

## Multiplayer company harnesses (for example YC QM)

Snapshot date: 2026-07-31. Sources:
[YC announcement](https://x.com/ycombinator/status/2083243960684908768),
[yc-software/qm](https://github.com/yc-software/qm).

Company multiplayer harnesses (QM is the public example) optimize for
**org-wide agent work**: Slack and web as primary surfaces, per-person
and per-room scopes, durable sandboxes, crons and webhooks,
company-brain connectors, shareable web-app artifacts, and org
admin / security postures. They are used across engineering and
non-engineering domains.

WorkBay optimizes for **software process correctness inside a git
repo**: tasks, slices, SHA-anchored review findings, verified tests,
and merge gates that every harness inherits via `core.hooksPath`. It
attaches to Claude, Codex, Cursor, grok, or Copilot over MCP instead of
owning the agent loop or requiring a cloud control plane.

| | Multiplayer company harness (e.g. QM) | WorkBay |
| --- | --- | --- |
| Job | Company agent OS for many kinds of work | Finish software work correctly across sessions and harnesses |
| Default home | Operator cloud (Postgres, HTTP core, plugins) | Your git repo (SQLite, MCP, hooks) |
| Unit of truth | Scope / session / memory / sandbox | Task / slice / finding / commit / gate |
| Multiplayer | Humans and agents in rooms and channels | Multiple agents on one repo; humans via CLI, dashboard, canvas |
| Authority | Identity, scope grants, sandbox / command policy | Git hooks the agent cannot route around |
| Background work | Crons, webhooks, watches | Task-driven lanes, `/offload`, review runs |
| Install | Deploy into operator account | Overlay into consumer repo |

**Composition.** Use a multiplayer harness (or plain Slack) as the human
front door when you need always-on org automation and non-engineering
domains. Use WorkBay as the repo process backend that still blocks
merge until findings, decisions, and tests are clean. They are not
substitutes: WorkBay is stronger at SHA-anchored review dispositions and
cross-harness attach without a control-plane deploy; QM-class systems
are stronger at productized multiplayer UX, org ACL, and background
triggers.

**Adaptation directions (not a commitment to implement):** event
triggers/watches, explicit lane security postures, skill grant/promote
workflows, and a light task-ops web surface — without adopting
Postgres/Slack/cloud as WorkBay defaults. Deeper adaptation notes are
maintainer-internal and are not required for consumer evaluation.

## Orchestration engines

LangGraph, Temporal, XState, n8n, and Airflow own *what runs next*:
graphs, durable workflows, statecharts, or batch DAGs. WorkBay does
not. The coding agent (or operator) decides the next step; WorkBay
records obligation and refuses a dirty merge. Pairing is natural when
you already have a workflow engine that should emit “open task /
close slice / run review” rather than inventing merge policy.

## Remote fan-out vs multi-agent chat

WorkBay lanes (`/offload`, orchestrator worktrees, `/review-parallel`)
fan out **isolated executions** with budgets and typed outcomes. That is
different from multi-agent conversation products that share a context
window or room transcript. Lane workers do not merge; reviewers write
findings into the ledger; git hooks still own the gate. Prefer fan-out
only when work is independent; pass artifacts, not summaries, when
handoffs are required.

## Semantic embeddings vs memory plugins

Optional WorkBay embeddings index **structured workflow rows** for
reinjection and ranked session packets. Memory plugins index free-text
facts or transcripts for general recall. Embeddings improve continuity
after compaction; they do not authorize merge. Toggle with
`workbay embeddings` / `/workbay`. Pair a memory plugin when you need
cross-project preferences; keep WorkBay for repo obligation.

## When another tool is the better fit

WorkBay assumes a git repository, a task-shaped workflow, and an
agent that can speak MCP or run `make`. If you want recall of
conversations and preferences across arbitrary projects, a memory tool
(mem0, claude-mem, engram) is the right shape and pairs well with
WorkBay. If you want a lightweight shared to-do list and nothing
enforced, beads or Backlog.md is less machinery. If your review process
lives entirely in GitHub PRs and you do not need pre-PR slice
discipline, branch protection plus an AI reviewer may be enough. If you
need org-wide multiplayer, crons, and non-engineering domains as the
primary product, a company harness (QM-class) fits better — optionally
with WorkBay under the engineering repos.
