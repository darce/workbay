# How WorkBay compares to other agent persistence tools

Evaluators usually arrive here with one of two questions: "is this
another memory plugin?" and "doesn't tool X already do this?" This page
answers both against the products that were publicly available as of
June 2026. Product capabilities change; treat the table as a survey
with a date on it, not a permanent scorecard.

## The short answer

Memory tools persist what an agent *said and learned*. WorkBay
persists what an agent *did and still owes*: the active task and its
branch, slice-complete decisions anchored to commit SHAs, review
findings with disposition lifecycles (open, fixed, deferred, wontfix,
resolved_on_branch, integrated),
test results with commands and exit codes, and close gates that refuse
a merge while any of that is unresolved. The two categories are
complementary, and several teams run a memory tool alongside WorkBay.

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

No surveyed product combines all four pieces — disposition-tracked
findings, commit provenance, test evidence, and enforced gates — and
none serves them cross-harness the way WorkBay's generated surfaces
do (Claude Code, Codex, Cursor, grok, VS Code Copilot).

The pieces exist separately. GitHub PRs hold SHA-anchored review state,
but only at PR granularity and only on GitHub. SonarQube MCP has
genuine dispositions and gates for static-analysis findings. beads has
durable cross-harness issue state without commit tracking. WorkBay's
position is the combination, plus enforcement: the agent cannot merge
until the recorded findings, decisions, and tests are clean.

Two design choices follow from that position rather than from the
memory-tool playbook:

- Findings carry two provenance anchors — the commit that resolved them
  on the branch and the commit that integrated them into main — so
  "fixed" is checkable against git history, including after rebases and
  worktree moves.
- Gates run at the git layer via `core.hooksPath`, so they hold for
  every harness identically, including ones WorkBay has never heard
  of, and an agent cannot disable them by editing its own settings.

## When another tool is the better fit

WorkBay assumes a git repository, a task-shaped workflow, and an
agent that can speak MCP or run `make`. If you want recall of
conversations and preferences across arbitrary projects, a memory tool
(mem0, claude-mem, engram) is the right shape and pairs well with
WorkBay. If you want a lightweight shared to-do list and nothing
enforced, beads or Backlog.md is less machinery. If your review process
lives entirely in GitHub PRs and you do not need pre-PR slice
discipline, branch protection plus an AI reviewer may be enough.
