# CURRENT_TASK.json Compatibility Export Template

> `CURRENT_TASK.json` is retired from the normal agent workflow. Do not copy this
> template to the monorepo root when starting a task. Canonical task state lives in
> the handoff DB; use `make context`, `make tasks`, MCP tools, and `DASHBOARD.txt`
> for routine workflow state.
>
> Keep this template only for legacy consumers that explicitly request a
> task-scoped export. Prefer `render_handoff(kind='current_task', write_file=False)`
> or an export under `.task-state/exports/` over a durable root file.

---

# Current Task: [TASK_TITLE]

**Started**: [DATE]
**Task Doc**: [Link to docs/tasks/X.Y/task-plan.md if applicable]
**Status**: [IN_PROGRESS | COMPLETE | BLOCKED]

## Objective

[1-2 sentence description of what we're trying to accomplish]

## Context

[Brief background that a new agent session needs to understand the task]

- Why this matters: [user impact or technical debt being addressed]
- Related issue/PR: [link if applicable]

## Latest Decision

[Most recent decision summary and why it mattered]

## Progress

### Completed

- [x] [Completed item with brief note]
- [x] [Completed item with brief note]

### In Progress

- [ ] [Current work item] ← **ACTIVE**

### Remaining

- [ ] [Future item]
- [ ] [Future item]

## Key Files

| File | Purpose |
|------|---------|
| `path/to/file.py` | [What this file does in context of the task] |
| `path/to/file.ts` | [What this file does in context of the task] |

## Technical Notes

[Any implementation details, decisions made, or gotchas discovered during work]

## Verification Commands

```bash
# How to verify the current state
cd path/to/package-or-app
pytest tests/unit/test_relevant_file.py -v

# Type checking
mypy .
```

## Next Agent Instructions

[Specific instructions for the next session, e.g.:]

1. Read [specific file] lines X-Y to understand the current implementation
2. Continue from [specific function/method]
3. Run [specific test] to verify before proceeding

---

## Session Log

### [DATE] - Session N

- What was done
- What was discovered
- What blocked progress (if anything)
