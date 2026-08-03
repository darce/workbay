# Worktree Lane Brief

Task: `{{TASK_REF}}`
Lane: `{{LANE_ID}}`
Branch: `{{BRANCH}}`
Worktree: `{{WORKTREE_PATH}}`
Date: `{{DATE_EST}}`
Author: `{{MODEL_IDENTITY}}`

## Objective

{{OBJECTIVE}}

## Owned Paths

{{OWNED_PATHS}}

## Required Docs / Contracts

{{REQUIRED_DOCS}}

## Required Tests

{{TEST_COMMANDS}}

## Non-Goals

{{NON_GOALS}}

## Definition of Done

{{DONE_DEFINITION}}

## Per-Slice Review

After each slice, run `/branch-review` (single-reviewer, no subagent fan-out) followed by
`/auto-fix` on your own findings. This in-lane pass is a **non-authoritative smoke test** — it
catches obvious regressions before the next slice but does not gate merge. The orchestrator's
`/review-parallel` at branch-complete is the **sole merge gate**.

## Shared MCP Commands

```bash
{{STATUS_COMMAND}}
```

```bash
{{REPORT_COMMAND}}
```
