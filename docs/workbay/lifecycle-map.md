# Task Lifecycle Map

Concise map of the default agent/operator workflow. This page links the
canonical docs rather than restating them.

## Tracked root docs

- [instructions.md](instructions.md) — portable command router and session protocol
- [environment-variables.md](environment-variables.md) — env knobs that affect lifecycle and MCP

## Lifecycle stages

| Stage | Skill / command | Outcome |
| --- | --- | --- |
| Scope | `/scope` | Requirements clarified before planning |
| Plan | `plan-analyze` → `planning-review` → `make plan-accept` | Reviewed plan baseline on `main` |
| Branch | `make task-start` | Feature branch + linked worktree |
| Slice | `make slice-start` → TDD → `make slice-commit` | One vertical, test-backed increment |
| Review | `make review-ready` → branch review | Findings recorded; blockers cleared |
| Close | `make close-check` → merge | Task done; handoff archived |

## Materialized rule docs

These live under `docs/workbay/rules/` after RULES_DIR materialization
(bootstrap install or `workbay-bootstrap bootstrap-surfaces` on self-host
worktrees). In a bare worktree checkout they may be absent until
materialization runs.

- [development-workflow.md](rules/development-workflow.md) — canonical six-step loop and dirty-main policy
- [lifecycle-recovery.md](rules/lifecycle-recovery.md) — guard invariants and escape hatches
- [planning-artifact-home.md](rules/planning-artifact-home.md) — where planning artifacts live
