# Review

## Overview

Thin mode-detection dispatcher. Durable review execution lives in the specialized skills below — do not re-implement their checklists here.

## Trigger

Use this skill only to **route** a review request when the operator says "review" / "audit" without naming a mode. Prefer invoking the specialized skill or makefile target directly when the mode is already known.

## Mode detection

| Target / signal | Route to | Entry |
|---|---|---|
| Source under `apps/`, `packages/`, `scripts/`, `mk/`, or a feature-branch / PR diff | [../branch-review/SKILL.md](../branch-review/SKILL.md) | `make review-run` |
| Artifact under `docs/tasks/`, `docs/epics/`, `docs/roadmaps/`, `docs/workbay/contracts/`, or a plan / epic / ADR | [../planning-review/SKILL.md](../planning-review/SKILL.md) | `make plan-review DOC=<path>` |
| Multi-reviewer or coverage-target branch review | [../review-parallel/SKILL.md](../review-parallel/SKILL.md) | `/wb-review-slice` |

If the diff contains both code and planning docs, run **separate** passes for each mode — never one mixed checklist.

## What this skill does not own

- Branch-review checklist, findings discipline, or verdict recording → `branch-review`
- Planning-review checklist or plan-accept gate → `planning-review`
- Parallel reviewer fan-out → `review-parallel`
- Portable-command registration: this skill is **not** a managed `/command_id` (see the repository agent instructions — `AGENTS.md`, `CLAUDE.md`, `.instructions.md`, or equivalent — for the managed id list). Prefer the specialized skills above.

## Core Process

1. Infer mode from the request and target paths (table above). Do not ask when inferable.
2. Load and follow the routed skill's body end-to-end — including MCP recording, verdict, review-run, and dashboard refresh.
3. If mode is ambiguous after inspecting paths, ask once which specialized skill to run; do not invent a hybrid process.

## Convergence Criteria

- Exactly one specialized skill owned the pass.
- Findings, verdict, and review-run were recorded under that skill's rules (not duplicated here).

## See Also

- [../branch-review/SKILL.md](../branch-review/SKILL.md)
- [../planning-review/SKILL.md](../planning-review/SKILL.md)
- [../review-parallel/SKILL.md](../review-parallel/SKILL.md)
- [../../rules/branch-review-guide.md](../../rules/branch-review-guide.md)
- [../../rules/planning-review-guide.md](../../rules/planning-review-guide.md)
