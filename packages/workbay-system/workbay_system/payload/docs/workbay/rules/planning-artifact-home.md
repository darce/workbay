# Planning Artifact Home

Where planning artifacts (scopes, plans, ADRs, assessments, reviews,
tech-debt notes, task plans) live before, during, and after a task
branch claims them.

## Invariant

A planning artifact lives in a tracked path under one of the canonical
planning homes and is either committed or staged for commit on a
feature, research, or planning branch. Untracked planning artifacts on
`main` are an anti-pattern and must not persist past the session that
created them.

## Canonical homes

- `docs/scopes/<slug>.md` — scope intake one-pagers (one per feature/epic).
- `docs/plans/<id>-<slug>.md` — implementation plans not tied to a single package.
- `docs/assessments/<slug>.md` — engineering assessments and discovery notes.
- `docs/adrs/<NN>-<slug>.md` — architecture decision records.
- `docs/reviews/<slug>.md` — review write-ups produced outside the MCP findings flow.
- `docs/tech-debt/<slug>.md` — durable tech-debt records.
- `packages/<pkg>/docs/tasks/<TASK_ID>-<slug>-task-plan.md` — per-package task plans.

Anything outside this list is not a planning artifact home. Stash drafts
in scratch (outside the repo) or delete them.

## Plan Id Allocation

`docs/plans/<NNNN>-<slug>.md` carries a monotonic four-digit plan id.
That id is **global across every branch and linked worktree**, not just
the current checkout — plan docs reach `main` only at branch merge, so
an in-flight plan on an unmerged feature branch is invisible to anyone
who only scans `main` or their own worktree. Picking "max on `main` + 1"
is how two concurrent branches collide on the same id (e.g. the
0053/0054 double-claims). ADR ids under `docs/adrs/<NN>-<slug>.md`
follow the same all-refs rule; use an analogous scan over `docs/adrs/*`
when allocating ADR numbers.

Allocation rule:

- The next free plan id is `max(<id>) + 1` computed over **all refs and
  all linked worktrees**, not the local tree. The shell recipe below
  prints the current max; add 1 (or use `0001` when the pipeline is
  empty):

  ```bash
  git log --all --pretty=format: --name-only -- 'docs/plans/*' \
    | grep -oE 'docs/plans/[0-9]{4}' | grep -oE '[0-9]{4}' | sort -n | tail -1
  ```

- **Claim the id as soon as the worktree is procured.** The moment a
  task branch/worktree exists for a plan-backed task, commit the plan doc
  (even a stub with just the `# Plan <NNNN> — <title>` heading) onto the
  feature branch. A committed-but-unmerged plan doc is the claim; an
  uncommitted or main-only-eyeballed id is not. This makes the id visible
  to the all-refs scan above before any concurrent worker can re-pick it.
  When the plan already has an accepted baseline on `main` from
  `plan-accept`, the id is fixed — do not re-allocate.
- If a collision is found after the fact, the **later-dated branch
  renumbers** to a fresh all-refs-free id (rename the file + update the
  `# Plan <NNNN> —` or `# ADR <NN> —` heading), since the earlier claim
  wins.

## Durable vs Ephemeral

- Durable docs: scopes, plans, assessments, ADRs, reviews, tech-debt notes, and other explanatory/operator docs intended to stay accurate after a task closes.
- Ephemeral planning state: slice history, mutable review counts, run logs, and checklist evidence that belong in the handoff DB or generated projections.
- Task plans stay in the canonical `packages/<pkg>/docs/tasks/` home, but they are treated as execution-scoped plans whose mutable state must not replace handoff truth.

## Task Plan Status Metadata

New or actively edited task plans should declare `Task Plan Status` in the metadata block.
Allowed values for the current workflow are `proposed`, `active`, `closed`, and `archived`.

Grandfathering rule:

- Active or proposed plans missing the field are docs-hygiene failures.
- Historical closed plans without the field are warnings or archive-candidate debt until a dedicated backfill task owns the migration.

## Four states

- **Committed on a feature/research/planning branch**: the artifact is in `git log`
  on the working branch. Normal.
- **Tracked, uncommitted, on a feature/research/planning branch**: in flight. Normal.
- **Accepted baseline on `main`** (internal): after a planning review records
  verdict `pass` with zero open planning findings for the subject, the operator
  runs `make plan-accept TASK=<ref>` to land a docs-only commit carrying just
  the plan file to `main`. Reviewed same-session drafts that are still
  untracked on root `main` can be accepted with the receipt-surfaced explicit
  command: `make plan-accept TASK=<ref> LIFECYCLE_ARGS="--json --local --plan <path> --source-branch main"`.
  Once accepted, the `main` copy is **immutable**: further edits go to the
  feature branch's working copy and are not re-mirrored.
  `resolve_plan_location(prefer="auto")` returns the `main` baseline once it
  exists, so coordinators read the accepted plan locally without checking out
  the feature branch.
- **Untracked on `main` (or any branch with no companion task plan)**:
  orphaned. Anti-pattern. Recover before session end.

## Accepted Baseline Enforcement

For plan-backed tasks, the accepted plan baseline on `main` is not only a
coordination convenience; it is a lifecycle gate. `task-start` refuses before
git mutation when the task's plan has a passing planning review but no accepted
baseline. Later branch gates (`review-ready` and `close-check`) fail closed on
the same missing baseline so implementation cannot proceed to review or merge
from a plan that only exists on a feature branch.

Receipts use the reason `plan_baseline_missing` and point at the same recovery
command: `make plan-accept TASK=<task-ref>` from the root `main` checkout. Run
that command only after the latest planning verdict is exactly `pass` and there
are zero open planning findings; `pass_with_findings`, `conditional_pass`,
`fail`, and absent planning review runs do not unlock acceptance.

## Receipt Detail Reasons

When the accepted baseline is missing, read the additive receipt detail fields
before guessing a recovery:

- `plan_baseline.detail_reason=untracked_draft_on_main` means the plan is still
  an untracked draft on `main`. If the planning verdict is `pass` and open
  planning findings are zero, run the receipt's explicit `plan-accept --local
  --plan <path> --source-branch main` command so the lifecycle command creates
  the docs-only accepted-baseline commit. If the review gate is not clean yet,
  finish planning review first.
- `plan_baseline.detail_reason=plan_missing_on_source_branch` means the source
  branch named by the receipt does not carry the task plan yet.
- `plan_baseline.detail_reason=wrong_ref_review_run` means the same subject was
  reviewed under a different task ref; the candidate ref is surfaced in
  `candidate_review_task_refs`, but that run does not satisfy the current task's
  acceptance gate.
- `plan_accept.recovery_kind=handoff_identity_missing` means the task row is not
  available; rerun `make plan-accept` with explicit `--plan` and
  `--source-branch` values or restore the correct task identity first.
- `plan_accept.recovery_kind=wrong_ref_review_run` means `plan-accept` found a
  same-subject planning run under another task ref and surfaced it in
  `candidate_review_task_refs` for operator triage.
- `plan_accept.recovery_kind=explicit_plan_source_required` means the receipt
  cannot safely infer the plan path and planning branch, so the recovery command
  must name both explicitly.

These fields are additive diagnostics; the stable gate tokens remain
`plan_baseline_missing`, `plan_missing_on_target_branch`, and
`handoff_state_unavailable`.

## Detection

```bash
git status --porcelain | awk '$1=="??" && ($2 ~ /^docs\/(scopes|plans|assessments|adrs|reviews|tech-debt)\// || $2 ~ /^packages\/[^\/]+\/docs\/tasks\//) { print }'
```

On `main`, any line of output is an orphan. The lifecycle
`review-ready` and `close-check` handlers run the same filter and
surface matches via the receipt's `warnings` field (warn-only; never
blocks).

## Recovery

1. If the artifact represents real planning work: switch to a
   conforming task branch — `git switch -c feature/<task-ref>-<slug>`
   where `<task-ref>` matches the canonical grammar (lowercase letters
   with at least one digit, e.g. `feature/internal-50-research-recovery`)
   — then `git add`, `git commit`, open a PR. Note that the gate is
   case-sensitive: `feature/internal-...` is rejected as
   non-conforming. Do not commit planning artifacts directly to `main`
   outside a PR.
2. For the rare hand-rolled research branch that cannot fit the
   conforming grammar, set `AGENTIC_ALLOW_NONCONFORMING_BRANCH=1` for
   the commit and `AGENTIC_ALLOW_NONCONFORMING_BRANCH_PUSH=1` for the
   push (both are audited; see
   [development-workflow.md](development-workflow.md) §Branch
   Isolation Protocol).
3. If the artifact is a scratch draft that should not persist: `rm` it.
4. Never leave the file untracked on `main` past session end.

## One-shot Acceptance Backfill (internal)

For environments that adopted task plans before the acceptance gate
existed, `make plan-accept-backfill` walks every live `handoff_state`
row and emits a docs-only commit recipe for each task that satisfies
the same gates as `plan-accept`: latest planning verdict exactly
`pass`, zero open planning findings, and plan path absent from `main`.

Default (receipt-only) dry-run prints what *would* be accepted but
never touches the index:

```bash
$ make plan-accept-backfill LIFECYCLE_ARGS=--json
{
  "command": "plan-accept-backfill",
  "accepted_count": 1,
  "skipped_count": 2,
  "tasks": [
    {"task_ref": "internal", "action": "accept", "next_command": "git switch main && git checkout feature/internal-67 -- docs/plans/0067-...md && ..."},
    {"task_ref": "internal", "action": "skip", "reason": "open_planning_findings", "open_planning_findings": 2},
    {"task_ref": "internal", "action": "skip", "reason": "already_accepted"}
  ]
}
```

Pass `LIFECYCLE_ARGS=--local` to apply each ready row in-process (the
operator must be on `main` in the canonical root checkout with a clean
tree). The pass is idempotent: rows whose plan already lives on `main`
report `already_accepted` and the second run is a no-op.

## See Also

- [development-workflow.md](development-workflow.md) — branch isolation
  protocol that interacts with planning branches and the audited
  non-conforming-branch override.
- [`scope` skill](../../../skills/scope/body.md) — produces
  `docs/scopes/<slug>.md`.
- [`plan-analyze` skill](../../../skills/plan-analyze/body.md),
  [`planning-review` skill](../../../skills/planning-review/body.md) —
  consume planning artifacts.
