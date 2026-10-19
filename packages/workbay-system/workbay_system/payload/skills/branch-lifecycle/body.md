# Branch Lifecycle

## Overview

Use this skill for the branch-owned implementation loop from `make task-start` through `make task-finish`. It owns branch isolation, exact-tip landing, and the invariant close sequence that leaves the root worktree back on `main` after integration and cleanup.

## Trigger

Use this skill when:

- starting implementation from an approved task plan
- preparing an active task branch for review
- checking merge readiness with `make review-ready` or `make handoff-close-check`
- finishing a task branch and tearing down its worktree

Do not use it for planning-only work on `main`, planning reviews, or within-slice RED -> GREEN loops.

## Goal

Move one task cleanly through branch start, slice execution, review, close check, merge, and teardown without leaving code on `main`, stale task state, or an unarchived worktree behind.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
- [../../../docs/workbay/rules/branch-review-guide.md](../../../docs/workbay/rules/branch-review-guide.md)

This skill owns the branch-scoped lifecycle. The `tdd` skill owns the failing-test gate inside a slice, and the review skills own review execution.

Allocation policy (owned by the `offload` skill, *Allocation and gate policy*): deterministic lifecycle mechanics (worktree create/teardown, lane close, close-check, checklist ticks) run as make targets / `wb <verb>` one-shots ([wb-lifecycle-runbook.md](../../docs/workbay/wb-lifecycle-runbook.md)) — never through a model; every skill-mandated handoff read names a `read_profile`. Select an installed backend explicitly and honor its declared capabilities. In `remote_only` mode the current compatibility default remains `grok-remote` at high effort; other installed remote backends such as `codex-remote` are explicit choices. A refusal is surfaced as a typed blocker, never silently downgraded to another backend ([offload-remote-playbook](../../docs/workbay/rules/offload-remote-playbook.md)).

## Core Process

1. Start from an accepted plan baseline. For plan-backed tasks, confirm the latest planning review verdict is `pass` with zero open planning findings, then run `make plan-accept TASK=<task-ref>` from the root `main` checkout before `make task-start`; when the reviewed plan is a brand-new untracked draft on root `main`, use the receipt's explicit `LIFECYCLE_ARGS="--json --local --plan <path> --source-branch main"` form so `plan-accept` commits only that plan file. `task-start`, `review-ready`, and `handoff-close-check` report `plan_baseline_missing` until the baseline exists. If the task does not already have a branch/worktree, create it with `make task-start TASK=<task-ref> OBJECTIVE="..."` before the first implementation edit, then run `make context`. **For a plan-backed task always pass `PLAN=docs/plans/<NNNN>-*.md`** — `make task-start TASK=<task-ref> PLAN=docs/plans/<NNNN>-*.md OBJECTIVE="..."`. On the `plan-accept`→`task-start` order there is no live handoff row yet, so without `PLAN=` `task-start` cannot resolve the implementing plan and silently drops the `-plan<NNNN>` segment **and** the `task_plan_path` link (breaking per-slice checklist sync). `plan-accept`'s accepted-baseline receipt emits this `PLAN=`-bearing command for copy-paste. `task-start` provisions a worktree-root `.venv` (pytest plus the discovered packages installed editably) so that after `cd <target_worktree_path>` bare `pytest` resolves to the worktree rather than a pyenv shim — activate it with `source .venv/bin/activate`, or rely on lifecycle commands such as `make slice-start` that prepend `.venv/bin` for you. In a package-less repo this provisioning step no-ops. When the task is backed by a numbered plan doc (`docs/plans/<NNNN>-*.md`, resolved from `--plan`/`PLAN=` or the live row's `task_plan_path`), `task-start` appends a trailing `-plan<NNNN>` segment to both the branch and the linked worktree directory (e.g. `feature/<task-ref>-plan<NNNN>`, worktree `<repo>-<task-ref>-plan<NNNN>`) so the branch names the plan it implements; a task with no plan is unchanged (`feature/<task-ref>`). The plan segment parses like a slug, so the task-ref resolver and every branch gate are unaffected. **Claim the plan id at worktree procurement (net-new plans only).** After `task-start`, when the task needs a new `docs/plans/<NNNN>-*.md` that is not yet on any branch (including no accepted baseline on `main`), allocate the next id as `max + 1` over **all refs** using the canonical shell recipe in [planning-artifact-home.md §Plan Id Allocation](../../../docs/workbay/rules/planning-artifact-home.md#plan-id-allocation), then commit the plan doc (a `# Plan <NNNN> — <title>` stub is enough) onto the feature branch immediately. When `plan-accept` already landed the plan on `main`, the id is fixed — skip re-allocation. The committed-but-unmerged doc is the claim; an id eyeballed from `main` alone is how two concurrent branches collide.
2. Confirm the implementation shell is on the task's `target_branch` and `target_worktree_path`. If the task is wrong, use `switch_task` instead of carrying changes across tasks.
3. Run implementation through bounded TDD slices: `make slice-start` -> edit -> passing test evidence -> `make slice-commit`. Commit messages follow the canonical convention in [../commit2git/body.md](../commit2git/body.md); when committing from a linked worktree, prefix the subject with the worktree directory name (`<worktree-name>: <subject>`). `make slice-commit` runs `sync-task-plan-checklist` before staging so any task plan `- [ ]` boxes the sync flips to `- [x]` (for items the just-recorded `close_slice` decision proved shipped) land inside the slice commit itself, not as a trailing uncommitted edit (granular evidence-driven sync; never auto-flips `## Stretch Goals`; one-way ratchet).
4. After each slice and before any lint or check pass, run `make format` (the hoist-safe entry point defined in `lifecycle.mk`; consumers wire it via `LIFECYCLE_FORMATTER`, the monorepo wires it to `make format-all` which walks every package). Lane workers in the monorepo can target a specific package directly: `make format-handoff`, `make format-orchestrator`, or `make format` from an app dir. This auto-fixes the majority of lint violations — do not manually fix lint errors without running the formatter first.
5. Run `make review-ready` before handoff. Fix missing verification, contract drift, and other concrete readiness defects. Classify findings by the landing policy; a finding label does not gate mechanical lane reap.
6. Integrate each durable, self-verified lane tip into its feature branch before admitting more work against that branch. Pin the receipt to the worker's exact 40-hex tip and explicitly name the feature target and canonical workspace root, for example:
   `make lane-land LAND_ARGS="--task <task-ref> --lane <lane-id> --expected-tip <40hex> --integration-ref <feature-ref>"` from the workspace root used by the orchestrator. Treat `moved`, incomplete, or indeterminate outcomes as typed retry/blocker states; do not claim that land, drain, and daemon reconciliation are fully joined when their receipt has not proved it.
7. Run one branch-complete review off the landing critical path. Allow at most one grouped fix wave; validate its changed files with tests and formatting, but do not start another review of that fix wave. Unresolved HIGH findings block integration to `main`. A MEDIUM finding gets a one-file fix or joins one named fix wave; LOW and lint-only findings default to defer. These are release dispositions, not prerequisites for a self-verified lane landing into its feature branch.
8. Run `make finalize-plan TASK=<task-ref>` on the feature branch after candidate-changing work is finished and before the exact-candidate gate. It runs the final full-plan `sync-task-plan-checklist` sweep with `--apply` and commits any newly-evidenced `- [ ]` -> `- [x]` ticks (only the plan file). This can change the candidate SHA, so never run it after the final gate. A clean no-op (`commit_status=nothing_to_tick`) is expected when per-slice `slice-commit` sweeps already covered everything.
9. Use `make wb VERB=ship TASK=<task-ref>` for the final exact-tip close-check, immutable candidate integration, and canonical task cleanup. It records the checked candidate SHA and only passes that SHA to the merge sink; the branch name remains identity metadata. The one-shot returns a typed receipt, so there is no extra operator approval between phases just because they compose. If invoking the gate separately, pass `TASK=<task-ref>` to `make handoff-close-check` and do not change the candidate after it passes.
10. Let the canonical lifecycle path own task status, lane reap/close, archive, dashboard, worktree, and branch cleanup. Do not set `done`, archive unconditionally, or force-delete state by hand. Operationally, a task is done only after integration is preserved and the final ship receipt proves cleanup; a legacy `done` row or archive snapshot alone is not completion evidence. If cleanup is pending, keep the outcome incomplete even if a child receipt has `ok: true`. Resume through the canonical `wb ship` / `task-finish` path after resolving the typed cleanup blocker.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I'll just patch this on `main` and branch later." | Code on `main` breaks branch isolation and makes review provenance ambiguous. The hooks block it because the workflow is not trustworthy afterward. | Start or switch to the task branch before any code edit. |
| "The worktree is already merged, so close order doesn't matter." | Integration alone does not prove lane, task, local worktree, or remote sandbox cleanup completed. | Read the canonical ship receipt and resume cleanup when it reports `cleanup_pending`. |
| "`task-finish` closes the loop — it'll merge and tear down for me." | `task-finish` is teardown authority; it never merges. An `ok: true` legacy child receipt does not prove cleanup completed. | Use `make wb VERB=ship TASK=<task-ref>` for exact-tip gate, merge, and finish, or invoke teardown only after integration is proved. |
| "I'll skip `handoff-close-check` this once because review already looked good." | Human review and gate evidence are different things. Missing close-check proof means stale tests, open findings, or missing slice decisions can still slip through. | Run the enforced gate on the final HEAD every time. |
| "I'll chain a gate and cleanup manually because the phases compose." | Manual chaining can hide a typed refusal or lose exact-candidate identity; the supported ship path records those phase receipts together. | Use the `wb ship` one-shot and branch on its final receipt. |
| "I'll just fix these lint errors manually, it's only a few." | `make format` auto-fixes the majority of lint violations. Manual fixes waste time and risk introducing inconsistent style. | Run `make format` first. Only manually fix what the formatter cannot. |

## Red Flags

Each flag is a re-entry trigger. Stop and re-enter at the step shown.

| Flag | Re-entry point |
|---|---|
| Code edits appear on `main` | Step 1: stop, isolate the work onto the feature branch, then continue there. |
| `plan_baseline_missing` appears in `task-start`, `review-ready`, or `handoff-close-check` | Step 1: run `make plan-accept TASK=<task-ref>` from root `main` after the plan has a passing review with zero open planning findings; if the receipt says `detail_reason=untracked_draft_on_main`, run its explicit `--local --plan <path> --source-branch main` command. |
| `task-start` created `feature/<task-ref>` with no `-plan<NNNN>` segment though the task implements a numbered plan (and `plan_path` came back `null`) | Step 1: `PLAN=` was omitted. Tear down the branch/worktree and re-run `make task-start TASK=<task-ref> PLAN=docs/plans/<NNNN>-*.md OBJECTIVE="..."`; the segment + `task_plan_path` link only auto-append from `PLAN=` or a pre-existing live row. A lifecycle gate backs this red flag: once the live row carries `task_plan_path`, `review-ready`/`handoff-close-check` emit `plan_id_missing_from_branch` and refuse to pass a segment-less plan-backed branch, and `task-start` refuses with `plan_id_unresolved_for_plan_backed_task` rather than mint a segment-less branch when it can already tell the task is numbered-plan-backed. |
| Plan id chosen from `max(main) + 1` (ignoring unmerged branches), or plan doc left uncommitted after the worktree exists | Step 1: re-allocate the id across **all refs** (canonical recipe in [planning-artifact-home.md §Plan Id Allocation](../../../docs/workbay/rules/planning-artifact-home.md#plan-id-allocation)) and commit the plan doc onto the feature branch to claim it; if a live branch already holds that id, the later branch renumbers (rename + update the `# Plan <NNNN> —` heading). |
| `make context` reports branch or worktree drift | Step 2: fix the shell context before recording any more MCP state. |
| Lint errors encountered without running `make format` first | Step 4: run the formatter before any manual lint fixes. |
| `command -v pytest` resolves to a pyenv shim inside a task worktree | Step 3: restore the worktree-local environment — rerun lifecycle provisioning (`make slice-start`, or the orchestrator `provision-env --worktree <path>` entry point) or `source .venv/bin/activate` before running pytest directly. |
| Review-ready reports missing tests, open findings, or contract drift | Step 5: clear the blocking condition before asking for review. |
| `handoff-close-check` fails | Step 9: resolve the named missing evidence or HIGH finding, then rerun `wb ship` against the new exact candidate. |
| `wb ship` returns `cleanup_pending` or any teardown facet is incomplete | Step 10: preserve the branch/worktree evidence and retry through the canonical cleanup path; do not treat child `ok: true` as completion or force-delete. |

## Recovery

- If dirty code is inherited on `main`, stop and move it to the owning feature branch before starting new implementation.
- If the wrong task is active, switch task state first and rerun `make context`.
- If integration lands but local or remote cleanup does not, follow the typed `cleanup_pending` receipt and rerun the canonical cleanup path instead of hand-editing `CURRENT_TASK.json` or `DASHBOARD.txt`.
- If lanes were opened, preserve each landing receipt and let the canonical lifecycle/reap path close them from ancestry and no-live-writer evidence; active finding or classifier text is not a mechanical reap gate.
- If a task-plan `- [ ]` box did not auto-tick after `make slice-commit` / `make finalize-plan`, the evidence anchor in the item's body did not match a recorded `close_slice` decision's `changed_files`, a recorded `test_result` command, or an explicit decision id reference — verify the anchor. (`make task-finish` does not persist ticks: its post-merge sweep is verify-only and warns `plan_checklist_drift`; persist final ticks via `make finalize-plan` *before* the exact-candidate gate.) The sync never unticks (one-way ratchet) and never auto-ticks `## Stretch Goals` items.

## Cold-Start Recovery

Use this runbook when an agent session begins (or resumes) with cwd in the primary worktree while the active task's `target_worktree_path` points at a linked worktree. Correct cwd up front so file-mutation tools are not blocked and so MCP writes attribute to the right task row.

1. Detect the mismatch:
   - `git rev-parse --show-toplevel` (current worktree root)
   - active task target via `load_session` / `get_handoff_state(sections="identity")` (canonical `target_worktree_path`)
   - If they differ and the active task is not `MAINT-*`, you are cold-starting in the wrong shell.
2. Recover in this order:
   1. `cd <target_worktree_path>` (the directive already emitted by `advise-worktree-cd.py` on `SessionStart` and `UserPromptSubmit`).
   2. `make context` — re-resolves the active task from the corrected cwd.
   3. Resume the next slice action; if you cannot `cd` first, pass `task_ref` explicitly on every MCP write so write-context attribution lands on the correct row.
   4. The worktree should carry a root `.venv` from `task-start`. If `command -v pytest` resolves to a pyenv shim instead of `<target_worktree_path>/.venv/bin/pytest`, `source .venv/bin/activate` or rerun lifecycle provisioning before running pytest directly; lifecycle commands like `make slice-start` already prepend `.venv/bin`.
3. Hook layering (see the `advise-worktree-cd` and `worktree-drift` hook entries in `harness-protocol.yaml`):
   - `advise-worktree-cd.py` — `SessionStart` / `UserPromptSubmit` advisory. Proactive nudge; may stay silent for `MAINT-*`, ambiguous task, or missing `target_worktree_path`.
   - `_worktree_drift.py` — `PreToolUse` blocker, **matched only on file-mutation tools** (`Edit|Write|apply_patch|create_file|replace_string_in_file|multi_replace_string_in_file`). It is the hard contract for edits, not for MCP writes.
   - MCP writes (`record_event`, `close_slice`, `review_findings`, `review_runs`, `get_handoff_state`, …) are covered by the rationale/shape hooks and by internal write-context attribution; an MCP write from the wrong cwd may warn or attribute to the cwd-resolved task rather than block. The runbook's job is to prevent that drift, not to rely on a blocker that does not fire here.
   - If the implementation task's `target_branch` has no linked worktree yet, do **not** call `set_handoff_state`, `record_event`, `close_slice`, `review_findings`, or `review_runs` against that implementation task. The write side fails with `WorktreeNotFoundError`. Safe choices are: run `make task-start TASK=<task-ref> OBJECTIVE="..."` once the accepted baseline exists, use a `target_branch=main` `MAINT-*` row for planning/review work, or wait until the feature worktree exists.

## Convergence Criteria

- The task branch was created from an approved plan and stayed isolated from `main`.
- Slice work closed through recorded `close_slice` decisions.
- Branch-complete review ran once, at most one fix wave was applied, and no unresolved HIGH finding remains for the main gate.
- `integrity_check(payload={"kind":"close","enforce":true})` passed on the final branch HEAD.
- The task ended through canonical cleanup after exact-tip integration; a `cleanup_pending` receipt remains incomplete and retryable.
- Root worktree is back on `main`, the feature branch is no longer active, and `DASHBOARD.txt` reflects the closed task with `CURRENT_TASK.json` available on demand.

## See Also

- [../tdd/body.md](../tdd/body.md)
- [../incremental-implementation/body.md](../incremental-implementation/body.md)
- [../commit2git/body.md](../commit2git/body.md)
- [../../../docs/workbay/rules/lifecycle-recovery.md](../../../docs/workbay/rules/lifecycle-recovery.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
