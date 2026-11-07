# Handoff Lifecycle

## Overview

Use this skill for the session-scoped handoff loop: enter the right task, load only the needed state, keep generated task views current after writes, and switch tasks safely when the session focus changes.

## Trigger

Use this skill when:

- starting or resuming a work session
- verifying task / branch alignment with `make context`
- switching from one task to another mid-session
- ending a session after recording decisions, blockers, or findings

Do not use it for branch review execution or the TDD loop inside a slice.

## Goal

Keep MCP task state, shell context, and generated task views aligned throughout the session so another agent can resume work without reconstructing intent from chat.

## Canonical Policy

- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
- [../../../docs/workbay/rules/development-workflow.md](../../../docs/workbay/rules/development-workflow.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)

This skill owns hot-state loading, safe task switching, and the rule that generated task views are regenerated rather than edited by hand.

## Core Process

1. Start with `make doctor LIFECYCLE_ARGS=--json`; use its structured receipt to choose the next safe command. Load only the needed context with `load_session(read_profile="hot_summary", ...)`, or `make context` as its compound workflow. Read the newest **continuation packet** (`data.continuation` / `CONTINUATION`: `done_do_not_redo`, `next_actions`, `verified_anchors`, `gotchas`) as primary resume context. Use `make status LIFECYCLE_ARGS=--json` or `make tasks LIFECYCLE_ARGS=--json` for deeper orientation only when needed. `list_handoff_rows(status_filter=[...])` is the lightweight task index. On ambiguous active-task resolution, follow [lifecycle-recovery.md § Surface 5](../../../docs/workbay/rules/lifecycle-recovery.md#surface-5-ambiguous-active-task-raise).
2. Confirm task, branch, and worktree alignment before editing or writing task state. Use bounded read profiles from the [handoff MCP contract](../../../docs/workbay/contracts/workbay-handoff-mcp.md): `identity` for revision/limits, `hot_summary` for orientation, `open_items` for blockers/findings, and `review_packet` for review scope. Set a response budget when needed and inspect `data.read_shape` / `data.read_budget` for omissions before relying on completeness; reserve `full_debug` for a justified full-history read.
3. **Semantic response check (shared protocol).** Do not invent FTS synonyms for `search_handoff`; use `find_related_prior_work` for paraphrase prior art; use `semantic_reinjection_packet` with changed-path/codemap excerpts or rationale anchors for bounded context reinjection. Read each response's `status`, `embeddings_mode`, `semantic_degrade_reason`, and, when present, `skip_reason` before interpreting empty results or relevant lines. A skipped/unavailable result is a capability gap, not evidence that no prior work exists; non-null degrade must be reported: [AGT-10] **Degrade loudly** (The Art of UNIX Programming, ch. 1), via the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md). Use direct source/lexical evidence while the capability is unavailable. An unspecified install mode is not a verified-install receipt, and similarity only suggests candidates for source verification. For keyword/ID discovery use the original terms rather than invented synonyms. This check also applies to semantic selections returned by context refresh and codemap reinjection; do not claim indexing or reinjection success from a skipped packet.
4. During work, persist decisions, blockers, tests, and findings through `record_event` and their owning MCP writes. Include actual command/results, changed paths, branch, lane, and exact verified commit where applicable. Slice checks do not attest a later amended, rebased, or joined commit; reverify the resulting commit before its evidence is consumed.
5. Complete a slice with `close_slice(session=..., task_ref=..., author_tag=..., work_ref=..., slug=..., rationale=..., actor={"branch": ..., "commit_sha": ...}, changed_files=..., expected_revision=...)`. Use the structured rationale from the [slice-complete template](../../../docs/workbay/templates/slice-complete-template.md). Read `get_handoff_state(task_ref=..., sections="identity").data.active.revision` for the target task first; pass it on `close_slice` / `set_handoff_state` updates. On required-revision or conflict rejection, reread identity and retry with the fresh revision; never drop the guard. Read `data.limits.write.slice_complete_decision_id` for the current decision-id contract, or call `validate(payload={"kind": "decision_id", "decision": ..., "decision_kind": "slice_complete"})` rather than copying registry patterns into prose. `close_slice` leaves an unfinished task `in_progress`; slice completion does not close the task.
6. Regenerate task views after writes that do not do so server-side: `render_handoff(kind='dashboard')`; use `render_handoff(kind='current_task')` only for an on-demand task snapshot. Generated task views are outputs, never hand-edited logs. Check actual write responses for rejected writes or side-effect failures before reporting success.
7. On `compaction_recommended: true`, persist session state before continuing. Resolve the transcript from `docs/workbay/contracts/harness-protocol.yaml`: harness `env_var` first, then configured `fallback_glob` only if unset. With the resolved transcript path, aligned task reference, harness label, and actual harness session identifier, call `compaction(payload={"operation": "record", "transcript_path": transcript_path, "task_ref": task_ref, "harness": harness, "session_id": session_id})`; all five payload fields are required for record. The served API accepts only `payload`; do not use the legacy `compaction(operation="record", ...)` keyword form. Check the actual returned `compaction_id` and `db_row_id` before reporting durable compaction; on a failed write, record the failure and preserve unsaved state for retry. If discovery fails or the harness is unknown, warn and skip under `unknown_harness: warn_and_skip`; do not guess a transcript path. Use `make compaction-disable [TASK=<ref>]`, `make compaction-enable`, and `make compaction-status` when needed; host-harness compaction remains separate.
8. Change focus with `switch_task`; it creates/reactivates the target without archiving the outgoing task. Mark genuinely finished work with `set_handoff_state(status="done", status_only=True)` before archiving. Never archive `in_progress` work. Final task completion requires the final join and owning review/test gates, not only a slice receipt.
9. Persist concrete follow-ups with `next_actions` (add/update/complete/skip). Before ending with delegated work, open slices, or a successor handoff, call `continuation(payload={"operation": "save", "task_ref": task_ref, "done_do_not_redo": completed_work_md, "next_actions": next_actions_md, "verified_anchors": verified_anchors_md, "gotchas": gotchas_md})`. Supply Markdown strings for the four resume sections, including actual commands/results and exact verified commits in the anchors; at least one section must be non-empty. Add `"lane_id": lane_id` inside the payload when saving a lane-scoped packet. The served API accepts only `payload`, not operation/task/section keyword arguments. Require `ok: true` and retain `data.packet_id` / `data.source_id` from the actual save receipt before claiming success; a rejected save leaves successor context undurable and requires retry or an explicit coordinator-recording gap. Skip only when fully closed and archived with no successor work. [lifecycle-recovery.md § SessionStart](../../../docs/workbay/rules/lifecycle-recovery.md#sessionstart-cold-start-orientation-recipe) owns the resume recipe.
10. For moves across machines/checkouts, use `export_handoff_state(task_ref=..., output_path=...)` and `import_handoff_state(input_path=..., mode="merge"|"replace_task")`; preserve decisions, findings, and test provenance rather than reconstructing them from chat.
11. Close out with task state aligned to reality and compact write receipts from actual responses: applicable row ids, decision keys, and view refresh outcomes. If shared MCP is unavailable in a remote sandbox, emit decisions, findings, command/results, exact commit, and remaining gaps in the structured worker report for coordinator durable recording. Never fabricate MCP success or ids; block only operations requiring the unavailable transport and continue independent work within existing authorization.

## Pre-Worktree Write Guard

When an implementation task's `target_branch` does not yet have a linked worktree, do not aim MCP writes at that implementation row. `set_handoff_state`, `record_event`, `close_slice`, `review_findings`, and `review_runs` can fail with `WorktreeNotFoundError` because the write actor cannot be grounded to a real worktree.

Use one of these safe choices instead:

1. Run `make task-start TASK=<task-ref>` once the accepted baseline exists.
2. Use a `target_branch=main` `MAINT-*` task for planning or review work that must be recorded before the feature worktree exists.
3. Wait to write implementation-task MCP state until the feature worktree exists and is the cwd.

## Recovery

- Stale session context: rerun `make context` / `load_session` before assuming what changed.
- Wrong-task writes: stop and repair task state before further writes.
- Stale generated views: regenerate them; do not patch them manually.
- Unsafe task switch: restore focus with `switch_task`, then mark and archive only genuinely completed outgoing work.

## Convergence Criteria

- Task, branch, and worktree align; reads expose their actual scope and semantic capability status.
- Decisions, tests, findings, and exact-commit evidence are durable, with actual MCP receipt ids or an explicit coordinator-recording gap.
- Generated views reflect successful writes; task switches use `switch_task` and archives follow truthful `done` state.
- Open work has durable next actions and a continuation packet for its successor.

## See Also

- [../branch-lifecycle/body.md](../branch-lifecycle/body.md)
- [../../../docs/workbay/rules/lifecycle-recovery.md](../../../docs/workbay/rules/lifecycle-recovery.md)
- [../../../docs/workbay/in-session-skills.md](../../../docs/workbay/in-session-skills.md)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
