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

Semantic capability: when consuming `semantic_reinjection_packet` (or other semantic surfaces), read response `embeddings_mode` + `semantic_degrade_reason` and follow [semantic-capability](../../docs/workbay/rules/semantic-capability.md) — surface a non-null degrade; never treat lexical-only output as full semantic success under a verified install.

## Core Process

1. At session start, run `make doctor LIFECYCLE_ARGS=--json` first — one structured `DoctorReceipt` answers the env / mcp / branch / lifecycle / dashboard / hooks question in a single call and points at the next safe command. Drop down to `make context`, `make status LIFECYCLE_ARGS=--json`, and (when more than one task may be active) `make tasks LIFECYCLE_ARGS=--json` only as the deeper-load follow-up. Use bounded reads unless you truly need full task history. When you only need to enumerate tasks rather than load one, `list_handoff_rows(status_filter=[...])` is the lightweight index — cheaper than repeated full `get_handoff_state` reads, and the fallback path when a harness breaks the identity read.
2. Confirm the active task, branch, and worktree align. If they do not, switch shells or fix task state before editing.
3. During work, record decisions, blockers, tests, and findings through MCP writes instead of chat-only notes.
4. For slice-complete decisions, do not retype the regex from memory. Prefer `close_slice(author_tag=..., work_ref=..., slug=...)` or run `validate(payload={"kind": "decision_id", "decision": ..., "decision_kind": "slice_complete"})` first, and treat `get_handoff_state(sections="identity").data.limits.write.slice_complete_decision_id` as the authoritative registry for the canonical form, segment rules, and examples. Valid: `cdx_slice_complete_plan0004_contract_pinning_and_docs`. Invalid: `cdx_slice_complete_plan0004_contract-pinning-and-docs`.
4a. Updates to an existing handoff row carry an optimistic-concurrency guard. Fetch `get_handoff_state(sections="identity").data.active.revision` first and pass it as `expected_revision` on `close_slice` / `set_handoff_state`; never guess it. An `expected_revision is required` or `Revision conflict` rejection means re-read identity and retry with the fresh revision — not a reason to drop the guard.
5. After each state-changing write that does not already regenerate views server-side, run `render_handoff(kind='dashboard')` so the operator-facing cross-task view stays current. Use `render_handoff(kind='current_task')` only as an on-demand fallback when a task-scoped machine snapshot is specifically needed. Treat generated task views as outputs, never as hand-edited logs.
6. If an MCP response envelope carries `compaction_recommended: true`, treat it as a prompt to persist session state before continuing. Resolve the transcript path from the active harness using `docs/workbay/contracts/harness-protocol.yaml`: read the harness-specific `env_var` first, then fall back to the configured `fallback_glob` only if the env var is unset. When a transcript path resolves, call `compaction(operation="record", transcript_path=..., task_ref=..., harness=..., session_id=...)` before proceeding with more work.
7. If transcript discovery fails or the current harness is not declared in the contract, warn the user and skip compaction rather than guessing a path. The contract's `unknown_harness: warn_and_skip` rule is authoritative.
7a. To silence internal compaction at runtime (advisory + Stop hook together), use the unified disable surface (internal) — do not edit YAML. Workspace-default: `make compaction-disable` (or `compaction(operation="disable")`). Per-task: `make compaction-disable TASK=<ref>`. Re-enable with `compaction-enable`; inspect with `compaction-status [TASK=<ref>]`. The `WORKBAY_HANDOFF_COMPACTION_DISABLED` env var still works and takes precedence over DB rows. When disabled, advisory envelopes carry `disabled=true, disabled_source=<env|db>` and the dashboard's Needs Attention prints `compaction: disabled via <source>`. Host-harness compaction (Claude Code, Codex, etc.) is unaffected.
8. When changing task focus mid-session, use `switch_task` as the safe transition path so the outgoing task is archived before the new task becomes active.
9. Only archive completed work after `set_handoff_state(status="done", status_only=True)`. Never archive a task that is still `in_progress`.
10. End the session with task state aligned to reality: active task correct, blockers explicit, generated task view current.
10a. Record concrete follow-ups as `next_actions` (operations add / update / complete / skip) so the next session resumes from a durable queue instead of re-deriving them from chat.
10b. To move an in-flight task across machines or checkouts, `export_handoff_state(task_ref=..., output_path=...)` writes a JSON snapshot of its decisions, findings, and test evidence; `import_handoff_state(input_path=..., mode="merge"|"replace_task")` rehydrates it in the other clone. This is the portable-handoff path — the same state every harness reads, carried between environments.
11. In the user-facing close-out, print a compact MCP write receipt with row ids from the tool responses. Use this shape: `MCP writes: <summary>; test_result id <id>; decision id <id> (<decision_key>); review_run id <id>. DASHBOARD.txt refreshed. Handoff updated: decision <decision_key> recorded.` Omit clauses that do not apply, but do not replace ids with a prose-only "recorded" claim.

## Pre-Worktree Write Guard

When an implementation task's `target_branch` does not yet have a linked worktree, do not aim MCP writes at that implementation row. `set_handoff_state`, `record_event`, `close_slice`, `review_findings`, and `review_runs` can fail with `WorktreeNotFoundError` because the write actor cannot be grounded to a real worktree.

Use one of these safe choices instead:

1. Run `make task-start TASK=<task-ref>` once the accepted baseline exists.
2. Use a `target_branch=main` `MAINT-*` task for planning or review work that must be recorded before the feature worktree exists.
3. Wait to write implementation-task MCP state until the feature worktree exists and is the cwd.

## Common Rationalizations

| Rationalization | Why it fails | Required action |
|---|---|---|
| "I already know the state, so I don't need `load_session`." | Session memory drifts and other agents may have written new findings or blockers. Skipping the read turns shared state into guesswork. | Load the current task state before acting on assumptions. |
| "I'll archive while the task is still in progress so I can clean things up later." | Archiving an active task lies to the dashboard and breaks safe task switching. Downstream agents will assume the task is closed or restorable from the wrong snapshot. | Leave it active until `set_handoff_state(status="done", status_only=True)` is true, then archive. |
| "I'll just update `CURRENT_TASK.json` directly." | Generated task views drift immediately from MCP when hand-edited and disappear on the next regeneration. | Regenerate the appropriate view from MCP instead of editing it. |

## Red Flags

Each flag is a re-entry trigger. Stop and re-enter at the step shown.

| Flag | Re-entry point |
|---|---|
| `make context` shows branch or worktree drift | Step 2: realign shell context before continuing. |
| Task write made, but `DASHBOARD.txt` still reflects old state | Step 5: regenerate the dashboard view. |
| Session switches tasks by calling `set_handoff_state` directly over another active task | Step 8: switch with `switch_task` so the outgoing task archives safely. |
| Archive requested while status is still `in_progress` | Step 9: set status truthfully first, then archive only when done. |

## Recovery

- If session context is stale, rerun `make context` and `load_session` instead of guessing what changed.
- If a write landed on the wrong task, stop and repair task state before continuing with more writes.
- If `DASHBOARD.txt` or `CURRENT_TASK.json` is stale, regenerate it; do not patch the markdown manually.
- If a task was switched unsafely, restore the intended active task and archive state through `switch_task` / `set_handoff_state(status_only=True)` in canonical order.

## Convergence Criteria

- Session started from aligned task, branch, and worktree context.
- Relevant MCP writes are recorded for work performed in the session.
- The response names the MCP row ids written during the session.
- `render_handoff(kind='dashboard')` was run after non-atomic state-changing writes, with `render_handoff(kind='current_task')` reserved for on-demand task snapshots.
- Task switches happened through `switch_task`, not by overwriting the active row ad hoc.
- Archive operations only happened after `set_handoff_state(status="done", status_only=True)`.

## See Also

- [../branch-lifecycle/SKILL.md](../branch-lifecycle/SKILL.md)
- [../../../docs/workbay/in-session-skills.md](../../../docs/workbay/in-session-skills.md) — operator skills callable in-session (not portable `/commands`)
- [../../../docs/workbay/lifecycle-map.md](../../../docs/workbay/lifecycle-map.md)
- [../../../docs/workbay/instructions.md](../../../docs/workbay/instructions.md)
