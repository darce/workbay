# Investigate

## Overview

Use this skill for systematic root-cause debugging when a test failure, runtime error, or unexpected behavior needs diagnosis before a fix is attempted. Record the evidence and outcome durably, including an inconclusive outcome when the available evidence cannot establish a cause.

## Trigger

Use this skill when the request matches any of:

- "investigate", "debug", "diagnose", or "root cause"
- "why is this failing", "what's causing", or "trace this"
- a presented test failure or error whose cause is not immediately obvious
- a review finding that has reopened two or more times

Do not use this skill for:

- known bugs with obvious fixes
- review or audit work that belongs to the `review` skill

## Goal

Identify and verify the root cause of a defect before applying a fix, then preserve the decision/evidence trail in MCP so the diagnosis survives handoff. Only confirmed defects become review findings.

## Canonical Policy

- Use [../../instructions.md](../../instructions.md) for startup, handoff, and evidence-logging policy.
- Use [../../rules/development-workflow.md](../../rules/development-workflow.md) for slice checklist and regression sweep conventions.
- Use [../../rules/branch-review-guide.md](../../rules/branch-review-guide.md) for bug-finding heuristics such as variable identity, cross-method contracts, boundary values, retry lifecycle, and stale-file verification.
- Use this skill for investigation phasing and MCP recording discipline only; broader process policy lives in the linked docs.
- Heuristics: follow the [heuristics canon pointer](../../../docs/workbay/rules/heuristics-canon.md) (one pinned, digest-verified release). Route this skill's artifact as **failing test or incident investigation**, keep only the rows whose trigger fires, apply the Verification of every card that lists a kept ID, and cite `[FAM-NN]` with the rule's bold name and Source cell. `B` blocks, `S` needs a named exemption, and a `J` rule you lack context for is escalated, not skipped.
- Semantic search / recall suggestions: read `embeddings_mode` + `semantic_degrade_reason` per the `find_related_prior_work` row in the [handoff MCP contract](../../../docs/workbay/contracts/workbay-handoff-mcp.md) (MCP Tool Surface) before treating semantic results as authoritative, and surface a non-null degrade rather than passing lexical-only output off as semantic ([AGT-10] **Degrade loudly** (The Art of UNIX Programming, ch. 1)).

## Core Process

0. **Ensure task scope before any cwd-resolving MCP read.** `search_handoff`, `review_findings`, `get_handoff_state` without `task_ref` all resolve from cwd. On cold start:
   - Run `make status LIFECYCLE_ARGS=--json` from the target worktree first so the initial orientation path stays on the public facade.
   - If more than one active task may explain the symptom, run `make tasks LIFECYCLE_ARGS=--json` before any raw MCP read.
   - **Resumption:** work from the existing task's `target_worktree_path`; pass `task_ref` explicitly.
   - **Ad-hoc investigation on main:** register a maintenance task first — `set_handoff_state(task_ref="MAINT-<slug>-<YYYYMMDD>", objective="...", status="in_progress", target_branch="main")`.
   - **`Ambiguous active task` error:** use the canonical escape hatch in [lifecycle-recovery.md § Surface 5](../../rules/lifecycle-recovery.md#surface-5-ambiguous-active-task-raise) (`task_ref=` / `TASK=`, or `make task-reap` then `make context`; exit `2` on ambiguity).

1. Collect symptoms before forming hypotheses ([DBG-02] **Facts and observation before mechanism** (Debugging: The 9 Indispensable Rules + Modern Software Engineering, ch. 7)):
   - exact error output
   - reproduction steps
   - recency checks with recent git history
   - related MCP findings via `search_handoff(...)` (tokens/IDs) and `review_findings(review={"operation":"list", ...})`. For paraphrase / like-this neighbors do not invent FTS synonyms; call `find_related_prior_work` and branch on `status`.
   - active task context via `get_handoff_state(read_profile="hot_summary")`
2. Bind the current session and assigned lane from the runtime/task inputs. Open a decision/evidence trail with `record_event(event={"event_kind":"decision", "session":<session>, "task_ref":<task>, "decision":"<tag>_investigate_open_<task>_<slug>", "rationale":<symptom, baseline, scope and budgets>, "actor":{"commit_sha":<HEAD>, "lane_id":<lane>}})` ([AGT-07] **Externalize at discovery** (The Programmer's Brain, ch. 11)). A symptom or speculative hypothesis is not a review finding. Bound elapsed time, experiment count, evidence reads and changed paths before starting; respect tighter task limits. If none are supplied, use a 30-minute deadline, at most six experiments per component and only the declared owned paths; record an evidence-read cap appropriate to the component.
3. Build a codemap dependency graph of the suspected failure paths and experiments, then partition independent defect components ([GRPH-06] **Partition by connected components first** (Graph Theory with Applications, ch. 1)). Trace calls/data flow across the relevant boundaries; race, stale-state, contract, boundary, integration and config patterns are prompts to inspect, not proof. Add an ordering edge only for an actual input or a named resource/side-effect conflict ([GRPH-32] **An edge exists only where data moves** (practice label bootstrap)). Schedule prerequisite experiments before dependents; separate defects need not serialize. Shared mutable fixtures or overlapping writes require isolation or a named ordering constraint.
4. For each hypothesis, record its ID, component, observed facts, predicted result, falsifying result, exact test/observation command, baseline and one variable to change ([DIAG-03] **Falsifiable hypothesis** (Why Programs Fail, ch. 6)). Reproduce the failure before changing code ([DBG-01] **Reproduce before changing** (Debugging: The 9 Indispensable Rules)). Run one controlled experiment per hypothesis; independent experiments may run concurrently with isolated state. Use the [dispatch contract](#independent-group-dispatch) for independent investigation groups and, once diagnosed, independent implementation groups.
5. Record each attempt's command, output, environment/commit and outcome as confirmed, refuted or inconclusive. Refuting a hypothesis does not dispose of the symptom. Nonreproduction is inconclusive: retain attempts and name the next discriminator (for example the missing fixture, environment or observation).
6. When progress stalls, obtain a fresh view of the evidence/failure path, revise the experiment plan and record the decision. Three refutations alone are not a stop condition. Continue useful experiments within the declared budget; on exhausted budget, missing input or an unresolved boundary, record the remaining uncertainty, next discriminator and an escalation/blocker. Stop without a speculative fix; unaffected components may continue within their budgets.
7. Only a confirmed defect gets `review_findings(review={"operation":"record", "session":<session>, "task_ref":<task>, "review_mode":"branch", "finding_id":<finding-id>, "severity":<high|medium|low>, "file_path":<workspace-relative-path>, "description":<confirmed defect and evidence>, "actor":{"commit_sha":<HEAD>, "lane_id":<lane>}})`; reuse an existing matching finding ID rather than duplicating it. After confirming the root cause, implement the smallest in-scope fix and rerun the actual failing sequence. Add a regression test when behavior/risk warrants one; documentation protocol corrections may use existing materialization/contracts without an implementation-mirroring test.
8. Resolve ONLY the explicitly diagnosed and fixed IDs through `review_findings(review={"operation":"resolve", "task_ref":<task>, "finding_ids":[<fixed-id>], "verification_evidence":<command, result and exact tested HEAD>, "resolution_notes":<how this commit fixes each named finding>, "actor":{"commit_sha":<HEAD>, "lane_id":<lane>}})`. The commit-backed path classifies those IDs against the current commit and populates verification provenance. Before claiming resolution, inspect each returned finding's status and guard evidence and confirm the receipt's `workspace_commit_sha` and each result's `verified_commit_sha` equal the tested HEAD; retain the receipt. If HEAD changes after verification, rerun the failing sequence and record fresh evidence before resolving. Never use `all_open:true` or manually claim fixed through an update. Leave unrelated findings untouched. For an existing confirmed finding whose investigation is inconclusive, defer that explicit ID with attempts and the next discriminator in `resolution_notes`; do not infer `wontfix` from nonreproduction. If its task is orphaned/already done, use the `disposition` operation for the named ID with evidence and rationale instead.
9. Record the outcome with `record_event(event={"event_kind":"decision", ...})`: accepted/refuted/inconclusive hypotheses, confirmed finding IDs, changed paths, commit-backed verification, attempts, remaining uncertainty and next discriminator. Keep provenance walkable ([GRPH-14] **Record provenance as a walkable lineage graph** (Building Knowledge Graphs, ch. 8 + Mental Models, ch. 8)).
10. Refresh the operator view with `render_handoff(kind='dashboard')` if needed; render `CURRENT_TASK.json` only for an explicit legacy export request.

### Useful commands

```bash
git log --oneline -20
git diff --stat HEAD~5..HEAD
```

Run the smallest relevant test command for the affected surface before widening the scope.

## Common Rationalizations

- "I already know the fix, so I don't need to prove the cause."
- "A quick workaround is fine for now."
- "Three refutations prove the symptom is invalid, or justify a guessed fix."
- "I can skip the MCP trail because this is just debugging."

## Red Flags

- A proposed fix does not explain how the symptom was produced.
- The investigation exceeds its declared work/evidence budget or owned paths.
- Cascading failures suggest the wrong architectural layer is being debugged.
- The same issue has been reopened multiple times, but prior findings are not being consulted.
- A fix is about to land before the root cause is recorded.

## Recovery

- If MCP is unavailable, capture decisions/evidence in the structured worker report or local notes for coordinator durable recording. Report the missing durable transfer; never fabricate MCP success.
- If the symptom cannot be reproduced, record an inconclusive outcome, the reproduction attempts and the next discriminator. Defer an existing confirmed finding explicitly; do not create a speculative finding or automatically use `wontfix`.
- If the fix requires changes outside the current lane or owned paths, record a cross-lane blocker and escalate instead of crossing boundaries silently.
- Consult a prior finding for the same symptom and reuse its ID; reopen it only when fresh evidence confirms recurrence, not merely because an investigation opened.

## Independent Group Dispatch

Every group needs explicit inputs and a structured output contract ([GRPH-33] **A node without a contract cannot be parallelised** (practice label bootstrap)). Pass the task/component IDs, exact symptom and reproduction, hypothesis predictions and one-variable experiments, base SHA/worktree, owned paths and read/write mode, commands and budgets, upstream inputs/conflicts, codemap symbols/edges plus freshness/coverage/source fallback, fired canon rows and their Source cells, semantic query status/model/degrade metadata and retrieved prior IDs/snippets (including null snippets), and prior decisions/findings. Similarity is advisory; do not invent rationale or claim reinjection success.

Dispatch independent ready groups through the `/wb-offload` protocol, which owns backend probing, preflight/materialization, pass submission and joins over the existing `scripts/remote_agent.sh` transport; the live registry/AgentSpec resolves capabilities, model, effort and timeouts. Join only this investigation's passes; a wait deadline or still-running result is not completion. Admit concurrency according to measured capacity and actual conflicts, without a fixed model roster or guessed worker cap. Read-only diagnosis precedes implementation within each component; independent components have no artificial global barrier.

Require each result to identify the component/hypotheses, predictions, experiments/commands/outputs, confirmed/refuted/inconclusive outcomes, source anchors, finding IDs, changes and commit SHA, verification, blockers and next discriminator. The coordinator adjudicates and owns finding record/resolve writes, resolving only accepted IDs. If dispatch is unavailable, record the capability/coverage limit and work within the available bounded scope; honor any remote-required mode without silently substituting a backend.

## Codebase-graph capability (optional)

If a codebase-graph MCP is connected, check project/index freshness and evidence-path coverage, then use `get_architecture`, `trace_path` (direction=both) and `query_graph` to trace failures across symbols, calls and data flow. Check affected symbols with `detect_changes` after edits. Verify relevant source at the cited anchors. If graph coverage is stale/partial or the tool is absent, build the bounded dependency map from source reads/searches and record the limitation; do not claim complete graph coverage.

## Convergence Criteria

- The outcome is recorded as diagnosed or inconclusive/deferred with attempts and the next discriminator; only a confirmed cause permits a fix.
- An applied fix is minimal and verified by the relevant failing sequence or existing documentation contracts, with a regression test when warranted.
- Only confirmed defects have findings; only explicitly diagnosed/fixed IDs are resolved with commit-backed evidence.
- The investigation outcome is recorded with `record_event(event={"event_kind":"decision", ...})`.
- `render_handoff(kind='dashboard')` has been run after state-changing writes that did not already refresh the operator view.

## See Also

- [review](../review/body.md)
- [lifecycle-recovery.md](../../rules/lifecycle-recovery.md) — Ambiguous active-task escape hatch (Surface 5)
- [branch-review-guide.md](../../rules/branch-review-guide.md)
- [development-workflow.md](../../rules/development-workflow.md)
