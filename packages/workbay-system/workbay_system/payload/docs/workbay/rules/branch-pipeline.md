# Deterministic branch pipeline

One state machine moves every branch from intent to reaped. It is harness- and
agent-agnostic: any coordinator — human or agent, in any harness — must be able
to resume the pipeline from durable state alone (lane rows, briefs, committed
artifacts, git), with no memory of who ran the previous step or on which
backend. Failures are protocol transitions, never judgments about a model or
vendor; recovery must be mechanical.

Companion rules: dispatch/brief mechanics live in
[offload-remote-playbook.md](offload-remote-playbook.md); reap/retention
mechanics live in
[inflight-worktree-lifecycle.md](inflight-worktree-lifecycle.md); review depth
and severity rubric live in [branch-review-guide.md](branch-review-guide.md).
This document owns only the pipeline: the states, the transitions, and the
recovery arms. Three transitions are enforced by machinery, not prose:
`make review-stage` (state 2), `make review-adjudicate` (state 5), and
`make merge-gate` plus the Bash lifecycle guard (state 7) — see
`Makefile.d/review-pipeline.mk` and `scripts/review_pipeline.py`. The doc is
the map; the primitives are the road.

## States

```
PLANNED → IMPLEMENTING → STAGED_FOR_REVIEW → IN_REVIEW → ADJUDICATING
        → (REVISING → STAGED_FOR_REVIEW)* → MERGING → REAPED
```

Every state is re-enterable. The current state is derivable from durable
artifacts only:

| State | Durable evidence |
|---|---|
| PLANNED | lane row exists (`manage_worktree_lane upsert`), no brief consumed |
| IMPLEMENTING | brief recorded + dispatched; lane worktree/branch exist |
| STAGED_FOR_REVIEW | review worktree holds a committed `docs/reviews/_input-<slug>-<sha>.patch` |
| IN_REVIEW | review lane row active with a recorded brief |
| ADJUDICATING | committed review doc(s) whose first line is `VERDICT: MERGE\|REVISE` |
| REVISING | REVISE verdict recorded; fix commits landing on the subject branch |
| MERGING | verdict gate passed; merge commit pending or landed on integration |
| REAPED | branch deleted, worktree removed, lane rows closed |

## 1 — PLANNED → IMPLEMENTING

- Lane row first (`upsert`), then manifest (materialized file **and** row),
  then the brief, then dispatch. A row without a manifest, or a manifest
  without a brief, is not dispatchable — fix the missing artifact, do not
  work around it.
- The brief carries context injection: a deterministic code-structure packet
  (anchors, blast radius, snippets) plus a prior-art section from the
  decision/finding store. A brief without both is incomplete — reviewers and
  implementers must weigh the change against recorded history, not rediscover
  it.
- The implementation product is one or more commits on the lane branch. A
  patch file is transport, not a commit: if the turn ends with an uncommitted
  product, salvage it (commit in the lane's own worktree) before anything
  else.

## 2 — IMPLEMENTING → STAGED_FOR_REVIEW

Primitive: `make review-stage SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>]`
— writes and commits the input patch, derives the proof-of-reading keys into a
gitignored dispatcher-held sidecar (`.task-state/review-por/`).

- Cut a review worktree from the integration head (`rev<N>-<slug>`), and
  commit the subject as `docs/reviews/_input-<slug>-<sha>.patch`. The patch
  file is the review subject of record — review sandboxes may have no history,
  so the subject must be a committed file, never a ref.
- Derive proof-of-reading keys from the patch (line count, content hash, two
  sampled lines, file count) and hold them **outside** the brief. They gate
  harvest, not dispatch.
- Set the lane row `test_cmd` to gate on the verdict line of the expected
  review doc. The gate is content-derived, not exit-code-derived.

## 3 — STAGED_FOR_REVIEW → IN_REVIEW

- Review brief structure is fixed: turn budget, proof-of-reading block,
  subject description, prior art, lenses, bounded output contract
  (max findings, exact output path, exact commit ceremony).
- Fan out: N remote review lanes + 1 local reviewer slot. The local slot is
  not optional; it is the control that catches instrument-level failures the
  remote lanes share.

## 4 — Failed-turn recovery (deterministic, applies to every dispatch)

State the failure as *observed artifact state*, never as a backend diagnosis.

1. **Turn ended with no product** (no findings block / no commit): the pass
   has consumed the recorded brief and left the lane `idle`. Recovery:
   re-record the full brief, re-dispatch **once**.
2. **Before any re-dispatch**, probe the lane sandbox/worktree for an
   uncommitted product. Salvage beats retry: a failed turn usually still
   landed its work somewhere.
3. **Second identical failure** on the same lane: stop dispatching. Inspect
   worker artifacts; hash-group the outputs of all failed lanes before
   blaming a schema or a backend. Identical hashes point at the shared
   instrument (brief, schema, gate), distinct hashes at per-lane state.
4. A failure classification that no artifact supports (e.g. an auth or
   rate-limit label on a lane with zero tool calls) is unverified — verify
   against the sandbox before acting on it.

## 5 — IN_REVIEW → ADJUDICATING → verdict

Primitive: `make review-adjudicate SUBJECT=<branch> PATCH=<input patch>
DOCS="<review docs>"` — re-derives the keys at gate time, discards docs whose
proof fails or whose first line is not a verdict, counts MEDIUM+ findings, and
writes the adjudication artifact (`.task-state/review-adjudication/`).

- Harvest the review doc; verify proof-of-reading keys byte-for-byte,
  re-deriving the reference values at gate time (never trust cached ones).
  A failed proof discards that review, not the pipeline.
- The local slot verifies each finding's claimed paths and line numbers
  against the subject before the finding counts. Citation accuracy is not
  claim accuracy — check both.
- Verdict rule: MERGE requires no MEDIUM+ finding surviving verification
  across all counted reviews. Any surviving MEDIUM+ → REVISE.

## 6 — REVISE loop

- Fix on the **same subject branch**; then re-stage (state 2) with a fresh
  input patch and fresh proof keys. Never merge a review branch to land its
  subject — the review branch carries only its review doc.
- Brief the defect **class**, not the fixed site, when re-dispatching related
  work.

## 7 — MERGING

Primitive: `make merge-gate SUBJECT=<branch>` — refuses unless the
adjudication artifact exists, says MERGE, and matches the subject's current
tip. The Bash lifecycle guard enforces the same check on any raw
`git merge feature/...`, so an agent that skips the primitive is refused at
the merge attempt, not after it.

- Pre-merge checks in the subject worktree: suite gate is
  *no-new-failures vs the pinned baseline*, never bare green; dirty-tree
  check; verdict gate.
- Merge to integration from the integration root. Record a decision in the
  task store for **every** merge (subject, verdict, evidence). Commit
  messages are neutral prose — no tool, model, or vendor references, no
  generated-by trailers.
- Harvest the counted review findings into the findings store as part of the
  merge step, so downstream close-checks see the review that justified the
  merge.

## 8 — MERGING → REAPED

Reap is part of the merge transition, not deferred hygiene:

- Run the registry-driven sweep (worktree removal first, then safe branch
  delete from the integration root) after every merge batch.
- Close the lane rows (`close`) — a deleted branch with a live row strands
  the row; a closed row with a live branch strands the branch. Both halves
  are the merge owner's job.
- Verify: the branch is gone, the worktree is gone, the rows are terminal.
  Only then is the branch REAPED.

## Invariants (hold in every state)

- Durable state is the only handoff medium. If a step's outcome exists only
  in an agent's context, the step is not done.
- Every destructive action (branch delete, worktree remove) requires a
  positive guard on that path — a refusal further down the chain is a
  fail-safe, not the guard.
- Probes fail closed: an unknown (`None`) answer from any merged-ness,
  ownership, or liveness probe means *skip*, never *proceed*.
- One writer per branch at a time: implementation lane, review lane, and
  merge owner serialize on the subject branch.
