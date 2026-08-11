# Lane durfix-payload-02 — an invalid handoff_action must not destroy a committed turn's report

Task `internal`. Branch `feature/durfix-payload-01`
(you continue on the SAME branch, on top of `07eca4b3`).

This lane fixes a regression that lane `durfix-payload-01` (your predecessor on
this branch) introduced. It is a real merge blocker found at the gate.

## Read budget (hard cap)

Read ONLY these files, and only the regions you need:

- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/remote_exec.py`
  (focus `_extract_review_payload` ~line 390-428, and the caller ~line 1300-1360)
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/_result_text.py`
  (focus `is_shaped_result_payload` and the tier constants, ~line 25-90)
- `packages/mcp-workbay-orchestrator/tests/test_commitless_turn_is_not_reported_as_evidence.py`
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/grok_cli.py`
  (ONLY to check whether its payload extractor has the same defect)

Do NOT sweep the package. Do NOT re-read the whole test suite. A wide sweep
blows the input budget and returns nothing.

## Proof of reading (mandatory, answer before you change anything)

Answer these from the ACTUAL pre-fix source. I already know the answers and
will check them; a wrong or hand-waved answer means your patch is not trusted.

- **P1.** In `_extract_review_payload`, when the `envelope` is not None and
  fails `is_shaped_result_payload`, name every subsequent tier the function
  tries, in order, and state the exact value it returns when all of them fail.
- **P2.** Quote the exact current value of `KNOWN_HANDOFF_ACTIONS` and give the
  two source locations elsewhere in the package that pin the same enum.
- **P3.** In `test_commitless_turn_is_not_reported_as_evidence.py`, quote the
  one-line comment that states the preserve-versus-destroy principle for the
  commitless path (it appears next to a `FABRICATED_SUMMARY` assertion).

## The defect

`_extract_review_payload` returns `None` in two very different situations:

1. no payload was found at all, and
2. a well-formed dict payload WAS parsed, but its `handoff_action` is not in
   the closed two-member enum.

The caller maps `None` to `result_unparseable` and synthesizes the summary
`"grok-remote turn committed but its structured result was unparseable"`,
discarding the worker's real `summary` and emptying `tests_run`.

So a turn that COMMITTED and emitted parseable JSON loses its entire report
because one field value was off-enum. That is the exact failure class this
branch was chartered to eliminate — reintroduced from the opposite direction.

### Measured A/B (I ran this; turn committed, payload parses in every case)

Question asked of each: is `summary` preserved and is `tests_run` kept?

| `handoff_action` | main | this branch (07eca4b3) |
|---|---|---|
| `"merge_ready"` | preserved | preserved |
| `"finished"` (off-enum) | preserved | **DESTROYED** |
| absent | destroyed | destroyed |
| `null` | preserved | **DESTROYED** |

The off-enum and null rows are the regression. The `absent` row is broken on
main too — fix it in the same pass.

Repro (green on main, red here):

    tests/test_commitless_turn_is_not_reported_as_evidence.py::test_a_turn_that_did_commit_is_reported_verbatim

That test file is on main and was NOT touched by this branch.

## What you must NOT do

- **Do NOT widen `KNOWN_HANDOFF_ACTIONS`.** It is correct. The enum is closed
  at exactly two members, `merge_ready` and `needs_guidance`, and it is pinned
  in two other places in the package. Widening it makes this module disagree
  with the validators, and anything else still hard-fails downstream.
- Do NOT relax `is_shaped_result_payload`. Strict SHAPE is right for
  SELECTION.
- Do NOT "fix" this by editing the failing test's fixture to use
  `merge_ready`. That hides the durability defect instead of fixing it.
- Do NOT touch `scripts/remote_agent.sh` or any file outside the list below.

## Required change

Decouple *action validation* from *payload selection*.

1. Keep the strict shape test for choosing among candidates.
2. When no candidate passes the shape test but a well-formed dict payload was
   nonetheless parsed, return that dict stamped with a NEW, distinct recovery
   tier (e.g. an `unshaped` tier constant alongside the existing balanced and
   embedded tiers) instead of returning `None`. Use the same tier/precedence
   order you already walk, just without the shape filter.
3. The caller must then preserve `summary` and `tests_run` from that payload,
   while clamping `handoff_action` fail-closed to `needs_guidance` so an
   invalid action never flows downstream as if valid, and while still marking
   the turn as not merge-ready (keep a typed blocker so nothing reads it as a
   green pass). The precedent for fail-closed defaulting already exists in
   `backend_adapter.from_dict`, which defaults an ABSENT action to
   `needs_guidance` rather than discarding the payload.
4. Check whether the grok adapter's payload extractor has the same
   `None`-conflation defect. If it does, fix it the same way. If it does not,
   say so explicitly in your report and change nothing there.

The commitless path (no commit) must keep its current behaviour exactly: it
still marks the summary UNVERIFIED and still banks no test evidence. Only the
committed-but-unshaped path changes.

## Tests you must add

Add a regression test that pins the full A/B table above for a COMMITTED turn:
for `handoff_action` off-enum, `null`, and absent, assert that `summary` is
preserved verbatim, that `tests_run` survives, that the emitted
`handoff_action` is clamped to `needs_guidance`, and that the turn is not
reported as merge-ready.

Then MUTATION-TEST your own new test: re-introduce the `return None` behaviour,
confirm the new test FAILS, then restore the fix and confirm it passes. State
both observed outcomes in your report. A test you did not watch fail is not
evidence.

## Verification (scoped — do not run the full suite)

    cd packages/mcp-workbay-orchestrator && WORKBAY_DISABLE_PYTEST_PATH_GUARD=1 ../../.venv/bin/python -m pytest tests/test_commitless_turn_is_not_reported_as_evidence.py tests/test_result_text.py tests/test_shaped_payload_recovery_contract.py tests/test_remote_exec_selfverify_tolerant.py tests/test_grok_cli.py -q -p no:randomly

All of these must be green, including the pre-existing pins. In particular
`test_result_text.py::test_extract_result_payload_does_not_unwrap_an_unknown_action`
covers a DIFFERENT function (`extract_result_payload` in `_result_text.py`) and
must stay green — scope your change so you do not disturb it.

## Files you own

- `.../orchestration/adapters/remote_exec.py`
- `.../orchestration/adapters/_result_text.py`
- `.../orchestration/adapters/grok_cli.py` (only if it shares the defect)
- any NEW test file you add under `packages/mcp-workbay-orchestrator/tests/`
- `LANE-REPORT.md` (overwrite with your report for this lane)

## Required actions before you finish

1. Run the scoped verification command above and confirm it is green.
2. `git add -A && git commit` your work on `feature/durfix-payload-01` with a
   conventional-commit subject. **You must commit.** Work that is not committed
   is destroyed when the lane ends.
3. Write `LANE-REPORT.md` with: your P1/P2/P3 proof-of-reading answers, the
   mutation-test observations (both arms), the exact test output tail, and an
   explicit statement of whether the grok adapter shared the defect.

## Commit-message policy (strict)

Never credit any LLM, model, or vendor in git history. No `Co-Authored-By`
trailer, no "generated by", no assistant or vendor name anywhere in the commit
message or the report. Plan/task provenance only.

## Reasoning posture

Follow the heuristics canon: https://github.com/darce/heuristics-canon
Cite stable rule IDs where they apply. Do not pin or mention a canon version.
