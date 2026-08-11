# Lane turnbudget-harm03 — a caller-supplied turn count must not be paired with a clock that cannot fund it

Task `internal`. Branch `feature/offload-turnbudget-01`
(you continue on the SAME branch, on top of `a21026aa`).

This is a real merge blocker found at the gate. The branch is otherwise green
(full host suite: 4155 passed, 0 failed) and is held solely on this defect.

## Read budget (hard cap)

Read ONLY these files, and only the regions you need:

- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/worker_daemon.py`
  (focus `_resolve_grok_cycle_bounds`, ~line 1195-1230)
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/offload_profiles.py`
  (focus `derive_grok_single_cycle_bounds`, ~line 84-91)
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/grok_lane_config.py`
  (ONLY the three constants near line 81)
- `packages/mcp-workbay-orchestrator/tests/test_worker_daemon.py`
  (ONLY the three `test_resolve_grok_cycle_bounds_*` tests near line 1463-1500)

Do NOT sweep the package. Do NOT re-read the whole test suite. A wide sweep
blows the input budget and returns nothing.

## Proof of reading (mandatory, answer before you change anything)

Answer these from the ACTUAL pre-fix source. I already know the answers and
will check them; a wrong or hand-waved answer means your patch is not trusted.

- **P1.** Quote verbatim the two-condition early-return in
  `_resolve_grok_cycle_bounds` that short-circuits when both bounds are already
  caller-supplied, and give its line numbers.
- **P2.** Give the exact current values of `GROK_MAX_TURNS_CAP`,
  `GROK_TIMEOUT_CAP` and `SECONDS_PER_TURN`, and name the module and line
  numbers that define them.
- **P3.** Quote verbatim the single expression in
  `derive_grok_single_cycle_bounds` that computes `timeout`.

## The defect

`_resolve_grok_cycle_bounds` returns early only when **both** `grok_max_turns`
and `grok_timeout` are caller-supplied. When a caller supplies **only**
`grok_max_turns`, the function derives `bounds` from `token_budget` alone — that
derivation has no knowledge of the caller's turn count — and then keeps the
caller's turns while taking the derived clock:

    grok_max_turns = config.grok_max_turns if not None else bounds["max_turns"]
    grok_timeout   = config.grok_timeout   if not None else bounds["timeout"]

The two halves are therefore paired incoherently.

### Worked example (this is the failure, verify it yourself)

`grok_max_turns=60`, `token_budget=8000`, no `grok_timeout`:

- derived `max_turns` = min(60, 8000 // 4000) = **2**
- derived `timeout`   = min(1800, max(60, 2 * 30)) = **60 seconds**
- result: the caller keeps **60 turns** against a **60-second** clock.

Sixty turns cannot run in sixty seconds. The lane dies on the wall clock with
work in flight. That is precisely the starvation this branch was chartered to
eliminate.

This branch already fixed the DOCUMENTATION half of this same defect
(the playbook now tells operators to pass `turn_timeout_seconds` alongside
`grok_max_turns`). Shipping that alone makes the code's contract depend on the
operator having read a page. Fix the code.

## Required change

In `_resolve_grok_cycle_bounds`, when `grok_max_turns` is caller-supplied and
`grok_timeout` is NOT, the derived timeout must be raised to at least

    min(GROK_TIMEOUT_CAP, grok_max_turns * SECONDS_PER_TURN)

so the clock actually funds the turns the caller asked for, while still
respecting the hard ceiling.

## What you must NOT do

- **Do NOT silently reduce a caller-supplied `grok_max_turns`** to fit a derived
  clock. That is a silent cap ([AGT-10]) and trades one defect for a worse one.
- Do NOT change `GROK_MAX_TURNS_CAP`, `GROK_TIMEOUT_CAP` or `SECONDS_PER_TURN`.
- Do NOT change `derive_grok_single_cycle_bounds` itself — other callers depend
  on its pure token-budget derivation.
- Do NOT touch `scripts/remote_agent.sh` or any file outside the list below.

## The inverse case — decide it explicitly and say so

There is a symmetric case: caller supplies `grok_timeout` but NOT
`grok_max_turns`, so the turns are derived while the clock is explicit. State in
your report what your change does in that case and why. Whatever you choose,
it must not silently cap a caller's value. A one-line justification is enough,
but an unaddressed answer counts as an incomplete lane.

## Tests you must add

Add a regression test that pins the worked example above: with
`grok_max_turns=60`, `token_budget=8000`, and no `grok_timeout`, assert the
resolved `grok_timeout` is at least `60 * SECONDS_PER_TURN` (capped at
`GROK_TIMEOUT_CAP`), and assert the resolved `grok_max_turns` is still exactly
60 (proving you did not silently cap it).

Also assert the existing behaviour is unchanged when BOTH values are supplied
and when NEITHER is.

Then MUTATION-TEST your own new test: revert your fix, confirm the new test
FAILS, then restore the fix and confirm it passes. State both observed outcomes
in your report. A test you did not watch fail is not evidence.

## Verification (scoped — do not run the full suite)

    cd packages/mcp-workbay-orchestrator && WORKBAY_DISABLE_PYTEST_PATH_GUARD=1 WORKBAY_DISABLE_INVOKING_HOOKS=1 python -m pytest tests/test_worker_daemon.py tests/test_offload_preflight.py tests/test_offload_worker_trust.py -q -p no:randomly

All of these must be green, including the pre-existing pins. In particular the
three `test_resolve_grok_cycle_bounds_*` tests near test_worker_daemon.py:1463
and the coupling assertions in test_offload_preflight.py must stay green.

## Files you own

- `.../orchestration/worker_daemon.py`
- any NEW test file you add under `packages/mcp-workbay-orchestrator/tests/`
  (or the three existing `test_resolve_grok_cycle_bounds_*` tests, to extend)
- `LANE-REPORT.md` (overwrite with your report for this lane)

## Required actions before you finish

1. Run the scoped verification command above and confirm it is green.
2. `git add -A && git commit` your work on `feature/offload-turnbudget-01` with a
   conventional-commit subject. **You must commit.** Work that is not committed
   is destroyed when the lane ends.
3. Write `LANE-REPORT.md` with: your P1/P2/P3 proof-of-reading answers, the
   mutation-test observations (both arms), your explicit decision on the inverse
   case, and the exact test output tail.

## Commit-message policy (strict)

Never credit any LLM, model, or vendor in git history. No `Co-Authored-By`
trailer, no "generated by", no assistant or vendor name anywhere in the commit
message or the report. Plan/task provenance only.

## Reasoning posture

Follow the heuristics canon: https://github.com/darce/heuristics-canon
Cite stable rule IDs where they apply. Do not pin or mention a canon version.
