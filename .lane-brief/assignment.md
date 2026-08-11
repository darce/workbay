# Lane durfix-payload-01 — unify the orchestrator result-payload reader

You are the worker agent for lane `durfix-payload-01` on task
`internal`. Branch `feature/durfix-payload-01`, based
on `main` at `e1ca93dc`.

You are fixing defects an adversarial review round found in code that is ALREADY
MERGED TO MAIN. This is not speculative cleanup — one of these defects destroyed a
completed piece of work while the review was running. See "Live incident" below.

## Files you own (do not touch anything else)

- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/_result_text.py`
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/grok_cli.py`
- `packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/adapters/remote_exec.py`
- `packages/mcp-workbay-orchestrator/tests/test_remote_exec_selfverify_tolerant.py` and any NEW test file you add under `packages/mcp-workbay-orchestrator/tests/`

**Explicitly NOT yours:** `scripts/remote_agent.sh`,
`tests/test_remote_agent_d9_real_script.py`, `tests/test_s7_cursor_remote_d9.py`.
A sibling lane owns those and is editing them concurrently. Touching them causes a
merge conflict that destroys one of the two lanes' work.

## Read budget

Read only: this file, the three source files above, the one existing test file
above. That is six files. If you are about to open a seventh, stop and write down
what you wanted instead. Do NOT sweep the repository. Do NOT read the whole of
`grok_cli.py` — jump to the function named in P1.

## Proof of reading (mandatory; put this FIRST in your report)

Answer from the actual files. The coordinator already knows the answers and will
check them. A wrong or hedged answer invalidates the entire lane and the work is
discarded.

- **P1.** In `grok_cli.py`, inside `_iter_balanced_objects`: when the scanner
  reaches the end of the text with an object still open, what exact statement
  executes, and does the scan continue looking at text after that point?
- **P2.** In `remote_exec.py`, inside `_extract_review_payload`: what exactly must
  the `findings` key be for the local `_shaped` helper to return True? Quote the line.
- **P3.** How many numbered tiers does the `_load_selfverify_dict` docstring enumerate?

## The defects to fix

**D1 — three copies of one scanner, and they disagree (finding DURREV-HARM-X2).**
There are THREE implementations of "scan text for balanced JSON objects":
`_result_text.py` (`_iter_balanced_json_objects` / `find_first_balanced_json`,
~lines 45-101), `grok_cli.py` (`_iter_balanced_objects`, ~lines 727-741), and one
in `scripts/remote_agent.sh` that the sibling lane owns. The first two ABANDON the
entire remaining text when a `{` never balances. The third does not — it advances
past the brace and keeps scanning. They also disagree on the shape discriminator
and on whether the first or the last match wins.

**D2 — the abandoning scan drops real payloads (finding DURREV-RP-F3).**
Concrete: text containing `=== capture start { ===` followed on the next line by a
complete, well-formed `{"command":"pytest -q","exit_code":0,"passed":true,
"self_verify_outcome":"passed"}` yields ZERO objects, because the banner's `{`
never balances. The greedy fallback then spans from the banner brace to the final
brace and fails `json.loads`. A green run is reported as a missing capture.

**D3 — split-brain discriminators on one file (finding DURREV-RP-F1).** Both call
sites in `remote_exec.py` (~line 317 with `discriminator="exit_code"`, ~line 357
with `discriminator="self_verify_outcome"`) parse the SAME `--selfverify-out` text
with INDEPENDENT scans. Given
`{"exit_code":1,"passed":false,"self_verify_outcome":"failed"}{"exit_code":0,"passed":true}`
the gate sees `passed=True` while the stamped outcome says `failed`. The gate and
the enum contradict each other about one file.

**D4 — last-wins is order-fragile (finding DURREV-RP-F2).** Selection keeps the
LAST dict carrying the discriminator. A real green result followed by a
progress/log object carrying the same key silently inverts green to red. The
existing tests pin only the stale-then-green ordering, never the inverse.

**D5 — tier 3 is vacuous (finding DURREV-RP-F4).** Deleting the entire greedy
`find_embedded_json_object` fallback leaves all 22 tests in
`test_remote_exec_selfverify_tolerant.py` green. Either pin it with an input that
ONLY it can recover, or delete it as dead code. Do not leave it unpinned.

**D6 — recovery is silent (finding DURREV-RP-F5).** Tolerant recovery emits no
log line, no counter, no flag. An operator cannot distinguish a clean capture from
a concatenated trailer that steered the gate.

**D7 — `_shaped` is narrower than the VM gate, and it destroys work (finding
DURREV-VM-F7).** `_extract_review_payload._shaped` accepts ONLY a list-valued
`findings`. The VM-side gate accepts `handoff_action` OR a findings list. So the VM
passes a payload as shaped, and the orchestrator then reports "no findings block".

### Live incident — this already happened, during the review that found it

Lane `durrev-vmgate` (pass `dd061b81`) completed a full adversarial review, wrote
`REVIEW-FINDINGS.md`, and committed it inside the sandbox. It returned
`handoff_action: "merge_ready"` with correct proof-of-reading answers. The VM gate
accepted the payload. The orchestrator reported `no_findings_block`,
`turn.patch` harvested as 0 bytes, and the finding bodies were destroyed
permanently. Only the titles survived, inside `structuredOutput`.

That is the bug you are fixing. It is not hypothetical.

## The contract to implement

The sibling lane is implementing this SAME contract on the VM side. Implement it
exactly as written so the two sides cannot drift again.

```
SHAPED-PAYLOAD RECOVERY CONTRACT v1

1. SCANNING. A '{' that never balances MUST NOT abandon the scan. Advance to that
   brace's offset + 1 and continue scanning. String handling stays escape-aware:
   braces inside JSON string literals are not structural.

2. SHAPE. An object is SHAPED if it carries EITHER
     (a) a 'handoff_action' key whose value is a non-empty string belonging to the
         known action enum, OR
     (b) a 'findings' key whose value is a list.
   Key PRESENCE alone is NOT sufficient: handoff_action of null, "", or an unknown
   value is NOT shaped.

3. SELECTION. Among shaped objects, select the LAST in text order.

4. OBSERVABILITY. Whenever a payload is produced by anything other than a strict
   top-level parse, that MUST be recorded: a flag on the emitted payload naming
   which tier recovered it, AND a warning-level log line. Recovery is never silent.
```

Additional requirement specific to your side, covering D3: the two consumers of a
single selfverify file MUST derive from the SAME selected object. Parse once, share
the result. A test must feed ONE file through BOTH consumers and assert they agree.

## Canon (guidance, not a gate)

Nothing fails merely because it diverges from these. Cite the ID when you rely on
one. Quoted verbatim because the sandbox has no network access to the canon repo.

- `[REF-26]` **DRY is knowledge, not text**: duplication of *intent* across
  representations is the defect; the acid test is one fact needing a multi-place,
  multi-format edit.
- `[REF-10]` **Coincidental vs real duplication**: don't DRY-merge code that won't
  change together; DRY stops at the pipeline boundary. *(Counter-case to REF-26 —
  apply judgement. Unify the scanner because all three copies encode ONE fact. Do
  not go on to merge unrelated adapter code that merely looks similar.)*
- `[NAME-05]` **Clones get mischunked**: readers chunk the copy as the original and
  discard exactly the small difference; unify or make the difference loud.
- `[OBS-08]` **Silence is not success**: dead instrumentation must break loudly (a
  freshness gate), not read as health.
- `[AGT-10]` **Degrade loudly**: a swallowed error that keeps the session alive must
  still land in a log.
- `[RLSE-05]` **Silent failure is the worst failure**: a crash is honest; silent
  data loss is a trust violation, P0 by definition.

## Tests

Enumerate mutants from the SOURCE, not from the tests. For each branch you add or
change, ask: if I deleted or inverted this branch, would a test go red? Any branch
where the answer is NO is not covered — either cover it or say so explicitly in
your report.

At minimum, pin: the truncated-brace-then-valid-object input from D2; the
split-brain input from D3 fed through both consumers with an agreement assertion;
the inverse ordering from D4; the recovery flag and log from D6; a `handoff_action`
payload with no findings list accepted per contract item 2; and a
`handoff_action: null` payload REJECTED per contract item 2.

Test command (run it once when you believe you are done, then at most twice more to
fix what it reports — do not re-run after every edit):

```
cd packages/mcp-workbay-orchestrator && WORKBAY_DISABLE_PYTEST_PATH_GUARD=1 python -m pytest tests/test_remote_exec_selfverify_tolerant.py -q
```

Plus whatever new test file you add.

## Required actions, in order

1. Implement the contract across your three source files. Delete the duplicate
   scanner rather than fixing it twice.
2. Add the tests named above.
3. Run the test command. Fix what it reports.
4. Write `LANE-REPORT.md` at the worktree root: proof-of-reading answers first,
   then per defect D1-D7 what you changed and which test pins it, then an explicit
   list of any branch you left uncovered.
5. `git add -A && git commit -m "fix(orchestrator): unify shaped-payload recovery across result readers"`

**The commit is mandatory.** Work that is not committed is destroyed when this lane
ends. The coordinator harvests from the committed tree, not from your final message.
Commit even if the tests are still failing — commit the work and say so in the
report. An uncommitted green result is worth nothing; a committed red one is
recoverable.
