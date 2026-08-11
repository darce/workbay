# Remote-delegation playbook (`grok-remote`)

Operational rules for offloading work to the remote execution gate — an
operator-provisioned VM reached over SSH, carrying the grok CLI. This doc is the
durable home of knowledge that previously lived in per-agent memory; skills link
here instead of restating it. Numbers below are descriptions of code-owned
constants — the named symbol is authoritative, not this page.

## When remote preference applies

- The preference is **install-scoped, not ambient**: it applies iff the bootstrap
  ledger records `execution_mode: remote_only` (written by
  `workbay-bootstrap install --with-remote`, read only via
  `workbay_protocol.bootstrap.load_execution_mode`). No ledger, or `local_ok`,
  means today's explicit-backend contract — `grok-remote` is then merely a valid
  explicit choice.
- Under `remote_only`, offload defaults resolve to `--agent grok-remote
  --effort high` (model pin `DEFAULT_GROK_MODEL`, currently grok-4.5).
- **Flag, never substitute**: when the remote gate is unavailable under
  `remote_only`, the engine refuses with the typed `remote_required` outcome and
  skills surface it (recorded decision/blocker). Falling back to a local backend
  is prohibited — dropping the policy is `repair --no-remote`, an operator act.

## Availability

- Configured by `WORKBAY_REMOTE_GATE_HOST` (`user@host` or ssh alias). Probe:
  `workbay_protocol.remote_probe.probe_remote_gate` — typed states
  `available | not_configured | malformed_host | unreachable | cli_absent`.
  The orchestrator's `list_available_backends(probe=true)` wraps the same probe
  with a short TTL cache.
- Install `--with-remote` hard-fails on a failed probe (deferred setup = install
  without the flag, then `repair --with-remote`).

## Bounds (code-owned)

- Single-cycle bounds derive from `token_budget`:
  `derive_grok_single_cycle_bounds` in `offload_profiles.py`
  (`GROK_MAX_TURNS_CAP`, `GROK_TIMEOUT_CAP` — ceilings of 60 turns / 1800 s;
  those named symbols are authoritative and the inlined numbers mirror them).
  A typical `token_budget` therefore yields less than 60 and 1800. The two are
  coupled: a pass gets `min(GROK_TIMEOUT_CAP, max_turns *
  SECONDS_PER_TURN)` seconds, so raising one without the other buys nothing.
- The VM enforces its own transport-side guards in `remote_agent.sh`:
  memory floor (`WORKBAY_REMOTE_GATE_MEM_FLOOR_MB`, exit 75 defer), lane cap
  (`WORKBAY_REMOTE_AGENT_MAX_LANES`, exit 75), unconfigured host (exit 78),
  per-scope `MemoryMax`/`CPUQuota`. Exit codes are **transport** semantics —
  policy refusals are typed outcomes (`remote_required`), never exit-code reuse.
- **Agent-bound ladder** (`resolve_agent_bound` in `remote_agent.sh`): every
  dispatch path either carries a process bound or exits **7** (policy refusal
  to run unbounded). One absolute deadline is written once; the lease expiry
  and the process bound are two views of that variable
  (`lease_expiry = max(deadline, now) + 300`). Arms:
  1. `--timeout N>0` + `timeout(1)` present → `timeout -k 10 <residual>` (`TW`);
     death code **124**, 10s kill grace.
  2. `--timeout N>0` + `timeout(1)` absent + scope supports `RuntimeMaxSec` →
     scope `-p RuntimeMaxSec=<residual>`; death code **143** (`SIGTERM`).
  3. `--timeout 0` + `RuntimeMaxSec` → scope
     `RuntimeMaxSec=${WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S:-21600}`
     residual from `_RP_START`; death code **143**. CLI `0=none` still means
     "no caller-supplied bound"; it no longer means "no bound".
  4. neither `timeout(1)` nor `RuntimeMaxSec` → **exit 7** on stderr naming
     both missing controls, before this lane's lease write or sandbox wipe.
  Do not normalize death codes across arms — neither the script nor
  `remote_exec.py` branches on 124/143 today.

## Dispatch discipline

- Brief must carry a **scoped `TEST_CMD`** (never a whole-package suite — it
  times out a pass; dispatch warns `brief_test_cmd_full_suite`), the known-red
  baseline, and the versionless heuristics link.
- Default `include_context_packet=true` with `context_targets` = the slice's
  files, so the worker cold-starts oriented.
- Judgment work stays inline (golden recapture, hermeticity, normalizers —
  see the offload skill's inline-only list); remote lanes get **mechanical
  multi-file slices** with deterministic verification.

## Verify at the gate, never re-dispatch

- `commit_landed: true` + a post-commit `failed_stage`
  (`review | handoff | attestation | null`) means the worker's self-verified
  commit already landed — inspect it (`git log`/diff on the lane branch) and
  route it to the review gate. Re-dispatching an already-green tree livelocks.
- Re-dispatch is correct only when `commit_landed: false`
  (`self_verify_failed`, or `failed_stage ∈ {execute, self_verify}`).
- Known misfit: a **read-only review brief** through the work-lane pass engine
  ends `outcome=error / failed_stage=review` even when the reviewer finished.
  Recover the verdict from the pass artifacts on the VM
  (`$WORKBAY_REMOTE_AGENT_ROOT/.lane-out-<LANE_KEY>-<DISPATCH_NONCE>/.grok-result.json`,
  where `LANE_KEY` is derived from the branch name alone — sanitized then
  suffixed with a branch hash) instead of re-running; treat the recovered
  findings as the review output.

## Review lanes

- Per-lane adversarial review runs on `grok-remote` at high effort too, citing
  stable rule IDs from the engineering heuristics lexicon (link the canon;
  never paste rule bodies, never pin a canon version).
- `/review-parallel` remains the orchestrator's branch-complete merge gate; a
  remote reviewer lane is pinned via `materialize_offload_lane_manifest
  (preferred_backend=grok-remote)`.

## Lane-brief protocol

Every remote brief is written so a cold lane can act without inventing missing
constraints. [NDM-07] (**Intent on the wire**): a handoff that "lists steps
only — no goal, priorities, constraints, or forbidden moves" fails common
ground; state purpose, priorities, constraints, and forbidden moves so the
executor can improvise and catch errors. The rules below are live-observed
failures from this repo's dispatch history.

- **Forbidden moves are mandatory, not optional [NDM-07].** A brief that lists
  only steps produces a lane that answers from the brief without opening a
  file. Observed: a review lane returned in one turn having read nothing,
  restating the brief's own hypotheses as findings; its single original claim
  contradicted the document it claimed to review. Every brief states what the
  lane must **not** do.
- **Demand verbatim evidence.** Require `path:line` quotes for every claim,
  and a `FILES_OPENED:` list in the response. A finding with no quote is
  discarded.
- **Plant falsifiable hypotheses.** State that some supplied hypotheses are
  deliberately false and must be refuted with evidence. A lane that refutes
  one outranks a lane that confirms all of them.
- **The sandbox is history-stripped.** `git log` / `git diff` / `git show` do
  not work and there is no `main` ref. Any commit-level evidence must be
  **inlined** into the brief. Tell the lane this explicitly, or it reports
  "git failed" instead of doing the work.
- **Turn budget and timeout sizing.** 60 turns / 1800 s is the ceiling
  (`GROK_MAX_TURNS_CAP` / `GROK_TIMEOUT_CAP`; those named symbols are
  authoritative — the inlined numbers mirror them); the effective bound is
  derived from `token_budget`. `run_offload_pass(grok_max_turns=N)` overrides
  the derived turn count, but the wall-clock bound only follows when
  `turn_timeout_seconds` is also set — that is the override pair that feeds
  the grok cycle (`offload_pass` reads `turn_timeout_seconds` for
  `grok_timeout`; `timeout_seconds` is a separate mandatory pass-level
  parameter and never becomes the grok bound). Passing `grok_max_turns`
  without `turn_timeout_seconds` leaves the timeout budget-derived, which is
  how a 60-turn run can end up on a 60-second clock. A brief that asks for a
  full test-suite run still times out; scope `--test-cmd` to the specific
  affected test files (same discipline as **Dispatch discipline** above).
  Scoping the brief buys back far more turns than raising the cap does —
  turn and wall-clock exhaustion route through the `agent_failed` arm of
  `remote_agent.sh`, which still runs salvage (`_emit_off_box_selfverify` and
  `git format-patch`) before exit so partial commits return; `fetch_result`
  is best-effort and scps `.grok-result.json` regardless of exit status. Exit
  code 3 is the generic agent-failed code — any nonzero grok rc reached before
  the wall-clock deadline, which is where turn exhaustion lands: uncommitted
  work in the lane is lost, while committed work survives in the sandbox and is
  recoverable. Wall-clock expiry is a *different* code: `_classify_agent_failed_exit`
  returns 8 once `_BOUND_DEADLINE` has passed or the agent rc is 124, and 3
  otherwise. Exit 3 also has a second, unrelated emit site — the post-classify
  `unknown status` default arm — so a 3 is not by itself proof of turn
  exhaustion; read the stderr tail. The genuinely unsalvageable arm is exit 4
  (agent produced no committed changes, so neither `_emit_off_box_selfverify`
  nor `git format-patch` runs).
- **`--test-cmd` environment contract.** `remote_agent.sh` does prepend
  `$HOME/.local/bin` and `$SBX/.venv/bin` to `PATH` (and export `VIRTUAL_ENV`)
  for the caller-supplied command, and it does symlink `$SBX/.venv` at the
  persistent per-lane env. **Do not rely on it.** That symlink points into the
  `$ROOT/.venv-lane-*` pool, which the same script LRU-reaps down to
  `WORKBAY_REMOTE_AGENT_MAX_LANE_VENVS` (default 24) — so a sandbox's `.venv`
  can be left dangling, and then the `PATH` entry is inert and resolves
  nothing. Measured 2026-08-05 on the gate VM: three live sandboxes, all three
  with a dangling `.venv`, where bare `python` and bare `pytest` were both
  unresolvable (only `/usr/bin/python3` remained). The same day a lane whose
  command used bare `python` exited 127
  (`bash: line 1: python: command not found`), losing its whole self-verify to
  `self_verify_outcome: harness_error`. Always pass an explicit interpreter
  path; the form proven green that day runs from the **repo root**:
  `WORKBAY_DISABLE_INVOKING_REPO_TRIPWIRE=1 WORKBAY_DISABLE_PYTEST_PATH_GUARD=1
  .venv/bin/python -m pytest <explicit test paths> -q`. A `cd packages/<pkg> &&
  ../../.venv/bin/python` form resolves to the same interpreter and is
  acceptable when the tests require the package cwd, but it inherits the same
  dangling-symlink exposure — it is not a fix for it. Ship an explicit
  interpreter in every brief that carries a test command.
- **Write-set isolation for parallel lanes.** Concurrent lanes must own
  disjoint file sets. `remote_agent.sh` derives `LANE_KEY` from the **branch
  name alone**, so two lanes dispatched on the same branch serialize on a
  non-blocking lock and the loser defers with exit 75 — give each concurrent
  lane its own ref. The governing rule is write-set collision-freedom: the
  union of files any two concurrent lanes may write must be disjoint.
- **Anti-fabrication.** A lane's self-report is not evidence. The gate
  re-verifies every patch locally; tell the lane this so
  `commit_landed: true` is never taken on trust (see **Verify at the gate**).
- **Read budget.** Every bound the code owns caps *output* — turns, wall clock,
  single-cycle token budget, JSON output schema. **Nothing caps input.** A lane
  that reads widely enough dies on `max_tokens_truncation` and returns an error
  envelope with no findings at all, so a bounded output schema does not prevent
  this and the brief is the only control. Observed: 899,645 input tokens over
  13 turns on a review lane whose brief asked for the widest file sweep of
  seven; nothing was recoverable. Every read-heavy brief therefore names a file
  ceiling ("read at most eight files in full") and asks the lane to list what it
  opened; requires `grep -n` then `sed -n 'A,Bp'` rather than `cat`; reads the
  artifact under review once, by line range; and says **partial beats
  discarded** — a lane that stops early strictly dominates one that dies.
  Scope the *lens*, not just the brief: a lens that honestly needs a dozen files
  is two lanes, split at dispatch, before paying for the discovery. Diagnostic —
  flock exit 1 + lane exit 3 + a body of
  `{"type":"error","message":"…max_tokens_truncation…"}` is this failure, not an
  agent failure; re-dispatch narrower, never retry the same brief.

## Repairing a lane's commit — do it in the lane's own worktree

A lane authors its own commit message, and some CLIs append provider
attribution **at commit time**, below whatever the model wrote (`cursor-agent`
appends `Co-authored-by: Cursor <cursoragent@cursor.com>`). The repair is
`git commit --amend` on the lane commit — and **it must be run inside that
lane's own worktree**.

From the primary repo, `git checkout lane/<x>` fails with
`already checked out at <worktree>`. That failure is loud on its own, but it is
destructive when chained: in `git checkout lane/x && git commit --amend …` the
checkout fails, the `&&` should stop there — and any variant that does not (a
`;` separator, a shell function that swallows the status, a heredoc run under
`set +e`) silently amends whatever `HEAD` you were **actually** on. In the
observed case that was the wave **merge commit**, recoverable only by
`git reset --hard`.

So:

- `cd` into the lane's worktree first — `git worktree list` names it — and amend
  there. Never `git checkout` a lane branch from the primary.
- Never chain a branch switch and an amend in one command. Run the switch, read
  its exit status, then amend in a separate call.
- Amending changes the SHA. Any finding, decision, or test-evidence row already
  anchored to the pre-amend SHA must be re-checked against the new one.
