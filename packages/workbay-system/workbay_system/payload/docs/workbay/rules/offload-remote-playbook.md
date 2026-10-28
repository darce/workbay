# Remote Offload Playbook

Operational rules for supported remote execution backends on the configured gate host.
The backend registry and install capability decide which backend can run a slice; this
playbook keeps shared remote-host contracts together. Grok-specific limits, transport
exit codes, and artifact names below are compatibility details for the grok adapter, not
requirements for every backend. Numbers below describe code-owned constants — the named
symbol is authoritative, not this page.

## When remote preference applies

- The preference is **install-scoped, not ambient**: it applies iff the bootstrap
  ledger records `execution_mode: remote_only` (written by
  `workbay-bootstrap install --with-remote`, read only via
  `workbay_protocol.bootstrap.load_execution_mode`). No ledger, or `local_ok`,
  means today's explicit-backend contract — `grok-remote` is then merely a valid
  explicit choice.
- Under `remote_only`, an omitted backend is refused with typed `remote_required` by
  `resolve_offload_backend_for_execution_mode`; pass an explicit off-box backend. Every
  `remote_only` dispatch passes an explicit backend that
  `list_available_backends(probe=true)` reports `is_available`; the probe reports reachability
  only and does not designate a default. `grok-remote` is discontinued by the operator; never a
  default for dispatch. The registry may still carry its row, so it is a candidate only when the
  probe reports it available. An operator may explicitly choose another installed remote
  backend, including `codex-remote`, when the registry capability and pre-flight probe permit it.
- **Flag, never substitute**: under `remote_only` the engine returns the typed
  `remote_required` outcome only when a local or non-admitted backend is requested or pinned, and
  skills surface it (recorded decision/blocker). An unavailable admitted remote backend is not an
  outcome: it surfaces as an `ok:false` preflight error carrying `remote_probe_state`. Either
  result is a blocker the coordinator records, never local execution. The only exception is
  the operator-authorised, capacity-only fallback in the offload skill's *Operator-authorised
  fallback (capacity only)* subsection. Under `remote_only` that fallback may select only another
  permitted remote backend, so a local alternative is refused. A `remote_required` refusal is not
  a capacity signal by itself. Dropping the policy is `repair --no-remote`, an operator act.
- **Implementation lanes use a permitted remote backend, not local subagents.** Under
  `remote_only` every implement/fix/repair lane is dispatched through
  `dispatch_wave` / `run_offload_pass` to a registry-approved remote backend; the model and
  effort are chosen per session (they vary — do not hardcode one in briefs or drivers).
  In-process harness subagents (Claude `Agent`, Codex `run_structured_turn`
  fan-out, and the like) are limited to **read-only** grunt work: classification,
  prior-art lookup, fetching, report drafting. A coordinator that edits code
  locally because "the lane is small" or "the VM is slow" has silently dropped
  the policy — that is the same prohibited substitution as a local backend
  fallback. The operator-authorised, capacity-only exception in the offload skill does not cover
  this case, so surface a `remote_required` blocker instead.
  (Operator directive 2026-09-08.) **Enforced** by the PreToolUse(`Agent`)
  guard `scripts/hooks/guard-agent-remote-only-implementation.py`: under
  `remote_only` it refuses implementation-shaped local subagents (implement /
  fix / edit / patch / refactor / commit / resolve-conflict briefs) and names
  `dispatch_wave` / `run_offload_pass`; read-only briefs (`READ-ONLY:`, "do not
  edit"), read-only subagent types (`Explore`, `Plan`), and briefs with no
  implementation intent pass. Easy override: put `[local-ok]` in the Agent
  prompt or set `WORKBAY_ALLOW_LOCAL_IMPLEMENT_SUBAGENT=1`; every bypass is
  logged to `.task-state/agent_remote_only_bypass.jsonl`. Inert on `local_ok`.

## Availability

- Configured by `WORKBAY_REMOTE_GATE_HOST` (`user@host` or ssh alias). Probe:
  `workbay_protocol.remote_probe.probe_remote_gate` — typed states
  `available | not_configured | malformed_host | unreachable | cli_absent`.
  The orchestrator's `list_available_backends(probe=true)` wraps the same probe
  with a short TTL cache.
- Install `--with-remote` hard-fails on a failed probe (deferred setup = install
  without the flag, then `repair --with-remote`).

The shared agent sandbox parent defaults to `workbay-sandbox` and can be overridden with
`WORKBAY_REMOTE_AGENT_ROOT`, including `grok-sandbox` to keep using the old location.
The legacy `grok-sandbox` root remains discoverable for cleanup and migration. Keep one
shared root across backends and do not add a per-backend root setting. New dispatches write
the neutral `.agent-result.json` and `.agent-phases.json` names. During migration, readers
try those names first and then accept `.grok-result.json` and `.grok-phases.json` from old
outboxes. Keep the legacy fallback until all outboxes created before neutral writes have
expired; then remove the `.grok-*` result and phase fallback names from readers.

## Gating under `remote_only`: the remote gate is the gate

Under `execution_mode: remote_only` (`workbay-bootstrap install --with-remote`),
**the remote gate is the gate of record for every suite target.** In the WorkBay
monorepo that gate is `make check-remote`. A local `make check-system` /
`make test-*` run is an advisory measurement only and must not be cited as
pre-merge evidence.

- **Why.** `--with-remote` is the install declaring that this host is not the
  execution surface. A local gate under `remote_only` measures an environment
  the policy has already said is not authoritative — the same class of error
  `slice-start` refuses by hand today (`workbay_lifecycle/handlers/slice_start.py`,
  the `execution_mode == "remote_only"` branch; finding `GATE-REMOTE-ROUTING-01`).
  A host-only measurement is also routinely *wrong* in a direction that reads as
  a code defect: a BSD-`stat` host files every candidate of the `remote_agent.sh`
  uid probe (`stat -c %u`, GNU-only) as `unmeasurable`, and a lane worktree with
  no `.venv` fails `offload_flock` with `interpreter not found`. Both are host
  artifacts; neither reproduces on the gate host.
- **Flag, never substitute.** An unreachable gate host is a blocker to record,
  not a licence to fall back to the local suite. Dropping the
  policy is `repair --no-remote`, an operator act.
- **How, in the WorkBay monorepo.** `make check-remote TARGETS="check-system test-handoff"`.
  Run it in the foreground: the SSH session is wall-clock bounded by `GATE_RUN_TIMEOUT_S`
  only when `timeout(1)` is on PATH (`scripts/remote_gate.sh`), which a stock macOS
  host does not have.
- **How, in a consumer repository.** Consumer repositories do not receive `make check-remote` or `scripts/remote_gate.sh`.
  The installer ships only the `Makefile.d/*.mk` fragments, so
  a consumer must supply an approved remote runner (a configured supported remote backend and
  its declared lane verification commands, dispatched through `/wb-offload`) and run its suite
  there. Until one is configured, the gate is a blocker to record, not a local run.
- **Local runs that are still legitimate** under `remote_only`: reproducing a
  failure the remote gate already reported, and bisecting. Label them as such;
  they are diagnostics, not the gate.

**Enforcement status: command-text only, deny-by-default for `make`.** Under `remote_only`,
the PreToolUse(Bash) guard `guard-bash-remote-gate-routing.py` refuses every `make` call whose
explicit goals are not all in its inert allowlist: `help`, `check-remote`, `lane-land`,
`finalize-plan`, `handoff-write`, `offload-flock` and `lane-reap`. It also refuses a `make`
call with no explicit goal, because the default goal may run tests. Numeric option values such
as `-j 2` and `-l 1` are not goals. A `-n` / `--dry-run` call is allowed. Pytest, py.test and
`run_named_tests.py` are refused as command text too. Not inspected: JS runners, unittest, tox,
nox, hatch, and scripts invoked by path. It is a guard against habitual and accidental runs,
not a complete control:

- **Habitual and accidental** workstation runs are what it stops: a command an
  agent or operator types without re-checking the policy.
- **Deliberate evasion** is out of scope. Built names, renamed copies,
  non-shell readers of produced files, and `_pytest` imports can still reach
  the host; the hook does not claim to stop an actor who chooses to bypass it.
- **The only sanctioned exception** is the audited override
  `WORKBAY_ALLOW_LOCAL_GATE_UNDER_REMOTE_ONLY=1` on the command, which logs each
  use to `.task-state/remote_gate_routing_bypass.jsonl`. Use it to reproduce a
  failure the remote gate already reported, or to bisect; never to produce
  pre-merge evidence.
- **Registration and deployment.** Tracked registrations are present on the seven tracked settings surfaces in this change.
  Native and operator deployment is still pending, so until that lands the routing also rests on this section.

The sibling rule — implementation lanes go to the VM, not to local subagents —
is already structural (`guard-agent-remote-only-implementation.py`, *When
remote preference applies* above) and is the shape that hook should copy.

## Pre-push guard and the transient ref

Before a remote turn starts, the transport pushes a machine-minted transient
ref to the VM. That ref is an implementation detail of `remote_exec`, so its
name is not required to satisfy the normal feature-branch grammar. The adapter
adds these values to the subprocess environment when the caller has not
already supplied them:

```text
WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH=1
WORKBAY_ALLOW_NONCONFORMING_BRANCH_PUSH_REASON=workbay remote_exec transient ref for lane <lane_id>
```

This is a transport-owned, audited authorization for the transient ref; an
operator does not need to export the push override for a normal remote lane.
Caller-provided values remain authoritative and are preserved verbatim. The
pre-push hook records a successful override as a decision event. If that
recorder is unavailable, it writes the JSON-lines fallback audit record to
`.task-state/branch_naming_push_overrides.log`, including the branch, reason,
and recorder error. Inspect that file when diagnosing a remote push that was
allowed through the branch-naming guard.

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
  times out a pass; dispatch warns `brief_test_cmd_full_suite` for unscoped
  pytest, `vitest run` with no `--changed` merge-base including
  `npx vitest run <dir>`, and `npm test` / `npm run test`). Merge-gate and
  lane self-verify use `npx vitest run --changed $(git merge-base HEAD
  <integration>)` or pytest spec paths; the full suite belongs to the
  release cut only. Wrapping the runner in `|| true` / `; true` warns
  `brief_test_cmd_swallows_failure` (a swallowed failure reads as green).
  Also include the known-red baseline, the pinned pointer
  (`docs/workbay/rules/heuristics-canon.md`, its pinned release), and the artifact
  route. When the sandbox has no network, inline the fired rows and their cards
  tagged with that release; reviewers cite each rule's bold name and Source as printed.
- Default `include_context_packet=true` with `context_targets` = the slice's
  files, so the worker cold-starts oriented. Treat structural codemap results as scoped
  context that may lag the worktree; they do not prove absence. Semantic reinjection is
  separate: the in-process ONNX model-load path is removed, and an unconfigured service
  returns `offload_semantic_service_unconfigured`. Preserve that typed degradation; do not
  copy or download ONNX/model assets into each remote sandbox. If a shared semantic service
  is configured, reuse its already-installed assets and bound diagnostics to the service
  response.
- **Judgment work** (golden recapture, normalizers, test isolation, hermeticity — see the offload
  skill's judgment list): the coordinator decides the expected value. A remote lane executes the
  edit only when the brief carries the human-verified expected value as an explicit artifact and
  the coordinator has recorded the human sign-off. A golden is written only as that artifact; a
  lane never regenerates a golden from its own run output. Without the artifact the edit is not
  offloaded; under `remote_only` the coordinator records a blocker decision, never a local
  edit. The engine does not inspect briefs for the artifact and returns no outcome for its
  absence. Remote lanes otherwise get **mechanical multi-file slices** with deterministic
  verification.

## Scheduling and recovery budget

- Build the ready frontier from the task's dependency DAG and run concurrent lanes only when
  their possible write sets are disjoint. Choose the maximum safe width from current measured
  capacity/headroom (including cgroup memory, pid, and CPU constraints), then apply the
  selected backend's own limits and active leases. Do not impose a universal four-lane cap.
  The grok transport's `WORKBAY_REMOTE_AGENT_MAX_LANES` default of 20 is specific to its
  `grok-lane-*` scopes; it is not a general host-capacity estimate.
- Prioritize finished lanes and retryable `cleanup_pending` work during admission/drain.
  Keep resource leases held until the worker process exits. The land/drain/daemon join is
  incomplete on some paths: follow typed `moved`, `admission_deferred`,
  `admission_refused`, and `cleanup_pending` outcomes, and do not report a join complete
  without its receipt.
- Run one bounded diagnostic pass per changed state: read the typed outcome, one current
  capacity snapshot, and a bounded stderr tail. Retry only after the named state changes;
  avoid repeated remote cleanup or VM measurement loops.

## Verify at the gate, never re-dispatch

- `commit_landed: true` + a post-commit `failed_stage`
  (`review | handoff | attestation | null`) means the worker's self-verified
  commit already landed — inspect it (`git log`/diff on the lane branch) and
  route it to exact-tip feature landing. Re-dispatching an already-green tree livelocks.
- Re-dispatch is correct only when `commit_landed: false`
  (`self_verify_failed`, or `failed_stage ∈ {execute, self_verify}`).
- Known misfit: a **read-only review brief** through the work-lane pass engine
  ends `outcome=error / failed_stage=review` even when the reviewer finished.
  Recover the verdict from the pass artifacts on the VM
  (`$WORKBAY_REMOTE_AGENT_ROOT/.lane-out-<LANE_KEY>-<DISPATCH_NONCE>/.agent-result.json`,
  where `LANE_KEY` is derived from the branch name alone — sanitized then
  suffixed with a branch hash) instead of re-running; older outboxes may still
  contain `.grok-result.json` during the migration window. Treat the recovered
  findings as the review output.

## Review lanes

- Per-lane adversarial review under `remote_only` pins a reviewer backend on the
  review-context trust allowlist (`codex-remote` or `cursor-remote`) at high effort,
  citing stable rule IDs from the engineering heuristics lexicon via
  [heuristics-canon.md](heuristics-canon.md). Reviewers cite IDs at that pointer's
  pinned release with each rule's bold name and Source; never paste rule bodies into
  this repository. grok-remote is refused there (`backend_not_allowlisted`).
- A durable, self-verified exact tip can land into its feature branch before the
  branch-complete review outcome and before new work is admitted against that branch. From
  the canonical workspace root, use the supported Make facade with both refs explicit:
  `make lane-land LAND_ARGS="--task <task-ref> --lane <lane-id> --expected-tip <40hex> --integration-ref <feature-ref>"`.
  The `expected-tip` is the worker-verified SHA; the feature ref is not `main` unless that
  is the intended integration target. A `moved`, incomplete, or indeterminate result is a
  typed refusal/retry state, not a successful landing.
  An ancestry-probe timeout or git transport failure returns `probe_unavailable` (exit 15):
  a typed retry state; retry with the same `run_id`, never a successful landing and
  never a terminal refusal.
- Run one `/wb-review-slice` off the lane admission critical path after safe feature landings.
  Allow at most one grouped fix wave; validate its files with focused tests and do not review
  the fix wave again. Unresolved HIGH findings block the final main gate. MEDIUM findings
  receive a one-file fix or one named wave; LOW and lint-only findings default to defer.
- The final main path first runs `make finalize-plan TASK=<task-ref>` after all
  candidate-changing work, then `make wb VERB=ship TASK=<task-ref>`. The ship receipt ties
  its close-check to the exact candidate SHA and reports merge/cleanup outcomes. No separate
  operator approval is required solely because the deterministic phases are composed.
  Finding status/classifier text does not block mechanical reap; retirement follows durable
  landing/ancestry evidence, preservation of worktree and sandbox data, and proof that no
  live writer remains. If a join is incomplete, use its typed pending/retry outcome; the
  active land/drain/daemon path is not fully joined on every route.

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
  is best-effort and scps `.agent-result.json` regardless of exit status,
  falling back to `.grok-result.json` for an old outbox. Exit
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
  nothing. A reaped lane-venv can leave the sandbox's `.venv` dangling, so
  bare `python` or `pytest` may be unresolvable and self-verify can return
  `harness_error`. Always pass an explicit interpreter
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
  re-verifies every patch through the configured authoritative runner; tell the lane this so
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
