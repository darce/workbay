# Wave executor v1

`workbay_orchestrator_mcp.orchestration.wave_executor.create(*, repo, spec)` returns a `(LaneExecutor, Gate)` pair for the wave supervisor. The factory receives the resolved repository and the raw loaded wave spec. A configured supervisor adapter remains an explicit deployment override.

The executor is VM-local and refuses creation outside Linux. Its single authority is Git plus durable files below the repository's absolute Git directory at `workbay-waveexecutor/waves/<wave>/`. It has no SSH, push, laptop-driver, shared-database, or integration behavior. The supervisor alone integrates lane tips onto `wave/<wave>`.

## Separate journal projection

Git journal refs remain authoritative. `wave_transport pull` and `status` require
an explicit `--store-path` for a separate local SQLite projection; the executor
and supervisor replay do not depend on that projection. There is no Handoff
co-location fallback.

The shared journal-store guard protects the primary repository's default
`.task-state/handoff.db` and the destination discovered by Handoff runtime
configuration, including a configured state directory and linked worktrees.
Their reserved `-wal`, `-shm` and `-journal` paths receive the same protection,
including absent sidecars and normalized aliases. Reader and writer paths both
expand literal `~` before checking the destination.
It rejects normalized canonical paths before creation, resolved symlink and
hardlink aliases, and Handoff-bearing or unrelated schemas at any filename.
A journal marker does not authorize a canonical Handoff destination or a mixed
schema. `user_version` alone does not establish database identity.

Supported stores use `application_id = 0x57424A4E`, exactly one
`journal_store_meta` row with `version = 1`, and the exact `journal_events`
column and primary-key contract. New or verified-empty separate stores receive
their schema and identity in the same transaction as imported events. Unknown
markers, versions, schema objects, or inspection errors fail closed with
`journal_store_identity_refused`; SQL failures during event import remain
`store_failed`. Duplicate/conflict detection, conflict commits, orphan retention,
canonical event JSON/hash semantics, and import ordering remain unchanged.
An inspector refusal after a successful import commit returns `ok=False` with reason `inspector_unsettled_after_commit` and the true inserted/duplicate counts.

Unmarked legacy stores are accepted only with the exact journal-only schema,
zero application/user versions, and at most 100,000 rows whose canonical event
JSON, hashes, and projected fields all agree. Readers validate without stamping
or upgrading; writers adopt a validated legacy store transactionally. Readers
use `mode=ro` with a five-second busy timeout; writers use a fifteen-second
timeout and revalidate file identity and schema under `BEGIN IMMEDIATE` before
schema or data writes. All handles close, and failures roll back.

Raw file creation/header inspection runs in a bounded child process that pins
the inode until all SQLite handles close; it never opens or closes unmanaged
database descriptors in the SQLite caller's process. This preserves locks held
by other connections in that process. Cleanup attempts rollback, connection
close and child release/join even after a cleanup failure, preserving the
original error. Settlement of the inspector is finite and uses one absolute
cleanup deadline of ten seconds, created when settlement begins. After stdin
closes, the child must reach stdout EOF and be joined within the first five
seconds. If it does not, SIGKILL is attempted on the leader (a no-op once it has exited),
and the independent leader reap and the post-kill stdout EOF wait draw only on
the remainder of that same deadline; no step receives a fresh allowance.
Parent-owned stdin and stdout are closed on every path, even when a descendant
keeps stdout open, so pipe closure never depends on descendant EOF. Observed
stdout EOF and leader reaping are tracked separately: `communicate` can observe
EOF before its leader wait fails, and forced parent stream closure is not EOF
evidence. Unobserved stdout EOF and an unreaped leader are reported separately;
neither establishes that a descendant ended or released the store. Diagnostics
distinguish failed SIGKILL attempts from attempts returning without error and
do not infer signal delivery from either. They claim deadline exhaustion only
when the original ten-second deadline has actually been reached, not merely
because a wait timed out or an ordinary cleanup operation failed. When no body error is
propagating, an unresolved state is reported as `journal_store_identity_refused`,
and a cleanup exception without an unresolved state propagates unchanged.
Otherwise the body error is preserved, never replaced, and annotated with the
inspector's pid for each unresolved state and with any cleanup failure; an
unreaped leader's descriptor pin may remain. Handoff
configuration refusals are translated before yielding a connection into
`journal_store_identity_refused`.

WAL-format stores are refused before SQLite inspection to avoid SHM side
effects. The caller must control the destination directory: substitution checks
do not secure it against another process with write privileges. Recovery needs
owner approval to rebuild a new separate store from the original Git journal;
this contract authorizes no deletion, silent upgrade, or deployment activation.
Live consumers and legacy destinations must be inventoried before rollout;
source enforcement alone does not establish their deployment configuration.

## Frozen spec

Execution configuration is an additional top-level field; scheduler fields keep their current meaning. The optional top-level `max_width` ceiling is a separate field, covered under Operator width ceilings below.

```json
{
  "wave": "wave-2026-09-26",
  "base_sha": "<supervisor-pinned-full-sha>",
  "lanes": {"parser": {"depends_on": [], "paths": ["packages/parser"]}},
  "execution": {
    "schema_version": 1,
    "lanes": {
      "parser": {
        "backend": "codex-remote",
        "model": "gpt-6-luna",
        "effort": "max",
        "speed": "fast",
        "commands": {
          "implement": ["/usr/bin/python3", "/srv/workbay/bin/wave-codex-stage.py", "implement"],
          "review": ["/usr/bin/python3", "/srv/workbay/bin/wave-codex-stage.py", "review"],
          "fix": ["/usr/bin/python3", "/srv/workbay/bin/wave-codex-stage.py", "fix"],
          "test": ["/usr/bin/python3", "-m", "pytest", "-q", "packages/parser/tests"]
        },
        "timeout_s": 3600
      }
    }
  }
}
```

The command arrays are trusted launcher configuration, not worker-authored input. Each lane requires four non-empty argv arrays, a positive finite `timeout_s` (maximum one day), and explicit backend/model/effort/speed strings. The optional `allow_noop` field must be a boolean and defaults to `false`; its normalized value is included in the plan digest. There is no backend-name allowlist. A plan is strict canonical JSON; its SHA-256 digest is frozen at factory creation and written into every job. A changed plan for an existing wave/lane/attempt returns `plan_mismatch`.

The worker receives `WORKBAY_WAVE_RESULT_PATH`, `WORKBAY_WAVE_JOB_ID`, `WORKBAY_WAVE_PLAN_DIGEST`, `WORKBAY_WAVE_STAGE`, and the configured backend/model/effort/speed as environment variables. Those values describe the requested configuration; the executor cannot prove that a trusted launcher used them. The launcher must turn the settings into its own command/API request and treat any provider response metadata it receives as separate evidence.

## Operator width ceilings

Two optional operator ceilings can limit how wide the supervisor runs a wave. Each term below has one meaning. The *requested width* is the `--width` value of `wave_supervisor run` (carried as `SupervisorContext.width`): the number of lanes the supervisor may hold in the `running` state at once. `wave_transport unit` passes its own `--width` through to that command. Both commands default it to 4; that is a default request, not a ceiling. A *width ceiling* is an upper limit on the requested width that an operator configures. The *effective width bound* is what the configured ceilings combine to.

- **Spec ceiling.** The optional top-level `max_width` field of the wave spec, beside `wave`, `base_sha`, `lanes`, and `execution`. If the key is absent the spec sets no ceiling. If the key is present its value must be a JSON integer of at least 1. Zero, negative numbers, booleans, strings, arrays, and objects are invalid, and so is `null`: a present `null` is invalid, not absent. A number written with a decimal point or exponent, such as `4.0` or `4e0`, is invalid even when its value is whole.

- **Environment ceiling.** The optional environment variable `WORKBAY_WAVE_MAX_WIDTH`, read from the environment of the process that evaluates it. If it is unset there is no environment ceiling. If it is set, its value must be ASCII decimal digits that parse to at least 1; leading zeros are accepted. The empty string, `0`, signs, whitespace, decimal points, digit separators, non-ASCII digits, and other text are invalid. An empty value counts as set, so it is invalid rather than unset.

**Effective width bound.** When both `max_width` and `WORKBAY_WAVE_MAX_WIDTH` are configured, the effective width bound is the lower of the two. When only one is configured, it is that one. With neither ceiling configured there is no effective width bound: every positive requested width is accepted.

**Invalid value.** A present but invalid ceiling, from either source, is refused with the typed reason `invalid_width_bound`; the transport and `render_unit` carry it differently, as listed below. It is not ignored, and a valid ceiling from the other source does not stand in for it. The reason names neither the source nor the value.

**Where it applies.** A request above the effective width bound is refused, not reduced to fit it. The comparison with the requested width happens only at the points below; `wave_executor.create` and the executor read neither ceiling.
- `wave_supervisor run` loads the spec (the `--spec` file or the published wave inputs) and evaluates both ceilings before it loads an executor. An invalid ceiling is refused with `invalid_width_bound`, and a requested width above the effective width bound is refused with `invalid_width`. Both exit with status 2 and write `{"ok": false, "reason": ...}` to stderr; no lane starts and nothing is journaled.
- Driving the supervisor in process (`step` or `run` with a `SupervisorContext`) evaluates the same two checks on every step. A refusal is journaled as a `wave.refused` event with the same reason, and later steps of that wave do nothing. A pre-parsed `WaveSpec` carries no `max_width`, so only the environment ceiling applies to it.
- `wave_transport push` and `status` validate the spec with the same ceiling reader. An invalid ceiling, whether in the spec's `max_width` or in the `WORKBAY_WAVE_MAX_WIDTH` of the process running the command, returns `ok: false` with reason `invalid_wave_spec` and the problem `{"type": "invalid_width_bound"}`. These commands compare no requested width, so a spec with a valid `max_width` is published unchanged and the ceiling takes effect when the supervisor starts.
- `render_unit`, and so `wave_transport unit`, is given no spec and reads only the environment ceiling. An invalid value raises `ValueError`, and so does a requested width above the environment ceiling; the `unit` command then exits 2. The unit's `--width` is not compared with the spec's `max_width`; the supervisor does that when it starts. The rendered unit sets no environment variables, so that supervisor reads `WORKBAY_WAVE_MAX_WIDTH` from its own service environment.

**Policy.** Both are optional operator ceilings with no default. WorkBay ships no value for either, the supervisor derives none, and this contract suggests none. They are limits an operator chooses to impose; nothing in the supervisor or the transport sets or adjusts them.

**Scope.** The spec's `max_width` is a wave executor field. It is not `max_width` in `HostMemoryPolicy`, which caps concurrent local heavy slots, and not the `wave_max_width` argument of `dispatch_wave`, a clamp on coordinator-side remote dispatch whose own environment cap is `WORKBAY_REMOTE_AGENT_MAX_LANES`. The supervisor, the transport, and the executor read none of those.

## Admission, isolation, and recovery

**Measured admission.** Width ceilings and measured admission are independent: a lane passes both before submission. The production CLI `wave_supervisor run` always supplies `admission=wave_admission.measured(repo)` in its `SupervisorContext`. Before each lane submit, including a retry after `lost`, `step` calls `ctx.admission(wave, lane_id, pending)`, where `pending` counts lanes submitted earlier in that same step. A refusal submits nothing: the lane stays `pending` and no attempt is consumed. No further lane is admitted in that step. In-process callers using `admission=None` are not measured.

A refusal journals `lane.admission_deferred` with `lane_id` and `reason`, only when the reason differs from that lane's last journaled deferral since its last submit. `lane.submitted` clears that remembered deferral. An exception from the admission callable becomes the deferral `vm_admission_unmeasured:admission_error`, never an admit.

The gauges and rules are those of `scripts/vm/lane_queue.py`, with the same reason strings: `vm_pids`, `vm_memory`, `vm_memory_psi`, `vm_cpu_psi`, `vm_disk`, and `vm_admission_unmeasured:<gauge>` (including `<gauge>_cost`, `vm_pending`, and `vm_memory_headroom`). The existing environment keys are `WORKBAY_REMOTE_GATE_PIDS_ROOT`, `WORKBAY_REMOTE_GATE_DISK_FLOOR_GB`, `WORKBAY_REMOTE_GATE_HEADROOM_PCT`, `WORKBAY_REMOTE_GATE_MEM_PSI_MAX`, `WORKBAY_REMOTE_GATE_CPU_PSI_MAX`, `WORKBAY_REMOTE_GATE_PID_FLOOR`, `WORKBAY_REMOTE_GATE_LANE_PIDS_FLOOR`, and `WORKBAY_REMOTE_GATE_LANE_ANON_FLOOR_MB`. Each lane's disk cost is the repository's `git count-objects -v` size-pack plus size; checkout size is a known-unknown.

`submit(wave, lane_id, attempt)` validates identifiers and arguments, pins the current `refs/heads/wave/<wave>` tip (or immutable `refs/heads/wave-base/<wave>` fallback), then fsyncs an identity reservation and job intent before starting a detached worker. Job identity is stable for `(wave, lane, attempt, plan digest)`; the reservation prevents a changed plan from creating a second effect for the same attempt. Same-key concurrent submissions serialize through `flock` and replay that one intent. An ambiguous persisted intent is never silently rerun.

Each worker makes its own `git clone --no-hardlinks --no-checkout` below its durable job directory, removes the clone's origin, checks out the pinned full SHA, and creates `lane/<lane-id>`. Stage subprocesses use argv directly with no shell. Tracked implementation/fix changes are committed with a fixed author identity; untracked files and dirty candidate trees cannot pass. Review and Linux-test execution are checked against the exact HEAD/tree before and after their command.

The configured timeout is one total monotonic deadline beginning at admission. It covers lock wait, clone, implementation, review, the optional single fix and second review, and test. Stage subprocesses receive only the remaining budget. If the admission deadline expires before a worker starts, submission raises `ExecutorError("budget_exhausted")` with status code 429. Once the worker starts, a deadline expiry during worker processing is recorded as a terminal job result with `terminal_reason="budget_exhausted"`; `collect()` reports it as `ok: false` with `detail: "budget_exhausted"`. For the default systemd adapter, a timeout while launching the job is wrapped as `ExecutorError("systemd_job_launch_failed")`, which has the default status code 400; the launch outcome is treated as unknown and the durable identity is retained without retry. Detached child PID, Linux process start identity, boot ID, job ID, and plan digest are checked on recovery. Parent exit alone does not mark a job lost. If the worker identity cannot be recovered, the job becomes `lost`; its files are retained for inspection.

There is one coordinator `flock` owner per wave, released by process exit. It does not impose a fixed lane-worker cap. Different lane jobs use different clones and can overlap. The wave DAG and owned-path conflict graph remain scheduler concerns (GRPH-01/09/32); the executor does not turn a path conflict into dependency data or integrate a ready antichain. Durable intent-before-effect and reconstructable identity follow DDIA's recovery-from-position principle (GRPH-29); total deadlines and explicit limits follow Latency ch. 9 and Release It! ch. 4/5 fail-fast and bulkhead guidance.

## Stage protocol and gate

For `implement`, `review`, and `fix`, the executor removes the prior result file before launch and accepts only a bounded strict JSON object with exactly these keys:

```json
{
  "schema_version": 1,
  "job_id": "<executor-provided-id>",
  "plan_digest": "<executor-provided-sha256>",
  "stage": "review",
  "ok": true
}
```

The command must exit zero and the verdict's types and identity must match the current stage. Command stdout/stderr go to `/dev/null` (zero retained log bytes); they are never verdicts. Result files are capped at 16 KiB. The review gets one fix at most and then exactly one final review. Linux test success is recorded from its zero exit status and exact pre/post commit SHA/tree; missing or failing test commands fail the job.

`collect(job)` fetches the lane commit from that job's local clone into the authority repository by local path only. It verifies the clone's `lane/<lane>` ref, the exact full tip, and ancestry from the admission base, then records a durable collect receipt containing the tip, tree, plan digest, and pipeline result. It returns the commit SHA unchanged. `Gate(repo, lane_id, tip)` accepts only a collected receipt for that exact SHA plus successful implement, review (and optional fix), and Linux-test evidence. After all existing identity and evidence checks, it refuses a tip equal to that job’s pinned admission base with `{"ok": false, "detail": "no_change"}`, unless the frozen lane plan explicitly sets `allow_noop: true`. It rejects arbitrary text verdicts, an uncollected commit, changed review/test trees, failed committed content, missing stages, and stale tips.

Job directories and bounded result files are retained; there is no destructive reaper. The implementation is a cooperative trusted-command boundary for one Unix user. It does not sandbox hostile commands running with the same UID, protect credentials from those commands, or claim authenticated model provenance.

## Codex CLI plan example

The installed lane CLI was `codex-cli 0.155.1`. Its `codex exec --help` exposes `--model`, `--config`, `--sandbox`, `--output-schema`, and `--output-last-message`; it has no dedicated effort or speed flag. `codex debug models` reported `gpt-6-luna` with `max` reasoning and `fast` speed-tier support. The corresponding explicit Codex arguments are:

```text
codex exec \
  --model gpt-6-luna \
  --config 'model_reasoning_effort="max"' \
  --config 'models.new_thread.service_tier="fast"' \
  --sandbox workspace-write \
  --output-schema "$SCHEMA_PATH" \
  --output-last-message "$WORKBAY_WAVE_RESULT_PATH" \
  "$PROMPT"
```

The CLI settings use the documented [`model_reasoning_effort` and `models.new_thread.service_tier` configuration](https://developers.openai.com/codex/config-reference/). `max` is a model-dependent reasoning level described in the [reasoning guide](https://developers.openai.com/api/docs/guides/reasoning), while API Fast mode uses the `service_tier` field in the [Fast mode guide](https://developers.openai.com/api/docs/guides/fast-mode). These arguments request model/effort/service tier; the CLI help and model catalog checks do not prove a live request used those settings. A real Codex invocation and provider receipt remain a deployment follow-up, not a deterministic executor test.

Install this small trusted launcher as `/srv/workbay/bin/wave-codex-stage.py` on the VM. It asks Codex for the exact executor verdict schema and lets `codex exec --output-last-message` write the result file that the executor validates. The executor controls the timeout and kills the launcher's process group on expiry. This is launcher plumbing only: the sample prompt provides no task objective or owned-path scope, so a deployment must supply a deployment-specific per-lane brief and scope. The gate checks valid stage and test evidence for the exact collected tip and rejects an unchanged admission-base tip with `no_change` by default. A lane intentionally permitting no changes must explicitly set `allow_noop: true`; this does not waive any identity or evidence checks.

```python
#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from pathlib import Path

stage = sys.argv[1]
if stage not in {"implement", "review", "fix"}:
    raise SystemExit(2)
result = Path(os.environ["WORKBAY_WAVE_RESULT_PATH"])
schema = result.with_suffix(".schema.json")
schema.write_text(json.dumps({
    "type": "object",
    "properties": {
        "schema_version": {"const": 1},
        "job_id": {"const": os.environ["WORKBAY_WAVE_JOB_ID"]},
        "plan_digest": {"const": os.environ["WORKBAY_WAVE_PLAN_DIGEST"]},
        "stage": {"const": stage},
        "ok": {"type": "boolean"}
    },
    "required": ["schema_version", "job_id", "plan_digest", "stage", "ok"],
    "additionalProperties": False
}), encoding="utf-8")

mode = "read-only" if stage == "review" else "workspace-write"
prompt = (
    f"Run the {stage} stage for this lane. Job={os.environ['WORKBAY_WAVE_JOB_ID']} "
    f"plan={os.environ['WORKBAY_WAVE_PLAN_DIGEST']}. For review, do not edit files. "
    "Return only a JSON object matching the supplied schema; set ok false when this "
    "stage found a blocker or could not complete its work."
)
argv = [
    "codex", "exec", "--model", os.environ["WORKBAY_WAVE_MODEL"],
    "--config", "model_reasoning_effort=" + json.dumps(os.environ["WORKBAY_WAVE_EFFORT"]),
    "--config", "models.new_thread.service_tier=" + json.dumps(os.environ["WORKBAY_WAVE_SPEED"]),
    "--sandbox", mode,
    "--output-schema", str(schema),
    "--output-last-message", str(result),
    prompt,
]
raise SystemExit(subprocess.run(argv, check=False).returncode)
```

The wrapper assumes `codex` is on `PATH` and the VM has configured authentication. It is a command plan example, not a live-model receipt.

### Concurrent state updates and inherited Git environment

After durable intent creation, coordinator patches and worker receipts share a
short per-job `state.lock`. Coordinator patches reload the latest state while
holding that lock; terminal worker state cannot be reverted by process discovery
or submission bookkeeping. Collection patches only its receipt, and worker writes
preserve any collected receipt. The worker lifetime lock remains separate and is
not inherited by stage commands.

Executor Git subprocesses, detached workers, and stage commands remove inherited
`GIT_*` variables before selecting a repository. Executor-owned commit identity
and optional-lock settings are then applied explicitly.

Detached workers use deterministic transient systemd user service units, separate
from the generated supervisor service control group. The unit identity includes
the repository Git directory and validated job identity. Supervisor restart does
not stop these units. Launch failure returns typed `systemd_job_launch_failed`;
an ambiguous launch retains durable identity and never retries the effect.
The user manager and `systemd-run` are required for the default adapter.
`LaneExecutor(..., local=True)` explicitly selects direct detached processes for
local callers/tests and does not provide the remote restart guarantee. Worker
environment is rebuilt with `env -i` to exclude manager-inherited Git routing.

Native lifecycle proof:
`tests/test_wave_launch_contract.py::test_systemd_generated_supervisor_restart_preserves_job`.
This test skips only when the systemd user manager cannot be accessed.
