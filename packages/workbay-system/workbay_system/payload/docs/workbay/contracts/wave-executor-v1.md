# Wave executor v1

`workbay_orchestrator_mcp.orchestration.wave_executor.create(*, repo, spec)` returns a `(LaneExecutor, Gate)` pair for the wave supervisor. The factory receives the resolved repository and the raw loaded wave spec. A configured supervisor adapter remains an explicit deployment override.

The executor is VM-local and refuses creation outside Linux. Its single authority is Git plus durable files below the repository's absolute Git directory at `workbay-waveexecutor/waves/<wave>/`. It has no SSH, push, laptop-driver, shared-database, or integration behavior. The supervisor alone integrates lane tips onto `wave/<wave>`.

## Frozen spec

Execution configuration is an additional top-level field; scheduler fields keep their current meaning.

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

The command arrays are trusted launcher configuration, not worker-authored input. Each lane requires four non-empty argv arrays, a positive finite `timeout_s` (maximum one day), and explicit backend/model/effort/speed strings. There is no backend-name allowlist. A plan is strict canonical JSON; its SHA-256 digest is frozen at factory creation and written into every job. A changed plan for an existing wave/lane/attempt returns `plan_mismatch`.

The worker receives `WORKBAY_WAVE_RESULT_PATH`, `WORKBAY_WAVE_JOB_ID`, `WORKBAY_WAVE_PLAN_DIGEST`, `WORKBAY_WAVE_STAGE`, and the configured backend/model/effort/speed as environment variables. Those values describe the requested configuration; the executor cannot prove that a trusted launcher used them. The launcher must turn the settings into its own command/API request and treat any provider response metadata it receives as separate evidence.

## Admission, isolation, and recovery

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

`collect(job)` fetches the lane commit from that job's local clone into the authority repository by local path only. It verifies the clone's `lane/<lane>` ref, the exact full tip, and ancestry from the admission base, then records a durable collect receipt containing the tip, tree, plan digest, and pipeline result. It returns the commit SHA unchanged. `Gate(repo, lane_id, tip)` accepts only a collected receipt for that exact SHA plus successful implement, review (and optional fix), and Linux-test evidence. It rejects arbitrary text verdicts, an uncollected commit, changed review/test trees, failed committed content, missing stages, and stale tips.

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

Install this small trusted launcher as `/srv/workbay/bin/wave-codex-stage.py` on the VM. It asks Codex for the exact executor verdict schema and lets `codex exec --output-last-message` write the result file that the executor validates. The executor controls the timeout and kills the launcher's process group on expiry. This is launcher plumbing only: the sample prompt provides no task objective or owned-path scope, so a deployment must supply a deployment-specific per-lane brief and scope. The gate checks valid stage and test evidence for the exact collected tip, but does not require that tip to differ from the admission base; it does not by itself reject a no-op lane.

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
