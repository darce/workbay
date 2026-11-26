# Runbook — VM Capacity Tuning (`internal`)

> Operator-actionable reference for the four WorkBay VM capacity levers introduced in
> `internal` (Wave 0 + Wave 1). Companion to `remote-gate-provisioning.md`
> and `remote-grok-review-lanes.md`.
>
> **Canon baseline:** `heuristics-canon` **v0.26.2** (`heuristics-canon/release@5`) —
> `[OPS-24]`, `[OPS-32]`, `[PERF-04]`, `[PERF-06]`, `[PERF-09]`, `[CON-11]`, `[SECD-05]`.

---

## Quick Reference

| Lever | Primary Entrypoint / Module | Key Env / Constants | Failure / Default Behavior |
|---|---|---|---|
| 1. Unified CPU-PSI admission & slice placement | `wave_admission.evaluate_cpu_psi_admission`, `wave_executor.resolve_wave_slice_properties` | `WORKBAY_REMOTE_GATE_CPU_PSI_MAX` (default `60.0`), `WORKBAY_WAVE_SLICE` (`wb-wave.slice`), `WORKBAY_WAVE_CPU_WEIGHT` (`50`), `WORKBAY_WAVE_MEMORY_HIGH` (`75%`) | Fail-closed when CPU PSI `some avg60 >= ceiling` or invalid/missing gauge (`[SECD-05]`, `[OPS-24]`) |
| 2. VM-wide `flock` compute semaphore | `workbay_orchestrator_mcp.orchestration.vm_compute_semaphore`, `scripts/remote_agent.sh` | `WORKBAY_VM_COMPUTE_SLOTS` (unset = disabled in `remote_agent.sh`; CLI default `2`), `WORKBAY_VM_COMPUTE_SLOT_DIR` | Opt-in in `remote_agent.sh`; fail-open fallback if module absent in lane venv (`[SECD-05]`); kernel auto-releases on `SIGKILL` (`[CON-11]`) |
| 3. Host-local fast-check routing | `statechart_tla.select_tla_execution_target` | `DEFAULT_HOST_MAX_DISTINCT_STATES = 5000`, `force_remote=True` | Fail-closed to `remote` when local Java or `tla2tools.jar` is absent or state space exceeds limit (`[PERF-04]`, `[SECD-05]`) |
| 4. Read-only VM capacity diagnostics | `workbay_protocol.remote_probe.probe_remote_capacity`, `evaluate_vm_capacity_warnings` | Warning codes: `no_swap`, `cpu_psi_saturated`, `high_non_ci_rss` | Strictly read-only; never mutates or kills remote processes (`[OPS-32]`, `[PERF-09]`) |

---

## 1. Unified CPU-PSI Admission & Systemd Slice Placement

Prevents CPU run-queue saturation from starving co-resident VM services or causing
cascade timeouts across concurrent wave workers (`[OPS-24]`, `[PERF-06]`, `[SECD-05]`).

- **Admission ceiling:** `WORKBAY_REMOTE_GATE_CPU_PSI_MAX` (default `60.0`, aligned
  with `lane_queue.py` and `scripts/remote_agent.sh`).
- **Evaluation:** `wave_admission.evaluate_cpu_psi_admission(*, cpu_some_avg60, ceiling=None)`
  is integrated into `wave_admission.decide()` whenever `cpu_some_avg60` or
  `cpu_psi_some_avg60` is present in gauges. It fails closed (refuses admission) when:
  - `cpu_some_avg60 >= ceiling`, or
  - the measurement, ceiling, or env override is missing, non-finite, or out of range.
- **Slice isolation:** `wave_executor.resolve_wave_slice_properties(env=None)` wires
  `systemd-run` worker launches into a bounded slice so wave compute bursts yield CPU
  and memory pressure gracefully:
  - `WORKBAY_WAVE_SLICE` (default `wb-wave.slice`)
  - `WORKBAY_WAVE_CPU_WEIGHT` (default `50`)
  - `WORKBAY_WAVE_MEMORY_HIGH` (default `75%`)

### Operator Tuning

```bash
# Tighten CPU PSI ceiling on a heavily shared VM:
export WORKBAY_REMOTE_GATE_CPU_PSI_MAX=45.0

# Adjust wave slice resource shares (validated fail-closed):
export WORKBAY_WAVE_SLICE=wb-wave.slice
export WORKBAY_WAVE_CPU_WEIGHT=40
export WORKBAY_WAVE_MEMORY_HIGH=70%
```

---

## 2. VM-Wide `flock` Compute Semaphore

Allows many I/O-bound off-box LLM lanes to stay active concurrently while bounding
simultaneous CPU/memory-heavy local bursts (`pytest`, `eslint`, `tlc2.TLC`) to a fixed
number of compute slots (`[CON-11]`, `[PERF-04]`, `[SECD-05]`).

### CLI Usage

```bash
# Run a compute-heavy command under a 2-slot VM-wide semaphore:
python -m workbay_orchestrator_mcp.orchestration.vm_compute_semaphore -- pytest -q

# Override slot count, lock directory, and wait timeout:
python -m workbay_orchestrator_mcp.orchestration.vm_compute_semaphore \
    --slots 2 \
    --slot-dir /tmp/workbay-vm-compute-slots \
    --timeout 120 \
    -- pytest -q
```

- **Slot directory precedence:** `--slot-dir` > `WORKBAY_VM_COMPUTE_SLOT_DIR` >
  `$XDG_RUNTIME_DIR/workbay-compute-slots` > `/tmp/workbay-vm-compute-slots`.
- **Crash safety (`[CON-11]`):** Slot ownership is backed by kernel `fcntl.flock` on
  `slot-{i}.lock`. Lock files remain on disk so waiters lock the same inode; an abrupt
  `SIGKILL` or OOM kill releases the slot immediately via the kernel.
- **Exit code fidelity:** Preserves exact child exit codes (`128 + sig` for signals,
  `124` on semaphore timeout).

### Opt-In Wiring in `scripts/remote_agent.sh`

Off-box self-verify (`_emit_off_box_selfverify`) supports the compute semaphore via
explicit opt-in so existing runs stay unchanged unless enabled:

```bash
# Enable a 2-slot compute semaphore for off-box self-verify:
export WORKBAY_VM_COMPUTE_SLOTS=2
# Optional custom lock directory (validated for safe path characters):
export WORKBAY_VM_COMPUTE_SLOT_DIR=/tmp/workbay-vm-compute-slots
```

- Must be an integer `>= 1` when set (invalid values fail fast with exit `2`).
- At runtime on the VM, `scripts/remote_agent.sh` probes whether
  `workbay_orchestrator_mcp.orchestration.vm_compute_semaphore` is importable in
  `$SBX/.venv/bin/python`. If present, it wraps self-verify execution while preserving
  exact exit codes, stdout/stderr capture, and residual budget timeouts.
- If the module is absent on older branches, it logs a diagnostic and **fails open**
  to unwrapped self-verify (`[SECD-05]`).

---

## 3. Host-Local Fast-Check Routing (TLA+ / TLC)

Offloads small bounded model checks from the shared VM onto the operator's local host
when local prerequisites are available, reducing VM CPU contention without risking
unbounded host runs (`[PERF-04]`, `[PERF-06]`, `[SECD-05]`).

- **Decision function:** `statechart_tla.select_tla_execution_target(*, max_distinct_states, has_local_java, has_local_tla_jar, host_state_limit=DEFAULT_HOST_MAX_DISTINCT_STATES, force_remote=False)`
- **Default threshold:** `DEFAULT_HOST_MAX_DISTINCT_STATES = 5000`.
- **Routing rules (fail-closed to `remote`):**
  - Routes to **`host`** only when `force_remote=False`, `has_local_java=True`,
    `has_local_tla_jar=True`, and `0 <= max_distinct_states <= host_state_limit`.
  - Routes to **`remote`** when `force_remote=True`, when Java or `tla2tools.jar` is
    absent on the host, when `max_distinct_states > host_state_limit`, or when inputs
    are `None`, boolean, or negative.

### Operator Guidance

- Keep local Java and `tla2tools.jar` installed for sub-5,000-state fast checks.
- Pass `force_remote=True` whenever you need canonical VM reproducibility or are
  checking state spaces above `5000` distinct states.

---

## 4. Read-Only VM Capacity Diagnostics

Inspects VM health over SSH without mutating host state or terminating processes
(`[OPS-32]`, `[PERF-09]`).

- **Probe:** `workbay_protocol.remote_probe.probe_remote_capacity(host, *, timeout_s=10.0)`
  validates the host string before spawning `ssh` (using `--` before host and
  `stdin=DEVNULL` / `-n`), parses `nproc`, `/proc/meminfo`, `/proc/pressure/cpu`, and
  non-CI RSS share, and fails closed on timeout, unreachability, or parse errors.
- **Warning evaluator:** `evaluate_vm_capacity_warnings(...)` returns `[]` on healthy
  snapshots and emits structured `CapacityWarning` codes when thresholds trip:

| Warning Code | Trigger | Operator Remediation |
|---|---|---|
| `no_swap` | `SwapTotal == 0` on the VM | Provision a modest swapfile (see below) so transient memory spikes page out cold pages instead of triggering immediate OOM kills. |
| `cpu_psi_saturated` | `cpu_some_avg60 >= 60.0` (default `DEFAULT_CPU_PSI_CEILING`) | Lower `WORKBAY_VM_COMPUTE_SLOTS` (e.g. `2` -> `1`), reduce `WORKBAY_REMOTE_AGENT_MAX_LANES`, or tighten `WORKBAY_REMOTE_GATE_CPU_PSI_MAX`. |
| `high_non_ci_rss` | Non-CI processes consume `> 25%` of total RAM (`DEFAULT_NON_CI_RSS_RATIO_MAX = 0.25`) | Audit co-resident services (see below) before raising lane concurrency or memory caps. |

### Operator Remediation: Adding Swap

Run on the VM as root (example: 4 GiB swapfile):

```bash
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
swapon --show && free -h
```

### Operator Remediation: Checking Non-CI Services

Inspect top non-WorkBay RSS consumers and systemd slices before adjusting admission
floors:

```bash
# Top processes by RSS:
ps -eo pid,ppid,user,rss,comm --sort=-rss | head -n 20

# Inspect cgroup memory and CPU usage by slice:
systemd-cgtop -b -n 1 --order=memory

# Check current CPU pressure (PSI):
cat /proc/pressure/cpu
```

---

## Canon Citations

All citations verified against `heuristics-canon` **v0.26.2** (`heuristics-canon/release@5`):

- **`[OPS-24]`** — Capacity admission & saturation protection (unified CPU-PSI gate and slice bounds).
- **`[OPS-32]`** — Read-only operational diagnostics & safe remediation procedures.
- **`[PERF-04]`** — Amdahl's law & bounded concurrency for compute-heavy bursts (`vm_compute_semaphore`, host fast-check offload).
- **`[PERF-06]`** — Resource isolation & contention avoidance (`wb-wave.slice`, `CPUWeight`, host-vs-VM routing).
- **`[PERF-09]`** — Continuous resource visibility (swap, CPU PSI, and non-CI RSS telemetry).
- **`[CON-11]`** — Atomic, crash-safe synchronization (`fcntl.flock` kernel-managed slot locks).
- **`[SECD-05]`** — Fail-safe defaults (fail-closed admission/routing, opt-in semaphore with fail-open fallback on older branches).
