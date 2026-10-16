"""Measured VM-pressure admission for wave supervisor lane submissions.

When a pending submission omits a PID, anonymous-memory, or disk cost, admission
uses the candidate lane's measured cost for that field as a conservative
estimate and records the estimate in ``AdmissionDecision.details``.
"""

from __future__ import annotations

import math
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

GIB = 1024**3
MIB = 1024**2
DEFAULT_PIDS_RESERVE = 64
DEFAULT_LANE_PIDS_COST = 66
DEFAULT_LANE_ANON_COST_BYTES = 512 * MIB
DEFAULT_MEMORY_RESERVE_BYTES = 512 * MIB
MEMORY_RESERVE_ENV = "WORKBAY_REMOTE_GATE_MEMORY_FLOOR_MB"
MEMINFO_PATH = Path("/proc/meminfo")
UNLIMITED_MEMORY_EXEMPTION_ENV = "WORKBAY_REMOTE_GATE_ALLOW_UNLIMITED_MEMORY"


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    admitted: bool
    reason: str
    details: Mapping[str, object] = field(default_factory=dict)


def _finite_number(value: object, *, integer: bool = False) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value < 0:
        return None
    if isinstance(value, int):
        return value
    if not math.isfinite(value) or (integer and not value.is_integer()):
        return None
    return int(value) if integer else value


def _gauge_number(gauges: Mapping[str, object], name: str, *, integer: bool = False) -> int | float | None:
    return _finite_number(gauges.get(name), integer=integer)


def _limit_value(gauges: Mapping[str, object], name: str) -> int | str | None:
    value = gauges.get(name)
    if value == "max":
        return "max"
    return _finite_number(value, integer=True)


def _pending_resource_costs(
    pending: int | object,
    candidate_costs: Mapping[str, int],
) -> tuple[dict[str, int], tuple[str, ...]] | None:
    resources = ("pids", "anon_bytes", "disk_bytes")
    if isinstance(pending, bool):
        return None
    if isinstance(pending, int):
        if pending < 0:
            return None
        return {key: pending * candidate_costs[key] for key in resources}, ()
    try:
        entries = iter(pending)  # type: ignore[arg-type]
    except TypeError:
        return None
    totals = {key: 0 for key in resources}
    estimates: list[str] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            return None
        for key in resources:
            if key not in entry:
                totals[key] += candidate_costs[key]
                estimates.append(f"pending[{index}].{key}")
                continue
            value = _finite_number(entry[key], integer=True)
            if value is None:
                return None
            totals[key] += value
    return totals, tuple(estimates)


def _decision(
    admitted: bool,
    reason: str,
    gauges: Mapping[str, object],
    pending_cost_estimates: tuple[str, ...] = (),
) -> AdmissionDecision:
    resource_error_cells = gauges.get("resource_error_cells")
    if not isinstance(resource_error_cells, Mapping):
        resource_error_cells = {
            "vm_pids": "not_instrumented",
            "vm_memory_oom_kill": None,
            "vm_memory_psi_burst": "not_instrumented",
            "vm_cpu": "not_instrumented",
            "vm_disk": "not_instrumented",
        }
    return AdmissionDecision(
        admitted,
        reason,
        {
            "resource_error_cells": dict(resource_error_cells),
            "memory_oom_kill_delta": "not_available_without_persistent_baseline",
            "pending_cost_estimates": pending_cost_estimates,
        },
    )


def decide(
    gauges: Mapping[str, object],
    pending: int | object,
    measured_cost: Mapping[str, object],
) -> AdmissionDecision:
    """Return a fail-closed measured-pressure decision with projection details."""
    required_gauges = (
        ("pids_current", True, "vm_pids"),
        ("memory_anon_bytes", True, "vm_memory"),
        ("memory_psi_avg60", False, "vm_memory_psi"),
        ("cpu_psi_avg60", False, "vm_cpu_psi"),
        ("disk_free_bytes", True, "vm_disk"),
        ("disk_floor_bytes", True, "vm_disk"),
        ("memory_headroom_pct", True, "vm_memory"),
        ("memory_psi_max", False, "vm_memory_psi"),
        ("cpu_psi_max", False, "vm_cpu_psi"),
        ("pids_reserve", True, "vm_pids"),
        ("memory_reserve_bytes", True, "vm_memory"),
    )
    values: dict[str, int | float] = {}
    for name, integer, reason_gauge in required_gauges:
        value = _gauge_number(gauges, name, integer=integer)
        if value is None or (name in {"pids_reserve", "memory_reserve_bytes"} and value <= 0):
            return _decision(False, f"vm_admission_unmeasured:{reason_gauge}", gauges)
        values[name] = value

    pids_max = _limit_value(gauges, "pids_max")
    if pids_max is None:
        return _decision(False, "vm_admission_unmeasured:vm_pids", gauges)
    memory_max = _limit_value(gauges, "memory_max_bytes")
    if memory_max is None:
        return _decision(False, "vm_admission_unmeasured:vm_memory", gauges)

    costs: dict[str, int] = {}
    for key, gauge_name in (("pids", "vm_pids"), ("anon_bytes", "vm_memory"), ("disk_bytes", "vm_disk")):
        cost = _finite_number(measured_cost.get(key), integer=True)
        if cost is None:
            return _decision(False, f"vm_admission_unmeasured:{gauge_name}_cost", gauges)
        costs[key] = cost

    pending_costs = _pending_resource_costs(pending, costs)
    if pending_costs is None:
        return _decision(False, "vm_admission_unmeasured:vm_pending", gauges)
    pending_totals, pending_cost_estimates = pending_costs

    projected_pids = values["pids_current"] + pending_totals["pids"] + costs["pids"]
    projected_anon = values["memory_anon_bytes"] + pending_totals["anon_bytes"] + costs["anon_bytes"]
    projected_job_dirs = pending_totals["disk_bytes"] + costs["disk_bytes"]

    def result(admitted: bool, reason: str) -> AdmissionDecision:
        decision = _decision(admitted, reason, gauges, pending_cost_estimates)
        return AdmissionDecision(
            admitted,
            reason,
            {
                **decision.details,
                "pending_resource_costs": dict(pending_totals),
                "projected_resource_costs": {
                    "pids": projected_pids,
                    "anon_bytes": projected_anon,
                    "disk_bytes": projected_job_dirs,
                },
            },
        )

    if not 1 <= values["memory_headroom_pct"] <= 100:
        return result(False, "vm_admission_unmeasured:vm_memory_headroom")

    if pids_max != "max" and projected_pids + values["pids_reserve"] > pids_max:
        return result(False, "vm_pids")

    if memory_max != "max":
        memory_limit = min(
            memory_max * values["memory_headroom_pct"] // 100,
            memory_max - values["memory_reserve_bytes"],
        )
    elif gauges.get("unlimited_memory_exemption") is True:
        memory_limit = None
    else:
        memory_available = _gauge_number(gauges, "memory_available_bytes", integer=True)
        if memory_available is None:
            return result(False, "vm_admission_unmeasured:vm_memory_limit")
        memory_limit = values["memory_anon_bytes"] + max(
            0,
            memory_available - values["memory_reserve_bytes"],
        )
    if memory_limit is not None and projected_anon > memory_limit:
        return result(False, "vm_memory")

    if values["memory_psi_avg60"] > values["memory_psi_max"]:
        return result(False, "vm_memory_psi")
    if values["cpu_psi_avg60"] > values["cpu_psi_max"]:
        return result(False, "vm_cpu_psi")

    if values["disk_free_bytes"] - projected_job_dirs < values["disk_floor_bytes"]:
        return result(False, "vm_disk")

    if memory_max == "max" and gauges.get("unlimited_memory_exemption") is True:
        return result(True, f"admit:{UNLIMITED_MEMORY_EXEMPTION_ENV}")
    return result(True, "admit")


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None


def _read_uint(path: Path) -> int | None:
    value = _read_text(path)
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        return None
    return int(value)


def _read_ceiling(path: Path) -> int | str | None:
    value = _read_text(path)
    if value == "max":
        return "max"
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        return None
    return int(value)


def _read_anon(path: Path) -> int | None:
    text = _read_text(path)
    if text is None:
        return None
    found: list[int] = []
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "anon":
            if len(fields) != 2 or re.fullmatch(r"[0-9]+", fields[1]) is None:
                return None
            found.append(int(fields[1]))
    return found[0] if len(found) == 1 else None


def _read_psi_avg60(path: Path) -> float | None:
    text = _read_text(path)
    if text is None:
        return None
    values: list[float] = []
    for line in text.splitlines():
        fields = line.split()
        if not fields or fields[0] != "some":
            continue
        matches = [item[6:] for item in fields[1:] if item.startswith("avg60=")]
        if len(matches) != 1 or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", matches[0]) is None:
            return None
        values.append(float(matches[0]))
    return values[0] if len(values) == 1 else None


def _read_memory_oom_kill(path: Path) -> int | None:
    text = _read_text(path)
    if text is None:
        return None
    values: list[int] = []
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "oom_kill":
            if len(fields) != 2 or re.fullmatch(r"[0-9]+", fields[1]) is None:
                return None
            values.append(int(fields[1]))
    return values[0] if len(values) == 1 else None


def _read_mem_available(path: Path) -> int | None:
    text = _read_text(path)
    if text is None:
        return None
    values: list[int] = []
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "MemAvailable:":
            if len(fields) != 3 or re.fullmatch(r"[0-9]+", fields[1]) is None or fields[2] != "kB":
                return None
            values.append(int(fields[1]) * 1024)
    return values[0] if len(values) == 1 else None


def _env_number(name: str, default: int | float, *, integer: bool = False) -> int | float | None:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = float(raw) if not integer else int(raw)
    except ValueError:
        return None
    return _finite_number(parsed, integer=integer)


def _env_positive_integer(name: str, default: int) -> int | None:
    value = _env_number(name, default, integer=True)
    return None if value is None or value <= 0 else int(value)


def read_gauges(root: Path) -> dict[str, object]:
    """Read cgroup v2, proc, PSI and disk values; absent values stay explicit.

    The absolute shared-memory reserve defaults to 512 MiB and is configurable
    with ``WORKBAY_REMOTE_GATE_MEMORY_FLOOR_MB``. The floor must be a positive
    integer number of MiB; zero, negative, or malformed values are refused as
    unmeasured. The PID floor likewise requires a positive integer.
    For unlimited cgroups,
    ``MemAvailable`` from ``/proc/meminfo`` supplies a host bound. An operator
    may bypass that bound only with ``WORKBAY_REMOTE_GATE_ALLOW_UNLIMITED_MEMORY=1``;
    the resulting admission reason names the exemption. The ``oom_kill`` value
    is cumulative; a delta needs a prior sample, so decisions record that the
    delta is unavailable without persistent state.
    """
    uid = os.getuid()
    pids_root = Path(os.environ.get("WORKBAY_REMOTE_GATE_PIDS_ROOT", f"/sys/fs/cgroup/user.slice/user-{uid}.slice"))
    try:
        disk_free: int | None = shutil.disk_usage(root).free
    except OSError:
        disk_free = None

    disk_floor_gb = _env_number("WORKBAY_REMOTE_GATE_DISK_FLOOR_GB", 10, integer=False)
    disk_floor_bytes = None if disk_floor_gb is None else int(disk_floor_gb * GIB)
    memory_reserve_mb = _env_positive_integer(
        MEMORY_RESERVE_ENV,
        DEFAULT_MEMORY_RESERVE_BYTES // MIB,
    )
    memory_reserve_bytes = None if memory_reserve_mb is None else int(memory_reserve_mb) * MIB
    return {
        "pids_current": _read_uint(pids_root / "pids.current"),
        "pids_max": _read_ceiling(pids_root / "pids.max"),
        "memory_anon_bytes": _read_anon(pids_root / "memory.stat"),
        "memory_max_bytes": _read_ceiling(pids_root / "memory.max"),
        "memory_available_bytes": _read_mem_available(MEMINFO_PATH),
        "memory_psi_avg60": _read_psi_avg60(pids_root / "memory.pressure"),
        "cpu_psi_avg60": _read_psi_avg60(pids_root / "cpu.pressure"),
        "disk_free_bytes": disk_free,
        "disk_floor_bytes": disk_floor_bytes,
        "memory_headroom_pct": _env_number("WORKBAY_REMOTE_GATE_HEADROOM_PCT", 75, integer=True),
        "memory_psi_max": _env_number("WORKBAY_REMOTE_GATE_MEM_PSI_MAX", 10),
        "cpu_psi_max": _env_number("WORKBAY_REMOTE_GATE_CPU_PSI_MAX", 60),
        "pids_reserve": _env_positive_integer("WORKBAY_REMOTE_GATE_PID_FLOOR", DEFAULT_PIDS_RESERVE),
        "memory_reserve_bytes": memory_reserve_bytes,
        "unlimited_memory_exemption": os.environ.get(UNLIMITED_MEMORY_EXEMPTION_ENV) == "1",
        "resource_error_cells": {
            "vm_pids": "not_instrumented",
            "vm_memory_oom_kill": _read_memory_oom_kill(pids_root / "memory.events"),
            "vm_memory_psi_burst": "not_instrumented",
            "vm_cpu": "not_instrumented",
            "vm_disk": "not_instrumented",
        },
    }


def _object_disk_bytes(repo: Path) -> int | None:
    """Measure packed and loose Git objects, with a bounded Git wait."""
    try:
        result = subprocess.run(
            ["git", "-C", os.fspath(repo), "count-objects", "-v"],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        lines = result.stdout.decode("ascii").splitlines()
    except UnicodeDecodeError:
        return None
    sizes: dict[str, int] = {}
    for line in lines:
        key, separator, value = line.partition(":")
        if key not in {"size-pack", "size"}:
            continue
        value = value.strip()
        if not separator or re.fullmatch(r"[0-9]+", value) is None or key in sizes:
            return None
        sizes[key] = int(value)
    if set(sizes) != {"size-pack", "size"}:
        return None
    return (sizes["size-pack"] + sizes["size"]) * 1024


def lane_cost(repo: Path) -> dict[str, int | None]:
    """Estimate one lane's VM cost from configured floors and copied Git objects.

    Checkout files are not counted, a known-unknown; ``--no-hardlinks`` clones
    copy the measured object database instead of sharing its object storage.
    """
    lane_pids = _env_number("WORKBAY_REMOTE_GATE_LANE_PIDS_FLOOR", DEFAULT_LANE_PIDS_COST, integer=True)
    lane_anon_mb = _env_number(
        "WORKBAY_REMOTE_GATE_LANE_ANON_FLOOR_MB", DEFAULT_LANE_ANON_COST_BYTES // MIB, integer=True
    )
    return {
        "pids": None if lane_pids is None else int(lane_pids),
        "anon_bytes": None if lane_anon_mb is None else int(lane_anon_mb) * MIB,
        "disk_bytes": _object_disk_bytes(repo),
    }


def measured(repo: Path) -> Callable[[str, str, int], AdmissionDecision]:
    """Build a supervisor admission callback reading live gauges for each lane."""

    def check(_wave: str, _lane_id: str, pending: int) -> AdmissionDecision:
        return decide(read_gauges(repo), pending, lane_cost(repo))

    return check
