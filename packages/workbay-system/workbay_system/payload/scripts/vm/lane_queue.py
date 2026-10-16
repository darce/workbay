#!/usr/bin/env python3
"""Admit queued VM jobs in a bounded burst measured with live cgroup, PSI, and disk gauges."""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import resource
import select
import signal
import socket
import stat
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

# BEGIN driver lifetime (single source: edit here; the payload twin must stay byte-identical)
ExitKind = Literal["continue", "idle_ttl", "stop_requested"]


@dataclass(frozen=True)
class ExitDecision:
    """The current driver's typed continue or exit decision."""

    kind: ExitKind
    reason: str
    idle_seconds: float


class DriverLifetime:
    """Track idle time and decide whether a driver should stop."""

    def __init__(self, idle_ttl_seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        if isinstance(idle_ttl_seconds, bool) or not isinstance(idle_ttl_seconds, (int, float)):
            raise ValueError("idle_ttl_seconds must be a finite non-negative number")
        if not math.isfinite(idle_ttl_seconds) or idle_ttl_seconds < 0:
            raise ValueError("idle_ttl_seconds must be a finite non-negative number")
        self.idle_ttl_seconds = float(idle_ttl_seconds)
        self.clock = clock
        self._idle_since: float | None = None

    def _now(self) -> float:
        instant = self.clock()
        if isinstance(instant, bool) or not isinstance(instant, (int, float)) or not math.isfinite(instant):
            raise ValueError("now must be a finite number")
        return float(instant)

    def observe(self, work_pending: bool) -> float:
        """Update the idle clock, returning the observed idle duration."""
        instant = self._now()
        if work_pending:
            self._idle_since = None
            return 0.0
        if self._idle_since is None:
            self._idle_since = instant
        return max(0.0, instant - self._idle_since)

    def should_exit(self, stop_requested: bool = False) -> ExitDecision:
        """Return the highest-priority stop condition, or continue."""
        now = self._now()
        idle_seconds = 0.0 if self._idle_since is None else max(0.0, now - self._idle_since)
        if stop_requested:
            return ExitDecision("stop_requested", "stop_requested", idle_seconds)
        if self._idle_since is not None and idle_seconds >= self.idle_ttl_seconds:
            return ExitDecision("idle_ttl", "idle_ttl_expired", idle_seconds)
        return ExitDecision("continue", "work_pending_or_idle_ttl_not_reached", idle_seconds)


def write_exit_receipt(path: str | Path, decision: ExitDecision, **extra: object) -> None:
    """Atomically replace a newline-terminated JSON exit receipt."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    receipt: dict[str, object] = {
        **extra,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "pid": os.getpid(),
        "kind": decision.kind,
        "reason": decision.reason,
        "idle_seconds": decision.idle_seconds,
    }
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            os.chmod(temporary_path, 0o600)
            stream.write(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


# END driver lifetime

GIB = 1024**3
MIB = 1024**2
DEFAULT_TICK_S = 5.0
DEFAULT_IDLE_TTL_S = 900
DEFAULT_MAX_WAIT_S = 1800
DEFAULT_PIDS_RESERVE = 64
DEFAULT_LANE_PIDS_COST = 66
DEFAULT_LANE_ANON_COST_BYTES = 512 * MIB
DEFAULT_MEMORY_HEADROOM_PCT = 75
DEFAULT_MEMORY_PSI_MAX = 10.0
DEFAULT_CPU_PSI_MAX = 60.0
DEFAULT_DISK_FLOOR_BYTES = 10 * GIB
START_TIMEOUT_S = 30
# Declared wall-clock stage budget for one tick's burst drain; the worst case is this plus one start timeout.
DEFAULT_BURST_BUDGET_S = 20.0
STOP_FILE_NAME = ".queue-stop"
EXIT_RECEIPT_NAME = ".queue-exit.json"


@dataclass(frozen=True)
class AdmissionDecision:
    admitted: bool
    reason: str


@dataclass(frozen=True)
class TickResult:
    started_job_id: str | None
    expired_job_ids: tuple[str, ...]
    last_refusal_reason: str | None
    queued: int
    running: int
    lock_acquired: bool = True
    # Every job this tick started, FIFO order; started_job_id stays the first for single-start callers.
    started_job_ids: tuple[str, ...] = ()


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


def _pending_disk_cost(pending: object, candidate_disk_bytes: int) -> int | None:
    if isinstance(pending, bool):
        return None
    if isinstance(pending, int):
        return pending * candidate_disk_bytes if pending >= 0 else None
    try:
        entries = list(pending)  # type: ignore[arg-type]
    except TypeError:
        return None
    total = 0
    for entry in entries:
        value = entry.get("disk_bytes") if isinstance(entry, Mapping) else entry
        disk_bytes = _finite_number(value, integer=True)
        if disk_bytes is None:
            return None
        total += disk_bytes
    return total


def admit(
    gauges: Mapping[str, object],
    pending: int | object,
    measured_cost: Mapping[str, object],
) -> AdmissionDecision:
    """Return a pure admission decision from measured gauges and projected cost.

    ``pending`` is the number of other job directories projected ahead of this
    candidate. The disk check reserves their measured directory sizes plus this
    candidate's size, making disk the queue bound without a fixed queue depth.
    """
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
    )
    values: dict[str, int | float] = {}
    for name, integer, reason_gauge in required_gauges:
        value = _gauge_number(gauges, name, integer=integer)
        if value is None:
            return AdmissionDecision(False, f"vm_admission_unmeasured:{reason_gauge}")
        values[name] = value

    pids_max = _limit_value(gauges, "pids_max")
    if pids_max is None:
        return AdmissionDecision(False, "vm_admission_unmeasured:vm_pids")
    memory_max = _limit_value(gauges, "memory_max_bytes")
    if memory_max is None:
        return AdmissionDecision(False, "vm_admission_unmeasured:vm_memory")

    costs: dict[str, int] = {}
    for key, gauge_name in (("pids", "vm_pids"), ("anon_bytes", "vm_memory"), ("disk_bytes", "vm_disk")):
        cost = _finite_number(measured_cost.get(key), integer=True)
        if cost is None:
            return AdmissionDecision(False, f"vm_admission_unmeasured:{gauge_name}_cost")
        costs[key] = cost

    pending_disk_bytes = _pending_disk_cost(pending, costs["disk_bytes"])
    if pending_disk_bytes is None:
        return AdmissionDecision(False, "vm_admission_unmeasured:vm_pending")

    if not 1 <= values["memory_headroom_pct"] <= 100:
        return AdmissionDecision(False, "vm_admission_unmeasured:vm_memory_headroom")

    if pids_max != "max" and values["pids_current"] + costs["pids"] + values["pids_reserve"] > pids_max:
        return AdmissionDecision(False, "vm_pids")

    if memory_max != "max":
        anon_after = values["memory_anon_bytes"] + costs["anon_bytes"]
        memory_limit = memory_max * values["memory_headroom_pct"] / 100
        if anon_after > memory_limit:
            return AdmissionDecision(False, "vm_memory")

    if values["memory_psi_avg60"] > values["memory_psi_max"]:
        return AdmissionDecision(False, "vm_memory_psi")
    if values["cpu_psi_avg60"] > values["cpu_psi_max"]:
        return AdmissionDecision(False, "vm_cpu_psi")

    projected_job_dirs = pending_disk_bytes + costs["disk_bytes"]
    if values["disk_free_bytes"] - projected_job_dirs < values["disk_floor_bytes"]:
        return AdmissionDecision(False, "vm_disk")

    return AdmissionDecision(True, "admit")


# (cost key, gauge, +1 when launches raise the gauge / -1 when they lower it)
BURST_RESERVED_GAUGES = (
    ("pids", "pids_current", 1),
    ("anon_bytes", "memory_anon_bytes", 1),
    ("disk_bytes", "disk_free_bytes", -1),
)


def unobserved_reservations(
    baseline: Mapping[str, object],
    fresh: Mapping[str, object],
    reserved: Mapping[str, int],
) -> dict[str, int]:
    """Aggregate movement has no job identity and supplies no reservation credit.

    Keep the old aggregate-only interface conservative. The drain credits only its
    accepted, identity-bound samples, individually bounded by each job's reservation.
    """
    return {key: reserved.get(key, 0) for key, _, _ in BURST_RESERVED_GAUGES}


# Bounds evidence collection, not admission capacity. In-process injected readers
# must be nonblocking; native RPCs and file reads below have explicit bounds.
ATTRIBUTION_BUDGET_NS = 500_000_000
ATTRIBUTION_FILE_LIMIT = 65536


def _attribution_text(path: Path) -> str | None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
        try:
            data = os.read(fd, ATTRIBUTION_FILE_LIMIT + 1)
        finally:
            os.close(fd)
        return data.decode("ascii").strip() if len(data) <= ATTRIBUTION_FILE_LIMIT else None
    except (OSError, UnicodeError):
        return None


def _sample_identity(sample: object, identity: Mapping[str, object], began_ns: int, ended_ns: int) -> bool:
    if not isinstance(sample, Mapping):
        return False
    if any(sample.get(key) != identity[key] for key in ("job_id", "unit", "launch_not_before_ns")):
        return False
    observed = _finite_number(sample.get("observed_at_ns"), integer=True)
    return observed is not None and began_ns <= observed <= ended_ns


def _disk_sample(sample: Mapping[str, object], job_dir: Path) -> int | None:
    """Only the exact job scope with a measured usage value can establish disk credit."""
    if sample.get("disk_scope") != str(job_dir):
        return None
    value = _finite_number(sample.get("disk_usage_bytes"), integer=True)
    return None if value is None else int(value)


def _job_credit(
    reservation: Mapping[str, Any], sample: object, began_ns: int, ended_ns: int,
) -> tuple[dict[str, int], dict[str, object]]:
    credit = {key: 0 for key, _, _ in BURST_RESERVED_GAUGES}
    identity = reservation["identity"]
    record: dict[str, object] = {**identity, "credit": credit, "fields": {}}
    if not reservation["identity_valid"] or not _sample_identity(sample, identity, began_ns, ended_ns):
        record["status"] = "unknown_identity_or_stale_sample"
        return credit, record
    record["status"] = "identity_bound"
    record["source"] = sample.get("source", "injected")
    fields = record["fields"]
    for key in credit:
        if key == "disk_bytes":
            before = reservation["disk_before"]
            after = _disk_sample(sample, reservation["job_dir"])
            value = None if before is None or after is None else max(0, after - before)
        else:
            value = _finite_number(sample.get(key), integer=True)
        fields[key] = "unknown_reserved" if value is None else "measured"
        if value is not None:
            credit[key] = min(reservation["cost"][key], int(value))
    return credit, record


def _consistent_job_credits(
    accepted: list[dict[str, Any]], samples: list[object],
    measure: Mapping[str, object], began_ns: int, ended_ns: int,
) -> list[tuple[dict[str, int], dict[str, object]]]:
    """Validate raw scoped totals before using any reservation-capped credit.

    Job scopes precede the enclosing gauge read, but those reads are not atomic
    with resource writers. If they contradict, retain all reservations for that
    resource; neither aggregate growth nor exits establish job ownership.
    Disk free space cannot bound scoped usage, so only exact-scope usage deltas
    validated by _job_credit can credit disk.
    """
    results = []
    for reservation, sample in zip(accepted, samples):
        credit, record = _job_credit(reservation, sample, began_ns, ended_ns)
        unit = reservation["identity"]["unit"]
        if sum(other["identity"]["unit"] == unit for other in accepted) != 1:
            credit = {key: 0 for key, _, _ in BURST_RESERVED_GAUGES}
            record.update(status="unknown_shared_unit", credit=credit)
        results.append((credit, record))

    for key, gauge, direction in BURST_RESERVED_GAUGES:
        if direction < 0:
            continue
        measured = [
            (sample, credit, record)
            for sample, (credit, record) in zip(samples, results)
            if record["status"] not in ("unknown_identity_or_stale_sample", "unknown_shared_unit")
            and record["fields"].get(key) == "measured"
        ]
        total = sum(int(sample[key]) for sample, _, _ in measured)
        current = _gauge_number(measure, gauge, integer=True)
        if current is not None and total <= current:
            continue
        status = "unknown_aggregate" if current is None else "contradictory_aggregate"
        for _, credit, record in measured:
            credit[key] = 0
            record["fields"][key] = f"{status}_reserved"
            record["status"] = status
            record.setdefault("consistency", {})[key] = {"scoped_total": total, "aggregate": current}
    return results


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


def _env_number(name: str, default: int | float, *, integer: bool = False) -> int | float | None:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = float(raw) if not integer else int(raw)
    except ValueError:
        return None
    return _finite_number(parsed, integer=integer)


def resolve_root(value: str | Path | None = None) -> Path:
    raw = value if value is not None else os.environ.get("WORKBAY_REMOTE_AGENT_ROOT", "workbay-sandbox")
    root = Path(raw).expanduser()
    if not root.is_absolute():
        root = Path.home() / root
    return root.resolve()


def read_gauges(root: Path) -> dict[str, object]:
    """Read cgroup v2, PSI and disk values; absent or malformed values stay unreadable."""
    uid = os.getuid()
    pids_root = Path(os.environ.get("WORKBAY_REMOTE_GATE_PIDS_ROOT", f"/sys/fs/cgroup/user.slice/user-{uid}.slice"))
    try:
        disk_free: int | None = shutil.disk_usage(root).free
    except OSError:
        disk_free = None

    disk_floor_gb = _env_number("WORKBAY_REMOTE_GATE_DISK_FLOOR_GB", 10, integer=False)
    disk_floor_bytes = None if disk_floor_gb is None else int(disk_floor_gb * GIB)
    gauges: dict[str, object] = {
        "pids_current": _read_uint(pids_root / "pids.current"),
        "pids_max": _read_ceiling(pids_root / "pids.max"),
        "memory_anon_bytes": _read_anon(pids_root / "memory.stat"),
        "memory_max_bytes": _read_ceiling(pids_root / "memory.max"),
        "memory_psi_avg60": _read_psi_avg60(pids_root / "memory.pressure"),
        "cpu_psi_avg60": _read_psi_avg60(pids_root / "cpu.pressure"),
        "disk_free_bytes": disk_free,
        "disk_floor_bytes": disk_floor_bytes,
        "memory_headroom_pct": _env_number("WORKBAY_REMOTE_GATE_HEADROOM_PCT", 75, integer=True),
        "memory_psi_max": _env_number("WORKBAY_REMOTE_GATE_MEM_PSI_MAX", 10),
        "cpu_psi_max": _env_number("WORKBAY_REMOTE_GATE_CPU_PSI_MAX", 60),
        "pids_reserve": _env_number("WORKBAY_REMOTE_GATE_PID_FLOOR", DEFAULT_PIDS_RESERVE, integer=True),
    }
    return gauges


def _default_measured_cost(job_dir: Path) -> dict[str, int | None]:
    """Use the existing VM lane cost floors and measure this queued directory's bytes."""
    lane_pids = _env_number("WORKBAY_REMOTE_GATE_LANE_PIDS_FLOOR", DEFAULT_LANE_PIDS_COST, integer=True)
    lane_anon_mb = _env_number(
        "WORKBAY_REMOTE_GATE_LANE_ANON_FLOOR_MB", DEFAULT_LANE_ANON_COST_BYTES // MIB, integer=True
    )
    return {
        "pids": None if lane_pids is None else int(lane_pids),
        "anon_bytes": None if lane_anon_mb is None else int(lane_anon_mb) * MIB,
        "disk_bytes": _directory_size(job_dir),
    }


def _directory_size(path: Path) -> int | None:
    total = 0

    def fail_walk(error: OSError) -> None:
        raise error

    try:
        for current, dirnames, filenames in os.walk(path, followlinks=False, onerror=fail_walk):
            dirnames[:] = [name for name in dirnames if not (Path(current) / name).is_symlink()]
            for name in filenames:
                item = Path(current) / name
                if not item.is_symlink():
                    total += item.stat().st_size
    except OSError:
        return None
    return total


def _iso_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_epoch(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        try:
            return float(value)
        except OverflowError:
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            return None
    return None


def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    data = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    with temporary.open("w", encoding="utf-8") as stream:
        os.chmod(temporary, 0o600)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


# A holder chain uses one parent IPC descriptor regardless of pending inventory.
# Each process owns at most its measured descriptor headroom worth of real flocks.
# Native publication forbids inode replacement; legacy writers retain the .lock ABI.
LOCK_IO_FDS = 12  # gate(2), launch stdout/stderr(4), exec error(2), probe/atomic IO(4)


def _fd_headroom() -> int:
    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    if limit == resource.RLIM_INFINITY:
        limit = os.sysconf("SC_OPEN_MAX")
    # Directory enumeration temporarily opens one descriptor; counting it is conservative.
    return int(limit) - len(os.listdir("/proc/self/fd"))


def _rss_bytes(pid: int | str = "self") -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise ValueError("holder RSS unavailable")


def _ipc_read(channel: socket.socket, count: int, deadline: float | None = None) -> bytes:
    if deadline is None:
        deadline = time.monotonic() + (channel.gettimeout() or START_TIMEOUT_S)
    if not select.select([channel], [], [], max(0, deadline - time.monotonic()))[0]:
        raise TimeoutError
    return os.read(channel.fileno(), count)


def _ipc_write(channel: socket.socket, data: bytes) -> None:
    deadline = time.monotonic() + (channel.gettimeout() or START_TIMEOUT_S)
    offset = 0
    while offset < len(data):
        if not select.select([], [channel], [], max(0, deadline - time.monotonic()))[1]:
            raise TimeoutError
        offset += os.write(channel.fileno(), data[offset:])


def _holder_reply(channel: socket.socket) -> dict[str, Any]:
    data = bytearray()
    deadline = time.monotonic() + (channel.gettimeout() or START_TIMEOUT_S)
    while not data.endswith(b"\n") and len(data) <= 4096:
        part = _ipc_read(channel, 1, deadline)
        if not part:
            raise OSError("lock holder died")
        data.extend(part)
    result = json.loads(data)
    if not isinstance(result, dict) or len(data) > 4096:
        raise ValueError("invalid holder reply")
    return result


def _holder_send(channel: socket.socket, record: Mapping[str, object]) -> None:
    _ipc_write(channel, json.dumps(record).encode() + b"\n")


def _holder_chain(channel: socket.socket, directories: list[str], capacity: int,
                  deadline: float, release_fd: int) -> None:
    locks: list[int] = []
    child_channel = None
    child_pid = None
    guardian_pid = None
    try:
        channel.settimeout(max(0.001, deadline - time.monotonic()))
        for directory in directories[:capacity]:
            if time.monotonic() >= deadline:
                raise TimeoutError
            fd = os.open(Path(directory) / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            locks.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A guardian inherits these SAME open file descriptions, so abrupt
        # holder death cannot drop authority between the parent's liveness check
        # and Popen. Only the queue owns the release pipe's writer. Parent death
        # or explicit post-commit release closes it; no helper can extend the lease.
        guardian_pid = os.fork()
        if guardian_pid == 0:
            keep = {*locks, release_fd}
            for entry in os.listdir("/proc/self/fd"):
                fd = int(entry)
                if fd not in keep:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            try:
                select.select([release_fd], [], [], max(0, deadline - time.monotonic()))
            finally:
                os._exit(0)
        usage = {"pids": 2, "anon_bytes": _rss_bytes() + _rss_bytes(guardian_pid)}
        if len(directories) > capacity:
            child_channel, child_end = socket.socketpair()
            child_pid = os.fork()
            if child_pid == 0:
                # Close inherited open file descriptions WITHOUT LOCK_UN: the
                # predecessor still owns them. Never inherit queue/publication IO.
                keep = child_end.fileno()
                for entry in os.listdir("/proc/self/fd"):
                    fd = int(entry)
                    if fd not in (keep, release_fd):
                        try:
                            os.close(fd)
                        except OSError:
                            pass
                try:
                    _holder_chain(child_end, directories[capacity:], capacity, deadline, release_fd)
                finally:
                    os._exit(0)
            child_end.close()
            child_channel.settimeout(max(0.001, deadline - time.monotonic()))
            reply = _holder_reply(child_channel)
            if "reason" in reply:
                _holder_send(channel, reply)
                return
            for key in usage:
                usage[key] += reply[key]
        _holder_send(channel, usage)
        while time.monotonic() < deadline:
            # Parent EOF cascades down the chain, even after abrupt parent death.
            readable, _, _ = select.select(
                [channel] + ([child_channel] if child_channel else []), [], [],
                max(0, deadline - time.monotonic()),
            )
            if not readable or child_channel in readable:
                return  # unsolicited EOF means a downstream owner died
            command = _ipc_read(channel, 1, deadline)
            if command != b"?":
                return
            if os.waitpid(guardian_pid, os.WNOHANG)[0]:
                guardian_pid = None
                return
            if child_channel:
                _ipc_write(child_channel, b"?")
                if _holder_reply(child_channel) != {"alive": True}:
                    return
            _holder_send(channel, {"alive": True})
    except BlockingIOError:
        _holder_send(channel, {"reason": "vm_queue_job_busy"})
    except TimeoutError:
        try:
            _holder_send(channel, {"reason": "vm_queue_burst_budget_exhausted"})
        except OSError:
            pass
    except (OSError, ValueError) as exc:
        reason = ("vm_queue_descriptor_exhausted:EMFILE" if getattr(exc, "errno", None) == errno.EMFILE
                  else "vm_queue_lock_holder_failed")
        try:
            _holder_send(channel, {"reason": reason})
        except OSError:
            pass
    finally:
        channel.close()  # report owner failure immediately; guardians retain authority
        if child_channel:
            child_channel.close()
        if child_pid:
            until = time.monotonic() + 0.5
            while time.monotonic() < until:
                if os.waitpid(child_pid, os.WNOHANG)[0]:
                    child_pid = None
                    break
                time.sleep(0.005)
            if child_pid:
                os.kill(child_pid, signal.SIGKILL)
                os.waitpid(child_pid, 0)
        if guardian_pid:
            # On upstream death, guardians keep protecting the transaction until
            # the queue closes release_fd. This bounded wait never unlocks them.
            until = deadline + 1
            while time.monotonic() < until:
                if os.waitpid(guardian_pid, os.WNOHANG)[0]:
                    guardian_pid = None
                    break
                time.sleep(0.005)
            if guardian_pid:
                os.kill(guardian_pid, signal.SIGKILL)
                os.waitpid(guardian_pid, 0)
        os.close(release_fd)
        for fd in locks:
            os.close(fd)
        channel.close()


def _holder_main(fd: int, release_fd: int) -> int:
    channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, fileno=fd)
    try:
        # A length-framed request avoids ARG_MAX and bounds IPC by the source deadline.
        channel.settimeout(START_TIMEOUT_S)
        deadline = time.monotonic() + START_TIMEOUT_S
        length = bytearray()
        while not length.endswith(b"\n") and len(length) < 32:
            part = _ipc_read(channel, 1, deadline)
            if not part:
                return 75
            length.extend(part)
        remaining = int(length)
        data = bytearray()
        while remaining:
            part = _ipc_read(channel, min(remaining, 65536), deadline)
            if not part:
                return 75
            data.extend(part)
            remaining -= len(part)
        request = json.loads(data)
        _holder_chain(channel, request["directories"], request["capacity"], request["deadline"], release_fd)
        return 0
    finally:
        channel.close()


@dataclass(frozen=True)
class BurstOutcome:
    started_job_ids: tuple[str, ...]
    refusal: str | None
    measure: Mapping[str, object]
    evidence: dict[str, object]
    expired_job_ids: tuple[str, ...] = ()


class LaneQueue:
    """Rebuild queue state from job directories and drain a bounded measured burst per tick."""

    def __init__(
        self,
        root: str | Path,
        *,
        gauge_reader: Any = None,
        measured_cost: Mapping[str, object] | None = None,
        clock: Any = time.time,
        max_wait_s: int | None = None,
        start_timeout_s: int = START_TIMEOUT_S,
        burst_budget_s: float | None = None,
        monotonic: Callable[[], float] | None = None,
        attribution_reader: Callable[[Path, Mapping[str, object]], Mapping[str, object] | None] | None = None,
        attribution_cgroup_root: str | Path = "/sys/fs/cgroup",
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self.jobs_root = self.root / ".jobs"
        self.gauge_reader = gauge_reader or (lambda: read_gauges(self.root))
        self.measured_cost = dict(measured_cost) if measured_cost is not None else None
        self.attribution_reader = attribution_reader
        self.attribution_cgroup_root = Path(attribution_cgroup_root).resolve()
        self.clock = clock
        configured_wait = _env_number("WORKBAY_VM_QUEUE_MAX_WAIT_S", DEFAULT_MAX_WAIT_S, integer=True)
        self.max_wait_s = (
            max_wait_s
            if max_wait_s is not None
            else int(configured_wait if configured_wait is not None else DEFAULT_MAX_WAIT_S)
        )
        self.start_timeout_s = start_timeout_s
        configured_budget = _env_number("WORKBAY_VM_QUEUE_BURST_BUDGET_S", DEFAULT_BURST_BUDGET_S)
        budget = burst_budget_s if burst_budget_s is not None else configured_budget
        budget_error = "vm_queue_burst_budget_invalid: burst budget must be finite and non-negative"
        if _finite_number(budget) is None:
            raise ValueError(budget_error)
        try:
            self.burst_budget_s = float(budget)
        except (ValueError, TypeError, OverflowError):
            raise ValueError(budget_error) from None
        self.monotonic = monotonic if monotonic is not None else time.monotonic
        self._lock_fd: int | None = None
        self._job_lock_refusal = "vm_queue_job_busy"
        self._holder_usage = {"pids": 0, "anon_bytes": 0}
        self._holder_channel = None
        self._holder_measure = {}

    @contextmanager
    def exclusive(self) -> Iterator[bool]:
        self.root.mkdir(parents=True, exist_ok=True)
        lock_path = self.root / ".queue.lock"
        if lock_path.is_symlink():
            yield False
            return
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            self._lock_fd = fd
            yield True
        finally:
            if self._lock_fd == fd:
                self._lock_fd = None
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _job_rows(self) -> list[tuple[Path, dict[str, object]]]:
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        rows: list[tuple[Path, dict[str, object]]] = []
        for job_dir in self.jobs_root.iterdir():
            if job_dir.is_symlink() or not job_dir.is_dir():
                continue
            state_path = job_dir / "state.json"
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if isinstance(state, dict):
                rows.append((job_dir, state))
        return sorted(rows, key=lambda row: row[0].name)

    def _queued_at(self, job_dir: Path, state: Mapping[str, object]) -> float | None:
        queued_at = _parse_epoch(state.get("queued_at"))
        if queued_at is not None:
            return queued_at
        try:
            return (job_dir / "state.json").stat().st_mtime
        except OSError:
            return None

    def _queued_order(self, row: tuple[Path, dict[str, object]]) -> tuple[float, str]:
        queued_at = self._queued_at(row[0], row[1])
        return (queued_at if queued_at is not None else float("inf"), row[0].name)

    def _write_done(self, job_dir: Path, rc: int, reason: str, **details: object) -> None:
        if (job_dir / "done.json").exists() or (job_dir / "done.json").is_symlink():
            return
        # Callers retain publication -> job locks through this read and the
        # atomic receipt publication. Missing stdout collects as an empty stream;
        # unsafe or unreadable output must never be certified as empty.
        digest = hashlib.sha256()
        stdout_bytes = 0
        try:
            fd = os.open(job_dir / "stdout", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            pass
        except OSError:
            return
        else:
            try:
                with os.fdopen(fd, "rb") as stdout:
                    if not stat.S_ISREG(os.fstat(stdout.fileno()).st_mode):
                        return
                    while chunk := stdout.read(1024 * 1024):
                        digest.update(chunk)
                        stdout_bytes += len(chunk)
            except OSError:
                return
        record: dict[str, object] = {
            "finished_at": _iso_time(self.clock()),
            "job_id": job_dir.name,
            "rc": rc,
            "reason": reason,
            "schema_version": 1,
        }
        record.update(details)
        record.update(stdout_sha256=digest.hexdigest(), stdout_bytes=stdout_bytes)
        _atomic_json(job_dir / "done.json", record)

    @contextmanager
    def _path_exclusive(self, lock_path: Path, busy_reason: str) -> Iterator[bool]:
        self._job_lock_refusal = busy_reason
        try:
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        except OSError as exc:
            # Exhausting this process's descriptor table is not a competing
            # flock holder. Preserve the reservation and report the exact cause.
            if exc.errno == errno.EMFILE:
                self._job_lock_refusal = "vm_queue_descriptor_exhausted:EMFILE"
            yield False
            return
        acquired = False
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            acquired = True
            yield True
        finally:
            if acquired:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def _job_exclusive(self, job_dir: Path):
        return self._path_exclusive(job_dir / ".lock", "vm_queue_job_busy")

    @contextmanager
    def _pending_exclusive(self, job_dir: Path, directories: list[Path],
                           launch_deadline: float | None, identity: ExitStack) -> Iterator[bool]:
        self._holder_usage = {"pids": 0, "anon_bytes": 0}
        self._holder_channel = None
        process = None
        channel = None
        release_write = None
        delivered = False
        with ExitStack() as locks:
            try:
                headroom = _fd_headroom()
                if len(directories) <= headroom - LOCK_IO_FDS:
                    for directory in directories:
                        if not (identity if directory == job_dir else locks).enter_context(self._job_exclusive(directory)):
                            yield False
                            return
                        if launch_deadline is not None and self.monotonic() >= launch_deadline:
                            self._job_lock_refusal = "vm_queue_burst_budget_exhausted"
                            yield False
                            return
                    delivered = True
                    yield True
                    return
                # All acquisitions are nonblocking. The candidate's identity stays
                # in the parent; holders take the remaining paths in sorted order.
                if not identity.enter_context(self._job_exclusive(job_dir)):
                    yield False
                    return
                if headroom < LOCK_IO_FDS + 3:
                    self._job_lock_refusal = "vm_queue_descriptor_exhausted:EMFILE"
                    yield False
                    return
                limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
                if limit == resource.RLIM_INFINITY:
                    limit = os.sysconf("SC_OPEN_MAX")
                capacity = int(limit) - 4 - LOCK_IO_FDS
                pending = [str(path) for path in directories if path != job_dir]
                count = 2 * math.ceil(len(pending) / capacity)
                estimate = {"pids": count, "anon_bytes": count * _rss_bytes(), "disk_bytes": 0}
                decision = admit(self._holder_measure, pending=(), measured_cost=estimate)
                if not decision.admitted:
                    self._job_lock_refusal = decision.reason
                    yield False
                    return
                remaining = (self.start_timeout_s if launch_deadline is None
                             else max(0, launch_deadline - self.monotonic()))
                if remaining <= 0:
                    self._job_lock_refusal = "vm_queue_burst_budget_exhausted"
                    yield False
                    return
                ready_deadline = time.monotonic() + remaining
                channel, child_end = socket.socketpair()
                release_read, release_write = os.pipe()
                try:
                    process = subprocess.Popen(
                        [sys.executable, str(Path(__file__).resolve()), "--lock-holder-fd", str(child_end.fileno()), str(release_read)],
                        pass_fds=(child_end.fileno(), release_read), stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
                    )
                finally:
                    child_end.close()
                    os.close(release_read)
                channel.settimeout(max(0.001, ready_deadline - time.monotonic()))
                request = json.dumps({"directories": pending, "capacity": capacity,
                                      "deadline": ready_deadline + self.start_timeout_s + 1}).encode()
                _ipc_write(channel, str(len(request)).encode() + b"\n" + request)
                channel.settimeout(max(0.001, ready_deadline - time.monotonic()))
                reply = _holder_reply(channel)
                if "reason" in reply:
                    self._job_lock_refusal = reply["reason"]
                    yield False
                    return
                if reply.get("pids") != count or _finite_number(reply.get("anon_bytes"), integer=True) is None:
                    raise ValueError("unmeasured lock holders")
                self._holder_usage = reply
                self._holder_channel = channel
                delivered = True
                yield True
            except TimeoutError:
                if delivered:
                    raise
                self._job_lock_refusal = "vm_queue_burst_budget_exhausted"
                yield False
            except (OSError, ValueError) as exc:
                if delivered:
                    raise
                self._job_lock_refusal = ("vm_queue_descriptor_exhausted:EMFILE"
                                          if getattr(exc, "errno", None) == errno.EMFILE
                                          else "vm_queue_lock_holder_failed")
                yield False
            finally:
                self._holder_channel = None
                if release_write is not None:
                    os.close(release_write)
                if channel:
                    channel.close()
                if process:
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=1)
                    # A killed upstream holder may already have exited while
                    # downstream processes still own locks. Sweep its private group.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                self._holder_usage = {"pids": 0, "anon_bytes": 0}

    def _holders_alive(self, launch_deadline: float | None) -> bool:
        if self._holder_channel is None:
            return True
        remaining = (self.start_timeout_s if launch_deadline is None
                     else max(0, launch_deadline - self.monotonic()))
        if remaining <= 0:
            self._job_lock_refusal = "vm_queue_burst_budget_exhausted"
            return False
        try:
            self._holder_channel.settimeout(remaining)
            _ipc_write(self._holder_channel, b"?")
            if _holder_reply(self._holder_channel) == {"alive": True}:
                return True
        except (OSError, ValueError):
            pass
        self._job_lock_refusal = "vm_queue_lock_holder_failed"
        return False

    def _publication_exclusive(self):
        # Native order: publication -> job -> admission. The queue first owns
        # .queue.lock (publishers never acquire it); all its other acquisitions
        # are nonblocking. Submit/retry and stamp/cancel take publication before
        # creating or modifying queued payload, eligibility, or state. Queue
        # reconciliation/expiry use the same fence. Completion of launched jobs
        # does not create queued reservations and remains outside this protocol.
        return self._path_exclusive(self.root / ".queue-publication.lock",
                                    "vm_queue_publication_busy")

    def _load_job_state(self, job_dir: Path) -> dict[str, object] | None:
        try:
            latest = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        return latest if isinstance(latest, dict) else None

    def _expire(self, rows: list[tuple[Path, dict[str, object]]], now: float) -> tuple[str, ...]:
        expired: list[str] = []
        for job_dir, _ in rows:
            with self._job_exclusive(job_dir) as acquired:
                if not acquired:
                    continue
                state = self._load_job_state(job_dir)
                if state is None or state.get("state") != "queued" or (job_dir / "done.json").exists():
                    continue
                queued_at = self._queued_at(job_dir, state)
                if queued_at is None:
                    continue
                age = max(0.0, now - queued_at)
                if age > self.max_wait_s:
                    self._write_done(job_dir, 75, "vm_queue_wait_expired")
                    expired.append(job_dir.name)
        return tuple(expired)

    def _cost_for_job(self, job_dir: Path) -> Mapping[str, object]:
        if self.measured_cost is not None:
            return self.measured_cost
        return _default_measured_cost(job_dir)

    def _record_reconcile_reason(self, job_dir: Path, state: dict[str, object], reason: str) -> None:
        state["reason"] = reason
        _atomic_json(job_dir / "state.json", state)

    def _native_resource_attribution(self, job_dir: Path, request: Mapping[str, object]) -> Mapping[str, object] | None:
        """Read the accepted service's cgroup; never infer ownership from aggregate deltas.

        The unit must identify this job, have started after this launch boundary,
        and live beneath the very cgroup used by read_gauges. Disk has no native
        before/after scope probe here, so it remains reserved. No filesystem walk.
        """
        if request["phase"] != "sample":
            return None
        unit = request["unit"]
        job_id = job_dir.name
        if not isinstance(unit, str) or not re.fullmatch(r"[A-Za-z0-9_.@:-]+\.service", unit):
            return None
        if re.search(r"(?<![A-Za-z0-9])" + re.escape(job_id) + r"(?![A-Za-z0-9])", unit) is None:
            return None

        def properties() -> dict[str, str] | None:
            remaining = (request["deadline_ns"] - time.monotonic_ns()) / 1e9
            if remaining <= 0:
                return None
            try:
                result = subprocess.run(
                    ["systemctl", "--user", "show", "-p",
                     "Id,LoadState,ActiveState,ControlGroup,InvocationID,ExecMainStartTimestampMonotonic", "--", unit],
                    check=False, timeout=min(0.2, remaining), stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL, text=True,
                )
            except (OSError, subprocess.TimeoutExpired):
                return None
            if result.returncode != 0 or len(result.stdout) > ATTRIBUTION_FILE_LIMIT:
                return None
            return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

        props = properties()
        if props is None or props.get("Id") != unit or props.get("LoadState") != "loaded" or props.get("ActiveState") != "active":
            return None
        invocation = props.get("InvocationID", "")
        started = props.get("ExecMainStartTimestampMonotonic", "")
        if re.fullmatch(r"[0-9a-f]{32}", invocation) is None or re.fullmatch(r"[0-9]+", started) is None:
            return None
        if int(started) * 1000 < request["launch_not_before_ns"]:
            return None
        relative = props.get("ControlGroup", "")
        if not relative.startswith("/") or ".." in Path(relative).parts:
            return None
        scope = self.attribution_cgroup_root / relative.lstrip("/")
        aggregate = Path(os.environ.get(
            "WORKBAY_REMOTE_GATE_PIDS_ROOT",
            f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice",
        )).resolve()
        try:
            if scope.resolve() != scope or not scope.is_relative_to(aggregate) or scope == aggregate or scope.name != unit:
                return None
            before = scope.stat()
            raw_pids = _attribution_text(scope / "pids.current")
            raw_memory = _attribution_text(scope / "memory.stat")
            after = scope.stat()
        except OSError:
            return None
        # Recheck the invocation around the files: a same-name restart is foreign
        # evidence, even when systemd reuses the cgroup directory.
        final = properties()
        if final != props or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            return None
        pids = int(raw_pids) if raw_pids is not None and re.fullmatch(r"[0-9]+", raw_pids) else None
        anon = None
        if raw_memory is not None:
            rows = [line.split() for line in raw_memory.splitlines() if line.split()[:1] == ["anon"]]
            if len(rows) == 1 and len(rows[0]) == 2 and re.fullmatch(r"[0-9]+", rows[0][1]):
                anon = int(rows[0][1])
        return {
            **{key: request[key] for key in ("job_id", "unit", "launch_not_before_ns")},
            "observed_at_ns": time.monotonic_ns(), "source": "native_cgroup",
            "cgroup": str(scope), "invocation_id": invocation, "pids": pids, "anon_bytes": anon,
        }

    def _read_attribution(self, job_dir: Path, request: Mapping[str, object]) -> Mapping[str, object] | None:
        try:
            reader = self.attribution_reader or self._native_resource_attribution
            return reader(job_dir, request)
        except Exception:
            return None

    def _unit_properties(self, unit: str) -> dict[str, str] | None:
        try:
            result = subprocess.run(
                [
                    "timeout",
                    "10",
                    "systemctl",
                    "--user",
                    "show",
                    "-p",
                    "LoadState,ActiveState,SubState,Result,ExecMainStatus",
                    unit,
                ],
                check=False,
                timeout=11,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode != 0:
            return None
        properties: dict[str, str] = {}
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                properties[key] = value
        if "LoadState" not in properties:
            return None
        return properties

    def _reconcile_launching(self, rows: list[tuple[Path, dict[str, object]]]) -> str | None:
        refusal: str | None = None
        for job_dir, _ in rows:
            with self._job_exclusive(job_dir) as acquired:
                if not acquired:
                    continue
                state = self._load_job_state(job_dir)
                if state is None or state.get("state") != "launching" or (job_dir / "done.json").exists():
                    continue
                unit = state.get("unit")
                if not isinstance(unit, str) or not unit or "\x00" in unit:
                    reason = "vm_queue_unit_unknown"
                    self._record_reconcile_reason(job_dir, state, reason)
                    refusal = refusal or reason
                    continue
                properties = self._unit_properties(unit)
                if properties is None:
                    reason = "vm_queue_reconcile_unavailable"
                    self._record_reconcile_reason(job_dir, state, reason)
                    refusal = refusal or reason
                    continue
                load_state = properties["LoadState"]
                active_state = properties.get("ActiveState", "")
                if active_state in {"active", "activating", "reloading"}:
                    state["state"] = "submitted"
                    state["submitted_at"] = _iso_time(self.clock())
                    state.pop("launching_at", None)
                    state.pop("reason", None)
                    _atomic_json(job_dir / "state.json", state)
                elif load_state == "not-found":
                    state["state"] = "queued"
                    state.pop("launching_at", None)
                    state.pop("submitted_at", None)
                    state.pop("reason", None)
                    _atomic_json(job_dir / "state.json", state)
                elif load_state == "loaded" and active_state in {"inactive", "failed"}:
                    status_text = properties.get("ExecMainStatus", "")
                    status_value: int | str = (
                        int(status_text) if re.fullmatch(r"-?[0-9]+", status_text) is not None else status_text
                    )
                    rc = status_value if isinstance(status_value, int) and 0 < status_value <= 255 else 75
                    self._write_done(
                        job_dir,
                        rc,
                        "vm_queue_unit_exited_without_record",
                        result=properties.get("Result", ""),
                        exec_main_status=status_value,
                    )
                else:
                    reason = "vm_queue_reconcile_unavailable"
                    self._record_reconcile_reason(job_dir, state, reason)
                    refusal = refusal or reason
        return refusal

    def _current_queued_eligibility(self, job_dir: Path) -> tuple[dict[str, object] | None, str | None]:
        """Reload eligibility and publish expiry while the caller holds this job's lock."""
        latest = self._load_job_state(job_dir)
        if latest is None:
            return None, "vm_queue_job_state_unreadable"
        if latest.get("state") != "queued" or (job_dir / "done.json").exists():
            return None, None
        queued_at = self._queued_at(job_dir, latest)
        if queued_at is None:
            return None, "vm_queue_wait_unmeasured"
        if max(0.0, self.clock() - queued_at) > self.max_wait_s:
            self._write_done(job_dir, 75, "vm_queue_wait_expired")
            if hasattr(self, "_burst_expired"):
                self._burst_expired.append(job_dir.name)
            return None, None
        return latest, None

    def _eligible_reservation_row(self, job_dir: Path) -> bool:
        """Drop terminal/expired pending rows; retain uncertain or busy reservations."""
        with self._job_exclusive(job_dir) as acquired:
            if not acquired:
                return True
            latest, reason = self._current_queued_eligibility(job_dir)
            return latest is not None or reason is not None

    def _start(
        self,
        job_dir: Path,
        state: dict[str, object],
        *,
        launch_deadline: float | None = None,
        reservation_dirs: tuple[Path, ...] = (),
        prepare_admission: Callable[[dict[str, object]], str | None] | None = None,
    ) -> tuple[bool, str | None]:
        start_script = job_dir / "start.sh"
        if start_script.is_symlink() or not start_script.is_file():
            return False, "vm_queue_start_missing"
        with ExitStack() as publication, ExitStack() as candidate_lock, ExitStack() as pending_locks:
            if not publication.enter_context(self._publication_exclusive()):
                return False, self._job_lock_refusal
            publication_released = False

            def reacquire_publication() -> bool:
                nonlocal publication_released
                if publication_released:
                    # The child may have admitted a publisher while this job's
                    # identity stays locked. Never wait in this reversed order:
                    # preserve launching for fenced reconciliation on contention.
                    if not publication.enter_context(self._publication_exclusive()):
                        return False
                    publication_released = False
                return True

            # Snapshot native additions under publication, before any cost read.
            directories = {job_dir, *reservation_dirs}
            if prepare_admission is not None:
                directories.update(path for path, current in self._job_rows()
                                   if current.get("state") == "queued")
            if not pending_locks.enter_context(
                self._pending_exclusive(job_dir, sorted(directories), launch_deadline, candidate_lock)
            ):
                return False, self._job_lock_refusal
            if prepare_admission is not None:
                latest, reason = self._current_queued_eligibility(job_dir)
                if latest is None:
                    return False, reason
                reason = prepare_admission(latest)
                if reason is not None:
                    return False, reason
            latest, reason = self._current_queued_eligibility(job_dir)
            if latest is None:
                return False, reason
            # Lock acquisition and the fresh eligibility reload consume the shared burst budget.
            if launch_deadline is not None and self.monotonic() >= launch_deadline:
                return False, "vm_queue_burst_budget_exhausted"
            unit = latest.get("unit")
            if not isinstance(unit, str) or not unit or "\x00" in unit:
                latest["reason"] = "vm_queue_unit_unknown"
                _atomic_json(job_dir / "state.json", latest)
                return False, "vm_queue_unit_unknown"
            queued_state = dict(latest)
            latest["state"] = "launching"
            latest["launching_at"] = _iso_time(self.clock())
            latest.pop("submitted_at", None)
            latest.pop("reason", None)
            _atomic_json(job_dir / "state.json", latest)
            # Durable intent preparation can also exhaust the budget. No script has run yet;
            # restore the fresh queued record under the same identity lock before refusing.
            if launch_deadline is not None and self.monotonic() >= launch_deadline:
                _atomic_json(job_dir / "state.json", queued_state)
                return False, "vm_queue_burst_budget_exhausted"
            try:
                if prepare_admission is None:
                    result = subprocess.run(
                        ["/bin/bash", str(start_script)], cwd=job_dir, check=False,
                        timeout=self.start_timeout_s, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, text=True,
                    )
                else:
                    # Spawn is the admission commit point. Gate the child until
                    # publication is released, so its script can publish successor
                    # changes after the admission commit. Only the gate read
                    # descriptor is inherited. Pending locks release after Popen
                    # commits creation; candidate identity remains in the parent.
                    # Parent death closes the gate: the child sees EOF and exits
                    # without running the launcher. SIG/fork cannot leak leases.
                    gate_read, gate_write = os.pipe()
                    try:
                        if launch_deadline is not None and self.monotonic() >= launch_deadline:
                            _atomic_json(job_dir / "state.json", queued_state)
                            return False, "vm_queue_burst_budget_exhausted"
                        if not self._holders_alive(launch_deadline):
                            _atomic_json(job_dir / "state.json", queued_state)
                            return False, self._job_lock_refusal
                        # A successful liveness reply can consume the remaining budget.
                        if launch_deadline is not None and self.monotonic() >= launch_deadline:
                            _atomic_json(job_dir / "state.json", queued_state)
                            return False, "vm_queue_burst_budget_exhausted"
                        with subprocess.Popen(
                            ["/bin/bash", "-c",
                             'launch_gate=$1; shift; '
                             'IFS= read -r -n 1 -u "$launch_gate" || exit 75; '
                             'exec {launch_gate}<&-; exec /bin/bash "$@"',
                             "lane-queue-launch", str(gate_read), str(start_script)],
                            cwd=job_dir, pass_fds=(gate_read,),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        ) as process:
                            try:
                                pending_locks.close()
                                publication.close()
                                publication_released = True
                                os.write(gate_write, b"1")
                                stdout, stderr = process.communicate(timeout=self.start_timeout_s)
                            except BaseException:
                                process.kill()
                                process.communicate(timeout=self.start_timeout_s)
                                raise
                            result = subprocess.CompletedProcess(
                                process.args, process.returncode, stdout, stderr,
                            )
                    finally:
                        os.close(gate_read)
                        os.close(gate_write)
            except subprocess.TimeoutExpired:
                if not reacquire_publication():
                    return False, self._job_lock_refusal
                current = self._load_job_state(job_dir)
                if current is not None and current.get("state") == "launching":
                    current["reason"] = "vm_queue_start_timeout"
                    _atomic_json(job_dir / "state.json", current)
                return False, "vm_queue_start_timeout"
            except OSError:
                if not reacquire_publication():
                    return False, self._job_lock_refusal
                current = self._load_job_state(job_dir)
                if current is not None and current.get("state") == "launching":
                    current["state"] = "queued"
                    current.pop("launching_at", None)
                    current["reason"] = "vm_queue_start_failed"
                    _atomic_json(job_dir / "state.json", current)
                return False, "vm_queue_start_failed"
            if result.returncode != 0:
                if not reacquire_publication():
                    return False, self._job_lock_refusal
                current = self._load_job_state(job_dir)
                if current is not None and current.get("state") == "launching":
                    current["state"] = "queued"
                    current.pop("launching_at", None)
                    current["reason"] = f"vm_queue_start_failed:{result.returncode}"
                    _atomic_json(job_dir / "state.json", current)
                return False, f"vm_queue_start_failed:{result.returncode}"
            if not reacquire_publication():
                # This accepted launch is durable; reconciliation will settle
                # its intent after the publisher releases the fence.
                return True, None
            current = self._load_job_state(job_dir)
            if current is not None and current.get("state") == "launching":
                current["state"] = "submitted"
                current["submitted_at"] = _iso_time(self.clock())
                current.pop("launching_at", None)
                current.pop("reason", None)
                _atomic_json(job_dir / "state.json", current)
            return True, None

    def _drain(
        self,
        queued_rows: list[tuple[Path, dict[str, object]]],
        baseline: Mapping[str, object],
    ) -> BurstOutcome:
        """Start FIFO snapshot rows while fresh gauges plus unobserved burst launches still admit them.

        Each snapshot row is visited at most once. The first row is the former single admission;
        every later row first honours the stop file and the declared stage budget, then is judged
        against a fresh reading charged with the accepted launches it does not show yet. Any typed
        refusal or failed start ends the burst; rows that are no longer queued are skipped unlaunched.
        """
        began = self.monotonic()
        deadline = began + self.burst_budget_s
        started: list[str] = []
        self._burst_expired: list[str] = []
        snapshot_size = len(queued_rows)
        reserved = {key: 0 for key, _, _ in BURST_RESERVED_GAUGES}
        unobserved = dict(reserved)
        costs: dict[str, Mapping[str, object]] = {}
        accepted: list[dict[str, Any]] = []
        attribution: dict[str, dict[str, object]] = {}
        measure = baseline
        decision_measure: Mapping[str, object] = baseline
        remeasure = False
        refusal: str | None = None

        class CostProbeDeadline(Exception):
            """Stop the current reservation scan without probing more directories."""

        def cost(job_dir: Path) -> Mapping[str, object]:
            if job_dir.name not in costs:
                budgeted = index > 0 or self.burst_budget_s > 0
                if budgeted and self.monotonic() >= deadline:
                    raise CostProbeDeadline
                costs[job_dir.name] = self._cost_for_job(job_dir)
                if budgeted and self.monotonic() >= deadline:
                    raise CostProbeDeadline
            return costs[job_dir.name]

        for index, (job_dir, state) in enumerate(queued_rows):
            if index > 0:
                if (self.root / STOP_FILE_NAME).exists():
                    refusal = "vm_queue_stop_requested"
                    break
                if self.monotonic() >= deadline:
                    refusal = "vm_queue_burst_budget_exhausted"
                    break
            if remeasure:
                remeasure = False
                # Sample job scopes before the enclosing aggregate. A later leaf
                # observation cannot be credited against an earlier aggregate.
                sample_began = time.monotonic_ns()
                sample_deadline = sample_began + ATTRIBUTION_BUDGET_NS
                samples: list[object] = []
                for reservation in accepted:
                    samples.append(
                        self._read_attribution(reservation["job_dir"], {
                            **reservation["identity"], "phase": "sample", "deadline_ns": sample_deadline,
                        }) if time.monotonic_ns() < sample_deadline else None
                    )
                sample_ended = time.monotonic_ns()
                try:
                    measure = self.gauge_reader()
                except Exception as exc:
                    measure = {}
                    refusal = f"vm_admission_unmeasured:vm_gauges:{type(exc).__name__}"
                    break
                unobserved = unobserved_reservations(baseline, measure, reserved)
                credits = _consistent_job_credits(accepted, samples, measure, sample_began, sample_ended)
                for reservation, (credit, record) in zip(accepted, credits):
                    for key in unobserved:
                        unobserved[key] -= credit[key]
                    attribution[reservation["identity"]["job_id"]] = record
                charged = dict(measure)
                for key, gauge, direction in BURST_RESERVED_GAUGES:
                    current = _gauge_number(measure, gauge, integer=True)
                    if direction > 0 and current is not None:
                        charged[gauge] = current + unobserved[key]
                decision_measure = charged
            with self._job_exclusive(job_dir) as acquired:
                if not acquired:
                    refusal = self._job_lock_refusal
                    break
                state, refusal = self._current_queued_eligibility(job_dir)
            if refusal is not None:
                break
            if state is None:
                continue
            candidate_cost: Mapping[str, object] = {}
            identity: dict[str, object] = {}
            disk_before = None

            def prepare(current_state: dict[str, object]) -> str | None:
                nonlocal candidate_cost, identity, disk_before
                # Prune only the unvisited tail so enumeration and FIFO retain their order.
                # The pending-cost expression sees these same live rows without changing its accounting.
                def eligible(directory: Path) -> bool:
                    if self.measured_cost is not None:
                        return self._eligible_reservation_row(directory)
                    latest, reason = self._current_queued_eligibility(directory)
                    return latest is not None or reason is not None

                queued_rows[index + 1 :] = [
                    row for row in queued_rows[index + 1 :] if eligible(row[0])
                ]
                # Real directories may grow during an earlier launch. Cache only within this
                # admission's eligible reservation scan; explicit static costs stay compatible.
                if self.measured_cost is None:
                    costs.clear()
                try:
                    candidate_cost = cost(job_dir)
                    # Native submissions can arrive between admissions. Under
                    # publication, charge every current queued directory, even
                    # rows absent from this tick's fixed FIFO launch snapshot.
                    pending_dirs = [row[0] for row in queued_rows[index + 1 :]]
                    if self.measured_cost is None:
                        known_dirs = {job_dir, *pending_dirs}
                        for other_dir, other_state in self._job_rows():
                            if other_dir in known_dirs or other_state.get("state") != "queued":
                                continue
                            # The complete current inventory was locked by _start.
                            latest, reason = self._current_queued_eligibility(other_dir)
                            if reason is not None:
                                return reason
                            if latest is not None:
                                pending_dirs.append(other_dir)
                    # Unobserved disk of accepted launches joins queued reservations.
                    pending_costs = [{"disk_bytes": unobserved["disk_bytes"]}] + [
                        {"disk_bytes": cost(other_dir).get("disk_bytes")} for other_dir in pending_dirs
                    ]
                except CostProbeDeadline:
                    return "vm_queue_burst_budget_exhausted"
                charged = dict(decision_measure)
                for cost_key, gauge in (("pids", "pids_current"), ("anon_bytes", "memory_anon_bytes")):
                    current = _gauge_number(charged, gauge, integer=True)
                    if current is not None:
                        charged[gauge] = current + self._holder_usage[cost_key]
                decision = admit(charged, pending=pending_costs, measured_cost=candidate_cost)
                if not decision.admitted:
                    return decision.reason
                # Only a measured exact-scope baseline taken before this launch can
                # establish new disk usage. Native evidence deliberately has none.
                launch_not_before_ns = time.monotonic_ns()
                identity = {"job_id": job_dir.name, "unit": current_state.get("unit"),
                            "launch_not_before_ns": launch_not_before_ns}
                disk_baseline = self._read_attribution(job_dir, {
                    **identity, "phase": "before_launch",
                    "deadline_ns": launch_not_before_ns + ATTRIBUTION_BUDGET_NS,
                })
                disk_before = (
                    _disk_sample(disk_baseline, job_dir)
                    if _sample_identity(disk_baseline, identity, launch_not_before_ns, time.monotonic_ns()) else None
                )
                # Preparation (including fresh gauges and the initial cost scan) uses the same budget.
                if index > 0 and (self.root / STOP_FILE_NAME).exists():
                    return "vm_queue_stop_requested"
                # Preserve the former first admission only when its declared budget is zero.
                if (index > 0 or self.burst_budget_s > 0) and self.monotonic() >= deadline:
                    return "vm_queue_burst_budget_exhausted"
                return None

            launch_deadline = None if index == 0 and self.burst_budget_s == 0 else deadline
            if self.measured_cost is not None:
                refusal = prepare(state)
                if refusal is not None:
                    break
                launched, refusal = self._start(job_dir, state, launch_deadline=launch_deadline)
            else:
                self._holder_measure = decision_measure
                launched, refusal = self._start(
                    job_dir, state, launch_deadline=launch_deadline,
                    reservation_dirs=tuple(row[0] for row in queued_rows[index + 1 :]),
                    prepare_admission=prepare,
                )
            if refusal is not None:
                break
            if launched:
                started.append(job_dir.name)
                remeasure = True
                latest = self._load_job_state(job_dir)
                accepted.append({
                    "job_dir": job_dir, "identity": identity, "disk_before": disk_before,
                    "identity_valid": latest is not None and latest.get("unit") == identity["unit"],
                    "cost": {key: int(_finite_number(candidate_cost.get(key), integer=True) or 0) for key in reserved},
                })
                attribution[job_dir.name] = {**identity, "status": "unmeasured_after_launch",
                                            "credit": {key: 0 for key in reserved}}
                for key in reserved:
                    accepted_cost = int(_finite_number(candidate_cost.get(key), integer=True) or 0)
                    reserved[key] += accepted_cost
                    # The last actual reading predates this accepted launch.
                    unobserved[key] += accepted_cost
        evidence: dict[str, object] = {
            "snapshot": len(queued_rows),
            "started": len(started),
            "stop_reason": refusal,
            "budget_s": self.burst_budget_s,
            "elapsed_s": round(max(0.0, self.monotonic() - began), 3),
            "reserved": reserved,
            "unobserved": unobserved,
            "attribution": attribution,
        }
        return BurstOutcome(
            tuple(started), refusal, measure, {**evidence, "snapshot": snapshot_size}, tuple(self._burst_expired)
        )

    def _write_heartbeat(
        self,
        gauges: Mapping[str, object],
        reason: str | None,
        now: float,
        started_job_ids: tuple[str, ...] = (),
        burst: Mapping[str, object] | None = None,
    ) -> tuple[int, int]:
        queued = 0
        running = 0
        for job_dir, state in self._job_rows():
            if (job_dir / "done.json").exists():
                continue
            if state.get("state") == "queued":
                queued += 1
            elif state.get("state") in {"launching", "submitted", "running"}:
                running += 1
        _atomic_json(
            self.root / ".queue-state.json",
            {
                "at": _iso_time(now),
                "queued": queued,
                "running": running,
                "last_refusal_reason": reason,
                "measure": dict(gauges),
                "started_job_ids": list(started_job_ids),
                "burst": dict(burst) if burst is not None else None,
            },
        )
        return queued, running

    def _tick_once(self, gauges: Mapping[str, object] | None = None) -> TickResult:
        now = float(self.clock())
        with self._publication_exclusive() as acquired:
            if not acquired:
                rows = self._job_rows()
                queued = sum(state.get("state") == "queued" and not (job / "done.json").exists()
                             for job, state in rows)
                return TickResult(None, (), self._job_lock_refusal, queued, 0)
            rows = self._job_rows()
            reconcile_refusal = self._reconcile_launching(rows)
            rows = self._job_rows()
            expired = self._expire(rows, now)
            rows = self._job_rows()
        queued_rows = [
            (job_dir, state)
            for job_dir, state in rows
            if state.get("state") == "queued" and not (job_dir / "done.json").exists()
        ]
        queued_rows.sort(key=self._queued_order)
        measure: Mapping[str, object]
        if gauges is None:
            try:
                measure = self.gauge_reader()
            except Exception as exc:  # keep the heartbeat alive while refusing admission
                measure = {}
                refusal = f"vm_admission_unmeasured:vm_gauges:{type(exc).__name__}"
            else:
                refusal = reconcile_refusal
        else:
            measure = gauges
            refusal = reconcile_refusal

        started_job_ids: tuple[str, ...] = ()
        burst: dict[str, object] | None = None
        if queued_rows and refusal is None:
            outcome = self._drain(queued_rows, measure)
            started_job_ids, refusal, measure, burst = (
                outcome.started_job_ids,
                outcome.refusal,
                outcome.measure,
                outcome.evidence,
            )
            expired += outcome.expired_job_ids

        queued, running = self._write_heartbeat(measure, refusal, now, started_job_ids, burst)
        started_job_id = started_job_ids[0] if started_job_ids else None
        return TickResult(started_job_id, expired, refusal, queued, running, started_job_ids=started_job_ids)

    def tick(self, gauges: Mapping[str, object] | None = None) -> TickResult:
        if self._lock_fd is not None:
            return self._tick_once(gauges)
        with self.exclusive() as acquired:
            if not acquired:
                return TickResult(None, (), "vm_queue_already_running", 0, 0, lock_acquired=False)
            return self._tick_once(gauges)

    def run(
        self,
        tick_s: float = DEFAULT_TICK_S,
        idle_ttl_seconds: float | None = None,
    ) -> ExitDecision | None:
        lifetime_source = "vendored"

        if idle_ttl_seconds is None:
            configured_ttl = _env_number("LANE_QUEUE_IDLE_TTL_SECONDS", DEFAULT_IDLE_TTL_S)
            if configured_ttl is None:
                print("lane_queue: LANE_QUEUE_IDLE_TTL_SECONDS must be finite and non-negative", file=sys.stderr)
                return None
            idle_ttl_seconds = float(configured_ttl)
        try:
            lifetime = DriverLifetime(idle_ttl_seconds, clock=time.monotonic)
        except ValueError as exc:
            print(f"lane_queue: {exc}", file=sys.stderr)
            return None
        stop_path = self.root / STOP_FILE_NAME
        receipt_path = self.root / EXIT_RECEIPT_NAME
        state_path = self.root / ".queue-state.json"
        with self.exclusive() as acquired:
            if not acquired:
                print("lane_queue: another instance holds .queue.lock", file=sys.stderr)
                return None
            while True:
                decision = lifetime.should_exit(stop_requested=stop_path.exists())
                if decision.kind != "continue":
                    write_exit_receipt(
                        receipt_path, decision, state_path=str(state_path), lifetime_source=lifetime_source
                    )
                    return decision
                result = self._tick_once()
                if result.last_refusal_reason:
                    print(f"lane_queue: admission deferred: {result.last_refusal_reason}", file=sys.stderr)
                lifetime.observe(work_pending=result.queued > 0 or result.running > 0)
                decision = lifetime.should_exit(stop_requested=stop_path.exists())
                if decision.kind != "continue":
                    write_exit_receipt(
                        receipt_path, decision, state_path=str(state_path), lifetime_source=lifetime_source
                    )
                    return decision
                time.sleep(tick_s)


def main(argv: list[str] | None = None) -> int:
    if argv is None and len(sys.argv) == 4 and sys.argv[1] == "--lock-holder-fd":
        return _holder_main(int(sys.argv[2]), int(sys.argv[3]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", help="remote agent root (defaults to WORKBAY_REMOTE_AGENT_ROOT)")
    parser.add_argument("--once", action="store_true", help="run one measured admission tick and exit")
    args = parser.parse_args(argv)
    worker = LaneQueue(resolve_root(args.root))
    if args.once:
        result = worker.tick()
        if not result.lock_acquired:
            print("lane_queue: another instance holds .queue.lock", file=sys.stderr)
        return 0
    tick_s = _env_number("WORKBAY_VM_QUEUE_TICK_S", DEFAULT_TICK_S)
    if tick_s is None or tick_s <= 0:
        print("lane_queue: WORKBAY_VM_QUEUE_TICK_S must be positive", file=sys.stderr)
        return 2
    idle_ttl_s = _env_number("LANE_QUEUE_IDLE_TTL_SECONDS", DEFAULT_IDLE_TTL_S)
    if idle_ttl_s is None:
        print("lane_queue: LANE_QUEUE_IDLE_TTL_SECONDS must be finite and non-negative", file=sys.stderr)
        return 2
    decision = worker.run(float(tick_s), idle_ttl_seconds=float(idle_ttl_s))
    if decision is not None:
        print((worker.root / EXIT_RECEIPT_NAME).read_text(encoding="utf-8"), end="")
        return 0
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
