#!/usr/bin/env python3
"""Measure host process count and memory use for an in-flight lane wave.

This records the host-side process and resident-memory/phys_footprint cost while remote lanes run. A process is a seed
when its full command contains the task ref or any ``--match`` needle; all of its unblocked descendants are counted
too, even when their command contains neither. Shared daemon pids are excluded from the per-lane process count and
their RSS is apportioned across the wave. Every per-lane quantity divides by the wave width supplied by the operator.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "workbay.lane_host_cost.v1"
WAVE_LANES = 5
BARS = {
    "resident_host_processes_per_lane": 0,
    "host_rss_mb_per_inflight_lane_max": 25,
    "time_to_vm_start_p50_s_max": 30,
}
PS_ARGV = ("ps", "-A", "-ww", "-o", "pid=,ppid=,rss=,command=")
PS_TIMEOUT_S = 30
MIN_INTERVAL_S = 1
MAX_DURATION_S = 21600


class PsError(RuntimeError):
    """Raised when the host process table cannot be read reliably."""


class FootprintError(RuntimeError):
    """Raised when a process's physical footprint cannot be read."""


class ProcessGone(FootprintError):
    """Raised when a process exited after it appeared in the ps snapshot."""


RUSAGE_INFO_V2 = 2


class RusageInfoV2(ctypes.Structure):
    """rusage_info_v2 layout mirroring the orchestrator's proc_footprint.py module."""

    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
    ]


def _load_libproc():
    try:
        return ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
    except OSError:
        return ctypes.CDLL(None, use_errno=True)


def _darwin_phys_footprint_bytes(pid: int) -> int:
    """Read any pid's phys_footprint via libproc, raising typed failures."""
    try:
        proc_pid_rusage = _load_libproc().proc_pid_rusage
        proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
        proc_pid_rusage.restype = ctypes.c_int
        info = RusageInfoV2()
        result = proc_pid_rusage(pid, RUSAGE_INFO_V2, ctypes.byref(info))
    except Exception as exc:
        raise FootprintError(f"could not read phys_footprint for pid {pid}: {exc}") from exc
    if result != 0:
        error = ctypes.get_errno()
        if result == errno.ESRCH or error == errno.ESRCH:
            raise ProcessGone(f"pid {pid} exited before phys_footprint could be read")
        raise FootprintError(f"proc_pid_rusage failed for pid {pid}: errno {error}")
    return int(info.ri_phys_footprint)


def _no_footprint(_pid: int) -> None:
    """No-op reader used where ps RSS is the supported measurement source."""
    return None


def _ps_text() -> str:
    """Return a full-width process table, failing closed on command errors."""
    try:
        result = subprocess.run(list(PS_ARGV), capture_output=True, text=True, timeout=PS_TIMEOUT_S)
    except subprocess.TimeoutExpired as exc:
        raise PsError(f"ps timed out after {PS_TIMEOUT_S}s: {exc}") from exc
    except OSError as exc:
        raise PsError(f"ps could not run: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or "").strip()
        suffix = f": {detail}" if detail else ""
        raise PsError(f"ps exited with status {result.returncode}{suffix}")
    return result.stdout


def parse_ps(text: str) -> list[tuple[int, int, int, str]]:
    """Parse ps's pid, ppid, RSS-kB and command columns without truncating argv."""
    rows: list[tuple[int, int, int, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split(None, 3)
        if len(fields) < 3:
            raise ValueError(f"malformed ps line {line!r}: expected pid, ppid and rss")
        try:
            pid, ppid, rss_kb = (int(value) for value in fields[:3])
        except ValueError as exc:
            raise ValueError(f"malformed ps line {line!r}: pid, ppid and rss must be integers") from exc
        command = fields[3] if len(fields) == 4 else ""
        rows.append((pid, ppid, rss_kb, command))
    return rows


def lane_processes(
    rows: Sequence[tuple[int, int, int, str]], needles: Sequence[str], exclude_pids: set[int]
) -> list[tuple[int, int, int, str]]:
    """Select needle-matching processes and descendants, while observing blocked-pid boundaries."""
    needles = tuple(needles)
    if not needles or any(not needle or needle.isspace() for needle in needles):
        raise ValueError("needles must be non-empty and contain a non-whitespace value")

    blocked = set(exclude_pids)
    blocked.update(pid for pid, _ppid, _rss, command in rows if "measure_lane_host_cost" in command)
    children: dict[int, list[int]] = defaultdict(list)
    for pid, ppid, _rss, _command in rows:
        children[ppid].append(pid)

    pending = [
        pid for pid, _ppid, _rss, command in rows if pid not in blocked and any(needle in command for needle in needles)
    ]
    selected: set[int] = set()
    visited: set[int] = set()
    while pending:
        pid = pending.pop()
        if pid in blocked or pid in visited:
            continue
        visited.add(pid)
        selected.add(pid)
        pending.extend(child for child in children.get(pid, ()) if child not in blocked and child not in visited)

    return [row for row in rows if row[0] in selected]


def _validate_run(lanes: int, interval_s: float, duration_s: float) -> None:
    if lanes < 1:
        raise ValueError("lanes must be at least 1")
    if not math.isfinite(interval_s) or interval_s < MIN_INTERVAL_S:
        raise ValueError(f"interval must be at least {MIN_INTERVAL_S} second")
    if not math.isfinite(duration_s) or duration_s <= 0 or duration_s > MAX_DURATION_S:
        raise ValueError(f"duration must be greater than 0 and at most {MAX_DURATION_S} seconds")


def _footprint_total(rows, reader) -> tuple[int, int]:
    footprint_kb = 0
    unreadable = 0
    for pid, _ppid, rss_kb, _command in rows:
        try:
            footprint_bytes = reader(pid)
            if type(footprint_bytes) is not int or footprint_bytes < 0:
                raise FootprintError(f"invalid phys_footprint for pid {pid}")
        except ProcessGone:
            continue
        except FootprintError:
            # RSS is the known lower bound, and unreadable makes the release evidence invalid.
            footprint_kb += rss_kb
            unreadable += 1
        else:
            footprint_kb += max(rss_kb, footprint_bytes // 1024)
    return footprint_kb, unreadable


def _sample_point(ps_text, needles, excluded, shared, elapsed, platform, footprint_reader) -> dict:
    rows = parse_ps(ps_text())
    lane = lane_processes(rows, needles, excluded)
    present = [row for row in rows if row[0] in shared]
    rss_kb = sum(row[2] for row in lane)
    shared_rss_kb = sum(row[2] for row in present)
    if platform == "darwin":
        footprint_kb, unreadable = _footprint_total(lane, footprint_reader)
        shared_footprint_kb, shared_unreadable = _footprint_total(present, footprint_reader)
    else:
        footprint_kb, shared_footprint_kb, unreadable, shared_unreadable = rss_kb, shared_rss_kb, 0, 0
    return {
        "t": round(elapsed(), 3),
        "process_count": len(lane),
        "rss_kb": rss_kb,
        "shared_rss_kb": shared_rss_kb,
        "footprint_kb": footprint_kb,
        "shared_footprint_kb": shared_footprint_kb,
        "footprint_unreadable": unreadable + shared_unreadable,
        "shared_missing": sorted(shared - {row[0] for row in present}),
    }


def _summarize(series, lanes: int) -> dict:
    process_counts = [int(point["process_count"]) for point in series]
    rss_mbs = [int(point["rss_kb"]) / 1024 for point in series]
    shared_rss_mbs = [int(point["shared_rss_kb"]) / 1024 for point in series]
    host_rss_per_lane = [
        (int(point["footprint_kb"]) + int(point["shared_footprint_kb"])) / 1024 / lanes for point in series
    ]
    footprint_unreadable = [int(point["footprint_unreadable"]) for point in series]
    return {
        "matched_any": any(process_counts),
        "process_count_max": max(process_counts),
        "process_count_p50": statistics.median(process_counts),
        "rss_mb_max": max(rss_mbs),
        "rss_mb_p50": statistics.median(rss_mbs),
        "shared_rss_mb_max": max(shared_rss_mbs),
        "footprint_unreadable_max": max(footprint_unreadable),
        "resident_host_processes_per_lane": max(process_counts) / lanes,
        "host_rss_mb_per_inflight_lane_max": max(host_rss_per_lane),
        "time_to_vm_start_p50_s": None,
    }


def _sleep_until_stopped(seconds: float, stop) -> None:
    """Bound default sleep slices so a signal flag is checked within one second."""
    deadline = time.monotonic() + seconds
    while not stop():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(1.0, remaining))


def _collect_samples(ps_text, needles, excluded, shared, interval_s, duration_s, clock, sleep, stop, platform, reader):
    series = []
    start = clock()
    while True:
        series.append(_sample_point(ps_text, needles, excluded, shared, lambda: clock() - start, platform, reader))
        if stop():
            stopped = "signal"
            break
        remaining = duration_s - (clock() - start)
        if remaining <= 0:
            stopped = "duration"
            break
        sleep(min(interval_s, remaining))
    return series, stopped


def run(
    task: str,
    *,
    mode: str,
    lanes: int,
    interval_s: float,
    duration_s: float,
    matches: Sequence[str] = (),
    shared_pids: Sequence[int] = (),
    ps_text=None,
    clock=None,
    sleep=None,
    stop=None,
    exclude_pids=None,
    footprint_reader=None,
) -> dict:
    """Sample matching processes and shared-daemon RSS/phys_footprint for a bounded wave."""
    _validate_run(lanes, interval_s, duration_s)
    ps_text = ps_text or _ps_text
    platform = sys.platform
    footprint_reader = (
        footprint_reader
        if footprint_reader is not None
        else (_darwin_phys_footprint_bytes if platform == "darwin" else _no_footprint)
    )
    clock = clock or time.monotonic
    stop = stop or (lambda: False)
    sleep = sleep or (lambda seconds: _sleep_until_stopped(seconds, stop))

    needles = [task, *matches]
    shared = set(shared_pids)
    excluded = ({os.getpid()} if exclude_pids is None else set(exclude_pids)) | shared
    started_at = datetime.now(timezone.utc).isoformat()
    series, stopped = _collect_samples(
        ps_text, needles, excluded, shared, interval_s, duration_s, clock, sleep, stop, platform, footprint_reader
    )

    return {
        "schema": SCHEMA,
        "mode": mode,
        "task": task,
        "needles": needles,
        "lanes": lanes,
        "interval_s": interval_s,
        "duration_s": duration_s,
        "shared_pids": sorted(shared),
        "samples": len(series),
        "stopped": stopped,
        "platform": platform,
        "memory_source": "max_rss_phys_footprint" if platform == "darwin" else "ps_rss",
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "series": series,
        **_summarize(series, lanes),
    }


def _nonnegative_number(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 0
        and (isinstance(value, int) or math.isfinite(value))
    )


def _valid_result(result, mode: str) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("schema") != SCHEMA or result.get("mode") != mode:
        return False
    memory_source = result.get("memory_source")
    if memory_source not in ("max_rss_phys_footprint", "ps_rss"):
        return False
    platform = result.get("platform")
    if not isinstance(platform, str) or not platform:
        return False
    if platform == "darwin" and memory_source != "max_rss_phys_footprint":
        return False
    footprint_unreadable = result.get("footprint_unreadable_max")
    if not _nonnegative_number(footprint_unreadable) or footprint_unreadable > 0:
        return False
    if type(result.get("lanes")) is not int or result["lanes"] != WAVE_LANES:
        return False
    if mode == "attached" and result.get("matched_any") is not True:
        return False
    if not all(
        _nonnegative_number(result.get(field))
        for field in ("resident_host_processes_per_lane", "host_rss_mb_per_inflight_lane_max")
    ):
        return False
    vm_start = result.get("time_to_vm_start_p50_s")
    return vm_start is None or _nonnegative_number(vm_start)


def verdict_for(acceptance: Mapping) -> str:
    """Validate O-4 evidence before judging it against the module's fixed plan bars."""
    if not isinstance(acceptance, dict):
        return "invalid_measurement"
    bars = acceptance.get("bars")
    if bars != BARS or not all(_nonnegative_number(value) for value in bars.values()):
        return "invalid_measurement"
    attached = acceptance.get("attached")
    detached = acceptance.get("detached")
    for mode, result in (("attached", attached), ("detached", detached)):
        if result is not None and not _valid_result(result, mode):
            return "invalid_measurement"
    if attached is None or detached is None:
        return "pending_operator_measurement"
    if detached.get("time_to_vm_start_p50_s") is None:
        return "pending_operator_measurement"

    if (
        detached["resident_host_processes_per_lane"] <= BARS["resident_host_processes_per_lane"]
        and detached["host_rss_mb_per_inflight_lane_max"] <= BARS["host_rss_mb_per_inflight_lane_max"]
        and detached["time_to_vm_start_p50_s"] <= BARS["time_to_vm_start_p50_s_max"]
    ):
        return "pass"
    return "fail"


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure host process count and memory use for in-flight lanes.")
    parser.add_argument("--mode", choices=("attached", "detached"))
    parser.add_argument("--lanes", type=int)
    parser.add_argument("--task")
    parser.add_argument("--match", action="append", default=[])
    parser.add_argument("--shared-pid", action="append", type=int, default=[])
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--duration", type=float, default=3600)
    parser.add_argument("--out")
    parser.add_argument("--verdict")
    return parser


def _validate_arguments(parser, args) -> None:
    if args.mode is None or args.lanes is None or args.task is None:
        parser.error("--mode, --lanes and --task are required unless --verdict is used")
    if args.lanes < 1:
        parser.error("--lanes must be at least 1")
    if not args.task or args.task.isspace():
        parser.error("--task must be non-empty and not whitespace")
    if any(not match or match.isspace() for match in args.match):
        parser.error("--match values must be non-empty and not whitespace")
    if any(pid < 1 for pid in args.shared_pid):
        parser.error("--shared-pid values must be at least 1")
    if not math.isfinite(args.interval) or args.interval < MIN_INTERVAL_S:
        parser.error(f"--interval must be at least {MIN_INTERVAL_S} second")
    if not math.isfinite(args.duration) or args.duration <= 0 or args.duration > MAX_DURATION_S:
        parser.error(f"--duration must be greater than 0 and at most {MAX_DURATION_S} seconds")


def _run_with_signals(args) -> dict:
    interrupted = False

    def on_signal(_signum, _frame) -> None:
        nonlocal interrupted
        interrupted = True

    previous_handlers = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, on_signal)
        return run(
            args.task,
            mode=args.mode,
            lanes=args.lanes,
            interval_s=args.interval,
            duration_s=args.duration,
            matches=args.match,
            shared_pids=args.shared_pid,
            stop=lambda: interrupted,
        )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def _write_result(result: dict, out: str | None) -> None:
    output = json.dumps(result, indent=1, sort_keys=True) + "\n"
    if out is None:
        print(output, end="")
    else:
        out_path = Path(out)
        temp_path = Path(f"{out_path}.tmp")
        temp_path.write_text(output, encoding="utf-8")
        os.replace(temp_path, out_path)


def main(argv: list[str] | None = None) -> int:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    if args.verdict is not None:
        acceptance = json.loads(Path(args.verdict).read_text(encoding="utf-8"))
        verdict = verdict_for(acceptance)
        print(verdict)
        return 1 if verdict in {"fail", "invalid_measurement"} else 0

    _validate_arguments(parser, args)
    try:
        result = _run_with_signals(args)
    except PsError as exc:
        print(f"measure_lane_host_cost: ps failed: {exc}", file=sys.stderr)
        return 3
    except ValueError as exc:
        print(f"measure_lane_host_cost: unreadable ps output: {exc}", file=sys.stderr)
        return 3
    _write_result(result, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
