#!/usr/bin/env python3
"""Bounded, observation-only VM accounting; unknown counters never become zero.

Scope and proc roots and the ledger clock are injectable. Final accounting reads
only retained ledger records and remains available after cgroup unload.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import re
import stat
import sys
import tempfile
import time
from pathlib import Path

CAP = 65536
PHASES = {"queue_wait", "setup", "model", "provider_wait", "test", "self_verify", "harvest"}
EVENTS = {"phase_start", "phase_end", "sample"}
CPU = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")


def _unknown(out, key, reason):
    out[key] = None
    out["unknown"].append({"counter": key, "reason": reason})


def _read(path):
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        mode = os.fstat(fd).st_mode
        if not stat.S_ISREG(mode) or not mode & 0o444:
            return None, "unreadable"
        data = os.read(fd, CAP + 1)
        if len(data) > CAP:
            return None, "oversize"
        return data.decode("ascii"), None
    except FileNotFoundError:
        return None, "missing"
    except (OSError, UnicodeError):
        return None, "unreadable"
    finally:
        if fd is not None:
            os.close(fd)


def _number(value, floating=False, maximum=False):
    if value == "max" and maximum:
        return value
    result = float(value) if floating else int(value)
    if result < 0 or not math.isfinite(result):
        raise ValueError("invalid counter")
    return result


def _fields(out, path, prefix, keys, style):
    text, reason = _read(path)
    values = {}
    if reason is None:
        try:
            if style == "pairs":
                values = dict(line.split() for line in text.splitlines())
            elif style == "pressure":
                for line in text.splitlines():
                    parts = line.split()
                    for pair in parts[1:]:
                        key, value = pair.split("=")
                        values[f"{parts[0]}:{key}"] = value
            elif style == "cpu":
                values = dict(zip(("quota", "period"), text.split(), strict=True))
            elif style == "stat":
                parts = next(line.split()[1:] for line in text.splitlines() if line.startswith("cpu "))
                values = dict(zip(CPU, parts[:8], strict=True))
            elif style == "load":
                values = dict(zip(keys, text.split()[:3], strict=True))
            else:
                values = {"value": text.strip()}
        except (ValueError, StopIteration):
            reason = "malformed"
    for key in keys:
        full = f"{prefix}:{key}"
        try:
            if reason:
                _unknown(out, full, reason)
            else:
                out[full] = _number(
                    values[key],
                    floating=style in ("pressure", "load") and not key.endswith("total"),
                    maximum=(style == "cpu" and key == "quota") or (style == "scalar" and prefix.endswith(".max")),
                )
                if key == "period" and out[full] == 0:
                    raise ValueError("zero period")
        except (KeyError, ValueError, OverflowError):
            _unknown(out, full, "malformed")


def _scope_path(root, control_group):
    if ".." in Path(control_group).parts:
        return None
    root = Path(root).resolve()
    path = root / control_group.lstrip("/")
    if not path.resolve().is_relative_to(root):
        return None
    return path


def _pressure(out, path, prefix):
    _fields(
        out,
        path,
        prefix,
        [f"{kind}:{key}" for kind in ("some", "full") for key in ("avg10", "avg60", "total")],
        "pressure",
    )


def read_scope(cgroup_root: Path, control_group: str) -> dict:
    """Read explicitly selected scope counters with per-value unknown reasons."""
    path = _scope_path(cgroup_root, control_group)
    if path is None:
        return {"refusal": "invalid_control_group"}
    out = {"unknown": []}
    _fields(out, path / "cpu.stat", "cpu.stat", ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec"), "pairs")
    _fields(out, path / "cpu.max", "cpu.max", ("quota", "period"), "cpu")
    _fields(out, path / "memory.events", "memory.events", ("oom", "oom_kill"), "pairs")
    for name in ("memory.current", "memory.peak", "memory.max", "pids.current", "pids.peak", "pids.max"):
        _fields(out, path / name, name, ("value",), "scalar")
    for name in ("cpu", "memory", "io"):
        _pressure(out, path / f"{name}.pressure", f"{name}.pressure")
    return out


def read_host(proc_root: Path) -> dict:
    """Read aggregate jiffies, load averages and host pressure."""
    out = {"unknown": []}
    _fields(out, proc_root / "stat", "stat", CPU, "stat")
    _fields(out, proc_root / "loadavg", "loadavg", ("1", "5", "15"), "load")
    for name in ("cpu", "memory", "io"):
        _pressure(out, proc_root / "pressure" / name, f"{name}.pressure")
    return out


def effective_limits(cgroup_root: Path, control_group: str) -> dict:
    """Intersect all ancestor limits; an unknown ancestor poisons its limit."""
    path = _scope_path(cgroup_root, control_group)
    if path is None:
        return {"refusal": "invalid_control_group"}
    root = Path(cgroup_root).resolve()
    out = {"unknown": []}
    for resource in ("cpu", "memory", "pids"):
        best, binding, failed = "max", "/", False
        ancestor = path
        while True:
            sample = {"unknown": []}
            name = f"{resource}.max"
            keys = ("quota", "period") if resource == "cpu" else ("value",)
            _fields(sample, ancestor / name, name, keys, "cpu" if resource == "cpu" else "scalar")
            label = "/" + str(ancestor.relative_to(root)) if ancestor != root else "/"
            # The hierarchy root has no controller limit interfaces on cgroup v2.
            if ancestor == root and sample["unknown"] and all(u["reason"] == "missing" for u in sample["unknown"]):
                break
            for unknown in sample["unknown"]:
                out["unknown"].append({**unknown, "ancestor": label})
            value = sample[f"{name}:{keys[0]}"]
            if sample["unknown"]:
                failed = True
            elif value != "max":
                value = value / sample[f"{name}:period"] if resource == "cpu" else value
                if best == "max" or value < best:
                    best, binding = value, label
            if ancestor == root:
                break
            ancestor = ancestor.parent
        out[resource] = {"value": None if failed else best, "ancestor": None if failed else binding}
    return out


def delta(prev: dict, cur: dict) -> dict:
    """Difference numeric observations, reporting resets and undefined ratios."""
    out = {"unknown": list(prev.get("unknown", [])) + list(cur.get("unknown", []))}
    for key in sorted((set(prev) | set(cur)) - {"unknown"}):
        a, b = prev.get(key), cur.get(key)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            if b < a:
                _unknown(out, key, "counter_reset")
            else:
                out[key] = b - a
        else:
            _unknown(out, key, "unknown_operand")
    periods, throttled = out.get("cpu.stat:nr_periods"), out.get("cpu.stat:nr_throttled")
    if periods and throttled is not None:
        out["throttled_ratio"] = throttled / periods
    else:
        _unknown(out, "throttled_ratio", "no_periods" if periods == 0 else "unknown_operand")
    jiffies = [out.get(f"stat:{key}") for key in CPU]
    if all(isinstance(v, (int, float)) for v in jiffies) and sum(jiffies) > 0:
        total = sum(jiffies)
        out["busy_fraction"] = (total - out["stat:idle"] - out["stat:iowait"]) / total
        out["steal_fraction"] = out["stat:steal"] / total
    else:
        for key in ("busy_fraction", "steal_fraction"):
            _unknown(out, key, "unknown_operand")
    return out


def _valid_nonce(nonce):
    return bool(re.fullmatch(r"[A-Za-z0-9._-]{1,128}", nonce)) and ".." not in nonce


def _clock():
    return time.time(), time.monotonic()


class Ledger:
    """Append one complete JSON record with one O_APPEND write."""

    def __init__(self, directory: Path, dispatch_nonce: str, clock=_clock):
        self.directory, self.nonce, self.clock = Path(directory), dispatch_nonce, clock

    def append(self, event: str, phase: str, scope_unit: str, control_group: str, sample: dict) -> dict:
        if not _valid_nonce(self.nonce):
            return {"refusal": "invalid_nonce"}
        if phase not in PHASES or event not in EVENTS:
            return {"refusal": "invalid_phase_or_event"}
        fd = None
        try:
            wall, mono = self.clock()
            record = {
                "event": event,
                "phase": phase,
                "dispatch_nonce": self.nonce,
                "scope_unit": scope_unit,
                "control_group": control_group,
                "timestamp": datetime.datetime.fromtimestamp(wall, datetime.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "monotonic": mono,
                "sample": sample,
            }
            line = (json.dumps(record, sort_keys=True, allow_nan=False) + "\n").encode()
            if len(line) > CAP:
                return {"refusal": "oversize_record"}
            self.directory.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                self.directory / f"{self.nonce}.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600
            )
            if os.write(fd, line) != len(line):
                return {"refusal": "short_write"}
            os.fsync(fd)
            return record
        except (OSError, ValueError, TypeError):
            return {"refusal": "ledger_write_failed"}
        finally:
            if fd is not None:
                os.close(fd)


def finalize(directory: Path, nonce: str) -> dict:
    """Atomically retain a deterministic summary using only bounded ledger reads."""
    if not _valid_nonce(nonce):
        return {"refusal": "invalid_nonce"}
    directory = Path(directory)
    temp = None
    fd = None
    try:
        fd = os.open(directory / f"{nonce}.jsonl", os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return {"refusal": "unreadable_ledger"}
        groups = {}
        with os.fdopen(fd, "rb") as stream:
            fd = None
            count = 0
            while True:
                line = stream.readline(CAP + 1)
                if not line:
                    break
                count += 1
                if len(line) > CAP or count > 100000:
                    return {"refusal": "oversize_ledger"}
                record = json.loads(line)
                if record["dispatch_nonce"] != nonce or record["phase"] not in PHASES or record["event"] not in EVENTS:
                    return {"refusal": "invalid_record"}
                groups.setdefault(record["phase"], []).append(record)
        result = {"dispatch_nonce": nonce, "phases": {}}
        for phase, records in groups.items():
            intervals, active = [], None
            complete = True
            previous_time = None
            for record in records:
                tick = record["monotonic"]
                if not isinstance(tick, (int, float)) or not math.isfinite(tick):
                    return {"refusal": "invalid_phase_timestamp"}
                if previous_time is not None and tick < previous_time:
                    return {"refusal": "nonmonotonic_phase_boundaries"}
                previous_time = tick
                if record["event"] == "phase_start":
                    if active is not None:
                        return {"refusal": "overlapping_phase_boundaries"}
                    active = record
                elif record["event"] == "phase_end":
                    if active is None:
                        complete = False
                    else:
                        if (active["scope_unit"], active["control_group"]) != (
                            record["scope_unit"],
                            record["control_group"],
                        ):
                            return {"refusal": "phase_scope_changed"}
                        intervals.append((active, record))
                        active = None
                elif active is None:
                    complete = False
            complete = complete and active is None and bool(intervals)
            changes = [delta(start["sample"], end["sample"]) for start, end in intervals]
            unknown = [u for change in changes for u in change["unknown"]]
            for record in records:
                unknown.extend(record["sample"].get("unknown", []))

            def total(key):
                values = [change.get(key) for change in changes]
                return sum(values) if complete and all(isinstance(v, (int, float)) for v in values) else None

            periods = total("cpu.stat:nr_periods")
            throttled = total("cpu.stat:nr_throttled")
            summary = {
                "wall_seconds": sum(end["monotonic"] - start["monotonic"] for start, end in intervals)
                if complete
                else None,
                "cpu_usec": total("cpu.stat:usage_usec"),
                "nr_throttled": throttled,
                "throttled_ratio": throttled / periods if periods and throttled is not None else None,
                "oom_kill": total("memory.events:oom_kill"),
                "psi": {k: v for k, v in records[-1]["sample"].items() if ".pressure:" in k},
            }
            if not complete:
                for key in ("wall_seconds", "cpu_usec", "nr_throttled", "throttled_ratio", "oom_kill"):
                    unknown.append({"counter": key, "reason": "missing_phase_boundary"})
            elif periods == 0:
                unknown.append({"counter": "throttled_ratio", "reason": "no_periods"})
            for resource in ("memory", "pids"):
                key = f"{resource}.peak:value"
                values = [r["sample"].get(key) for r in records]
                summary[f"{resource}_peak"] = max(values) if all(isinstance(v, (int, float)) for v in values) else None
            summary["unknown"] = [json.loads(v) for v in sorted({json.dumps(u, sort_keys=True) for u in unknown})]
            result["phases"][phase] = summary
        data = (json.dumps(result, sort_keys=True, allow_nan=False) + "\n").encode()
        tmpfd, temp = tempfile.mkstemp(prefix=f".{nonce}.", dir=directory)
        with os.fdopen(tmpfd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, directory / f"{nonce}.final.json")
        return result
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return {"refusal": "invalid_or_unreadable_ledger"}
    finally:
        if fd is not None:
            os.close(fd)
        if temp is not None:
            try:
                os.unlink(temp)
            except FileNotFoundError:
                pass


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def main(argv=None):
    try:
        parser = _Parser(description=__doc__)
        commands = parser.add_subparsers(dest="command", required=True)
        sample = commands.add_parser("sample")
        finish = commands.add_parser("finalize")
        for command in (sample, finish):
            command.add_argument("--nonce", required=True)
            command.add_argument("--ledger-dir", required=True, type=Path)
        sample.add_argument("--cgroup-root", required=True, type=Path)
        sample.add_argument("--control-group", required=True)
        sample.add_argument("--scope-unit", default="")
        sample.add_argument("--phase", required=True)
        sample.add_argument("--event", default="sample")
        args = parser.parse_args(argv)
        if args.command == "finalize":
            result = finalize(args.ledger_dir, args.nonce)
        else:
            observation = read_scope(args.cgroup_root, args.control_group)
            observation["effective_limits"] = effective_limits(args.cgroup_root, args.control_group)
            result = (
                observation
                if "refusal" in observation
                else Ledger(args.ledger_dir, args.nonce).append(
                    args.event, args.phase, args.scope_unit, args.control_group, observation
                )
            )
    except (ValueError, OSError):
        result = {"refusal": "invalid_arguments"}
    print(json.dumps(result, sort_keys=True))
    return 3 if "refusal" in result else 0


if __name__ == "__main__":
    sys.exit(main())
