#!/usr/bin/env python3
"""Report filesystem use and visible WorkBay disk classes.

``--root`` defaults to ``~/.workbay`` and anchors ``.task-state`` staging.
Sandboxes use the configured
``WORKBAY_REMOTE_AGENT_ROOT`` or ``workbay-sandbox`` from
``remote_sandbox_reap.py`` (``_DEFAULT_AGENT_ROOT``). Evidence directories
match that module's ``_EVIDENCE_KEY_RE`` and are kept separate from
marker-owned sandbox entries. Staging
children use ``remote-exec-`` from ``remote_exec_staging_reaper.py``
(``STAGING_DIR_PREFIX``), which ``lane_census.py`` imports. The ``uv_cache``
and ``tmp_scratch`` buckets follow the cache and marked scratch roots the VM
reaper measures. Sessions include existing ``~/.codex`` and ``~/.grok``
session/log stores. Archives include ``archives`` directories under those
homes and ``--root``, plus ``verify-*`` trees under the configured sandbox
root. ``other`` is the allocated filesystem-use residue after measured classes.
All byte counts use allocated space (du -s -B1), matching df -B1; foreign
devices are reported separately by mount. Hardlinked entries are charged to
the first class in ``CLASS_NAMES`` order, with duplicate bytes reported
separately.

This tool observes those classes; it does not invoke either reaper.  Each
``df`` and ``du`` process has a bounded timeout.  Failed class measurements are
reported as typed refusals and do not prevent the remaining ledger from being
produced. If configured class roots overlap, both affected classes are refused
so their bytes cannot be counted twice.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import NotRequired, TypedDict

DEFAULT_TIMEOUT_SECONDS = 30.0
CLASS_NAMES = ("sandboxes", "staging", "uv_cache", "tmp_scratch", "evidence", "sessions", "archives", "other")
DEFAULT_AGENT_ROOT = "workbay-sandbox"
STAGING_DIR_NAME = ".task-state"
STAGING_DIR_PREFIX = "remote-exec-"
ARCHIVE_DIR_NAME = "archives"
ARCHIVE_DIR_PREFIX = "verify-"
LANE_SANDBOX_MARKER = ".workbay-lane-sandbox"
TMP_SCRATCH_MARKER = ".workbay-scratch"
TMP_SCRATCH_PREFIXES = ("workbay-run-", "workbay-scratch-")
EVIDENCE_KEY_RE = re.compile(r"^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$")


class Refusal(TypedDict):
    """A machine-readable reason a disk measurement could not be trusted."""

    type: str
    message: str
    class_name: NotRequired[str]


def _parse_df_line(df_line: str) -> tuple[int, int, int] | None:
    fields = df_line.split()
    if len(fields) != 3 or any(not field.isdecimal() for field in fields):
        return None
    total, used, available = (int(field) for field in fields)
    if total < 0 or used < 0 or available < 0:
        return None
    return total, used, available


def _parse_df_record(df_line: str) -> dict[str, int | str] | None:
    fields = df_line.split(maxsplit=3)
    if len(fields) != 4 or not fields[3].strip() or any(not field.isdecimal() for field in fields[:3]):
        return None
    total, used, available = (int(field) for field in fields[:3])
    if min(total, used, available) < 0:
        return None
    return {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": available,
        "mount": fields[3],
    }


def _normalise_class_sizes(
    class_sizes: dict[str, int | None],
) -> tuple[dict[str, dict[str, int | bool]], list[Refusal]]:
    classes: dict[str, dict[str, int | bool]] = {}
    refusals: list[Refusal] = []
    for name, size in class_sizes.items():
        if size is None:
            classes[name] = {"bytes": 0, "measured": False}
        elif type(size) is int and size >= 0:
            classes[name] = {"bytes": size, "measured": True}
        else:
            classes[name] = {"bytes": 0, "measured": False}
            refusals.append(
                {
                    "type": "invalid_class_size",
                    "class_name": name,
                    "message": "class size must be a non-negative integer or None",
                }
            )
    return classes, refusals


def build_ledger(
    df_line: str,
    class_sizes: dict[str, int | None],
    *,
    allocated_total_bytes: int | None = None,
) -> dict[str, object]:
    """Build a ledger from a root ``df`` line and allocated class byte counts."""
    classes, refusals = _normalise_class_sizes(class_sizes)
    parsed_df = _parse_df_line(df_line)
    if parsed_df is None:
        fs_total = fs_used = fs_available = None
        refusals.insert(
            0,
            {
                "type": "df_unparseable",
                "message": "df output must contain exactly three non-negative byte counts",
            },
        )
        inaccessible_bytes = 0
        inaccessible_negative = False
    else:
        fs_total, fs_used, fs_available = parsed_df
        allocated_total = fs_used if allocated_total_bytes is None else allocated_total_bytes
        classified_bytes = sum(
            int(info["bytes"]) for name, info in classes.items() if name != "other" and info["measured"] is True
        )
        remainder = allocated_total - classified_bytes
        inaccessible_bytes = max(0, remainder)
        inaccessible_negative = remainder < 0

    visible_bytes = sum(int(info["bytes"]) for info in classes.values() if info["measured"] is True)
    return {
        "basis": "allocated",
        "bytes_basis": "allocated",
        "allocated_total_bytes": fs_used
        if parsed_df is None or allocated_total_bytes is None
        else allocated_total_bytes,
        "fs_total_bytes": fs_total,
        "fs_used_bytes": fs_used,
        "fs_available_bytes": fs_available,
        "classes": classes,
        "visible_bytes": visible_bytes,
        "inaccessible_bytes": inaccessible_bytes,
        "inaccessible_negative": inaccessible_negative,
        "refusals": refusals,
    }


def _df_refusal_ledger(refusal_type: str, message: str) -> dict[str, object]:
    ledger = build_ledger("", {name: None for name in CLASS_NAMES})
    ledger["refusals"] = [{"type": refusal_type, "message": message}]
    return ledger


def _last_data_line(output: str) -> str:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _parse_du_bytes(
    output: str,
    expected_paths: list[str],
    *,
    deduplicated_paths: set[str] | None = None,
) -> int | None:
    lines = [line for line in output.splitlines() if line.strip()]
    expected = set(expected_paths)
    parsed: dict[str, int] = {}
    for line in lines:
        fields = line.split("\t", maxsplit=1)
        if len(fields) != 2 or not fields[0].isdecimal() or fields[1] not in expected or fields[1] in parsed:
            return None
        parsed[fields[1]] = int(fields[0])
    missing = expected - parsed.keys()
    if not missing.issubset(deduplicated_paths or set()):
        return None
    return sum(parsed.values())


def _walk_class_inodes(
    paths: list[str | Path], device_lookup=None
) -> tuple[dict[tuple[int, int], int], set[str], str | None]:
    """Find allocated bytes for hardlinked entries and direct duplicate operands."""
    inode_allocations: dict[tuple[int, int], int] = {}
    deduplicated_paths: set[str] = set()
    seen_operands: set[tuple[int, int]] = set()
    seen_hardlink_inodes: set[tuple[int, int]] = set()

    def walk(path: str, root_device: int) -> None:
        pending = [path]
        while pending:
            current = pending.pop()
            info = os.lstat(current)
            if info.st_dev != root_device:
                continue
            key = (info.st_dev, info.st_ino)
            is_inode_entry = stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
            if is_inode_entry and info.st_nlink > 1:
                seen_hardlink_inodes.add(key)
                inode_allocations.setdefault(key, int(getattr(info, "st_blocks", 0)) * 512)
            if stat.S_ISLNK(info.st_mode):
                continue
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(current) as iterator:
                    children = sorted(entry.name for entry in iterator)
                pending.extend(os.path.join(current, child) for child in reversed(children))

    try:
        for raw_path in paths:
            path = os.fspath(raw_path)
            if device_lookup is not None:
                device_lookup(raw_path)
            info = os.lstat(path)
            key = (info.st_dev, info.st_ino)
            is_inode_entry = stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
            if key in seen_operands or (is_inode_entry and info.st_nlink > 1 and key in seen_hardlink_inodes):
                deduplicated_paths.add(str(path))
            seen_operands.add(key)
            walk(path, info.st_dev)
    except OSError as exc:
        return {}, set(), str(exc)
    return inode_allocations, deduplicated_paths, None


def _command_failure(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    return detail or f"command exited with status {result.returncode}"


def _run_subprocess(command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, timeout=timeout, capture_output=True, text=True, check=False)


def _sandbox_root(root: Path) -> Path:
    configured = os.environ.get("WORKBAY_REMOTE_AGENT_ROOT", "").strip()
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_absolute() else Path.home() / path
    if root.name == DEFAULT_AGENT_ROOT:
        return root
    if root.name == ".workbay":
        return root.parent / DEFAULT_AGENT_ROOT
    return root / DEFAULT_AGENT_ROOT


def _skip(skipped, class_name, path, kind="symlink_skipped"):
    skipped.append({"type": kind, "class_name": class_name, "path": str(path)})


def _directories_matching(parent, predicate, class_name, skipped):
    try:
        if parent.is_symlink():
            _skip(skipped, class_name, parent)
            return [], None
        children = sorted(parent.iterdir())
        matches = []
        for child in children:
            try:
                mode = child.lstat().st_mode
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(mode):
                _skip(skipped, class_name, child)
            elif predicate(child):
                matches.append(child)
        return matches, None
    except FileNotFoundError:
        return [], None
    except OSError as exc:
        return [], str(exc)


def _existing_targets(paths, class_name, skipped, *, resolve=False):
    targets = []
    try:
        for path in paths:
            try:
                mode = path.lstat().st_mode
            except FileNotFoundError:
                _skip(skipped, class_name, path, "missing_path")
                continue
            if stat.S_ISLNK(mode):
                if resolve and path.exists():
                    targets.append(path)
                else:
                    _skip(skipped, class_name, path)
            else:
                targets.append(path)
        return targets, None
    except OSError as exc:
        return [], str(exc)


def _class_targets(root, class_name, skipped):
    sandbox_root = _sandbox_root(root)
    scans = {
        "sandboxes": (
            sandbox_root,
            lambda p: (
                not EVIDENCE_KEY_RE.fullmatch(p.name)
                and not p.name.startswith(ARCHIVE_DIR_PREFIX)
                and (p / LANE_SANDBOX_MARKER).is_file()
            ),
        ),
        "evidence": (sandbox_root, lambda p: EVIDENCE_KEY_RE.fullmatch(p.name) is not None),
        "staging": (root / STAGING_DIR_NAME, lambda p: p.name.startswith(STAGING_DIR_PREFIX)),
    }
    if class_name in scans:
        parent, predicate = scans[class_name]
        return _directories_matching(parent, lambda p: p.is_dir() and predicate(p), class_name, skipped)
    if class_name == "uv_cache":
        configured = os.environ.get("UV_CACHE_DIR", "").strip()
        local = root / "uv_cache"
        cache = (
            Path(configured).expanduser() if configured else (local if local.is_dir() else Path.home() / ".cache/uv")
        )
        return _existing_targets([cache], class_name, skipped, resolve=bool(configured))
    if class_name == "sessions":
        paths = [Path.home() / agent / store for agent in (".codex", ".grok") for store in ("sessions", "log", "logs")]
        return _existing_targets(paths, class_name, skipped)
    if class_name == "archives":
        archive_stores = [Path.home() / agent / ARCHIVE_DIR_NAME for agent in (".codex", ".grok")]
        archive_stores.append(root / ARCHIVE_DIR_NAME)
        stores, error = _existing_targets(archive_stores, class_name, skipped)
        if error:
            return [], error
        verify_trees, error = _directories_matching(
            sandbox_root,
            lambda path: path.is_dir() and path.name.startswith(ARCHIVE_DIR_PREFIX),
            class_name,
            skipped,
        )
        if error:
            return [], error
        return [*stores, *verify_trees], None
    if class_name == "tmp_scratch":
        local = root / "tmp_scratch"
        parent = local if local.is_dir() else Path(os.environ.get("TMPDIR") or "/tmp").expanduser()
        return _directories_matching(
            parent,
            lambda p: p.is_dir() and (p.name.startswith(TMP_SCRATCH_PREFIXES) or (p / TMP_SCRATCH_MARKER).is_file()),
            class_name,
            skipped,
        )
    if class_name == "other":
        return [], None
    raise ValueError(f"unknown disk ledger class: {class_name}")


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _canonicalize_targets(targets):
    """Resolve each measured root once so later overlap checks are path-only."""
    canonical = {}
    for name, paths in targets.items():
        unique = {}
        for path in paths:
            resolved = Path(os.path.realpath(path))
            unique.setdefault(resolved, None)
        canonical[name] = list(unique)
    return canonical


def _discover_class_targets(root):
    targets, refusals, skipped = {}, [], []
    for name in CLASS_NAMES:
        paths, error = _class_targets(root, name, skipped)
        if error:
            refusals.append({"type": "directory_scan_failed", "class_name": name, "message": error})
        else:
            targets[name] = paths
    return targets, refusals, skipped


def _overlapping_classes(targets):
    overlapping = {}
    names = list(targets)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            if any(_paths_overlap(a, b) for a in targets[left] for b in targets[right]):
                overlapping.setdefault(left, set()).add(right)
                overlapping.setdefault(right, set()).add(left)
    return overlapping


def _measure_class(name, paths, run, timeout, devices):
    def refusal(kind, message):
        return None, None, {"type": kind, "class_name": name, "message": message}, {}

    if not paths:
        return 0, None, None, {}
    if len(devices) != 1:
        return refusal("class_spans_filesystems", "class targets span multiple devices; measurement withheld")
    inode_allocations, deduplicated_paths, walk_error = _walk_class_inodes(paths)
    if walk_error:
        return refusal("class_walk_failed", walk_error)
    expected = [str(path) for path in paths]
    # -x also prevents nested mounts from contaminating this device's total.
    command = ["du", "-s", "-B1", "-x", "--", *expected]
    try:
        result = run(command, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return refusal("timeout", str(exc))
    except OSError as exc:
        return refusal("command_failed", str(exc))
    if result.returncode != 0:
        return refusal("command_failed", _command_failure(result))
    size = _parse_du_bytes(result.stdout or "", expected, deduplicated_paths=deduplicated_paths)
    if size is None:
        return refusal("du_unparseable", "du output must contain one byte count per expected path")
    return size, next(iter(devices)), None, inode_allocations


def _collect_df(path, device, run, timeout):
    try:
        result = run(["df", "-B1", "--output=size,used,avail,target", "--", str(path)], timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return None, {"type": "df_timeout", "device": device, "message": str(exc)}
    except OSError as exc:
        return None, {"type": "df_command_failed", "device": device, "message": str(exc)}
    if result.returncode != 0:
        return None, {"type": "df_command_failed", "device": device, "message": _command_failure(result)}
    line = _last_data_line(result.stdout or "")
    record = _parse_df_record(line)
    if record is None:
        return None, {
            "type": "df_unparseable",
            "device": device,
            "message": "df output must contain byte counts and a mount",
        }
    record.update(device=device, representative_path=str(path))
    return record, None


def _target_devices(targets, root_path, root_device, lookup, path_devices):
    devices_by_class, representatives, errors = {}, {root_device: root_path}, {}
    for name in CLASS_NAMES:
        devices_by_class[name] = set()
        for path in targets.get(name, []):
            try:
                device = lookup(path).st_dev
            except OSError as exc:
                errors[name] = {"type": "device_lookup_failed", "class_name": name, "message": str(exc)}
                continue
            devices_by_class[name].add(device)
            path_devices[str(path)] = device
            representatives.setdefault(device, path)
    return devices_by_class, representatives, errors


def _measure_targets(targets, overlapping, run, timeout, devices_by_class, device_errors, root_device):
    sizes, class_devices, foreign, shared_by_class, refusals = {}, {}, {}, {}, []
    claimed_inodes: set[tuple[int, int]] = set()
    for name in CLASS_NAMES:
        class_devices[name] = next(iter(devices_by_class.get(name, set())), None)
        shared_by_class[name] = 0
        if name == "other":
            sizes[name] = None
            continue
        if name not in targets:
            sizes[name] = None
            continue
        if name in device_errors:
            sizes[name] = None
            refusals.append(device_errors[name])
            continue
        if name in overlapping:
            sizes[name] = None
            refusals.append(
                {
                    "type": "class_path_overlap",
                    "class_name": name,
                    "message": f"class paths overlap with {', '.join(sorted(overlapping[name]))}; measurement withheld",
                }
            )
            continue
        devices = devices_by_class.get(name, set())
        size, device, refusal, inode_allocations = _measure_class(name, targets[name], run, timeout, devices)
        sizes[name] = size
        if refusal:
            refusals.append(refusal)
        elif device is not None:
            shared_bytes = sum(value for key, value in inode_allocations.items() if key in claimed_inodes)
            if shared_bytes > int(size):
                sizes[name] = None
                refusals.append(
                    {
                        "type": "inode_accounting_mismatch",
                        "class_name": name,
                        "message": "shared inode allocation exceeds the measured class total",
                    }
                )
                continue
            sizes[name] = int(size) - shared_bytes
            shared_by_class[name] = shared_bytes
            claimed_inodes.update(inode_allocations)
            if device != root_device:
                foreign[name] = {"device": device, "bytes": sizes[name]}
    return sizes, class_devices, foreign, shared_by_class, refusals


def _recording_device_lookup(lookup, devices):
    def recorded(path):
        result = lookup(path)
        devices[str(path)] = result.st_dev
        return result

    return recorded


def _filesystem_residues(filesystems, sizes, class_devices, allocation_complete):
    residues, negative = {}, False
    for device, filesystem in filesystems.items():
        class_bytes = sum(
            int(sizes[name])
            for name in CLASS_NAMES
            if name != "other" and sizes.get(name) is not None and str(class_devices.get(name)) == device
        )
        remainder = int(filesystem["used_bytes"]) - class_bytes
        residues[str(device)] = {
            "mount": filesystem["mount"],
            "bytes": max(0, remainder) if allocation_complete else None,
            "measured": allocation_complete,
            "negative": remainder < 0 if allocation_complete else False,
        }
        negative = negative or remainder < 0
    return residues, negative


def _collect_filesystems(representatives, run, timeout_seconds):
    filesystems, refusals = {}, []
    for device, representative in representatives.items():
        filesystem, refusal = _collect_df(representative, device, run, timeout_seconds)
        if refusal:
            refusals.append(refusal)
        else:
            filesystems[str(device)] = filesystem
    return filesystems, refusals


def _df_failure_ledger(df_refusals, filesystems, root_device):
    first = df_refusals[0]
    failure = _df_refusal_ledger(first["type"], first["message"])
    failure.update(filesystems=filesystems, root_device=root_device, refusals=df_refusals)
    root_filesystem = filesystems.get(str(root_device))
    if root_filesystem is not None:
        failure.update(
            fs_total_bytes=root_filesystem["total_bytes"],
            fs_used_bytes=root_filesystem["used_bytes"],
            fs_available_bytes=root_filesystem["available_bytes"],
        )
    failure.update(
        allocated_total_bytes=None,
        inaccessible_bytes=None,
        allocation_complete=False,
        other_by_filesystem=_filesystem_residues(filesystems, {}, {}, False)[0],
    )
    return failure


def _summarize_collection(
    root_device,
    path_devices,
    filesystems,
    sizes,
    class_devices,
    foreign,
    shared_by_class,
    skipped,
    refusals,
    allocation_complete,
):
    other_by_filesystem, negative_by_mount = _filesystem_residues(
        filesystems, sizes, class_devices, allocation_complete
    )
    allocated_total = sum(int(filesystem["used_bytes"]) for filesystem in filesystems.values())
    classified_bytes = sum(int(size) for name, size in sizes.items() if name != "other" and size is not None)
    other_remainder = allocated_total - classified_bytes
    sizes["other"] = max(0, other_remainder) if allocation_complete else None
    root_fs = filesystems[str(root_device)]
    root_line = " ".join(str(root_fs[key]) for key in ("total_bytes", "used_bytes", "available_bytes"))
    ledger = build_ledger(root_line, sizes, allocated_total_bytes=allocated_total if allocation_complete else None)
    ledger["classes"] = _normalise_class_sizes(sizes)[0]
    ledger.update(
        filesystems=filesystems,
        other_by_filesystem=other_by_filesystem,
        allocated_total_bytes=allocated_total if allocation_complete else None,
        inaccessible_bytes=max(0, other_remainder) if allocation_complete else None,
        inaccessible_negative=(negative_by_mount or other_remainder < 0) if allocation_complete else False,
        allocation_complete=allocation_complete,
        root_device=root_device,
        path_devices=path_devices,
        foreign_filesystems=foreign,
        shared_bytes_by_class=shared_by_class,
        shared_bytes=sum(shared_by_class.values()),
        skipped=skipped,
    )
    ledger["binding_mount"] = min(filesystems.values(), key=lambda fs: fs["available_bytes"])["mount"]
    ledger["refusals"].extend(refusals)
    ledger["refusals"].extend(
        {
            "type": "class_totals_exceed_filesystem",
            "message": f"measured classes exceed allocated use on {residue['mount']}",
            "device": int(device),
        }
        for device, residue in other_by_filesystem.items()
        if allocation_complete and residue["negative"]
    )
    return ledger


def collect(
    root: str | Path, runner=None, *, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS, device_lookup=None
) -> dict[str, object]:
    """Collect allocated bytes with bounded, injectable subprocess and stat calls.

    Each distinct target device has its own ``df`` record. Mixed-device classes
    are refused, while hardlinked file allocations belong to the first measured
    class. Discovery omissions are explicit in ``skipped``.
    """
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    run = runner or _run_subprocess
    path_devices = {}
    lookup = _recording_device_lookup(device_lookup or os.stat, path_devices)
    root_path = Path(root).expanduser()
    try:
        root_device = lookup(root_path).st_dev
    except OSError as exc:
        return _df_refusal_ledger("df_device_lookup_failed", str(exc))
    targets, discovery_refusals, skipped = _discover_class_targets(root_path)
    targets = _canonicalize_targets(targets)
    overlapping = _overlapping_classes(targets)
    devices_by_class, representatives, device_errors = _target_devices(
        targets, root_path, root_device, lookup, path_devices
    )
    filesystems, df_refusals = _collect_filesystems(representatives, run, timeout_seconds)
    if df_refusals:
        failure = _df_failure_ledger(df_refusals, filesystems, root_device)
        failure["refusals"].extend(discovery_refusals + list(device_errors.values()))
        failure["skipped"] = skipped
        return failure
    sizes, class_devices, foreign, shared_by_class, refusals = _measure_targets(
        targets, overlapping, run, timeout_seconds, devices_by_class, device_errors, root_device
    )
    refusals = discovery_refusals + refusals
    allocation_complete = not refusals and all(sizes.get(name) is not None for name in CLASS_NAMES if name != "other")
    return _summarize_collection(
        root_device,
        path_devices,
        filesystems,
        sizes,
        class_devices,
        foreign,
        shared_by_class,
        skipped,
        refusals,
        allocation_complete,
    )


def _positive_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number")
    return timeout


def _render_human(ledger: dict[str, object]) -> None:
    for device, filesystem in ledger.get("filesystems", {}).items():
        print(
            f"Filesystem {filesystem['mount']} (device {device}): "
            f"{filesystem['used_bytes']} of {filesystem['total_bytes']} bytes used; "
            f"{filesystem['available_bytes']} bytes available"
        )
    if ledger.get("binding_mount"):
        print(f"Binding mount: {ledger['binding_mount']}")
    print(f"Filesystem total: {ledger['fs_total_bytes']} bytes")
    print(f"Filesystem used: {ledger['fs_used_bytes']} bytes")
    print(f"Filesystem available: {ledger['fs_available_bytes']} bytes")
    print(f"Visible classes: {ledger['visible_bytes']} bytes")
    print(f"Inaccessible remainder: {ledger['inaccessible_bytes']} bytes")
    if ledger["inaccessible_negative"]:
        print("Warning: visible class sum exceeds filesystem used bytes", file=sys.stderr)
    for class_name, detail in ledger["classes"].items():
        state = "measured" if detail["measured"] else "unmeasured"
        if class_name in ledger.get("foreign_filesystems", {}):
            state += ", foreign filesystem"
        print(f"{class_name}: {detail['bytes']} bytes ({state})")
    for omission in ledger.get("skipped", []):
        print(json.dumps(omission, sort_keys=True), file=sys.stderr)
    for refusal in ledger["refusals"]:
        print(json.dumps(refusal, sort_keys=True), file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="~/.workbay", help="ledger root (default: ~/.workbay)")
    parser.add_argument("--json", action="store_true", help="write one JSON ledger object to stdout")
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_timeout,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"timeout for each df/du command (default: {DEFAULT_TIMEOUT_SECONDS:g})",
    )
    args = parser.parse_args(argv)
    ledger = collect(args.root, timeout_seconds=args.timeout_seconds)
    if args.json:
        print(json.dumps(ledger, sort_keys=True))
    else:
        _render_human(ledger)
    refusals = ledger["refusals"]
    if refusals and refusals[0]["type"].startswith("df_"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
