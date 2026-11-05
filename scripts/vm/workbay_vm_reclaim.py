#!/usr/bin/env python3
"""Release and reclaim owner-finished VM job directories using local facts only.

Scan is read-only. Apply requires a valid matching owner release, holds unknown
activity, archives bounded evidence, and renames before deleting. No TTL or force
path exists. Producers use allocate; owners mark legacy dirs with allocate --adopt.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import inspect
import io
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import tarfile
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

JOB_NAME = re.compile(r"workbay-.+-[0-9]{8}\Z")
EXCLUDED = {"workbay-sandbox", "workbay-control-reviews"}
FILE_CAP = 2 * 1024 * 1024
VERDICTS = (
    "skip_not_a_dir",
    "hold_unmarked",
    "hold_unit_active",
    "hold_locked",
    "hold_cwd",
    "hold_lender",
    "hold_not_released",
    "reclaimable",
)


class Refusal(Exception):
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(command, timeout=30):
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)


def state_root():
    return Path.home() / ".local/state/workbay/vm-reclaim"


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, data):
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_dir(path.parent)
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)


def read_regular(path):
    """Read a bounded regular file without following its final symlink."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > FILE_CAP:
            raise ValueError("not a regular file of at most 2 MB")
        data = stream.read(FILE_CAP + 1)
        if len(data) > FILE_CAP:
            raise ValueError("file exceeds 2 MB")
        return data


def manifest(path):
    value = json.loads(read_regular(path))
    if not isinstance(value, dict) or not isinstance(value.get("owner"), str) or not value["owner"].strip():
        raise ValueError("manifest needs a nonempty owner string")
    return value


def allocate(directory, owner, retention_class, task_ref=None, note=None, adopt=False):
    directory = Path(os.path.abspath(os.path.expanduser(directory)))
    if directory.name in EXCLUDED or not JOB_NAME.fullmatch(directory.name):
        raise Refusal("bad_name", "allocate target must have a VM job basename")
    if not owner.strip():
        raise Refusal("owner_missing", "owner must not be empty")
    try:
        parent_fd = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise Refusal("bad_parent", "parent must be an existing real directory") from exc
    try:
        # Serialize allocators without adding a lock file to legacy directories.
        fcntl.flock(parent_fd, fcntl.LOCK_EX)
        if adopt:
            try:
                mode = directory.lstat().st_mode
            except OSError as exc:
                raise Refusal("not_a_dir", "adopt target must be an existing real directory") from exc
            if not stat.S_ISDIR(mode):
                raise Refusal("not_a_dir", "adopt target must be an existing real directory")
            if os.path.lexists(directory / "RETENTION.json"):
                raise Refusal("already_marked", "existing RETENTION.json wins")
        else:
            try:
                os.mkdir(directory, 0o755)
            except FileExistsError as exc:
                raise Refusal("exists", "allocate target already exists") from exc
        data = dict(
            schema=1,
            owner=owner,
            retention_class=retention_class,
            task_ref=task_ref,
            note=note,
            created_at=utc_now(),
            host=socket.gethostname(),
            adopted=adopt,
        )
        # A crash between mkdir and publication leaves an empty unmarked dir;
        # scan safely holds it as hold_unmarked.
        try:
            atomic_json(directory / "RETENTION.json", data)
        except Exception as exc:
            if not adopt:
                try:
                    directory.rmdir()  # Only remove our new directory if still empty.
                except OSError:
                    pass
            raise Refusal("retention_write_failed", str(exc)) from exc
    finally:
        os.close(parent_fd)
    return {"outcome": "adopted" if adopt else "allocated", "basename": directory.name, "path": str(directory), **data}


def release(directory, owner, reason, evidence_copied_to=None):
    directory = Path(directory).expanduser().absolute()
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise Refusal("not_a_dir", "release target must be a real directory")
    try:
        retention = manifest(directory / "RETENTION.json")
    except FileNotFoundError as exc:
        raise Refusal("retention_missing", "RETENTION.json is required") from exc
    except (OSError, ValueError) as exc:
        raise Refusal("retention_invalid", str(exc)) from exc
    if "no_reap" in retention and not isinstance(retention["no_reap"], bool):
        raise Refusal("retention_invalid", "no_reap must be boolean")
    if retention.get("no_reap") is True:
        raise Refusal("retention_pinned", "RETENTION.json pins this directory")
    if retention["owner"] != owner:
        raise Refusal("owner_mismatch", "--owner differs from RETENTION.json owner")
    if not reason.strip():
        raise Refusal("reason_missing", "release reason must not be empty")
    data = dict(
        schema=1,
        owner=owner,
        reason=reason,
        released_at=utc_now(),
        evidence_copied_to=evidence_copied_to,
        host=socket.gethostname(),
    )
    atomic_json(directory / "RELEASE.json", data)
    return {"outcome": "released", "basename": directory.name, **data}


def inside(path, directory):
    return path == directory or directory in path.parents


def observed_lock_busy(info):
    """Observe flock holders through procfs without acquiring a job lock."""
    for line in Path("/proc/locks").read_text(encoding="utf-8").splitlines():
        fields = line.split()
        # Waiting lock requests are not holders.
        if len(fields) < 6 or fields[1] != "FLOCK":
            continue
        major, minor, inode = fields[5].split(":")
        if (int(major, 16), int(minor, 16), int(inode)) == (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino):
            return True
    return False


def activity(directory, read_only=False):
    """Return the first unit, lock or procfs hold, in the required order."""
    try:
        result = run(["systemctl", "--user", "is-active", directory.name], timeout=10)
        unit_state = result.stdout.strip()
        if result.returncode == 0:
            return "hold_unit_active", "unit_active_or_unknown"
        if result.returncode == 3:
            if unit_state not in {"inactive", "failed"}:
                return "hold_unit_active", "unit_active_or_unknown"
        else:
            # --collect removes finished transient units. Verify absence with a
            # successful manager query; an unavailable bus still denies.
            shown = run(
                ["systemctl", "--user", "show", "-p", "LoadState", "-p", "ActiveState", directory.name],
                timeout=10,
            )
            lines = shown.stdout.splitlines()
            properties = dict(line.split("=", 1) for line in lines)
            if (
                shown.returncode != 0
                or len(lines) != 2
                or properties.get("LoadState") != "not-found"
                or properties.get("ActiveState") not in {"inactive", ""}
            ):
                return "hold_unit_active", "unit_active_or_unknown"
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return "hold_unit_active", "unit_check_failed"
    try:
        fd = os.open(directory / "job.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        pass
    except OSError:
        return "hold_locked", "lock_unreadable"
    else:
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                return "hold_locked", "lock_not_regular"
            if read_only:
                try:
                    if observed_lock_busy(os.fstat(fd)):
                        return "hold_locked", "lock_busy"
                except (OSError, ValueError):
                    return "hold_locked", "lock_unreadable"
            else:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    return "hold_locked", "lock_busy"
        finally:
            os.close(fd)
    try:
        processes = list(Path("/proc").iterdir())
    except OSError:
        return "hold_cwd", "proc_unreadable"
    for process in processes:
        if not process.name.isdigit():
            continue
        try:
            cwd = (process / "cwd").resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if inside(cwd, directory):
            return "hold_cwd", "process_cwd_inside"
    return None


def lenders(directory, root):
    """Inspect every root/home child, including non-job borrower names.

    Git resolves relative alternates against the borrower's objects directory.
    An unreadable census holds rather than proving there are no borrowers.
    """
    borrowers = set()
    for parent in {root, Path.home().resolve()}:
        for child in parent.iterdir():
            if child == directory:
                continue
            alternate = child / "repo/.git/objects/info/alternates"
            try:
                lines = alternate.read_text(encoding="utf-8").splitlines()
            except (FileNotFoundError, NotADirectoryError):
                continue
            for line in lines:
                if not line:
                    continue
                target = Path(line)
                if not target.is_absolute():
                    target = alternate.parent.parent / target
                if inside(target.resolve(), directory):
                    borrowers.add(child.name)
    return sorted(borrowers)


def released_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("released_at must include a UTC offset")
    return parsed.timestamp()


def classify(directory, root, read_only=False):
    row = {"basename": directory.name, "path": str(directory), "lends_to": []}

    def verdict(kind, reason):
        return {**row, "verdict": kind, "reason": reason}

    try:
        if not stat.S_ISDIR(directory.lstat().st_mode):
            return verdict("skip_not_a_dir", "not_a_real_directory")
    except OSError:
        return verdict("skip_not_a_dir", "directory_unavailable")
    try:
        retention = manifest(directory / "RETENTION.json")
    except FileNotFoundError:
        return verdict("hold_unmarked", "retention_missing")
    except (OSError, ValueError):
        return verdict("hold_unmarked", "retention_invalid")
    row["owner"] = retention["owner"]
    if "no_reap" in retention and not isinstance(retention["no_reap"], bool):
        return verdict("hold_unmarked", "retention_invalid")
    if retention.get("no_reap") is True:
        return verdict("hold_not_released", "retention_pinned")
    hold = activity(directory, read_only=True) if read_only else activity(directory)
    if hold:
        return verdict(*hold)
    try:
        row["lends_to"] = lenders(directory, root)
    except (OSError, ValueError, RuntimeError):
        return verdict("hold_lender", "alternates_unreadable")
    if row["lends_to"]:
        return verdict("hold_lender", "git_objects_borrowed")
    try:
        released = manifest(directory / "RELEASE.json")
    except FileNotFoundError:
        return verdict("hold_not_released", "release_missing")
    except (OSError, ValueError):
        return verdict("hold_not_released", "release_invalid")
    if released["owner"] != retention["owner"]:
        return verdict("hold_not_released", "release_owner_mismatch")
    try:
        if released.get("schema") != 1:
            raise ValueError("invalid release schema")
        released_time(released["released_at"])
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        return verdict("hold_not_released", "release_invalid")
    row["released_at"] = released["released_at"]
    return verdict("reclaimable", "owner_released")


def scan(root=None, read_only=False):
    root = Path(root or Path.home()).expanduser().resolve()
    rows = [
        classify(child, root, read_only=True) if read_only else classify(child, root)
        for child in sorted(root.iterdir())
        if child.name not in EXCLUDED and not child.name.endswith(".git") and JOB_NAME.fullmatch(child.name)
    ]
    counts = dict.fromkeys(VERDICTS, 0)
    counts.update(Counter(row["verdict"] for row in rows))
    return {"root": str(root), "entries": rows, "counts": counts}


def measure(command):
    try:
        result = run(command)
        if result.returncode:
            raise ValueError(result.stderr.strip())
        return int(result.stdout.strip().splitlines()[-1].split()[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError) as exc:
        raise Refusal("measurement_failed", f"{command[0]}: {exc}") from exc


def disk_free(root):
    return measure(["df", "-B1", "--output=avail", "--", str(root)])


def size_bytes(directory):
    return measure(["du", "-s", "-B1", "--", str(directory)])


def status(root=None, stale_after_seconds=5400, floor_bytes=10 * 1024**3):
    """Report observed state only; never create state, acquire locks or reclaim."""
    root = Path(root or Path.home()).expanduser().resolve()
    free = disk_free(root)
    scanned = scan(root, read_only=True)
    alarms = []
    last_run, age = None, None
    try:
        value = json.loads(read_regular(state_root() / "last-run.json"))
        if (
            not isinstance(value, dict)
            or value.get("schema") != 1
            or value.get("outcome") not in {"ok", "error"}
            or value.get("trigger") not in {"timer", "headroom"}
            or not isinstance(value.get("counts"), dict)
            or any(not isinstance(value.get(key), int) for key in ("reclaimed", "recovered", "bytes_freed"))
            or any(
                key not in value or (value[key] is not None and not isinstance(value[key], int))
                for key in ("df_free_before", "df_free_after")
            )
            or (value.get("outcome") == "ok" and value.get("error", True) is not None)
            or (
                value.get("outcome") == "error"
                and (
                    not isinstance(value.get("error"), dict)
                    or not isinstance(value["error"].get("type"), str)
                    or not isinstance(value["error"].get("message"), str)
                )
            )
        ):
            raise ValueError("invalid last-run receipt")
        released_time(value["started_at"])
        age = max(0, released_time(utc_now()) - released_time(value["finished_at"]))
        last_run = value
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
        alarms.append("last_run_missing")
    if last_run is not None:
        if age > stale_after_seconds:
            alarms.append("reaper_stale")
        if last_run["outcome"] == "error":
            alarms.append("last_run_error")
    if free < floor_bytes:
        alarms.append("free_below_floor")
    unmarked = [row for row in scanned["entries"] if row["verdict"] == "hold_unmarked"]
    if unmarked:
        alarms.append("unmarked_present")
    try:
        unmarked_bytes = sum(size_bytes(Path(row["path"])) for row in unmarked)
    except Refusal:
        unmarked_bytes = None
        alarms.append("unmarked_size_unmeasured")
    return {
        "schema": 1,
        "root": str(root),
        "df_free_bytes": free,
        "floor_bytes": floor_bytes,
        "last_run": last_run,
        "last_run_age_seconds": age,
        "counts": scanned["counts"],
        "unmarked": {
            "count": len(unmarked),
            "bytes": unmarked_bytes,
            "sample": [row["basename"] for row in unmarked[:10]],
        },
        "alarms": alarms,
    }


def archive_evidence(directory, archive_root, cap):
    if inside(archive_root.resolve(), directory):
        raise Refusal("archive_inside_job", "archive root must be outside the directory being reclaimed")
    archive_root.mkdir(parents=True, exist_ok=True)
    destination = archive_root / f"{directory.name}.tgz"
    skipped, total = [], 0
    candidates = [directory / "RETENTION.json", directory / "RELEASE.json"]
    out = directory / "out"
    try:
        mode = out.lstat().st_mode
    except FileNotFoundError:
        mode = 0
    if stat.S_ISDIR(mode):
        candidates.extend(p for p in sorted(out.iterdir()) if p.suffix in {".json", ".md", ".log"})
    elif mode:
        skipped.append({"path": "out", "reason": "not_a_real_directory"})
    fd, temporary = tempfile.mkstemp(prefix=f".{directory.name}-", suffix=".tgz", dir=archive_root)
    os.close(fd)
    try:
        with tarfile.open(temporary, "w:gz", dereference=False) as archive:
            for path in candidates:
                relative = str(path.relative_to(directory))
                required = path.parent == directory
                try:
                    data = read_regular(path)
                except (OSError, ValueError) as exc:
                    if required:
                        raise Refusal("evidence_required_unreadable", relative) from exc
                    skipped.append({"path": relative, "reason": "oversize_or_not_regular_or_unreadable"})
                    continue
                if total + len(data) > cap:
                    if required:
                        raise Refusal("evidence_cap_too_small", "cap cannot hold required manifests")
                    skipped.append({"path": relative, "reason": "total_cap"})
                    continue
                info = tarfile.TarInfo(relative)
                info.size = len(data)
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(data))
                total += len(data)
        with open(temporary, "rb") as stream:
            digest = (
                hashlib.file_digest(stream, "sha256").hexdigest()
                if hasattr(hashlib, "file_digest")
                else hashlib.sha256(stream.read()).hexdigest()
            )
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        fsync_dir(archive_root)
    finally:
        if os.path.lexists(temporary):
            os.unlink(temporary)
    return {"archive": str(destination), "sha256": digest, "skipped_evidence": skipped}


def append_receipt(state, receipt):
    path = state / "receipts.jsonl"
    # Pending intents are replayed after a crash, including a crash after append.
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    if json.loads(line).get("reclaim_id") == receipt["reclaim_id"]:
                        return
                except ValueError:
                    continue
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({**receipt, "reclaimed_at": utc_now()}, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def finish_delete(target, state, intent, receipt):
    retry_failed = False

    def retry_permission(function, path, error):
        nonlocal retry_failed
        path = Path(os.path.abspath(path))
        if retry_failed or not isinstance(error, PermissionError) or not inside(path, target):
            raise error
        # Some rmtree versions report callback errors through onexc again.
        retry_failed = True
        parts = path.relative_to(target).parts
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        fd = os.open(target, flags)
        try:
            # Walk from the tombstone by directory fd, never through a symlink.
            # Only the failed operation's parent and directory itself get u+wx.
            for part in parts[:-1]:
                if not stat.S_ISDIR(os.lstat(part, dir_fd=fd).st_mode):
                    raise error
                child = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            os.fchmod(fd, os.fstat(fd).st_mode | stat.S_IWUSR | stat.S_IXUSR)
            if parts and stat.S_ISDIR(os.lstat(parts[-1], dir_fd=fd).st_mode):
                child = os.open(parts[-1], flags, dir_fd=fd)
                try:
                    os.fchmod(child, os.fstat(child).st_mode | stat.S_IWUSR | stat.S_IXUSR)
                finally:
                    os.close(child)
        finally:
            os.close(fd)
        # Retry once, outside rmtree's callback machinery: another failure escapes.
        function(str(path))
        retry_failed = False

    if target.exists():
        if not stat.S_ISDIR(target.lstat().st_mode):
            raise Refusal("recovery_not_a_dir", str(target))
        if "onexc" in inspect.signature(shutil.rmtree).parameters:
            shutil.rmtree(target, onexc=retry_permission)
        else:
            shutil.rmtree(target, onerror=lambda function, path, info: retry_permission(function, path, info[1]))
        fsync_dir(target.parent)
    append_receipt(state, receipt)
    intent.unlink(missing_ok=True)
    fsync_dir(state)


def recover(root, state, summary, limit):
    """Finish tombstones before scanning jobs; durable intents preserve receipts."""
    intents = {}
    for intent in state.glob("pending-*.json"):
        data = json.loads(intent.read_text(encoding="utf-8"))
        if data["root"] == str(root):
            intents[root / data["tombstone"]] = (intent, data)
    targets = sorted(set(root.glob(".reclaiming-*")) | set(intents))
    remaining = []
    for target in targets:
        if summary["recovered"] >= limit:
            remaining.append(target.name)
            continue
        if target.is_symlink():
            continue
        pending = intents.get(target)
        if pending:
            intent, receipt = pending
            if not os.path.lexists(target) and (root / receipt["basename"]).exists():
                # Intent persisted but rename never happened; scan the live job.
                intent.unlink()
                continue
        else:
            if not target.is_dir():
                continue
            receipt = {
                "root": str(root),
                "tombstone": target.name,
                "basename": target.name.removeprefix(".reclaiming-").rsplit("-", 1)[0],
                "owner": None,
                "released_at": None,
                "bytes_freed": size_bytes(target),
                "archive": None,
                "sha256": None,
                "skipped_evidence": [],
                "reclaim_id": f"{root}/{target.name}",
            }
            intent = state / f"pending-{hashlib.sha256(receipt['reclaim_id'].encode()).hexdigest()}.json"
            atomic_json(intent, receipt)
        finish_delete(target, state, intent, receipt)
        summary["recovered"] += 1
        summary["bytes_freed"] += receipt["bytes_freed"]
    return remaining


def write_last_run(state, summary, trigger, started_at, error=None):
    atomic_json(
        state / "last-run.json",
        {
            "schema": 1,
            "trigger": trigger,
            "started_at": started_at,
            "finished_at": utc_now(),
            **{
                key: summary[key]
                for key in (
                    "outcome",
                    "df_free_before",
                    "df_free_after",
                    "counts",
                    "reclaimed",
                    "recovered",
                    "bytes_freed",
                )
            },
            "error": error,
        },
    )


def apply(root=None, max_dirs=10, archive_root=None, evidence_cap_mb=20, trigger="timer"):
    root = Path(root or Path.home()).expanduser().resolve()
    archive_root = Path(archive_root or state_root() / "archive").expanduser().absolute()
    state = state_root()
    state.mkdir(parents=True, exist_ok=True)
    summary = {
        "outcome": "ok",
        "counts": dict.fromkeys(VERDICTS, 0),
        "reclaimed": 0,
        "recovered": 0,
        "bytes_freed": 0,
        "df_free_before": None,
        "df_free_after": None,
        "remaining": [],
        "held_on_recheck": [],
    }
    with (state / "apply.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {**summary, "outcome": "busy"}
        started_at = utc_now()
        try:
            summary["df_free_before"] = disk_free(root)
            recovery_remaining = recover(root, state, summary, max_dirs)
            scanned = scan(root)
            summary["counts"] = scanned["counts"]
            eligible = sorted(
                (r for r in scanned["entries"] if r["verdict"] == "reclaimable"),
                key=lambda r: (released_time(r["released_at"]), r["basename"]),
            )
            available = max_dirs - summary["recovered"]
            summary["remaining"] = recovery_remaining + [r["basename"] for r in eligible[available:]]
            for row in eligible[:available]:
                directory = root / row["basename"]
                identity = directory.lstat()
                checked = classify(directory, root)
                if checked["verdict"] != "reclaimable":
                    summary["held_on_recheck"].append(checked)
                    continue
                evidence = archive_evidence(directory, archive_root, int(evidence_cap_mb * 1024 * 1024))
                allocated = size_bytes(directory)
                # Archive/du can take time: recheck local activity and owner authority
                # again immediately before making the job disappear from its live name.
                checked = classify(directory, root)
                current = directory.lstat()
                if checked["verdict"] != "reclaimable" or (identity.st_dev, identity.st_ino) != (
                    current.st_dev,
                    current.st_ino,
                ):
                    summary["held_on_recheck"].append(checked)
                    continue
                tomb = root / f".reclaiming-{directory.name}-{os.getpid()}"
                if os.path.lexists(tomb):
                    raise Refusal("tombstone_exists", str(tomb))
                reclaim_id = f"{root}/{tomb.name}/{utc_now()}"
                receipt = {
                    "root": str(root),
                    "tombstone": tomb.name,
                    "basename": directory.name,
                    "owner": checked["owner"],
                    "released_at": checked["released_at"],
                    "bytes_freed": allocated,
                    "reclaim_id": reclaim_id,
                    **evidence,
                }
                intent = state / f"pending-{hashlib.sha256(reclaim_id.encode()).hexdigest()}.json"
                atomic_json(intent, receipt)
                os.rename(directory, tomb)
                fsync_dir(root)
                finish_delete(tomb, state, intent, receipt)
                summary["reclaimed"] += 1
                summary["bytes_freed"] += allocated
            summary["df_free_after"] = disk_free(root)
            write_last_run(state, summary, trigger, started_at)
        except (Refusal, OSError, ValueError) as exc:
            summary["outcome"] = "error"
            error = {"type": getattr(exc, "kind", "io_error"), "message": str(exc)}
            try:
                write_last_run(state, summary, trigger, started_at, error)
            except OSError:
                # A full/unwritable state filesystem must not mask the pass error.
                pass
            raise
    return summary


def nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    allocate_parser = commands.add_parser("allocate", help="create an owned job or adopt an unmarked legacy directory")
    allocate_parser.add_argument("directory")
    allocate_parser.add_argument("--owner", required=True)
    allocate_parser.add_argument(
        "--retention-class", required=True, choices=["lane", "gate", "qa", "recovery", "release", "scratch"]
    )
    allocate_parser.add_argument("--task-ref")
    allocate_parser.add_argument("--note")
    allocate_parser.add_argument("--adopt", action="store_true")
    release_parser = commands.add_parser("release", help="publish an owner release; deletes nothing")
    release_parser.add_argument("directory")
    release_parser.add_argument("--owner", required=True)
    release_parser.add_argument("--reason", required=True)
    release_parser.add_argument("--evidence-copied-to")
    scan_parser = commands.add_parser("scan", help="read-only classification")
    scan_parser.add_argument("--root", default=str(Path.home()))
    scan_parser.add_argument("--json", action="store_true")
    status_parser = commands.add_parser("status", help="read-only reclaim health and headroom gauge")
    status_parser.add_argument("--root", default=str(Path.home()))
    status_parser.add_argument("--stale-after-seconds", type=nonnegative, default=5400)
    status_parser.add_argument("--floor-bytes", type=nonnegative, default=10 * 1024**3)
    status_parser.add_argument("--json", action="store_true")
    apply_parser = commands.add_parser("apply", help="archive and reclaim released jobs")
    apply_parser.add_argument("--root", default=str(Path.home()))
    apply_parser.add_argument("--max", type=nonnegative, default=10)
    apply_parser.add_argument("--archive-root")
    apply_parser.add_argument("--evidence-cap-mb", type=nonnegative, default=20)
    apply_parser.add_argument("--if-free-below", type=nonnegative)
    args = parser.parse_args(argv)
    try:
        if args.command == "allocate":
            output = allocate(args.directory, args.owner, args.retention_class, args.task_ref, args.note, args.adopt)
        elif args.command == "release":
            output = release(args.directory, args.owner, args.reason, args.evidence_copied_to)
        elif args.command == "scan":
            output = scan(args.root)
            if not args.json:
                for row in output["entries"]:
                    print(f"{row['basename']}: {row['verdict']} ({row['reason']})")
                print(json.dumps(output["counts"], sort_keys=True))
                return 0
        elif args.command == "status":
            output = status(args.root, args.stale_after_seconds, args.floor_bytes)
            if not args.json:
                for alarm in output["alarms"]:
                    print(alarm)
                print(
                    f"{output['root']}: free_bytes={output['df_free_bytes']} "
                    f"floor_bytes={output['floor_bytes']} unmarked={output['unmarked']['count']} "
                    f"last_run_age_seconds={output['last_run_age_seconds']}"
                )
                return 0
        else:
            trigger = "timer"
            if args.if_free_below is not None:
                free = disk_free(Path(args.root).expanduser().resolve())
                if free >= args.if_free_below:
                    print(
                        json.dumps(
                            {
                                "outcome": "skipped_headroom_ok",
                                "df_free_bytes": free,
                                "floor_bytes": args.if_free_below,
                            },
                            sort_keys=True,
                        )
                    )
                    return 0
                trigger = "headroom"
            output = apply(args.root, args.max, args.archive_root, args.evidence_cap_mb, trigger=trigger)
    except (Refusal, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"outcome": "error", "error": {"type": getattr(exc, "kind", "io_error"), "message": str(exc)}},
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
