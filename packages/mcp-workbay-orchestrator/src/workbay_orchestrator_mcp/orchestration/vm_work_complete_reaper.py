"""Versioned work-complete contract and a self-contained VM-side reclaimer.

The runtime block is the single authored source for both the importable
validator and the rendered stdlib script. No installed WorkBay code is needed
on the VM. Completion is a producer assertion of external product custody,
never an age or disk-pressure inference.
"""

from __future__ import annotations

# BEGIN VM RUNTIME
import argparse
import base64
import contextlib
import fcntl
import glob
import hashlib  # noqa: F401 - used by the embedded producer key function
import json
import os
import pwd
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

WORK_COMPLETE_SCHEMA = "workbay/work-complete@1"
WORK_COMPLETE_FIELDS = {
    "schema": str,
    "owner": str,
    "task_ref": (str, type(None)),
    "lane_key": (str, type(None)),
    "completed_at": str,
    "product": dict,
    "note": (str, type(None)),
}
PRODUCT_FIELDS = {"kind", "ref", "sha256"}
PRODUCT_KINDS = {"git_commit", "bundle", "none"}
RECEIPT_SCHEMA = "workbay/vm-reaper-receipt@1"
MARKER_NAME = ".workbay-work-complete"
DEFAULT_ROOTS = ("~/workbay-sandbox/*", "~/workbay-*")
RECEIPT_PATH = "~/.local/state/workbay/vm-reaper/last.json"
# Ten minutes bounds accumulation independently of dispatch and leaves room
# for ordinary bursts; this is not a target to fill the disk (PERF-13).
DEFAULT_INTERVAL_SECONDS = 600
# Leave time to publish a receipt before systemd's outer execution bound.
RUN_BUDGET_SECONDS = 240
SERVICE_TIMEOUT_SECONDS = 300
GIT_TIMEOUT_SECONDS = 5
SYSTEMCTL_TIMEOUT_SECONDS = 10
BOOT_DELAY_SECONDS = 30
MAX_JSON_BYTES = 65_536
UNMARKED_REPORT_LIMIT = 50
KERNEL_THREAD_FLAG = 0x00200000
VERDICTS = (
    "pinned",
    "unmarked",
    "unknown",
    "in_use",
    "unverified_in_use",
    "lock_held",
    "custody_mismatch",
    "would_reclaim",
    "reclaimed",
    "error",
)


def _utc_timestamp(value):
    if (
        not isinstance(value, str)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)", value) is None
    ):
        return False
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).utcoffset().total_seconds() == 0
    except ValueError:
        return False


def validate_work_complete(obj) -> tuple[bool, str]:
    """Validate @1 exactly; an unrecognized or malformed claim grants nothing."""
    if not isinstance(obj, dict) or set(obj) != set(WORK_COMPLETE_FIELDS):
        return False, "expected exactly the work-complete fields"
    for field, types in WORK_COMPLETE_FIELDS.items():
        if not isinstance(obj[field], types):
            return False, f"invalid {field} type"
    if obj["schema"] != WORK_COMPLETE_SCHEMA:
        return False, "unknown schema"
    if not obj["owner"].strip():
        return False, "owner is empty"
    if not _utc_timestamp(obj["completed_at"]):
        return False, "completed_at must be RFC3339 UTC"
    product = obj["product"]
    if set(product) != PRODUCT_FIELDS:
        return False, "invalid product fields"
    kind = product["kind"]
    if not isinstance(kind, str) or kind not in PRODUCT_KINDS:
        return False, "unknown product kind"
    if any(product[key] is not None and not isinstance(product[key], str) for key in ("ref", "sha256")):
        return False, "invalid product ref or sha256 type"
    if kind == "git_commit" and (
        not isinstance(product["ref"], str) or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", product["ref"]) is None
    ):
        return False, "git_commit requires a full immutable commit ID"
    if kind == "bundle" and (not product["ref"] or not product["sha256"]):
        return False, "bundle requires external ref and sha256"
    if product["sha256"] is not None and re.fullmatch(r"[0-9a-f]{64}", product["sha256"]) is None:
        return False, "invalid sha256"
    if kind == "none" and (product["ref"] is not None or product["sha256"] is not None):
        return False, "none product must have null ref and sha256"
    return True, "ok"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _unique_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate JSON keys")
    return result


def _reject_constant(value):
    raise ValueError(f"invalid JSON constant: {value}")


def _read_json(path):
    # No blocking special files or symlink redirection in authorization data.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("metadata is not a regular file")
        raw = stream.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("metadata exceeds size limit")
    return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)


def _owner(path, *objects):
    # Ownership is diagnostic only. Unreadable metadata falls through to the
    # next source, never to authorization; marker ownership takes precedence.
    metadata = []
    for name in (MARKER_NAME, "RETENTION.json"):
        try:
            metadata.append(_read_json(path / name))
        except (OSError, ValueError):
            continue
    for obj in (*metadata, *objects):
        if isinstance(obj, dict) and isinstance(obj.get("owner"), str) and obj["owner"].strip():
            return obj["owner"]
    try:
        return pwd.getpwuid(path.lstat().st_uid).pw_name
    except (OSError, KeyError):
        return "unknown"


def _completion_gate(path):
    """Return (typed verdict, completion payload, owner, reason); pins first."""
    retention = None
    if os.path.lexists(path / ".workbay-pin"):
        return "pinned", None, _owner(path), ".workbay-pin"
    if os.path.lexists(path / "RETENTION.json"):
        try:
            retention = _read_json(path / "RETENTION.json")
            if not isinstance(retention, dict):
                raise ValueError("retention must be an object")
            if retention.get("no_reap") is True:
                return "pinned", None, _owner(path, retention), "RETENTION.json no_reap"
            if "no_reap" in retention and not isinstance(retention["no_reap"], bool):
                raise ValueError("no_reap must be boolean")
        except (OSError, ValueError) as exc:
            return "unknown", None, _owner(path, retention), f"retention unreadable: {exc}"
    marker_path = path / MARKER_NAME
    if os.path.lexists(marker_path):
        body = None
        try:
            body = _read_json(marker_path)
            ok, reason = validate_work_complete(body)
            return ("complete" if ok else "unknown"), body, _owner(path, body, retention), reason
        except (OSError, ValueError) as exc:
            return "unknown", None, _owner(path, body, retention), f"marker unreadable: {exc}"
    lane_marker = path / ".workbay-lane-sandbox"
    if os.path.lexists(lane_marker):
        merged_path = path.parent / ".merged" / (path.name + ".json")
        if not os.path.lexists(merged_path):
            return "unmarked", None, _owner(path, retention), "no terminal marker"
        try:
            if not stat.S_ISREG(lane_marker.lstat().st_mode) or (path.parent / ".merged").is_symlink():
                raise ValueError("unsafe lane metadata")
            body = _read_json(merged_path)
            if not isinstance(body, dict) or set(body) != {"branch", "receipt_sha", "landed_at"}:
                raise ValueError("invalid merged marker fields")
            branch = body["branch"]
            if not isinstance(branch, str) or not branch.strip() or "\n" in branch or "\r" in branch:
                raise ValueError("invalid merged branch")
            if _derive_lane_key(branch) != path.name:
                raise ValueError("merged marker key mismatch")
            receipt = body["receipt_sha"]
            if not isinstance(receipt, str) or (receipt and re.fullmatch(r"[0-9a-f]{7,40}", receipt) is None):
                raise ValueError("invalid merged receipt")
            if not _utc_timestamp(body["landed_at"]):
                raise ValueError("invalid merged timestamp")
            if merged_path.lstat().st_mtime_ns < lane_marker.lstat().st_mtime_ns:
                raise ValueError("terminal marker predates this dispatch")
            return "complete", body, _owner(path, retention), "merged"
        except (OSError, ValueError) as exc:
            return "unknown", None, _owner(path, retention), f"merged marker unreadable: {exc}"
    return "unmarked", None, _owner(path, retention), "no completion marker"


def _check_budget(deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError("run budget exhausted; remaining stock deferred")


def _under(target, path):
    target = target.removesuffix(" (deleted)")
    return target == str(path) or target.startswith(str(path) + os.sep)


def _has_no_userspace(entry):
    # Same stat state/flags proof as remote_sandbox_reap's reference census.
    # Kernel threads and zombies have neither a userspace cwd nor open files;
    # a missing or malformed stat record grants no such exemption.
    try:
        with (entry / "stat").open() as stream:
            record = stream.read(MAX_JSON_BYTES + 1)
        if len(record) > MAX_JSON_BYTES:
            return False
        fields = record.rsplit(") ", 1)[1].split()
        flags = int(fields[6])
        if flags < 0:
            return False
        if flags & KERNEL_THREAD_FLAG:
            return True
        # A zombie group leader can still have live sibling threads. Only a
        # sole zombie task proves this entire PID has no userspace users.
        return fields[0] == "Z" and {task.name for task in (entry / "task").iterdir()} == {entry.name}
    except (OSError, ValueError, IndexError):
        return False


def _proc_view_error(proc_root):
    mountinfo = proc_root / "self" / "mountinfo"
    # An explicit private proc fixture can omit mountinfo. The deployed default
    # must prove that procfs does not omit otherwise invisible PIDs (hidepid).
    if proc_root != Path("/proc") and not mountinfo.exists():
        return None
    try:
        with mountinfo.open() as stream:
            record = stream.read(MAX_JSON_BYTES + 1)
        if len(record) > MAX_JSON_BYTES:
            return "proc mount view exceeds verification bound"
        views = [line for line in record.splitlines() if " - proc " in line and line.split()[4] == str(proc_root)]
        if views and all(re.search(r"hidepid=(?!0(?:,|\s|$))", line) is None for line in views):
            return None
        return "proc mount view can hide PIDs or is unverified"
    except (OSError, ValueError, IndexError) as exc:
        return f"proc mount view unreadable: {exc}"


def _in_use(path, proc_root, deadline):
    """Every inaccessible potentially live process is a hold, never absence."""
    visibility_error = _proc_view_error(proc_root)
    if visibility_error:
        return "unverified_in_use", visibility_error
    try:
        entries = list(proc_root.iterdir())
    except OSError as exc:
        return "unverified_in_use", f"proc enumeration failed: {exc}"
    skipped_pids = {os.getpid(), os.getppid()}
    uncertainty = None
    for entry in entries:
        _check_budget(deadline)
        if not entry.name.isdigit() or int(entry.name) in skipped_pids:
            continue
        if _has_no_userspace(entry):
            continue
        try:
            for name in ("cwd", "root"):
                if _under(os.readlink(entry / name), path):
                    return "in_use", f"pid {entry.name} {name}"
            for fd in (entry / "fd").iterdir():
                try:
                    target = os.readlink(fd)
                except FileNotFoundError:
                    # A descriptor closed during enumeration cannot hold use.
                    continue
                if _under(target, path):
                    return "in_use", f"pid {entry.name} fd"
        except OSError as exc:
            # A vanished process is harmless; a still-present but inaccessible
            # one could hide a cwd, chroot or descriptor in the candidate.
            if not entry.exists():
                continue
            uncertainty = f"pid {entry.name} proc unreadable: {exc}"
    if uncertainty:
        return "unverified_in_use", uncertainty
    return None, "verified idle"


def _tree_bytes(path, deadline):
    total = 0
    for root, dirs, files in os.walk(path, followlinks=False, onerror=_raise_walk_error):
        _check_budget(deadline)
        for name in dirs + files:
            info = (Path(root) / name).lstat()
            if not stat.S_ISDIR(info.st_mode):
                total += info.st_size
    return total


def _raise_walk_error(error):
    raise error


def _minimal_environment():
    # Resolve HOME from the account database, never from the invoking shell.
    # No PWD, Python/loader knobs, XDG config paths or caller Git settings.
    return {"PATH": "/usr/bin:/bin", "HOME": pwd.getpwuid(os.getuid()).pw_dir, "LANG": "C", "LC_ALL": "C"}


def _custody(path, body):
    product = body.get("product", {})
    if product.get("kind") != "git_commit" or not os.path.lexists(path / ".git"):
        return True, "producer attests product safe elsewhere"
    # Candidate paths are never passed in argv or environment, even to Git.
    env = _minimal_environment()
    env.update(
        GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null"
    )
    args = ["git", "--work-tree=.", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false"]
    try:
        status_result = subprocess.run(
            [*args, "status", "--porcelain", "--untracked-files=all"],
            cwd=path,
            env=env,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
        head_result = subprocess.run(
            [*args, "rev-parse", "--verify", "HEAD"],
            cwd=path,
            env=env,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"git custody unverified: {exc}"
    if (
        status_result.returncode
        or head_result.returncode
        or status_result.stdout
        or head_result.stdout.strip() != product["ref"]
    ):
        return False, "dirty tree, unreadable repository, or HEAD differs from product.ref"
    return True, "git custody verified"


def _reap_candidate(path, proc_root, dry_run, deadline):
    row = {"path": str(path), "owner": _owner(path), "bytes": 0}
    try:
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or path.resolve() != path:
            return {**row, "verdict": "unknown", "reason": "candidate is redirected or not a directory"}
        gate, body, owner, reason = _completion_gate(path)
        row["owner"] = owner
        if gate == "pinned":
            return {**row, "verdict": gate, "reason": reason}
        try:
            row["bytes"] = _tree_bytes(path, deadline)
        except OSError as exc:
            if gate in ("unmarked", "unknown"):
                return {**row, "verdict": gate, "reason": reason, "error": f"size unverified: {exc}"}
            raise
        if gate != "complete":
            return {**row, "verdict": gate, "reason": reason}
        busy, reason = _in_use(path, proc_root, deadline)
        if busy:
            return {**row, "verdict": busy, "reason": reason}
        with contextlib.ExitStack() as stack:
            if os.path.lexists(path / ".workbay-lane-sandbox"):
                lock = path.parent / (".lane-lock-" + path.name)
                fd = os.open(lock, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
                stack.callback(os.close, fd)
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    return {**row, "verdict": "unknown", "reason": "lane lock is not regular"}
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return {**row, "verdict": "lock_held", "reason": "dispatch holds lane lock"}
            # Hold the shared lane lock through all rechecks and deletion.
            new_gate, new_body, owner, reason = _completion_gate(path)
            if new_gate != "complete":
                return {**row, "owner": owner, "verdict": new_gate, "reason": reason}
            if new_body != body:
                return {**row, "verdict": "unknown", "reason": "completion marker changed"}
            ok, reason = _custody(path, body)
            if not ok:
                return {**row, "verdict": "custody_mismatch", "reason": reason}
            busy, reason = _in_use(path, proc_root, deadline)
            if busy:
                return {**row, "verdict": busy, "reason": reason}
            _check_budget(deadline)
            current = path.lstat()
            if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
                return {**row, "verdict": "unknown", "reason": "candidate identity changed"}
            if dry_run:
                return {**row, "verdict": "would_reclaim", "reason": "all checks passed"}
            if not shutil.rmtree.avoids_symlink_attacks or sys.version_info < (3, 11):
                return {**row, "verdict": "unknown", "reason": "safe rmtree unavailable"}
            # Fence deletion away from the public candidate name. Opening the
            # checked inode alone does not bind a later path-based rmtree to it.
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            try:
                parent_fd = os.open(path.parent, flags)
                stack.callback(os.close, parent_fd)
                candidate_fd = os.open(path.name, flags, dir_fd=parent_fd)
                stack.callback(os.close, candidate_fd)
                opened = os.fstat(candidate_fd)
            except OSError as exc:
                return {**row, "verdict": "unknown", "reason": "delete_identity_unverified", "error": str(exc)}
            identity = (info.st_dev, info.st_ino)
            if identity != (opened.st_dev, opened.st_ino):
                return {**row, "verdict": "unknown", "reason": "delete_identity_changed"}
            # A fresh private directory in the same parent supplies an atomic,
            # same-filesystem rename destination and an anchored deletion dirfd.
            # Never recursively clean this fence on refusal: it may hold a
            # substituted directory. Report its location for manual recovery.
            fence_name = ".workbay-reap-" + secrets.token_hex(16)
            try:
                os.mkdir(fence_name, mode=0o700, dir_fd=parent_fd)
                fence_fd = os.open(fence_name, flags, dir_fd=parent_fd)
                stack.callback(os.close, fence_fd)
                os.rename(path.name, "candidate", src_dir_fd=parent_fd, dst_dir_fd=fence_fd)
                row["retained_path"] = str(path.parent / fence_name / "candidate")
                fenced = os.stat("candidate", dir_fd=fence_fd, follow_symlinks=False)
            except OSError as exc:
                return {**row, "verdict": "unknown", "reason": "delete_fence_unverified", "error": str(exc)}
            if identity != (fenced.st_dev, fenced.st_ino):
                return {**row, "verdict": "unknown", "reason": "delete_identity_changed"}
            shutil.rmtree("candidate", dir_fd=fence_fd)
            row.pop("retained_path")
            os.rmdir(fence_name, dir_fd=parent_fd)
            if os.path.lexists(path):
                return {**row, "verdict": "error", "reason": "path survived delete"}
            return {**row, "verdict": "reclaimed", "reason": "path absent after delete"}
    except (OSError, ValueError) as exc:
        return {**row, "verdict": "error", "reason": str(exc)}


def _atomic_json(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".tmp.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(body, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _decode(value):
    return base64.b64decode(value, validate=True).decode("utf-8")


def _vm_main():
    env = _minimal_environment()
    os.environ.clear()
    os.environ.update(env)
    parser = argparse.ArgumentParser(description="Reclaim only proven work-complete VM roots")
    parser.add_argument("--roots-b64", required=True, help="base64 JSON list of explicit candidate paths/globs")
    parser.add_argument("--proc-root-b64", default=base64.b64encode(b"/proc").decode())
    parser.add_argument("--receipt-b64", default=base64.b64encode(RECEIPT_PATH.encode()).decode())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    roots = json.loads(_decode(args.roots_b64))
    if not isinstance(roots, list) or not roots or any(not isinstance(root, str) or not root for root in roots):
        parser.error("roots must be a nonempty JSON list of path patterns")
    proc_root = Path(os.path.abspath(os.path.expanduser(_decode(args.proc_root_b64))))
    receipt_path = Path(os.path.abspath(os.path.expanduser(_decode(args.receipt_b64))))
    receipt = {
        "schema": RECEIPT_SCHEMA,
        "started_at": _now(),
        "finished_at": None,
        "roots": roots,
        "counts": dict.fromkeys(VERDICTS, 0),
        "bytes_reclaimed": 0,
        "unmarked": [],
        "errors": [],
        "verdicts": [],
        "dry_run": args.dry_run,
    }
    deadline = time.monotonic() + RUN_BUDGET_SECONDS
    try:
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with (receipt_path.parent / "run.lock").open("a") as run_lock:
            fcntl.flock(run_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            candidates = set()
            for pattern in roots:
                _check_budget(deadline)
                for value in glob.iglob(os.path.expanduser(pattern)):
                    candidate = Path(os.path.abspath(value))
                    if candidate.is_dir() or candidate.is_symlink():
                        candidates.add(candidate)
            for path in sorted(candidates):
                _check_budget(deadline)
                # An overlapping container is not itself a reclaimable root:
                # it could contain independently pinned/unmarked candidates.
                if any(other != path and path in other.parents for other in candidates):
                    row = {
                        "path": str(path),
                        "owner": _owner(path),
                        "bytes": 0,
                        "verdict": "unknown",
                        "reason": "contains separately configured roots",
                    }
                elif path == receipt_path.parent or path in receipt_path.parents:
                    row = {
                        "path": str(path),
                        "owner": _owner(path),
                        "bytes": 0,
                        "verdict": "unknown",
                        "reason": "contains reaper state",
                    }
                else:
                    row = _reap_candidate(path, proc_root, args.dry_run, deadline)
                receipt["verdicts"].append(row)
                receipt["counts"][row["verdict"]] += 1
                if row["verdict"] == "reclaimed":
                    receipt["bytes_reclaimed"] += row["bytes"]
                if row["verdict"] in ("unknown", "unmarked"):
                    receipt["unmarked"].append(row)
                if row["verdict"] in ("error", "unverified_in_use") or row.get("error"):
                    receipt["errors"].append({"path": row["path"], "reason": row.get("error", row["reason"])})
    except (OSError, ValueError) as exc:
        receipt["errors"].append({"reason": str(exc)})
    receipt["unmarked"] = sorted(receipt["unmarked"], key=lambda row: row["bytes"], reverse=True)[
        :UNMARKED_REPORT_LIMIT
    ]
    receipt["finished_at"] = _now()
    _atomic_json(receipt_path, receipt)
    return 0


# END VM RUNTIME

import inspect

from .remote_sandbox_reap import SSH_TIMEOUT_S, SshRunner, _derive_lane_key, _run_ssh


def render_reaper_script() -> str:
    """Render the actual validator/runtime plus the producer's exact key rule."""
    source = inspect.getsource(inspect.getmodule(render_reaper_script))
    runtime = source.split("# BEGIN VM RUNTIME\n", 1)[1].split("# END VM RUNTIME\n", 1)[0]
    return (
        "#!/usr/bin/env python3\n"
        + runtime
        + "\n"
        + inspect.getsource(_derive_lane_key)
        + "\nif __name__ == '__main__':\n    raise SystemExit(_vm_main())\n"
    )


def render_systemd_units(interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> dict[str, str]:
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int) or interval_seconds <= 0:
        raise ValueError("interval_seconds must be a positive integer")
    roots = base64.b64encode(json.dumps(DEFAULT_ROOTS).encode()).decode()
    return {
        "workbay-vm-reaper.service": (
            "[Unit]\nDescription=Reclaim proven WorkBay work-complete roots\n\n"
            "[Service]\nType=oneshot\nNice=10\nIOSchedulingClass=idle\n"
            f"TimeoutStartSec={SERVICE_TIMEOUT_SECONDS}s\n"
            "ExecStart=/usr/bin/env -i PATH=/usr/bin:/bin HOME=%h LANG=C LC_ALL=C "
            "/usr/bin/python3 -I %h/.local/share/workbay/vm-reaper/reaper.py "
            f"--roots-b64 {roots}\n"
        ),
        "workbay-vm-reaper.timer": (
            "[Unit]\nDescription=Periodically reclaim work-complete VM stock\n\n"
            f"[Timer]\nOnBootSec={BOOT_DELAY_SECONDS}s\nOnUnitActiveSec={interval_seconds}s\n"
            "Persistent=true\nUnit=workbay-vm-reaper.service\n\n[Install]\nWantedBy=timers.target\n"
        ),
    }


def install_reaper(runner: SshRunner, *, dry_run: bool) -> dict:
    """One bounded SSH publishes the runtime and units, then enables the timer."""
    files = {".local/share/workbay/vm-reaper/reaper.py": render_reaper_script()}
    files.update({f".config/systemd/user/{name}": value for name, value in render_systemd_units().items()})
    payload = base64.b64encode(json.dumps({"files": files, "dry_run": dry_run}).encode()).decode()
    script = f"""python3 - <<'WORKBAY_VM_REAPER_INSTALL'
import base64, json, os, subprocess, tempfile
from pathlib import Path
plan = json.loads(base64.b64decode({payload!r}))
commands = [["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", "--now", "workbay-vm-reaper.timer"]]
if not plan["dry_run"]:
    for relative, content in plan["files"].items():
        path = Path.home() / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=path.name + ".tmp.", dir=path.parent)
        try:
            with os.fdopen(fd, "w") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    for command in commands:
        subprocess.run(command, check=True, timeout={SYSTEMCTL_TIMEOUT_SECONDS}, capture_output=True, text=True)
print(json.dumps({{"dry_run": plan["dry_run"], "files": list(plan["files"]), "commands": commands}}))
WORKBAY_VM_REAPER_INSTALL
"""
    stdout, error = _run_ssh(runner, script, timeout=SSH_TIMEOUT_S)
    return {"ok": error is None, "dry_run": dry_run, "files": list(files), "output": stdout, "error": error}


def _main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("render", "install", "status"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.operation == "render":
        print(render_reaper_script(), end="")
        return 0
    from .lane_census import _default_sandbox_ssh_runner

    def runner(script, *, timeout):
        if os.environ.get("PYTEST_CURRENT_TEST"):
            raise OSError("default reaper SSH is forbidden under pytest")
        return _default_sandbox_ssh_runner(script, timeout=timeout, workspace_root=Path.cwd())

    if args.operation == "install":
        result = install_reaper(runner, dry_run=args.dry_run)
    else:
        script = "python3 - <<'WORKBAY_VM_REAPER_STATUS'\nfrom pathlib import Path\nprint((Path.home() / '.local/state/workbay/vm-reaper/last.json').read_text())\nWORKBAY_VM_REAPER_STATUS\n"
        stdout, error = _run_ssh(runner, script, timeout=SSH_TIMEOUT_S)
        try:
            receipt = json.loads(stdout) if error is None else None
        except ValueError as exc:
            receipt, error = None, f"invalid receipt: {exc}"
        result = {"ok": error is None, "receipt": receipt, "error": error}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(_main())
