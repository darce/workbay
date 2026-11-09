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
import time
from collections import Counter, deque
from itertools import chain
from datetime import datetime, timezone
from pathlib import Path

JOB_NAME = re.compile(r"workbay-.+-[0-9]{8}\Z")
EXCLUDED = {"workbay-sandbox", "workbay-control-reviews"}
FILE_CAP = 2 * 1024 * 1024
# du walks a whole job tree and is slowest exactly when IO is contended.
MEASURE_TIMEOUT_SECONDS = 300
# One pass spends at most three full du timeouts measuring; later entries are
# reclaimed unmeasured, so a starved du cannot hold apply.lock past the timer.
PASS_MEASURE_BUDGET_SECONDS = 900
# The doctor kills status at 60 s: df, the scan and every du share 30 s. The scan
# checks the deadline before each entry and caps each systemctl call at what is
# left of it. The dependency census also checks it between bounded local probes.
STATUS_MEASURE_TIMEOUT_SECONDS = 20
STATUS_MEASURE_BUDGET_SECONDS = 30
# Native discovery visits fixed slots and one level of named workspace
# containers, never repository/runtime trees. These limits cover those probes;
# exhaustion is UNKNOWN, never an empty proof authorizing reclamation.
CENSUS_TIMEOUT_SECONDS = 2
CENSUS_MAX_PROBES = 131072
CENSUS_MAX_LINKS = 40
SOURCE_PATHS = ("repo", "source", "src")
# Native producers: lane kits use sibling repo/venv; remote_gate and the
# handoff Makefile use .venv-handoff; root Makefile uses .venv-overlay-eval.
RUNTIME_PATHS = ("venv", ".venv", ".venv-handoff", ".venv-overlay-eval", "node_modules")
WORKSPACE_CONTAINERS = ("apps", "packages")
DEPENDENCY_ERROR_SAMPLE_CAP = 10
DEPENDENCY_ERROR_TEXT_CAP = 1024
# Three consecutive failures over at least two passes rule out a transient race;
# after that an undeletable tombstone is retried at most once per six hours. It
# stays in recover_failed meanwhile, so the alarm and exit 3 persist.
RECOVER_RETRY_FAILURES = 3
RECOVER_BACKOFF_SECONDS = 6 * 3600
# A pass killed mid archive_evidence (TimeoutStartSec=50min in both units) leaves
# its mkstemp temp behind; the next pass sweeps exactly that shape once it is older.
EVIDENCE_TEMP = re.compile(r"\.workbay-.+-[0-9]{8}-[a-z0-9_]{8}\.tgz\Z")
EVIDENCE_TEMP_STALE_SECONDS = 50 * 60
UNIT_QUERY_TIMEOUT_SECONDS = 10
# Keys of every counts object (scan, status, last-run). A status scan that reaches
# its deadline also counts "unclassified" (never reclaimable), present only when
# nonzero; apply scans without a deadline and never writes it.
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


class ScanDeadline(Exception):
    """The status deadline left no time for a unit query or cut one short."""


class Refusal(Exception):
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


class ResourcePermissionError(PermissionError):
    """Permission denial at a resource operation, never a custody/state write."""

    def __init__(self, stage, error):
        super().__init__(*error.args)
        if error.filename is not None:
            self.filename = error.filename
        if error.filename2 is not None:
            self.filename2 = error.filename2
        self.stage = stage


def resource_operation(stage, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except PermissionError as exc:
        raise ResourcePermissionError(stage, exc) from exc


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


def activity(directory, read_only=False, deadline=None):
    """Return the first unit, lock or procfs hold, in the required order."""

    def unit_query(*arguments):
        # Only status passes a deadline: each call gets at most what is left of it.
        timeout = UNIT_QUERY_TIMEOUT_SECONDS
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
            if timeout <= 0:
                raise ScanDeadline
        try:
            return run(["systemctl", "--user", *arguments, directory.name], timeout=timeout)
        except subprocess.TimeoutExpired:
            if timeout < UNIT_QUERY_TIMEOUT_SECONDS:
                raise ScanDeadline from None
            raise

    try:
        result = unit_query("is-active")
        unit_state = result.stdout.strip()
        if result.returncode == 0:
            return "hold_unit_active", "unit_active_or_unknown"
        if result.returncode == 3:
            if unit_state not in {"inactive", "failed"}:
                return "hold_unit_active", "unit_active_or_unknown"
        else:
            # --collect removes finished transient units. Verify absence with a
            # successful manager query; an unavailable bus still denies.
            shown = unit_query("show", "-p", "LoadState", "-p", "ActiveState")
            lines = shown.stdout.splitlines()
            properties = dict(line.split("=", 1) for line in lines)
            if (
                shown.returncode != 0
                or len(lines) != 2
                or properties.get("LoadState") != "not-found"
                or properties.get("ActiveState") not in {"inactive", ""}
            ):
                return "hold_unit_active", "unit_active_or_unknown"
    except ScanDeadline:
        # Unclassified like the entries after it: the bus was not given a full timeout.
        return "unclassified", "scan_deadline"
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


class DependencyError(ValueError):
    """Bounded diagnostic only: never include exception messages or file bytes."""

    def __init__(self, diagnostic):
        super().__init__("dependency census unknown")
        self.diagnostic = diagnostic


class CensusLimit(ValueError):
    """No complete observation can be made within the census budget."""


class DependencyCensus:
    """Bounded local observations; errors deliberately escape to the hold path."""

    def __init__(self, deadline: float | None = None):
        self.deadline = None
        self.scan_deadline = deadline
        self.probes = 0
        self.consumer_path = self.probe_path = Path("/")
        self.phase = "census"
        self.observations = None
        self.incomplete = None

    def locate(self, path, phase):
        self.probe_path, self.phase = path, phase

    def diagnostic(self, error):
        return {key: str(value)[:DEPENDENCY_ERROR_TEXT_CAP] for key, value in {
            "consumer_path": self.consumer_path,
            "probe_path": self.probe_path,
            "phase": self.phase,
            "error_type": type(error).__name__,
        }.items()}

    def check(self) -> None:
        now = time.monotonic()
        if self.deadline is None:
            self.deadline = min(self.scan_deadline if self.scan_deadline is not None else float("inf"),
                                now + CENSUS_TIMEOUT_SECONDS)
        if self.scan_deadline is not None and now >= self.scan_deadline:
            raise ScanDeadline
        self.probes += 1
        if now >= self.deadline or self.probes > CENSUS_MAX_PROBES:
            raise CensusLimit("dependency census limit exhausted")

    def resolve(self, path: Path) -> tuple[Path, set[Path]]:
        """Strict component resolution with a hop cap, including intermediate edges.

        Never enumerate link targets. Remember each component actually visited,
        including real directories before '..': removing one breaks the literal
        consumer path even when its final target is outside the provider. Expand
        symlinks before processing '..'; lexical normalization changes that order.
        """
        pending = deque(path.absolute().parts[1:])
        # This Linux reclaimer has one filesystem namespace: // and / agree.
        current = Path("/")
        traversed = set()
        links = 0
        while pending:
            part = pending.popleft()
            self.locate(current / part, "resolve_component")
            self.check()
            if part == "..":
                current = current.parent
                continue
            if part == ".":
                continue
            candidate = current / part
            info = candidate.lstat()
            if stat.S_ISLNK(info.st_mode):
                traversed.add(candidate)
                links += 1
                if links > CENSUS_MAX_LINKS:
                    raise ValueError("dependency symlink cycle or excessive chain")
                self.check()
                target = Path(os.readlink(candidate))
                if target.is_absolute():
                    current = Path("/")
                    pending.extendleft(reversed(target.parts[1:]))
                else:
                    pending.extendleft(reversed(target.parts))
            else:
                if pending and not stat.S_ISDIR(info.st_mode):
                    raise ValueError("dependency path crosses a non-directory")
                traversed.add(candidate)
                current = candidate
        return current, traversed

    def directory(self, path: Path, targets: set[Path]) -> Path | None:
        """Resolve one fixed slot, recording links, and require a readable directory.

        Resolution permits the caller's next *named slot*, never enumeration of
        a foreign tree. Only a missing optional slot is absent; an opaque link,
        wrong type or denied open must escape to the UNKNOWN hold.
        """
        self.locate(path, "directory_slot")
        self.check()
        try:
            info = path.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(info.st_mode):
            target, traversed = self.resolve(path)
            targets.update(traversed)
            targets.add(target)
            path = target
            self.check()
            info = path.lstat()
        self.locate(path, "directory_slot")
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("dependency slot is not a directory")
        self.check()
        self.locate(path, "open_directory")
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
        os.close(fd)
        return path

    def alternates(self, repo: Path, targets: set[Path]) -> None:
        objects = repo
        for part in (".git", "objects"):
            # A gitfile (e.g. linked worktree) needs gitdir/commondir semantics.
            # Unsupported inspection is UNKNOWN, not a false empty census.
            objects = self.directory(objects / part, targets)
            if objects is None:
                return
        # Git alternate stores can themselves borrow from other stores. Follow
        # only their info/alternates slot, with the same global time/probe cap.
        # An explicit stack avoids recursion; active vs completed distinguishes
        # cycles (UNKNOWN) from a shared store reached along multiple edges.
        pending = [(objects, False)]
        active, completed = set(), set()
        while pending:
            self.check()
            store, leaving = pending.pop()
            if leaving:
                active.remove(store)
                completed.add(store)
                continue
            if store in active:
                raise ValueError("dependency alternate cycle")
            if store in completed:
                continue
            active.add(store)
            pending.append((store, True))
            pending.extend((target, False) for target in self.object_alternates(store, targets))

    def object_alternates(self, objects: Path, targets: set[Path]) -> list[Path]:
        info = self.directory(objects / "info", targets)
        if info is None:
            return []
        alternate = info / "alternates"
        self.locate(alternate, "alternates_slot")
        self.check()
        try:
            alternate.lstat()
        except FileNotFoundError:
            return []
        # Bounded regular reads also refuse FIFO, symlink, oversized and invalid
        # UTF-8 inputs rather than silently treating them as no dependencies.
        self.check()
        self.locate(alternate, "read_alternates")
        lines = read_regular(alternate).decode("utf-8").splitlines()
        stores = []
        for line in lines:
            self.check()
            if not line:
                continue
            if "\x00" in line or line.startswith('"'):
                raise ValueError("ambiguous Git alternate")
            target = Path(line)
            if not target.is_absolute():
                # info may resolve elsewhere; Git's base remains objects.
                target = objects / target
            resolved, traversed = self.resolve(target)
            targets.update(traversed)
            targets.add(resolved)
            store = self.directory(resolved, targets)
            if store is None:
                raise ValueError("dependency alternate disappeared")
            stores.append(store)
        return stores


    def children(self, parent):
        """Enumerate only an explicitly authorized namespace/container, with caps."""
        self.locate(parent, "enumerate_container")
        self.check()
        with os.scandir(parent) as entries:
            while True:
                self.locate(parent, "enumerate_container")
                self.check()
                entry = next(entries, None)
                if entry is None:
                    break
                yield parent / entry.name

    def workspace(self, source, targets):
        # Direct checkouts as well as repo/source/src aliases are native roots.
        # Only named container levels authorize enumeration, including when a
        # source/container link resolves outside root/home. Never recurse into
        # a member, its runtime, or any arbitrary symlink target.
        self.alternates(source, targets)
        for runtime in RUNTIME_PATHS:
            self.directory(source / runtime, targets)
        for name in WORKSPACE_CONTAINERS:
            container = self.directory(source / name, targets)
            if container is None:
                continue
            for member in self.children(container):
                local = self.directory(member, targets)
                if local is None:
                    raise ValueError("workspace member disappeared")
                self.alternates(local, targets)
                for runtime in RUNTIME_PATHS:
                    self.directory(local / runtime, targets)

    def collect(self, root):
        # One bounded observation generation per scan. Rechecks call lenders
        # without this object, creating a fresh generation under apply.lock.
        if self.observations is not None:
            return
        self.observations = []
        try:
            parents = {self.resolve(Path(parent))[0] for parent in (root, Path.home())}
            for parent in sorted(parents):
                self.consumer_path = parent
                for child in self.children(parent):
                    self.consumer_path = child
                    self.locate(child, "consumer_type")
                    self.check()
                    targets, error = set(), None
                    try:
                        mode = child.lstat().st_mode
                        if not (stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
                            continue
                        local = self.directory(child, targets)
                        if local is None:
                            raise ValueError("consumer disappeared")
                        self.workspace(local, targets)
                        for name in SOURCE_PATHS:
                            source = self.directory(local / name, targets)
                            if source is not None:
                                self.workspace(source, targets)
                        # remote_gate's default is HOME/src/<repo-slug>. This
                        # single named source container is not a foreign walk.
                        if parent == Path.home() and child.name == "src":
                            for member in self.children(local):
                                self.locate(member, "consumer_type")
                                self.check()
                                # Namespace metadata is not a source slot. Only
                                # an observed regular file is safely excluded;
                                # symlinks and other types still need validation.
                                if stat.S_ISREG(member.lstat().st_mode):
                                    continue
                                source = self.directory(member, targets)
                                if source is None:
                                    raise ValueError("source member disappeared")
                                self.workspace(source, targets)
                    except CensusLimit:
                        raise
                    except (OSError, ValueError, RuntimeError) as exc:
                        error = self.diagnostic(exc)
                    self.observations.append((child, targets, error))
                self.consumer_path = parent
            self.check()
        except ScanDeadline:
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            self.incomplete = self.diagnostic(exc)

    def errors(self, directory):
        errors = [error for child, _, error in self.observations or [] if child != directory and error]
        if self.incomplete:
            errors.append(self.incomplete)
        return errors


def lenders(directory, root, deadline=None, census=None):
    """Observe ordinary and terminal consumers; hardlinks are independent entries.

    An unreadable/unsupported slot remains UNKNOWN, regardless of consumer UID.
    No manifest-authored empty dependency list replaces native observations.
    """
    census = census if census is not None else DependencyCensus(deadline)
    census.consumer_path = Path(directory)
    try:
        resolver = DependencyCensus(deadline)
        resolver.consumer_path = Path(directory)
        try:
            directory, _ = resolver.resolve(Path(directory))
        except (OSError, ValueError, RuntimeError) as exc:
            raise DependencyError(resolver.diagnostic(exc)) from None
        census.collect(root)
    except ScanDeadline:
        raise
    except DependencyError:
        raise
    except (OSError, ValueError, RuntimeError) as exc:
        raise DependencyError(census.diagnostic(exc)) from None
    errors = census.errors(directory)
    if errors:
        raise DependencyError(errors[0])
    return sorted({child.name for child, targets, _ in census.observations
                   if child != directory and any(inside(target, directory) for target in targets)})


def dependency_diagnostics(rows, census=None):
    """Deduplicate a bounded sample; duplicates alone do not mean truncation."""
    sample, seen, truncated = [], set(), False
    errors = (error for _, _, error in census.observations or [] if error) if census else ()
    if census and census.incomplete:
        errors = chain(errors, (census.incomplete,))
    for error in chain(errors, (row["dependency_error"] for row in rows if "dependency_error" in row)):
        key = tuple(sorted(error.items()))
        if key in seen:
            continue
        if len(sample) >= DEPENDENCY_ERROR_SAMPLE_CAP:
            truncated = True
            break
        seen.add(key)
        sample.append(error)
    return {"dependency_errors": sample, "dependency_errors_truncated": truncated}


def released_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("released_at must include a UTC offset")
    return parsed.timestamp()


def classify(directory, root, read_only=False, deadline=None, dependency_census=None):
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
    hold = activity(directory, read_only=True, deadline=deadline) if read_only else activity(directory)
    if hold:
        return verdict(*hold)
    try:
        row["lends_to"] = lenders(directory, root, deadline=deadline, census=dependency_census)
    except ScanDeadline:
        return verdict("unclassified", "scan_deadline")
    except DependencyError as exc:
        row["dependency_error"] = exc.diagnostic
        return verdict("hold_lender", "dependency_census_unknown")
    except (OSError, ValueError, RuntimeError) as exc:
        row["dependency_error"] = {"consumer_path": str(directory)[:DEPENDENCY_ERROR_TEXT_CAP],
                                   "probe_path": str(directory)[:DEPENDENCY_ERROR_TEXT_CAP],
                                   "phase": "lenders", "error_type": type(exc).__name__}
        return verdict("hold_lender", "dependency_census_unknown")
    if row["lends_to"]:
        return verdict("hold_lender", "native_dependency_borrowed")
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


def scan(root=None, read_only=False, deadline=None):
    """Classify every job dir; past a monotonic deadline, report the rest unclassified."""
    root = Path(root or Path.home()).expanduser().resolve()
    rows = []
    census = DependencyCensus(deadline)
    for child in sorted(root.iterdir()):
        if child.name in EXCLUDED or child.name.endswith(".git") or not JOB_NAME.fullmatch(child.name):
            continue
        if deadline is not None and time.monotonic() >= deadline:
            # Unclassified is never reclaimable; apply scans without a deadline.
            rows.append(
                {
                    "basename": child.name,
                    "path": str(child),
                    "lends_to": [],
                    "verdict": "unclassified",
                    "reason": "scan_deadline",
                }
            )
            continue
        rows.append(classify(child, root, read_only=read_only, deadline=deadline, dependency_census=census))
    counts = dict.fromkeys(VERDICTS, 0)
    counts.update(Counter(row["verdict"] for row in rows))
    return {"root": str(root), "entries": rows, "counts": counts, **dependency_diagnostics(rows, census)}


def measure(command, timeout=30):
    try:
        result = run(command, timeout=timeout)
        if result.returncode:
            raise ValueError(result.stderr.strip())
        return int(result.stdout.strip().splitlines()[-1].split()[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError) as exc:
        raise Refusal("measurement_failed", f"{command[0]}: {exc}") from exc


def disk_free(root, timeout=30):
    return measure(["df", "-B1", "--output=avail", "--", str(root)], timeout=timeout)


def size_bytes(directory, timeout=MEASURE_TIMEOUT_SECONDS):
    return measure(["du", "-s", "-B1", "--", str(directory)], timeout=timeout)


class MeasureBudget:
    """Wall-clock accounting for measurement and census; past it, du is skipped."""

    def __init__(self, seconds, timeout):
        self.seconds, self.timeout, self.spent = seconds, timeout, 0.0

    def charge(self, started):
        self.spent += time.monotonic() - started

    def size(self, directory):
        """Size is accounting, not authority: a failed or skipped du never decides a reclaim."""
        left = self.seconds - self.spent
        if left <= 0:
            return None, f"measurement_skipped: the {self.seconds} s measure budget (--measure-budget-seconds) is spent"
        started = time.monotonic()
        try:
            return size_bytes(directory, min(self.timeout, left)), None
        except Refusal as exc:
            return None, f"{exc.kind}: {exc}"
        finally:
            self.charge(started)


def count_freed(summary, receipt):
    if receipt["bytes_freed"] is None:
        summary["unmeasured"] += 1
        summary["unmeasured_basenames"].append(receipt["basename"])
    else:
        summary["bytes_freed"] += receipt["bytes_freed"]


def status(
    root=None,
    stale_after_seconds=5400,
    floor_bytes=10 * 1024**3,
    measure_timeout_seconds=STATUS_MEASURE_TIMEOUT_SECONDS,
    measure_budget_seconds=STATUS_MEASURE_BUDGET_SECONDS,
):
    """Report observed state only; never create state, acquire locks or reclaim."""
    started = time.monotonic()
    root = Path(root or Path.home()).expanduser().resolve()
    # A hung user bus costs each marked dir up to 20 s, so the scan shares the
    # deadline too; a 0 budget skips du only, and the scan keeps the default.
    deadline = started + (measure_budget_seconds or STATUS_MEASURE_BUDGET_SECONDS)
    free = disk_free(root, timeout=max(0, min(30, deadline - time.monotonic())))
    scanned = scan(root, read_only=True, deadline=deadline)
    # A caller bounds the whole call, so df and the scan spend the du budget too.
    budget = MeasureBudget(measure_budget_seconds, measure_timeout_seconds)
    budget.charge(started)
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
            or not isinstance(value.get("recover_failed", []), list)
            or any(not isinstance(name, str) for name in value.get("recover_failed", []))
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
        if last_run.get("recover_failed"):
            alarms.append("recover_failed")
    if free < floor_bytes:
        alarms.append("free_below_floor")
    if scanned["counts"].get("unclassified"):
        alarms.append("scan_incomplete")
    unmarked = [row for row in scanned["entries"] if row["verdict"] == "hold_unmarked"]
    if unmarked:
        alarms.append("unmarked_present")
    unmarked_bytes, unmeasured_reason = 0, None
    for row in unmarked:
        size, _ = budget.size(Path(row["path"]))
        if size is None:
            unmarked_bytes = None
            # A du the shared budget skipped or cut short is "budget"; any other failure is du's own.
            unmeasured_reason = "budget" if budget.spent >= budget.seconds else "du_error"
            alarms.append("unmarked_size_unmeasured")
            break
        unmarked_bytes += size
    return {
        "schema": 1,
        "root": str(root),
        "df_free_bytes": free,
        "floor_bytes": floor_bytes,
        "last_run": last_run,
        "last_run_age_seconds": age,
        "counts": scanned["counts"],
        "dependency_errors": scanned["dependency_errors"],
        "dependency_errors_truncated": scanned["dependency_errors_truncated"],
        "unmarked": {
            "count": len(unmarked),
            "bytes": unmarked_bytes,
            "sample": [row["basename"] for row in unmarked[:10]],
            # Optional: present only when bytes is null.
            **({"reason": unmeasured_reason} if unmeasured_reason else {}),
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
        mode = resource_operation("archive", out.lstat).st_mode
    except FileNotFoundError:
        mode = 0
    if stat.S_ISDIR(mode):
        children = resource_operation("archive", lambda: sorted(out.iterdir()))
        candidates.extend(p for p in children if p.suffix in {".json", ".md", ".log"})
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
                        if isinstance(exc, PermissionError):
                            raise ResourcePermissionError("archive", exc) from exc
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


def sweep_evidence_temps(archive_root, now):
    """Delete archive_evidence temps a killed pass left; never a finished archive."""
    swept = []
    try:
        children = sorted(archive_root.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return swept
    for path in children:
        if not EVIDENCE_TEMP.fullmatch(path.name):
            continue
        try:
            info = path.lstat()
            # Younger than the unit timeout could still belong to a running pass's tar.
            if not stat.S_ISREG(info.st_mode) or now - info.st_mtime <= EVIDENCE_TEMP_STALE_SECONDS:
                continue
            path.unlink()
        except OSError:
            continue  # A temp left in place is only swept by a later pass.
        swept.append(path.name)
    return swept


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
            resource_operation("delete", shutil.rmtree, target, onexc=retry_permission)
        else:
            resource_operation(
                "delete",
                shutil.rmtree,
                target,
                onerror=lambda function, path, info: retry_permission(function, path, info[1]),
            )
        fsync_dir(target.parent)
    append_receipt(state, receipt)
    intent.unlink(missing_ok=True)
    fsync_dir(state)


def recover_backoff(receipt, now):
    """True while a repeatedly undeletable tombstone waits out its retry window."""
    failures = receipt.get("recover_failures")
    if type(failures) is not int or failures < RECOVER_RETRY_FAILURES:
        return False
    try:
        # A failure stamped in the future (clock stepped back) is retried at once.
        return 0 <= now - released_time(receipt["recover_failed_at"]) < RECOVER_BACKOFF_SECONDS
    except (KeyError, TypeError, ValueError, OverflowError, AttributeError):
        return False


def recover(root, state, summary, limit, budget, retry_failed=False):
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
            if os.path.lexists(target) and not retry_failed and recover_backoff(receipt, time.time()):
                # Still not deleted, so still reported; only the rmtree retry waits.
                summary["recover_failed"].append(target.name)
                summary["recover_deferred"].append(target.name)
                continue
        else:
            if not target.is_dir():
                continue
            allocated, measurement_error = budget.size(target)
            receipt = {
                "root": str(root),
                "tombstone": target.name,
                "basename": target.name.removeprefix(".reclaiming-").rsplit("-", 1)[0],
                "owner": None,
                "released_at": None,
                "bytes_freed": allocated,
                "archive": None,
                "sha256": None,
                "skipped_evidence": [],
                "reclaim_id": f"{root}/{target.name}",
            }
            if measurement_error:
                receipt["measurement_error"] = measurement_error
            intent = state / f"pending-{hashlib.sha256(receipt['reclaim_id'].encode()).hexdigest()}.json"
            atomic_json(intent, receipt)
        try:
            finish_delete(target, state, intent, receipt)
        except (Refusal, OSError):
            if not os.path.lexists(target):
                raise
            # An undeletable tombstone keeps its intent for the next pass; later
            # tombstones and the scan still run, and the pass outcome reports it.
            summary["recover_failed"].append(target.name)
            failures = receipt.get("recover_failures")
            failures = failures if type(failures) is int and failures > 0 else 0
            try:
                # New keys only: older reclaimers still read this intent.
                atomic_json(intent, {**receipt, "recover_failures": failures + 1, "recover_failed_at": utc_now()})
            except OSError:
                pass  # An uncounted failure is only retried sooner.
            continue
        summary["recovered"] += 1
        # A pending intent may carry an unmeasured apply receipt.
        count_freed(summary, receipt)
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
                    "unmeasured",
                    "unmeasured_basenames",
                    "recover_failed",
                    "recover_deferred",
                    "dependency_errors",
                    "dependency_errors_truncated",
                )
            },
            "error": error,
            **({"resource_failed": summary["resource_failed"]} if summary.get("resource_failed") else {}),
        },
    )


def apply(
    root=None,
    max_dirs=10,
    archive_root=None,
    evidence_cap_mb=20,
    trigger="timer",
    measure_timeout_seconds=MEASURE_TIMEOUT_SECONDS,
    measure_budget_seconds=PASS_MEASURE_BUDGET_SECONDS,
    retry_recover_failed=False,
):
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
        "unmeasured": 0,
        "unmeasured_basenames": [],
        "recover_failed": [],
        "recover_deferred": [],
        "resource_failed": [],
        "df_free_before": None,
        "df_free_after": None,
        "df_delta_bytes": None,
        "remaining": [],
        "held_on_recheck": [],
        "swept_evidence_temps": [],
        "dependency_errors": [],
        "dependency_errors_truncated": False,
    }
    with (state / "apply.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {**summary, "outcome": "busy"}
        started_at = utc_now()
        budget = MeasureBudget(measure_budget_seconds, measure_timeout_seconds)
        try:
            # Under apply.lock: no other pass is writing a temp while this one sweeps.
            summary["swept_evidence_temps"] = sweep_evidence_temps(archive_root, time.time())
            summary["df_free_before"] = disk_free(root)
            recovery_remaining = recover(root, state, summary, max_dirs, budget, retry_recover_failed)
            census_started = time.monotonic()
            scanned = scan(root)
            budget.charge(census_started)
            summary["counts"] = scanned["counts"]
            summary.update({key: scanned[key] for key in ("dependency_errors", "dependency_errors_truncated")})
            eligible = sorted(
                (r for r in scanned["entries"] if r["verdict"] == "reclaimable"),
                key=lambda r: (released_time(r["released_at"]), r["basename"]),
            )
            available = max_dirs - summary["recovered"]
            summary["remaining"] = recovery_remaining + [r["basename"] for r in eligible[available:]]
            for row in eligible[:available]:
                try:
                    directory = root / row["basename"]
                    identity = directory.lstat()
                    census_started = time.monotonic()
                    checked = classify(directory, root)
                    budget.charge(census_started)
                    if checked["verdict"] != "reclaimable":
                        summary["held_on_recheck"].append(checked)
                        diagnostic = dependency_diagnostics([{"dependency_error": e} for e in summary["dependency_errors"]] + [checked])
                        summary["dependency_errors"] = diagnostic["dependency_errors"]
                        summary["dependency_errors_truncated"] |= diagnostic["dependency_errors_truncated"]
                        continue
                    evidence = archive_evidence(directory, archive_root, int(evidence_cap_mb * 1024 * 1024))
                    allocated, measurement_error = budget.size(directory)
                    # Archive/du can take time: recheck local activity and owner authority
                    # again immediately before making the job disappear from its live name.
                    census_started = time.monotonic()
                    checked = classify(directory, root)
                    budget.charge(census_started)
                    current = directory.lstat()
                    if checked["verdict"] != "reclaimable" or (identity.st_dev, identity.st_ino) != (
                        current.st_dev,
                        current.st_ino,
                    ):
                        summary["held_on_recheck"].append(checked)
                        diagnostic = dependency_diagnostics([{"dependency_error": e} for e in summary["dependency_errors"]] + [checked])
                        summary["dependency_errors"] = diagnostic["dependency_errors"]
                        summary["dependency_errors_truncated"] |= diagnostic["dependency_errors_truncated"]
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
                    if measurement_error:
                        receipt["measurement_error"] = measurement_error
                    intent = state / f"pending-{hashlib.sha256(reclaim_id.encode()).hexdigest()}.json"
                    atomic_json(intent, receipt)
                    resource_operation("rename", os.rename, directory, tomb)
                    fsync_dir(root)
                    finish_delete(tomb, state, intent, receipt)
                    summary["reclaimed"] += 1
                    count_freed(summary, receipt)
                except ResourcePermissionError as exc:
                    # Only source access, rename and rmtree denials reach here.
                    # Archive publication/fsync and every state write still end
                    # the pass. No success accounting occurs for this resource.
                    if exc.stage == "delete":
                        if not os.path.lexists(tomb):
                            raise
                        # Keep the original authority, archive and reclaim_id;
                        # recovery owns subsequent retries and its existing backoff.
                        atomic_json(intent, {**receipt, "recover_failures": 1, "recover_failed_at": utc_now()})
                        summary["recover_failed"].append(tomb.name)
                    summary["resource_failed"].append(
                        {
                            "basename": row["basename"],
                            "stage": exc.stage,
                            "type": "permission_denied",
                            "errno": exc.errno,
                            "message": str(exc),
                        }
                    )
                    continue
            summary["df_free_after"] = disk_free(root)
            summary["df_delta_bytes"] = summary["df_free_after"] - summary["df_free_before"]
            error = None
            if summary["recover_failed"]:
                # The pass still reclaimed what it could; "error" keeps the stuck
                # tombstone loud for every status reader, including older ones.
                summary["outcome"] = "error"
                message = "tombstones not deleted: " + ", ".join(summary["recover_failed"])
                if summary["recover_deferred"]:
                    message += "; retry deferred by backoff: " + ", ".join(summary["recover_deferred"])
                error = summary["error"] = {"type": "recover_failed", "message": message}
            if summary["resource_failed"]:
                message = "resources not reclaimed: " + ", ".join(
                    f"{failure['basename']} ({failure['stage']}: {failure['message']})"
                    for failure in summary["resource_failed"]
                )
                summary["outcome"] = "error"
                if error is None:
                    error = {"type": "resource_failed", "message": message}
                else:
                    error["message"] += "; " + message
                summary["error"] = error
            write_last_run(state, summary, trigger, started_at, error)
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


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
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
    status_parser.add_argument(
        "--measure-timeout-seconds",
        type=positive,
        default=STATUS_MEASURE_TIMEOUT_SECONDS,
        help=f"seconds one du may take (default {STATUS_MEASURE_TIMEOUT_SECONDS}, at least 1); "
        "capped by what is left of --measure-budget-seconds, so raise both for a longer du",
    )
    status_parser.add_argument(
        "--measure-budget-seconds",
        type=nonnegative,
        default=STATUS_MEASURE_BUDGET_SECONDS,
        help=f"seconds df, the scan and every du may take together (default {STATUS_MEASURE_BUDGET_SECONDS}); "
        "dirs not classified by then are counted unclassified and raise scan_incomplete; "
        "unmarked dirs left unmeasured raise unmarked_size_unmeasured; "
        f"0 skips du (the scan keeps the {STATUS_MEASURE_BUDGET_SECONDS} s deadline)",
    )
    status_parser.add_argument("--json", action="store_true")
    apply_parser = commands.add_parser(
        "apply",
        help="archive and reclaim released jobs",
        epilog="exit 0: ok, busy or skipped; 2: error; 3: the pass finished but left recover_failed tombstones or resource permission failures. "
        f"After {RECOVER_RETRY_FAILURES} consecutive failures a tombstone is retried at most once per "
        f"{RECOVER_BACKOFF_SECONDS // 3600} h; it stays in recover_failed (and exit 3) meanwhile.",
    )
    apply_parser.add_argument("--root", default=str(Path.home()))
    apply_parser.add_argument("--max", type=nonnegative, default=10)
    apply_parser.add_argument("--archive-root")
    apply_parser.add_argument("--evidence-cap-mb", type=nonnegative, default=20)
    apply_parser.add_argument("--if-free-below", type=nonnegative)
    apply_parser.add_argument(
        "--measure-timeout-seconds",
        type=positive,
        default=MEASURE_TIMEOUT_SECONDS,
        help=f"seconds one du may take (default {MEASURE_TIMEOUT_SECONDS}, at least 1); "
        "capped by what is left of --measure-budget-seconds; an entry whose du fails is reclaimed unmeasured",
    )
    apply_parser.add_argument(
        "--measure-budget-seconds",
        type=nonnegative,
        default=PASS_MEASURE_BUDGET_SECONDS,
        help=f"seconds one pass may spend in du, recover included (default {PASS_MEASURE_BUDGET_SECONDS}); "
        "native scan and rechecks also spend this budget; "
        "later entries are reclaimed unmeasured; 0 skips measurement",
    )
    apply_parser.add_argument(
        "--retry-recover-failed",
        action="store_true",
        help="retry recover_failed tombstones now instead of after their backoff (after fixing owner or mode)",
    )
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
            output = status(
                args.root,
                args.stale_after_seconds,
                args.floor_bytes,
                args.measure_timeout_seconds,
                args.measure_budget_seconds,
            )
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
            output = apply(
                args.root,
                args.max,
                args.archive_root,
                args.evidence_cap_mb,
                trigger=trigger,
                measure_timeout_seconds=args.measure_timeout_seconds,
                measure_budget_seconds=args.measure_budget_seconds,
                retry_recover_failed=args.retry_recover_failed,
            )
            if output["outcome"] == "error":
                print(json.dumps(output, sort_keys=True))
                return 3
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
