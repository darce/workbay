#!/usr/bin/env python3
"""Release and reclaim owner-finished VM job directories using local facts only.

Scan is read-only. Apply requires a valid matching owner release, holds unknown
activity, archives bounded evidence, and renames before deleting. An opt-in unit
lease may mark a release after verified termination and grace; it never deletes.
Producers use allocate; owners mark legacy dirs with allocate --adopt.
"""

from __future__ import annotations

import argparse
import ast
import errno
import fcntl
import hashlib
import inspect
import io
import json
import os
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from collections import Counter, deque
from contextlib import ExitStack
from itertools import chain
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

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
# exhaustion holds, never an empty proof authorizing reclamation. At a modeled
# loaded cost of 0.25 ms/probe, the independent 131072-probe cap costs 32.768 s;
# 60 s leaves scheduling headroom. Status uses its own scan deadline instead.
CENSUS_TIMEOUT_SECONDS = 60
CENSUS_MAX_PROBES = 131072
CENSUS_MAX_LINKS = 40
SOURCE_PATHS = ("repo", "source", "src")
# Native producers: lane kits use sibling repo/venv; remote_gate and the
# handoff Makefile use .venv-handoff; root Makefile uses .venv-overlay-eval.
RUNTIME_PATHS = ("venv", ".venv", ".venv-handoff", ".venv-overlay-eval", "node_modules")
WORKSPACE_CONTAINERS = ("apps", "packages")
# setuptools' finder module; its .pth is read like every other .pth.
EDITABLE_FINDER = re.compile(r"__editable__.*finder.*\.py\Z")
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


class ForeignOwner(Refusal):
    def __init__(self, path, uid):
        super().__init__("recover_blocked_foreign_owner", str(path))
        self.diagnostic = {"path": str(path), "owner_uid": uid}


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


def path_constants(tree):
    """Absolute string constants of parsed consumer code, which is never run."""
    return [node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("/")]


def imported_modules(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module


def manifest(path):
    value = json.loads(read_regular(path))
    if not isinstance(value, dict) or not isinstance(value.get("owner"), str) or not value["owner"].strip():
        raise ValueError("manifest needs a nonempty owner string")
    return value


LEASE_DEFAULT_GRACE = {"lane": 21600, "gate": 21600, "qa": 21600, "scratch": 3600}


def allocate(directory, owner, retention_class, task_ref=None, note=None, adopt=False,
             release_on_unit_exit=False, grace_seconds=None):
    if release_on_unit_exit:
        if retention_class not in LEASE_DEFAULT_GRACE:
            raise Refusal("lease_class_refused", "unit leases require lane, gate, qa or scratch retention")
        grace_seconds = LEASE_DEFAULT_GRACE[retention_class] if grace_seconds is None else grace_seconds
        if type(grace_seconds) is not int or not 600 <= grace_seconds <= 604800:
            raise Refusal("lease_grace_invalid", "grace_seconds must be between 600 and 604800")
    elif grace_seconds is not None:
        raise Refusal("lease_required", "--grace-seconds requires --release-on-unit-exit")
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
        if release_on_unit_exit:
            data["lease"] = {"unit": directory.name, "grace_seconds": grace_seconds, "token": secrets.token_hex(32)}
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


def terminal_unit_state(directory, deadline=None):
    """Only manager-verified terminal states authorize a lease observation."""
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

    result = unit_query("is-active")
    unit_state = result.stdout.strip()
    if result.returncode == 0:
        return None
    if result.returncode == 3:
        if unit_state not in {"inactive", "failed"}:
            return None
        return unit_state
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
            return None
        return "not-found"


def activity(directory, read_only=False, deadline=None):
    """Return the first unit, lock or procfs hold, in the required order."""
    try:
        if terminal_unit_state(directory, deadline) is None:
            return "hold_unit_active", "unit_active_or_unknown"
    except ScanDeadline:
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

    def __init__(self, diagnostic, census=None):
        super().__init__("dependency census unknown")
        self.diagnostic = diagnostic
        self.exhaustion = census.exhaustion() if census else None


class CensusLimit(ValueError):
    """No complete observation can be made within the census budget."""


class DependencyCensus:
    """Bounded local observations; errors deliberately escape to the hold path."""

    def __init__(self, deadline: float | None = None, sandbox_root=None):
        self.sandbox_root = Path(sandbox_root).expanduser().absolute() if sandbox_root is not None else None
        self.deadline = None
        self.scan_deadline = deadline
        self.probes = 0
        self.consumer_path = self.probe_path = Path("/")
        self.phase = "census"
        self.observations = None
        self.incomplete = None
        self.started = self.finished = None
        self.cap_hit = None
        self.walks = {}
        self.enumerations = {}
        self.excluded_opaque = []

    def start(self):
        self.started = time.monotonic()
        self.deadline = (self.scan_deadline if self.scan_deadline is not None
                         else self.started + CENSUS_TIMEOUT_SECONDS)

    def exhaustion(self):
        if self.cap_hit is None:
            return None
        finished = self.finished if self.finished is not None else time.monotonic()
        return {"probes": self.probes, "elapsed_seconds": round(max(0, finished - self.started), 6),
                "cap_hit": self.cap_hit}

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
        if self.scan_deadline is not None and now >= self.scan_deadline:
            self.cap_hit = "scan_deadline"
            raise ScanDeadline
        self.probes += 1
        if self.probes > CENSUS_MAX_PROBES:
            self.cap_hit = "probes"
        elif now >= self.deadline:
            self.cap_hit = "time"
        if self.cap_hit:
            raise CensusLimit("dependency census limit exhausted")

    def resolve(self, path: Path, absent=False) -> tuple[Path, set[Path]]:
        """Strict component resolution with a hop cap, including intermediate edges.

        Never enumerate link targets. Remember each component actually visited,
        including real directories before '..': removing one breaks the literal
        consumer path even when its final target is outside the provider. Expand
        symlinks before processing '..'; lexical normalization changes that order.

        With absent, a missing component is observed absence, not UNKNOWN: site
        skips such an entry. The lexical rest still names where the reference
        points, so an absent path inside a released tree still holds it.
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
            try:
                info = candidate.lstat()
            except (FileNotFoundError, NotADirectoryError):
                if not absent:
                    raise
                return Path(os.path.normpath(candidate.joinpath(*pending))), traversed
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

    def directory(self, path: Path, targets: set[Path], *, absent_link=False) -> Path | None:
        """Resolve one fixed slot, recording links, and require a readable directory.

        Resolution permits the caller's next *named slot*, never enumeration of
        a foreign tree. Missing optional slots and dangling runtime links are
        absent; opaque links, wrong types and denied opens remain UNKNOWN.
        Git metadata and source slots keep strict link resolution.
        """
        self.locate(path, "directory_slot")
        self.check()
        try:
            info = path.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(info.st_mode):
            target, traversed = self.resolve(path, absent=absent_link)
            targets.update(traversed)
            targets.add(target)
            path = target
            self.check()
            try:
                info = path.lstat()
            except FileNotFoundError:
                if not absent_link:
                    raise
                # Absence still observes the lexical target and every link edge.
                return None
        self.locate(path, "directory_slot")
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("dependency slot is not a directory")
        self.check()
        self.locate(path, "open_directory")
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
        os.close(fd)
        return path

    def alternates(self, repo: Path, targets: set[Path]) -> None:
        self.walk("alternates", repo, targets)

    def walk(self, kind, directory, targets):
        """Reuse a resolved walk's facts, including partial facts and errors.

        Link edges belong to each caller's directory() observation. These facts
        belong to the real directory and are replayed for every consumer alias.
        A fresh DependencyCensus under the locks always starts a new generation.
        """
        self.check()
        key = kind, directory
        if key not in self.walks:
            observed, error, result = set(), None, None
            try:
                result = getattr(self, "_" + kind)(directory, observed)
            except (CensusLimit, ScanDeadline):
                targets.update(observed)
                raise
            except (OSError, ValueError, RuntimeError) as exc:
                error = exc, self.probe_path, self.phase
            self.walks[key] = frozenset(observed), error, result
        observed, error, result = self.walks[key]
        targets.update(observed)
        if error:
            exc, self.probe_path, self.phase = error
            raise exc
        return result

    def _alternates(self, repo, targets):
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
        return self.walk("object_alternates", objects, targets)

    def _object_alternates(self, objects, targets):
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
        if parent in self.enumerations:
            yield from self.enumerations[parent]
            return
        children = []
        with os.scandir(parent) as entries:
            while True:
                self.locate(parent, "enumerate_container")
                self.check()
                entry = next(entries, None)
                if entry is None:
                    break
                child = parent / entry.name
                children.append(child)
                yield child
        self.enumerations[parent] = tuple(children)

    def reference(self, path, targets):
        try:
            self.read_reference(path, targets)
        except (KeyError, TypeError, SyntaxError) as exc:
            raise ValueError("invalid installed source reference") from exc

    def read_reference(self, path, targets):
        self.locate(path, "read_installed_source")
        self.check()
        data = read_regular(path)
        self.check()
        # Dispatch by file type, never by name: setuptools also names a plain
        # path-only .pth __editable__.<name>-<version>.pth.
        if path.name == "direct_url.json":
            url = urlsplit(json.loads(data.decode("utf-8"))["url"])
            if url.scheme != "file":
                return
            if url.netloc not in {"", "localhost"} or not url.path.startswith("/"):
                raise ValueError("unsupported installed source URL")
            values = [unquote(url.path)]
        elif path.suffix == ".pth":
            values = self.site_lines(path, data)
        elif path.suffix == ".py":
            values = path_constants(ast.parse(data.decode("utf-8")))
        else:
            # .egg-link: the project path, then its setup path.
            values = [line.strip() for line in data.decode("utf-8").splitlines() if line.strip()]
        for value in values:
            self.check()
            target = Path(value)
            if not target.is_absolute():
                target = path.parent / target
            resolved, traversed = self.resolve(target, absent=True)
            targets.update(traversed)
            targets.add(resolved)

    def site_lines(self, path, data):
        """Read one .pth as site.addpackage does, without executing any of it.

        '#' and blank lines are skipped. An import line is parsed for path
        constants and for the site-packages modules it names (e.g. a setuptools
        or editables finder), which are parsed in turn. Any other line is a path
        entry, joined to the site directory and normalized as site.makepath does.
        """
        sitedir, values = path.parent, []
        for line in data.decode("utf-8-sig").splitlines():
            self.locate(path, "read_installed_source")
            self.check()
            if line.startswith("#") or not line.strip():
                continue
            if not line.startswith(("import ", "import\t")):
                values.append(os.path.abspath(os.path.join(sitedir, line.rstrip())))
                continue
            statements = ast.parse(line)
            values.extend(path_constants(statements))
            for name in imported_modules(statements):
                parts = name.split(".")
                for depth in range(1, len(parts) + 1):
                    base = sitedir.joinpath(*parts[:depth])
                    for module in (base.with_name(base.name + ".py"), base / "__init__.py"):
                        self.locate(module, "read_installed_module")
                        self.check()
                        try:
                            module.lstat()
                        except (FileNotFoundError, NotADirectoryError):
                            # Not in this site directory: stdlib, builtin, or a
                            # path entry that its own .pth line already reports.
                            continue
                        values.extend(path_constants(ast.parse(read_regular(module).decode("utf-8"))))
        return values

    def installed_sources(self, venv, targets):
        self.walk("installed_sources", venv, targets)

    def _installed_sources(self, venv, targets):
        """Only named venv slots and one site-packages level, under shared caps."""
        binary = self.directory(venv / "bin", targets)
        if binary is not None:
            for child in self.children(binary):
                self.check()
                if child.is_symlink():
                    resolved, traversed = self.resolve(child)
                    targets.update(traversed)
                    targets.add(resolved)
        sites = set()
        for name in ("lib", "lib64"):
            library = self.directory(venv / name, targets)
            if library is None:
                continue
            for child in self.children(library):
                if not re.fullmatch(r"python[0-9]+\.[0-9]+[a-z]*", child.name):
                    continue
                python = self.directory(child, targets)
                if python is None:
                    raise ValueError("venv disappeared")
                packages = self.directory(python / "site-packages", targets)
                if packages is not None:
                    sites.add(packages)
        for packages in sorted(sites):
            for child in self.children(packages):
                self.check()
                if child.is_symlink():
                    resolved, traversed = self.resolve(child)
                    targets.update(traversed)
                    targets.add(resolved)
                if child.suffix in {".pth", ".egg-link"} or EDITABLE_FINDER.fullmatch(child.name):
                    self.reference(child, targets)
                elif child.name.endswith(".dist-info"):
                    metadata = self.directory(child, targets)
                    if metadata is None:
                        raise ValueError("metadata disappeared")
                    reference = metadata / "direct_url.json"
                    self.check()
                    try:
                        reference.lstat()
                    except FileNotFoundError:
                        continue
                    self.reference(reference, targets)

    def workspace(self, source, targets):
        self.walk("workspace", source, targets)

    def _workspace(self, source, targets):
        # Direct checkouts as well as repo/source/src aliases are native roots.
        # Only named container levels authorize enumeration, including when a
        # source/container link resolves outside root/home. Never recurse into
        # a member, its runtime, or any arbitrary symlink target.
        self.alternates(source, targets)
        for runtime in RUNTIME_PATHS:
            runtime_path = self.directory(source / runtime, targets, absent_link=True)
            if runtime_path is not None and self.sandbox_root is not None:
                self.installed_sources(runtime_path, targets)
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
                    runtime_path = self.directory(local / runtime, targets, absent_link=True)
                    if runtime_path is not None and self.sandbox_root is not None:
                        self.installed_sources(runtime_path, targets)

    def collect(self, root):
        # One bounded observation generation per scan. Rechecks call lenders
        # without this object, creating a fresh generation under apply.lock.
        if self.observations is not None:
            if self.cap_hit == "scan_deadline":
                raise ScanDeadline
            return
        self.start()
        candidate = self.sandbox_root or Path.home() / "workbay-sandbox"
        records, _ = sandbox_records(candidate)
        self.sandbox_root = candidate if records else None
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
            if self.sandbox_root is not None:
                for child in self.children(self.sandbox_root):
                    self.consumer_path = child
                    targets, error = set(), None
                    try:
                        self.check()
                        mode = child.lstat().st_mode
                        if child.name == ".workbay-release" or stat.S_ISREG(mode):
                            continue
                        local = self.directory(child, targets)
                        if local is None:
                            raise ValueError("sandbox consumer disappeared")
                        self.workspace(local, targets)
                        for name in SOURCE_PATHS:
                            source = self.directory(local / name, targets)
                            if source is not None:
                                self.workspace(source, targets)
                        if child.name.startswith(".venv-lane-"):
                            self.installed_sources(local, targets)
                    except CensusLimit:
                        raise
                    except (OSError, ValueError, RuntimeError) as exc:
                        error = self.diagnostic(exc)
                    self.observations.append((child, targets, error))
            self.check()
        except ScanDeadline:
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            self.incomplete = self.diagnostic(exc)
        finally:
            self.finished = time.monotonic()

    def errors(self, directory, excluding=()):
        errors = [error for child, _, error in self.observations or []
                  if child != directory and child not in excluding and error]
        if self.incomplete:
            errors.append(self.incomplete)
        return errors


def lenders(directory, root, deadline=None, census=None, excluding=()):
    """Observe ordinary and terminal consumers; hardlinks are independent entries.

    An unreadable/unsupported slot remains UNKNOWN, regardless of consumer UID.
    No manifest-authored empty dependency list replaces native observations.
    """
    census = census if census is not None else DependencyCensus(deadline)
    census.consumer_path = Path(directory)
    try:
        resolver = DependencyCensus(deadline)
        resolver.consumer_path = Path(directory)
        resolver.start()
        try:
            directory, _ = resolver.resolve(Path(directory))
        except (OSError, ValueError, RuntimeError) as exc:
            raise DependencyError(resolver.diagnostic(exc), resolver) from None
        census.collect(root)
    except ScanDeadline:
        raise
    except DependencyError:
        raise
    except (OSError, ValueError, RuntimeError) as exc:
        raise DependencyError(census.diagnostic(exc), census) from None
    errors = census.errors(directory, excluding)
    borrowers = sorted({child.name for child, targets, _ in census.observations
                        if child != directory and child not in excluding and any(inside(target, directory) for target in targets)})
    # A completed observation of a borrower remains proof even if later probes
    # exhaust the census. Missing observations never prove absence of borrowers.
    unknowns = [error for error in errors if error["error_type"] != "CensusLimit"]
    if unknowns or (errors and not borrowers):
        raise DependencyError((unknowns or errors)[0], census)
    return borrowers


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
    exhaustion = census.exhaustion() if census else None
    exhaustion = exhaustion or next((row["dependency_census"] for row in rows if "dependency_census" in row), None)
    return {"dependency_errors": sample, "dependency_errors_truncated": truncated,
            **({"dependency_census": exhaustion} if exhaustion else {})}


def released_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("released_at must include a UTC offset")
    return parsed.timestamp()


def valid_lease(directory, retention):
    lease = retention.get("lease")
    if (not isinstance(lease, dict) or not isinstance(retention.get("retention_class"), str)
            or retention["retention_class"] not in LEASE_DEFAULT_GRACE
            or lease.get("unit") != directory.name
            or type(lease.get("grace_seconds")) is not int or not 600 <= lease["grace_seconds"] <= 604800
            or not isinstance(lease.get("token"), str) or not re.fullmatch(r"[0-9a-f]+", lease["token"])):
        return None
    return lease


def valid_release(directory, retention):
    try:
        released = manifest(directory / "RELEASE.json")
        if released["owner"] != retention["owner"] or released.get("schema") != 1:
            return None
        released_time(released["released_at"])
        return released
    except (OSError, ValueError, KeyError, TypeError, OverflowError, AttributeError):
        return None


def clear_lease_stamp(directory):
    path = state_root() / "leases" / f"{directory.name}.json"
    if path.exists():
        path.unlink()
        fsync_dir(path.parent)


def exclusive_lease_release(directory, retention, data):
    """Fenced publication: a newer lease or any existing owner release wins."""
    destination = directory / "RELEASE.json"
    fd, temporary = tempfile.mkstemp(prefix=".RELEASE.json-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        current = manifest(directory / "RETENTION.json")
        lease = valid_lease(directory, current)
        if (lease != retention["lease"] or current["owner"] != retention["owner"]
                or current.get("no_reap", False) is not False):
            clear_lease_stamp(directory)
            return False
        try:
            os.link(temporary, destination)
        except FileExistsError:
            return False
        fsync_dir(directory)
        return True
    finally:
        os.unlink(temporary)


def observe_lease(directory, retention, journal):
    """Apply-only mark step. Suspicion is local, provisional and token-bound."""
    lease = valid_lease(directory, retention)
    if lease is None:
        clear_lease_stamp(directory)
        return False
    if os.path.lexists(directory / "RELEASE.json"):
        return False
    try:
        terminal = terminal_unit_state(directory)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        terminal = None
    if terminal is None:
        clear_lease_stamp(directory)
        return False
    path = state_root() / "leases" / f"{directory.name}.json"
    now = time.time()
    try:
        observation = json.loads(read_regular(path))
        first = released_time(observation["first_terminal_at"])
        if observation["token"] != lease["token"]:
            raise ValueError("new lease generation")
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(path, {"token": lease["token"],
                           "first_terminal_at": datetime.fromtimestamp(now, timezone.utc).isoformat()})
        return False
    if now - first < lease["grace_seconds"]:
        return False
    # Re-observe immediately before publication; an unavailable bus restarts grace.
    try:
        terminal = terminal_unit_state(directory)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        terminal = None
    if terminal is None:
        clear_lease_stamp(directory)
        return False
    data = {"schema": 1, "owner": retention["owner"], "reason": "lease_unit_terminal",
            "released_at": utc_now(), "lease_token": lease["token"], "unit_state": terminal,
            "first_terminal_at": observation["first_terminal_at"], "host": socket.gethostname()}
    if exclusive_lease_release(directory, retention, data):
        journal.append({"basename": directory.name, **data})
        return True
    # An owner release that won the publication race is also left for a fresh pass.
    return os.path.lexists(directory / "RELEASE.json")


def lender_details(directory, root, retention, row, census, deadline, explain):
    row["lender_released"] = valid_release(directory, retention) is not None
    row["blocking_borrowers"] = []
    if not explain:
        return
    borrowers = {child.name: child for child, targets, _ in census.observations or []
                 if child != directory and any(inside(target, directory) for target in targets)}
    for name in sorted(borrowers)[:10]:
        borrower = classify(borrowers[name], root, read_only=True, deadline=deadline,
                            dependency_census=census, explain_borrowers=False)
        row["blocking_borrowers"].append({key: borrower[key] for key in ("basename", "verdict", "reason")})


def classify(directory, root, read_only=False, deadline=None, dependency_census=None, sandbox_root=None,
             lease_released=None, explain_borrowers=True):
    row = {"basename": directory.name, "path": str(directory), "lends_to": []}

    def verdict(kind, reason):
        if kind == "hold_lender":
            lender_details(directory, root, retention, row, dependency_census, deadline, explain_borrowers)
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
    row.update(owner=retention["owner"], created_at=retention.get("created_at"),
               retention_class=retention.get("retention_class"), leased=valid_lease(directory, retention) is not None)
    if "no_reap" in retention and not isinstance(retention["no_reap"], bool):
        return verdict("hold_unmarked", "retention_invalid")
    if retention.get("no_reap") is True:
        return verdict("hold_not_released", "retention_pinned")
    hold = activity(directory, read_only=True, deadline=deadline) if read_only else activity(directory)
    if hold and hold[0] in {"hold_unit_active", "unclassified"}:
        if lease_released is not None and not read_only:
            clear_lease_stamp(directory)
        return verdict(*hold)
    # Finish activity's unit observation before the lease's final verification
    # and publication. Locks/cwds still hold deletion even when a lease marks.
    auto_released = (observe_lease(directory, retention, lease_released)
                     if lease_released is not None and not read_only else False)
    if hold:
        return verdict(*hold)
    dependency_census = dependency_census if dependency_census is not None else DependencyCensus(deadline, sandbox_root)
    try:
        row["lends_to"] = lenders(directory, root, deadline=deadline, census=dependency_census)
    except ScanDeadline:
        if dependency_census.exhaustion():
            row["dependency_census"] = dependency_census.exhaustion()
        return verdict("unclassified", "scan_deadline")
    except DependencyError as exc:
        row["dependency_error"] = exc.diagnostic
        if exc.exhaustion:
            row["dependency_census"] = exc.exhaustion
        return verdict("hold_lender", "dependency_census_exhausted"
                       if exc.diagnostic["error_type"] == "CensusLimit" else "dependency_census_unknown")
    except (OSError, ValueError, RuntimeError) as exc:
        row["dependency_error"] = {"consumer_path": str(directory)[:DEPENDENCY_ERROR_TEXT_CAP],
                                   "probe_path": str(directory)[:DEPENDENCY_ERROR_TEXT_CAP],
                                   "phase": "lenders", "error_type": type(exc).__name__}
        return verdict("hold_lender", "dependency_census_unknown")
    if dependency_census.exhaustion():
        row["dependency_census"] = dependency_census.exhaustion()
    if row["lends_to"]:
        return verdict("hold_lender", "native_dependency_borrowed")
    if auto_released:
        return verdict("hold_not_released", "lease_released")
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


def sandbox_records(root):
    """Discover records only; an absent release namespace enables no new walk."""
    namespace = root / ".workbay-release"
    try:
        if not stat.S_ISDIR(root.lstat().st_mode):
            raise ValueError("sandbox root is not a real directory")
        info = namespace.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
            raise ValueError("release namespace is not a private real directory")
        return sorted(namespace.glob("*.json")), None
    except FileNotFoundError:
        return [], None
    except (OSError, ValueError) as exc:
        return [], str(exc)


def lane_fields(summary):
    summary.setdefault("lane_sandbox_released", 0)
    summary.setdefault("lane_sandbox_reclaimed", {"count": 0, "bytes": 0})
    summary.setdefault("lane_sandbox_holds", {})
    summary.setdefault("lane_sandbox_assessments", [])


def lane_hold(summary, key, kind, reason, **detail):
    lane_fields(summary)
    summary["lane_sandbox_holds"].setdefault(kind, []).append({
        "key": key, "owner": "workbay-sandbox-reap", "reason": reason, **detail,
    })


def lane_record(path, root):
    data = json.loads(read_regular(path))
    if not isinstance(data, dict):
        raise ValueError("release is not an object")
    key = data["key"]
    if (type(data.get("schema")) is not int or data["schema"] != 1
            or data.get("kind") != "lane_sandbox_release"
            or data.get("owner") != "workbay-sandbox-reap"
            or data.get("reason") != "completion_proven"
            or not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9._-]+", key)
            or key in {".", ".."} or path.name != key + ".json"):
        raise ValueError("release identity invalid")
    for field in ("proven_tip", "proven_tree"):
        if not isinstance(data.get(field), str) or not re.fullmatch(r"[0-9a-f]{40}", data[field]):
            raise ValueError("release git proof invalid")
    for field in ("proven_generation", "proven_artifacts", "host", "sweep_run_id"):
        if not isinstance(data.get(field), str) or not data[field]:
            raise ValueError("release proof missing")
    if not isinstance(data.get("proven_delta"), str):
        raise ValueError("release delta proof missing")
    if not isinstance(data.get("released_at"), str):
        raise ValueError("release time missing")
    released_time(data["released_at"])
    paths = data["paths"]
    allowed = {str(root / key), str(root / f".venv-lane-{key}"), str(root / f".venv-sync-stamp-{key}")}
    if (not isinstance(paths, list) or not paths or any(not isinstance(p, str) for p in paths)
            or len(paths) != len(set(paths)) or not set(paths) <= allowed or str(root / key) not in paths):
        raise ValueError("release paths invalid")
    for value in paths:
        info = Path(value).lstat()
        expected = stat.S_ISREG if Path(value).name == f".venv-sync-stamp-{key}" else stat.S_ISDIR
        if not expected(info.st_mode):
            raise ValueError("release path is not a real allowed child")
    return data


def lane_locks(root, key, stack, read_only):
    for name in (f".lane-lock-{key}", f".lane-lease-{key}.lock"):
        try:
            fd = os.open(root / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                         | (0 if read_only else os.O_CREAT), 0o600)
        except FileNotFoundError:
            if read_only:
                continue
            raise
        except OSError:
            raise Refusal("hold_lane_locked", "lane_lock_unreadable") from None
        stack.callback(os.close, fd)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise Refusal("hold_lane_locked", "lane_lock_not_regular")
        if read_only:
            if observed_lock_busy(os.fstat(fd)):
                raise Refusal("hold_lane_locked", "lane_lock_busy")
        else:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise Refusal("hold_lane_locked", "lane_lock_busy") from None


def lane_revalidate(record, root, deadline=None):
    directory = root / record["key"]
    def probe(*args):
        left = 30 if deadline is None else min(30, deadline - time.monotonic())
        if left <= 0:
            raise Refusal("hold_lender", "dependency_census_unknown")
        result = run(["git", "--no-optional-locks", "-C", str(directory), *args], timeout=left)
        if result.returncode:
            raise Refusal("hold_release_unknown", "git_proof_unavailable")
        return result.stdout.strip()
    try:
        if probe("rev-parse", "HEAD") != record["proven_tip"]:
            raise Refusal("hold_release_stale", "head_mismatch")
        if probe("rev-parse", "HEAD^{tree}") != record["proven_tree"]:
            raise Refusal("hold_release_stale", "tree_mismatch")
        if probe("status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"):
            raise Refusal("hold_release_stale", "worktree_dirty")
    except subprocess.TimeoutExpired:
        # An observation that timed out is not a stale proof.
        raise Refusal("hold_lender", "dependency_census_unknown") from None


def excluded_opaque_task(process, census):
    """Contract v1.3: exact cgroup-v2 authority, narrowed by UID and argv."""
    def read(name):
        census.locate(process / name, "opaque_process_identity")
        census.check()
        return read_regular(process / name)

    try:
        uid = os.geteuid()
        status = read("status").decode("utf-8").splitlines()
        uids = [line.split()[1:] for line in status if line.startswith("Uid:")]
        if len(uids) != 1 or len(uids[0]) != 4 or any(not value.isdecimal() for value in uids[0]):
            return None
        if [int(value) for value in uids[0][:2]] != [uid, uid]:
            return None
        groups = read("cgroup").decode("utf-8").splitlines()
        if len(groups) != 1 or not groups[0].startswith("0::"):
            return None
        group = groups[0][3:]
        comm = read("comm").decode("utf-8").removesuffix("\n")
        argv = read("cmdline").split(b"\0")
        prefix = f"/user.slice/user-{uid}.slice/"
        if group == prefix + f"user@{uid}.service/init.scope":
            kind = "user_manager"
        elif group == prefix + f"user@{uid}.service/app.slice/gpg-agent.service":
            if any(arg == b"--homedir" or arg.startswith(b"--homedir=") for arg in argv):
                return None
            kind = "gpg_agent"
        elif re.fullmatch(re.escape(prefix) + r"session-[A-Za-z0-9]+\.scope", group):
            # tailscaled execs its SSH session child by absolute path (/usr/sbin/tailscaled);
            # only the final path component is compared, and only for equality.
            if not ((comm == "tailscaled" and argv[0].rsplit(b"/", 1)[-1] == b"tailscaled"
                     and argv[1:3] == [b"be-child", b"ssh"])
                    or (comm == "sshd" and argv[0].startswith(b"sshd:"))):
                return None
            kind = "ssh_session_daemon"
        else:
            return None
        return {"pid": int(process.name), "comm": comm, "class": kind}
    except (CensusLimit, ScanDeadline):
        raise
    except (OSError, ValueError, RuntimeError):
        return None


def lane_live(paths, census):
    """Owned reference facets hold unless an opaque task meets contract v1.2."""
    def referenced(value):
        if not value.startswith("/"):
            return False
        return any(inside(Path(value.removesuffix(" (deleted)")), path) for path in paths)

    try:
        for process in census.children(Path("/proc")):
            if not process.name.isdigit():
                continue
            owned = None
            try:
                census.check()
                owned = process.stat().st_uid == os.geteuid()
                for slot in ("cwd", "exe"):
                    census.check()
                    try:
                        if referenced(os.readlink(process / slot)):
                            return {"reason": "process_reference_inside", "borrower": f"pid:{process.name}"}
                    except FileNotFoundError:
                        # Zombie processes have no cwd/exe; disappearing PIDs are idle.
                        continue
                for fd in census.children(process / "fd"):
                    try:
                        if referenced(os.readlink(fd)):
                            return {"reason": "process_reference_inside", "borrower": f"pid:{process.name}"}
                    except FileNotFoundError:
                        continue
                census.locate(process / "maps", "read_process_maps")
                census.check()
                for line in read_regular(process / "maps").decode("utf-8").splitlines():
                    census.check()
                    fields = line.split(None, 5)
                    if len(fields) == 6:
                        value = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[5])
                        if referenced(value):
                            return {"reason": "process_reference_inside", "borrower": f"pid:{process.name}"}
            except (FileNotFoundError, ProcessLookupError):
                continue
            except (CensusLimit, ScanDeadline):
                raise
            except (OSError, ValueError, RuntimeError) as exc:
                if owned is False:
                    continue
                if owned and isinstance(exc, OSError) and exc.errno in {errno.EACCES, errno.EPERM}:
                    excluded = excluded_opaque_task(process, census)
                    if excluded:
                        census.excluded_opaque.append(excluded)
                        continue
                return {"reason": "process_census_unknown", "borrower": f"pid:{process.name}",
                        "dependency_error": census.diagnostic(exc)}
    except (OSError, ValueError, RuntimeError, ScanDeadline) as exc:
        return {"reason": "process_census_unknown", "dependency_error": census.diagnostic(exc)}
    return None


def lane_assess(record, root, jobs_root, deadline=None, assessment=None):
    census = DependencyCensus(deadline, sandbox_root=root)
    dependency_probes = None
    try:
        lane_revalidate(record, root, deadline)
        paths = [Path(p) for p in record["paths"]]
        borrowers = set()
        try:
            for path in paths:
                borrowers.update(lenders(path, jobs_root, deadline, census, excluding=paths))
        except (DependencyError, ScanDeadline) as exc:
            return "hold_lender", {"reason": "dependency_census_unknown",
                                   "dependency_error": getattr(exc, "diagnostic", census.diagnostic(exc))}
        dependency_probes = census.probes
        if borrowers:
            return "hold_lender", {"reason": "native_dependency_borrowed", "lends_to": sorted(borrowers),
                                   "borrower": ", ".join(sorted(borrowers))}
        live = lane_live(paths, census)
        if live:
            return "hold_live", {**live, "excluded_opaque": census.excluded_opaque}
        return None
    finally:
        if assessment is not None:
            assessment.update(excluded_opaque=census.excluded_opaque, probes=census.probes,
                              dependency_probes=census.probes if dependency_probes is None else dependency_probes,
                              cap_hit=census.cap_hit)


def lane_cleanup_record(receipt, completing_intent):
    # All names were validated before their durable intents were published.
    # The record is the last thing removed, including during recovery.
    if receipt.get("lane_sandbox_key") and not any(os.path.lexists(p) for p in receipt["lane_release_paths"]):
        record = Path(receipt["lane_release_record"])
        # A failed deletion can leave other tombstones whose live names are gone.
        for intent in state_root().glob("pending-*.json"):
            if intent == completing_intent:
                continue
            data = json.loads(read_regular(intent))
            if data.get("lane_release_record") == str(record):
                return
        try:
            if json.loads(read_regular(record)) != receipt["lane_release_proof"]:
                return  # A writer has published a newer release for the same key.
        except FileNotFoundError:
            return
        record.unlink()
        fsync_dir(record.parent)


def recover_lane_sandboxes(root, state, summary, limit, budget, retry_failed):
    groups = {}
    for intent in state.glob("pending-*.json"):
        receipt = json.loads(read_regular(intent))
        if receipt.get("root") == str(root) and receipt.get("lane_sandbox_key"):
            groups.setdefault(receipt["lane_sandbox_key"], []).append((intent, receipt))
    remaining = []
    for key, group in sorted(groups.items()):
        try:
            with ExitStack() as stack:
                lane_locks(root, key, stack, False)
                proof = group[0][1]
                # A crash can interrupt the batch of renames. Before deletion has
                # begun every path still exists under one of its two names. Put
                # those names back, then perform the full validation/census again.
                if any(os.path.lexists(path) for path in proof["lane_release_paths"]):
                    if json.loads(read_regular(Path(proof["lane_release_record"]))) != proof["lane_release_proof"]:
                        raise Refusal("hold_recovery", "release_changed_during_recovery")
                    for _, receipt in group:
                        live, tomb = root / receipt["basename"], root / receipt["tombstone"]
                        if os.path.lexists(live) == os.path.lexists(tomb):
                            raise Refusal("hold_recovery", "ambiguous_partial_rename")
                        found = live if os.path.lexists(live) else tomb
                        info = found.lstat()
                        if [info.st_dev, info.st_ino] != receipt["source_identity"]:
                            raise Refusal("hold_recovery", "recovery_path_changed")
                    for _, receipt in group:
                        live, tomb = root / receipt["basename"], root / receipt["tombstone"]
                        if os.path.lexists(tomb):
                            os.rename(tomb, live)
                            fsync_dir(root)
                    for intent, _ in group:
                        intent.unlink()
                    fsync_dir(state)
                else:
                    remaining += recover(root, state, summary, limit, budget, retry_failed,
                                         intents_only=True, only_key=key)
        except (OSError, Refusal) as exc:
            lane_hold(summary, key, getattr(exc, "kind", "hold_recovery"), str(exc))
    return remaining


def lane_sandboxes(root, jobs_root, summary, *, read_only=False, deadline=None, budget=None, limit=10):
    records, error = sandbox_records(root)
    if not records and error is None:
        return
    lane_fields(summary)
    summary["lane_sandbox_released"] = max(summary["lane_sandbox_released"], len(records))
    reclaimed_before = summary["lane_sandbox_reclaimed"]["count"]
    if error:
        lane_hold(summary, ".workbay-release", "hold_release_invalid", error)
        return
    for record_path in records:
        key = record_path.stem
        assessment = {"key": key, "excluded_opaque": [], "probes": 0, "dependency_probes": 0, "cap_hit": None}
        summary["lane_sandbox_assessments"].append(assessment)
        try:
            record = lane_record(record_path, root)
            with ExitStack() as stack:
                lane_locks(root, key, stack, read_only)
                # The writer uses these locks too. Do not act on a replaced record.
                if lane_record(record_path, root) != record:
                    raise Refusal("hold_release_invalid", "release_changed")
                hold = lane_assess(record, root, jobs_root, deadline, assessment)
                if hold:
                    kind, detail = hold
                    assessment.update(decision=kind, **detail)
                    lane_hold(summary, key, kind, **detail)
                    continue
                assessment.update(decision="reclaimable", reason="release_revalidated")
                if read_only:
                    continue
                if summary["lane_sandbox_reclaimed"]["count"] - reclaimed_before >= limit:
                    summary["remaining"].append(key)
                    continue
                paths = [Path(value) for value in record["paths"]]
                identities = [path.lstat() for path in paths]
                sizes = [budget.size(path) for path in paths]
                hold = lane_assess(record, root, jobs_root, deadline, assessment)
                if hold:
                    kind, detail = hold
                    assessment.update(decision=kind, **detail)
                    lane_hold(summary, key, kind, **detail)
                    continue
                for path, identity in zip(paths, identities):
                    current = path.lstat()
                    if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
                        raise Refusal("hold_release_invalid", "release_path_changed")
                pending = []
                for path, identity, (allocated, measurement_error) in zip(paths, identities, sizes):
                    tomb = root / f".reclaiming-{path.name}-{os.getpid()}"
                    if os.path.lexists(tomb):
                        raise Refusal("hold_release_invalid", "tombstone_exists")
                    reclaim_id = f"{root}/{tomb.name}/{utc_now()}"
                    receipt = {
                        "root": str(root), "tombstone": tomb.name, "basename": path.name,
                        "owner": record["owner"], "released_at": record["released_at"],
                        "bytes_freed": allocated, "reclaim_id": reclaim_id,
                        "lane_sandbox_key": key, "lane_sandbox_count": int(path.name == key),
                        "lane_release_record": str(record_path), "lane_release_paths": record["paths"],
                        "lane_release_proof": record,
                        "source_identity": [identity.st_dev, identity.st_ino],
                        "excluded_opaque": assessment["excluded_opaque"],
                    }
                    if measurement_error:
                        receipt["measurement_error"] = measurement_error
                    intent = state_root() / f"pending-{hashlib.sha256(reclaim_id.encode()).hexdigest()}.json"
                    atomic_json(intent, receipt)
                    pending.append((path, tomb, intent, receipt))
                # All paths disappear from their live names before any deletion.
                for path, tomb, _, _ in pending:
                    resource_operation("rename", os.rename, path, tomb)
                    fsync_dir(root)
                for _, tomb, intent, receipt in pending:
                    try:
                        finish_delete(tomb, state_root(), intent, receipt)
                    except ForeignOwner as exc:
                        request_owner_repair(tomb, intent, receipt, exc, summary)
                        continue
                    except OSError:
                        summary["recover_failed"].append(tomb.name)
                        lane_hold(summary, key, "hold_recovery", "delete_failed", tombstone=tomb.name)
                        continue
                    count_freed(summary, receipt)
        except Refusal as exc:
            assessment.update(decision=exc.kind, reason=str(exc))
            lane_hold(summary, key, exc.kind, str(exc))
            if exc.kind == "hold_release_stale" and not read_only:
                record_path.unlink(missing_ok=True)
                fsync_dir(record_path.parent)
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            # Unexpected termination during deletion must leave intents for recovery.
            if isinstance(exc, RuntimeError):
                raise
            lane_hold(summary, key, "hold_release_invalid", type(exc).__name__)
            assessment.update(decision="hold_release_invalid", reason=type(exc).__name__)


def scan(root=None, read_only=False, deadline=None, sandbox_root=None, lease_released=None):
    """Classify every job dir; past a monotonic deadline, report the rest unclassified."""
    root = Path(root or Path.home()).expanduser().resolve()
    rows = []
    census = DependencyCensus(deadline, sandbox_root)
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
        rows.append(classify(child, root, read_only=read_only, deadline=deadline, dependency_census=census,
                             lease_released=lease_released))
    counts = dict.fromkeys(VERDICTS, 0)
    counts.update(Counter(row["verdict"] for row in rows))
    blocked = blocked_foreign_owners(root)
    diagnostics = dependency_diagnostics(rows, census)
    return {"root": str(root), "entries": rows, "counts": counts,
            "health": "degraded" if blocked or diagnostics.get("dependency_census") else "ok",
            "recover_blocked_foreign_owner": blocked, **diagnostics}


def starvation(rows):
    held = [row for row in rows if row["verdict"] == "hold_not_released"]
    if not held:
        return None

    def age(row):
        try:
            return released_time(row["created_at"]), row["basename"]
        except (ValueError, TypeError, AttributeError, OverflowError):
            return float("inf"), row["basename"]

    return {"hold_not_released": len(held),
            "release_missing": sum(row["reason"] == "release_missing" for row in held),
            "leased_waiting": sum(row.get("leased", False) for row in held),
            "oldest": [{key: row.get(key) for key in
                        ("basename", "owner", "created_at", "retention_class", "leased")}
                       for row in sorted(held, key=age)[:10]]}


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
    if receipt.get("lane_sandbox_key"):
        lane_fields(summary)
        summary["lane_sandbox_reclaimed"]["count"] += receipt["lane_sandbox_count"]
        if receipt["bytes_freed"] is not None:
            summary["lane_sandbox_reclaimed"]["bytes"] += receipt["bytes_freed"]
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
    sandbox_root=None,
):
    """Report observed state only; never create state, acquire locks or reclaim."""
    started = time.monotonic()
    root = Path(root or Path.home()).expanduser().resolve()
    sandbox_options = {} if sandbox_root is None else {"sandbox_root": sandbox_root}
    # A hung user bus costs each marked dir up to 20 s, so the scan shares the
    # deadline too; a 0 budget skips du only, and the scan keeps the default.
    deadline = started + (measure_budget_seconds or STATUS_MEASURE_BUDGET_SECONDS)
    free = disk_free(root, timeout=max(0, min(30, deadline - time.monotonic())))
    scanned = scan(root, read_only=True, deadline=deadline, **sandbox_options)
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
            or value.get("outcome") not in {"ok", "degraded", "error"}
            or value.get("trigger") not in {"timer", "headroom", "manual"}
            or not isinstance(value.get("counts"), dict)
            or any(not isinstance(value.get(key), int) for key in ("reclaimed", "recovered", "bytes_freed"))
            or not isinstance(value.get("recover_failed", []), list)
            or any(not isinstance(name, str) for name in value.get("recover_failed", []))
            or any(
                key not in value or (value[key] is not None and not isinstance(value[key], int))
                for key in ("df_free_before", "df_free_after")
            )
            or (value.get("outcome") in {"ok", "degraded"} and value.get("error", True) is not None)
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
    if scanned["recover_blocked_foreign_owner"]:
        alarms.append("recover_blocked_foreign_owner")
    exhaustion = scanned.get("dependency_census") or (last_run or {}).get("dependency_census")
    if exhaustion:
        alarms.append("census_exhausted")
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
    lanes = {}
    lane_sandboxes(Path(sandbox_root or Path.home() / "workbay-sandbox").expanduser().absolute(),
                   root, lanes, read_only=True, deadline=deadline)
    if (last_run or {}).get("lane_sandbox_reclaimed"):
        lane_fields(lanes)
        lanes["lane_sandbox_reclaimed"] = last_run["lane_sandbox_reclaimed"]
    # Starvation describes a completed pass, never an unattempted scan.
    starved = (last_run or {}).get("starved")
    if not isinstance(starved, dict):
        starved = None
    if starved:
        alarms.append("reclaim_starved")
    return {
        **lanes,
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
        **({"health": "degraded", "recover_blocked_foreign_owner": scanned["recover_blocked_foreign_owner"]}
           if scanned["recover_blocked_foreign_owner"] else {}),
        **({"health": "degraded", "dependency_census": exhaustion} if exhaustion else {}),
        **({"health": "starved", "starved": starved} if starved else {}),
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


def blocked_foreign_owners(root):
    """Read current pending diagnostics; vanished tombstones no longer degrade scan."""
    blocked = []
    for intent in state_root().glob("pending-*.json"):
        try:
            data = json.loads(read_regular(intent))
            diagnostic = data.get("recover_blocked_foreign_owner")
            tomb = data.get("tombstone", "")
            if (data.get("root") == str(root) and Path(tomb).name == tomb
                    and tomb.startswith(".reclaiming-workbay-")
                    and (root / tomb).is_dir() and not (root / tomb).is_symlink()
                    and isinstance(diagnostic, dict) and isinstance(diagnostic.get("path"), str)
                    and type(diagnostic.get("owner_uid")) is int):
                blocked.append(diagnostic)
        except (OSError, ValueError, TypeError, AttributeError):
            continue
    return blocked


def request_owner_repair(target, intent, receipt, error, summary):
    if receipt.get("lane_sandbox_key"):
        atomic_json(intent, {**receipt, "recover_blocked_foreign_owner": error.diagnostic})
        summary["recover_blocked_foreign_owner"].append(error.diagnostic)
        lane_hold(summary, receipt["lane_sandbox_key"], "hold_recovery", "foreign_owner",
                  **error.diagnostic)
        return
    queue = Path.home() / ".local/state/workbay-reclaim/owner-repair"
    queue.mkdir(parents=True, exist_ok=True)
    atomic_json(queue / f"{target.name}.json", {"schema": 1, "tombstone": target.name})
    atomic_json(intent, {**receipt, "recover_blocked_foreign_owner": error.diagnostic})
    summary["recover_blocked_foreign_owner"].append(error.diagnostic)


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
        def check_owner(info, entry):
            if info.st_uid != os.geteuid():
                raise ForeignOwner(entry, info.st_uid)

        check_owner(target.lstat(), target)
        fd = os.open(target, flags)
        try:
            # Walk from the tombstone by directory fd, never through a symlink.
            # Only the failed operation's parent and directory itself get u+wx.
            traversed = target
            for part in parts[:-1]:
                traversed = traversed / part
                info = os.lstat(part, dir_fd=fd)
                check_owner(info, traversed)
                if not stat.S_ISDIR(info.st_mode):
                    raise error
                child = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            if parts:
                check_owner(os.lstat(parts[-1], dir_fd=fd), path)
            check_owner(os.fstat(fd), path.parent if parts else target)
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

    if receipt.get("lane_sandbox_key") and target.is_file() and not target.is_symlink():
        resource_operation("delete", target.unlink)
        fsync_dir(target.parent)
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
    if receipt.get("lane_sandbox_key"):
        lane_cleanup_record(receipt, intent)
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


def recover(root, state, summary, limit, budget, retry_failed=False, intents_only=False, only_key=None):
    """Finish tombstones before scanning jobs; durable intents preserve receipts."""
    intents = {}
    for intent in state.glob("pending-*.json"):
        data = json.loads(intent.read_text(encoding="utf-8"))
        if (data["root"] == str(root) and (not intents_only or data.get("lane_sandbox_key"))
                and (only_key is None or data.get("lane_sandbox_key") == only_key)):
            intents[root / data["tombstone"]] = (intent, data)
    targets = sorted((set() if intents_only else set(root.glob(".reclaiming-*"))) | set(intents))
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
            if (os.path.lexists(target) and not retry_failed
                    and not receipt.get("recover_blocked_foreign_owner") and recover_backoff(receipt, time.time())):
                # Still not deleted, so still reported; only the rmtree retry waits.
                summary["recover_failed"].append(target.name)
                summary["recover_deferred"].append(target.name)
                if receipt.get("lane_sandbox_key"):
                    lane_hold(summary, receipt["lane_sandbox_key"], "hold_recovery", "retry_deferred",
                              tombstone=target.name)
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
        except ForeignOwner as exc:
            request_owner_repair(target, intent, receipt, exc, summary)
            continue
        except (Refusal, OSError):
            if not os.path.lexists(target):
                raise
            # An undeletable tombstone keeps its intent for the next pass; later
            # tombstones and the scan still run, and the pass outcome reports it.
            summary["recover_failed"].append(target.name)
            if receipt.get("lane_sandbox_key"):
                lane_hold(summary, receipt["lane_sandbox_key"], "hold_recovery", "delete_failed",
                          tombstone=target.name)
            # This attempt failed for a different reason; restore ordinary
            # recovery backoff instead of retaining a stale foreign-owner hold.
            receipt.pop("recover_blocked_foreign_owner", None)
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
        clear_lease_stamp(root / receipt["basename"])
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
            **{key: summary[key] for key in
               ("lane_sandbox_released", "lane_sandbox_reclaimed", "lane_sandbox_holds",
                "lane_sandbox_assessments") if key in summary},
            **{key: summary[key] for key in ("starved", "lease_released") if summary.get(key)},
            "error": error,
            **({"resource_failed": summary["resource_failed"]} if summary.get("resource_failed") else {}),
            **({"recover_blocked_foreign_owner": summary["recover_blocked_foreign_owner"]}
               if summary.get("recover_blocked_foreign_owner") else {}),
            **({"dependency_census": summary["dependency_census"]} if summary.get("dependency_census") else {}),
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
    sandbox_root=None,
):
    root = Path(root or Path.home()).expanduser().resolve()
    sandbox_options = {} if sandbox_root is None else {"sandbox_root": sandbox_root}
    sandbox_root = Path(sandbox_root or Path.home() / "workbay-sandbox").expanduser().absolute()
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
        "recover_blocked_foreign_owner": [],
        "resource_failed": [],
        "df_free_before": None,
        "df_free_after": None,
        "df_delta_bytes": None,
        "remaining": [],
        "held_on_recheck": [],
        "lease_released": [],
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
            released_records, namespace_error = sandbox_records(sandbox_root)
            if released_records or namespace_error:
                lane_fields(summary)
                summary["lane_sandbox_released"] = len(released_records)
            recovery_remaining = recover(root, state, summary, max_dirs, budget, retry_recover_failed)
            if sandbox_root.is_dir() and not sandbox_root.is_symlink():
                recovery_remaining += recover_lane_sandboxes(sandbox_root, state, summary, max_dirs,
                                                              budget, retry_recover_failed)
            census_started = time.monotonic()
            scanned = scan(root, False, None, sandbox_options.get("sandbox_root"), summary["lease_released"])
            budget.charge(census_started)
            summary["counts"] = scanned["counts"]
            summary.update({key: scanned[key] for key in
                            ("dependency_errors", "dependency_errors_truncated", "dependency_census") if key in scanned})
            eligible = sorted(
                (r for r in scanned["entries"] if r["verdict"] == "reclaimable"),
                key=lambda r: (released_time(r["released_at"]), r["basename"]),
            )
            available = max(0, max_dirs - summary["recovered"])
            summary["remaining"] = recovery_remaining + [r["basename"] for r in eligible[available:]]
            references = {}
            for lender in scanned["entries"]:
                if lender["verdict"] == "hold_lender":
                    for borrower in lender["lends_to"]:
                        references.setdefault(borrower, set()).add(lender["basename"])
            pending = eligible[:available]
            while pending:
                deleted = []
                for row in pending:
                    try:
                        directory = root / row["basename"]
                        identity = directory.lstat()
                        census_started = time.monotonic()
                        checked = classify(directory, root, **sandbox_options)
                        budget.charge(census_started)
                        if checked["verdict"] != "reclaimable":
                            summary["held_on_recheck"].append(checked)
                            diagnostic = dependency_diagnostics([{"dependency_error": e} for e in summary["dependency_errors"]] + [checked])
                            summary["dependency_errors"] = diagnostic["dependency_errors"]
                            summary["dependency_errors_truncated"] |= diagnostic["dependency_errors_truncated"]
                            if "dependency_census" in diagnostic:
                                summary["dependency_census"] = diagnostic["dependency_census"]
                            continue
                        evidence = archive_evidence(directory, archive_root, int(evidence_cap_mb * 1024 * 1024))
                        allocated, measurement_error = budget.size(directory)
                        # Archive/du can take time: recheck local activity and owner authority
                        # again immediately before making the job disappear from its live name.
                        census_started = time.monotonic()
                        checked = classify(directory, root, **sandbox_options)
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
                            if "dependency_census" in diagnostic:
                                summary["dependency_census"] = diagnostic["dependency_census"]
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
                        try:
                            finish_delete(tomb, state, intent, receipt)
                        except OSError as exc:
                            if isinstance(exc, PermissionError) or not os.path.lexists(tomb):
                                raise
                            atomic_json(intent, {**receipt, "recover_failures": 1, "recover_failed_at": utc_now()})
                            summary["recover_failed"].append(tomb.name)
                            continue
                        summary["reclaimed"] += 1
                        count_freed(summary, receipt)
                        clear_lease_stamp(directory)
                        deleted.append(row["basename"])
                    except ForeignOwner as exc:
                        request_owner_repair(tomb, intent, receipt, exc, summary)
                        continue
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
                affected = sorted({name for borrower in deleted for name in references.get(borrower, ())})
                if not affected:
                    break
                available = max_dirs - summary["reclaimed"] - summary["recovered"]
                if available <= 0:
                    summary["remaining"].extend(name for name in affected if name not in summary["remaining"])
                    break
                # Only lenders of a borrower just deleted are revisited. Each
                # batch and every destructive recheck use fresh native facts.
                census_started = time.monotonic()
                census = DependencyCensus(sandbox_root=sandbox_root)
                marked = {row["basename"] for row in summary["lease_released"]}
                candidates = [classify(root / name, root, dependency_census=census, **sandbox_options)
                              for name in affected if name not in marked]
                budget.charge(census_started)
                pending = sorted((row for row in candidates if row["verdict"] == "reclaimable"),
                                 key=lambda row: (released_time(row["released_at"]), row["basename"]))
                summary["remaining"].extend(row["basename"] for row in pending[available:]
                                            if row["basename"] not in summary["remaining"])
                pending = pending[:available]
            lane_sandboxes(sandbox_root, root, summary, budget=budget,
                           limit=max(0, max_dirs - summary["reclaimed"] - summary["recovered"]))
            if (summary["reclaimed"] + summary["recovered"] == 0
                    and not summary.get("lane_sandbox_reclaimed", {}).get("count")):
                starved = starvation(scanned["entries"])
                if starved:
                    summary["starved"] = starved
                    print("RECLAIM_STARVED " + json.dumps(starved, sort_keys=True), file=sys.stderr)
            summary["df_free_after"] = disk_free(root)
            summary["df_delta_bytes"] = summary["df_free_after"] - summary["df_free_before"]
            error = None
            if summary["recover_blocked_foreign_owner"] or summary.get("dependency_census"):
                summary["outcome"] = "degraded"
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
    allocate_parser.add_argument("--release-on-unit-exit", action="store_true",
                                 help="authorize release after verified unit termination plus grace")
    allocate_parser.add_argument("--grace-seconds", type=int,
                                 help="lease grace: 600..604800 seconds (lane/gate/qa: 21600; scratch: 3600)")
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
    status_parser.add_argument("--sandbox-root", help="lane sandbox root (default: ~/workbay-sandbox)")
    apply_parser = commands.add_parser(
        "apply",
        help="archive and reclaim released jobs",
        epilog="exit 0: ok, busy or skipped; 2: error; 3: the pass left recover_failed tombstones or resource permission failures, or a manual/headroom pass was reclaim_starved. Timer starvation exits 0 and logs RECLAIM_STARVED. "
        f"After {RECOVER_RETRY_FAILURES} consecutive failures a tombstone is retried at most once per "
        f"{RECOVER_BACKOFF_SECONDS // 3600} h; it stays in recover_failed (and exit 3) meanwhile.",
    )
    apply_parser.add_argument("--trigger", choices=["timer", "headroom", "manual"],
                              help="invocation source; timer starvation exits 0. Without this option, "
                              "systemd INVOCATION_ID identifies the installed timer service; "
                              "other calls use manual starvation exit 3. Legacy receipts default to timer.")
    apply_parser.add_argument("--root", default=str(Path.home()))
    apply_parser.add_argument("--max", type=nonnegative, default=10)
    apply_parser.add_argument("--sandbox-root", help="lane sandbox root (default: ~/workbay-sandbox)")
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
            output = allocate(args.directory, args.owner, args.retention_class, args.task_ref, args.note, args.adopt,
                              args.release_on_unit_exit, args.grace_seconds)
        elif args.command == "release":
            output = release(args.directory, args.owner, args.reason, args.evidence_copied_to)
        elif args.command == "scan":
            output = scan(args.root)
            if not args.json:
                for row in output["entries"]:
                    print(f"{row['basename']}: {row['verdict']} ({row['reason']})")
                for blocked in output["recover_blocked_foreign_owner"]:
                    print(f"{blocked['path']}: degraded (recover_blocked_foreign_owner uid={blocked['owner_uid']})")
                print(json.dumps(output["counts"], sort_keys=True))
                return 0
        elif args.command == "status":
            output = status(
                args.root,
                args.stale_after_seconds,
                args.floor_bytes,
                args.measure_timeout_seconds,
                args.measure_budget_seconds,
                sandbox_root=args.sandbox_root,
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
            trigger = args.trigger or "timer"
            timer_invocation = args.trigger == "timer" or (args.trigger is None and bool(os.environ.get("INVOCATION_ID")))
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
                sandbox_root=args.sandbox_root,
            )
            if output["outcome"] == "error" or (output.get("starved") and (trigger == "headroom" or not timer_invocation)):
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
