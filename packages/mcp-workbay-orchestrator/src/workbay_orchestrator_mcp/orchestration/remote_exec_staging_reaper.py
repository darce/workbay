"""Bounded reaper for ``.task-state/remote-exec-*`` lane staging directories.

Tiered retention (see ``tests/test_remote_exec_staging_reaper.py``):

* Tier 1 — cull ``debug.log`` from completed turns and from abandoned incomplete
  turns; keep evidence files.
* Tier 2 — remove merged + older-than-``RETENTION_WINDOW`` + dead-owner completed dirs.
  Under disk pressure (staging volume free bytes below the shared floor) the
  age requirement is dropped; merged + completed + dead-or-absent owner
  still suffices. Unmerged spools and live-owner incomplete spools stay.
* Retired, unproven — gzip large debug logs in old completed, dead-owner
  spools. Retain streams for plain-file rescue readers and all recovery evidence.
* Tier 3 — never remove an unmerged spool, regardless of age.

Identity comes from ``spec.json`` keys ``branch`` / ``head_sha``. Merge is
``git merge-base --is-ancestor <branch> main`` against the live local ref;
``head_sha`` is required for presence but is not the merge signal (it is
HEAD at spool open, often already on main). Missing identity or
AgentSpec-only ``spec.json`` is fail-closed as unmerged. Deleted refs may
instead use verified landing receipts or tombstones bound to completion
metadata in ``result.json``: ``task_ref`` and ``branch_tip_sha`` (the final
tip, never the opening HEAD). Legacy spools without that metadata remain.
Directory names and open-time ``head_sha`` are never used as merge proof.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from workbay_orchestrator_mcp.orchestration import disk_probe, landing_log

__all__ = (
    "reap_remote_exec_staging",
    "guard_staging_candidate",
    "pid_is_live",
    "PathUnsafeError",
    "StagingReapResult",
    "RETENTION_WINDOW",
    "BATCH_LIMIT",
    "SCAN_LIMIT",
    "STAGING_DIR_PREFIX",
    "DEBUG_LOG_NAME",
    "OWNER_PID_NAME",
    "EVIDENCE_FILENAMES",
    "SKIP_REASON_UNMERGED",
    "SKIP_REASON_LIVE_OWNER",
    "SKIP_REASON_NO_INTEGRATION_REF",
    "SKIP_REASON_ABANDONED",
    "SKIP_REASON_UNSAFE",
    "SKIP_REASON_NO_LANDING_EVIDENCE",
    "SKIP_REASON_AMBIGUOUS_LANDING",
    "SKIP_REASON_UNBOUND_GENERATION",
)

RETENTION_WINDOW = timedelta(days=7)
BATCH_LIMIT = 32
SCAN_LIMIT = BATCH_LIMIT * 8
STAGING_DIR_PREFIX = "remote-exec-"
DEBUG_LOG_NAME = "debug.log"
OWNER_PID_NAME = "owner.pid"
EVIDENCE_FILENAMES = ("turn.patch", "result.json", "spec.json", "brief.md")
SKIP_REASON_UNMERGED = "unmerged_branch"
SKIP_REASON_LIVE_OWNER = "live_owner"
SKIP_REASON_NO_INTEGRATION_REF = "no_integration_ref"
SKIP_REASON_ABANDONED = "abandoned"
SKIP_REASON_UNSAFE = "unsafe_path"
SKIP_REASON_NO_LANDING_EVIDENCE = "no_landing_evidence"
SKIP_REASON_AMBIGUOUS_LANDING = "ambiguous_landing"
SKIP_REASON_UNBOUND_GENERATION = "retained_unbound_generation"
_COMPRESSION_MIN_BYTES = 1024 * 1024
# Plain-stream readers at orchestration/offload_pass.py:3419 and
# orchestration/pass_rescue.py:565 cannot consume gzip. Coordinator narrowed
# this tier to debug.log only; stream compression needs gzip-aware readers
# in a separate follow-up before changing this compatibility contract.
_COMPRESSIBLE_LOGS = (DEBUG_LOG_NAME,)

_SCAN_CURSOR_NAME = ".remote-exec-reaper-cursor"
_MAX_CURSOR_BYTES = 512
_MAX_SCAN_OFFSET = 100_000
logger = logging.getLogger(__name__)


@dataclass
class _ScanState:
    entries: Iterator[os.DirEntry[str]]
    cursor: str | None
    past_cursor: bool
    wrapped: bool = False
    traversed: int = 0
    last_examined: str | None = None
    productive: int = 0
    offset: int = 0


@dataclass(frozen=True)
class _CandidateVerdict:
    guarded: Path | None = None
    skip_reason: str | None = None
    remove: bool = False
    cull_debug: bool = False
    compress: bool = False


@dataclass
class _LandingState:
    missing_refs: set[str] = field(default_factory=set)
    scan: landing_log.ScanResult | None = None
    verdicts: dict[tuple[str, tuple[str, str] | None], tuple[bool, str | None]] = field(default_factory=dict)


_INTEGRATION_REF_CANDIDATES = (
    "refs/heads/main",
    "main",
    "refs/remotes/origin/main",
    "origin/main",
    "refs/heads/master",
    "master",
)


class PathUnsafeError(ValueError):
    """Raised when a staging path is empty, root, contains ``..``, or escapes the root."""


@dataclass(frozen=True)
class StagingSkip:
    path: Path
    reason: str


@dataclass
class StagingReapResult:
    removed: list[Path] = field(default_factory=list)
    skipped: list[StagingSkip] = field(default_factory=list)
    truncated_debug: list[Path] = field(default_factory=list)
    truncated: bool = False
    retained_bytes: int = 0
    under_pressure: bool = False
    compressed: list[Path] = field(default_factory=list)
    bytes_freed: int = 0

    @property
    def skipped_by_reason(self) -> dict[str, int]:
        return dict(Counter(item.reason for item in self.skipped))


def pid_is_live(pid: int) -> bool:
    """True only when the process exists and is not a zombie.

    ``ps -p`` and ``os.kill(pid, 0)`` both succeed for zombies, so they are
    not sufficient on their own. Matches the C3 control helper.
    """
    if pid <= 0:
        return False
    procfs = Path(f"/proc/{pid}/stat")
    if procfs.is_file():
        try:
            text = procfs.read_text(encoding="utf-8")
        except OSError:
            return False
        close = text.rfind(")")
        if close == -1:
            return False
        fields = text[close + 2 :].split()
        if not fields:
            return False
        return fields[0] != "Z"
    proc = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return False
    stat = proc.stdout.strip()
    if not stat:
        return False
    return not stat.startswith("Z")


def _is_blank(value: object) -> bool:
    raw = os.fspath(value) if isinstance(value, (str, os.PathLike)) else str(value)
    stripped = raw.strip()
    return stripped in {"", ".", "./"}


def _has_lexical_dotdot(path: Path) -> bool:
    return ".." in path.parts or ".." in path.as_posix().split("/")


def _guard_staging_root(staging_root: Path | str) -> Path:
    if _is_blank(staging_root):
        raise PathUnsafeError("staging_root is empty")
    given = Path(staging_root)
    if _has_lexical_dotdot(given):
        raise PathUnsafeError("staging_root lexically contains '..'")
    resolved = given.resolve()
    if resolved == Path("/"):
        raise PathUnsafeError("staging_root is the filesystem root")
    return resolved


def guard_staging_candidate(path: Path | str, *, staging_root: Path | str) -> Path:
    """Single deletion chokepoint. Returns the candidate resolved, or raises."""
    if _is_blank(path):
        raise PathUnsafeError("candidate path is empty")
    if _is_blank(staging_root):
        raise PathUnsafeError("staging_root is empty")
    given = Path(path)
    root = Path(staging_root)
    if _has_lexical_dotdot(given):
        raise PathUnsafeError("candidate path lexically contains '..'")
    resolved_root = root.resolve()
    if resolved_root == Path("/"):
        raise PathUnsafeError("staging_root is the filesystem root")
    resolved = given.resolve()
    if resolved == resolved_root:
        raise PathUnsafeError("candidate is the staging root itself")
    if resolved == Path("/"):
        raise PathUnsafeError("candidate resolved to the filesystem root")
    if not resolved.is_relative_to(resolved_root):
        raise PathUnsafeError("candidate resolved outside staging_root")
    return resolved


def _identity_from_spec(staging_dir: Path) -> tuple[str, str] | None:
    spec_path = staging_dir / "spec.json"
    if not spec_path.is_file():
        return None
    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    branch = payload.get("branch")
    head_sha = payload.get("head_sha")
    if not isinstance(branch, str) or not branch.strip():
        return None
    if not isinstance(head_sha, str) or not head_sha.strip():
        return None
    return branch.strip(), head_sha.strip()


def _resolve_integration_ref(git_repo: Path) -> str | None:
    """Resolve the integration ref the same way as offload_preflight/review_runner.

    Prefer local main, then origin/main, then master, then origin's default HEAD.
    Cached once per reap so a large leftover spool does not re-probe git.
    """
    for candidate in _INTEGRATION_REF_CANDIDATES:
        proc = subprocess.run(
            ["git", "-C", str(git_repo), "rev-parse", "--verify", "--quiet", candidate],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return candidate
    origin_head = subprocess.run(
        ["git", "-C", str(git_repo), "symbolic-ref", "--short", "-q", "refs/remotes/origin/HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    tip = (origin_head.stdout or "").strip()
    if origin_head.returncode == 0 and tip:
        return tip
    return None


def _branch_is_merged(
    git_repo: Path,
    branch: str,
    cache: dict[str, bool],
    *,
    integration_ref: str,
    missing_refs: set[str] | None = None,
) -> bool:
    """True only when the live local branch ref is an ancestor of the integration ref.
    Pre-turn ``head_sha`` is ignored here: the producer stamps HEAD at spool
    open, so a first-turn spool often carries a main SHA while the branch is
    still unmerged. A missing or unsafe ref is fail-closed as unmerged.
    """
    cached = cache.get(branch)
    if cached is not None:
        return cached
    if not branch or branch.startswith("-") or ".." in branch or "\n" in branch:
        cache[branch] = False
        return False
    ref = f"refs/heads/{branch}"
    verify = subprocess.run(
        ["git", "-C", str(git_repo), "rev-parse", "--verify", "--quiet", ref],
        capture_output=True,
        text=True,
        check=False,
    )
    if verify.returncode != 0:
        if verify.returncode == 1 and missing_refs is not None:
            missing_refs.add(branch)
        cache[branch] = False
        return False
    proc = subprocess.run(
        ["git", "-C", str(git_repo), "merge-base", "--is-ancestor", ref, integration_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    merged = proc.returncode == 0
    cache[branch] = merged
    return merged


def _receipt_tasks(
    git_repo: Path, branch: str, integration_ref: str, scan: landing_log.ScanResult
) -> tuple[set[str], list[landing_log.LandingReceipt], bool]:
    tasks: set[str] = set()
    receipts: list[landing_log.LandingReceipt] = []
    verification_failed = False
    for commit, receipt in scan.receipts:
        if receipt.lane_id != branch.removeprefix("feature/"):
            continue
        proof = landing_log.verify_receipt(git_repo, commit, main_ref=integration_ref)
        if proof.verified:
            tasks.add(receipt.task_ref)
            receipts.append(receipt)
        elif proof.failed_check in {
            "runtime_unconfigured",
            "v1_probe_failed",
            "v2_probe_failed",
            "v3_lookup_failed",
            "v4_lookup_failed",
        }:
            verification_failed = True
            logger.warning(
                "Remote-exec receipt verification failed for %s at %s: %s (%s)",
                branch,
                commit,
                proof.failed_check,
                proof.detail,
            )
    return tasks, receipts, verification_failed


def _completion_generation(staging_dir: Path) -> tuple[str, str] | None:
    """Require spool-local task and final tip; never substitute open-time HEAD."""
    for name in ("completion.json", "result.json"):
        try:
            record = staging_dir / name
            if not stat.S_ISREG(record.lstat().st_mode):
                return None
            flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
            with os.fdopen(os.open(record, flags), encoding="utf-8") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    return None
                payload = json.load(stream)
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        break
    else:
        return None
    if not isinstance(payload, dict):
        return None
    task = payload.get("task_ref")
    tip = payload.get("branch_tip_sha")
    if not isinstance(task, str) or not task.strip() or not isinstance(tip, str):
        return None
    if len(tip) != 40 or any(ch not in "0123456789abcdef" for ch in tip):
        return None
    return task.strip(), tip


def _generation_binding(git_repo: Path, tip: str, evidence_tip: str) -> landing_log.CarrierResult:
    # The designated primitive resolves both objects and bounds its ancestry
    # probe. Containment here means the spool tip precedes the evidence tip.
    binding = landing_log.find_carrier(git_repo, tip, main_ref=evidence_tip, timeout_s=10.0)
    if binding.error:
        logger.warning("Remote-exec spool generation binding failed for %s: %s", tip, binding.error)
    return binding


def _tombstone_merge_proof(
    git_repo: Path, branch: str, integration_ref: str, scan: landing_log.ScanResult, tip: str
) -> tuple[bool, str | None]:
    """Establish the spool's ancestry independently of the tombstone claim."""
    verdict: tuple[bool, str | None] = (False, SKIP_REASON_UNMERGED)
    for _commit, tombstone in scan.tombstones:
        if tombstone.branch != branch:
            continue
        binding = _generation_binding(git_repo, tip, tombstone.tip)
        if binding.error or not binding.contained or binding.main_tip is None:
            verdict = (False, SKIP_REASON_UNBOUND_GENERATION)
            continue
        proof = landing_log.verify_tombstone(
            git_repo, tombstone, main_ref=integration_ref, current_tip=binding.main_tip
        )
        if proof.verified:
            return True, None
        if proof.failed_check == "probe_failed":
            verdict = (False, SKIP_REASON_NO_LANDING_EVIDENCE)
            logger.warning("Remote-exec tombstone verification failed for %s: %s", branch, proof.detail)
    return verdict


def _retired_branch_is_merged(
    git_repo: Path, branch: str, *, integration_ref: str, state: _LandingState, generation: tuple[str, str] | None
) -> tuple[bool, str | None]:
    """Cache proof by branch, task and final tip, never by branch name alone."""
    key = (branch, generation)
    verdict: tuple[bool, str | None]
    if key in state.verdicts:
        return state.verdicts[key]
    if state.scan is None:
        state.scan = landing_log.scan_first_parent(git_repo, main_ref=integration_ref)
        if not state.scan.complete:
            logger.warning("Remote-exec landing evidence scan incomplete: %s", state.scan.error)
    if not state.scan.complete:
        verdict = (False, SKIP_REASON_NO_LANDING_EVIDENCE)
    elif generation is None:
        verdict = (False, SKIP_REASON_UNBOUND_GENERATION)
    else:
        task, tip = generation
        tasks, receipts, verification_failed = _receipt_tasks(git_repo, branch, integration_ref, state.scan)
        if len(tasks) > 1:
            verdict = (False, SKIP_REASON_AMBIGUOUS_LANDING)
        elif verification_failed:
            verdict = (False, SKIP_REASON_NO_LANDING_EVIDENCE)
        elif tasks:
            verdict = (False, SKIP_REASON_UNBOUND_GENERATION)
            for receipt in receipts:
                if receipt.task_ref != task:
                    continue
                binding = _generation_binding(git_repo, tip, receipt.tip)
                if not binding.error and binding.contained:
                    verdict = (True, None)
                    break
        else:
            verdict = _tombstone_merge_proof(git_repo, branch, integration_ref, state.scan, tip)
    state.verdicts[key] = verdict
    return verdict


def _owner_pid(staging_dir: Path) -> int | None:
    lock = staging_dir / OWNER_PID_NAME
    if not lock.is_file():
        return None
    try:
        text = lock.read_text(encoding="ascii")
    except (OSError, UnicodeError):
        return None
    text = text.strip()
    if not text or not text.isdigit():
        return None
    return int(text)


def _turn_completed(staging_dir: Path) -> bool:
    result = staging_dir / "result.json"
    try:
        return result.is_file() and result.stat().st_size > 0
    except OSError:
        return False


def _dir_older_than_retention(staging_dir: Path) -> bool:
    try:
        mtime = staging_dir.stat().st_mtime
    except OSError:
        return False
    return (time.time() - mtime) > RETENTION_WINDOW.total_seconds()


def _completion_older_than_retention(staging_dir: Path) -> bool:
    """Anchor retired-log age to completion, which compression never rewrites."""
    try:
        info = (staging_dir / "result.json").lstat()
    except FileNotFoundError:
        # No completion record means this turn cannot qualify for compression.
        return False
    except OSError as exc:
        logger.warning("Could not probe staging completion age %s: %s", staging_dir / "result.json", exc)
        # The candidate boundary records unsafe_path and retains all its logs.
        raise
    return stat.S_ISREG(info.st_mode) and (time.time() - info.st_mtime) > RETENTION_WINDOW.total_seconds()


def _lazy_reaper_floor_bytes(floor_bytes: int | None, git_repo: Path) -> int | None:
    if floor_bytes is not None:
        return floor_bytes
    try:
        from workbay_orchestrator_mcp.orchestration import offload_preflight

        return offload_preflight.resolve_disk_floor_bytes(git_repo)
    except Exception:
        return None


def _staging_root_under_pressure(
    staging_root: Path,
    git_repo: Path,
    floor_bytes: int | None,
) -> bool:
    resolved_floor = _lazy_reaper_floor_bytes(floor_bytes, git_repo)
    if resolved_floor is None:
        return False
    try:
        filesystem = disk_probe.DEFAULT_STATVFS(staging_root)
    except OSError:
        return False
    return filesystem.f_bavail * filesystem.f_frsize < resolved_floor


def _dir_retained_file_bytes(path: Path) -> int:
    total = 0
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    total += entry.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    except OSError:
        return total
    return total


def _tree_file_bytes(path: Path) -> int:
    """Count reclaimable regular files without following evidence symlinks."""
    total = 0
    pending = [path]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


def _write_gzip_partial(reader, fd: int) -> None:
    with os.fdopen(fd, "wb") as writer:
        with gzip.GzipFile(fileobj=writer, mode="wb", filename="", mtime=0) as archive:
            shutil.copyfileobj(reader, archive)
        writer.flush()
        os.fsync(writer.fileno())


def _clear_stale_partials(source: Path) -> None:
    """Called only for old completed spools under the dead-owner guard."""
    prefix = source.name + ".gz."
    partials = [source.with_name(source.name + ".gz.partial"), *source.parent.glob(prefix + "*.partial")]
    for partial in partials:
        try:
            info = partial.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode):
            raise PathUnsafeError(f"Unsafe compression partial: {partial}")
        # The spool producer can be dead while a daemon or CLI compressor is
        # active. Unique names carry that compressor's PID; leave its work.
        owner = partial.name.removeprefix(prefix).split(".", 1)[0]
        if owner.isascii() and owner.isdigit() and pid_is_live(int(owner)):
            continue
        partial.unlink()


def _compress_log(source: Path, info: os.stat_result) -> int:
    destination = source.with_name(source.name + ".gz")
    partial: Path | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK
        with os.fdopen(os.open(source, flags), "rb") as reader:
            opened = os.fstat(reader.fileno())
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                raise PathUnsafeError(f"Log changed before compression: {source}")
            _clear_stale_partials(source)
            fd, name = tempfile.mkstemp(prefix=f"{source.name}.gz.{os.getpid()}.", suffix=".partial", dir=source.parent)
            partial = Path(name)
            _write_gzip_partial(reader, fd)
            os.replace(partial, destination)
            partial = None
            source.unlink()
            return max(0, opened.st_size - destination.stat().st_size)
    except (OSError, PathUnsafeError) as exc:
        logger.warning("Could not compress retired staging log %s: %s", source, exc)
        raise
    finally:
        if partial is not None:
            partial.unlink(missing_ok=True)


def _compress_retired_logs(staging_dir: Path, *, apply: bool) -> tuple[bool, int]:
    owner = _owner_pid(staging_dir)
    if owner is not None and pid_is_live(owner):
        return False, 0
    compressed = False
    freed = 0
    for name in _COMPRESSIBLE_LOGS:
        source = staging_dir / name
        try:
            info = source.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_size < _COMPRESSION_MIN_BYTES:
            continue
        if apply:
            freed += _compress_log(source, info)
        compressed = True
    return compressed, freed


def _cull_debug_log(staging_dir: Path) -> Path | None:
    debug = staging_dir / DEBUG_LOG_NAME
    if not debug.exists() and not debug.is_symlink():
        return None
    recorded = debug.resolve()
    try:
        debug.unlink()
    except OSError:
        try:
            flags = getattr(os, "O_WRONLY", 0) | getattr(os, "O_TRUNC", 0)
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(debug, flags | nofollow)
            try:
                os.ftruncate(fd, 0)
            finally:
                os.close(fd)
        except OSError:
            return None
    return recorded


def _remove_staging_dir(staging_dir: Path, *, staging_root: Path, apply: bool = True) -> Path:
    guarded = guard_staging_candidate(staging_dir, staging_root=staging_root)
    recorded = Path(staging_dir) if staging_dir.is_symlink() else guarded
    if apply:
        if staging_dir.is_symlink():
            recorded.unlink()
        else:
            shutil.rmtree(guarded)
    return recorded


def _debug_log_target(staging_dir: Path) -> Path | None:
    debug = staging_dir / DEBUG_LOG_NAME
    if not debug.exists() and not debug.is_symlink():
        return None
    try:
        return debug.resolve()
    except OSError:
        return debug


def _valid_cursor_name(cursor: str) -> str | None:
    if (
        not cursor
        or cursor in {".", "..", _SCAN_CURSOR_NAME}
        or Path(cursor).name != cursor
        or any(ord(character) < 32 for character in cursor)
    ):
        return None
    return cursor


def _read_scan_progress(staging_root: Path) -> tuple[str | None, int]:
    """Return (last name, skip offset), failing closed to a fresh scan."""
    cursor_path = staging_root / _SCAN_CURSOR_NAME
    fd: int | None = None
    try:
        if cursor_path.is_symlink():
            return None, 0
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(cursor_path, flags)
        raw = os.read(fd, _MAX_CURSOR_BYTES + 1)
    except OSError:
        return None, 0
    finally:
        if fd is not None:
            os.close(fd)
    if len(raw) > _MAX_CURSOR_BYTES:
        return None, 0
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError:
        return None, 0
    name = _valid_cursor_name(lines[0].strip()) if lines else None
    offset_text = lines[1].strip() if len(lines) > 1 else ""
    if offset_text.isdigit():
        offset = int(offset_text)
        if offset > _MAX_SCAN_OFFSET:
            return name, 0
        return name, offset
    return name, 0


def _read_scan_cursor(staging_root: Path) -> str | None:
    """Return the last traversed child name, failing closed to a fresh scan."""
    name, _offset = _read_scan_progress(staging_root)
    return name


def _write_scan_cursor(staging_root: Path, cursor: str | None, offset: int = 0) -> None:
    """Atomically persist no-follow cursor metadata and report any failure."""
    cursor_path = staging_root / _SCAN_CURSOR_NAME
    temporary_path: Path | None = None
    try:
        if cursor is None:
            cursor_path.unlink(missing_ok=True)
            return
        payload = f"{cursor}\n{max(offset, 0)}\n".encode("utf-8")
        fd, temporary_name = tempfile.mkstemp(
            dir=staging_root,
            prefix=f".{_SCAN_CURSOR_NAME}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            remaining = memoryview(payload)
            while remaining:
                written = os.write(fd, remaining)
                remaining = remaining[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary_path, cursor_path)
        temporary_path = None
    except OSError as exc:
        logger.warning("Could not persist remote-exec reaper cursor %s: %s", cursor_path, exc)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not clean up remote-exec reaper cursor temp file %s", temporary_path)


def _close_entries(entries: Iterator[os.DirEntry[str]]) -> None:
    close = getattr(entries, "close", None)
    if close is not None:
        close()


def _cursor_child_exists(staging_root: Path, name: str) -> bool:
    path = staging_root / name
    try:
        return path.is_symlink() or path.exists()
    except OSError:
        return False


def _consume_persisted_cursor(staging_root: Path) -> str | None:
    """Return the durable child-name cursor and unlink metadata before scandir."""
    name, _offset = _read_scan_progress(staging_root)
    cursor_path = staging_root / _SCAN_CURSOR_NAME
    try:
        if name is not None and cursor_path.is_file() and not cursor_path.is_symlink():
            cursor_path.unlink()
    except OSError as exc:
        logger.warning("Could not clear remote-exec reaper cursor %s: %s", cursor_path, exc)
    if name is None or not _cursor_child_exists(staging_root, name):
        return None
    return name


def _open_scan(staging_root: Path, *, apply: bool) -> _ScanState:
    """Open a scan window from a persisted child name. Dry-run never stores state."""
    if not apply:
        return _ScanState(entries=os.scandir(staging_root), cursor=None, past_cursor=True)
    name = _consume_persisted_cursor(staging_root)
    return _ScanState(entries=os.scandir(staging_root), cursor=name, past_cursor=name is None)


def _next_dir_entry(entries: Iterator[os.DirEntry[str]]) -> os.DirEntry[str] | None:
    try:
        return next(entries)
    except StopIteration:
        return None


def _next_window_entry(state: _ScanState, staging_root: Path) -> os.DirEntry[str] | None:
    """Yield the next entry in this bounded window, counting prefix visits."""
    while state.traversed < SCAN_LIMIT:
        entry = _next_dir_entry(state.entries)
        if entry is None:
            if state.past_cursor or state.wrapped:
                return None
            _close_entries(state.entries)
            state.entries = os.scandir(staging_root)
            state.past_cursor = True
            state.wrapped = True
            continue
        state.last_examined = entry.name
        if not state.past_cursor:
            if entry.name == state.cursor:
                state.past_cursor = True
                continue
            state.traversed += 1
            continue
        state.traversed += 1
        return entry
    return None


def _commit_scan_progress(
    *,
    staging_root: Path,
    state: _ScanState,
    apply: bool,
    result: StagingReapResult,
) -> None:
    result.truncated = state.traversed >= SCAN_LIMIT or state.productive >= BATCH_LIMIT
    if apply and result.truncated:
        _close_entries(state.entries)
        _write_scan_cursor(staging_root, state.last_examined, state.offset + state.traversed)
        return
    _close_entries(state.entries)
    if apply:
        _write_scan_cursor(staging_root, None)


def _candidate_merge_status(
    guarded: Path,
    git_repo: Path,
    merge_cache: dict[str, bool],
    integration_ref: str | None,
    landing_state: _LandingState,
    owner: int | None,
) -> tuple[bool, bool, str | None]:
    identity = _identity_from_spec(guarded)
    if identity is None:
        return False, False, SKIP_REASON_UNMERGED
    if integration_ref is None:
        return False, False, SKIP_REASON_NO_INTEGRATION_REF
    merged = _branch_is_merged(
        git_repo,
        identity[0],
        merge_cache,
        integration_ref=integration_ref,
        missing_refs=landing_state.missing_refs,
    )
    retired = identity[0] in landing_state.missing_refs
    if retired:
        if owner is not None and pid_is_live(owner):
            return False, True, SKIP_REASON_LIVE_OWNER
        merged, skip_reason = _retired_branch_is_merged(
            git_repo,
            identity[0],
            integration_ref=integration_ref,
            state=landing_state,
            generation=_completion_generation(guarded),
        )
        return merged, True, skip_reason
    return merged, False, None if merged else SKIP_REASON_UNMERGED


def _early_candidate_verdict(guarded: Path, completed: bool, owner: int | None) -> _CandidateVerdict | None:
    if owner is not None and pid_is_live(owner) and not completed:
        return _CandidateVerdict(guarded=guarded, skip_reason=SKIP_REASON_LIVE_OWNER)
    if not completed and _dir_older_than_retention(guarded):
        return _CandidateVerdict(guarded=guarded, skip_reason=SKIP_REASON_ABANDONED, cull_debug=True)
    return None


def _classify_staging_candidate(
    candidate: Path,
    *,
    staging_root: Path,
    git_repo: Path,
    merge_cache: dict[str, bool],
    integration_ref: str | None,
    under_pressure: bool = False,
    landing_state: _LandingState | None = None,
) -> _CandidateVerdict:
    """Decide eligibility for one staging child. Performs no mutations."""
    guarded = guard_staging_candidate(candidate, staging_root=staging_root)
    if not guarded.is_dir():
        return _CandidateVerdict(guarded=guarded)
    completed = _turn_completed(guarded)
    owner = _owner_pid(guarded)
    early = _early_candidate_verdict(guarded, completed, owner)
    if early is not None:
        return early
    merged, retired, skip_reason = _candidate_merge_status(
        guarded, git_repo, merge_cache, integration_ref, landing_state or _LandingState(), owner
    )
    if skip_reason == SKIP_REASON_LIVE_OWNER:
        return _CandidateVerdict(guarded=guarded, skip_reason=skip_reason)
    age_ok = under_pressure or _dir_older_than_retention(guarded)
    if merged and age_ok and completed:
        return _CandidateVerdict(guarded=guarded, remove=True)
    if retired and not merged and completed:
        # Retain the only log even when young, small, or not yet compressible.
        return _CandidateVerdict(
            guarded=guarded, skip_reason=skip_reason, compress=_completion_older_than_retention(guarded)
        )
    return _CandidateVerdict(guarded=guarded, skip_reason=skip_reason, cull_debug=completed)


def _apply_log_verdict(guarded: Path, verdict: _CandidateVerdict, *, apply: bool, result: StagingReapResult) -> bool:
    if verdict.compress:
        compressed, freed = _compress_retired_logs(guarded, apply=apply)
        if compressed:
            result.compressed.append(guarded)
            result.bytes_freed += freed
            result.retained_bytes -= freed
        return compressed
    if not verdict.cull_debug:
        return False
    debug = guarded / DEBUG_LOG_NAME
    freed = debug.lstat().st_size if apply and (debug.exists() or debug.is_symlink()) else 0
    culled = _cull_debug_log(guarded) if apply else _debug_log_target(guarded)
    if culled is None:
        return False
    result.truncated_debug.append(culled)
    result.bytes_freed += freed
    return True


def _apply_candidate_verdict(
    candidate: Path,
    verdict: _CandidateVerdict,
    *,
    staging_root: Path,
    apply: bool,
    result: StagingReapResult,
) -> bool:
    """Mutate or record would-mutate for one verdict. True when productive."""
    guarded = verdict.guarded
    if guarded is None:
        return False
    if verdict.remove:
        freed = _tree_file_bytes(guarded) if apply and not candidate.is_symlink() else 0
        result.removed.append(_remove_staging_dir(candidate, staging_root=staging_root, apply=apply))
        result.bytes_freed += freed
        return True
    result.retained_bytes += _dir_retained_file_bytes(guarded)
    if verdict.skip_reason is not None:
        result.skipped.append(StagingSkip(path=guarded, reason=verdict.skip_reason))
    if verdict.skip_reason == SKIP_REASON_LIVE_OWNER:
        return False
    return _apply_log_verdict(guarded, verdict, apply=apply, result=result)


def _reap_one_candidate(
    candidate: Path,
    *,
    staging_root: Path,
    git_repo: Path,
    merge_cache: dict[str, bool],
    integration_ref: str | None,
    apply: bool,
    result: StagingReapResult,
    under_pressure: bool = False,
    landing_state: _LandingState | None = None,
) -> int:
    try:
        verdict = _classify_staging_candidate(
            candidate,
            staging_root=staging_root,
            git_repo=git_repo,
            merge_cache=merge_cache,
            integration_ref=integration_ref,
            under_pressure=under_pressure,
            landing_state=landing_state,
        )
    except (PathUnsafeError, OSError):
        result.skipped.append(StagingSkip(path=candidate, reason=SKIP_REASON_UNSAFE))
        return 0
    try:
        return int(_apply_candidate_verdict(candidate, verdict, staging_root=staging_root, apply=apply, result=result))
    except (PathUnsafeError, OSError):
        result.skipped.append(StagingSkip(path=candidate, reason=SKIP_REASON_UNSAFE))
        return 0


def _start_staging_reap(
    resolved_root: Path,
    git_repo: Path | str,
    floor_bytes: int | None,
) -> tuple[StagingReapResult, Path, dict[str, bool], str | None]:
    result = StagingReapResult()
    git_repo_path = Path(git_repo)
    result.under_pressure = _staging_root_under_pressure(resolved_root, git_repo_path, floor_bytes)
    return result, git_repo_path, {}, _resolve_integration_ref(git_repo_path)


def _finish_staging_reap(
    root: Path, state: _ScanState, apply: bool, result: StagingReapResult, *, external_scan: bool
) -> None:
    if not external_scan:
        _commit_scan_progress(staging_root=root, state=state, apply=apply, result=result)
    else:
        # Both CLI modes keep their iterator across batches. Apply also
        # persists progress for a later independent reaper invocation.
        result.truncated = state.traversed >= SCAN_LIMIT or state.productive >= BATCH_LIMIT
        state.offset += state.traversed
        if apply:
            _write_scan_cursor(root, state.last_examined if result.truncated else None, state.offset)
        state.traversed = state.productive = 0


def reap_remote_exec_staging(
    *,
    staging_root: Path | str,
    git_repo: Path | str,
    apply: bool = True,
    floor_bytes: int | None = None,
    _scan_state: _ScanState | None = None,
) -> StagingReapResult:
    """Scan ``staging_root`` for ``remote-exec-*`` children and reap a bounded batch."""
    resolved_root = _guard_staging_root(staging_root)
    if not resolved_root.is_dir():
        return StagingReapResult()
    result, git_repo_path, merge_cache, integration_ref = _start_staging_reap(resolved_root, git_repo, floor_bytes)
    state = _scan_state if _scan_state is not None else _open_scan(resolved_root, apply=apply)
    landing_state = _LandingState()
    try:
        while state.productive < BATCH_LIMIT:
            entry = _next_window_entry(state, resolved_root)
            if entry is None:
                break
            if not entry.name.startswith(STAGING_DIR_PREFIX):
                continue
            state.productive += _reap_one_candidate(
                Path(entry.path),
                staging_root=resolved_root,
                git_repo=git_repo_path,
                merge_cache=merge_cache,
                integration_ref=integration_ref,
                apply=apply,
                result=result,
                under_pressure=result.under_pressure,
                landing_state=landing_state,
            )
    finally:
        _finish_staging_reap(resolved_root, state, apply, result, external_scan=_scan_state is not None)
    return result


def _cli_batches(args: argparse.Namespace, root: Path, scan_state: _ScanState | None) -> dict[str, object]:
    """Aggregate batches, deduplicating skips revisited by the apply cursor."""
    counts = {"removed": 0, "compressed": 0, "truncated_debug": 0, "bytes_freed": 0, "batches": 0}
    skips: Counter[str] = Counter()
    seen_skips: set[tuple[Path, str]] = set()
    for _ in range(max(1, min(256, args.max_batches))):
        result = reap_remote_exec_staging(
            staging_root=root, git_repo=args.git_repo, apply=args.apply, _scan_state=scan_state
        )
        counts["batches"] += 1
        for name in ("removed", "compressed", "truncated_debug"):
            counts[name] += len(getattr(result, name))
        counts["bytes_freed"] += result.bytes_freed
        for skip in result.skipped:
            key = (skip.path, skip.reason)
            if key not in seen_skips:
                seen_skips.add(key)
                skips[skip.reason] += 1
        if not result.truncated:
            break
    return {**counts, "skipped_by_reason": dict(skips), "stopped_on_batch_limit": result.truncated}


def main(argv: list[str] | None = None) -> int:
    """Run bounded maintenance without requiring a daemon; default to preview."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--staging-root", default=".task-state")
    parser.add_argument("--git-repo", default=".")
    parser.add_argument("--max-batches", type=int, default=16)
    args = parser.parse_args(argv)
    scan_state: _ScanState | None = None
    try:
        root = _guard_staging_root(args.staging_root)
        try:
            from workbay_handoff_mcp.api import configure_runtime
            from workbay_handoff_mcp.config import RuntimeConfig

            workspace = landing_log._workspace_root_for_args(
                argparse.Namespace(workspace_root=None, repo=args.git_repo)
            )
            configure_runtime(RuntimeConfig.for_workspace(workspace))
        except Exception as exc:
            print(f"runtime_unconfigured: {exc}", file=sys.stderr)
            return 4
        if root.is_dir():
            # A CLI run scans from the beginning once, then keeps the live
            # iterator so retained prefixes cannot exhaust every batch budget.
            scan_state = _open_scan(root, apply=False)
        print(json.dumps(_cli_batches(args, root, scan_state)))
        return 0
    except PathUnsafeError as exc:
        print(f"Unsafe staging root: {exc}", file=sys.stderr)
        return 2
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"Remote-exec staging reap failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if scan_state is not None:
            _close_entries(scan_state.entries)


if __name__ == "__main__":
    raise SystemExit(main())
