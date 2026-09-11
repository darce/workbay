"""Bounded reaper for ``.task-state/remote-exec-*`` lane staging directories.

Tiered retention (see ``tests/test_remote_exec_staging_reaper.py``):

* Tier 1 — cull ``debug.log`` from completed turns; keep evidence files.
* Tier 2 — remove merged + older-than-``RETENTION_WINDOW`` + dead-owner completed dirs.
* Tier 3 — never remove an unmerged spool, regardless of age.

Identity comes from ``spec.json`` keys ``branch`` / ``head_sha``. Merge is
``git merge-base --is-ancestor <branch> main`` against the live local ref;
``head_sha`` is required for presence but is not the merge signal (it is
HEAD at spool open, often already on main). Missing branch ref or
AgentSpec-only ``spec.json`` is fail-closed as unmerged. Directory names
are never parsed as merge identity.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

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
    "SKIP_REASON_UNSAFE",
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
SKIP_REASON_UNSAFE = "unsafe_path"

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


def _classify_staging_candidate(
    candidate: Path,
    *,
    staging_root: Path,
    git_repo: Path,
    merge_cache: dict[str, bool],
    integration_ref: str | None,
) -> _CandidateVerdict:
    """Decide eligibility for one staging child. Performs no mutations."""
    guarded = guard_staging_candidate(candidate, staging_root=staging_root)
    if not guarded.is_dir():
        return _CandidateVerdict(guarded=guarded)
    completed = _turn_completed(guarded)
    owner = _owner_pid(guarded)
    if owner is not None and pid_is_live(owner) and not completed:
        return _CandidateVerdict(guarded=guarded, skip_reason=SKIP_REASON_LIVE_OWNER)
    identity = _identity_from_spec(guarded)
    merged = False
    skip_reason: str | None = None
    if identity is None:
        skip_reason = SKIP_REASON_UNMERGED
    elif integration_ref is None:
        skip_reason = SKIP_REASON_NO_INTEGRATION_REF
    else:
        merged = _branch_is_merged(git_repo, identity[0], merge_cache, integration_ref=integration_ref)
        if not merged:
            skip_reason = SKIP_REASON_UNMERGED
    if merged and _dir_older_than_retention(guarded) and completed:
        return _CandidateVerdict(guarded=guarded, remove=True)
    return _CandidateVerdict(guarded=guarded, skip_reason=skip_reason, cull_debug=completed)


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
    if verdict.skip_reason == SKIP_REASON_LIVE_OWNER:
        result.skipped.append(StagingSkip(path=guarded, reason=verdict.skip_reason))
        return False
    if verdict.remove:
        result.removed.append(_remove_staging_dir(candidate, staging_root=staging_root, apply=apply))
        return True
    if verdict.skip_reason is not None:
        result.skipped.append(StagingSkip(path=guarded, reason=verdict.skip_reason))
    if not verdict.cull_debug:
        return False
    culled = _cull_debug_log(guarded) if apply else _debug_log_target(guarded)
    if culled is None:
        return False
    result.truncated_debug.append(culled)
    return True


def _reap_one_candidate(
    candidate: Path,
    *,
    staging_root: Path,
    git_repo: Path,
    merge_cache: dict[str, bool],
    integration_ref: str | None,
    apply: bool,
    result: StagingReapResult,
) -> int:
    try:
        verdict = _classify_staging_candidate(
            candidate,
            staging_root=staging_root,
            git_repo=git_repo,
            merge_cache=merge_cache,
            integration_ref=integration_ref,
        )
    except (PathUnsafeError, OSError):
        result.skipped.append(StagingSkip(path=candidate, reason=SKIP_REASON_UNSAFE))
        return 0
    try:
        return int(
            _apply_candidate_verdict(
                candidate, verdict, staging_root=staging_root, apply=apply, result=result
            )
        )
    except (PathUnsafeError, OSError):
        result.skipped.append(StagingSkip(path=candidate, reason=SKIP_REASON_UNSAFE))
        return 0


def reap_remote_exec_staging(
    *,
    staging_root: Path | str,
    git_repo: Path | str,
    apply: bool = True,
) -> StagingReapResult:
    """Scan ``staging_root`` for ``remote-exec-*`` children and reap a bounded batch."""
    resolved_root = _guard_staging_root(staging_root)
    result = StagingReapResult()
    if not resolved_root.is_dir():
        return result
    git_repo_path = Path(git_repo)
    merge_cache: dict[str, bool] = {}
    integration_ref = _resolve_integration_ref(git_repo_path)
    state = _open_scan(resolved_root, apply=apply)
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
            )
    finally:
        _commit_scan_progress(staging_root=resolved_root, state=state, apply=apply, result=result)
    return result
