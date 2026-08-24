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
import os
import shutil
import subprocess
import time
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
STAGING_DIR_PREFIX = "remote-exec-"
DEBUG_LOG_NAME = "debug.log"
OWNER_PID_NAME = "owner.pid"
EVIDENCE_FILENAMES = ("turn.patch", "result.json", "spec.json", "brief.md")
SKIP_REASON_UNMERGED = "unmerged_branch"
SKIP_REASON_LIVE_OWNER = "live_owner"
SKIP_REASON_NO_INTEGRATION_REF = "no_integration_ref"
SKIP_REASON_UNSAFE = "unsafe_path"

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


def _remove_staging_dir(staging_dir: Path, *, staging_root: Path) -> Path:
    guarded = guard_staging_candidate(staging_dir, staging_root=staging_root)
    if staging_dir.is_symlink():
        unlinked = Path(staging_dir)
        unlinked.unlink()
        return unlinked
    shutil.rmtree(guarded)
    return guarded


def _debug_log_target(staging_dir: Path) -> Path | None:
    debug = staging_dir / DEBUG_LOG_NAME
    if not debug.exists() and not debug.is_symlink():
        return None
    try:
        return debug.resolve()
    except OSError:
        return debug


def reap_remote_exec_staging(
    *,
    staging_root: Path | str,
    git_repo: Path | str,
    apply: bool = True,
) -> StagingReapResult:
    """Scan ``staging_root`` for ``remote-exec-*`` children and reap a bounded batch.

    ``apply=False`` computes the same removed/truncated_debug lists without
    unlinking or rmtree (daemon dry-run). Root-level empty/'/'/'..' still
    raise; per-candidate PathUnsafeError/OSError is a skip, not a batch abort.
    """
    resolved_root = _guard_staging_root(staging_root)
    git_repo_path = Path(git_repo)
    result = StagingReapResult()
    if not resolved_root.is_dir():
        return result

    merge_cache: dict[str, bool] = {}
    integration_ref = _resolve_integration_ref(git_repo_path)
    processed = 0
    with os.scandir(resolved_root) as entries:
        for entry in entries:
            if not entry.name.startswith(STAGING_DIR_PREFIX):
                continue
            if processed >= BATCH_LIMIT:
                result.truncated = True
                break
            processed += 1
            candidate = Path(entry.path)
            try:
                guarded = guard_staging_candidate(candidate, staging_root=resolved_root)
                if not guarded.is_dir():
                    continue

                completed = _turn_completed(guarded)
                owner = _owner_pid(guarded)
                if owner is not None and pid_is_live(owner) and not completed:
                    result.skipped.append(StagingSkip(path=guarded, reason=SKIP_REASON_LIVE_OWNER))
                    continue

                identity = _identity_from_spec(guarded)
                merged = False
                if identity is None:
                    merged = False
                elif integration_ref is None:
                    result.skipped.append(StagingSkip(path=guarded, reason=SKIP_REASON_NO_INTEGRATION_REF))
                    continue
                else:
                    merged = _branch_is_merged(
                        git_repo_path,
                        identity[0],
                        merge_cache,
                        integration_ref=integration_ref,
                    )
                aged = _dir_older_than_retention(guarded)

                if merged and aged and completed:
                    if apply:
                        removed = _remove_staging_dir(candidate, staging_root=resolved_root)
                    else:
                        removed = guarded
                    result.removed.append(removed)
                    continue

                if not merged:
                    result.skipped.append(StagingSkip(path=guarded, reason=SKIP_REASON_UNMERGED))
                if completed:
                    if apply:
                        culled = _cull_debug_log(guarded)
                    else:
                        culled = _debug_log_target(guarded)
                    if culled is not None:
                        result.truncated_debug.append(culled)
            except (PathUnsafeError, OSError):
                result.skipped.append(StagingSkip(path=candidate, reason=SKIP_REASON_UNSAFE))
                continue
    return result
