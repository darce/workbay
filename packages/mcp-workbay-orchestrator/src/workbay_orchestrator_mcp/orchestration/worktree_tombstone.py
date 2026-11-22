"""Crash-consistent removal and recovery for retired lane worktrees.

Retirement first records durable intent, then atomically moves the linked
worktree out of its live path. Slow deletion is fenced and happens only at the
tombstone path. A later sweep can safely resume any interrupted intent.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from . import landing_cleanup

_INTENT_RELATIVE = Path(".task-state/worktree-tombstones")
_GIT_TIMEOUT_S = 5.0
_SCHEMA = 1
_PERMANENT_REASONS = {
    "tombstone_foreign_identity",
    "tombstone_foreign_content",
    "tip_moved_after_intent",
    "row_changed_after_close",
}
_TRANSIENT_ATTEMPT_LIMIT = 16
_TRANSIENT_BACKOFF_BASE_S = 1.0
_TRANSIENT_BACKOFF_MAX_S = 300.0


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_text(path: Path, contents: str) -> None:
    """Publish a file with write, file fsync, rename, and directory fsync."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _write_intent(path: Path, intent: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _replace_text(path, json.dumps(intent, sort_keys=True, separators=(",", ":")) + "\n")
    if "landing_intent_id" in intent:
        _fsync_directory(path.parent.parent)


def _git(cwd: Path, args: list[str], *, timeout_s: float = _GIT_TIMEOUT_S) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", "-C", str(cwd), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=max(0.01, timeout_s),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _git_mutation(
    cwd: Path, args: list[str], *, timeout_s: float = _GIT_TIMEOUT_S
) -> subprocess.CompletedProcess[str] | None:
    """Run a git mutation with the reaper's TERM grace and KILL escalation."""
    import workbay_orchestrator_mcp.lane_reaping as lane_reaping  # noqa: PLC0415

    return lane_reaping._run_reclaim_command(
        ["git", "-C", str(cwd), *args],
        timeout=max(0.01, timeout_s),
        mutating=True,
    )


def _mutation_timeout_detail(result: subprocess.CompletedProcess[str]) -> str | None:
    stderr = result.stderr or ""
    marker = re.search(
        r"reclaim_mutation_timeout:[^;\r\n]+;\s*(terminated_gracefully|killed)",
        stderr,
    )
    if marker is None:
        return None
    return f"timed_out:{marker.group(1)}"


def _git_text(cwd: Path, args: list[str], *, timeout_s: float = _GIT_TIMEOUT_S) -> str | None:
    result = _git(cwd, args, timeout_s=timeout_s)
    if result is None or result.returncode != 0:
        return None
    return (result.stdout or "").strip()


def _admin_identity(
    worktree: Path, *, timeout_s: float = _GIT_TIMEOUT_S, linked_only: bool = True
) -> tuple[str, str] | None:
    admin_dir = _git_text(worktree, ["rev-parse", "--path-format=absolute", "--git-dir"], timeout_s=timeout_s)
    common_dir = _git_text(worktree, ["rev-parse", "--path-format=absolute", "--git-common-dir"], timeout_s=timeout_s)
    if not admin_dir or not common_dir:
        return None
    admin = Path(admin_dir).resolve()
    common = Path(common_dir).resolve()
    expected_parent = (common / "worktrees").resolve()
    if linked_only and admin.parent != expected_parent:
        return None
    return str(admin), str(common)


def _directory_identity(path: Path) -> list[int] | None:
    try:
        entry = path.lstat()
    except OSError:
        return None
    return [entry.st_dev, entry.st_ino] if stat.S_ISDIR(entry.st_mode) else None


def _entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _new_intent(
    repo_root: Path,
    worktree_path: str,
    lane_id: str,
    task_ref: str,
    *,
    expected_tip: str | None = None,
    close_authority: Mapping[str, Any] | None = None,
) -> tuple[Path, dict[str, Any]]:
    original = Path(worktree_path).expanduser().resolve(strict=True)
    branch = _git_text(original, ["symbolic-ref", "--quiet", "--short", "HEAD"])
    head_sha = _git_text(original, ["rev-parse", "--verify", "HEAD"])
    admin_identity = _admin_identity(original)
    if not branch or not head_sha or admin_identity is None:
        raise RuntimeError("worktree_identity_unavailable")

    nonce = uuid.uuid4().hex
    tombstone = original.with_name(f".{original.name}.workbay-retire-{nonce}")
    if tombstone.exists():
        raise FileExistsError("worktree_tombstone_collision")
    lane_key = hashlib.sha256(lane_id.encode("utf-8")).hexdigest()[:16]
    path = repo_root / _INTENT_RELATIVE / f"{lane_key}-{nonce}.json"
    return path, {
        "schema": _SCHEMA,
        "nonce": nonce,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "original_worktree_path": str(original),
        "tombstone_path": str(tombstone),
        "branch": branch,
        "head_sha": head_sha,
        "admin_git_dir": admin_identity[0],
        "git_common_dir": admin_identity[1],
        **({"expected_tip": expected_tip} if expected_tip is not None else {}),
        **({"close_authority": dict(close_authority)} if close_authority is not None else {}),
    }


def _move_worktree(repo_root: Path, intent: dict[str, Any], *, timeout_s: float = _GIT_TIMEOUT_S) -> tuple[bool, str]:
    original = Path(str(intent["original_worktree_path"]))
    tombstone = Path(str(intent["tombstone_path"]))
    if not original.exists() or tombstone.exists():
        return False, "tombstone_move_state_ambiguous"
    branch = _git_text(original, ["symbolic-ref", "--quiet", "--short", "HEAD"], timeout_s=timeout_s)
    head_sha = _git_text(original, ["rev-parse", "--verify", "HEAD"], timeout_s=timeout_s)
    admin_identity = _admin_identity(original)
    if branch is None or head_sha is None or admin_identity is None:
        return False, "tombstone_guard_unavailable"
    if branch != intent.get("branch"):
        return False, "tombstone_foreign_identity"
    if head_sha != intent.get("head_sha"):
        return False, "tombstone_foreign_content"
    if admin_identity[0] != intent.get("admin_git_dir") or admin_identity[1] != intent.get("git_common_dir"):
        return False, "tombstone_foreign_identity"
    result = _git_mutation(
        repo_root,
        ["worktree", "move", str(original), str(tombstone)],
        timeout_s=timeout_s,
    )
    if result is not None and result.returncode == 0:
        _fsync_directory(original.parent)
        _fsync_directory(Path(str(intent["admin_git_dir"])))
        _fsync_directory(Path(str(intent["git_common_dir"])) / "worktrees")
        return True, ""
    detail = "git_worktree_move_failed"
    if result is None:
        return False, "tombstone_guard_unavailable"
    timeout_detail = _mutation_timeout_detail(result)
    if timeout_detail is not None:
        return False, f"git_worktree_move_{timeout_detail}"
    detail = (result.stderr or result.stdout or detail).strip()[:300]
    if "lock" in detail.lower():
        return False, "tombstone_landing_lock_busy"
    return False, detail


def _read_porcelain(path: Path, *, timeout_s: float = _GIT_TIMEOUT_S) -> str | None:
    result = _git(
        path,
        ["status", "--porcelain", "--ignored", "--untracked-files=all"],
        timeout_s=timeout_s,
    )
    if result is None or result.returncode != 0:
        return None
    return (result.stdout or "").rstrip("\n")


def _foreign_content_reason(intent: dict[str, Any], tombstone: Path, *, timeout_s: float) -> str | None:
    if "landing_intent_id" in intent:
        return _landing_content_reason(intent, tombstone, timeout_s=timeout_s)
    branch = _git_text(tombstone, ["symbolic-ref", "--quiet", "--short", "HEAD"], timeout_s=timeout_s)
    head_sha = _git_text(tombstone, ["rev-parse", "--verify", "HEAD"], timeout_s=timeout_s)
    if branch is None or head_sha is None:
        return "tombstone_guard_unavailable"
    if branch != intent.get("branch"):
        return "tombstone_foreign_identity"
    if head_sha != intent.get("head_sha"):
        return "tombstone_foreign_content"
    admin_identity = _admin_identity(tombstone)
    if admin_identity is None:
        return "tombstone_guard_unavailable"
    if admin_identity[0] != intent.get("admin_git_dir") or admin_identity[1] != intent.get("git_common_dir"):
        return "tombstone_foreign_identity"
    status = _read_porcelain(tombstone, timeout_s=timeout_s)
    if status is None:
        return "tombstone_guard_unavailable"
    try:
        from workbay_orchestrator_mcp.lane_reaping import _probe_worktree_ignored  # noqa: PLC0415

        ignored_ok, _ignored_detail = _probe_worktree_ignored(str(tombstone))
    except Exception:  # noqa: BLE001 — an unavailable ignored-content policy cannot authorize deletion
        return "tombstone_guard_unavailable"
    if ignored_ok is None:
        return "tombstone_guard_unavailable"
    if ignored_ok is False:
        return "tombstone_foreign_content"
    if any(line and not (line.startswith(" D ") or line.startswith("!! ")) for line in status.splitlines()):
        return "tombstone_foreign_content"
    return None


def _landing_content_reason(intent: dict[str, Any], checkout: Path, *, timeout_s: float) -> str | None:
    admin = Path(intent["admin_git_dir"])
    dot_git = checkout / ".git"
    redo = checkout == Path(intent["tombstone_path"]) and intent.get("remove_started") is True
    checkout_identity = _directory_identity(checkout)
    if checkout.is_symlink() or dot_git.is_symlink():
        return "tombstone_foreign_identity"
    if checkout_identity is None:
        return "tombstone_guard_unavailable"
    if checkout_identity != intent.get("worktree_identity"):
        return "tombstone_foreign_identity"
    if refusal := _landing_admin_reason(intent):
        return refusal
    if _entry_exists(dot_git):
        observed = _admin_identity(checkout, timeout_s=timeout_s, linked_only=False)
        if observed is None:
            return "tombstone_guard_unavailable"
        if observed != (str(admin), intent["git_common_dir"]):
            return "tombstone_foreign_identity"
    elif not redo:
        return "tombstone_foreign_identity"

    # An interrupted recursive remove may have unlinked the .git pointer.
    # Inspect the remaining files using the exact recorded admin directory.
    git_args = [f"--git-dir={admin}", f"--work-tree={checkout}"]
    head = _git(checkout, [*git_args, "symbolic-ref", "--quiet", "HEAD"], timeout_s=timeout_s)
    if head is None or head.returncode not in {0, 1}:
        return "tombstone_guard_unavailable"
    if head.returncode == 1 or head.stdout.strip() != f"refs/heads/{intent['branch']}":
        return "tombstone_foreign_identity"
    # Git status omits changes to assume-unchanged or skip-worktree entries.
    tracked = _git_text(checkout, [*git_args, "ls-files", "-v", "-z"], timeout_s=timeout_s)
    status = _git(
        checkout,
        [*git_args, "status", "--porcelain=v1", "-z", "-uall", "--ignored", "--ignore-submodules=none"],
        timeout_s=timeout_s,
    )
    if tracked is None or status is None or status.returncode != 0:
        return "tombstone_guard_unavailable"
    if any(entry and not entry.startswith("H ") for entry in tracked.split("\0")):
        return "tombstone_foreign_content"
    if any(entry and not (redo and entry.startswith(" D ")) for entry in status.stdout.split("\0")):
        return "tombstone_foreign_content"
    return None


def _repair_landing_links(intent: dict[str, Any]) -> tuple[bool, str]:
    """Repair only either end of the exact Git link interrupted by this retirement."""
    admin = Path(intent["admin_git_dir"])
    backlink = admin / "gitdir"
    dot_git = Path(intent["tombstone_path"]) / ".git"
    try:
        if backlink.is_symlink() or not backlink.is_file():
            return False, "tombstone_foreign_identity"
        registered = backlink.read_text(encoding="utf-8").rstrip("\n")
        if registered == str(Path(intent["original_worktree_path"]) / ".git"):
            # Git moved the checkout before updating its admin backlink.
            _fsync_directory(dot_git.parent.parent)
            _replace_text(backlink, f"{dot_git}\n")
        elif registered != str(dot_git):
            return False, "tombstone_foreign_identity"
        if not os.path.lexists(dot_git):
            if intent.get("remove_started") is not True:
                return False, "tombstone_foreign_identity"
            with dot_git.open("x", encoding="utf-8") as handle:
                handle.write(f"gitdir: {admin}\n")
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(dot_git.parent)
    except (OSError, UnicodeError):
        return False, "tombstone_link_repair_failed"
    return True, ""


def _live_process_reason(original: Path, tombstone: Path, *, timeout_s: float) -> str | None:
    try:
        from . import lane_retire_sweep  # noqa: PLC0415

        snapshot = lane_retire_sweep._process_snapshot(timeout_s=timeout_s)
        if snapshot is None:
            return "tombstone_guard_unavailable"
        if lane_retire_sweep._has_live_process(str(tombstone), snapshot) or lane_retire_sweep._has_live_process(
            str(original), snapshot
        ):
            return "tombstone_live_process"
    except Exception:  # noqa: BLE001 — an unavailable owner probe cannot authorize deletion
        return "tombstone_guard_unavailable"
    return None


def _mark_blocked(path: Path, intent: dict[str, Any], reason: str, *, fence_release: str = "") -> None:
    updated = {**intent, "blocked_reason": reason}
    if fence_release in {"not_ours", "release_failed"}:
        updated["reap_fence_release"] = fence_release
    _write_intent(path, updated)


def _is_permanent_reason(reason: str) -> bool:
    return reason in _PERMANENT_REASONS


def _mark_transient(path: Path, intent: dict[str, Any], reason: str) -> None:
    """Persist retry diagnostics while keeping transient failures drainable."""
    previous = intent.get("transient_attempt_count", 0)
    if not isinstance(previous, int) or isinstance(previous, bool) or previous < 0:
        previous = 0
    attempts = min(previous + 1, _TRANSIENT_ATTEMPT_LIMIT)
    delay_s = min(_TRANSIENT_BACKOFF_MAX_S, _TRANSIENT_BACKOFF_BASE_S * (2 ** (attempts - 1)))
    updated = {key: value for key, value in intent.items() if key != "blocked_reason"}
    updated.update(
        {
            "last_transient_reason": reason or "unknown_transient_probe_result",
            "transient_attempt_count": attempts,
            "next_eligible_at": time.time() + delay_s,
        }
    )
    _write_intent(path, updated)


def _intent_paths(repo_root: Path) -> list[Path]:
    directory = repo_root / _INTENT_RELATIVE
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.glob("*.json") if path.is_file())


def _valid_intent(repo_root: Path, path: Path, intent: dict[str, Any], *, current_common: str | None = None) -> bool:
    lane_id = intent.get("lane_id")
    nonce = intent.get("nonce")
    original_text = intent.get("original_worktree_path")
    tombstone_text = intent.get("tombstone_path")
    common_text = intent.get("git_common_dir")
    admin_text = intent.get("admin_git_dir")
    if not all(
        isinstance(value, str) and value
        for value in (lane_id, nonce, original_text, tombstone_text, common_text, admin_text)
    ):
        return False
    if re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        return False
    lane_key = hashlib.sha256(lane_id.encode("utf-8")).hexdigest()[:16]
    landing_id = intent.get("landing_intent_id")
    if path.name != f"{lane_key}-{nonce}.json" and (
        not isinstance(landing_id, str) or path.name != f"{landing_id}.json"
    ):
        return False
    original = Path(original_text).resolve(strict=False)
    tombstone = Path(tombstone_text).resolve(strict=False)
    if original.parent != tombstone.parent or tombstone.name != f".{original.name}.workbay-retire-{nonce}":
        return False
    common = Path(common_text).resolve(strict=False)
    admin = Path(admin_text).resolve(strict=False)
    if admin.parent != (common / "worktrees").resolve(strict=False):
        return False
    if current_common is None:
        current_common = _git_text(repo_root, ["rev-parse", "--path-format=absolute", "--git-common-dir"])
    return current_common is not None and Path(current_common).resolve(strict=False) == common


def has_intent_for_worktree(
    repo_root: Path | str,
    *,
    lane_id: str,
    task_ref: str,
    worktree_path: str,
    branch: str,
    head_sha: str,
) -> bool:
    """Whether a durable, unblocked intent proves this exact retire was started."""
    root = Path(repo_root).expanduser().resolve()
    worktree = Path(worktree_path).expanduser().resolve(strict=False)
    for path in _intent_paths(root):
        try:
            intent = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if (
            not isinstance(intent, dict)
            or intent.get("schema") != _SCHEMA
            or intent.get("blocked_reason")
            or "landing_intent_id" in intent
            or not _valid_intent(root, path, intent)
        ):
            continue
        try:
            recorded_head = str(intent.get("head_sha") or "").lower()
            recorded_branch = str(intent.get("branch") or "").removeprefix("refs/heads/")
            if (
                intent.get("lane_id") == lane_id
                and intent.get("task_ref") == task_ref
                and Path(str(intent.get("original_worktree_path"))).resolve(strict=False) == worktree
                and recorded_branch == branch.removeprefix("refs/heads/")
                and recorded_head == head_sha.lower()
            ):
                return True
        except (OSError, RuntimeError, ValueError):
            continue
    return False


def pending_intents(repo_root: Path | str) -> int:
    return len(_wake_intent_paths(Path(repo_root).resolve()))


def _remove_intent(path: Path) -> None:
    path.unlink(missing_ok=True)
    _fsync_directory(path.parent)
    try:
        path.parent.rmdir()
        _fsync_directory(path.parent.parent)
    except OSError:
        pass


def _linked_admin_exists(intent: dict[str, Any]) -> bool | None:
    admin_value = intent.get("admin_git_dir")
    if not isinstance(admin_value, str) or not admin_value:
        return None
    admin = Path(admin_value)
    if not admin.exists():
        return False
    if not admin.is_dir() or admin.is_symlink():
        return None
    gitdir_file = admin / "gitdir"
    common_file = admin / "commondir"
    try:
        target_text = gitdir_file.read_text(encoding="utf-8").strip()
        common_text = common_file.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    target = Path(target_text).resolve(strict=False)
    expected = {
        (Path(str(intent["original_worktree_path"])) / ".git").resolve(strict=False),
        (Path(str(intent["tombstone_path"])) / ".git").resolve(strict=False),
    }
    common = (admin / common_text).resolve(strict=False)
    if target not in expected or common != Path(str(intent.get("git_common_dir") or "")).resolve(strict=False):
        return None
    return True


def _drop_exact_stale_admin(intent: dict[str, Any]) -> tuple[bool, str]:
    """Remove only this intent's stale, unlocked admin entry after its tree vanished."""
    admin = Path(str(intent.get("admin_git_dir") or ""))
    exists = _linked_admin_exists(intent)
    if exists is False:
        return True, ""
    if exists is not True:
        return False, "tombstone_admin_identity_unknown"
    if (admin / "locked").exists():
        return False, "tombstone_locked"
    try:
        head = (admin / "HEAD").read_text(encoding="utf-8").strip()
    except OSError:
        return False, "tombstone_admin_identity_unknown"
    if head != f"ref: refs/heads/{intent.get('branch')}":
        return False, "tombstone_foreign_identity"
    if not admin.is_dir() or admin.is_symlink():
        return False, "tombstone_admin_identity_unknown"
    parent = admin.parent
    shutil.rmtree(admin)
    _fsync_directory(parent)
    return True, ""


def _landing_admin_reason(intent: Mapping[str, Any]) -> str | None:
    admin = Path(intent["admin_git_dir"])
    identity = _directory_identity(admin)
    if admin.is_symlink():
        return "tombstone_foreign_identity"
    if identity is None:
        return "tombstone_guard_unavailable"
    if identity != intent.get("admin_identity"):
        return "tombstone_foreign_identity"
    backlink, common = admin / "gitdir", admin / "commondir"
    if backlink.is_symlink() or common.is_symlink():
        return "tombstone_foreign_identity"
    try:
        target = backlink.read_text(encoding="utf-8").rstrip("\n")
        common_dir = (admin / common.read_text(encoding="utf-8").strip()).resolve()
    except (OSError, UnicodeError):
        return "tombstone_guard_unavailable"
    if target not in {str(Path(intent[key]) / ".git") for key in ("original_worktree_path", "tombstone_path")}:
        return "tombstone_foreign_identity"
    if common_dir != Path(intent["git_common_dir"]):
        return "tombstone_foreign_identity"
    return None


def _remove_bound_tree(parent_fd: int, name: str, identity: list[int]) -> tuple[bool, str]:
    if not shutil.rmtree.avoids_symlink_attacks:
        return False, "tombstone_guard_unavailable"
    observed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(observed.st_mode) or [observed.st_dev, observed.st_ino] != identity:
        return False, "tombstone_foreign_identity"
    shutil.rmtree(name, dir_fd=parent_fd)
    os.fsync(parent_fd)
    return True, ""


def _delete_landing_checkout(path: Path, intent: dict[str, Any], tombstone: Path) -> tuple[bool, str]:
    # Git's worktree remove couples tree deletion with unjournaled admin deletion.
    # Retain the registration for content checks until the tree is fully gone.
    admin = Path(intent["admin_git_dir"])
    if any(_entry_exists(admin / marker) for marker in ("locked", "HEAD.lock", "index.lock")):
        return False, "tombstone_locked"
    repaired, detail = _repair_landing_links(intent)
    if not repaired:
        return False, detail
    if intent.get("remove_started") is not True:
        intent["remove_started"] = True
        _write_intent(path, intent)
    descriptor = os.open(tombstone.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        return _remove_bound_tree(descriptor, tombstone.name, intent["worktree_identity"])
    finally:
        os.close(descriptor)


def _retire_landing_admin(path: Path, intent: dict[str, Any]) -> tuple[bool, str]:
    """Cut over the exact registration, then delete it from a private directory."""
    from ..lane_reaping import _worktree_operation_refusal  # noqa: PLC0415

    admin = Path(intent["admin_git_dir"])
    common = Path(intent["git_common_dir"])
    holder = common / f".workbay-retire-{intent['nonce']}"
    destination = holder / "admin"
    identity = intent.get("admin_identity")
    if intent.get("remove_started") is not True:
        return False, "tombstone_foreign_identity"
    if any(_entry_exists(Path(intent[key])) for key in ("original_worktree_path", "tombstone_path")):
        return False, "tombstone_remove_incomplete"

    expected_move = {"source": str(admin), "destination": str(destination), "identity": identity}
    phase = intent.get("admin_move")
    if phase is None:
        _fsync_directory(Path(intent["tombstone_path"]).parent)
        if not _entry_exists(admin):
            # Older Git removal could finish both effects before the journal was cleared.
            return (False, "tombstone_foreign_identity") if _entry_exists(holder) else (True, "")
        if _entry_exists(holder):
            return False, "tombstone_foreign_identity"
        intent["admin_move"] = phase = {**expected_move, "cutover": False}
        _write_intent(path, intent)
    else:
        cutover = phase.get("cutover") if isinstance(phase, dict) else None
        if type(cutover) is not bool or phase != {**expected_move, "cutover": cutover}:
            return False, "tombstone_foreign_identity"
    if phase["cutover"] and _directory_identity(admin) == identity:
        return False, "tombstone_foreign_identity"

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    with ExitStack() as stack:

        def open_directory(directory: Path | str, *, dir_fd: int | None = None) -> int:
            descriptor = os.open(directory, flags, dir_fd=dir_fd)
            stack.callback(os.close, descriptor)
            return descriptor

        common_fd = open_directory(common)
        if not _entry_exists(holder):
            if phase["cutover"]:
                return True, ""
            os.mkdir(holder.name, mode=0o700, dir_fd=common_fd)
            os.fsync(common_fd)
        holder_fd = open_directory(holder.name, dir_fd=common_fd)
        holder_stat = os.fstat(holder_fd)
        if holder_stat.st_uid != os.geteuid() or holder_stat.st_mode & 0o077:
            return False, "tombstone_foreign_identity"
        if not phase["cutover"]:
            if not _entry_exists(destination):
                admin_parent_fd = open_directory(admin.parent)
                if refusal := _landing_admin_reason(intent):
                    return False, refusal
                if any(_entry_exists(admin / marker) for marker in ("locked", "HEAD.lock", "index.lock")):
                    return False, "tombstone_locked"
                refused, detail = _worktree_operation_refusal(admin)
                if refused:
                    return False, detail
                head = admin / "HEAD"
                if (
                    head.is_symlink()
                    or head.read_text(encoding="utf-8").strip() != f"ref: refs/heads/{intent['branch']}"
                ):
                    return False, "tombstone_foreign_identity"
                bound = os.fstat(open_directory(admin.name, dir_fd=admin_parent_fd))
                if [bound.st_dev, bound.st_ino] != identity:
                    return False, "tombstone_foreign_identity"
                os.rename(admin.name, destination.name, src_dir_fd=admin_parent_fd, dst_dir_fd=holder_fd)
            if _directory_identity(destination) != identity:
                return False, "tombstone_foreign_identity"
            # Git may prune an empty worktrees directory after the rename.
            try:
                _fsync_directory(admin.parent)
            except FileNotFoundError:
                os.fsync(common_fd)
            os.fsync(holder_fd)
            intent["admin_move"] = {**expected_move, "cutover": True}
            _write_intent(path, intent)

        if _entry_exists(destination):
            removed, detail = _remove_bound_tree(holder_fd, destination.name, identity)
            if not removed:
                return False, detail
        observed = os.stat(holder.name, dir_fd=common_fd, follow_symlinks=False)
        if (observed.st_dev, observed.st_ino) != (holder_stat.st_dev, holder_stat.st_ino):
            return False, "tombstone_foreign_identity"
        os.rmdir(holder.name, dir_fd=common_fd)
        os.fsync(common_fd)
    return True, ""


def _delete_tombstone(
    repo_root: Path, intent: dict[str, Any], *, timeout_s: float = _GIT_TIMEOUT_S, force: bool = True
) -> tuple[bool, str]:
    tombstone = Path(str(intent["tombstone_path"]))
    result = _git_mutation(
        repo_root,
        ["worktree", "remove", *(["--force"] if force else []), str(tombstone)],
        timeout_s=timeout_s,
    )
    if result is not None and result.returncode == 0:
        return True, ""
    detail = "git_worktree_remove_failed"
    if result is not None:
        timeout_detail = _mutation_timeout_detail(result)
        if timeout_detail is not None:
            return False, f"git_worktree_remove_{timeout_detail}"
        detail = (result.stderr or result.stdout or detail).strip()[:300]
    return False, detail


def _drain_one(
    repo_root: Path,
    path: Path,
    intent: dict[str, Any],
    *,
    deadline: float | None,
    landing_lock_held: bool = False,
    fence_releases: list[str] | None = None,
) -> tuple[str, str]:
    """Drain one intent under the landing and worker locks.

    ``fence_releases`` receives the reclaim fence release state of each fenced step that ran, in order.
    """
    stack = ExitStack()
    try:
        if not landing_lock_held:
            try:
                from .orchestrator_lanes import _landing_mutation_lock  # noqa: PLC0415

                acquired = stack.enter_context(_landing_mutation_lock(repo_root))
            except Exception:  # noqa: BLE001 — an unavailable lane fence cannot authorize deletion
                return "pending", "tombstone_guard_unavailable"
            if acquired is False:
                return "pending", "tombstone_landing_lock_busy"
            if acquired is not True:
                return "pending", "tombstone_guard_unavailable"

        # Keep the lock order landing mutation lock, configured lane worker flock,
        # then legacy lane worker flock. The worker flocks cover the checks below
        # and the forced removal.
        try:
            from ..api import get_runtime_config  # noqa: PLC0415

            configured_state_dir = Path(get_runtime_config().state_dir).resolve()
            legacy_state_dir = (repo_root / ".task-state").resolve()
        except Exception:  # noqa: BLE001 — an unknown worker-lock location cannot authorize deletion
            return "pending", "tombstone_worker_lock_config_unavailable"

        try:
            from .worker_daemon_ctl import _lock_path  # noqa: PLC0415
            from .worker_lock import (
                current_process_holds_worker_lock,
                release_worker_lock,
                try_acquire_worker_lock,
            )

            lane_id = intent.get("lane_id")
            if not isinstance(lane_id, str) or not lane_id:
                return "pending", "tombstone_worker_lock_unavailable"
            lock_state_dirs = [configured_state_dir]
            if configured_state_dir != legacy_state_dir:
                lock_state_dirs.append(legacy_state_dir)
            for state_dir in lock_state_dirs:
                worker_lock_path = _lock_path(state_dir, lane_id)
                if current_process_holds_worker_lock(worker_lock_path, claim="reaping"):
                    continue
                worker_lock = try_acquire_worker_lock(worker_lock_path)
                if worker_lock is None:
                    return "pending", "tombstone_worker_lock_busy"
                stack.callback(release_worker_lock, worker_lock)
        except BlockingIOError:
            return "pending", "tombstone_worker_lock_busy"
        except OSError:
            return "pending", "tombstone_worker_lock_unavailable"
        except Exception:  # noqa: BLE001 — an unavailable lane fence cannot authorize deletion
            return "pending", "tombstone_worker_lock_unavailable"

        return _drain_one_locked(repo_root, path, intent, deadline=deadline, fence_releases=fence_releases)
    finally:
        stack.close()


def _drain_one_locked(
    repo_root: Path,
    path: Path,
    intent: dict[str, Any],
    *,
    deadline: float | None,
    fence_releases: list[str] | None = None,
) -> tuple[str, str]:
    blocked = intent.get("blocked_reason")
    if isinstance(blocked, str) and _is_permanent_reason(blocked):
        # This records an earlier effect, not a new attempt. A permanent refusal
        # remains visible on replay without retrying its actuator.
        release = intent.get("reap_fence_release")
        if release in ("not_ours", "release_failed") and fence_releases is not None:
            fence_releases.append(release)
        return "foreign", blocked
    if "close_authority" in intent and not isinstance(intent["close_authority"], dict):
        return "foreign", "tombstone_foreign_identity"

    def run_actuator(
        actuator: Callable[[], tuple[bool, str]], *, affected_path: Path | None
    ) -> tuple[str, str, tuple[bool, str]]:
        """Run one removal step under the authority the lane's row carries now.

        Returns ``(state, reason, result)``. An empty ``state`` means ``result`` is the step outcome,
        including any custody refusal. Otherwise ``(state, reason)`` is the lane authority refusal.
        The authority is re-derived before every step (``_current_step_authority``), so a persisted
        intent, legacy or not, never stands in for the row. A fenced step that ran appends its fence
        release state to ``fence_releases``, beside its result and never as a refusal, also when it
        raised [AGT-10]. Only a positively rowless or legacy lane runs unfenced.
        """
        import workbay_orchestrator_mcp.lane_reaping as lane_reaping  # noqa: PLC0415

        def guarded_actuator() -> tuple[bool, str]:
            # Read current Handoff custody under the landing and worker locks, after
            # any row fence has been claimed and its SQLite transaction has ended.
            custody_refusal, _detail = lane_reaping._live_task_target_refusal(
                repo_root, intent["branch"], str(affected_path) if affected_path is not None else None
            )
            if custody_refusal:
                return False, custody_refusal
            if landing_removal:
                review_hold = lane_reaping._generic_review_deferral(repo_root, intent["branch"], intent["expected_tip"])
                if review_hold:
                    return False, review_hold
                # A session or another lane can claim the checkout between
                # attempts. Keep the original path in the liveness check after move.
                for checkout in dict.fromkeys((original, affected_path)):
                    if checkout is None:
                        continue
                    guard_identity = {
                        "repo_root": repo_root,
                        "worktree_path": str(checkout),
                        "task_ref": intent["task_ref"],
                        "lane_id": intent["lane_id"],
                    }
                    for probe in (
                        lane_reaping._session_heartbeat_blocks_reclaim,
                        lane_reaping._shared_path_blocks_reclaim,
                    ):
                        blocked, detail = probe(**guard_identity)
                        if blocked:
                            return False, detail
                if affected_path is not None:
                    refused, detail = lane_reaping._worktree_operation_refusal(Path(intent["admin_git_dir"]))
                    if refused:
                        return False, detail
                    tip_refusal = check_expected_tip(remaining)
                    if tip_refusal is not None:
                        return False, tip_refusal[1]
                    foreign = _landing_content_reason(intent, affected_path, timeout_s=remaining)
                    if foreign:
                        return False, foreign
            return actuator()

        refused, authority = _current_step_authority(intent)
        if refused:
            if refused == "row_changed_after_close":
                _mark_blocked(path, intent, refused)
                return "foreign", refused, (False, refused)
            return "pending", refused, (False, refused)
        if authority is None:
            return "", "", guarded_actuator()
        try:
            refusal, result, release = lane_reaping._run_under_reclaim_fence(
                task_ref=str(intent["task_ref"]),
                lane_id=str(intent["lane_id"]),
                authority=authority,
                actuator=guarded_actuator,
            )
        except BaseException as exc:
            escaped = lane_reaping.reclaim_fence_release_of(exc)
            if escaped and fence_releases is not None:
                fence_releases.append(escaped)
            raise
        if not refusal and result is not None:
            if fence_releases is not None:
                fence_releases.append(release)
            return "", "", result
        if refusal == "row_changed_after_close":
            _mark_blocked(path, intent, refusal)
            return "foreign", refusal, (False, refusal)
        return "pending", refusal or "reclaim_fence_unavailable", (False, refusal)

    def check_expected_tip(timeout_s: float) -> tuple[str, str] | None:
        if "expected_tip" not in intent:
            return None
        expected_tip = intent["expected_tip"]
        if not isinstance(expected_tip, str) or re.fullmatch(r"[0-9a-f]{40}", expected_tip) is None:
            return "foreign", "tombstone_foreign_identity"
        ref = f"refs/heads/{intent['branch']}"
        probe = _git(repo_root, ["rev-parse", "--verify", f"{ref}^{{commit}}"], timeout_s=timeout_s)
        if probe is None:
            return "pending", "tombstone_guard_unavailable"
        if landing_removal and probe.returncode != 0:
            present = _git(repo_root, ["show-ref", "--verify", "--quiet", ref], timeout_s=timeout_s)
            if present is None or present.returncode != 1:
                return "pending", "tombstone_guard_unavailable"
        if probe.returncode != 0 or probe.stdout.strip() != expected_tip:
            _mark_blocked(path, intent, "tip_moved_after_intent")
            return "foreign", "tip_moved_after_intent"
        return None

    original = Path(str(intent["original_worktree_path"]))
    tombstone = Path(str(intent["tombstone_path"]))
    landing_removal = "landing_intent_id" in intent
    if landing_removal and (original.is_symlink() or tombstone.is_symlink()):
        return "foreign", "tombstone_foreign_identity"
    original_exists = original.exists()
    tombstone_exists = tombstone.exists()
    if original_exists and tombstone_exists:
        return "pending", "tombstone_move_state_ambiguous"
    remaining = _GIT_TIMEOUT_S if deadline is None else min(_GIT_TIMEOUT_S, deadline - time.monotonic())
    if remaining <= 0:
        return "pending", "budget_exhausted"
    # The tip guard protects a path that still exists. With both paths gone, the
    # removal already completed; a missing ref must not strand the intent as blocked.
    if original_exists or tombstone_exists:
        tip_refusal = check_expected_tip(remaining)
        if tip_refusal is not None:
            return tip_refusal
    if not tombstone_exists and original_exists:
        owner = _live_process_reason(original, tombstone, timeout_s=remaining)
        if owner is not None:
            return ("foreign", owner) if _is_permanent_reason(owner) else ("pending", owner)
        foreign = _foreign_content_reason(intent, original, timeout_s=remaining)
        if foreign is not None:
            return ("foreign", foreign) if _is_permanent_reason(foreign) else ("pending", foreign)
        tip_refusal = check_expected_tip(remaining)
        if tip_refusal is not None:
            return tip_refusal
        state, reason, moved = run_actuator(
            lambda: _move_worktree(repo_root, intent, timeout_s=remaining), affected_path=original
        )
        if state:
            return state, reason
        _moved, detail = moved
        original_exists = original.exists()
        tombstone_exists = tombstone.exists()
        if not tombstone_exists:
            if _is_permanent_reason(detail):
                return "foreign", detail
            return "pending", detail or "tombstone_move_incomplete"
        if original_exists:
            return "pending", "tombstone_move_state_ambiguous"

    if tombstone_exists:
        if deadline is not None:
            remaining = min(_GIT_TIMEOUT_S, deadline - time.monotonic())
            if remaining <= 0:
                return "pending", "budget_exhausted"
        tip_refusal = check_expected_tip(remaining)
        if tip_refusal is not None:
            return tip_refusal
        if (Path(str(intent.get("admin_git_dir") or "")) / "locked").exists():
            return "pending", "tombstone_guard_unavailable"
        owner = _live_process_reason(original, tombstone, timeout_s=remaining)
        if owner is not None:
            return ("foreign", owner) if _is_permanent_reason(owner) else ("pending", owner)
        foreign = _foreign_content_reason(intent, tombstone, timeout_s=remaining)
        if foreign is not None:
            return ("foreign", foreign) if _is_permanent_reason(foreign) else ("pending", foreign)
        tip_refusal = check_expected_tip(remaining)
        if tip_refusal is not None:
            return tip_refusal

        # The move already retired the original path. Only the linked branch
        # and the checkout at its verified tombstone path are affected here.
        def delete() -> tuple[bool, str]:
            if not landing_removal:
                return _delete_tombstone(repo_root, intent, timeout_s=remaining)
            return _delete_landing_checkout(path, intent, tombstone)

        state, reason, removed = run_actuator(delete, affected_path=tombstone)
        if state:
            return state, reason
        deleted, detail = removed
        if not deleted and tombstone.exists():
            return ("foreign" if landing_removal and _is_permanent_reason(detail) else "pending"), detail

    if original.exists() or tombstone.exists():
        return "pending", "tombstone_remove_incomplete"
    if landing_removal:
        state, reason, (dropped, detail) = run_actuator(lambda: _retire_landing_admin(path, intent), affected_path=None)
        if state:
            return state, reason
        if not dropped:
            return ("foreign" if _is_permanent_reason(detail) else "pending"), detail
    else:
        admin_exists = _linked_admin_exists(intent)
        if admin_exists is True:
            state, reason, (dropped, detail) = run_actuator(lambda: _drop_exact_stale_admin(intent), affected_path=None)
            if state:
                return state, reason
            if not dropped:
                if _is_permanent_reason(detail):
                    _mark_blocked(path, intent, detail)
                    return "foreign", detail
                return "pending", detail
        elif admin_exists is None:
            return "pending", "tombstone_admin_identity_unknown"
    _remove_intent(path)
    return "drained", ""


def _current_step_authority(intent: Mapping[str, Any]) -> tuple[str, dict[str, Any] | None]:
    """The close authority one replay step must run under, re-derived from the row now.

    Returns ``(refusal, authority)``. ``authority`` is the row's current close authority, which the
    step's fence then claims; ``None`` with no refusal means the step may run unfenced, which only a
    positively established lane earns:

    - rowless: the row read succeeded and found no row for the intent's task and lane;
    - legacy: the row carries no close authority, and its branch and worktree path are the intent's.

    An intent that carries authority must find exactly that authority still on the row. A legacy
    intent may bind a modern row's authority only when that authority names the intent's branch,
    worktree and head. Anything else is ``row_changed_after_close``. An unreadable or malformed row
    is a retryable refusal. A missing field is never read as legacy [SECD-03].
    """
    import workbay_orchestrator_mcp.lane_reaping as lane_reaping  # noqa: PLC0415

    persisted = intent.get("close_authority")
    if "close_authority" in intent and not isinstance(persisted, dict):
        return "tombstone_foreign_identity", None
    row, authority, refusal = lane_reaping._durable_close_authority(
        task_ref=intent.get("task_ref"), lane_id=intent.get("lane_id")
    )
    if refusal:
        return refusal, None
    original = _resolved_text(intent.get("original_worktree_path"))
    if "landing_intent_id" in intent and (
        row is None
        or row.get("id") != intent.get("row_id")
        or row.get("status") != "merged"
        or row.get("branch_tip_sha") != intent.get("expected_tip")
        or row.get("landing_commit_sha") != intent.get("landing_commit")
        or lane_reaping._recorded_branch_short_name(row.get("branch")) != intent.get("branch")
        or _resolved_text(row.get("worktree_path")) != original
    ):
        return "row_changed_after_close", None
    if isinstance(persisted, dict):
        if authority is None or authority != persisted:
            return "row_changed_after_close", None
        return "", authority
    if authority is not None:
        identity = authority.get("closed_identity") or {}
        bound = (
            identity.get("branch") == intent.get("branch")
            and _resolved_text(identity.get("worktree_path")) == original
            and authority.get("authorized_tip") == intent.get("head_sha")
            and intent.get("expected_tip", authority.get("authorized_tip")) == authority.get("authorized_tip")
        )
        return ("", authority) if bound else ("row_changed_after_close", None)
    if row is None:
        return "", None
    if "landing_intent_id" in intent:
        return "", None
    if row.get("branch") != intent.get("branch") or _resolved_text(row.get("worktree_path")) != original:
        return "row_changed_after_close", None
    return "", None


def _resolved_text(raw: object) -> str:
    """``raw`` as a resolved path string; ``""`` for anything that is not a non-empty path."""
    if not isinstance(raw, str) or not raw:
        return ""
    try:
        return str(Path(raw).expanduser().resolve())
    except (OSError, RuntimeError, ValueError):
        return ""


def _unreleased_fence_detail(releases: list[str]) -> str:
    """``reap_fence_release:<state>`` when the last fenced step left the row fenced or not ours, else ``""``."""
    if not releases or releases[-1] == "released":
        return ""
    return f"reap_fence_release:{releases[-1]}"


def retire_worktree(
    repo_root: Path | str,
    worktree_path: str,
    *,
    lane_id: str,
    task_ref: str,
    landing_lock_held: bool = False,
    expected_tip: str | None = None,
    close_authority: Mapping[str, Any] | None = None,
    landing_intent_id: str | None = None,
) -> tuple[bool, str]:
    """Write intent, atomically cut over to a tombstone, then drain it.

    The removal is bound to the row's durable close authority. A caller that passes none, such as an
    entry path that wraps or replaces ``lane_reaping._remove_lane_worktree``, has it re-derived here
    the same way, so a closed row is removed only under its reclaim fence, and a row that no longer
    matches its authority is refused before any intent is written.

    Returns ``(True, "")`` once drained. A drain whose last fenced step could not release the row's
    fence returns ``(True, "reap_fence_release:<state>")``: the removal happened and the row stays
    fenced or was not ours [AGT-10].

    A drain refused after such a step returns ``(False, "<cause>;reap_fence_release:<state>")``,
    the format ``branch_reclaim_delete.fence_release_of`` reads, and retries the intent under the
    bare cause. A permanent cause stays byte-identical in a ``FenceReleaseDetail``
    carrying the independent receipt; envelope consumers use ``fence_release_of``.
    """
    root = Path(repo_root).expanduser().resolve(strict=True)
    if close_authority is None:
        import workbay_orchestrator_mcp.lane_reaping as lane_reaping  # noqa: PLC0415

        _row, close_authority, refusal = lane_reaping._durable_close_authority(task_ref=task_ref, lane_id=lane_id)
        if refusal:
            return False, refusal
    if close_authority is not None:
        authorized_tip = close_authority.get("authorized_tip")
        if not isinstance(authorized_tip, str) or expected_tip not in {None, authorized_tip}:
            return False, "authority_mismatch"
        expected_tip = authorized_tip
    path, intent = _new_intent(
        root, worktree_path, lane_id, task_ref, expected_tip=expected_tip, close_authority=close_authority
    )
    if landing_intent_id is not None:
        landing = landing_cleanup.load_intent(root, landing_intent_id)
        if landing is None or landing.get("local_status") != "pending":
            return False, "landing_cleanup_intent_unavailable"
        identity = landing["identity"]
        row = landing["lane_row"]
        original = Path(intent["original_worktree_path"])
        if (
            not landing_lock_held
            or landing_cleanup._intent_id(identity)[0] != landing_intent_id
            or identity["task_ref"] != task_ref
            or identity["lane_id"] != lane_id
            or identity["expected_tip"] != expected_tip
            or intent["head_sha"] != expected_tip
            or type(row.get("id")) is not int
            or str(row.get("branch") or "").removeprefix("refs/heads/") != intent["branch"]
            or _resolved_text(row.get("worktree_path")) != str(original)
        ):
            return False, "tombstone_foreign_identity"
        worktree_identity = _directory_identity(original)
        admin_identity = _directory_identity(Path(intent["admin_git_dir"]))
        if worktree_identity is None or admin_identity is None:
            return False, "tombstone_guard_unavailable"
        directory = _landing_intent_dir(root)
        if directory.parents[1] != Path(intent["git_common_dir"]):
            return False, "tombstone_foreign_identity"
        if _landing_retirement_records(root, landing, directory):
            return False, "landing_tombstone_pending"
        path = directory / f"{landing_intent_id}.json"
        intent.update(
            landing_intent_id=landing_intent_id,
            landing_commit=identity["landing_commit"],
            row_id=row["id"],
            worktree_identity=worktree_identity,
            admin_identity=admin_identity,
        )
    _write_intent(path, intent)
    return _finish_retirement(root, path, intent, landing_lock_held=landing_lock_held)


def _landing_intent_dir(root: Path) -> Path:
    return landing_cleanup._cleanup_dir(root, create=False) / _INTENT_RELATIVE.name


def _wake_intent_paths(root: Path) -> list[Path]:
    paths = _intent_paths(root)
    try:
        paths.extend(sorted(_landing_intent_dir(root).glob("*.json")))
    except landing_cleanup.CleanupIntentError:
        # The legacy sweep can still report its own journals when Git is unavailable.
        pass
    return paths


def _landing_retirement_records(
    root: Path, landing: Mapping[str, Any], directory: Path
) -> list[tuple[Path, dict[str, Any]]]:
    landing_id = landing["intent_id"]
    canonical = directory / f"{landing_id}.json"
    matches = []
    try:
        intent = json.loads(canonical.read_text(encoding="utf-8"))
    except FileNotFoundError:
        pass
    else:
        if not isinstance(intent, dict) or intent.get("landing_intent_id") != landing_id:
            raise ValueError("landing_retirement_identity_invalid")
        matches.append((canonical, intent))

    lane_key = hashlib.sha256(landing["identity"]["lane_id"].encode("utf-8")).hexdigest()[:16]
    # Old landing producers wrote inside their invocation checkout. Keep those
    # journals at their original addresses and let the receipt owner replay them.
    directories = {root / _INTENT_RELATIVE}
    directories.update(checkout / _INTENT_RELATIVE for checkout, _ in landing_cleanup._registered_worktrees(root))
    for candidate in sorted(directories):
        try:
            paths = sorted(candidate.iterdir())
        except FileNotFoundError:
            continue
        for path in paths:
            if not path.name.startswith(f"{lane_key}-") or path.suffix != ".json":
                continue
            try:
                intent = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                # Canonical replay may have just completed this record.
                continue
            if isinstance(intent, dict) and intent.get("landing_intent_id") == landing_id:
                matches.append((path, intent))
    return matches


def has_pending_landing_retirement(root: Path, landing: Mapping[str, Any]) -> bool:
    """Whether this landing still has a worktree retirement record to settle."""
    return bool(_landing_retirement_records(root, landing, _landing_intent_dir(root)))


def resume_landing_retirement(root: Path, landing: Mapping[str, Any], *, deadline: float | None) -> tuple[bool, str]:
    """Resume only the tombstone owned by this verified, locked landing replay."""
    identity = landing["identity"]
    row = landing["lane_row"]
    try:
        directory = _landing_intent_dir(root)
        matches = _landing_retirement_records(root, landing, directory)
    except (landing_cleanup.CleanupIntentError, OSError, subprocess.TimeoutExpired, ValueError):
        return False, "tombstone_guard_unavailable"
    if not matches:
        return True, ""
    if len(matches) != 1:
        return False, "tombstone_move_state_ambiguous"
    path, intent = matches[0]
    if (
        intent.get("schema") != _SCHEMA
        or not _valid_intent(root, path, intent, current_common=str(directory.parents[1]))
        or intent.get("task_ref") != identity["task_ref"]
        or intent.get("lane_id") != identity["lane_id"]
        or intent.get("expected_tip") != identity["expected_tip"]
        or intent.get("head_sha") != identity["expected_tip"]
        or intent.get("landing_commit") != identity["landing_commit"]
        or intent.get("row_id") != row.get("id")
        or intent.get("branch") != str(row.get("branch") or "").removeprefix("refs/heads/")
        or intent.get("original_worktree_path") != _resolved_text(row.get("worktree_path"))
    ):
        return False, "tombstone_foreign_identity"
    return _finish_retirement(root, path, intent, landing_lock_held=True, deadline=deadline)


def _finish_retirement(
    root: Path, path: Path, intent: dict[str, Any], *, landing_lock_held: bool, deadline: float | None = None
) -> tuple[bool, str]:
    releases: list[str] = []
    state, detail = _drain_one(
        root, path, intent, deadline=deadline, landing_lock_held=landing_lock_held, fence_releases=releases
    )
    if state == "drained":
        return True, _unreleased_fence_detail(releases)
    if state == "foreign" and _is_permanent_reason(detail):
        from .branch_reclaim_delete import FenceReleaseDetail  # noqa: PLC0415

        release = releases[-1] if _unreleased_fence_detail(releases) else ""
        _mark_blocked(path, intent, detail, fence_release=release)
        if release:
            return False, FenceReleaseDetail(detail, release)
        return False, detail
    _mark_transient(path, intent, detail)
    cause = detail or "tombstone_remove_incomplete"
    unreleased = _unreleased_fence_detail(releases)
    return False, f"{cause};{unreleased}" if unreleased else cause


def drain_intents(repo_root: Path | str, *, deadline: float | None = None) -> dict[str, Any]:
    """Idempotently finish durable removals, bounded by a monotonic deadline."""
    root = Path(repo_root).expanduser().resolve()
    drained = 0
    budget_exhausted = False
    could_not_act: list[dict[str, str]] = []
    fence_release: list[dict[str, str]] = []
    for path in _wake_intent_paths(root):
        if deadline is not None and time.monotonic() >= deadline:
            budget_exhausted = True
            break
        try:
            intent = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(intent, dict) or intent.get("schema") != _SCHEMA:
                raise ValueError("invalid_intent_schema")
            required = (
                "lane_id",
                "task_ref",
                "original_worktree_path",
                "tombstone_path",
                "branch",
                "head_sha",
                "admin_git_dir",
                "git_common_dir",
            )
            if any(not isinstance(intent.get(key), str) or not intent[key] for key in required):
                raise ValueError("invalid_intent_fields")
            if not _valid_intent(root, path, intent):
                raise ValueError("invalid_intent_identity")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            could_not_act.append(
                {
                    "lane_id": "",
                    "task_ref": "",
                    "reason": "tombstone_intent_invalid",
                    "detail": f"{type(exc).__name__}:{exc}"[:300],
                }
            )
            continue
        if "landing_intent_id" in intent:
            # Landing replay re-verifies receipt, projection and bundle before
            # acting on its tombstone. Leave that authorization with its owner.
            could_not_act.append(
                {
                    "lane_id": intent["lane_id"],
                    "task_ref": intent["task_ref"],
                    "reason": "landing_cleanup_replay_required",
                    "detail": str(intent.get("blocked_reason") or intent.get("last_transient_reason") or ""),
                }
            )
            continue
        releases: list[str] = []
        try:
            state, reason = _drain_one(root, path, intent, deadline=deadline, fence_releases=releases)
        except Exception as exc:  # noqa: BLE001 — retain intent after any interrupted recovery step
            state, reason = "pending", f"tombstone_drain_failed:{type(exc).__name__}"
        unreleased = _unreleased_fence_detail(releases)
        if unreleased:
            # The step ran but its row stays fenced (or was not ours); report it beside the drain
            # state, typed, and as a could_not_act entry that every existing consumer already reads.
            fence_release.append(
                {
                    "lane_id": str(intent.get("lane_id") or ""),
                    "task_ref": str(intent.get("task_ref") or ""),
                    "reap_fence_release": releases[-1],
                    "drain_state": state,
                }
            )
            could_not_act.append(
                {
                    "lane_id": str(intent.get("lane_id") or ""),
                    "task_ref": str(intent.get("task_ref") or ""),
                    "reason": f"reclaim_fence_{releases[-1]}",
                    "detail": unreleased,
                }
            )
        if state == "drained":
            drained += 1
        elif reason == "budget_exhausted":
            budget_exhausted = True
        elif state == "foreign" and _is_permanent_reason(reason):
            _mark_blocked(path, intent, reason, fence_release=releases[-1] if unreleased else "")
            could_not_act.append(
                {
                    "lane_id": str(intent.get("lane_id") or ""),
                    "task_ref": str(intent.get("task_ref") or ""),
                    "reason": reason,
                    "detail": reason,
                }
            )
        else:
            _mark_transient(path, intent, reason)
            could_not_act.append(
                {
                    "lane_id": str(intent.get("lane_id") or ""),
                    "task_ref": str(intent.get("task_ref") or ""),
                    "reason": reason or "unknown_transient_probe_result",
                    "detail": reason or "unknown_transient_probe_result",
                }
            )
    result: dict[str, Any] = {
        "drained": drained,
        "pending": pending_intents(root),
        "could_not_act": could_not_act,
        "budget_exhausted": budget_exhausted,
    }
    if fence_release:
        result["fence_release"] = fence_release
    return result
