"""Guarded choke point for mutating git commands during bootstrap install."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from workbay_bootstrap.install_receipt import InstallReceipt

_COMMON_DIR_CACHE: dict[str, Path | None] = {}
_COMMON_DIR_CACHE_MAX = 64


class GitWriteEscapeError(RuntimeError):
    """Raised when a mutating git command would touch a repo outside the target."""

    def __init__(self, *args: Any, common_dir: Path | None = None) -> None:
        super().__init__(*args)
        # The offending foreign common-dir, when known at raise time, so a blocked
        # receipt records the real repo instead of a generic placeholder.
        self.common_dir = common_dir


def reset_git_write_cache() -> None:
    """Clear the process-lifetime common-dir cache (test helper)."""
    _COMMON_DIR_CACHE.clear()


def _cache_common_dir(key: str, value: Path | None) -> Path | None:
    if key not in _COMMON_DIR_CACHE:
        if len(_COMMON_DIR_CACHE) >= _COMMON_DIR_CACHE_MAX:
            _COMMON_DIR_CACHE.pop(next(iter(_COMMON_DIR_CACHE)))
        _COMMON_DIR_CACHE[key] = value
    return _COMMON_DIR_CACHE[key]


# Fail-CLOSED classification: rather than allow-list a handful of mutating verbs
# (which lets any unknown mutating verb run UNGUARDED), we allow-list the verbs
# that are unambiguously reads and treat everything else as mutating so the
# ownership guard runs. ``config`` and ``worktree`` need positional context and
# are classified by their own helpers below.
_READ_GIT_VERBS = frozenset(
    {
        "rev-parse",
        "status",
        "log",
        "show",
        "cat-file",
        "for-each-ref",
        "ls-files",
        "ls-tree",
        "ls-remote",
        "diff",
        "merge-base",
        "name-rev",
        "describe",
        "symbolic-ref",
    }
)

_CONFIG_MUTATING_FLAGS = frozenset(
    {
        "--unset",
        "--unset-all",
        "--remove-section",
        "--rename-section",
        "--replace-all",
        "--add",
    }
)
_CONFIG_READ_FLAGS = frozenset(
    {
        "--get",
        "--get-all",
        "--get-regexp",
        "--list",
        "--show-origin",
        "--show-scope",
        "-l",
    }
)


def _is_mutating_config_args(args: tuple[str, ...]) -> bool:
    tokens = args[1:]
    for arg in tokens:
        if arg in _CONFIG_MUTATING_FLAGS:
            return True
    for arg in tokens:
        if arg in _CONFIG_READ_FLAGS or arg.startswith("--get"):
            return False
    # No explicit read/write flag: classify by token POSITION, not a leading-dash
    # heuristic. The first positional after any leading options is the key; anything
    # after it — including a VALUE that itself starts with ``-`` — makes this a write.
    positionals: list[str] = []
    seen_key = False
    for arg in tokens:
        if not seen_key and arg.startswith("-"):
            continue
        positionals.append(arg)
        seen_key = True
    # ``config <key>`` reads (implicit get); ``config <key> <value>`` writes.
    return len(positionals) != 1


def _is_mutating_git_args(args: tuple[str, ...]) -> bool:
    if not args:
        return False
    cmd = args[0]
    if cmd == "config":
        return _is_mutating_config_args(args)
    if cmd == "worktree":
        # Only ``worktree list`` reads; add/remove/prune/move/lock/repair mutate.
        sub = next((a for a in args[1:] if not a.startswith("-")), None)
        return sub != "list"
    return cmd not in _READ_GIT_VERBS


def _normalize_common_dir(cwd: Path, raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = (cwd / path).resolve()
    else:
        path = path.resolve()
    return path


def _rev_parse_common_dir(cwd: Path) -> Path | None:
    from workbay_bootstrap.external import run_external

    cmd = ["git", "-C", str(cwd), "rev-parse", "--git-common-dir"]
    result = run_external(
        cmd,
        call_class="git",
        check=False,
        capture_output=True,
        text=True,
    )
    # git exits 128 when ``cwd`` is not a resolvable repository (e.g. a sham
    # ``.git`` marker that the underlying ``git config`` write still tolerates).
    # A cwd with no resolvable common dir has no foreign shared repo to escape
    # to, so signal "unknown" rather than crashing the install.
    if result.returncode != 0:
        return None
    return _normalize_common_dir(cwd, result.stdout.strip())


def _resolve_common_dir_cached(cwd: Path) -> Path | None:
    key = str(cwd.resolve())
    if key in _COMMON_DIR_CACHE:
        return _COMMON_DIR_CACHE[key]
    return _cache_common_dir(key, _rev_parse_common_dir(cwd))


def _path_inside_target(path: Path, target_root: Path) -> bool:
    try:
        path.resolve().relative_to(target_root.resolve())
        return True
    except ValueError:
        return False


def _clone_destination(args: tuple[str, ...], cwd: Path | None) -> Path:
    dest = Path(args[-1])
    if not dest.is_absolute():
        base = (cwd or Path.cwd()).resolve()
        dest = (base / dest).resolve()
    return dest


def _write_is_owned(
    target_root: Path,
    write_common_dir: Path,
    target_common_dir: Path | None,
    *,
    target_is_repo_root: bool,
) -> bool:
    write_resolved = write_common_dir.resolve()
    # The common-dir equality prong is only trustworthy when target_root is itself
    # a git repo/worktree root (a .git dir or gitdir-pointer file). When target_root
    # is a plain directory NESTED inside a foreign repo, `rev-parse` discovers that
    # OUTER repo for BOTH the write cwd and target_root, so equality would vacuously
    # pass and let a foreign mutation through. In that case fall through to the
    # strict containment check (the write's common dir must live inside target_root).
    if (
        target_is_repo_root
        and target_common_dir is not None
        and write_resolved == target_common_dir.resolve()
    ):
        return True
    return _path_inside_target(write_resolved, target_root)


def _effective_cwd_for_write(
    args: tuple[str, ...],
    cwd: Path | None,
    target_root: Path,
) -> Path:
    if args and args[0] == "clone":
        return _clone_destination(args, cwd)
    if cwd is not None:
        return cwd.resolve()
    return target_root.resolve()


def _assert_write_owned(
    target_root: Path,
    args: tuple[str, ...],
    cwd: Path | None,
) -> Path:
    target_root = target_root.resolve()
    if args and args[0] == "clone":
        dest = _effective_cwd_for_write(args, cwd, target_root)
        if _path_inside_target(dest, target_root):
            git_dir = dest / ".git"
            return git_dir if git_dir.exists() else dest
        raise GitWriteEscapeError(
            f"git clone destination {dest} is outside install target {target_root}"
        )

    effective = _effective_cwd_for_write(args, cwd, target_root)
    if not (effective / ".git").exists() and not (effective.parent / ".git").exists():
        if args and args[0] == "init":
            if _path_inside_target(effective, target_root):
                return effective
            raise GitWriteEscapeError(
                f"git init cwd {effective} is outside install target {target_root}"
            )

    write_common_dir = _resolve_common_dir_cached(effective)
    target_common_dir = _resolve_common_dir_cached(target_root)
    if write_common_dir is None:
        # git could not resolve a common dir for the write cwd — there is no
        # foreign shared repo at risk. Allow only when the write targets the
        # target's own tree; otherwise fail closed.
        if effective == target_root or _path_inside_target(effective, target_root):
            return effective
        raise GitWriteEscapeError(
            f"git write cwd {effective} is outside install target {target_root} "
            f"and its git common dir is unresolvable"
        )
    target_is_repo_root = (target_root / ".git").exists()
    if _write_is_owned(
        target_root,
        write_common_dir,
        target_common_dir,
        target_is_repo_root=target_is_repo_root,
    ):
        return write_common_dir
    raise GitWriteEscapeError(
        f"git write would mutate foreign repo (common_dir={write_common_dir}, "
        f"target={target_root}, target_common_dir={target_common_dir})",
        common_dir=write_common_dir,
    )


def _record_git_write(
    receipt: InstallReceipt | None,
    *,
    argv: tuple[str, ...],
    cwd: Path | None,
    common_dir: Path | None,
    duration_ms: float,
    status: str,
) -> None:
    entry: dict[str, Any] = {
        "argv": list(argv),
        "cwd": str(cwd.resolve()) if cwd is not None else None,
        "common_dir": str(common_dir) if common_dir is not None else None,
        "duration_ms": round(duration_ms, 3),
        "status": status,
    }
    if receipt is not None:
        receipt.record_git_write(entry)
        if status == "blocked":
            receipt.failed(
                "git_write_containment",
                reason=entry.get("common_dir") or "foreign git write",
                failure_class="application",
                criticality="abort",
            )


def _git_write(
    target_root: Path,
    *args: str,
    cwd: Path | None = None,
    receipt: InstallReceipt | None = None,
) -> str:
    """Run a mutating ``git`` command only when owned by ``target_root``."""
    from workbay_bootstrap.install import _git

    start = time.monotonic()
    common_dir: Path | None = None
    run_cwd = cwd
    is_clone = bool(args) and args[0] == "clone"
    if _is_mutating_git_args(args):
        try:
            common_dir = _assert_write_owned(target_root, args, cwd)
        except GitWriteEscapeError as err:
            duration_ms = (time.monotonic() - start) * 1000
            _record_git_write(
                receipt,
                argv=args,
                cwd=cwd,
                common_dir=err.common_dir,
                duration_ms=duration_ms,
                status="blocked",
            )
            raise
        # The guard validated ``target_root`` as the owned root; for a non-clone
        # mutating write with no explicit cwd, bind git to that validated dir so it
        # does not run in the ambient process cwd (which the guard never inspected).
        if run_cwd is None and not is_clone:
            run_cwd = target_root.resolve()
    try:
        result = _git(*args, cwd=run_cwd)
    except Exception:
        raise
    else:
        duration_ms = (time.monotonic() - start) * 1000
        _record_git_write(
            receipt,
            argv=args,
            cwd=cwd,
            common_dir=common_dir,
            duration_ms=duration_ms,
            status="ok",
        )
        return result
