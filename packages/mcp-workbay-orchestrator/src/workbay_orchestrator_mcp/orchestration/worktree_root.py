"""Resolve lane checkout paths from the repository's per-clone Git config."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Literal

WorktreeRootReason = Literal[
    "worktree_root_config_unreadable",
    "worktree_root_not_absolute",
    "worktree_root_missing",
    "worktree_volume_unmounted",
]


class WorktreeRootError(ValueError):
    """A configured checkout root cannot safely be used."""

    def __init__(self, reason: WorktreeRootReason) -> None:
        self.reason = reason
        super().__init__(reason)


_COMMON_DIRS: dict[str, Path] = {}
_CONFIGURED_ROOTS: dict[tuple[str, int], str | None] = {}


def _git(orchestrator_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(orchestrator_root), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise WorktreeRootError("worktree_root_config_unreadable") from exc


def configured_root(orchestrator_root: Path | str) -> str | None:
    """Read the optional root, refreshing it when the common config changes.

    Plain paths without Git metadata retain the sibling layout without a Git
    subprocess. Configured values keep their spelling, including symlinks.
    """
    primary = Path(orchestrator_root)
    if not os.path.lexists(primary / ".git"):
        return None
    realpath = os.path.realpath(primary)
    common = _COMMON_DIRS.get(realpath)
    if common is None:
        result = _git(primary, "rev-parse", "--git-common-dir")
        if result.returncode != 0 or not result.stdout.strip():
            raise WorktreeRootError("worktree_root_config_unreadable")
        common = Path(result.stdout.strip())
        if not common.is_absolute():
            common = Path(realpath) / common
        _COMMON_DIRS[realpath] = common
    try:
        config_mtime = (common / "config").stat().st_mtime_ns
    except OSError as exc:
        raise WorktreeRootError("worktree_root_config_unreadable") from exc
    key = (realpath, config_mtime)
    if key in _CONFIGURED_ROOTS:
        return _CONFIGURED_ROOTS[key]

    result = _git(primary, "config", "--local", "--get", "workbay.worktreeRoot")
    if result.returncode == 1:
        root = None
    elif result.returncode == 0:
        value = result.stdout.strip()
        if not os.path.isabs(value):
            raise WorktreeRootError("worktree_root_not_absolute")
        root = value.rstrip("/") or "/"
    else:
        raise WorktreeRootError("worktree_root_config_unreadable")
    _CONFIGURED_ROOTS[key] = root
    return root


def lane_dir(orchestrator_root: Path | str, suffix: str) -> Path:
    """Build a new lane path, using the sibling default when unset."""
    primary = Path(orchestrator_root)
    root = configured_root(primary)
    if root is None:
        return primary.parent / f"{primary.name}-{suffix}"
    return Path(root) / primary.name / suffix


def expand_lane_path(template: str, *, orchestrator_root: Path | str) -> str:
    """Expand default lane templates, retaining pre-existing sibling paths."""
    prefix = "{orchestrator_root}-"
    if template.startswith(prefix):
        rest = template[len(prefix) :]
        if "/" not in rest and "{" not in rest:
            sibling = f"{orchestrator_root}-{rest}"
            root = configured_root(orchestrator_root)
            if root is None or os.path.lexists(sibling):
                return sibling
            return str(Path(root) / Path(orchestrator_root).name / rest)
    return template.replace("{orchestrator_root}", str(orchestrator_root))
