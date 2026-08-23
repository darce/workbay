"""Shared git merge/reachability helpers."""

from __future__ import annotations

import subprocess

from .runtime import get_runtime_config


def is_ancestor_of_ref(candidate: str, integration_ref: str) -> bool:
    """Return True iff ``candidate`` is an ancestor of (or equal to) ``integration_ref`` HEAD."""
    config = get_runtime_config()
    try:
        proc = subprocess.run(
            ["git", "merge-base", "--is-ancestor", candidate, integration_ref],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def branch_is_merged(target_branch: str, integration_ref: str = "main") -> bool:
    """True when ``target_branch`` is fully merged into ``integration_ref``."""
    if not target_branch:
        return False
    if is_ancestor_of_ref(target_branch, integration_ref):
        return True
    # Post-merge on main often deletes the local feature branch while the
    # remote-tracking ref still resolves (typical ``git pull`` of a PR merge).
    return is_ancestor_of_ref(f"origin/{target_branch}", integration_ref)


def branch_exists(target_branch: str) -> bool:
    """True when a local branch ref exists for ``target_branch``."""
    if not target_branch:
        return False
    config = get_runtime_config()
    try:
        proc = subprocess.run(
            ["git", "show-ref", "--verify", f"refs/heads/{target_branch}"],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0
