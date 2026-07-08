#!/usr/bin/env python3
"""Warn when the root worktree is checked out on a non-main branch.

Called from the post-checkout git hook. Exits 0 always (advisory only — git
has no pre-checkout hook to block). The PreToolUse guard in _worktree_drift.py
enforces the hard block on subsequent edits.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MAIN_BRANCHES = frozenset({"main", "master"})


def is_root_worktree() -> bool:
    """Return True if the current working directory is the root (non-linked) worktree.

    Root worktrees have a `.git` *directory*; linked worktrees have a `.git`
    *file* pointing to the shared gitdir.
    """
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            capture_output=True, text=True, timeout=3, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if proc.returncode != 0:
        return False
    git_dir = Path(proc.stdout.strip())
    # Absolute or relative, resolve to check if it's a directory
    if not git_dir.is_absolute():
        git_dir = Path.cwd() / git_dir
    return git_dir.is_dir() and git_dir.name == ".git"


def current_branch() -> str:
    try:
        proc = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True, text=True, timeout=3, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def main() -> int:
    if not is_root_worktree():
        return 0
    branch = current_branch()
    if not branch or branch in MAIN_BRANCHES:
        return 0
    print(
        f"\n⚠ ROOT WORKTREE ON NON-MAIN BRANCH: {branch}\n"
        "  The root worktree must stay on main. Use a linked worktree instead:\n"
        f"    git checkout main\n"
        f"    git worktree add ../<repo>-<task-id> -b {branch}\n"
        "  The PreToolUse guard will block edits until this is fixed.\n",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
