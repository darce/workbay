#!/usr/bin/env python3
"""Shared primary-worktree detection for branch-isolation guards.

Single source of truth for ``has_linked_worktrees`` and
``primary_workspace_root`` — used by guard-bash-main-branch (A1) and
check_root_branch (commit-time backstop).
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def has_linked_worktrees(primary: str) -> bool:
    """True when the primary checkout has at least one *live* linked worktree.

    The root-must-stay-on-main invariant only matters in the multi-worktree
    workflow: a single-worktree consumer doing the normal ``git checkout -b
    feature/...`` has no concurrent linked session to strand, so blocking there
    would be hostile.

    ``.git/worktrees/<name>`` dirs exist iff linked worktrees were added, but a
    worktree removed with a raw ``rm -rf`` (no ``git worktree prune``) leaves a
    *prunable* stale entry behind for ~3 months. Counting those raw dirs would
    keep an effectively single-worktree consumer blocked (FUP-2 false positive),
    so each entry is validated: a worktree counts only if it is ``locked`` (git
    never auto-prunes those) or its recorded ``gitdir`` target still exists on
    disk. This mirrors git's own prunable semantics without shelling out.
    """
    try:
        wt_dir = Path(primary) / ".git" / "worktrees"
        if not wt_dir.is_dir():
            return False
        for entry in wt_dir.iterdir():
            if not entry.is_dir():
                continue
            # Locked worktrees are never auto-pruned, even if their path is
            # temporarily absent (e.g. an unmounted removable drive).
            if (entry / "locked").exists():
                return True
            try:
                recorded = (entry / "gitdir").read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if not recorded:
                continue
            # ``gitdir`` records the absolute path to the linked worktree's
            # ``.git`` file; its parent is the worktree itself. A pruned /
            # rm -rf'd worktree no longer exists on disk.
            if Path(recorded).parent.exists():
                return True
        return False
    except OSError:
        return False


def primary_workspace_root(workspace_root: Path) -> str:
    """Resolve the PRIMARY (root) worktree path for ``workspace_root``."""
    resolved_root = workspace_root.resolve(strict=False)
    try:
        proc = subprocess.run(
            ["git", "-C", str(resolved_root), "rev-parse", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return str(resolved_root)

    if proc.returncode == 0 and proc.stdout.strip():
        common_dir = Path(proc.stdout.strip())
        if not common_dir.is_absolute():
            common_dir = resolved_root / common_dir
        common_dir = common_dir.resolve(strict=False)
        if common_dir.name == ".git":
            return str(common_dir.parent)
    return str(resolved_root)
