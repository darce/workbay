#!/usr/bin/env python3
"""Shared primary-worktree detection for branch-isolation guards.

Single source of truth for ``has_linked_worktrees`` and
``primary_workspace_root`` — used by guard-bash-main-branch (A1) and
check_root_branch (commit-time backstop).
"""

from __future__ import annotations

from pathlib import Path

# Bound upward ``.git`` search. Reproduces rev-parse's walk without a
# subprocess and without unbounded parent recursion [ARCH-13].
_MAX_GIT_WALK_UP_HOPS = 128


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


def primary_workspace_root(workspace_root: Path) -> str | None:
    """Resolve the PRIMARY (root) worktree path for ``workspace_root``.

    Pure stdlib filesystem walk of git's on-disk metadata — no subprocess, no
    timeout, no process-creation stall [ARCH-13][RES-03].

    Walks parent directories looking for a ``.git`` entry (bounded; stops at
    the filesystem root), matching ``git rev-parse``'s upward search without a
    subprocess. Once a ``.git`` entry is found, resolution is authoritative:
    failure to parse or resolve it yields ``None`` rather than continuing
    upward (which would silently attribute a nested repo to its parent)
    [OBS-08].

    Returns ``None`` when the layout cannot be determined (missing/unreadable
    ``.git``, unparseable ``gitdir:``, missing ``commondir``, common dir not
    named ``.git``). Never fabricates an answer by returning the caller's own
    directory [OBS-08].
    """
    try:
        start = workspace_root.resolve(strict=False)
    except OSError:
        return None

    # Walk only via ``.parent`` (no re-resolve per hop) so symlink escapes
    # cannot pull the walk out of the tree [ARCH-13].
    cursor = start
    found_at: Path | None = None
    git_entry: Path | None = None
    for _ in range(_MAX_GIT_WALK_UP_HOPS):
        candidate = cursor / ".git"
        try:
            is_dir = candidate.is_dir()
            is_file = (not is_dir) and candidate.is_file()
        except OSError:
            return None
        if is_dir or is_file:
            found_at = cursor
            git_entry = candidate
            break
        parent = cursor.parent
        if parent == cursor:
            return None
        cursor = parent
    else:
        return None

    if found_at is None or git_entry is None:
        return None

    try:
        if git_entry.is_dir():
            # Primary checkout: the worktree root is itself the primary.
            return str(found_at)
        if not git_entry.is_file():
            return None
        raw = git_entry.read_text(encoding="utf-8")
    except OSError:
        return None

    # Linked worktree: ``.git`` is a file with ``gitdir: <path>``.
    # Resolution from here is authoritative — do not walk past this ``.git``.
    gitdir_path: Path | None = None
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if lower.startswith("gitdir:"):
            payload = stripped[len("gitdir:") :].strip()
            if not payload:
                return None
            candidate = Path(payload)
            if not candidate.is_absolute():
                candidate = found_at / candidate
            try:
                gitdir_path = candidate.resolve(strict=False)
            except OSError:
                return None
            break
    if gitdir_path is None:
        return None

    try:
        common_raw = (gitdir_path / "commondir").read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not common_raw:
        return None

    common_dir = Path(common_raw)
    if not common_dir.is_absolute():
        common_dir = gitdir_path / common_dir
    try:
        common_dir = common_dir.resolve(strict=False)
    except OSError:
        return None
    if common_dir.name != ".git":
        return None
    return str(common_dir.parent)
