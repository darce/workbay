"""Working-tree integrity helpers and MCP tools.

Items E + G of the working-tree-integrity assessment:
- `working_tree_integrity_check`: compare tracked-but-modified paths against
  `.task-state/dirty-allowlist`. Wired into `handoff_close_check` so the
  pre-merge gate refuses to pass when the tree has drifted from HEAD.
- `post_merge_integrity_check`: immediately after a merge fast-forward,
  confirm the working tree is a subset of the expected changed-file set.

The logic mirrors the bash-side check in `scripts/task-finish.sh` and the
Python-side warning in `scripts/check-task-context.py` so all three
surfaces report the same set of paths.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from .runtime import get_runtime_config
from .shared_primitives import _envelope

# `.task-state/dirty-allowlist` is gitignored, so fresh worktrees need these
# DB-derived operator views tolerated by default.
_IMPLICIT_DIRTY_ALLOWLIST: frozenset[str] = frozenset({"DASHBOARD.txt", "CURRENT_TASK.json"})


def _resolve_workspace_root(workspace_root: str | Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root).expanduser().resolve()
    return get_runtime_config().workspace_root


def _git_dirty_paths(workspace_root: Path) -> list[str] | None:
    """Return tracked-but-modified paths (vs HEAD), or None when git is unavailable."""
    try:
        proc = subprocess.run(
            ["git", "diff", "--name-only", "HEAD"],
            cwd=str(workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def _load_dirty_allowlist(state_dir: Path) -> set[str]:
    """Read `.task-state/dirty-allowlist` into a set of repo-relative paths."""
    allowlist_path = state_dir / "dirty-allowlist"
    if not allowlist_path.exists():
        return set()
    allowed: set[str] = set()
    try:
        for raw in allowlist_path.read_text().splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            allowed.add(stripped)
    except OSError:
        return set()
    return allowed


def _effective_dirty_allowlist(state_dir: Path) -> set[str]:
    return _load_dirty_allowlist(state_dir) | set(_IMPLICIT_DIRTY_ALLOWLIST)


def _check_working_tree_integrity(
    workspace_root: str | Path | None = None,
    expected_dirty: list[str] | None = None,
) -> dict:
    """Shared helper: diff working tree vs HEAD against an expected-dirty set.

    Returns a dict with ok/dirty_paths/unexpected_dirty/allowlist_source.
    `ok=True` when the tree is clean or differs only on expected paths.
    """
    resolved_workspace = _resolve_workspace_root(workspace_root)
    dirty = _git_dirty_paths(resolved_workspace)
    if dirty is None:
        return {
            "ok": True,
            "git_unavailable": True,
            "workspace_root": str(resolved_workspace),
            "dirty_paths": [],
            "unexpected_dirty": [],
            "allowlist": [],
            "allowlist_source": None,
        }
    if expected_dirty is not None:
        allowlist = {str(p).strip() for p in expected_dirty if str(p).strip()}
        allowlist_source = "param:expected_dirty"
    else:
        state_dir = get_runtime_config().state_dir
        allowlist = _effective_dirty_allowlist(state_dir)
        allowlist_source = str(state_dir / "dirty-allowlist")
    unexpected = sorted(p for p in dirty if p not in allowlist)
    return {
        "ok": not unexpected,
        "workspace_root": str(resolved_workspace),
        "dirty_paths": dirty,
        "unexpected_dirty": unexpected,
        "allowlist": sorted(allowlist),
        "allowlist_source": allowlist_source,
    }


def working_tree_integrity_check(
    workspace_root: str | None = None,
    expected_dirty: list[str] | None = None,
) -> dict:
    """MCP tool wrapper for `_check_working_tree_integrity`."""
    result = _check_working_tree_integrity(workspace_root=workspace_root, expected_dirty=expected_dirty)
    return _envelope(ok=result["ok"], tool="working_tree_integrity_check", data=result)


def _git_diff_names_since(workspace_root: Path, merged_sha: str) -> list[str] | None:
    """Return paths modified between `merged_sha` and the working tree, or None on git error."""
    try:
        proc = subprocess.run(
            ["git", "diff", "--name-only", merged_sha],
            cwd=str(workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def post_merge_integrity_check(
    merged_sha: str,
    expected_changed_files: list[str],
    workspace_root: str | None = None,
) -> dict:
    """Verify the working tree at HEAD matches the merge expectation.

    After `git merge --ff-only`, diffs the working tree against `merged_sha`
    and returns `ok=False` with the divergence list when anything outside
    `expected_changed_files` has been modified since the merge committed.
    Typical caller flow: merge, call this, then `make task-finish`.
    """
    if not isinstance(merged_sha, str) or not merged_sha.strip():
        return _envelope(
            ok=False,
            tool="post_merge_integrity_check",
            data={"error": "merged_sha must be a non-empty string."},
        )
    expected = {str(p).strip() for p in (expected_changed_files or []) if str(p).strip()}
    resolved_workspace = _resolve_workspace_root(workspace_root)
    diff = _git_diff_names_since(resolved_workspace, merged_sha.strip())
    if diff is None:
        return _envelope(
            ok=False,
            tool="post_merge_integrity_check",
            data={
                "error": f"git diff against {merged_sha!r} failed in {resolved_workspace}",
                "workspace_root": str(resolved_workspace),
            },
        )
    divergence = sorted(p for p in diff if p not in expected)
    data = {
        "ok": not divergence,
        "merged_sha": merged_sha.strip(),
        "workspace_root": str(resolved_workspace),
        "diff_paths": diff,
        "expected_changed_files": sorted(expected),
        "divergence": divergence,
    }
    return _envelope(ok=not divergence, tool="post_merge_integrity_check", data=data)
