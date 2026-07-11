"""internal: the `branch_isolation` contract splits into two
intent-named surfaces.

- ``state_dirty_surfaces`` drives the post-merge ``check-main-clean``
  tripwire (state files only — ``CURRENT_TASK.json``, ``DASHBOARD.txt``,
  handoff DB files, archived task snapshots).
- ``first_edit_protected_surfaces`` drives the PreToolUse file-mutation
  isolation hooks (planning artifacts under ``docs/<type>/**`` and
  package-local mirrors).

This module also pins the new ``find_dirty_state_files()`` helper which
combines direct filesystem checks with ``git status --ignored`` so tracked
state paths still surface when Git reports a directory-level entry, while
ignored-untracked local projections remain non-blocking.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from _branch_isolation_guard import find_dirty_state_files  # noqa: E402
from _harness_protocol import (  # noqa: E402
    BranchIsolationPolicy,
    MainSurfacePattern,
    is_first_edit_protected_path,
    is_state_dirty_path,
    load_branch_isolation_policy,
)


def _git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=5, check=True)
    return proc.stdout.strip()


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    (repo / "seed").write_text("seed\n", encoding="utf-8")
    _git("add", "seed", cwd=repo)
    _git("commit", "-q", "-m", "init", cwd=repo)
    return repo


def _split_policy() -> BranchIsolationPolicy:
    """A hand-rolled policy with both new surfaces populated so the
    predicates and the dirty-state helper can be exercised without
    depending on the live workspace contract.
    """
    return BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(
            MainSurfacePattern(pattern="docs/scopes/**", reason="planning artifact"),
        ),
        permitted_main_surfaces=(),
        state_dirty_surfaces=(
            MainSurfacePattern(pattern="CURRENT_TASK.json", reason="active-task snapshot"),
            MainSurfacePattern(pattern="DASHBOARD.txt", reason="dashboard render"),
            MainSurfacePattern(pattern=".task-state/handoff.db", reason="handoff db"),
            MainSurfacePattern(pattern="docs/tasks/archive/**", reason="archived task snapshots"),
        ),
        first_edit_protected_surfaces=(
            MainSurfacePattern(pattern="docs/scopes/**", reason="scopes from task branch"),
            MainSurfacePattern(pattern="docs/tasks/**/*.md", reason="task plans from task branch"),
        ),
    )


# ---------------------------------------------------------------------------
# Schema: the live workspace contract carries both intent-named surfaces.
# ---------------------------------------------------------------------------


def test_loader_exposes_state_dirty_surfaces_non_empty() -> None:
    policy = load_branch_isolation_policy(REPO_ROOT)
    assert policy.state_dirty_surfaces, "state_dirty_surfaces must be non-empty"
    patterns = {surface.pattern for surface in policy.state_dirty_surfaces}
    assert "CURRENT_TASK.json" in patterns
    assert "DASHBOARD.txt" in patterns


def test_loader_exposes_first_edit_protected_surfaces_non_empty() -> None:
    policy = load_branch_isolation_policy(REPO_ROOT)
    assert policy.first_edit_protected_surfaces, "first_edit_protected_surfaces must be non-empty"
    patterns = {surface.pattern for surface in policy.first_edit_protected_surfaces}
    assert "docs/scopes/**" in patterns


# ---------------------------------------------------------------------------
# Predicates: each path classifies to exactly one surface.
# ---------------------------------------------------------------------------


def test_is_state_dirty_path_matches_state_files_only() -> None:
    policy = _split_policy()
    assert is_state_dirty_path("CURRENT_TASK.json", policy) is True
    assert is_state_dirty_path("DASHBOARD.txt", policy) is True
    assert is_state_dirty_path(".task-state/handoff.db", policy) is True
    # Planning artifacts are NOT state-dirty paths even when they share
    # the legacy ``protected_main_surfaces`` list during transition.
    assert is_state_dirty_path("docs/scopes/foo.md", policy) is False
    assert is_state_dirty_path("docs/tasks/SOME-TASK-task-plan.md", policy) is False


def test_is_first_edit_protected_path_matches_planning_only() -> None:
    policy = _split_policy()
    assert is_first_edit_protected_path("docs/scopes/foo.md", policy) is True
    assert is_first_edit_protected_path("docs/tasks/SOME-TASK-task-plan.md", policy) is True
    # State files must NOT register as first-edit-protected; they have
    # their own post-merge tripwire instead.
    assert is_first_edit_protected_path("CURRENT_TASK.json", policy) is False
    assert is_first_edit_protected_path(".task-state/handoff.db", policy) is False


# ---------------------------------------------------------------------------
# Dirty state-file detection (the internal over-fire fix).
# ---------------------------------------------------------------------------


def test_find_dirty_state_files_ignores_untracked_ignored_local_state(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    # Operator commits a `.gitignore` that excludes local handoff state. A
    # subsequent runtime write to these generated projections is invisible to
    # plain `git status --porcelain` and should remain non-blocking.
    (repo / ".gitignore").write_text(
        ".task-state/\n/CURRENT_TASK.json\n/DASHBOARD.txt\n",
        encoding="utf-8",
    )
    _git("add", ".gitignore", cwd=repo)
    _git("commit", "-q", "-m", "ignore state", cwd=repo)
    state_dir = repo / ".task-state"
    state_dir.mkdir()
    (state_dir / "handoff.db").write_text("sqlite-bytes", encoding="utf-8")
    (repo / "CURRENT_TASK.json").write_text("{}\n", encoding="utf-8")
    (repo / "DASHBOARD.txt").write_text("dashboard\n", encoding="utf-8")

    porcelain = _git("status", "--porcelain=v1", cwd=repo)
    assert porcelain == "", "ignored local state must be invisible to plain porcelain"

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_split_policy())
    assert dirty == [], dirty


def test_find_dirty_state_files_detects_tracked_archive_state(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    archive = repo / "docs" / "tasks" / "archive" / "TASK-1.json"
    archive.parent.mkdir(parents=True)
    archive.write_text('{"status":"done"}\n', encoding="utf-8")
    _git("add", "docs/tasks/archive/TASK-1.json", cwd=repo)
    _git("commit", "-q", "-m", "track archive", cwd=repo)

    archive.write_text('{"status":"done","updated":true}\n', encoding="utf-8")

    dirty = find_dirty_state_files(repo_root=str(repo), policy=_split_policy())
    assert "docs/tasks/archive/TASK-1.json" in dirty, dirty


def test_find_dirty_state_files_detects_untracked_top_level_state(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "CURRENT_TASK.json").write_text("{}\n", encoding="utf-8")
    dirty = find_dirty_state_files(repo_root=str(repo), policy=_split_policy())
    assert "CURRENT_TASK.json" in dirty, dirty


def test_find_dirty_state_files_ignores_planning_artifacts(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    (repo / "docs" / "scopes").mkdir(parents=True)
    (repo / "docs" / "scopes" / "wip-scope.md").write_text("draft\n", encoding="utf-8")
    dirty = find_dirty_state_files(repo_root=str(repo), policy=_split_policy())
    assert dirty == [], f"planning artifacts must not trip the state-file guard, got {dirty!r}"
