"""Regression: dirty paths on a permitted_main_surface must NOT count as
'dirty protected' and therefore must not block unrelated permitted edits on
main. The user-reported friction was: an untracked WIP file on a permitted
docs surface (e.g. CLAUDE.md, docs/workbay/contracts/**) blocked further
edits to other permitted surfaces, even though the contract explicitly
carves those paths out.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

from _branch_isolation_guard import find_dirty_protected_paths  # noqa: E402
from _harness_protocol import BranchIsolationPolicy, MainSurfacePattern  # noqa: E402


_PROTECTED = frozenset({"main", "master"})


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


def _policy_with_overlap() -> BranchIsolationPolicy:
    """Policy where the same path matches BOTH a protected surface AND a
    permitted carve-out. The carve-out should win for the dirty-paths check.
    """
    return BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=(),
        protected_main_surfaces=(
            MainSurfacePattern(pattern="docs/specs/**", reason="Specs are planning artifacts"),
        ),
        permitted_main_surfaces=(
            MainSurfacePattern(pattern="docs/specs/charter.md", reason="Charter exception stays editable on main"),
        ),
    )


def test_dirty_permitted_path_is_not_counted_as_dirty_protected(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    spec_dir = repo / "docs" / "specs"
    spec_dir.mkdir(parents=True)
    # File is in a permitted carve-out within an otherwise-protected surface.
    (spec_dir / "charter.md").write_text("draft\n", encoding="utf-8")

    result = find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_policy_with_overlap(),
        protected_branches=_PROTECTED,
    )
    assert result is None, f"permitted carve-out path should not be dirty-protected, got {result!r}"


def test_dirty_protected_path_outside_carveout_still_counts(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    spec_dir = repo / "docs" / "specs"
    spec_dir.mkdir(parents=True)
    # Sibling file under the same protected surface but NOT in the carve-out.
    (spec_dir / "roadmap.md").write_text("draft\n", encoding="utf-8")

    result = find_dirty_protected_paths(
        branch="main",
        repo_root=str(repo),
        policy=_policy_with_overlap(),
        protected_branches=_PROTECTED,
    )
    assert result is not None
    _, dirty = result
    assert dirty == ["docs/specs/roadmap.md"]
