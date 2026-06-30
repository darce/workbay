"""Shared worktree identity helpers (internal S0).

Behavior-preserving extraction from guard-bash-main-branch.py so A1 and the
commit-time root-branch guard share one source of truth.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from _worktree_identity import has_linked_worktrees, primary_workspace_root


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    (path / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "-A", cwd=path)
    _git("commit", "-q", "-m", "init", cwd=path)


@pytest.fixture
def primary_with_linked(tmp_path: Path) -> tuple[Path, Path]:
    primary = tmp_path / "primary"
    primary.mkdir()
    _init_repo(primary)
    linked = tmp_path / "primary-linked"
    _git("worktree", "add", "-q", str(linked), "-b", "feature/seed", cwd=primary)
    return primary, linked


def test_has_linked_worktrees_false_for_single_worktree(tmp_path: Path) -> None:
    repo = tmp_path / "solo"
    repo.mkdir()
    _init_repo(repo)
    assert has_linked_worktrees(str(repo.resolve(strict=False))) is False


def test_has_linked_worktrees_true_when_linked_exists(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    assert has_linked_worktrees(str(primary.resolve(strict=False))) is True


def test_has_linked_worktrees_false_when_linked_dir_removed(primary_with_linked) -> None:
    """RCG-A-3 / RCG-B-1: a worktree dir deleted with raw ``rm -rf`` (no
    ``git worktree prune``) leaves a stale/prunable entry whose recorded gitdir
    target is gone. It must not count as a live linked worktree."""
    primary, linked = primary_with_linked
    shutil.rmtree(linked)
    assert has_linked_worktrees(str(primary.resolve(strict=False))) is False


def test_primary_workspace_root_matches_primary_checkout(primary_with_linked) -> None:
    primary, linked = primary_with_linked
    assert primary_workspace_root(primary) == str(primary.resolve(strict=False))
    assert primary_workspace_root(linked) == str(primary.resolve(strict=False))
