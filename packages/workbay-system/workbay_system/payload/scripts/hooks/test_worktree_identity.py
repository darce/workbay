"""Shared worktree identity helpers (internal S0).

Behavior-preserving extraction from guard-bash-main-branch.py so A1 and the
commit-time root-branch guard share one source of truth.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from _worktree_identity import has_linked_worktrees, primary_workspace_root
import _active_task_context as atc


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


def _layout_linked_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """Build a primary + linked worktree layout on disk without shelling out to git.

    Layout mirrors what ``git worktree add`` writes:
    - primary/.git/ (directory)
    - linked/.git  (file: ``gitdir: <primary>/.git/worktrees/<name>``)
    - primary/.git/worktrees/<name>/commondir → ``../..``
    """
    primary = tmp_path / "primary"
    linked = tmp_path / "primary-linked"
    primary.mkdir()
    linked.mkdir()
    (primary / ".git").mkdir()
    wt_private = primary / ".git" / "worktrees" / "primary-linked"
    wt_private.mkdir(parents=True)
    (wt_private / "commondir").write_text("../..\n", encoding="utf-8")
    (linked / ".git").write_text(f"gitdir: {wt_private}\n", encoding="utf-8")
    return primary, linked


def _dir_with_no_git_ancestor() -> Path:
    """Temp dir whose ancestors have no ``.git`` (for true walk-to-root None).

    pytest's default root is often under ``/tmp``, and some hosts keep a real
    ``/tmp/.git``; walk-up correctly treats those paths as inside that repo.
    Prefer a base known free of ``.git`` ancestors.
    """
    for base in (Path("/var/tmp"), Path.home(), Path("/tmp")):
        if not base.is_dir():
            continue
        # Reject bases that already sit under a .git ancestor.
        cursor = base.resolve(strict=False)
        blocked = False
        for _ in range(128):
        # Over-approximation is deliberate: bare .git existence is the SAFE
        # direction for this OPPOSITE contract (find a temp base with no repo
        # above it). Rejecting doubtful .git entries is more conservative about
        # isolation; do not harden this probe to the production ascent predicate.
            if (cursor / ".git").exists():
                blocked = True
                break
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent
        if blocked:
            continue
        return Path(tempfile.mkdtemp(prefix="wt-id-none-", dir=str(base)))
    raise RuntimeError("no base directory free of .git ancestors for negative walk-up")


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


def test_primary_from_on_disk_gitdir_commondir_layout(tmp_path: Path) -> None:
    """Linked worktree resolves to the primary via gitdir/commondir only."""
    primary, linked = _layout_linked_worktree(tmp_path)
    assert primary_workspace_root(linked) == str(primary.resolve(strict=False))
    assert primary_workspace_root(primary) == str(primary.resolve(strict=False))


def test_primary_unresolvable_layout_returns_none() -> None:
    """No .git at all is could-not-determine — never a fabricated path [OBS-08]."""
    root = _dir_with_no_git_ancestor()
    try:
        bare = root / "not-a-repo"
        bare.mkdir()
        assert primary_workspace_root(bare) is None
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_primary_walks_up_from_nested_dir_under_primary(tmp_path: Path) -> None:
    """W1: nested dir under primary resolves to primary, not None."""
    primary, _ = _layout_linked_worktree(tmp_path)
    nested = primary / "some" / "nested" / "dir"
    nested.mkdir(parents=True)
    assert primary_workspace_root(nested) == str(primary.resolve(strict=False))


def test_primary_walks_up_from_nested_dir_under_linked(tmp_path: Path) -> None:
    """W1: nested dir under linked worktree resolves to primary, not linked."""
    primary, linked = _layout_linked_worktree(tmp_path)
    nested = linked / "some" / "nested" / "dir"
    nested.mkdir(parents=True)
    expected = str(primary.resolve(strict=False))
    got = primary_workspace_root(nested)
    assert got == expected
    assert got != str(linked.resolve(strict=False))
    assert got != str(nested.resolve(strict=False))


def test_primary_no_git_up_to_filesystem_root_is_none() -> None:
    """W1: no .git anywhere up the tree is specifically None (not '')."""
    root = _dir_with_no_git_ancestor()
    try:
        bare = root / "a" / "b" / "c"
        bare.mkdir(parents=True)
        assert primary_workspace_root(bare) is None
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_primary_workspace_root_spawns_no_child_process(tmp_path: Path) -> None:
    """Structural: resolution must not call subprocess.run / Popen [ARCH-13]."""
    primary, linked = _layout_linked_worktree(tmp_path)
    nested = primary / "deep" / "nested"
    nested.mkdir(parents=True)
    isolated = _dir_with_no_git_ancestor()
    try:
        missing = isolated / "missing"

        def _boom(*_a, **_k):  # noqa: ANN002, ANN003
            raise AssertionError("primary_workspace_root must not spawn a subprocess")

        with patch("subprocess.run", side_effect=_boom), patch(
            "subprocess.Popen", side_effect=_boom
        ):
            assert primary_workspace_root(linked) == str(primary.resolve(strict=False))
            assert primary_workspace_root(primary) == str(primary.resolve(strict=False))
            assert primary_workspace_root(nested) == str(primary.resolve(strict=False))
            assert primary_workspace_root(missing) is None
    finally:
        shutil.rmtree(isolated, ignore_errors=True)


def test_primary_malformed_commondir_returns_none(tmp_path: Path) -> None:
    """W2: commondir resolving outside a dir named .git is could-not-determine.

    Pins the terminal ``common_dir.name != ".git"`` guard [TEST-04]. Walk-up
    must not paper over a broken linked layout by climbing to a parent repo.
    """
    primary = tmp_path / "primary"
    linked = tmp_path / "primary-linked"
    primary.mkdir()
    linked.mkdir()
    (primary / ".git").mkdir()
    not_git_common = tmp_path / "not-git-common"
    not_git_common.mkdir()
    wt_private = primary / ".git" / "worktrees" / "primary-linked"
    wt_private.mkdir(parents=True)
    (wt_private / "commondir").write_text(f"{not_git_common}\n", encoding="utf-8")
    (linked / ".git").write_text(f"gitdir: {wt_private}\n", encoding="utf-8")
    assert primary_workspace_root(linked) is None
    # Nested under the broken linked worktree must also stay None (no walk-past).
    nested = linked / "deep"
    nested.mkdir()
    assert primary_workspace_root(nested) is None


def test_primary_delegate_matches_identity_source(primary_with_linked) -> None:
    """_active_task_context and _worktree_identity must agree for the same input.

    Uses a real linked worktree so a re-inlined ``return str(resolved_root)``
    copy would diverge from the identity source (TEST-04).
    """
    primary, linked = primary_with_linked
    for root in (primary, linked):
        assert atc._primary_workspace_root(root) == primary_workspace_root(root)
    isolated = _dir_with_no_git_ancestor()
    try:
        bare = isolated / "not-a-repo"
        bare.mkdir()
        assert atc._primary_workspace_root(bare) == primary_workspace_root(bare)
        assert primary_workspace_root(bare) is None
    finally:
        shutil.rmtree(isolated, ignore_errors=True)
