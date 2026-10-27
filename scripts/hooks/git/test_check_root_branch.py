"""Commit-time root-branch guard (internal S1).

Block/allow/consumer-safe/bypass matrix for check_root_branch.py and the
pre-commit / pre-merge-commit wiring.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

CHECK_SCRIPT = Path(__file__).resolve().parent / "check_root_branch.py"
PRE_COMMIT = Path(__file__).resolve().parent / "pre-commit"
HOOKS_DIR = Path(__file__).resolve().parent.parent

# Neutralize any inherited branch-naming override so the chained
# check_branch_naming.py gate behaves deterministically under test.
_NO_NAMING_OVERRIDE = {"WORKBAY_ALLOW_NONCONFORMING_BRANCH": ""}


def _git(*args: str, cwd: Path, env: dict | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        capture_output=True,
        text=True,
        check=False,
        env=merged,
    )


def _init_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    (path / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "-A", cwd=path).check_returncode()
    _git("commit", "-q", "-m", "init", cwd=path).check_returncode()


def _install_hooks(repo: Path) -> None:
    guard_link = repo / "scripts" / "hooks"
    guard_link.parent.mkdir(parents=True, exist_ok=True)
    if not guard_link.exists():
        guard_link.symlink_to(HOOKS_DIR.resolve())
    _git("config", "core.hooksPath", "scripts/hooks/git", cwd=repo).check_returncode()


def _invoke_check(cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECK_SCRIPT)],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def primary_with_linked(tmp_path: Path) -> tuple[Path, Path]:
    primary = tmp_path / "primary"
    primary.mkdir()
    _init_repo(primary)
    linked = tmp_path / "primary-linked"
    _git("worktree", "add", "-q", str(linked), "-b", "feature/seed", cwd=primary).check_returncode()
    _install_hooks(primary)
    _install_hooks(linked)
    return primary, linked


@pytest.fixture
def single_worktree(tmp_path: Path) -> Path:
    repo = tmp_path / "solo"
    repo.mkdir()
    _init_repo(repo)
    _install_hooks(repo)
    return repo


def test_blocks_primary_non_main_with_linked_worktrees(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    _git("checkout", "-q", "-b", "feature/x", cwd=primary).check_returncode()
    proc = _invoke_check(primary)
    assert proc.returncode == 1, proc.stderr
    assert "BLOCKED" in proc.stderr and "PRIMARY" in proc.stderr


def test_allows_primary_on_main(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke_check(primary)
    assert proc.returncode == 0, proc.stderr


def test_allows_single_worktree_feature_branch(single_worktree: Path) -> None:
    _git("checkout", "-q", "-b", "feature/x", cwd=single_worktree).check_returncode()
    proc = _invoke_check(single_worktree)
    assert proc.returncode == 0, proc.stderr


def test_allows_linked_worktree_feature_branch(primary_with_linked) -> None:
    _, linked = primary_with_linked
    proc = _invoke_check(linked)
    assert proc.returncode == 0, proc.stderr


def test_allows_primary_after_linked_worktree_removed(primary_with_linked) -> None:
    """RCG-A-3 / RCG-B-1: a linked worktree removed with a raw ``rm -rf`` (no
    ``git worktree prune``) leaves a stale/prunable ``.git/worktrees/<name>``
    entry. has_linked_worktrees must NOT count it, so an effectively
    single-worktree consumer's feature-branch commit is allowed (no FUP-2
    false positive)."""
    primary, linked = primary_with_linked
    shutil.rmtree(linked)  # simulate `rm -rf` without `git worktree prune`
    _git("checkout", "-q", "-b", "feature/x", cwd=primary).check_returncode()
    proc = _invoke_check(primary)
    assert proc.returncode == 0, proc.stderr


def test_pre_commit_blocks_primary_non_main(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    _git("checkout", "-q", "-b", "feature/x", cwd=primary).check_returncode()
    (primary / "touch.txt").write_text("x\n", encoding="utf-8")
    _git("add", "touch.txt", cwd=primary).check_returncode()
    proc = _git("commit", "-m", "should block", cwd=primary)
    assert proc.returncode != 0
    assert "BLOCKED" in (proc.stderr or proc.stdout)


def test_pre_commit_allows_primary_on_main(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    (primary / "touch.txt").write_text("x\n", encoding="utf-8")
    _git("add", "touch.txt", cwd=primary).check_returncode()
    proc = _git("commit", "-m", "ok on main", cwd=primary)
    assert proc.returncode == 0, proc.stderr


def test_pre_commit_allows_single_worktree_feature_commit(single_worktree: Path) -> None:
    """RCG-B-4: end-to-end (real ``git commit``, not just the direct script
    call) that a single-worktree consumer on a conforming feature branch is
    unaffected by the backstop."""
    _git("checkout", "-q", "-b", "feature/wb-solo-1", cwd=single_worktree).check_returncode()
    (single_worktree / "s.txt").write_text("x\n", encoding="utf-8")
    _git("add", "s.txt", cwd=single_worktree).check_returncode()
    proc = _git("commit", "-m", "ok solo feature", cwd=single_worktree)
    assert proc.returncode == 0, proc.stderr + proc.stdout


def test_pre_commit_allows_linked_worktree_feature_commit(primary_with_linked) -> None:
    """RCG-B-4: end-to-end that a commit inside a LINKED worktree on a
    conforming feature branch proceeds (the backstop only fires in the
    primary)."""
    _, linked = primary_with_linked
    _git("checkout", "-q", "-b", "feature/wb-linked-2", cwd=linked).check_returncode()
    (linked / "l.txt").write_text("x\n", encoding="utf-8")
    _git("add", "l.txt", cwd=linked).check_returncode()
    proc = _git("commit", "-m", "ok linked feature", cwd=linked)
    assert proc.returncode == 0, proc.stderr + proc.stdout


def test_pre_commit_branch_naming_exec_still_runs(single_worktree: Path) -> None:
    """RCG-B-4: when check_root_branch ALLOWS a commit (single worktree), the
    chained ``exec check_branch_naming.py`` must still run — proving the new
    check did not clobber the existing gate. A non-conforming branch name is
    blocked by check_branch_naming, not by the root-branch backstop."""
    _git("checkout", "-q", "-b", "wip", cwd=single_worktree).check_returncode()
    (single_worktree / "n.txt").write_text("x\n", encoding="utf-8")
    _git("add", "n.txt", cwd=single_worktree).check_returncode()
    proc = _git("commit", "-m", "bad branch name", cwd=single_worktree, env=_NO_NAMING_OVERRIDE)
    out = proc.stderr + proc.stdout
    assert proc.returncode != 0, out
    # The branch-naming gate's signature, distinct from the root-branch backstop.
    assert "does not match" in out and "PRIMARY" not in out


def test_no_verify_bypasses_pre_commit(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    _git("checkout", "-q", "-b", "feature/y", cwd=primary).check_returncode()
    (primary / "bypass.txt").write_text("x\n", encoding="utf-8")
    _git("add", "bypass.txt", cwd=primary).check_returncode()
    proc = _git("commit", "--no-verify", "-m", "bypass", cwd=primary)
    assert proc.returncode == 0, proc.stderr


def test_pre_merge_commit_blocks_primary_non_main(primary_with_linked) -> None:
    """RCG-A-1: git runs pre-commit for ordinary commits but NOT for the merge
    commit created by ``git merge --no-ff``. The pre-merge-commit hook must
    extend the backstop to that path — the repo's actual integration mechanism."""
    primary, _ = primary_with_linked
    # A topic branch with a commit to merge in (--no-verify: primary is off main
    # with a linked worktree, so the pre-commit guard would otherwise block it).
    _git("checkout", "-q", "-b", "topic", cwd=primary).check_returncode()
    (primary / "t.txt").write_text("t\n", encoding="utf-8")
    _git("add", "t.txt", cwd=primary).check_returncode()
    _git("commit", "--no-verify", "-q", "-m", "topic work", cwd=primary).check_returncode()
    # Onto a non-main feature branch at the original main tip, then non-ff merge.
    _git("checkout", "-q", "main", cwd=primary).check_returncode()
    _git("checkout", "-q", "-b", "feature/x", cwd=primary).check_returncode()
    proc = _git("merge", "--no-ff", "-m", "merge topic", "topic", cwd=primary)
    out = proc.stderr + proc.stdout
    assert proc.returncode != 0, out
    assert "BLOCKED" in out and "PRIMARY" in out


def test_pre_merge_commit_allows_primary_on_main(primary_with_linked) -> None:
    """RCG-A-1: a legitimate non-ff integration merge onto main (the protected
    branch) must proceed."""
    primary, _ = primary_with_linked
    _git("checkout", "-q", "-b", "topic", cwd=primary).check_returncode()
    (primary / "t.txt").write_text("t\n", encoding="utf-8")
    _git("add", "t.txt", cwd=primary).check_returncode()
    _git("commit", "--no-verify", "-q", "-m", "topic work", cwd=primary).check_returncode()
    _git("checkout", "-q", "main", cwd=primary).check_returncode()
    proc = _git("merge", "--no-ff", "-m", "merge topic into main", "topic", cwd=primary)
    assert proc.returncode == 0, proc.stderr + proc.stdout


def test_pre_merge_commit_no_verify_bypasses(primary_with_linked) -> None:
    """RCG-A-1: ``git merge --no-verify`` bypasses the merge-time backstop,
    mirroring the pre-commit escape hatch."""
    primary, _ = primary_with_linked
    _git("checkout", "-q", "-b", "topic", cwd=primary).check_returncode()
    (primary / "t.txt").write_text("t\n", encoding="utf-8")
    _git("add", "t.txt", cwd=primary).check_returncode()
    _git("commit", "--no-verify", "-q", "-m", "topic work", cwd=primary).check_returncode()
    _git("checkout", "-q", "main", cwd=primary).check_returncode()
    _git("checkout", "-q", "-b", "feature/x", cwd=primary).check_returncode()
    proc = _git("merge", "--no-verify", "--no-ff", "-m", "merge topic", "topic", cwd=primary)
    assert proc.returncode == 0, proc.stderr + proc.stdout


def test_a1_regression_suite_still_passes() -> None:
    """RCG-1: after extracting the shared _worktree_identity helper, re-run the
    A1 root-switch behavior suite plus the extracted-helper unit tests to
    confirm guard-bash-main-branch (A1) did not regress. (Scoped to those two
    files; it is the explicit extraction-regression guard, not a full sweep.)"""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            str(HOOKS_DIR / "test_guard_bash_main_branch_root_switch.py"),
            str(HOOKS_DIR / "test_worktree_identity.py"),
        ],
        cwd=HOOKS_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
