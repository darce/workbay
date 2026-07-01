"""Root-worktree branch-switch guard (internal / A1).

Drives ``guard-bash-main-branch.py`` as a subprocess against tmp repos that
mirror the misroute configuration: a PRIMARY checkout on main with at least one
LINKED worktree. In that configuration a branch *creation/switch* that targets
the primary worktree must hard-block (exit 2) so a concurrent session's
main-integration commit can never land on a feature branch. Single-worktree
consumers (the normal `git checkout -b` workflow) must stay unaffected.

HOOK_SCRIPT uses the sibling path (not the materialized-only parents[] form the
lightweight test uses) so these run from the payload source tree too.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-bash-main-branch.py"


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
    """A primary repo on main plus one linked worktree (multi-worktree config)."""
    primary = tmp_path / "primary"
    primary.mkdir()
    _init_repo(primary)
    linked = tmp_path / "primary-linked"
    _git("worktree", "add", "-q", str(linked), "-b", "feature/seed", cwd=primary)
    return primary, linked


@pytest.fixture
def single_worktree(tmp_path: Path) -> Path:
    repo = tmp_path / "solo"
    repo.mkdir()
    _init_repo(repo)
    return repo


def _invoke(cwd: Path, command: str, *, extra_env: dict | None = None) -> subprocess.CompletedProcess[str]:
    payload = {"toolName": "Bash", "toolInput": {"command": command}}
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=cwd,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


# --- BLOCK: branch creation / switch that targets the primary worktree --------


def test_checkout_b_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr and "PRIMARY" in proc.stderr


def test_switch_c_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch -c feature/x")
    assert proc.returncode == 2, proc.stderr


def test_plain_switch_to_nonmain_in_primary_blocked(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch feature/seed")
    assert proc.returncode == 2, proc.stderr


def test_cross_worktree_dashC_from_linked_blocked(primary_with_linked) -> None:
    # GPR-1: `git -C <primary> checkout -b` issued from a LINKED worktree cwd
    # (on a feature branch) is the real misroute and must still be caught.
    primary, linked = primary_with_linked
    proc = _invoke(linked, f"git -C {primary} checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


def test_cd_to_primary_from_linked_blocked(primary_with_linked) -> None:
    primary, linked = primary_with_linked
    proc = _invoke(linked, f"cd {primary} && git checkout -b feature/x")
    assert proc.returncode == 2, proc.stderr


# --- ALLOW: switch-to-main, worktree add, bypass, ambiguous checkout ----------


def test_checkout_main_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout main")
    assert proc.returncode == 0, proc.stderr


def test_switch_main_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git switch main")
    assert proc.returncode == 0, proc.stderr


def test_worktree_add_allowed(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git worktree add ../wt2 -b feature/y")
    assert proc.returncode == 0, proc.stderr


def test_inline_bypass_token_allows(primary_with_linked) -> None:
    primary, _ = primary_with_linked
    proc = _invoke(primary, "WORKBAY_ALLOW_ROOT_BRANCH_SWITCH=1 git checkout -b feature/x")
    assert proc.returncode == 0, proc.stderr
    assert "bypass" in proc.stderr.lower()


def test_ambiguous_checkout_path_not_treated_as_switch(primary_with_linked) -> None:
    # `git checkout -- <file>` (no -b) is a restore, not a branch switch; the
    # switch-guard must not fire (README.md is unprotected so write-scan passes).
    primary, _ = primary_with_linked
    proc = _invoke(primary, "git checkout -- README.md")
    assert proc.returncode == 0, proc.stderr


# --- CONSUMER-SAFE: single-worktree repos keep the normal branch workflow -----


def test_single_worktree_checkout_b_allowed(single_worktree: Path) -> None:
    # No linked worktrees ⇒ no concurrent session to strand ⇒ normal workflow.
    proc = _invoke(single_worktree, "git checkout -b feature/x")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr
