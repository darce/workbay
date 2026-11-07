"""Tests for check_root_on_main.py (post-checkout root-worktree guard)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "check_root_on_main.py"

_spec = importlib.util.spec_from_file_location("check_root_on_main", SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules[_spec.name] = _mod  # type: ignore[index]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


def test_main_returns_0_on_non_root_worktree(monkeypatch):
    monkeypatch.setattr(_mod, "is_root_worktree", lambda: False)
    assert _mod.main() == 0


def test_main_returns_0_on_root_worktree_main_branch(monkeypatch):
    monkeypatch.setattr(_mod, "is_root_worktree", lambda: True)
    monkeypatch.setattr(_mod, "current_branch", lambda: "main")
    assert _mod.main() == 0


def test_main_returns_0_on_root_worktree_master_branch(monkeypatch):
    monkeypatch.setattr(_mod, "is_root_worktree", lambda: True)
    monkeypatch.setattr(_mod, "current_branch", lambda: "master")
    assert _mod.main() == 0


def test_main_warns_on_root_worktree_feature_branch(monkeypatch, capsys):
    monkeypatch.setattr(_mod, "is_root_worktree", lambda: True)
    monkeypatch.setattr(_mod, "current_branch", lambda: "feature/e17-10")
    result = _mod.main()
    assert result == 0  # advisory only
    captured = capsys.readouterr()
    assert "ROOT WORKTREE ON NON-MAIN BRANCH" in captured.err
    assert "feature/e17-10" in captured.err


def test_main_returns_0_on_empty_branch(monkeypatch):
    """Detached HEAD or failure to detect branch should not warn."""
    monkeypatch.setattr(_mod, "is_root_worktree", lambda: True)
    monkeypatch.setattr(_mod, "current_branch", lambda: "")
    assert _mod.main() == 0
