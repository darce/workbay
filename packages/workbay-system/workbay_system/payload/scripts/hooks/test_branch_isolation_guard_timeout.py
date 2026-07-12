"""Regression: guard git scans degrade on TimeoutExpired instead of crashing.

internal (three live repros 2026-07-12): the
pre-push ``check_main_clean`` hook aborted a push from a CLEAN main with a raw
``subprocess.TimeoutExpired`` traceback because ``find_dirty_state_files`` ran
``git status`` with ``timeout=5`` and no handler — an fsmonitor stall over a
slow volume was fatal. The guard must treat a slow git call as
could-not-determine ([RES-03]/[AGT-10]), never as a crash, and must disable
fsmonitor on its own scans (root cause).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _branch_isolation_guard as guard  # noqa: E402
from _harness_protocol import load_branch_isolation_policy  # noqa: E402


def _timeout_run(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
    raise subprocess.TimeoutExpired(cmd=cmd, timeout=kwargs.get("timeout", 5))


def test_find_dirty_state_files_survives_status_timeout(tmp_path, monkeypatch, capsys):
    """The 2026-07-12 pre-push repro: status timeout must not raise."""
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    monkeypatch.setattr(guard.subprocess, "run", _timeout_run)

    policy = load_branch_isolation_policy(REPO_ROOT)
    dirty = guard.find_dirty_state_files(repo_root=str(tmp_path), policy=policy)

    assert dirty == []  # degraded scan; pass 2 found nothing on an empty repo
    assert "timed out" in capsys.readouterr().err


def test_git_dirty_paths_survives_timeout(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(guard.subprocess, "run", _timeout_run)
    assert guard._git_dirty_paths(tmp_path) == []
    assert "timed out" in capsys.readouterr().err


def test_run_git_degraded_disables_fsmonitor(monkeypatch):
    """Root cause: the guard's own scans must not depend on the fsmonitor daemon."""
    seen: list[list[str]] = []

    def _capture(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        seen.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(guard.subprocess, "run", _capture)
    guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"])
    assert seen and seen[0][:3] == ["git", "-c", "core.fsmonitor=false"]


def test_untracked_ignored_path_timeout_keeps_path_flagged(tmp_path, monkeypatch):
    """Conservative on timeout: an undeterminable path stays flagged (named)."""
    monkeypatch.setattr(guard.subprocess, "run", _timeout_run)
    assert guard._is_untracked_ignored_path(tmp_path, "some/state.file") is False
