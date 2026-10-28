"""Regression: guard git scans degrade on TimeoutExpired instead of crashing.

internal (three live repros 2026-07-12): the
pre-push ``check_main_clean`` hook aborted a push from a CLEAN main with a raw
``subprocess.TimeoutExpired`` traceback because ``find_dirty_state_files`` ran
``git status`` with ``timeout=5`` and no handler — an fsmonitor stall over a
slow volume was fatal. The guard must treat a slow git call as
could-not-determine ([RES-03]/[AGT-10]), never as a crash, and must disable
fsmonitor on its own scans (root cause).

internal (live repro 2026-09-05 20:19, right after merge 1b900a2bb): the
post-merge hook's ``git status --porcelain=v1 -z`` hit the 5s timeout, the
killed git left a stale 0-byte ``.git/index.lock`` behind, and the next
``make slice-commit`` died with ``SLICE_COMMIT_STATUS=git_index_lock_held``.
A degraded *read-only* scan must never poison the repository: every guard git
call runs with ``GIT_OPTIONAL_LOCKS=0`` so ``git status`` skips the
opportunistic index refresh and takes no lock, and the budget is tunable via
``WORKBAY_GUARD_GIT_TIMEOUT`` (named in the degrade warning, [AGT-10]/[OBS-08]).
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

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
    """Timeout must not crash; result is could-not-determine (None), not clean []."""
    monkeypatch.setattr(guard.subprocess, "run", _timeout_run)
    assert guard._git_dirty_paths(tmp_path) is None
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


# --- internal: a timed-out scan must not leave ``.git/index.lock`` -----


def _capture_kwargs(seen: list[dict]):  # noqa: ANN202 - test helper
    def _capture(cmd, **kwargs):  # noqa: ANN001, ANN003 - test stub
        seen.append({"cmd": cmd, **kwargs})
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return _capture


def test_run_git_degraded_sets_git_optional_locks_env(monkeypatch):
    """Every guard git call carries GIT_OPTIONAL_LOCKS=0 in a *copy* of the env."""
    seen: list[dict] = []
    monkeypatch.setattr(guard.subprocess, "run", _capture_kwargs(seen))
    monkeypatch.setenv("WB_HOOKLOCK_SENTINEL", "carried")

    guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"])

    assert seen, "subprocess.run was not invoked"
    env = seen[0].get("env")
    assert env is not None, "guard must pass an explicit child env"
    assert env["GIT_OPTIONAL_LOCKS"] == "0"
    # The caller's environment is copied, not dropped.
    assert env["PATH"] == os.environ["PATH"]
    assert env["WB_HOOKLOCK_SENTINEL"] == "carried"
    # The parent process env is untouched.
    assert "GIT_OPTIONAL_LOCKS" not in os.environ or os.environ["GIT_OPTIONAL_LOCKS"] == "0"
    # argv shape preserved (fsmonitor hardening still first).
    assert seen[0]["cmd"][:3] == ["git", "-c", "core.fsmonitor=false"]


@pytest.mark.parametrize("raw", ["abc", "", "-1", "inf", "nan", "0", " "])
def test_guard_git_timeout_env_invalid_falls_back_to_default(monkeypatch, raw):
    """Garbage / empty / non-finite / non-positive knob values fall back to 5.0."""
    seen: list[dict] = []
    monkeypatch.setattr(guard.subprocess, "run", _capture_kwargs(seen))
    monkeypatch.setenv("WORKBAY_GUARD_GIT_TIMEOUT", raw)

    guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"])

    assert seen[0]["timeout"] == 5.0


def test_guard_git_timeout_env_valid_replaces_default(monkeypatch):
    seen: list[dict] = []
    monkeypatch.setattr(guard.subprocess, "run", _capture_kwargs(seen))
    monkeypatch.setenv("WORKBAY_GUARD_GIT_TIMEOUT", "2.5")

    guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"])

    assert seen[0]["timeout"] == 2.5


def test_guard_git_timeout_explicit_caller_wins_over_env(monkeypatch):
    """Precedence pin: explicit ``timeout=`` beats the env knob.

    The env only replaces the *default*; callers that size their own budget
    (e.g. the 30s ignored-file inventory) keep it.
    """
    seen: list[dict] = []
    monkeypatch.setattr(guard.subprocess, "run", _capture_kwargs(seen))
    monkeypatch.setenv("WORKBAY_GUARD_GIT_TIMEOUT", "2.5")

    guard._run_git_degraded(["-C", ".", "ls-files"], timeout=30.0)

    assert seen[0]["timeout"] == 30.0


def test_guard_git_timeout_unset_keeps_default(monkeypatch):
    seen: list[dict] = []
    monkeypatch.setattr(guard.subprocess, "run", _capture_kwargs(seen))
    monkeypatch.delenv("WORKBAY_GUARD_GIT_TIMEOUT", raising=False)

    guard._run_git_degraded(["-C", ".", "status", "--porcelain=v1"])

    assert seen[0]["timeout"] == 5.0


def _write_git_shim(shim_dir: Path, body: str) -> Path:
    """Write an executable fake ``git`` on *shim_dir* with the given sh *body*."""
    shim_dir.mkdir(parents=True, exist_ok=True)
    shim = shim_dir / "git"
    shim.write_text("#!/bin/sh\n" + body)
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return shim


def test_child_git_observes_optional_locks_disabled_end_to_end(tmp_path, monkeypatch):
    """End-to-end: a real child process sees ``GIT_OPTIONAL_LOCKS=0``.

    The shim records its environment and exits 0 immediately, so no timeout is
    in play: this pins the env contract with a real spawn and no timing
    dependency (a fresh script's first exec can cost ~0.5s on macOS, so the
    timeout leg lives in its own test below).
    """
    env_dump = tmp_path / "git-env.txt"
    _write_git_shim(tmp_path / "bin", f"env > '{env_dump}'\nexit 0\n")
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("WB_HOOKLOCK_SENTINEL", "carried")

    result = guard._run_git_degraded(["-C", str(tmp_path), "status", "--porcelain=v1", "-z"], text=False)

    assert result is not None and result.returncode == 0
    recorded = env_dump.read_text().splitlines()
    assert "GIT_OPTIONAL_LOCKS=0" in recorded
    assert "WB_HOOKLOCK_SENTINEL=carried" in recorded  # caller env carried, not dropped


def test_timed_out_scan_honours_env_budget_and_names_knob_end_to_end(tmp_path, monkeypatch, capsys):
    """End-to-end: real subprocess, blocking fake git, tiny env-tuned budget.

    ``exec`` replaces the shell with ``sleep`` so the kill from
    ``subprocess.run`` lands on the only process; nothing is orphaned. The call
    must return ``None`` promptly and the degrade warning must name the knob
    and the lock it avoided ([AGT-10]/[OBS-08]).
    """
    _write_git_shim(tmp_path / "bin", "exec sleep 30\n")
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("WORKBAY_GUARD_GIT_TIMEOUT", "0.2")

    started = time.monotonic()
    result = guard._run_git_degraded(["-C", str(tmp_path), "status", "--porcelain=v1", "-z"], text=False)
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 2.0, f"timeout not honoured: {elapsed:.2f}s"
    err = capsys.readouterr().err
    assert "timed out" in err
    assert "WORKBAY_GUARD_GIT_TIMEOUT" in err
    assert "index.lock" in err
    assert "GIT_OPTIONAL_LOCKS=0" in err


def test_git_dirty_paths_real_git_leaves_no_index_lock(tmp_path):
    """Real-git regression: a clean probe returns [] and leaves no index.lock."""
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / "tracked.txt").write_text("hello\n")
    subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
    subprocess.run(
        [
            "git", "-C", str(repo),
            "-c", "user.name=wb", "-c", "user.email=wb@example.invalid",
            "-c", "commit.gpgsign=false",
            "commit", "-q", "-m", "init",
        ],
        check=True,
    )

    assert guard._git_dirty_paths(repo) == []
    assert not (repo / ".git" / "index.lock").exists()
