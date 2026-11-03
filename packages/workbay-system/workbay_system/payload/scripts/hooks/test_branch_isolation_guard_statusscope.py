"""Regression tests for the state-scoped pass-1 git status scan."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hooks"))

import _branch_isolation_guard as guard  # noqa: E402
from _harness_protocol import BranchIsolationPolicy, MainSurfacePattern  # noqa: E402

STATE_PATTERNS = (
    "CURRENT_TASK.json",
    "DASHBOARD.txt",
    ".task-state/handoff.db",
    ".task-state/handoff.db-shm",
    ".task-state/handoff.db-wal",
    "docs/tasks/archive/**",
    "packages/*/docs/tasks/archive/**",
)


EXPECTED_DIRTY = [
    "CURRENT_TASK.json",
    "DASHBOARD.txt",
    "docs/tasks/archive/skip.json",
    "packages/demo/docs/tasks/archive/modified.json",
    "packages/demo/nested/docs/tasks/archive/deep.json",
]


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


def _policy(patterns: tuple[str, ...] = STATE_PATTERNS) -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("apps/",),
        protected_extensions=(".py",),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
        state_dirty_surfaces=tuple(
            MainSurfacePattern(pattern=pattern, reason="test state surface") for pattern in patterns
        ),
        first_edit_protected_surfaces=(),
    )


def _populate_status_fixture(repo: Path) -> None:
    (repo / ".gitignore").write_text(
        "/outside-noise/*.txt\n/.task-state/handoff.db\n/.task-state/handoff.db-shm\n/.task-state/handoff.db-wal\n",
        encoding="utf-8",
    )
    _git("add", ".gitignore", cwd=repo)
    _git("commit", "-q", "-m", "ignore local state", cwd=repo)

    tracked_state = {
        "CURRENT_TASK.json": '{"task_ref":"before"}\n',
        "docs/tasks/archive/skip.json": '{"status":"done"}\n',
        "docs/tasks/archive/clean.json": '{"status":"done"}\n',
        "packages/demo/docs/tasks/archive/modified.json": '{"status":"done"}\n',
        "packages/demo/nested/docs/tasks/archive/deep.json": '{"status":"done"}\n',
    }
    for rel_path, contents in tracked_state.items():
        path = repo / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
    _git("add", *tracked_state, cwd=repo)
    _git("commit", "-q", "-m", "track state files", cwd=repo)

    _git("update-index", "--skip-worktree", "docs/tasks/archive/skip.json", cwd=repo)
    (repo / "CURRENT_TASK.json").write_text('{"task_ref":"after"}\n', encoding="utf-8")
    (repo / "packages/demo/docs/tasks/archive/modified.json").write_text('{"status":"changed"}\n', encoding="utf-8")
    # fnmatch lets ``*`` cross ``/``, so this is a state surface; a ``:(glob)``
    # pathspec would drop it from the scoped status.
    (repo / "packages/demo/nested/docs/tasks/archive/deep.json").write_text('{"status":"changed"}\n', encoding="utf-8")
    (repo / "docs/tasks/archive/skip.json").write_text('{"status":"changed while skip-worktree"}\n', encoding="utf-8")

    (repo / "DASHBOARD.txt").write_text("untracked state\n", encoding="utf-8")
    ignored_state = repo / ".task-state" / "handoff.db"
    ignored_state.parent.mkdir(parents=True)
    ignored_state.write_bytes(b"ignored local projection")

    noise = repo / "outside-noise"
    noise.mkdir()
    for index in range(3000):
        (noise / f"ignored-{index:04d}.txt").write_text("noise\n", encoding="utf-8")


def test_find_dirty_state_files_scopes_pass1_without_changing_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_repo(tmp_path)
    _populate_status_fixture(repo)

    original_run = guard._run_git_degraded
    calls: list[list[str]] = []
    remove_pathspecs = False

    def capture_run(args: list[str], **kwargs):  # noqa: ANN001, ANN003 - test wrapper
        nonlocal remove_pathspecs
        calls.append(list(args))
        actual_args = args
        if remove_pathspecs and "status" in args and "--" in args:
            actual_args = args[: args.index("--")]
        return original_run(actual_args, **kwargs)

    monkeypatch.setattr(guard, "_run_git_degraded", capture_run)
    policy = _policy()
    scoped_result = guard.find_dirty_state_files(repo_root=str(repo), policy=policy)

    status_args = next(args for args in calls if "status" in args)
    assert "--" in status_args, f"pass-1 status must scope its paths: {status_args!r}"
    assert status_args[status_args.index("--") + 1 :] == list(STATE_PATTERNS)

    calls.clear()
    remove_pathspecs = True
    unscoped_result = guard.find_dirty_state_files(repo_root=str(repo), policy=policy)

    assert scoped_result == unscoped_result
    assert scoped_result == EXPECTED_DIRTY


@pytest.mark.parametrize("mode_var", ["GIT_LITERAL_PATHSPECS", "GIT_GLOB_PATHSPECS"])
def test_inherited_pathspec_mode_does_not_narrow_pass1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode_var: str
) -> None:
    repo = _init_repo(tmp_path)
    _populate_status_fixture(repo)
    monkeypatch.setenv(mode_var, "1")

    assert guard.find_dirty_state_files(repo_root=str(repo), policy=_policy()) == EXPECTED_DIRTY


def test_empty_state_surfaces_skip_pass1_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _init_repo(tmp_path)
    original_run = guard._run_git_degraded
    calls: list[list[str]] = []

    def capture_run(args: list[str], **kwargs):  # noqa: ANN001, ANN003 - test wrapper
        calls.append(list(args))
        return original_run(args, **kwargs)

    monkeypatch.setattr(guard, "_run_git_degraded", capture_run)
    dirty = guard.find_dirty_state_files(repo_root=str(repo), policy=_policy(()))

    assert dirty == []
    assert not any("status" in args for args in calls), calls
