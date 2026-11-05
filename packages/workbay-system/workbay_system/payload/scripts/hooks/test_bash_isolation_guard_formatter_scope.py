"""Formatter mutability and worktree-scope regressions for the Bash guard."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _bash_isolation_guard import scan_bash_command  # noqa: E402
from _harness_protocol import BranchIsolationPolicy  # noqa: E402


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _policy() -> BranchIsolationPolicy:
    return BranchIsolationPolicy(
        code_roots=("apps/", "packages/", "scripts/"),
        protected_extensions=(".py", ".js", ".sh"),
        root_protected_files=("Makefile",),
        protected_main_surfaces=(),
        permitted_main_surfaces=(),
    )


@pytest.fixture()
def repo_pair(tmp_path: Path) -> tuple[Path, Path]:
    primary = tmp_path / "primary"
    primary.mkdir()
    _git(primary, "init", "-b", "main")
    _git(primary, "config", "user.email", "t@example.invalid")
    _git(primary, "config", "user.name", "t")
    (primary / "Makefile").write_text("all:\n\ttrue\n")
    package = primary / "packages" / "pkg"
    package.mkdir(parents=True)
    (package / "mod.py").write_text("x = 1\n")
    _git(primary, "add", "-A")
    _git(primary, "commit", "-m", "init")
    worktree = tmp_path / "feature-worktree"
    _git(primary, "worktree", "add", "-b", "feature/task-1", str(worktree))
    return primary, worktree


def _formatter_labels(command: str, primary: Path) -> list[str]:
    return [
        item
        for item in scan_bash_command(command, primary, _policy())
        if item.endswith("(formatter)")
    ]


@pytest.mark.parametrize(
    "command",
    (
        "black --check packages/",
        "black --diff packages/",
        "black --dry-run packages/",
        "black --help",
        "black --version",
        "prettier --check apps/",
        "prettier --list-different apps/",
        "ruff format --check packages/",
        "ruff format --diff packages/",
        "ruff format --help",
    ),
)
def test_read_only_formatter_modes_are_not_blocked(
    repo_pair: tuple[Path, Path], command: str
) -> None:
    primary, _worktree = repo_pair
    assert _formatter_labels(command, primary) == []


@pytest.mark.parametrize(
    "command",
    (
        "ruff format packages/",
        "black packages/",
        "black -l 88 packages/",
        "prettier --write apps/",
        "prettier -w apps/",
        "prettier --check --write apps/",
    ),
)
def test_writing_formatter_modes_on_main_are_blocked(
    repo_pair: tuple[Path, Path], command: str
) -> None:
    primary, _worktree = repo_pair
    assert _formatter_labels(command, primary)


def test_formatter_in_feature_worktree_is_allowed(repo_pair: tuple[Path, Path]) -> None:
    primary, worktree = repo_pair
    assert _formatter_labels(f"cd {worktree} && ruff format packages/", primary) == []


def test_semicolon_propagates_feature_worktree(repo_pair: tuple[Path, Path]) -> None:
    primary, worktree = repo_pair
    assert _formatter_labels(f"cd {worktree}; black packages/", primary) == []


def test_pipe_joiner_fails_closed(repo_pair: tuple[Path, Path]) -> None:
    primary, worktree = repo_pair
    assert _formatter_labels(f"cd {worktree} | ruff format packages/", primary)


def test_or_joiner_fails_closed(repo_pair: tuple[Path, Path]) -> None:
    primary, worktree = repo_pair
    assert _formatter_labels(f"cd {worktree} || ruff format packages/", primary)


def test_background_joiner_fails_closed(repo_pair: tuple[Path, Path]) -> None:
    primary, worktree = repo_pair
    assert _formatter_labels(f"cd {worktree} & ruff format packages/", primary)


def test_unresolvable_cd_fails_closed(repo_pair: tuple[Path, Path]) -> None:
    primary, _worktree = repo_pair
    assert _formatter_labels('cd "$UNKNOWN_WORKTREE" && ruff format packages/', primary)


def test_non_git_cwd_fails_closed(repo_pair: tuple[Path, Path], tmp_path: Path) -> None:
    primary, _worktree = repo_pair
    outside = tmp_path / "not-a-repository"
    outside.mkdir()
    assert _formatter_labels(f"cd {outside} && ruff format packages/", primary)


def test_all_formatter_stages_in_feature_worktree_are_allowed(
    repo_pair: tuple[Path, Path],
) -> None:
    primary, worktree = repo_pair
    command = f"cd {worktree} && ruff format packages/ && black packages/"
    assert _formatter_labels(command, primary) == []


def test_one_main_formatter_stage_makes_chain_block(
    repo_pair: tuple[Path, Path],
) -> None:
    primary, worktree = repo_pair
    command = f"ruff format packages/ && cd {worktree} && black packages/"
    assert _formatter_labels(command, primary)
