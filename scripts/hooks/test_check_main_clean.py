"""Unit tests for ``check_main_clean.py`` mode flags (internal).

This file is created by internal to pin the new explicit
``--mode warn|block|doctor`` flag and to keep ``--block`` compat alive.
implementation note expands the file with publish-boundary regression cases
(post-merge warn-only by default, pre-push hard block).

The tests run the script as a subprocess so they exercise the real
argparse surface and exit codes the hooks rely on.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "workbay-system"
    / "scripts"
    / "hooks"
    / "check_main_clean.py"
)
PACKAGE_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_SOURCE = PACKAGE_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"


def _seed_contract_from(repo: Path, source: Path) -> None:
    """Copy the harness-protocol contract into the fixture repo.

    ``check_main_clean.py`` resolves protected paths from
    ``docs/workbay/contracts/harness-protocol.yaml`` relative to the
    workspace root. Without it the script exits 2 with a
    ``HarnessContractMissingError`` regardless of mode.
    """
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _seed_contract(repo: Path) -> None:
    _seed_contract_from(repo, CONTRACT_SOURCE)


def _run(cwd: Path, *argv: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(
        [sys.executable, str(SCRIPT), *argv],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


@pytest.fixture
def feature_branch_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    (repo / "README.md").write_text("hello\n")
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "init",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "feature/internal-53-x"],
        check=True,
    )
    return repo


def test_mode_warn_short_circuits_off_protected_branch(feature_branch_repo: Path) -> None:
    """--mode=warn on a non-protected branch must exit 0 without complaint."""
    proc = _run(feature_branch_repo, "--mode", "warn")
    assert proc.returncode == 0, proc.stderr


def test_mode_block_short_circuits_off_protected_branch(feature_branch_repo: Path) -> None:
    """--mode=block on a non-protected branch must still exit 0."""
    proc = _run(feature_branch_repo, "--mode", "block")
    assert proc.returncode == 0, proc.stderr


def test_mode_doctor_short_circuits_off_protected_branch(feature_branch_repo: Path) -> None:
    """--mode=doctor on a non-protected branch must exit 0."""
    proc = _run(feature_branch_repo, "--mode", "doctor")
    assert proc.returncode == 0, proc.stderr


def test_block_flag_still_works_after_mode_introduction(feature_branch_repo: Path) -> None:
    """The legacy ``--block`` flag must keep its prior semantics for hook scripts."""
    proc = _run(feature_branch_repo, "--block")
    assert proc.returncode == 0, proc.stderr


def test_unknown_mode_value_rejected_at_parse_time(feature_branch_repo: Path) -> None:
    """Unknown --mode values must be rejected before any git work runs."""
    proc = _run(feature_branch_repo, "--mode", "yolo")
    assert proc.returncode != 0
    assert "yolo" in proc.stderr or "mode" in proc.stderr


def test_block_and_mode_block_are_equivalent(feature_branch_repo: Path) -> None:
    """``--block`` must continue to behave like ``--mode block`` for hook compat."""
    block_proc = _run(feature_branch_repo, "--block")
    mode_proc = _run(feature_branch_repo, "--mode", "block")
    assert block_proc.returncode == mode_proc.returncode


# internal: publish-boundary tuning. The git hooks run
# ``check_main_clean.py`` on real ``main`` checkouts; the regression
# we guard here is that a routine post-merge with no dirty protected
# paths must exit 0 in any mode, including ``warn`` and ``doctor``.


@pytest.fixture
def clean_main_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "main-repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    _seed_contract(repo)
    (repo / "README.md").write_text("hello\n")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "init",
        ],
        check=True,
    )
    return repo


def test_clean_main_warn_mode_exits_zero(clean_main_repo: Path) -> None:
    proc = _run(clean_main_repo, "--mode", "warn", "--trigger", "post-merge")
    assert proc.returncode == 0, proc.stderr


def test_clean_main_block_mode_exits_zero(clean_main_repo: Path) -> None:
    proc = _run(clean_main_repo, "--mode", "block", "--trigger", "pre-push")
    assert proc.returncode == 0, proc.stderr


def test_clean_main_doctor_mode_exits_zero(clean_main_repo: Path) -> None:
    proc = _run(clean_main_repo, "--mode", "doctor", "--trigger", "manual")
    assert proc.returncode == 0, proc.stderr


def test_package_script_prefers_local_hook_helpers_over_repo_mirror(clean_main_repo: Path) -> None:
    """A stale repo-local ``scripts/hooks`` mirror must not shadow the helper
    module shipped next to the script under test.

    internal added ``find_dirty_state_files`` to the package-local helper. The
    actual script previously prepended ``<repo>/scripts/hooks`` to
    ``sys.path``, so a stale mirror disabled the guard with an import failure.
    """
    stale_mirror = clean_main_repo / "scripts" / "hooks"
    stale_mirror.mkdir(parents=True)
    (stale_mirror / "_branch_isolation_guard.py").write_text("# stale mirror without helper\n", encoding="utf-8")

    proc = _run(clean_main_repo, "--mode", "block", "--trigger", "manual")

    assert proc.returncode == 0, proc.stderr
    assert "import failed" not in proc.stderr


def _hook_invocations(hook_path: Path) -> list[str]:
    """Return only the executable lines of the hook (skipping comments)."""
    return [
        line
        for line in hook_path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


# ---------------------------------------------------------------------------
# internal: ``check-main-clean`` tripwires only on state-file dirt.
#
# The post-merge over-fire happened because ``find_dirty_protected_paths``
# matched ANY ``protected_main_surfaces`` entry — including planning
# artefacts (``docs/scopes/**``, ``docs/tasks/**`` and their package
# mirrors) that arrive legitimately via a clean fast-forward. After Slice
# 2 the post-merge gate consumes ``state_dirty_surfaces`` only, via the
# new ``find_dirty_state_files()`` helper. Gitignored state-file dirt
# (``.task-state/handoff.db``) must still hard-block under ``--mode
# block`` because the helper combines git status with a direct
# filesystem walk over canonical state paths.
# ---------------------------------------------------------------------------


def _commit_all(repo: Path, msg: str) -> None:
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            msg,
        ],
        check=True,
    )


def test_planning_artifact_dirt_on_main_does_not_block_pre_push(clean_main_repo: Path) -> None:
    """Routine post-merge with a fresh planning artefact in the working
    tree must exit 0 even on the hard-block surface (pre-push). The
    internal close-out friction was operators being told to stash and
    branch a clean fast-forward that legitimately introduced new
    ``docs/scopes/**`` or ``docs/tasks/**`` files.
    """
    repo = clean_main_repo
    scope_dir = repo / "docs" / "scopes"
    scope_dir.mkdir(parents=True)
    (scope_dir / "wip-scope.md").write_text("planning draft\n", encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")
    assert proc.returncode == 0, (
        "planning artefact dirt on main must no longer trip check-main-clean; "
        f"stderr=\n{proc.stderr}"
    )


def test_dirty_top_level_state_file_still_blocks_pre_push(clean_main_repo: Path) -> None:
    """An untracked ``CURRENT_TASK.json`` at the repo root is a real
    state-file regression and must still hard-block under ``--mode block``.
    """
    repo = clean_main_repo
    (repo / "CURRENT_TASK.json").write_text("{}\n", encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")
    assert proc.returncode == 2, (
        "state-file dirt on main must still block; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )
    assert "CURRENT_TASK.json" in proc.stderr


def test_ignored_local_handoff_state_does_not_block_pre_push(clean_main_repo: Path) -> None:
    """Ignored local handoff projections are by-design per-checkout state.

    The repo ignores these files because they are generated from the handoff
    DB on demand. Their existence alone must not make the main control-plane
    checkout unpushable.
    """
    repo = clean_main_repo
    (repo / ".gitignore").write_text(
        ".task-state/\n/CURRENT_TASK.json\n/DASHBOARD.txt\n",
        encoding="utf-8",
    )
    _commit_all(repo, "ignore state")
    state_dir = repo / ".task-state"
    state_dir.mkdir()
    (state_dir / "handoff.db").write_text("sqlite-bytes", encoding="utf-8")
    (repo / "CURRENT_TASK.json").write_text("{}\n", encoding="utf-8")
    (repo / "DASHBOARD.txt").write_text("dashboard\n", encoding="utf-8")

    # Sanity: plain porcelain output is empty, proving the regression
    # would have been invisible to a tracked-file-only helper.
    porcelain = subprocess.run(
        ["git", "-C", str(repo), "status", "--porcelain=v1"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert porcelain == "", f"gitignored file should be invisible to plain porcelain: {porcelain!r}"

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")

    assert proc.returncode == 0, (
        "ignored generated handoff projections must not hard-block normal main control-plane use; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )


def test_tracked_archive_state_file_still_blocks_pre_push(clean_main_repo: Path) -> None:
    """Tracked archived task snapshots remain real main-clean state dirt."""
    repo = clean_main_repo
    archive = repo / "docs" / "tasks" / "archive" / "TASK-1.json"
    archive.parent.mkdir(parents=True)
    archive.write_text('{"status":"done"}\n', encoding="utf-8")
    _commit_all(repo, "track archived task snapshot")
    archive.write_text('{"status":"done","updated":true}\n', encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")

    assert proc.returncode == 2, (
        "tracked archived state dirt on main must still hard-block; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )
    assert "docs/tasks/archive/TASK-1.json" in proc.stderr


def test_gitignored_task_state_spools_do_not_block_pre_push(clean_main_repo: Path) -> None:
    """Local handoff spools and dashboard fragments are ignored runtime noise,
    not the canonical state files internal's hard block protects.
    """
    repo = clean_main_repo
    (repo / ".gitignore").write_text(".task-state/\n", encoding="utf-8")
    _commit_all(repo, "ignore state")
    dashboard_fragments = repo / ".task-state" / "DASHBOARD.d"
    dashboard_fragments.mkdir(parents=True)
    (dashboard_fragments / "fragment.md").write_text("local projection\n", encoding="utf-8")
    (repo / ".task-state" / "terminal_guard.jsonl").write_text("{}\n", encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")

    assert proc.returncode == 0, (
        "ignored non-canonical .task-state spools must not make check-main-clean unusable; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )


def test_nested_task_state_db_does_not_match_root_state_surface(clean_main_repo: Path) -> None:
    """State-file surfaces are repo-relative, not suffix matches inside
    arbitrary fixture or prompt directories.
    """
    repo = clean_main_repo
    (repo / ".gitignore").write_text(".task-state/\n", encoding="utf-8")
    _commit_all(repo, "ignore state")
    nested_state = repo / "base prompt" / ".task-state"
    nested_state.mkdir(parents=True)
    (nested_state / "handoff.db").write_text("nested fixture db\n", encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")

    assert proc.returncode == 0, (
        "root .task-state/handoff.db* must not match nested fixture paths; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )


def test_stale_root_contract_without_state_surface_uses_package_fallback(clean_main_repo: Path) -> None:
    """A stale root overlay must not silently disable internal's state-file
    tripwire in a source checkout.
    """
    repo = clean_main_repo
    stale_contract = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    stale_contract.write_text(
        """version: 1

branch_isolation:
  protected_branches:
    - main
  code_roots:
    - packages/
  protected_extensions:
    - .py
  root_protected_files:
    - Makefile
  protected_main_surfaces:
    - pattern: "docs/tasks/**/*.md"
      reason: "Task plans must live on a task branch"
  permitted_main_surfaces:
    - pattern: "DASHBOARD.txt"
      reason: "Dashboard artifact is regenerated on main"
""",
        encoding="utf-8",
    )
    _commit_all(repo, "stale root contract")

    archive = repo / "docs" / "tasks" / "archive" / "TASK-1.json"
    archive.parent.mkdir(parents=True)
    archive.write_text('{"status":"done"}\n', encoding="utf-8")
    _commit_all(repo, "track archive")
    archive.write_text('{"status":"done","updated":true}\n', encoding="utf-8")

    proc = _run(repo, "--mode", "block", "--trigger", "pre-push")

    assert proc.returncode == 2, proc.stderr
    assert "docs/tasks/archive/TASK-1.json" in proc.stderr


def test_post_merge_hook_no_longer_uses_hard_block_flag() -> None:
    """The post-merge git hook must not pass ``--block`` anymore.

    internal retunes the publish boundary so routine post-merge
    fires warn-only. ``pre-push`` still hard-blocks (asserted in a
    sibling test). This guards against a future refactor reintroducing
    ``--block`` in the post-merge wrapper.
    """
    post_merge = SCRIPT.parent / "git" / "post-merge"
    invocations = "\n".join(_hook_invocations(post_merge))
    assert "--block" not in invocations, (
        "internal: post-merge git hook must not pass --block "
        f"to check_main_clean.py:\n{invocations}"
    )
    assert "--mode" in invocations, (
        "internal: post-merge git hook should opt into the "
        f"explicit --mode flag for the warn-only contract:\n{invocations}"
    )


def test_pre_push_hook_keeps_hard_block_flag() -> None:
    """The pre-push hook must still hard-block on dirty protected paths."""
    pre_push = SCRIPT.parent / "git" / "pre-push"
    invocations = "\n".join(_hook_invocations(pre_push))
    assert "--block" in invocations or "--mode block" in invocations, (
        "internal: pre-push git hook must still hard-block "
        f"protected dirty paths on main:\n{invocations}"
    )
