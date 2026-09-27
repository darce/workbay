"""The archived-plan guard must survive a branch it cannot determine.

``guard_archived_plan_checklist.py`` runs as a git ``pre-commit`` hook
(``scripts/hooks/git/pre-commit`` invokes it with ``|| exit $?``, and the
bootstrap installer points ``core.hooksPath`` at ``scripts/hooks/git``), so any
exception escaping ``main()`` becomes a non-zero pre-commit exit and git
refuses the commit.

The root copy calls ``subprocess.check_output(["git", "rev-parse",
"--abbrev-ref", "HEAD"])`` with no exception handling. On an unborn HEAD --
exactly the state during a repository's *first* commit -- git exits 128 and the
guard raises ``CalledProcessError``, deadlocking the repo: it can never make a
first commit. The payload twin wraps the call and returns ``""``, which
short-circuits because ``""`` is not a protected branch. That is the correct
direction, not a softening: a branch that cannot be determined also cannot be
known to be ``main``.

The blocking-path controls matter as much as the crash test. "Return 0
unconditionally" would satisfy the crash test alone while silently retiring the
guard, so the archived-plan refusal and the branch short-circuit are both
pinned here.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

HOOK_DIR = Path(__file__).resolve().parent
HOOK = HOOK_DIR / "guard_archived_plan_checklist.py"

_spec = importlib.util.spec_from_file_location(
    "_guard_archived_plan_checklist_ut", HOOK
)
assert _spec and _spec.loader
guard = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(guard)


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    )


def _fresh_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    return repo


def test_unborn_head_does_not_crash_the_guard(tmp_path: Path) -> None:
    """Direct call: an undeterminable branch is a pass, not an exception."""
    repo = _fresh_repo(tmp_path)
    (repo / "seed.md").write_text("seed\n", encoding="utf-8")
    _git("add", "seed.md", cwd=repo)

    proc = subprocess.run(
        [sys.executable, str(HOOK)], cwd=repo, capture_output=True, text=True
    )
    assert proc.returncode == 0, (
        "the guard failed on an unborn HEAD instead of passing; as a pre-commit "
        f"hook this refuses the repository's first commit. stderr={proc.stderr!r}"
    )


def test_unborn_head_does_not_deadlock_the_first_commit(tmp_path: Path) -> None:
    """End-to-end: the failure mode users actually hit.

    Installs the real ``pre-commit`` with ``core.hooksPath`` exactly as the
    bootstrap installer does, so ``GUARD_DIR`` resolves to the copied hook tree
    rather than to ``.git`` -- resolving it wrongly makes the ``[ -f ]`` test
    fail, the guard never runs, and the commit succeeds for a reason unrelated
    to the code under test.
    """
    repo = _fresh_repo(tmp_path)
    hooks = repo / "scripts" / "hooks"
    (hooks / "git").mkdir(parents=True)
    for src, dst in (
        (HOOK_DIR / "git" / "pre-commit", hooks / "git" / "pre-commit"),
        (HOOK, hooks / HOOK.name),
    ):
        dst.write_bytes(src.read_bytes())
        dst.chmod(0o755)
    _git("config", "core.hooksPath", "scripts/hooks/git", cwd=repo)
    _git("add", "-A", cwd=repo)

    proc = subprocess.run(
        ["git", "commit", "-m", "first commit"],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    committed = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"], cwd=repo, capture_output=True, text=True
    )
    assert proc.returncode == 0 and committed.stdout.strip() == "1", (
        "the pre-commit guard blocked the repository's first commit; the only "
        f"escape is --no-verify. exit={proc.returncode} stderr={proc.stderr!r}"
    )


def test_archived_plan_checklist_edit_still_blocks_on_main(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Control: the guard must still refuse the thing it exists to refuse.

    ``_is_archived_task`` shells out to the handoff CLI, which is not available
    hermetically, so it is stubbed. Without this control a fix that returns 0
    unconditionally would pass the crash tests while retiring the guard.
    """
    repo = _fresh_repo(tmp_path)
    rel = "packages/example/docs/tasks/internal-some-slug-task-plan.md"
    plan = repo / rel
    plan.parent.mkdir(parents=True)
    plan.write_text("# Plan\n\n- [ ] first item\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "seed", cwd=repo)

    plan.write_text("# Plan\n\n- [x] first item\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)

    monkeypatch.setattr(guard, "_is_archived_task", lambda _repo, _ref: True)
    violations = guard.scan_staged(repo, "main")
    assert violations == [rel], (
        f"the guard stopped detecting archived-plan checklist edits: {violations!r}"
    )

    monkeypatch.chdir(repo)
    assert guard.main([]) == 2, "a detected violation must exit 2"


@pytest.mark.parametrize(
    "rel",
    [
        "docs/tasks/internal-some-slug-task-plan.md",
        "docs/epics/internal-some-slug-task-plan.md",
    ],
)
def test_repo_root_plan_paths_are_recognised(rel: str) -> None:
    """The canonical location must match, not just nested consumer layouts.

    ``_TASK_PLAN_MARKERS`` are ``"/docs/tasks/"`` and ``"/docs/epics/"`` -- with
    a leading slash -- but ``git diff --cached --name-only`` emits repo-relative
    paths with no leading slash. A plan at the repo root therefore never
    matches, so the guard is inert at exactly the location this repository
    keeps its task plans; only nested ``packages/*/docs/tasks/`` paths fire.
    Both trees carry this, so it is not mirror drift.

    This is the second inertness defect in this file: the docstring on
    ``_task_ref_from_plan_path`` records an earlier one where a truncated task
    ref made the archive lookup never match.
    """
    assert guard._is_task_plan_path(rel), (
        f"{rel} is not recognised as a task plan, so the archived-plan guard "
        "never inspects it"
    )


def test_is_archived_task_false_on_ok_false_miss_envelope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BR-H4-01: root hook must match payload twin — miss envelope is not archived."""
    task_ref = "internal"
    miss = (
        '{"ok": false, "schema_version": 2, "tool": "archive", '
        f'"scope": {{"task_ref": "{task_ref}"}}, '
        f'"data": {{"error": "No archived task found for task_ref={task_ref}"}}}}'
    )

    def fake_run(cmd, *a, **k):
        return type("P", (), {"stdout": miss, "returncode": 0})()

    monkeypatch.setattr(guard.subprocess, "run", fake_run)
    assert guard._is_archived_task(tmp_path, task_ref) is False


def test_non_protected_branch_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Control: the block is branch-scoped, so feature work stays unaffected."""
    repo = _fresh_repo(tmp_path)
    plan_dir = repo / "docs" / "tasks"
    plan_dir.mkdir(parents=True)
    plan = plan_dir / "internal-some-slug-task-plan.md"
    plan.write_text("# Plan\n\n- [ ] first item\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "seed", cwd=repo)

    plan.write_text("# Plan\n\n- [x] first item\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)

    monkeypatch.setattr(guard, "_is_archived_task", lambda _repo, _ref: True)
    assert guard.scan_staged(repo, "feature/whatever") == []
