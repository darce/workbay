"""internal: regression coverage for the Bash main-branch surface.

The implementation note goal is a contract-pinning slice with no behaviour edit to
``guard-bash-main-branch.py`` itself. We prove three claims by driving
the hook as a subprocess against a tmp repo that mirrors the production
contract:

- **Plain branch creation is allowed.** ``git checkout -b
  feature/internal-03-smoke`` from a main worktree must exit 0 — the
  surface 4 friction in the parent scope was operators expecting this
  path to be blocked. The Bash scanner does not write to a protected
  path here, so it must pass through.
- **Bash writes to protected code paths still block.** A ``sed -i`` or
  redirect that targets a ``packages/**/*.py`` file is still a real
  bypass of the file-mutation PreToolUse hook, and must continue to
  exit 2.
- **The legacy planning-artefact surface still blocks Bash writes.**
  ``protected_main_surfaces`` was retained in the contract during the
  internal transition for back-compat with this scanner; an ``echo > ``
  to ``docs/scopes/foo.md`` must still trip the hook so first-edit
  isolation does not regress via the Bash bypass.

State-file Bash writes (``echo > CURRENT_TASK.json``,
``rm .task-state/handoff.db``) are intentionally NOT asserted here:
their enforcement surface is the post-merge ``check-main-clean``
tripwire (implementation note). Extending the Bash scanner to also block state
surfaces is a follow-up scope edit, recorded in the slice-close
rationale.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


HOOK_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "workbay-system"
    / "scripts"
    / "hooks"
    / "guard-bash-main-branch.py"
)
PACKAGE_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_SOURCE = PACKAGE_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"


def _seed_contract(repo: Path) -> None:
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(CONTRACT_SOURCE, target)


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def main_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _seed_contract(repo)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "init", cwd=repo)
    return repo


def _invoke(repo: Path, command: str) -> subprocess.CompletedProcess[str]:
    payload = {"toolName": "Bash", "toolInput": {"command": command}}
    env = os.environ.copy()
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=repo,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


# ---------------------------------------------------------------------------
# Pass-through: plain branch creation from main must not block.
# ---------------------------------------------------------------------------


def test_plain_branch_creation_allowed_on_main(main_repo: Path) -> None:
    proc = _invoke(main_repo, "git checkout -b feature/internal-03-smoke")
    assert proc.returncode == 0, (
        "plain `git checkout -b feature/<task>` from main must not be blocked; "
        f"stderr=\n{proc.stderr}"
    )
    assert "BLOCKED" not in proc.stderr


def test_maint_branch_creation_allowed_on_main(main_repo: Path) -> None:
    proc = _invoke(main_repo, "git checkout -b MAINT-cleanup-2026-05-23")
    assert proc.returncode == 0, proc.stderr
    assert "BLOCKED" not in proc.stderr


def test_branch_creation_with_set_upstream_allowed(main_repo: Path) -> None:
    proc = _invoke(main_repo, "git checkout -b feature/internal-03-smoke && git push -u origin HEAD")
    assert proc.returncode == 0, proc.stderr


# ---------------------------------------------------------------------------
# Hard-block regression: Bash writes to protected code/planning surfaces.
# ---------------------------------------------------------------------------


def test_sed_in_place_on_code_file_still_blocked(main_repo: Path) -> None:
    proc = _invoke(main_repo, "sed -i 's/x/y/' packages/foo/bar.py")
    assert proc.returncode == 2, (
        "sed -i on a packages/**/*.py path on main must still hard-block; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )
    assert "BLOCKED" in proc.stderr
    assert "packages/foo/bar.py" in proc.stderr


def test_redirect_to_planning_artifact_still_blocked(main_repo: Path) -> None:
    proc = _invoke(main_repo, 'echo "draft" > docs/scopes/new-scope.md')
    assert proc.returncode == 2, (
        "Bash redirect to a planning artefact on main must still hard-block via "
        "the legacy `protected_main_surfaces` surface; "
        f"stdout=\n{proc.stdout}\nstderr=\n{proc.stderr}"
    )
    assert "BLOCKED" in proc.stderr
    assert "docs/scopes/new-scope.md" in proc.stderr


def test_rm_on_protected_code_file_still_blocked(main_repo: Path) -> None:
    proc = _invoke(main_repo, "rm packages/foo/bar.py")
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr


# ---------------------------------------------------------------------------
# Non-protected commands stay non-blocking.
# ---------------------------------------------------------------------------


def test_read_only_command_not_blocked(main_repo: Path) -> None:
    proc = _invoke(main_repo, "git status --porcelain")
    assert proc.returncode == 0, proc.stderr


def test_pytest_invocation_not_blocked(main_repo: Path) -> None:
    proc = _invoke(main_repo, "pytest packages/foo/tests -q")
    assert proc.returncode == 0, proc.stderr
