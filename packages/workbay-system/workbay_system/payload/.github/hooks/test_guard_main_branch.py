"""Tests for the VS Code PreToolUse branch-isolation hook."""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


HOOK_SCRIPT = Path(__file__).parent / "guard-main-branch.py"

_spec = importlib.util.spec_from_file_location("guard_main_branch", HOOK_SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

_check_file_edit = _mod._check_file_edit
_extract_candidate_paths = _mod._extract_candidate_paths
_check_branch_naming = _mod._check_branch_naming
HELPER_DIR = HOOK_SCRIPT.parents[2] / "scripts" / "hooks"
sys.path.insert(0, str(HELPER_DIR))

from _harness_protocol import BranchIsolationPolicy  # noqa: E402
import _branch_isolation_guard  # noqa: E402
from workbay_handoff_mcp import TASK_REF_RE as _CANONICAL_TASK_REF_RE  # noqa: E402


POLICY = BranchIsolationPolicy(
    code_roots=("apps/", "packages/", "scripts/", ".github/hooks/", ".claude/", "mk/"),
    protected_extensions=(".py", ".ts", ".tsx", ".js", ".jsx", ".php", ".sql", ".sh", ".css", ".scss", ".mk"),
    root_protected_files=("Makefile",),
    protected_main_surfaces=(),
    permitted_main_surfaces=(),
)
FIXTURE_COPY_PATHS = (
    Path(".github/hooks/guard-main-branch.py"),
    Path("scripts/hooks/_branch_isolation_guard.py"),
    Path("scripts/hooks/_harness_protocol.py"),
)


def _run_hook(
    payload: dict,
    cwd: str | None = None,
    env: dict | None = None,
) -> tuple[int, dict | None]:
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=15,
        cwd=cwd,
        env=env,
    )
    stdout_json = None
    if proc.stdout.strip():
        stdout_json = json.loads(proc.stdout)
    return proc.returncode, stdout_json


def _write_fixture_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    for relative in FIXTURE_COPY_PATHS:
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(HOOK_SCRIPT.parents[2] / relative, destination)

    contract_path = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(
        """version: 1

branch_isolation:
  protected_branches:
    - main
    - master
  code_roots:
    - apps/
    - packages/
    - scripts/
  protected_extensions:
    - .py
    - .sh
    - .ts
  root_protected_files:
    - Makefile
  permitted_main_surfaces:
    - pattern: "docs/tasks/**/*.md"
      reason: "Task plans"
    - pattern: "docs/workbay/rules/**"
      reason: "Workflow rules"
  enforcers:
    - path: .github/hooks/guard-main-branch.py
      harness: vscode
    - path: scripts/hooks/guard-main-branch.sh
      harness: claude
""",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=repo, check=True)
    return repo


def test_extract_candidate_paths_from_apply_patch() -> None:
    patch = """*** Begin Patch
*** Update File: /repo/apps/web/api/main.py
@@
-old
+new
*** Add File: /repo/docs/notes.md
+hello
*** End Patch"""
    result = _extract_candidate_paths("apply_patch", {"input": patch})
    assert result == [
        "/repo/apps/web/api/main.py",
        "/repo/docs/notes.md",
    ]


def test_check_file_edit_blocks_code_paths_on_main() -> None:
    result = _check_file_edit(
        "create_file",
        {"filePath": "/repo/apps/web/api/main.py"},
        branch="main",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result == ("main", ["apps/web/api/main.py"])


def test_check_file_edit_allows_docs_on_main() -> None:
    result = _check_file_edit(
        "create_file",
        {"filePath": "/repo/docs/workbay/rules/development-workflow.md"},
        branch="main",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result is None


def test_check_file_edit_allows_code_on_feature_branch() -> None:
    patch = """*** Begin Patch
*** Update File: /repo/packages/workbay-orchestrator-mcp/src/workbay_orchestrator_mcp/api.py
@@
-old
+new
*** End Patch"""
    result = _check_file_edit(
        "apply_patch",
        {"input": patch},
        branch="feature/e15-branch-guard",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result is None


def test_check_file_edit_blocks_mixed_patch_when_code_file_present() -> None:
    patch = """*** Begin Patch
*** Update File: /repo/docs/workbay/rules/development-workflow.md
@@
-old
+new
*** Update File: /repo/packages/workbay-orchestrator-mcp/src/workbay_orchestrator_mcp/api.py
@@
-old
+new
*** End Patch"""
    result = _check_file_edit(
        "apply_patch",
        {"input": patch},
        branch="main",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result == ("main", ["packages/workbay-orchestrator-mcp/src/workbay_orchestrator_mcp/api.py"])


def test_check_file_edit_blocks_root_makefile_on_main() -> None:
    result = _check_file_edit(
        "replace_string_in_file",
        {"filePath": "/repo/Makefile"},
        branch="main",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result == ("main", ["Makefile"])


def test_check_file_edit_blocks_scripts_path_on_main() -> None:
    result = _check_file_edit(
        "multi_replace_string_in_file",
        {"file_path": "/repo/scripts/check_skills.py"},
        branch="main",
        repo_root="/repo",
        policy=POLICY,
        protected_branches={"main", "master"},
    )
    assert result == ("main", ["scripts/check_skills.py"])


def test_hook_emits_block_json_for_create_file_on_main(tmp_path: Path) -> None:
    repo = _write_fixture_repo(tmp_path)
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "apps" / "web" / "api" / "main.py")},
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is not None
    assert output["hookSpecificOutput"]["permissionDecision"] == "block"


def test_hook_allows_doc_create_on_clean_main(tmp_path: Path) -> None:
    repo = _write_fixture_repo(tmp_path)
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "docs" / "workbay" / "rules" / "development-workflow.md")},
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is None


def test_hook_allows_allowed_doc_edit_when_unrelated_protected_paths_are_dirty(tmp_path: Path) -> None:
    repo = _write_fixture_repo(tmp_path)
    protected_path = repo / "packages" / "demo" / "worker.py"
    protected_path.parent.mkdir(parents=True, exist_ok=True)
    protected_path.write_text("print('dirty main')\n", encoding="utf-8")

    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "docs" / "workbay" / "rules" / "development-workflow.md")},
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is None


# ---------------------------------------------------------------------------
# internal — branch-naming validator
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "branch",
    [
        "main",
        "master",
        "release/v1",
        "release/2026-q1",
        "hotfix/foo",
        "feature/internal-37",
        "feature/internal-37-branch-naming-enforcement",
        "feature/maint-dirty-br-01",
        "feature/plan-0006",
    ],
)
def test_check_branch_naming_accepts_protected_and_conforming(branch: str) -> None:
    assert _check_branch_naming(branch) is None


@pytest.mark.parametrize(
    "branch",
    [
        "feature/bad",  # single-segment
        "feature/bad-name",  # no digit
        "feature/internal",  # uppercase rejected
        "feature/badName",  # camelCase rejected
        "feature/-x",  # leading hyphen
        "fix/foo",  # other prefix, no digit
        "chore/bar",  # other prefix
        "wip-thing",  # bare name
        "random-branch",
    ],
)
def test_check_branch_naming_rejects_non_conforming(branch: str) -> None:
    assert _check_branch_naming(branch) == branch


@pytest.mark.parametrize("branch", ["", None])
def test_check_branch_naming_handles_empty_branch(branch) -> None:
    """Detached HEAD / unknown branch must not wedge the gate. The
    pre-commit / pre-push gates own the branchless-checkout concern via
    their own carve-outs; PreToolUse cannot afford to block here."""
    assert _check_branch_naming(branch) is None


def test_branch_naming_uses_canonical_regex() -> None:
    """The validator must consume the same compiled regex object exposed
    by ``workbay_protocol`` (via ``workbay_handoff_mcp`` re-export) so that
    a grammar tweak in one module updates every gate. A second compiled
    pattern would let drift creep in silently."""
    assert _branch_isolation_guard.TASK_REF_RE is _CANONICAL_TASK_REF_RE


def _checkout_branch(repo: Path, branch: str) -> None:
    subprocess.run(["git", "checkout", "-q", "-b", branch], cwd=repo, check=True)


def test_hook_blocks_on_non_conforming_branch(tmp_path: Path) -> None:
    repo = _write_fixture_repo(tmp_path)
    _checkout_branch(repo, "feature/bad-name")
    payload = {
        "toolName": "create_file",
        "toolInput": {
            "filePath": str(repo / "apps" / "demo" / "main.py"),
        },
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is not None
    decision = output["hookSpecificOutput"]
    assert decision["permissionDecision"] == "block"
    reason = decision["permissionDecisionReason"]
    assert "Branch name does not match" in reason
    assert "feature/bad-name" in reason
    assert "workbay_protocol.branch_naming.TASK_REF_RE" in reason
    assert "WORKBAY_ALLOW_NONCONFORMING_BRANCH" in reason


def test_hook_blocks_on_other_prefix(tmp_path: Path) -> None:
    """``fix/<x>``, ``chore/<x>``, ``wip-<x>`` are non-conforming too —
    implementation note r3 widened the rejection class beyond ``feature/<bad>``."""
    repo = _write_fixture_repo(tmp_path)
    _checkout_branch(repo, "fix/foo")
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "apps" / "demo" / "main.py")},
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is not None
    assert output["hookSpecificOutput"]["permissionDecision"] == "block"
    assert "fix/foo" in output["hookSpecificOutput"]["permissionDecisionReason"]


def test_hook_allows_conforming_feature_branch(tmp_path: Path) -> None:
    repo = _write_fixture_repo(tmp_path)
    _checkout_branch(repo, "feature/internal-37-foo")
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "apps" / "demo" / "main.py")},
    }
    exit_code, output = _run_hook(payload, cwd=str(repo))
    assert exit_code == 0
    assert output is None


def test_hook_allows_with_override_env_var(tmp_path: Path) -> None:
    """``WORKBAY_ALLOW_NONCONFORMING_BRANCH=1`` is the documented escape
    valve for PreToolUse. Pre-commit / pre-push read separate env vars
    (implementation note §4 / §4b) so this leniency does not silently leak."""
    repo = _write_fixture_repo(tmp_path)
    _checkout_branch(repo, "feature/bad-name")
    payload = {
        "toolName": "create_file",
        "toolInput": {"filePath": str(repo / "apps" / "demo" / "main.py")},
    }
    env = {**os.environ, "WORKBAY_ALLOW_NONCONFORMING_BRANCH": "1"}
    exit_code, output = _run_hook(payload, cwd=str(repo), env=env)
    assert exit_code == 0
    # Override bypasses naming check; subsequent protected-path check
    # is what governs (this branch is not protected, so the edit is
    # allowed).
    assert output is None
