"""internal: repo-root resolution regression coverage."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

HOOKS = Path(__file__).parent
RESOLVE_SNIPPET = HOOKS / "_resolve_repo_root.sh"
DRIFT_HOOK = HOOKS / "guard-worktree-drift.sh"
REGEN_HOOK = HOOKS / "regenerate-task-views.sh"
GUARD_PY = HOOKS / "guard-task-plan-findings.py"


def _source_repo_root(env: dict[str, str], cwd: str) -> str:
    proc = subprocess.run(
        ["bash", "-c", f'. "{RESOLVE_SNIPPET}"; printf "%s" "$REPO_ROOT"'],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


def test_resolve_snippet_uses_git_toplevel_in_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)
    assert _source_repo_root(env, str(repo)) == str(repo.resolve())


def test_resolve_snippet_falls_back_to_grok_without_git(tmp_path: Path) -> None:
    ws = tmp_path / "grok-ws"
    ws.mkdir()
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env["GROK_WORKSPACE_ROOT"] = str(ws)
    assert _source_repo_root(env, str(tmp_path)) == str(ws)


def test_guard_worktree_drift_fail_open_from_non_git_cwd(tmp_path: Path) -> None:
    ws = tmp_path / "grok-ws"
    (ws / "scripts" / "hooks").mkdir(parents=True)
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env["GROK_WORKSPACE_ROOT"] = str(ws)
    payload = json.dumps({"tool_input": {"file_path": str(ws / "README.md")}})
    proc = subprocess.run(
        ["bash", str(DRIFT_HOOK)],
        cwd=str(tmp_path),
        env=env,
        input=payload,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr


def test_regenerate_task_views_delegates_workspace_root_to_snippet() -> None:
    text = REGEN_HOOK.read_text()
    assert "_resolve_repo_root.sh" in text
    assert 'WORKSPACE_ROOT="$REPO_ROOT"' in text


def test_harness_workspace_hooks_use_shared_python_resolver() -> None:
    guard_text = GUARD_PY.read_text()
    assert "resolve_harness_workspace_root" in guard_text
    interp_text = (HOOKS / "_interp.py").read_text()
    mcp_text = (HOOKS / "mcp_launch.py").read_text()
    assert "resolve_harness_workspace_root" not in interp_text
    assert "resolve_harness_workspace_root" not in mcp_text
