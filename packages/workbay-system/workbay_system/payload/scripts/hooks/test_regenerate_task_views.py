from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK_SCRIPT = REPO_ROOT / "scripts" / "hooks" / "regenerate-task-views.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("git") is None,
    reason="bash and git required to drive the hook end-to-end",
)


def _init_git_repo(path: Path) -> None:
    """A throwaway git repo so the hook + shim resolve their root here.

    Both ``_resolve_repo_root.sh`` (the hook) and ``mcp_launch.py`` (the shim)
    resolve the workspace via ``git rev-parse`` in the process cwd. Running the
    hook with ``cwd`` inside a temp repo — one with no ``.venv`` — makes the
    shim's ``.venv`` console probe miss, so the ``UV_TOOL_BIN_DIR`` fake below is
    what it execs.
    """
    subprocess.run(["git", "init", "-q"], cwd=str(path), check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "t@t"], cwd=str(path), check=True, capture_output=True
    )
    subprocess.run(
        ["git", "config", "user.name", "t"], cwd=str(path), check=True, capture_output=True
    )


def _fake_console(bin_dir: Path) -> None:
    """Shadow the git-installed ``mcp-workbay-handoff`` console the shim execs.

    Git-only delivery launches the dashboard refresh through ``mcp_launch.py``,
    which resolves the console from the workspace ``.venv`` (absent in the temp
    repo) then the ``uv tool`` bin dir. Pointing ``UV_TOOL_BIN_DIR`` at this stub
    proves the shim path is taken — never a per-session ``uvx`` PyPI resolve —
    and logs the forwarded argv so the launch is observable.
    """
    bin_dir.mkdir(parents=True, exist_ok=True)
    console = bin_dir / "mcp-workbay-handoff"
    console.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$*" >> "$TMP_HOOK_LOG"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    console.chmod(console.stat().st_mode | stat.S_IEXEC)


def _run_hook(tmp_path: Path, payload: dict) -> tuple[int, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _init_git_repo(repo)
    bin_dir = tmp_path / "toolbin"
    log_path = tmp_path / "calls.log"
    _fake_console(bin_dir)
    env = os.environ.copy()
    env["UV_TOOL_BIN_DIR"] = str(bin_dir)
    env["TMP_HOOK_LOG"] = str(log_path)
    # Drop ambient overrides that would pull root/console resolution back to the
    # real monorepo instead of the temp repo + fake console.
    for var in ("XDG_BIN_HOME", "XDG_DATA_HOME", "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT"):
        env.pop(var, None)
    proc = subprocess.run(
        ["bash", str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=str(repo),
        env=env,
        timeout=30,
    )
    calls = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
    return proc.returncode, calls


def test_review_findings_list_string_payload_is_skipped(tmp_path: Path) -> None:
    code, calls = _run_hook(
        tmp_path,
        {
            "tool_name": "mcp__workbay_mcp__review_findings",
            "tool_input": {"review": '{"operation":"list"}'},
        },
    )
    assert code == 0
    assert calls == ""


def test_review_findings_record_string_payload_triggers_refresh(tmp_path: Path) -> None:
    code, calls = _run_hook(
        tmp_path,
        {
            "tool_name": "mcp__workbay_mcp__review_findings",
            "tool_input": {"review": '{"operation":"record"}'},
        },
    )
    assert code == 0
    assert "--workspace-root" in calls
    assert "render-handoff" in calls
    assert "--kind dashboard" in calls


def test_record_event_triggers_shim_refresh(tmp_path: Path) -> None:
    """Drift guard: a triggering tool routes the refresh through the shim-execed
    console (`render-handoff --kind dashboard`), not a per-session `uvx` launch.
    """
    code, calls = _run_hook(
        tmp_path,
        {"tool_name": "mcp__workbay_mcp__record_event", "tool_input": {}},
    )
    assert code == 0
    launched = calls.strip()
    assert launched, "expected the hook to launch the shim-resolved console"
    assert "render-handoff" in launched
    assert "--kind dashboard" in launched


def test_hook_launches_via_shim_not_uvx() -> None:
    """Git-only delivery (Dist-1): the hook refreshes the dashboard through the
    ``mcp_launch.py`` shim — which execs the git-installed / workspace ``.venv``
    console — never a per-session ``uvx`` PyPI resolve. Guards against a
    regression to the retired ``uvx --from mcp-workbay-handoff@<pin>`` launcher.
    """
    text = HOOK_SCRIPT.read_text()
    assert "mcp_launch.py" in text, "hook must launch via the mcp_launch.py shim"
    # No `uvx` *command* — the explanatory comment may still say "never uvx",
    # so only non-comment (code) lines are checked.
    code_lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    assert "uvx" not in "\n".join(code_lines), (
        "hook must not per-session resolve via uvx/PyPI"
    )
    assert "render-handoff" in text and "--kind dashboard" in text, (
        "hook must forward the render-handoff dashboard refresh"
    )
    # The shim's server-id arg is `workbay-handoff-mcp`; a bare
    # `mcp-workbay-handoff ` console call would bypass the shim's resolution.
    assert "mcp-workbay-handoff " not in text
