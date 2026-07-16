"""Tests for the SessionStart coherence self-check (internal).

For already-installed Claude workspaces the wrapper only engages once
configs are re-rendered; this SessionStart hook surfaces dangling hook
references at session start instead. Always exits 0 — it warns via
``additionalContext``, never blocks a session.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HOOK_SCRIPT = Path(__file__).parent / "coherence-self-check.py"


def _run(root: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["CLAUDE_PROJECT_DIR"] = str(root)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="{}",
        capture_output=True,
        text=True,
        timeout=15,
        cwd=root,
        env=env,
    )


def _workspace(tmp_path: Path) -> Path:
    root = tmp_path / "ws"
    (root / ".github" / "hooks").mkdir(parents=True)
    (root / "scripts" / "hooks").mkdir(parents=True)
    return root


def _config(commands: list[str]) -> str:
    return json.dumps(
        {
            "hooks": {
                "PreToolUse": [
                    {"matcher": "Bash", "type": "command", "command": c}
                    for c in commands
                ]
            }
        }
    )


def test_coherent_workspace_silent_exit_0(tmp_path: Path) -> None:
    root = _workspace(tmp_path)
    (root / "scripts" / "hooks" / "guard-ok.py").write_text("print('ok')\n")
    (root / ".github" / "hooks" / "terminal-guard.json").write_text(
        _config(["python3 scripts/hooks/guard-ok.py"])
    )

    result = _run(root)

    assert result.returncode == 0
    assert result.stdout == ""


def test_dangling_reference_warns_via_additional_context(tmp_path: Path) -> None:
    root = _workspace(tmp_path)
    (root / ".github" / "hooks" / "terminal-guard.json").write_text(
        _config(["python3 scripts/hooks/deleted-guard.py"])
    )

    result = _run(root)

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    context = payload["hookSpecificOutput"]["additionalContext"]
    assert payload["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    assert "deleted-guard.py" in context
    assert "coherence" in context.lower()


def test_wrapper_two_path_form_checks_handler_argument(tmp_path: Path) -> None:
    """The implementation note rendered form is two-path; a dangling handler behind a
    healthy wrapper must still be reported."""
    root = _workspace(tmp_path)
    (root / "scripts" / "hooks" / "_run_guard.py").write_text("pass\n")
    (root / ".github" / "hooks" / "terminal-guard.json").write_text(
        _config(["python3 scripts/hooks/_run_guard.py scripts/hooks/gone.py"])
    )

    result = _run(root)

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert "gone.py" in payload["hookSpecificOutput"]["additionalContext"]


def test_no_configs_silent_exit_0(tmp_path: Path) -> None:
    root = _workspace(tmp_path)

    result = _run(root)

    assert result.returncode == 0
    assert result.stdout == ""


def test_unreadable_config_never_blocks(tmp_path: Path) -> None:
    root = _workspace(tmp_path)
    (root / ".github" / "hooks" / "terminal-guard.json").write_text("{not json")

    result = _run(root)

    assert result.returncode == 0
