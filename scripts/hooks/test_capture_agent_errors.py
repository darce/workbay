"""Tests for the capture-agent-errors PostToolUse hook (internal).

The hook pattern-matches workbay-related failures in Bash tool results,
classifies them per the agent-error taxonomy, and writes through
``mcp-workbay-handoff errors-record``. Non-workbay failures stay
silent; the matcher errs toward silence on ambiguity.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HOOK_SCRIPT = Path(__file__).parent / "capture-agent-errors.py"
PACKAGE_ROOT = Path(__file__).resolve().parents[2]

# The 2026-06-05 consumer-repo incident that motivated implementation note.
IMPORT_ERROR_FIXTURE = (
    "Traceback (most recent call last):\n"
    '  File "<stdin>", line 1, in <module>\n'
    "ImportError: cannot import name 'list_handoff_rows' from 'workbay_handoff_mcp' "
    "(/Users/x/.pyenv/versions/3.13.9/lib/python3.13/site-packages/workbay_handoff_mcp/__init__.py)"
)


def _load_hook_module():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("capture_agent_errors_hook", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_hook(payload: dict, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=15,
        cwd=cwd,
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_classify_workbay_import_error_as_install_drift() -> None:
    mod = _load_hook_module()
    event = mod.classify(
        command="python -c 'import workbay_handoff_mcp'",
        output=IMPORT_ERROR_FIXTURE,
        exit_code=1,
    )
    assert event is not None
    assert event["error_class"] == "install_drift"
    assert event["package_name"] == "workbay_handoff_mcp"
    assert "cannot import name 'list_handoff_rows'" in event["summary"]


def test_classify_non_workbay_import_error_silent() -> None:
    mod = _load_hook_module()
    output = (
        "Traceback (most recent call last):\n"
        "ImportError: cannot import name 'foo' from 'requests' (/x/site-packages/requests/__init__.py)"
    )
    assert mod.classify(command="python app.py", output=output, exit_code=1) is None


def test_classify_make_task_failure_as_cli_failure() -> None:
    mod = _load_hook_module()
    event = mod.classify(
        command="make task-start TASK=internal",
        output="task-start: refusing: workspace_ambiguous",
        exit_code=2,
    )
    assert event is not None
    assert event["error_class"] == "cli_failure"


def test_classify_workbay_cli_failure() -> None:
    mod = _load_hook_module()
    event = mod.classify(
        command="mcp-workbay-handoff state",
        output="error: no handoff_state row",
        exit_code=1,
    )
    assert event is not None
    assert event["error_class"] == "cli_failure"


def test_classify_successful_command_silent() -> None:
    mod = _load_hook_module()
    assert (
        mod.classify(command="make task-start TASK=internal", output="ok", exit_code=0)
        is None
    )


def test_classify_generic_failure_silent() -> None:
    mod = _load_hook_module()
    assert mod.classify(command="npm test", output="1 failing", exit_code=1) is None


def test_classify_mcp_unreachable() -> None:
    mod = _load_hook_module()
    event = mod.classify(
        command="claude mcp call workbay-handoff-mcp load_session",
        output="McpError: MCP error -32000: Connection closed (workbay-handoff-mcp)",
        exit_code=1,
    )
    assert event is not None
    assert event["error_class"] == "mcp_unreachable"


# ---------------------------------------------------------------------------
# Hook process behavior
# ---------------------------------------------------------------------------


def test_hook_exits_zero_on_garbage_stdin() -> None:
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="not json",
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert proc.returncode == 0


def test_hook_exits_zero_and_silent_on_non_bash_tool() -> None:
    proc = _run_hook(
        {"tool_name": "Edit", "tool_input": {"file_path": "/x"}, "tool_response": {}}
    )
    assert proc.returncode == 0


def test_hook_builds_errors_record_argv(monkeypatch) -> None:
    mod = _load_hook_module()
    captured: list[list[str]] = []

    def _fake_run(argv, **kwargs):
        captured.append([str(a) for a in argv])

        class _P:
            returncode = 0

        return _P()

    monkeypatch.setattr(mod.subprocess, "run", _fake_run)
    rc = mod.process_event(
        {
            "tool_name": "Bash",
            "tool_input": {"command": "python -c 'import workbay_handoff_mcp'"},
            "tool_response": {
                "stdout": "",
                "stderr": IMPORT_ERROR_FIXTURE,
                "exitCode": 1,
            },
        }
    )
    assert rc == 0
    assert len(captured) == 1
    argv = captured[0]
    assert "errors-record" in argv
    assert "--error-class" in argv
    assert argv[argv.index("--error-class") + 1] == "install_drift"
    assert argv[argv.index("--package-name") + 1] == "workbay_handoff_mcp"
    assert argv[argv.index("--harness") + 1] == "claude-code"


def test_hook_silent_for_non_workbay_failure(monkeypatch) -> None:
    mod = _load_hook_module()
    captured: list[list[str]] = []
    monkeypatch.setattr(mod.subprocess, "run", lambda argv, **k: captured.append(argv))
    rc = mod.process_event(
        {
            "tool_name": "Bash",
            "tool_input": {"command": "npm test"},
            "tool_response": {"stdout": "", "stderr": "1 failing", "exitCode": 1},
        }
    )
    assert rc == 0
    assert captured == []


# ---------------------------------------------------------------------------
# Harness registration
# ---------------------------------------------------------------------------


def test_terminal_guard_config_registers_capture_agent_errors() -> None:
    payload = json.loads(
        (PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json").read_text()
    )
    entries = payload["hooks"]["PostToolUse"]
    commands: list[str] = []
    for entry in entries:
        if entry.get("matcher") != "Bash":
            continue
        command = entry.get("command")
        if command:
            commands.append(command)
        for hook in entry.get("hooks", []) or []:
            if isinstance(hook, dict) and hook.get("command"):
                commands.append(hook["command"])
    assert any(
        "scripts/hooks/capture-agent-errors.py" in command for command in commands
    )


# ---------------------------------------------------------------------------
# End-to-end acceptance (implementation note acceptance criterion 1, implementation note)
# ---------------------------------------------------------------------------


def test_import_error_fixture_lands_install_drift_row_with_provenance(
    tmp_path: Path,
) -> None:
    """The 2026-06-05 ImportError, replayed through the real hook subprocess,
    lands as an ``agent_errors`` row classed ``install_drift`` with package +
    version provenance (version resolved from the installed distribution by
    ``errors-record``)."""
    import os
    import sqlite3

    import pytest

    pytest.importorskip("workbay_handoff_mcp")
    from workbay_handoff_mcp import api as mcp_server
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    runtime = RuntimeConfig.for_workspace(
        tmp_path,
        state_dir=tmp_path / ".task-state",
        current_task_path=tmp_path / "CURRENT_TASK.json",
    )
    mcp_server.configure_runtime(runtime)
    with _get_db_connection():
        pass  # bootstrap schema at the current version

    # Hermetic PATH: a stale global `mcp-workbay-handoff` console script
    # (e.g. a pyenv shim) must not shadow the under-test install — force the
    # hook's module-form fallback (sys.executable -m workbay_handoff_mcp).
    # Keep git's own directory on PATH (not guaranteed to be /usr/bin on
    # Linux CI) since errors-record shells out to bare `git`.
    import shutil

    git_path = shutil.which("git")
    assert git_path, "git required on PATH for this test"
    env = os.environ.copy()
    env["PATH"] = os.pathsep.join(
        dict.fromkeys([os.path.dirname(git_path), "/usr/bin", "/bin"])
    )
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(
            {
                "hook_event_name": "PostToolUse",
                "tool_name": "Bash",
                "tool_input": {
                    "command": "python -c 'from workbay_handoff_mcp import list_handoff_rows'"
                },
                "tool_response": {
                    "stdout": "",
                    "stderr": IMPORT_ERROR_FIXTURE,
                    "exitCode": 1,
                },
            }
        ),
        capture_output=True,
        text=True,
        timeout=30,
        cwd=tmp_path,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr

    conn = sqlite3.connect(tmp_path / ".task-state" / "handoff.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM agent_errors").fetchall()
    conn.close()
    assert len(rows) == 1
    row = rows[0]
    assert row["error_class"] == "install_drift"
    assert row["package_name"] == "workbay_handoff_mcp"
    assert row["package_version"]  # version provenance from the installed dist
    assert row["harness"] == "claude-code"
    assert "cannot import name 'list_handoff_rows'" in row["summary"]
