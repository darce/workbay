"""implementation note (internal): Stop-hook compaction_failed self-capture."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

HOOK_SCRIPT = Path(__file__).parent / "compact-session.py"
CAPTURE_SCRIPT = Path(__file__).parent / "capture-agent-errors.py"
PACKAGES_DIR = Path(__file__).resolve().parents[5]
HANDOFF_SRC = PACKAGES_DIR / "mcp-workbay-handoff" / "src"
PROTOCOL_SRC = PACKAGES_DIR / "workbay-protocol" / "src"


@pytest.fixture()
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state_dir = tmp_path / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WORKBAY_HANDOFF_STATE_DIR", str(state_dir))
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_SHA_VALIDATION", "1")
    monkeypatch.setenv("WORKBAY_HANDOFF_SKIP_BRANCH_ENFORCEMENT", "1")
    for src in (PROTOCOL_SRC, HANDOFF_SRC):
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime, set_handoff_state

    runtime = RuntimeConfig.for_workspace(
        tmp_path,
        state_dir=state_dir,
        current_task_path=tmp_path / "CURRENT_TASK.json",
    )
    configure_runtime(runtime)
    set_handoff_state(
        task_ref="internal",
        objective="compaction_failed capture test",
        status="in_progress",
        target_branch="feature/ws-cmperr-01",
    )
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    return tmp_path


def _load_module(path: Path, name: str):
    spec = spec_from_file_location(name, str(path))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_emit_compaction_failed_invokes_errors_record(monkeypatch) -> None:
    mod = _load_module(HOOK_SCRIPT, "compact_session_hook")
    captured: list[list[str]] = []

    class _Proc:
        returncode = 0

    monkeypatch.setattr(mod, "_resolve_errors_record_argv", lambda: ["python", "errors-record"])
    monkeypatch.setattr(mod, "_git_repo_root", lambda: "/tmp/repo")
    monkeypatch.setattr(
        mod.subprocess,
        "run",
        lambda argv, **kwargs: captured.append([str(a) for a in argv]) or _Proc(),
    )

    mod._emit("compaction failed: transcript unreadable: too large")

    errors_calls = [argv for argv in captured if "errors-record" in argv]
    assert len(errors_calls) == 1
    argv = errors_calls[0]
    assert argv[argv.index("--error-class") + 1] == "compaction_failed"
    summary = argv[argv.index("--summary") + 1]
    assert summary.startswith("hook=Stop compaction failed:")
    assert "--tool-name" not in argv
    assert argv[argv.index("--harness") + 1] == "claude-code"


def test_capture_agent_errors_resolve_harness_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module(CAPTURE_SCRIPT, "capture_agent_errors_hook")
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "grok")
    assert mod._resolve_harness() == "grok"


def test_capture_agent_errors_resolve_harness_unknown_coerces_manual(monkeypatch) -> None:
    mod = _load_module(CAPTURE_SCRIPT, "capture_agent_errors_hook")
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "mystery-harness")
    assert mod._resolve_harness() == "manual"


def test_capture_agent_errors_resolve_harness_defaults_claude_code(monkeypatch) -> None:
    mod = _load_module(CAPTURE_SCRIPT, "capture_agent_errors_hook")
    monkeypatch.delenv("WORKBAY_HANDOFF_HARNESS", raising=False)
    monkeypatch.delenv("GROK_WORKSPACE_ROOT", raising=False)
    assert mod._resolve_harness() == "claude-code"


def test_capture_agent_errors_resolve_harness_grok_workspace_root_sniff(monkeypatch) -> None:
    """REV-E-010: grok compat-delivery has no inline env export (internal);
    GROK_WORKSPACE_ROOT presence identifies the grok launcher."""
    mod = _load_module(CAPTURE_SCRIPT, "capture_agent_errors_hook")
    monkeypatch.delenv("WORKBAY_HANDOFF_HARNESS", raising=False)
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", "/tmp/grok-ws")
    assert mod._resolve_harness() == "grok"


def test_capture_agent_errors_resolve_harness_explicit_env_beats_grok_sniff(monkeypatch) -> None:
    mod = _load_module(CAPTURE_SCRIPT, "capture_agent_errors_hook")
    monkeypatch.setenv("WORKBAY_HANDOFF_HARNESS", "codex")
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", "/tmp/grok-ws")
    assert mod._resolve_harness() == "codex"


def test_compact_session_resolve_harness_grok_workspace_root_sniff(monkeypatch) -> None:
    """compact-session.py mirrors capture-agent-errors._resolve_harness."""
    mod = _load_module(HOOK_SCRIPT, "compact_session_hook_grok_sniff")
    monkeypatch.delenv("WORKBAY_HANDOFF_HARNESS", raising=False)
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", "/tmp/grok-ws")
    assert mod._resolve_harness() == "grok"


def test_invalid_settings_records_compaction_failed_row(workspace: Path) -> None:
    """Failure stderr path also lands agent_errors.compaction_failed."""
    pytest.importorskip("workbay_handoff_mcp")

    transcript = workspace / "transcript.jsonl"
    transcript.write_text("turn 1 user: hi\n")
    payload = {
        "hook_event_name": "Stop",
        "session_id": "session-capture-fail",
        "transcript_path": str(transcript),
        "cwd": str(workspace),
    }
    env = {
        # MIN_NEW_TOKENS, not MIN_NEW_TURNS: the TURNS env mapping was retired
        # (implementation note) and is no longer read, so an invalid TURNS value parses
        # clean and the failure path under test never fires.
        "WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS": "abc",
        "CLAUDE_PROJECT_DIR": str(workspace),
        "WORKBAY_HANDOFF_STATE_DIR": str(workspace / ".task-state"),
    }
    packages = Path(__file__).resolve().parents[5]
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(packages / "mcp-workbay-handoff" / "src"),
            str(packages / "workbay-protocol" / "src"),
        ]
    )
    # Hermetic PATH (REV-C-010): a stale global `mcp-workbay-handoff`
    # console script (e.g. a pyenv shim) must not shadow the under-test
    # sources — it can predate WORKBAY_HANDOFF_STATE_DIR and write the
    # compaction_failed row into the developer's primary handoff.db. Force
    # the hook's module-form fallback (sys.executable -m) by reducing PATH
    # to git's own directory (mirrors test_capture_agent_errors.py).
    git_path = shutil.which("git")
    assert git_path, "git required on PATH for this test"
    env["PATH"] = os.pathsep.join(
        dict.fromkeys([os.path.dirname(git_path), "/usr/bin", "/bin"])
    )
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=30,
        cwd=str(workspace),
        env={**os.environ, **env},
    )
    assert proc.returncode == 0
    assert "compaction failed: invalid compaction settings:" in proc.stderr

    db_path = workspace / ".task-state" / "handoff.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT error_class, summary, harness, tool_name FROM agent_errors ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["error_class"] == "compaction_failed"
    assert row["summary"].startswith("hook=Stop compaction failed:")
    assert row["harness"] == "claude-code"
    assert row["tool_name"] is None