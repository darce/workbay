"""Tests for the advise-worktree-cd advisory hook.

Drives the hook via subprocess with a synthetic payload on stdin and
asserts on the JSON shape printed to stdout. The active-task resolver
is monkeypatched at module level by importing the hook as a module and
swapping `_load_active_task` for a fake.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

HOOK_SCRIPT = Path(__file__).parent / "advise-worktree-cd.py"
HOOKS_DIR = Path(__file__).parent


def _load_hook() -> ModuleType:
    sys.path.insert(0, str(HOOKS_DIR))
    spec = importlib.util.spec_from_file_location("advise_worktree_cd_hook", HOOK_SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_hook(payload: dict[str, Any], *, env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    import os

    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


def _fake_context(task_ref: str | None = None, target: str | None = None, branch: str | None = None, primary: str = "/tmp/primary") -> Any:
    from _active_task_context import ActiveTaskContext

    return ActiveTaskContext(task_ref, target, branch, primary)


def test_silent_when_cwd_matches_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    target = tmp_path / "worktree"
    target.mkdir()

    mod = _load_hook()
    monkeypatch.setattr(mod, "_load_active_task", lambda _root: _fake_context("internal", str(target)))
    monkeypatch.setattr(mod, "_workspace_root", lambda: target)
    monkeypatch.setattr(mod, "_cwd_worktree", lambda _cwd: str(target.resolve(strict=False)))

    payload = {"hook_event_name": "SessionStart", "session_id": "s", "source": "startup", "cwd": str(target)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_silent_when_no_active_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_hook()
    monkeypatch.setattr(mod, "_load_active_task", lambda _root: _fake_context(None, None))
    monkeypatch.setattr(mod, "_workspace_root", lambda: tmp_path)

    payload = {"hook_event_name": "UserPromptSubmit", "session_id": "s", "prompt": "hi", "cwd": str(tmp_path)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_silent_for_maint_task_prefix(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    target = tmp_path / "worktree"
    target.mkdir()
    cwd = tmp_path / "elsewhere"
    cwd.mkdir()

    mod = _load_hook()
    monkeypatch.setattr(mod, "_load_active_task", lambda _root: _fake_context("MAINT-cleanup-20260501", str(target)))
    monkeypatch.setattr(mod, "_workspace_root", lambda: cwd)
    monkeypatch.setattr(mod, "_cwd_worktree", lambda _cwd: str(cwd.resolve(strict=False)))

    payload = {"hook_event_name": "UserPromptSubmit", "session_id": "s", "prompt": "hi", "cwd": str(cwd)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_silent_when_target_worktree_path_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_hook()
    monkeypatch.setattr(mod, "_load_active_task", lambda _root: _fake_context("internal", None))
    monkeypatch.setattr(mod, "_workspace_root", lambda: tmp_path)

    payload = {"hook_event_name": "SessionStart", "session_id": "s", "source": "startup", "cwd": str(tmp_path)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_silent_on_unresolved_task_context_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_hook()

    def _raise(_root: Path) -> Any:
        raise ValueError("Ambiguous active task for workspace path.")

    monkeypatch.setattr(mod, "_load_active_task", _raise)
    monkeypatch.setattr(mod, "_workspace_root", lambda: tmp_path)

    payload = {"hook_event_name": "UserPromptSubmit", "session_id": "s", "prompt": "hi", "cwd": str(tmp_path)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_silent_on_handoff_import_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """When the handoff exports return None (MCP package unavailable),
    `_load_active_task` returns an empty context — the hook stays silent."""
    mod = _load_hook()
    import _active_task_context as ctx_mod

    monkeypatch.setattr(ctx_mod, "_load_handoff_exports", lambda: None)
    monkeypatch.setattr(mod, "_workspace_root", lambda: tmp_path)

    payload = {"hook_event_name": "SessionStart", "session_id": "s", "source": "startup", "cwd": str(tmp_path)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == ""


def test_strict_mode_rejects_mismatched_event_name() -> None:
    """Strict-mode validation must reject a payload whose hook_event_name
    is not SessionStart or UserPromptSubmit."""
    payload = {
        "hook_event_name": "Stop",
        "session_id": "s",
        "tool_name": "Bash",
        "tool_input": {"command": "echo ok"},
        "tool_response": {"stdout": "", "stderr": "", "exitCode": 0},
        "prompt": "",
    }
    result = _run_hook(payload, env_extra={"WORKBAY_HOOK_PROTOCOL_STRICT": "1"})
    # Mismatched event name shape: hook treats it as unrecognized and routes
    # through validate_event which exits non-zero in strict mode.
    assert result.returncode != 0, (
        f"strict mode did not block mismatched event name. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def test_emits_directive_in_process(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """In-process variant of the divergence test: full monkeypatching
    proves the directive payload shape and content."""
    target = tmp_path / "worktree-feature"
    target.mkdir()
    cwd = tmp_path / "worktree-primary"
    cwd.mkdir()

    mod = _load_hook()
    monkeypatch.setattr(mod, "_load_active_task", lambda _root: _fake_context("internal", str(target)))
    monkeypatch.setattr(mod, "_workspace_root", lambda: cwd)
    monkeypatch.setattr(mod, "_cwd_worktree", lambda _cwd: str(cwd.resolve(strict=False)))

    payload = {"hook_event_name": "UserPromptSubmit", "session_id": "s", "prompt": "hi", "cwd": str(cwd)}
    monkeypatch.setattr("sys.stdin", _StdinFromString(json.dumps(payload)))
    rc = mod.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out, "expected stdout payload on cwd/target divergence"
    parsed = json.loads(captured.out)
    assert parsed["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    addl = parsed["hookSpecificOutput"]["additionalContext"]
    assert "internal" in addl
    assert str(target.resolve(strict=False)) in addl
    assert f"cd {target.resolve(strict=False)}" in addl


def test_invalid_json_payload_exits_zero() -> None:
    """Non-JSON stdin must not crash; hook exits silent."""
    result = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="this is not json",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert result.stdout == ""


class _StdinFromString:
    """Tiny stdin-replacement for in-process hook execution."""

    def __init__(self, text: str) -> None:
        self._text = text

    def read(self) -> str:
        return self._text
