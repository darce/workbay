"""Tests for the record-file-touch PostToolUse hook."""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOK_SCRIPT = Path(__file__).parent / "record-file-touch.py"


def _load_hook_module():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("record_file_touch_hook", str(HOOK_SCRIPT))
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


def test_edit_tool_exits_zero() -> None:
    """Hook exits 0 for Edit tool calls (best-effort, never blocks)."""
    result = _run_hook({
        "tool_name": "Edit",
        "tool_input": {"file_path": "/tmp/test_file.py", "old_string": "a", "new_string": "b"},
        "tool_response": {"success": True},
    })
    assert result.returncode == 0


def test_write_tool_exits_zero() -> None:
    """Hook exits 0 for Write tool calls."""
    result = _run_hook({
        "tool_name": "Write",
        "tool_input": {"file_path": "/tmp/test_file.py", "content": "hello"},
        "tool_response": {"success": True},
    })
    assert result.returncode == 0


def test_non_edit_write_tool_exits_zero() -> None:
    """Hook exits 0 immediately for non-Edit/Write tools."""
    result = _run_hook({
        "tool_name": "Bash",
        "tool_input": {"command": "ls"},
        "tool_response": {"stdout": "file.py"},
    })
    assert result.returncode == 0


def test_malformed_json_exits_zero() -> None:
    """Hook exits 0 on malformed input (best-effort)."""
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input="not json",
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert proc.returncode == 0


def test_empty_file_path_exits_zero() -> None:
    """Hook exits 0 when file_path is empty."""
    result = _run_hook({
        "tool_name": "Edit",
        "tool_input": {"file_path": "", "old_string": "a", "new_string": "b"},
    })
    assert result.returncode == 0


def test_determine_change_kind_edit_tool() -> None:
    """Edit tool always produces change_kind='edit'."""
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("record_file_touch_hook", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod._determine_change_kind("Edit", "/tmp/any_file.py") == "edit"


def test_determine_change_kind_write_untracked(tmp_path: Path) -> None:
    """Write tool on an untracked file produces change_kind='add'."""
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("record_file_touch_hook", str(HOOK_SCRIPT))
    assert spec and spec.loader
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)

    untracked = tmp_path / "new_file.py"
    untracked.write_text("hello")
    assert mod._determine_change_kind("Write", str(untracked)) == "add"


def test_monorepo_layout_fallback_uses_renamed_workbay_handoff_mcp_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """internal regression test: the monorepo-layout PYTHONPATH
    fallback must point at the current ``packages/mcp-workbay-handoff/src``
    directory, not the pre-v0.2.0 ``packages/mcp-workbay-handoff/src`` name.
    The directory was renamed when the package was repackaged for PyPI;
    the stale fallback meant the hook ran but could not resolve
    ``workbay_handoff_mcp`` and silently swallowed an ImportError, so file
    touches stopped being recorded in the monorepo itself.
    """
    mod = _load_hook_module()
    tracked = tmp_path / "README.md"
    tracked.write_text("hello")
    seen_env_pythonpath: list[str] = []

    def fake_run(command: list[str], *args, **kwargs) -> subprocess.CompletedProcess[str]:
        env = kwargs.get("env") or {}
        seen_env_pythonpath.append(env.get("PYTHONPATH", ""))
        return subprocess.CompletedProcess(command, 0, "", "")

    # Force the importlib.metadata branch in ``_resolve_agent_handoff_src``
    # to fail so the test exercises the monorepo-layout fallback path,
    # regardless of whether the host environment happens to have the
    # ``mcp-workbay-handoff`` distribution installed.
    from importlib import metadata as importlib_metadata

    def fake_distribution(_name: str):
        raise importlib_metadata.PackageNotFoundError(_name)

    monkeypatch.setattr(importlib_metadata, "distribution", fake_distribution)
    monkeypatch.setattr(mod, "_git_repo_root", lambda: str(tmp_path))
    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    monkeypatch.setattr(
        mod.sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "tool_name": "Edit",
                    "tool_input": {"file_path": str(tracked)},
                }
            )
        ),
    )

    assert mod.main() == 0
    assert seen_env_pythonpath, "expected record_file_touch subprocess to run"
    pythonpath = seen_env_pythonpath[0]
    expected = str(tmp_path / "packages" / "mcp-workbay-handoff" / "src")
    stale = str(tmp_path / "packages" / "workbay-handoff-mcp" / "src")
    assert expected in pythonpath, (
        f"PYTHONPATH must include renamed {expected!r}, got {pythonpath!r}"
    )
    assert stale not in pythonpath, (
        f"PYTHONPATH must not include the pre-v0.2.0 directory name {stale!r}"
    )


def test_consumer_overlay_symlink_is_preferred_over_monorepo_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """internal regression: a fresh bootstrap install symlinks the
    workbay-handoff source under
    ``.workbay/remote/packages/mcp-workbay-handoff/src`` without
    pip-installing the wheel. When ``importlib.metadata`` fails to
    resolve the distribution, ``_resolve_agent_handoff_src`` must prefer
    the consumer-overlay path over the monorepo fallback so the hook
    can resolve ``workbay_handoff_mcp`` on a vanilla consumer (which has
    no ``packages/mcp-workbay-handoff/`` checkout).
    """
    mod = _load_hook_module()
    overlay_src = tmp_path / ".workbay" / "remote" / "packages" / "mcp-workbay-handoff" / "src"
    overlay_src.mkdir(parents=True)
    (overlay_src / "workbay_handoff_mcp").mkdir()  # overlay must expose the module

    from importlib import metadata as importlib_metadata

    def fake_distribution(_name: str):
        raise importlib_metadata.PackageNotFoundError(_name)

    monkeypatch.setattr(importlib_metadata, "distribution", fake_distribution)

    resolved = mod._resolve_agent_handoff_src(str(tmp_path))
    assert resolved == str(overlay_src), (
        f"expected consumer-overlay path {overlay_src!r}, got {resolved!r}"
    )

    # When the overlay no longer exposes workbay_handoff_mcp, the monorepo fallback wins.
    (overlay_src / "workbay_handoff_mcp").rmdir()
    overlay_src.rmdir()
    overlay_src.parent.rmdir()
    resolved_fallback = mod._resolve_agent_handoff_src(str(tmp_path))
    assert resolved_fallback == str(
        tmp_path / "packages" / "mcp-workbay-handoff" / "src"
    )


def test_main_accepts_camel_case_post_tool_use_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Hook should accept toolName/toolInput/filePath payload variants."""
    mod = _load_hook_module()
    tracked = tmp_path / "README.md"
    tracked.write_text("hello")
    calls: list[list[str]] = []

    def fake_run(command: list[str], *args, **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(mod, "_git_repo_root", lambda: str(tmp_path))
    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    monkeypatch.setattr(
        mod.sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "toolName": "Edit",
                    "toolInput": {"filePath": str(tracked)},
                }
            )
        ),
    )

    assert mod.main() == 0
    assert calls, "expected record_file_touch subprocess for camelCase payload"
    assert calls[0][0] == sys.executable
    assert "record_file_touch" in calls[0][2]
    assert "file_path='README.md'" in calls[0][2]
    assert "change_kind='edit'" in calls[0][2]


def test_git_repo_root_falls_back_to_grok_when_git_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Call-site coverage: when `git rev-parse` fails, ``_git_repo_root`` resolves
    a non-Claude harness root via GROK_WORKSPACE_ROOT through the shared
    ``workspace_env_anchor`` fallback — not a Claude-only lookup."""
    mod = _load_hook_module()

    def fake_git_fail(command, *args, **kwargs):
        return subprocess.CompletedProcess(command, 128, "", "fatal: not a git repo")

    monkeypatch.setattr(mod.subprocess, "run", fake_git_fail)
    monkeypatch.delenv("CLAUDE_PROJECT_DIR", raising=False)
    monkeypatch.setenv("GROK_WORKSPACE_ROOT", "/tmp/grok-ws")

    assert mod._git_repo_root() == "/tmp/grok-ws"
