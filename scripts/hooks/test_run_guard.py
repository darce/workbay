"""Tests for the fail-open `_run_guard.py` wrapper (internal).

The wrapper is the rendered prefix for tool-guard hook commands:
``python3 scripts/hooks/_run_guard.py <handler-relpath> [args...]``.

Semantics under test:

- handler resolves + spawns -> byte-transparent passthrough of stdout,
  stderr, AND exit code (both verified block mechanisms survive: the
  stdout-JSON ``permissionDecision=block`` + exit-0 shape and the exit-2
  shape; the wrapper never parses or rewrites handler output),
- handler missing/unspawnable -> exit 0, no block, best-effort
  ``hook_infra_failure`` telemetry through the implementation note errors-record
  channel,
- ``--fail-mode=closed`` opt-out -> missing handler exits 2 instead,
- handlers resolve across BOTH hook surfaces (``scripts/hooks`` and
  ``.github/hooks``),
- a handler with no ``main()`` runs unaffected (subprocess execution, not
  import).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

WRAPPER = Path(__file__).parent / "_run_guard.py"


def _load_guard():
    """Import the wrapper as a module for white-box interpreter-routing tests."""
    spec = importlib.util.spec_from_file_location("_run_guard_under_test", str(WRAPPER))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    (root / "scripts" / "hooks").mkdir(parents=True)
    (root / ".github" / "hooks").mkdir(parents=True)
    return root


def _run_wrapper(
    root: Path,
    *args: str,
    stdin: str = "{}",
    env_extra: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, str(WRAPPER), *args],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=15,
        cwd=root,
        env=env,
    )


# ---------------------------------------------------------------------------
# Byte-transparent passthrough
# ---------------------------------------------------------------------------


def test_stdout_json_block_preserved_verbatim(tmp_path: Path) -> None:
    """The dominant block mechanism: stdout JSON + exit 0 must pass through
    byte-for-byte — the wrapper must not parse, reserialize, or wrap it."""
    root = _make_workspace(tmp_path)
    block_json = (
        '{"hookSpecificOutput": {"hookEventName": "PreToolUse", '
        '"permissionDecision": "block", "permissionDecisionReason": "nope"}}'
    )
    handler = root / "scripts" / "hooks" / "guard-block.py"
    handler.write_text(f"import sys\nsys.stdout.write({block_json!r})\nsys.exit(0)\n")

    result = _run_wrapper(root, "scripts/hooks/guard-block.py")

    assert result.returncode == 0
    assert result.stdout == block_json
    json.loads(result.stdout)  # still valid JSON after passthrough


def test_exit_2_preserved(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-exit2.py"
    handler.write_text(
        "import sys\nsys.stderr.write('blocked: policy violation\\n')\nsys.exit(2)\n"
    )

    result = _run_wrapper(root, "scripts/hooks/guard-exit2.py")

    assert result.returncode == 2
    assert "blocked: policy violation" in result.stderr


def test_handler_reads_stdin_payload(tmp_path: Path) -> None:
    """The hook payload arrives on stdin; the wrapper must hand it through."""
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-echo.py"
    handler.write_text(
        "import json, sys\n"
        "data = json.load(sys.stdin)\n"
        "sys.stdout.write(data['tool_name'])\n"
    )

    result = _run_wrapper(
        root,
        "scripts/hooks/guard-echo.py",
        stdin=json.dumps({"tool_name": "Bash"}),
    )

    assert result.returncode == 0
    assert result.stdout == "Bash"


def test_no_main_handler_unaffected(tmp_path: Path) -> None:
    """One live guard has no main(); subprocess execution must not care."""
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-nomain.py"
    handler.write_text("print('top-level ran')\n")

    result = _run_wrapper(root, "scripts/hooks/guard-nomain.py")

    assert result.returncode == 0
    assert result.stdout == "top-level ran\n"


# ---------------------------------------------------------------------------
# Fail-open on infra absence
# ---------------------------------------------------------------------------


def test_missing_handler_exits_0(tmp_path: Path) -> None:
    """errno-2 on the handler must NOT block (the incident class)."""
    root = _make_workspace(tmp_path)

    result = _run_wrapper(root, "scripts/hooks/deleted-guard.py")

    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_handler_records_hook_infra_failure_telemetry(
    tmp_path: Path,
) -> None:
    """Missing handler emits hook_infra_failure through errors-record.

    The test substitutes a recording stub for the errors-record CLI via
    WORKBAY_RUN_GUARD_ERRORS_RECORD (the wrapper's test seam — production
    resolution mirrors capture-agent-errors.py).
    """
    root = _make_workspace(tmp_path)
    sink = tmp_path / "telemetry.json"
    stub = tmp_path / "record-stub.py"
    stub.write_text(
        f"import json, sys\nopen({str(sink)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
    )

    result = _run_wrapper(
        root,
        "scripts/hooks/deleted-guard.py",
        env_extra={"WORKBAY_RUN_GUARD_ERRORS_RECORD": f"{sys.executable} {stub}"},
    )

    assert result.returncode == 0
    # Telemetry is fire-and-forget (REV-A-001): the detached recorder may
    # still be running when the wrapper exits — poll briefly for the sink.
    deadline = time.monotonic() + 10
    while not sink.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    recorded = json.loads(sink.read_text())
    assert "--error-class" in recorded
    assert recorded[recorded.index("--error-class") + 1] == "hook_infra_failure"
    summary = recorded[recorded.index("--summary") + 1]
    assert "deleted-guard.py" in summary


def test_missing_handler_returns_fast_despite_slow_telemetry(tmp_path: Path) -> None:
    """REV-A-001: telemetry is fire-and-forget. A slow errors-record must not
    push the wrapper past the smallest hook timeout (5s) — a harness-killed
    wrapper reads as a deny on Copilot/Codex, the exact incident class."""
    root = _make_workspace(tmp_path)
    slow_stub = tmp_path / "slow-record.py"
    slow_stub.write_text("import time\ntime.sleep(30)\n")

    start = time.monotonic()
    result = _run_wrapper(
        root,
        "scripts/hooks/deleted-guard.py",
        env_extra={
            "WORKBAY_RUN_GUARD_ERRORS_RECORD": f"{sys.executable} {slow_stub}"
        },
    )
    elapsed = time.monotonic() - start

    assert result.returncode == 0
    assert elapsed < 4, f"wrapper took {elapsed:.1f}s; must beat the 5s hook timeout"


def test_telemetry_failure_still_exits_0(tmp_path: Path) -> None:
    """Telemetry is best-effort: a broken errors-record must not block."""
    root = _make_workspace(tmp_path)

    result = _run_wrapper(
        root,
        "scripts/hooks/deleted-guard.py",
        env_extra={"WORKBAY_RUN_GUARD_ERRORS_RECORD": "/nonexistent/errors-record"},
    )

    assert result.returncode == 0


def test_no_handler_argument_exits_0(tmp_path: Path) -> None:
    """A malformed rendered command (no handler argument) fails open too."""
    root = _make_workspace(tmp_path)

    result = _run_wrapper(root)

    assert result.returncode == 0


# ---------------------------------------------------------------------------
# fail_mode: closed opt-out
# ---------------------------------------------------------------------------


def test_fail_mode_closed_missing_handler_exits_2(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)

    result = _run_wrapper(root, "--fail-mode=closed", "scripts/hooks/security-guard.py")

    assert result.returncode == 2
    assert "security-guard.py" in result.stderr


def test_fail_mode_closed_present_handler_passthrough(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "security-guard.py"
    handler.write_text("print('ok')\n")

    result = _run_wrapper(root, "--fail-mode=closed", "scripts/hooks/security-guard.py")

    assert result.returncode == 0
    assert result.stdout == "ok\n"


# ---------------------------------------------------------------------------
# Resolution: anchors, dual surface, bash dispatch
# ---------------------------------------------------------------------------


def test_resolves_github_hooks_surface(tmp_path: Path) -> None:
    """guard-main-branch.py / guard-worktree-drift.py live in .github/hooks."""
    root = _make_workspace(tmp_path)
    handler = root / ".github" / "hooks" / "guard-main-branch.py"
    handler.write_text("print('gh surface')\n")

    result = _run_wrapper(root, ".github/hooks/guard-main-branch.py")

    assert result.returncode == 0
    assert result.stdout == "gh surface\n"


def test_dual_surface_fallback_resolves_sibling_surface(tmp_path: Path) -> None:
    """A handler rendered under one surface but shipped under the other still
    resolves (dual-surface resolution per the plan's D2 graft)."""
    root = _make_workspace(tmp_path)
    handler = root / ".github" / "hooks" / "moved-guard.py"
    handler.write_text("print('found on sibling')\n")

    result = _run_wrapper(root, "scripts/hooks/moved-guard.py")

    assert result.returncode == 0
    assert result.stdout == "found on sibling\n"


def test_env_anchor_resolution_claude(tmp_path: Path) -> None:
    """$CLAUDE_PROJECT_DIR-anchored handler paths arrive pre-substituted by
    the harness as absolute paths; the wrapper must accept them."""
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-abs.py"
    handler.write_text("print('абс ok')\n")

    other_cwd = tmp_path / "elsewhere"
    other_cwd.mkdir()
    result = subprocess.run(
        [sys.executable, str(WRAPPER), str(handler)],
        input="{}",
        capture_output=True,
        text=True,
        timeout=15,
        cwd=other_cwd,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(root)},
    )

    assert result.returncode == 0
    assert result.stdout == "абс ok\n"


def test_workspace_root_env_anchor_beats_cwd(tmp_path: Path) -> None:
    """When CLAUDE_PROJECT_DIR is set, relative handler paths resolve against
    it even if the harness spawned the wrapper from another cwd."""
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-anchored.py"
    handler.write_text("print('anchored')\n")
    other_cwd = tmp_path / "elsewhere2"
    other_cwd.mkdir()

    result = subprocess.run(
        [sys.executable, str(WRAPPER), "scripts/hooks/guard-anchored.py"],
        input="{}",
        capture_output=True,
        text=True,
        timeout=15,
        cwd=other_cwd,
        env={**os.environ, "CLAUDE_PROJECT_DIR": str(root)},
    )

    assert result.returncode == 0
    assert result.stdout == "anchored\n"


def test_bash_handler_dispatched_via_bash(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-shell.sh"
    handler.write_text("#!/usr/bin/env bash\necho shell-ok\n")

    result = _run_wrapper(root, "scripts/hooks/guard-shell.sh")

    assert result.returncode == 0
    assert result.stdout == "shell-ok\n"


def test_handler_args_forwarded(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path)
    handler = root / "scripts" / "hooks" / "guard-args.py"
    handler.write_text("import sys\nprint(' '.join(sys.argv[1:]))\n")

    result = _run_wrapper(root, "scripts/hooks/guard-args.py", "--strict", "x")

    assert result.returncode == 0
    assert result.stdout == "--strict x\n"


# ---------------------------------------------------------------------------
# Interpreter routing (REV-B-HOOK-INTERP-01): Python handlers and the
# errors-record module fallback run under a deps-bearing interpreter; bash
# handlers stay on bash.
# ---------------------------------------------------------------------------


def test_python_handler_spawned_under_deps_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_guard()
    monkeypatch.setattr(mod, "_deps_python", lambda: "/fake/venv/python")
    monkeypatch.setattr(mod, "_resolve_handler", lambda root, handler: "/abs/guard.py")
    captured: dict = {}

    def fake_run(cmd, *a, **k):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    rc = mod.main(["scripts/hooks/guard.py", "--flag"])

    assert rc == 0
    assert captured["cmd"] == ["/fake/venv/python", "/abs/guard.py", "--flag"]


def test_bash_handler_not_routed_through_deps_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_guard()
    monkeypatch.setattr(
        mod, "_deps_python", lambda: pytest.fail("bash must not use _deps_python")
    )
    monkeypatch.setattr(mod, "_resolve_handler", lambda root, handler: "/abs/guard.sh")
    captured: dict = {}

    def fake_run(cmd, *a, **k):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    rc = mod.main(["scripts/hooks/guard.sh"])

    assert rc == 0
    assert captured["cmd"] == ["bash", "/abs/guard.sh"]


def test_errors_record_module_fallback_uses_deps_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_guard()
    monkeypatch.delenv(mod._ERRORS_RECORD_ENV, raising=False)
    monkeypatch.setattr(mod.shutil, "which", lambda name: None)  # no console script
    monkeypatch.setattr(mod, "_deps_python", lambda: "/fake/venv/python")

    argv = mod._errors_record_argv()

    assert argv == ["/fake/venv/python", "-m", "workbay_handoff_mcp", "errors-record"]


def test_deps_python_fails_open_to_launch_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # If the shared _interp helper can't be imported, the wrapper must fall
    # back to the launch interpreter rather than crash the guard.
    mod = _load_guard()
    monkeypatch.setitem(sys.modules, "_interp", None)  # force ImportError
    assert mod._deps_python() == sys.executable
