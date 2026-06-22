from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import yaml  # type: ignore[import-not-found]


REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK_SCRIPT = REPO_ROOT / "scripts" / "hooks" / "regenerate-task-views.sh"
MCP_MANIFEST = REPO_ROOT / "config" / "agent-workflows" / "mcp_servers.yaml"


def _manifest_handoff_pin() -> str:
    """Return the ``mcp-workbay-handoff@<version>`` spec pinned in the manifest.

    Single source of truth for the version the hook must launch. Tests
    derive the expected spec from here (never a hard-coded literal) so a
    manifest version bump can never silently diverge from the hook.
    """
    manifest = yaml.safe_load(MCP_MANIFEST.read_text())
    for server in manifest["mcp_servers"]:
        if server.get("name") == "workbay-handoff-mcp":
            for arg in server.get("args", []):
                if isinstance(arg, str) and arg.startswith("mcp-workbay-handoff@"):
                    return arg
    raise AssertionError(
        f"{MCP_MANIFEST}: no pinned mcp-workbay-handoff@<version> spec found"
    )


def _write_fake_uvx(tmp_path: Path) -> Path:
    """Shadow ``uvx`` on PATH so the hook's launch is observable.

    The hook invokes the pinned handoff CLI via ``uvx <spec> ...`` exactly
    like the manifest's server entry, so intercepting ``uvx`` (rather than a
    bare ``mcp-workbay-handoff`` binary) is what proves the manifest-pinned
    launch path is taken.
    """
    fake_uvx = tmp_path / "uvx"
    fake_uvx.write_text(
        "#!/usr/bin/env bash\n"
        'echo "$@" >> "$TMP_HOOK_LOG"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_uvx.chmod(fake_uvx.stat().st_mode | stat.S_IEXEC)
    return fake_uvx


def _run_hook(tmp_path: Path, payload: dict) -> tuple[int, str]:
    _write_fake_uvx(tmp_path)
    log_path = tmp_path / "calls.log"
    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}:{env.get('PATH', '')}"
    env["TMP_HOOK_LOG"] = str(log_path)
    proc = subprocess.run(
        ["bash", str(HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=env,
        timeout=5,
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


def test_refresh_launches_manifest_pinned_spec_via_uvx(tmp_path: Path) -> None:
    """Drift guard: the version the hook launches is the one the manifest
    pins, resolved at runtime — not a literal baked into the hook. If the
    manifest pin changes, the hook follows it with no code edit; if the hook
    stops deriving from the manifest (e.g. a hard-coded or stale version, a
    bare PATH binary, or a broken manifest path), the launched spec no longer
    equals the manifest pin and this test fails.
    """
    expected_pin = _manifest_handoff_pin()
    code, calls = _run_hook(
        tmp_path,
        {"tool_name": "mcp__workbay_mcp__record_event", "tool_input": {}},
    )
    assert code == 0
    launched = calls.strip().split()
    assert launched, "expected the hook to launch uvx with the pinned spec"
    assert launched[0] == expected_pin, (
        f"hook launched {launched[0]!r}; manifest pins {expected_pin!r}"
    )


def test_hook_does_not_invoke_stale_binary_or_bare_path_cli() -> None:
    """The pre-v0.2.0 ``workbay-handoff-mcp`` binary name was dropped, and a
    bare ``mcp-workbay-handoff`` PATH call resolves to whatever pyenv shim is
    installed (the version-drift hazard). The hook must reference neither:
    it launches via ``uvx`` using the manifest-derived spec.
    """
    text = HOOK_SCRIPT.read_text()
    assert "workbay-handoff-mcp" not in text, (
        "stale pre-v0.2.0 binary name workbay-handoff-mcp must not be invoked"
    )
    # The only mcp-workbay-handoff reference is the pinned spec grep pattern
    # (mcp-workbay-handoff@...); a bare `mcp-workbay-handoff ` command would be a
    # regression to an unpinned PATH call.
    assert "mcp-workbay-handoff " not in text
    assert "uvx " in text, "hook must launch the handoff CLI via uvx"
    assert "mcp_servers.yaml" in text, "hook must derive the pin from the manifest"
