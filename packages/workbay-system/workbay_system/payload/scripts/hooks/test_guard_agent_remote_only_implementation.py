"""Regression coverage for the remote-only Agent-tool guard.

Operator directive (2026-09-08): under ``--with-remote`` (install ledger
``execution_mode: remote_only``) implementation lanes go to the remote VM via
``dispatch_wave`` / ``run_offload_pass``; in-process subagents are limited to
read-only grunt work. A rule that lives only in a playbook holds exactly as long
as the coordinator remembers it, so this hook is the drain -- and the tests pin
both halves of that contract:

- **Implementation-shaped local subagents are refused** under ``remote_only``.
- **Everything else passes**: ``local_ok`` installs, read-only subagent types,
  prompts that declare themselves read-only, prompts with no implementation
  intent, and -- the easy override the directive asked for -- a ``[local-ok]``
  marker or ``WORKBAY_ALLOW_LOCAL_IMPLEMENT_SUBAGENT=1``, both audit-logged.
- **Fail-open on noise**: malformed stdin, foreign tool names, unreadable
  ledgers never block work.

Driven as a subprocess against the real hook so the stdin/exit-code contract is
exercised exactly as the harness invokes it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-agent-remote-only-implementation.py"
BYPASS_ENV = "WORKBAY_ALLOW_LOCAL_IMPLEMENT_SUBAGENT"
BYPASS_MARKER = "[local-ok]"
AUDIT_LOG = Path(".task-state") / "agent_remote_only_bypass.jsonl"

IMPL_PROMPT = (
    "Implement the fix in packages/mcp-workbay-orchestrator/src/lane_reaping.py: "
    "edit _probe_branch_dead to route through _run_reclaim_command, then run the "
    "tests and commit with make slice-commit."
)
READONLY_PROMPT = (
    "Classify each of these seven branches as SUPERSEDED or LANDABLE by comparing "
    "their diffs against main. Write your report to stdout only."
)


def _ledger(root: Path, mode: str | None) -> None:
    if mode is None:
        return
    (root / ".workbay-bootstrap.json").write_text(
        json.dumps({"schema_version": 1, "execution_mode": mode}), encoding="utf-8"
    )


def _run(
    root: Path,
    *,
    prompt: str = IMPL_PROMPT,
    subagent_type: str | None = "general-purpose",
    tool_name: str = "Agent",
    env: dict[str, str] | None = None,
    raw_stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    tool_input: dict[str, object] = {"prompt": prompt, "description": "lane work"}
    if subagent_type is not None:
        tool_input["subagent_type"] = subagent_type
    payload = {"tool_name": tool_name, "tool_input": tool_input}
    run_env = dict(os.environ)
    run_env.pop(BYPASS_ENV, None)
    run_env["CLAUDE_PROJECT_DIR"] = str(root)
    if env:
        run_env.update(env)
    return subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        input=json.dumps(payload) if raw_stdin is None else raw_stdin,
        capture_output=True,
        text=True,
        env=run_env,
        cwd=str(root),
    )


@pytest.fixture
def remote_root(tmp_path: Path) -> Path:
    _ledger(tmp_path, "remote_only")
    return tmp_path


# --- refusals -----------------------------------------------------------


def test_remote_only_blocks_implementation_subagent(remote_root: Path) -> None:
    proc = _run(remote_root)
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr
    # The refusal must name the sanctioned primitive AND the override, or it is
    # a wall instead of a drain.
    assert "dispatch_wave" in proc.stderr
    assert "run_offload_pass" in proc.stderr
    assert BYPASS_MARKER in proc.stderr
    assert BYPASS_ENV in proc.stderr


@pytest.mark.parametrize(
    "prompt",
    [
        "Fix the failing test in test_lane_routing.py and commit.",
        "Refactor resolve_routing_quad to keep provenance; run make slice-commit when green.",
        "Apply the patch from turn.patch with patch -p1 and stage the result.",
        "Write the new module scripts/hooks/foo.py implementing the guard.",
        "Resolve the merge conflict in lane_worktree.py and complete the merge.",
    ],
)
def test_remote_only_blocks_other_implementation_phrasings(remote_root: Path, prompt: str) -> None:
    proc = _run(remote_root, prompt=prompt)
    assert proc.returncode == 2, (prompt, proc.stderr)


def test_legacy_task_tool_name_is_guarded_too(remote_root: Path) -> None:
    proc = _run(remote_root, tool_name="Task")
    assert proc.returncode == 2, proc.stderr


# --- pass-throughs -----------------------------------------------------


def test_local_ok_install_never_blocks(tmp_path: Path) -> None:
    _ledger(tmp_path, "local_ok")
    assert _run(tmp_path).returncode == 0


def test_missing_ledger_reads_as_local_ok(tmp_path: Path) -> None:
    assert _run(tmp_path).returncode == 0


def test_unreadable_ledger_fails_open(tmp_path: Path) -> None:
    (tmp_path / ".workbay-bootstrap.json").write_text("{not json", encoding="utf-8")
    assert _run(tmp_path).returncode == 0


@pytest.mark.parametrize("subagent_type", ["Explore", "Plan", "claude-code-guide"])
def test_read_only_subagent_types_pass(remote_root: Path, subagent_type: str) -> None:
    # Tool-restricted types cannot edit even when the prompt says "implement".
    assert _run(remote_root, subagent_type=subagent_type).returncode == 0


def test_read_only_prompt_passes(remote_root: Path) -> None:
    assert _run(remote_root, prompt=READONLY_PROMPT).returncode == 0


def test_explicit_read_only_declaration_wins_over_impl_words(remote_root: Path) -> None:
    prompt = "READ-ONLY: do not edit any file. Report which fix landed and where."
    assert _run(remote_root, prompt=prompt).returncode == 0


def test_foreign_tool_name_passes(remote_root: Path) -> None:
    assert _run(remote_root, tool_name="Bash").returncode == 0


def test_malformed_stdin_fails_open(remote_root: Path) -> None:
    assert _run(remote_root, raw_stdin="{not json").returncode == 0
    assert _run(remote_root, raw_stdin="[]").returncode == 0


# --- the easy override ----------------------------------------------------


def _audit_lines(root: Path) -> list[dict[str, object]]:
    path = root / AUDIT_LOG
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_prompt_marker_overrides_and_is_audited(remote_root: Path) -> None:
    proc = _run(remote_root, prompt=f"{BYPASS_MARKER} {IMPL_PROMPT}")
    assert proc.returncode == 0, proc.stderr
    rows = _audit_lines(remote_root)
    assert len(rows) == 1
    assert rows[0]["event"] == "agent_remote_only_bypass"
    assert rows[0]["via"] == "marker"


def test_env_override_overrides_and_is_audited(remote_root: Path) -> None:
    proc = _run(remote_root, env={BYPASS_ENV: "1"})
    assert proc.returncode == 0, proc.stderr
    rows = _audit_lines(remote_root)
    assert len(rows) == 1
    assert rows[0]["via"] == "env"


def test_no_audit_line_when_nothing_was_bypassed(remote_root: Path) -> None:
    _run(remote_root, prompt=READONLY_PROMPT)
    _run(remote_root)  # blocked, not bypassed
    assert _audit_lines(remote_root) == []
