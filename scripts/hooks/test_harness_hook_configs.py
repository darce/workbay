from __future__ import annotations

import json
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def test_vscode_matchers_cover_current_workbay_tool_names() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    matchers = [entry.get("matcher", "") for entry in payload["hooks"]["PreToolUse"] + payload["hooks"]["PostToolUse"]]

    assert any("mcp_workbay-handoff-mcp_record_event" in matcher for matcher in matchers)
    assert any("mcp_workbay-handoff-mcp_close_slice" in matcher for matcher in matchers)
    assert any("mcp_workbay-handoff-mcp_get_handoff_state" in matcher for matcher in matchers)
    assert any("mcp_workbay-handoff-mcp_load_session" in matcher for matcher in matchers)
    assert any("mcp_workbay-handoff-mcp_review_findings" in matcher for matcher in matchers)


def test_vscode_hooks_register_bash_test_output_filter() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    entries = payload["hooks"]["PostToolUse"]
    bash_entries = [entry for entry in entries if entry.get("matcher") == "Bash"]

    assert bash_entries, "expected a Bash PostToolUse hook registration for VS Code"
    # PostToolUse entries use the nested matcher+hooks[] grouping.
    commands = [
        hook.get("command", "")
        for entry in bash_entries
        for hook in (entry.get("hooks") or [entry])
    ]
    assert any("scripts/hooks/filter-test-output.py" in command for command in commands)


def test_vscode_pre_tool_hooks_remove_broad_terminal_guard_and_keep_targeted_guards() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    commands = _stage_commands(payload, "PreToolUse")

    assert not any(".github/hooks/terminal-guard.py" in command for command in commands)
    assert not any("scripts/hooks/terminal-guard.py" in command for command in commands)
    assert any(".github/hooks/guard-worktree-drift.py" in command for command in commands)
    assert any(".github/hooks/guard-main-branch.py" in command for command in commands)
    assert any("scripts/hooks/guard-bash-main-branch.py" in command for command in commands)
    assert any("scripts/hooks/guard-task-plan-findings.py" in command for command in commands)


def test_vscode_hooks_do_not_register_regenerate_task_views() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    post_tool_entries = payload["hooks"]["PostToolUse"]

    commands = [
        hook.get("command", "")
        for entry in post_tool_entries
        for hook in entry.get("hooks", [])
        if isinstance(hook, dict)
    ]

    assert not any("scripts/hooks/regenerate-task-views.sh" in command for command in commands)


def _stage_commands(payload: dict, stage: str) -> list[str]:
    """Collect command strings from both flat and nested-hooks entry shapes."""
    commands: list[str] = []
    for entry in payload["hooks"].get(stage, []):
        if not isinstance(entry, dict):
            continue
        nested = entry.get("hooks")
        if isinstance(nested, list):
            for hook in nested:
                if isinstance(hook, dict) and isinstance(hook.get("command"), str):
                    commands.append(hook["command"])
            continue
        command = entry.get("command")
        if isinstance(command, str):
            commands.append(command)
    return commands


def test_vscode_hooks_register_advise_worktree_cd_for_session_start() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    commands = _stage_commands(payload, "SessionStart")
    assert any("scripts/hooks/advise-worktree-cd.py" in command for command in commands), (
        f"expected advise-worktree-cd.py registered in SessionStart; got {commands!r}"
    )


def test_vscode_hooks_register_advise_worktree_cd_for_user_prompt_submit() -> None:
    payload = _read_json(PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json")
    commands = _stage_commands(payload, "UserPromptSubmit")
    assert any("scripts/hooks/advise-worktree-cd.py" in command for command in commands), (
        f"expected advise-worktree-cd.py registered in UserPromptSubmit; got {commands!r}"
    )


def test_review_finding_resolve_guard_registered_for_generated_harnesses() -> None:
    generated = [
        PACKAGE_ROOT / ".github" / "hooks" / "terminal-guard.json",
        PACKAGE_ROOT / ".codex" / "hooks.json",
        PACKAGE_ROOT / ".claude" / "settings.hooks.json",
    ]

    for path in generated:
        payload = _read_json(path)
        commands = _stage_commands(payload, "PreToolUse")
        assert any("scripts/hooks/guard-review-finding-resolve.py" in command for command in commands), path


def test_vscode_contract_omits_terminal_guard_policy() -> None:
    """Terminal-guard is retired; only the durable telemetry ledger remains."""
    import yaml  # type: ignore[import-not-found]

    contract_path = PACKAGE_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    contract = yaml.safe_load(contract_path.read_text())

    assert "terminal_guard" not in contract


def test_vscode_contract_has_no_allowlist_supplement_field() -> None:
    """The retired terminal-guard contract must not keep allowlist leftovers."""
    import yaml  # type: ignore[import-not-found]

    contract_path = PACKAGE_ROOT / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    raw = contract_path.read_text()
    assert "allowlist_supplement" not in raw, (
        "terminal_guard.allowlist_supplement contract field must be absent under internal"
    )
    assert "terminal_guard_allowlist.json" not in raw, (
        "no reference to the legacy supplement path may survive in the contract"
    )

    contract = yaml.safe_load(raw)
    assert "terminal_guard" not in contract
