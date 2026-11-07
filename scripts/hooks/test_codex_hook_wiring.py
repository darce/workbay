"""Codex coupling tests: rendered routing, anchoring, and coherence."""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PAYLOAD = ROOT / "packages/workbay-system/workbay_system/payload"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


generator = load("wiring_generator", PAYLOAD / "scripts/generate_agent_workflows.py")
wrapper = load("wiring_wrapper", PAYLOAD / "scripts/_guard_wrap.py")
checker = load(
    "wiring_checker",
    ROOT / "packages/workbay-system/workbay_system/overlay_tooling/check_harness_sync.py",
)
SPEC = yaml.safe_load((ROOT / "docs/workbay/contracts/harness-protocol.yaml").read_text())


def config():
    return json.loads((ROOT / ".codex/hooks.json").read_text())


def test_case10_exec_command_reaches_guards():
    reached = set()
    for group in config()["hooks"]["PreToolUse"]:
        matcher = group.get("matcher", "")
        if re.fullmatch(matcher, "Bash"):
            assert re.fullmatch(matcher, "exec_command")
            assert re.fullmatch(matcher, "functions.exec_command")
            reached.update(
                name
                for name in (
                    "guard-bash-main-branch.py",
                    "guard-bash-lifecycle-primitives.py",
                    "guard-bash-remote-gate-routing.py",
                    "guard-codemap-first.py",
                )
                if name in json.dumps(group)
            )
    assert len(reached) == 4


def test_all_stages_nested_and_anchored():
    for groups in config()["hooks"].values():
        for group in groups:
            assert "command" not in group
            for handler in group["hooks"]:
                assert handler["statusMessage"].strip()
                command = handler["command"]
                assert command.startswith("sh -c ")
                assert 'git rev-parse --show-toplevel 2>/dev/null || pwd' in command
                assert wrapper.anchor_to_repo_root(command) == command


def test_anchor_runtime(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    nested = tmp_path / "nested"
    nested.mkdir()
    (tmp_path / "handler.py").write_text(
        "import os, sys\nprint(os.getcwd())\nprint(sys.stdin.read(), end='')\nsys.exit(7)\n"
    )
    command = wrapper.anchor_to_repo_root(f"{sys.executable} handler.py")
    result = subprocess.run(
        ["sh", "-c", command], cwd=nested, input="payload", text=True, capture_output=True
    )
    assert result.returncode == 7
    assert result.stdout == f"{tmp_path}\npayload"


def test_rerender_and_vscode_unchanged():
    rendered = generator.render_codex_hooks_config(SPEC["hooks"])
    assert rendered == generator.render_codex_hooks_config(SPEC["hooks"])
    assert rendered == config()
    assert rendered == json.loads((PAYLOAD / ".codex/hooks.json").read_text())
    assert generator.render_vscode_hooks_config(SPEC["hooks"]) == json.loads(
        (ROOT / ".github/hooks/terminal-guard.json").read_text()
    )


@pytest.mark.parametrize("missing", [False, True])
def test_coherence_resolves_inner_handler(tmp_path, missing):
    from workbay_bootstrap import coherence

    hook = load("wiring_coherence", ROOT / "scripts/hooks/coherence-self-check.py")
    handler = tmp_path / "scripts/handler.py"
    handler.parent.mkdir()
    if not missing:
        handler.touch()
    command = 'sh -c \'cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)" && exec python3 scripts/handler.py\''
    config_path = tmp_path / ".codex/hooks.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps({"hooks": {"PreToolUse": [{"hooks": [{"command": command}]}]}}))
    assert bool(hook._dangling_references(str(tmp_path))) == missing
    assert bool(coherence._resolve_every_script(tmp_path, config_path, ".codex/hooks.json")) == missing


def test_checker_rejects_flat_and_unanchored(tmp_path):
    target = tmp_path / ".codex/hooks.json"
    target.parent.mkdir()
    vscode = tmp_path / ".github/hooks/terminal-guard.json"
    vscode.parent.mkdir(parents=True)
    vscode.write_text(json.dumps(generator.render_vscode_hooks_config(SPEC["hooks"])))
    rendered = generator.render_codex_hooks_config(SPEC["hooks"])
    target.write_text(json.dumps(rendered))
    assert not checker._check_hooks(SPEC, repo_root=tmp_path)
    assert not checker._check_codex_status_messages(repo_root=tmp_path)
    group = rendered["hooks"]["PreToolUse"][0]
    handler = group["hooks"][0]
    group.update(group.pop("hooks")[0])
    target.write_text(json.dumps(rendered))
    assert any("flat" in error.lower() for error in checker._check_codex_status_messages(repo_root=tmp_path))
    group.clear()
    group.update(generator.render_codex_hooks_config(SPEC["hooks"])["hooks"]["PreToolUse"][0])
    handler = group["hooks"][0]
    handler["command"] = "python3 scripts/unanchored.py"
    target.write_text(json.dumps(rendered))
    assert checker._check_hooks(SPEC, repo_root=tmp_path)
