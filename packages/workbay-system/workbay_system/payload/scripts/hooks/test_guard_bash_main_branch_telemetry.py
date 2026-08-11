"""internal: guard-bash-main-branch block path wires terminal-guard-record.

Proves the BLOCK path builds+fires the correct detached argv (real-shape)
and that a broken/missing telemetry CLI does not change exit-2 (degrade).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

# Sibling hook under this hooks dir (works for payload/ and scripts/ trees).
HOOK_SCRIPT = Path(__file__).resolve().parent / "guard-bash-main-branch.py"
# Contract lives on the monorepo package docs path when running from payload
# or scripts tree; fall back to repo-root docs.
_HERE = Path(__file__).resolve()
_CANDIDATE_CONTRACTS = [
    _HERE.parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml",  # payload
    _HERE.parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml",
    _HERE.parents[1].parent / "docs" / "workbay" / "contracts" / "harness-protocol.yaml",
]
# Prefer monorepo root docs when present (scripts/hooks → parents[2] = repo root).
for _p in (
    _HERE.parents[2] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml",
    _HERE.parents[4] / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    if len(_HERE.parents) > 4
    else None,
    Path.cwd() / "docs" / "workbay" / "contracts" / "harness-protocol.yaml",
):
    if _p is not None and _p.is_file():
        CONTRACT_SOURCE = _p
        break
else:
    CONTRACT_SOURCE = Path("docs/workbay/contracts/harness-protocol.yaml")


def _seed_contract(repo: Path) -> None:
    target = repo / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    if CONTRACT_SOURCE.is_file():
        shutil.copy2(CONTRACT_SOURCE, target)
    else:
        pytest.skip(f"harness-protocol.yaml not found at {CONTRACT_SOURCE}")


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-C", str(cwd), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def main_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    _seed_contract(repo)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "init", cwd=repo)
    return repo


def _load_hook_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "guard_bash_main_branch_telem", HOOK_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_block_path_builds_and_fires_terminal_guard_record_argv(main_repo: Path) -> None:
    """Wire proof: block path constructs terminal-guard-record argv and Popen's it."""
    mod = _load_hook_module()
    captured: list[list[str]] = []

    def _fake_popen(argv, **_kwargs):
        captured.append(list(argv))

        class _Proc:
            returncode = 0

        return _Proc()

    with mock.patch.object(mod.subprocess, "Popen", side_effect=_fake_popen):
        mod._record_terminal_guard_block(
            command="sed -i 's/x/y/' packages/foo/bar.py",
            blocked=["packages/foo/bar.py"],
            decision="block",
        )

    assert len(captured) == 1
    argv = captured[0]
    assert "terminal-guard-record" in argv
    assert "--decision" in argv
    assert argv[argv.index("--decision") + 1] == "block"
    assert argv[argv.index("--tool-name") + 1] == "Bash"
    assert argv[argv.index("--policy-version") + 1] == "branch-isolation-v1"
    assert argv[argv.index("--policy-source") + 1] == "guard-bash-main-branch"
    assert "packages/foo/bar.py" in argv[argv.index("--command-preview") + 1]
    assert "packages/foo/bar.py" in argv[argv.index("--trigger") + 1]


def test_block_path_still_exits_2_when_telemetry_broken(main_repo: Path) -> None:
    """Mandate (b): telemetry failure must not break exit-2 block."""
    env = os.environ.copy()
    # Force a broken terminal-guard-record resolution path.
    env["WORKBAY_TERMINAL_GUARD_RECORD"] = "/nonexistent/terminal-guard-record-cli"
    payload = {
        "toolName": "Bash",
        "toolInput": {"command": "sed -i 's/x/y/' packages/foo/bar.py"},
    }
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=main_repo,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr
    assert "packages/foo/bar.py" in proc.stderr


def test_block_path_popen_exception_swallowed(main_repo: Path) -> None:
    """Popen raising must not escape _record_terminal_guard_block."""
    mod = _load_hook_module()

    def _boom(*_a, **_k):
        raise OSError("spawn failed")

    with mock.patch.object(mod.subprocess, "Popen", side_effect=_boom):
        mod._record_terminal_guard_block(
            command="rm packages/foo/bar.py",
            blocked=["packages/foo/bar.py"],
            decision="block",
        )
    # No exception = pass.


def test_malformed_record_override_does_not_escape(main_repo: Path) -> None:
    """REV-S3-1: a malformed WORKBAY_TERMINAL_GUARD_RECORD (unbalanced quote)
    makes shlex.split raise ValueError during argv build; that must be swallowed
    inside _record_terminal_guard_block, not escape."""
    mod = _load_hook_module()
    with mock.patch.dict(
        os.environ,
        {"WORKBAY_TERMINAL_GUARD_RECORD": "cli 'unterminated"},
    ):
        # shlex.split("cli 'unterminated") raises ValueError; must not propagate.
        mod._record_terminal_guard_block(
            command="rm packages/foo/bar.py",
            blocked=["packages/foo/bar.py"],
            decision="block",
        )
    # No exception = pass.


def test_block_path_still_exits_2_with_malformed_record_override(main_repo: Path) -> None:
    """End-to-end: a malformed WORKBAY_TERMINAL_GUARD_RECORD still yields the
    block (exit 2) and never inverts BLOCK→ALLOW via an escaping ValueError."""
    env = os.environ.copy()
    env["WORKBAY_TERMINAL_GUARD_RECORD"] = "cli 'unterminated"
    payload = {
        "toolName": "Bash",
        "toolInput": {"command": "sed -i 's/x/y/' packages/foo/bar.py"},
    }
    proc = subprocess.run(
        [sys.executable, str(HOOK_SCRIPT)],
        cwd=main_repo,
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert proc.returncode == 2, proc.stderr
    assert "BLOCKED" in proc.stderr
    assert "packages/foo/bar.py" in proc.stderr
