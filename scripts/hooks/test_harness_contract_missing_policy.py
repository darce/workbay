"""internal — hook portability fixes.

End-user PreToolUse hooks must not BLOCK when
``docs/workbay/contracts/harness-protocol.yaml`` is absent: the
contract YAML lives in the bootstrap overlay and a fresh
``--profile minimal`` install legitimately ships without it. Pre-Slice-4,
``check_main_clean.py`` and ``guard-bash-main-branch.py`` both exited 2
in that case, blocking the user's edit.

implementation note introduces ``HarnessContractMissingPolicy`` (``block`` / ``warn``
/ ``silent``) and the shared ``handle_missing_contract`` helper. End-user
hooks default to ``warn``: structured stderr message + exit 0. The
internal verification suite invokes ``check_main_clean.py --mode block``
to keep the hard-fail contract for CI.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


HOOKS_DIR = Path(__file__).resolve().parent
CHECK_MAIN_CLEAN = HOOKS_DIR / "check_main_clean.py"
GUARD_BASH = HOOKS_DIR / "guard-bash-main-branch.py"


def _init_repo_without_contract(repo: Path) -> None:
    """Initialize an empty repo on ``main`` with no harness-protocol.yaml.

    The repo is one commit deep on ``main`` with a tracked README. This
    mirrors the ``--profile minimal`` consumer install shape: a real
    repo, on ``main``, without the contract overlay.
    """
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "t@t"], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "t"], check=True
    )
    (repo / "README.md").write_text("hello\n")
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "init"], check=True
    )


def _run(cmd: list[str], cwd: Path, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
        input=input_text,
    )


def test_check_main_clean_warn_mode_missing_contract_exits_zero(tmp_path: Path) -> None:
    """``check_main_clean.py`` default ``--mode warn`` must NOT block when
    the contract YAML is absent. Pre-Slice-4 behavior: exit 2 with
    ``HarnessContractMissingError``. Post-Slice-4: structured stderr
    warning + exit 0 so end-user editing flow is not blocked."""
    repo = tmp_path / "consumer"
    _init_repo_without_contract(repo)

    result = _run([sys.executable, str(CHECK_MAIN_CLEAN), "--mode", "warn"], cwd=repo)
    assert result.returncode == 0, (
        f"warn mode must exit 0 on missing contract; got exit={result.returncode}, "
        f"stderr={result.stderr!r}"
    )
    assert "harness-protocol" in result.stderr.lower()


def test_check_main_clean_block_mode_missing_contract_still_blocks(tmp_path: Path) -> None:
    """``--mode block`` preserves the hard-fail contract for the internal
    verification suite (CI / pre-push gate)."""
    repo = tmp_path / "consumer"
    _init_repo_without_contract(repo)

    result = _run([sys.executable, str(CHECK_MAIN_CLEAN), "--mode", "block"], cwd=repo)
    assert result.returncode == 2, (
        f"block mode must hard-fail on missing contract; got exit={result.returncode}, "
        f"stderr={result.stderr!r}"
    )
    assert "harness-protocol" in result.stderr.lower()


def test_guard_bash_main_branch_missing_contract_warns_and_exits_zero(
    tmp_path: Path,
) -> None:
    """``guard-bash-main-branch.py`` is an end-user PreToolUse hook; when
    the contract is absent it must warn and exit 0 (not block the Bash
    command)."""
    repo = tmp_path / "consumer"
    _init_repo_without_contract(repo)

    payload = json.dumps(
        {
            "tool_name": "Bash",
            "tool_input": {"command": "ls"},
        }
    )
    result = _run(
        [sys.executable, str(GUARD_BASH)], cwd=repo, input_text=payload
    )
    assert result.returncode == 0, (
        f"guard-bash-main-branch must exit 0 on missing contract; got exit={result.returncode}, "
        f"stderr={result.stderr!r}"
    )


def test_harness_contract_missing_policy_enum_exists() -> None:
    """The policy enum must be importable from ``_harness_protocol``."""
    sys.path.insert(0, str(HOOKS_DIR))
    try:
        from _harness_protocol import HarnessContractMissingPolicy  # type: ignore
    finally:
        sys.path.pop(0)

    members = {member.name for member in HarnessContractMissingPolicy}
    assert members == {"BLOCK", "WARN", "SILENT"}
