"""Post-merge remediation coverage for SSOT-source robustness (internal).

Covers the review findings:
  - RR-A-001: guard hooks sourced _resolve_repo_root.sh unguarded under
    `set -euo pipefail`; a missing SSOT aborted the source (non-zero exit)
    BEFORE the fail-open check, turning a partial-overlay skew into a spurious
    PreToolUse block (drift) or a guard bypass (main-branch).
  - RR-B-002: there was no forward gate ensuring future root-resolving hooks go
    through the shared helper rather than inlining their own logic.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

HOOKS = Path(__file__).parent
DRIFT_HOOK = HOOKS / "guard-worktree-drift.sh"
MAIN_HOOK = HOOKS / "guard-main-branch.sh"
SSOT = HOOKS / "_resolve_repo_root.sh"


def test_guard_worktree_drift_fail_open_when_ssot_missing(tmp_path: Path) -> None:
    """A drift hook whose sibling SSOT was never materialized must fail OPEN.

    Regression for RR-A-001: previously the unguarded `. _resolve_repo_root.sh`
    under `set -e` aborted with exit 1 (= PreToolUse block) when the file was
    absent. The `[ -f ]` guard + env fallback now lets it exit 0.
    """
    isolated = tmp_path / "hooks"
    isolated.mkdir()
    shutil.copy(DRIFT_HOOK, isolated / DRIFT_HOOK.name)
    # Deliberately do NOT copy _resolve_repo_root.sh or _worktree_drift.py.
    env = os.environ.copy()
    env.pop("CLAUDE_PROJECT_DIR", None)
    env.pop("GROK_WORKSPACE_ROOT", None)
    proc = subprocess.run(
        ["bash", str(isolated / DRIFT_HOOK.name)],
        cwd=str(tmp_path),
        env=env,
        input='{"tool_input": {"file_path": "README.md"}}',
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr


def test_guard_hooks_guard_the_ssot_source() -> None:
    """Both `set -e` guards must `[ -f ]`-guard the SSOT source (RR-A-001)."""
    for hook in (DRIFT_HOOK, MAIN_HOOK):
        text = hook.read_text()
        assert '[ -f "${HOOK_DIR}/_resolve_repo_root.sh" ]' in text, hook.name
        # The fallback must not re-inline git toplevel (that is the SSOT's job).
        assert "git rev-parse --show-toplevel" not in text, hook.name


def test_root_resolving_hooks_source_the_shared_ssot() -> None:
    """Forward gate (RR-B-002): any shell hook that touches a root-resolution
    token must route through the shared helper, not inline its own logic."""
    root_tokens = ("git rev-parse --show-toplevel", "CLAUDE_PROJECT_DIR", "GROK_WORKSPACE_ROOT")
    for sh in HOOKS.glob("*.sh"):
        if sh.name == SSOT.name:
            continue  # the SSOT is the single allowed home for inline resolution
        text = sh.read_text()
        if any(token in text for token in root_tokens):
            assert "_resolve_repo_root.sh" in text, (
                f"{sh.name} resolves a repo root but does not source the shared "
                "_resolve_repo_root.sh SSOT"
            )
