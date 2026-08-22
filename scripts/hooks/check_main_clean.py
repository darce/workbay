#!/usr/bin/env python3
"""Detect dirty protected paths on the main branch (BR-16 / BR-22).

Invoked from:
  - git hooks (post-checkout, post-commit, post-merge, post-rewrite) as a WARNING scanner.
    internal retuned ``post-merge`` to warn-only; ``pre-push`` keeps the hard block.
  - git pre-push hook as a HARD BLOCK when pushing main.
  - ``make check-main-clean`` as an on-demand operator/agent check.

Severity is selected with ``--mode {warn,block,doctor}`` (default ``warn``).
The legacy ``--block`` flag is preserved as a synonym for ``--mode block`` so
existing hook scripts and operator muscle memory keep working.

Reads no stdin. Exit 0 when clean, when not on a protected branch, or when
``mode`` is ``warn`` / ``doctor`` regardless of dirty state. Exit 2 only when
dirty protected paths exist and ``mode`` is ``block`` (pre-push surface).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _current_branch(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _repo_root() -> Path:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return Path.cwd()
    if proc.returncode != 0:
        return Path.cwd()
    return Path(proc.stdout.strip() or ".")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--block",
        action="store_true",
        help="Exit 2 instead of 0 when dirty protected paths are found "
        "(equivalent to --mode block; preserved for legacy hook scripts).",
    )
    # internal: explicit mode flag aligned with the doctor
    # facet. ``warn`` keeps the prior post-checkout/post-commit
    # severity (always exit 0). ``block`` mirrors ``--block``. ``doctor``
    # exits 0 like ``warn`` but signals the operator should triage via
    # `make doctor LIFECYCLE_ARGS=--json`.
    parser.add_argument(
        "--mode",
        choices=("warn", "block", "doctor"),
        default=None,
        help="Severity mode. `warn` (default) and `doctor` exit 0; `block` exits 2 "
        "when dirty protected paths exist.",
    )
    parser.add_argument(
        "--trigger",
        default="manual",
        help="Source hook/op that fired this scan (post-checkout, post-commit, "
        "post-merge, post-rewrite, pre-push, manual). Shown in the output header.",
    )
    args = parser.parse_args()

    # Resolve effective mode: an explicit ``--mode`` wins; otherwise
    # ``--block`` maps to ``block`` and the default is ``warn``.
    if args.mode is not None:
        mode = args.mode
    elif args.block:
        mode = "block"
    else:
        mode = "warn"

    repo_root = _repo_root()
    branch = _current_branch(repo_root)
    if branch not in {"main", "master"}:
        return 0

    hook_dir = Path(__file__).resolve().parent
    root_hook_dir = repo_root / "scripts" / "hooks"
    for candidate in (root_hook_dir, hook_dir):
        candidate_str = str(candidate)
        sys.path[:] = [path for path in sys.path if path != candidate_str]
        sys.path.insert(0, candidate_str)
    try:
        # internal: narrow the post-merge tripwire to the
        # state-file surface. ``find_dirty_state_files`` consumes
        # ``policy.state_dirty_surfaces`` only and combines git status
        # with a direct filesystem walk so gitignored state files
        # (``.task-state/handoff.db``) surface even though plain
        # porcelain hides them. Planning artefacts (``docs/scopes/**``,
        # ``docs/tasks/**`` and package mirrors) live in
        # ``first_edit_protected_surfaces`` and are enforced by the
        # PreToolUse file-mutation hook, not by this post-merge gate.
        from _branch_isolation_guard import find_dirty_state_files
        from _harness_protocol import (
            HarnessContractMissingError,
            HarnessContractMissingPolicy,
            handle_missing_contract,
            load_branch_isolation_policy,
        )
    except ImportError as exc:
        print(f"check_main_clean: import failed — {exc}", file=sys.stderr)
        return 0

    # internal: align the missing-contract policy with the
    # operator-selected mode so a fresh `--profile minimal` consumer
    # install does not hard-block when the contract overlay is absent.
    # `--mode block` (CI / pre-push) retains the legacy exit-2 contract.
    missing_policy = (
        HarnessContractMissingPolicy.BLOCK
        if mode == "block"
        else HarnessContractMissingPolicy.WARN
    )
    try:
        policy = load_branch_isolation_policy(repo_root)
    except HarnessContractMissingError as exc:
        return handle_missing_contract(exc, policy=missing_policy)
    if not policy.state_dirty_surfaces:
        # Source-checkout fallback: this monorepo's root-level contract can be
        # a local overlay, while the tracked package contract lives next to the
        # hook sources. Do not let a stale overlay silently disable internal's
        # state-file tripwire.
        package_root = hook_dir.parents[1]
        package_contract = package_root / "docs" / "workbay" / "contracts" / "harness-protocol.yaml"
        if package_contract.exists():
            try:
                fallback_policy = load_branch_isolation_policy(package_root)
            except HarnessContractMissingError:
                fallback_policy = None
            if fallback_policy is not None and fallback_policy.state_dirty_surfaces:
                policy = fallback_policy

    dirty_paths = find_dirty_state_files(
        repo_root=str(repo_root),
        policy=policy,
    )
    if not dirty_paths:
        if args.trigger != "manual":
            # Silent on hook triggers when clean.
            return 0
        print(f"check-main-clean: OK ({branch}, no dirty state files)")
        return 0

    resolved_branch = branch
    rendered = "\n".join(f"  - {path}" for path in dirty_paths)
    severity = {
        "block": "BLOCKED",
        "doctor": "DOCTOR",
        "warn": "WARNING",
    }[mode]
    trigger = args.trigger
    print(
        f"\n{severity} (check-main-clean / trigger={trigger}): "
        f"runtime state files are dirty on {resolved_branch}.\n"
        f"Dirty state files:\n{rendered}\n\n"
        "Invariant: after a clean fast-forward into main, no runtime\n"
        "state file (CURRENT_TASK.json, DASHBOARD.txt, .task-state/handoff.db\n"
        "and SQLite sidecars,\n"
        "archived task snapshots) should be uncommitted or untracked.\n"
        "Planning artefacts that arrive via the merge are not state\n"
        "files and do not trigger this gate — they are enforced by the\n"
        "PreToolUse file-mutation hook instead.\n\n"
        "How this likely happened:\n"
        "  - the handoff database or dashboard regenerated without a commit\n"
        "  - a runtime hook wrote state mid-merge\n"
        "  - an editor or background process touched a state file on main\n\n"
        "Remediation:\n"
        "  1. Review each file (git diff / cat) before acting.\n"
        "  2. Commit the regenerated state on main if it is the canonical update,\n"
        "     or restore the prior version:\n"
        "       git restore <file>\n"
        "  3. For gitignored handoff DB state, confirm with `git check-ignore`\n"
        "     before deleting; the file may be local-only and safe to remove.\n\n"
        "See: docs/workbay/rules/development-workflow.md"
        "#branch-isolation-protocol-mandatory\n",
        file=sys.stderr,
    )
    return 2 if mode == "block" else 0


if __name__ == "__main__":
    raise SystemExit(main())
