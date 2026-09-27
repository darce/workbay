#!/usr/bin/env python3
"""Pre-merge guard for MCP handoff integrity behavior.

Validates the mcp-workbay-handoff package CLI (not the deprecated unified_server.py).
Exercises: lifecycle transitions, review finding states, destructive-clear guards,
and close-check readiness against a temporary state directory.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ORCHESTRATOR_PACKAGE_DIR = Path("packages") / "mcp-workbay-orchestrator"


def _has_orchestrator_package(repo_root: Path) -> bool:
    return (repo_root / ORCHESTRATOR_PACKAGE_DIR).is_dir()


def _discover_repo_root() -> Path:
    override = os.environ.get("ORCHESTRATOR_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    for candidate in Path(__file__).resolve().parents:
        if _has_orchestrator_package(candidate):
            return candidate
    return Path.cwd().resolve()


REPO_ROOT = _discover_repo_root()


def _unwrap_cli_payload(payload: dict) -> dict:
    """Flatten v2 envelopes so legacy guard assertions keep working."""
    if payload.get("schema_version") == 2:
        data = payload.get("data")
        if isinstance(data, dict):
            flat = dict(payload)
            flat.update(data)
            return flat
    return payload


def _run_cli(
    args: list[str],
    env: dict[str, str],
    state_root: Path,
    expect_success: bool = True,
    timeout_seconds: float = 15.0,
) -> dict:
    """Run the mcp-workbay-handoff CLI and parse JSON output."""
    base_args = [
        "mcp-workbay-handoff",
        "--workspace-root",
        str(state_root),
        "--state-dir",
        str(state_root / ".task-state"),
        "--current-task-path",
        str(state_root / "CURRENT_TASK.json"),
        "--exports-dir",
        str(state_root / ".task-state" / "exports"),
    ]
    try:
        proc = subprocess.run(
            base_args + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Command timed out after {timeout_seconds:.1f}s: {' '.join(args)}") from exc

    payload: dict = {}
    stdout = proc.stdout.strip()
    if stdout:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Non-JSON output for command {' '.join(args)}:\n{proc.stdout}\n{proc.stderr}") from exc

    # The CLI always returns exit 0 for JSON responses; check payload "ok" field
    # for business-level success/failure. Exit code != 0 means a true crash.
    succeeded = proc.returncode == 0 and payload.get("ok", True) is not False

    if expect_success and not succeeded:
        raise RuntimeError(f"Command failed unexpectedly ({' '.join(args)}):\n{proc.stdout}\n{proc.stderr}")
    if not expect_success and succeeded:
        raise RuntimeError(f"Command succeeded unexpectedly ({' '.join(args)}):\n{proc.stdout}\n{proc.stderr}")
    return _unwrap_cli_payload(payload)


def main() -> int:
    if _has_orchestrator_package(REPO_ROOT) is False:
        print(f"orchestrator repo root not found at {REPO_ROOT}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="handoff-integrity-") as temp_dir:
        temp_root = Path(temp_dir)
        env = os.environ.copy()

        task_ref = f"ci-handoff-{uuid.uuid4().hex[:8]}"

        _run_cli(
            ["set", "--task-ref", task_ref, "--objective", "CI handoff integrity guard", "--status", "in_progress"],
            env,
            temp_root,
        )
        _run_cli(
            [
                "review-record",
                "--session",
                "ci-handoff-guard",
                "--finding-id",
                "CI-H-1",
                "--file-path",
                "scripts/mcp/handoff_integrity_guard.py",
                "--description",
                "CI guard lifecycle smoke finding",
                "--severity",
                "low",
            ],
            env,
            temp_root,
        )
        _run_cli(
            ["review-update", "--finding-id", "CI-H-1", "--status", "fixed", "--session", "ci-handoff-guard"],
            env,
            temp_root,
        )

        missing_notes = _run_cli(
            ["review-update", "--finding-id", "CI-H-1", "--status", "deferred", "--session", "ci-handoff-guard"],
            env,
            temp_root,
            expect_success=False,
        )
        if missing_notes.get("ok", True) is not False:
            raise RuntimeError("Expected deferred update without notes to fail.")

        _run_cli(
            [
                "review-update",
                "--finding-id",
                "CI-H-1",
                "--status",
                "deferred",
                "--resolution-notes",
                "Deferred for CI parser smoke validation.",
                "--session",
                "ci-handoff-guard",
            ],
            env,
            temp_root,
        )

        # --- wontfix without resolution_notes must fail ---
        missing_wontfix_notes = _run_cli(
            ["review-update", "--finding-id", "CI-H-1", "--status", "wontfix", "--session", "ci-handoff-guard"],
            env,
            temp_root,
            expect_success=False,
        )
        if missing_wontfix_notes.get("ok", True) is not False:
            raise RuntimeError("Expected wontfix update without notes to fail.")

        # --- wontfix with resolution_notes succeeds ---
        _run_cli(
            [
                "review-update",
                "--finding-id",
                "CI-H-1",
                "--status",
                "wontfix",
                "--resolution-notes",
                "Wontfix for CI parser smoke validation.",
                "--session",
                "ci-handoff-guard",
            ],
            env,
            temp_root,
        )

        missing_reopen_reason = _run_cli(
            ["review-update", "--finding-id", "CI-H-1", "--status", "open", "--session", "ci-handoff-guard"],
            env,
            temp_root,
            expect_success=False,
        )
        if missing_reopen_reason.get("ok", True) is not False:
            raise RuntimeError("Expected reopen without reopen_reason to fail.")

        _run_cli(
            [
                "review-update",
                "--finding-id",
                "CI-H-1",
                "--status",
                "open",
                "--reopen-reason",
                "Reopened after deferred status while CI lifecycle continues.",
                "--session",
                "ci-handoff-guard",
            ],
            env,
            temp_root,
        )

        open_findings = _run_cli(
            ["review-list", "--task-ref", task_ref, "--status", "open"],
            env,
            temp_root,
        )
        finding_row = (open_findings.get("findings") or [None])[0]
        if not finding_row:
            raise RuntimeError(f"Expected open finding row after reopen, got: {open_findings}")
        if int(finding_row.get("reopen_count") or 0) < 1:
            raise RuntimeError(f"Expected reopen_count >= 1 after reopen, got: {finding_row}")
        if not finding_row.get("last_reopen_reason"):
            raise RuntimeError(f"Expected last_reopen_reason to be populated, got: {finding_row}")

        not_ready = _run_cli(
            ["integrity-check", "--kind", "close", "--task-ref", task_ref, "--enforce"],
            env,
            temp_root,
            expect_success=False,
        )
        if not_ready.get("ready_to_close", True):
            raise RuntimeError("Close-check should fail while finding is still open.")

        _run_cli(
            ["review-update", "--finding-id", "CI-H-1", "--status", "fixed", "--session", "ci-handoff-guard"],
            env,
            temp_root,
        )

        state = _run_cli(["state", task_ref], env, temp_root)
        active = state.get("active") or {}
        revision = int(active.get("revision", -1))
        if revision < 0:
            raise RuntimeError(f"Could not read active revision from state payload: {state}")

        _run_cli(
            [
                "set",
                "--task-ref",
                task_ref,
                "--objective",
                "CI handoff integrity guard",
                "--status",
                "done",
                "--expected-revision",
                str(revision),
            ],
            env,
            temp_root,
        )
        _run_cli(["task", task_ref], env, temp_root)

        ready = _run_cli(
            ["integrity-check", "--kind", "close", "--task-ref", task_ref, "--enforce"],
            env,
            temp_root,
        )
        if ready.get("ready_to_close") is not True:
            raise RuntimeError(f"Expected close-check to pass, got: {ready}")

        destructive_task_ref = f"ci-destructive-{uuid.uuid4().hex[:8]}"
        active_state = _run_cli(["state"], env, temp_root)
        active_revision = int((active_state.get("active") or {}).get("revision", -1))
        if active_revision < 0:
            raise RuntimeError(f"Could not read revision before destructive guard checks: {active_state}")

        _run_cli(
            [
                "set",
                "--task-ref",
                destructive_task_ref,
                "--objective",
                "CI destructive clear safeguards",
                "--status",
                "in_progress",
                "--expected-revision",
                str(active_revision),
            ],
            env,
            temp_root,
        )
        _run_cli(
            [
                "review-record",
                "--session",
                "ci-handoff-guard",
                "--finding-id",
                "CI-H-CLEAR-1",
                "--file-path",
                "scripts/mcp/handoff_integrity_guard.py",
                "--description",
                "CI destructive clear safeguard finding",
                "--severity",
                "low",
            ],
            env,
            temp_root,
        )

        destructive_payload_path = temp_root / "destructive-clear-payload.json"
        destructive_payload_path.write_text(
            json.dumps(
                {
                    "export_version": 1,
                    "task_ref": destructive_task_ref,
                    "snapshot": {
                        "task_ref": destructive_task_ref,
                        "active": {
                            "task_ref": destructive_task_ref,
                            "objective": "CI destructive clear safeguards",
                            "status": "in_progress",
                        },
                        "blockers": [],
                        "next_actions": [],
                        "decisions": [],
                        "verified_tests": [],
                        "review_findings": [],
                        "worktree_lanes": [],
                        "worker_reports": [],
                        "lane_messages": [],
                    },
                },
                indent=2,
            )
        )

        blocked_replace = _run_cli(
            ["import", "--input-path", str(destructive_payload_path), "--mode", "replace_task"],
            env,
            temp_root,
            expect_success=False,
        )
        if blocked_replace.get("ok", True) is not False:
            raise RuntimeError(f"Expected replace_task import without ack to fail, got: {blocked_replace}")
        if "allow_destructive_clear" not in (blocked_replace.get("error") or ""):
            raise RuntimeError(
                f"Expected replace_task failure to mention allow_destructive_clear, got: {blocked_replace}"
            )

        blocked_prune = _run_cli(
            ["archive", "--task-ref", destructive_task_ref, "--prune-working-rows"],
            env,
            temp_root,
            expect_success=False,
        )
        if blocked_prune.get("ok", True) is not False:
            raise RuntimeError(f"Expected prune archive without ack to fail, got: {blocked_prune}")
        if "allow_destructive_clear" not in (blocked_prune.get("error") or ""):
            raise RuntimeError(f"Expected prune failure to mention allow_destructive_clear, got: {blocked_prune}")

        finding_still_present = _run_cli(
            ["review-list", "--task-ref", destructive_task_ref, "--status", "open"],
            env,
            temp_root,
        )
        open_count_before_ack = len(finding_still_present.get("findings") or [])
        if open_count_before_ack != 1:
            raise RuntimeError(
                f"Expected finding to remain after blocked destructive operations, got: {finding_still_present}"
            )

        _run_cli(
            [
                "import",
                "--input-path",
                str(destructive_payload_path),
                "--mode",
                "replace_task",
                "--allow-destructive-clear",
            ],
            env,
            temp_root,
        )
        finding_cleared_after_ack = _run_cli(
            ["review-list", "--task-ref", destructive_task_ref, "--status", "open"],
            env,
            temp_root,
        )
        open_count_after_ack = len(finding_cleared_after_ack.get("findings") or [])
        if open_count_after_ack != 0:
            raise RuntimeError(
                f"Expected acknowledged destructive replace to clear finding rows, got: {finding_cleared_after_ack}"
            )

    print("handoff-integrity-guard: pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
