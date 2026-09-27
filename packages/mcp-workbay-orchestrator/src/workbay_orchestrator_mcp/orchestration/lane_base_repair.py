"""Dry-run-first repair of review procurement metadata, never review identity.

RES-02 lexicons/engineering.md:113: Git probes have bounded timeouts.
The worker claim precedes the native DB transaction and manifest CAS.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import re
import subprocess
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from . import lane_manifest
from .secure_sandbox import review_subject_from_lane_row


class RepairBlocked(RuntimeError):
    """Failure-closed refusal with a machine-readable reason."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class RepairPlan:
    repo: str
    task_ref: str
    lane_id: str
    branch: str
    before: str
    after: str
    tip: str
    manifest_digest: str
    row_digest: str
    provenance_digest: str
    creation_record: str


@dataclass(frozen=True)
class RepairReceipt:
    outcome: str
    plan: RepairPlan
    audit: str = "not_requested"


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _git(repo: Path, *args: str) -> str:
    from workbay_handoff_mcp.shared_write_context import run_subprocess

    try:
        return run_subprocess(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise RepairBlocked("git_provenance_unavailable") from exc


def _path(repo: Path, task: str, lane: str) -> Path:
    if not all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", x) for x in (task, lane)):
        raise RepairBlocked("invalid_identity")
    return repo / "config" / "lane-orchestration" / f"{task}.json"


def _read_row(conn: Any, task: str, lane: str) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?", (task, lane)).fetchall()
    if len(rows) != 1:
        raise RepairBlocked("lane_row_missing_or_ambiguous")
    if conn.execute(
        "SELECT 1 FROM lane_messages WHERE task_ref = ? AND lane_id = ? "
        "AND subject = 'brief:dispatch' AND status = 'open' LIMIT 1",
        (task, lane),
    ).fetchone():
        raise RepairBlocked("live_dispatch")
    row = dict(rows[0])
    if conn.execute(
        "SELECT 1 FROM worktree_lanes WHERE (branch = ? OR worktree_path = ?) "
        "AND status NOT IN ('closed', 'closed_stale', 'merged') LIMIT 1",
        (row["branch"], row["worktree_path"]),
    ).fetchone():
        raise RepairBlocked("active_owner")
    return row


def _inspect(
    repo: Path, task: str, lane: str, old: str, tip: str, row: dict[str, Any], manifest: dict[str, Any]
) -> RepairPlan:
    if not all(re.fullmatch(r"[0-9a-f]{40}", x) for x in (old, tip)):
        raise RepairBlocked("full_sha_pins_required")
    if Path(_git(repo, "rev-parse", "--show-toplevel")).resolve() != repo:
        raise RepairBlocked("wrong_repository")
    if manifest.get("task_ref") != task or row.get("task_ref") != task or row.get("lane_id") != lane:
        raise RepairBlocked("wrong_task_or_lane")
    spec = manifest.get("lanes", {}).get(lane)
    if not isinstance(spec, dict):
        raise RepairBlocked("manifest_lane_missing")
    if row.get("lane_kind") != "review" or row.get("status") not in {"closed", "closed_stale", "merged"}:
        raise RepairBlocked("nonterminal_or_nonreview_lane")
    if review_subject_from_lane_row(row) != (old, tip):
        raise RepairBlocked("immutable_subject_mismatch")
    branch = row.get("branch")
    if not isinstance(branch, str) or spec.get("branch") != branch:
        raise RepairBlocked("branch_mismatch")
    _git(repo, "check-ref-format", f"refs/heads/{branch}")
    if spec.get("worktree_path") != row.get("worktree_path"):
        raise RepairBlocked("worktree_mismatch")
    worktree = Path(row["worktree_path"])
    if not worktree.is_dir():
        raise RepairBlocked("worktree_unavailable")
    if (
        Path(_git(worktree, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
        != Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
    ):
        raise RepairBlocked("wrong_repository")
    if _git(worktree, "symbolic-ref", "HEAD") != f"refs/heads/{branch}":
        raise RepairBlocked("worktree_branch_mismatch")
    if _git(repo, "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}") != tip:
        raise RepairBlocked("changed_tip")
    if row.get("branch_tip_sha") not in (None, "", tip):
        raise RepairBlocked("row_tip_mismatch")
    # Read the raw log: formatted git-log output loses the zero old-OID that
    # distinguishes initial creation from a later reset or an incomplete log.
    log_path = Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-path", f"logs/refs/heads/{branch}"))
    try:
        records = log_path.read_text().splitlines()
    except OSError as exc:
        raise RepairBlocked("creation_provenance_missing") from exc
    if not records:
        raise RepairBlocked("creation_provenance_missing")
    previous = "0" * 40
    creation = None
    for index, record in enumerate(records):
        match = re.fullmatch(r"([0-9a-f]{40}) ([0-9a-f]{40}) .+ <[^>]*> \d+ [+-]\d{4}\t(.+)", record)
        if match is None or match[1] != previous:
            raise RepairBlocked("creation_provenance_malformed_or_pruned")
        if index == 0:
            if not match[3].startswith("branch: Created from "):
                raise RepairBlocked("creation_provenance_missing")
            creation = match[2]
        elif match[3].startswith("branch: Created from ") or match[2] == "0" * 40:
            raise RepairBlocked("creation_provenance_ambiguous")
        previous = match[2]
    if creation != tip or previous != tip:
        raise RepairBlocked("creation_subject_mismatch")
    before = spec.get("base_sha")
    if before not in (old, tip):
        raise RepairBlocked("old_base_mismatch")
    return RepairPlan(
        str(repo), task, lane, branch, before, tip, tip, _digest(manifest), _digest(row), _digest(records), records[0]
    )


def plan_repair(
    *, repo: Path | str, task_ref: str, lane_id: str, expected_old_base: str, expected_tip: str
) -> RepairPlan:
    """Observe native state without writing any lane metadata."""
    from workbay_handoff_mcp.shared_schema import _get_db_connection
    from workbay_handoff_mcp.shared_write_context import acquire_flock

    repo = Path(repo).resolve()
    path = _path(repo, task_ref, lane_id)
    from workbay_orchestrator_mcp.lane_reaping import _lane_worker_lock_path

    lock_path = _lane_worker_lock_path(lane_id)
    if lock_path is None:
        raise RepairBlocked("worker_lock_path_unresolved")
    try:
        # Observe an existing lock without creating or stamping state in dry-run.
        with lock_path.open("r") as handle:
            acquire_flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquire_flock(handle.fileno(), fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise RepairBlocked("worker_lock_held_or_unavailable") from exc
    with _get_db_connection() as conn:
        row = _read_row(conn, task_ref, lane_id)
        manifest = json.loads(path.read_text())
        return _inspect(repo, task_ref, lane_id, expected_old_base, expected_tip, row, manifest)


def apply_repair(plan: RepairPlan, *, expected_old_base: str) -> RepairReceipt:
    """Apply only an unchanged plan, under native worker, manifest and DB locks."""
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    from workbay_orchestrator_mcp.lane_reaping import _acquire_lane_worker_lock, _release_lane_worker_lock

    repo = Path(plan.repo).resolve()
    path = _path(repo, plan.task_ref, plan.lane_id)
    handle, reason = _acquire_lane_worker_lock(plan.lane_id, claim="review_base_repair")
    if handle is None:
        raise RepairBlocked(reason)
    try:
        with ExitStack() as transactions:

            def mutate(manifest: dict[str, Any]) -> None:
                with _get_db_connection() as reader:
                    row = _read_row(reader, plan.task_ref, plan.lane_id)
                current = _inspect(repo, plan.task_ref, plan.lane_id, expected_old_base, plan.tip, row, manifest)
                if current != plan:
                    raise RepairBlocked("evidence_changed")
                # All subprocesses precede RESERVED. Hold the row reservation
                # through atomic publication, not merely through the callback.
                conn = transactions.enter_context(_get_db_connection(begin_immediate=True))
                if _digest(_read_row(conn, plan.task_ref, plan.lane_id)) != plan.row_digest:
                    raise RepairBlocked("evidence_changed")
                manifest["lanes"][plan.lane_id]["base_sha"] = plan.after

            lane_manifest.atomic_update_manifest(path, mutate)
    finally:
        _release_lane_worker_lock(handle)
    outcome = "already_correct" if plan.before == plan.after else "applied"
    # Audit failure cannot undo a published filesystem change: retain the true
    # outcome and explicitly report that the native audit needs attention.
    try:
        from workbay_handoff_mcp import record_decision

        result = record_decision(
            session="review_base_repair",
            decision="review_lane_procurement_base_repaired",
            rationale=json.dumps(asdict(RepairReceipt(outcome, plan)), sort_keys=True),
            task_ref=plan.task_ref,
            event_id="review-base-repair-" + _digest(asdict(plan)),
        )
        audit = "recorded" if result.get("ok") is True else "failed"
    except Exception:
        audit = "failed"
    return RepairReceipt(outcome, plan, audit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--expected-old-base", required=True)
    parser.add_argument("--expected-tip", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        from ..api import configure_runtime
        from ..cli import _build_config

        repo_root = Path(args.repo).expanduser().resolve()
        configure_runtime(_build_config(repo_root))
    except Exception as exc:
        print(json.dumps({"outcome": "blocked", "reason": f"runtime_configuration_failed: {exc}"}))
        return 1
    try:
        plan = plan_repair(
            repo=repo_root,
            task_ref=args.task_ref,
            lane_id=args.lane_id,
            expected_old_base=args.expected_old_base,
            expected_tip=args.expected_tip,
        )
        result = (
            apply_repair(plan, expected_old_base=args.expected_old_base)
            if args.apply
            else RepairReceipt("dry_run", plan)
        )
        print(json.dumps(asdict(result), sort_keys=True))
        return 0
    except (RepairBlocked, OSError, ValueError) as exc:
        print(json.dumps({"outcome": "blocked", "reason": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
