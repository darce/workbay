"""CLI entry point for the WorkBay Orchestrator MCP server.

Subcommands:
  serve / serve-stdio      Start the MCP server over stdio.
  doctor                   Print server diagnostics.
  tools-snapshot           Capture a normalized tools/list snapshot.
  orchestrator-start       Start the orchestrator daemon for a task.
  orchestrator-status      Print orchestrator daemon status.
  orchestrator-pause       Pause the orchestrator daemon.
  orchestrator-resume      Resume the orchestrator daemon.
  orchestrator-stop        Stop the orchestrator daemon.
  orchestrator-cycle       Run one orchestrator cycle synchronously.
  worker-start             Start a worker daemon for a specific lane.
  worker-status            Print worker daemon status for a lane.
  worker-stop              Stop a worker daemon for a lane.
  worker-resume            Resume a worker daemon for a lane.
  worker-start-all         Start worker daemons for all lanes in a task.
  worker-events            Print worker event history for a lane.
  dispatch                 Dispatch (upsert) work for a lane.
  lane-upsert              Upsert worktree lane metadata.
  lane-close               Close a worktree lane (terminal status transition).
  lane-list                List worktree lanes for a task.
  lane-activity            Read lane activity summary.
  lane-message             Record a lane message.
  lane-message-list        List lane messages.
  lane-message-update      Update lane message status.
  lane-report              Record a worker lane report.
  lane-report-list         List worker lane reports.
  lane-report-ack-backfill Backfill stranded submitted worker reports to acknowledged.
  lane-reap                Reap conclusive-dead non-terminal lane rows (dry-run by default).
                           --cross-session reaps preserved, clean, unowned lanes of an
                           in-progress task (exit 0 nothing to reclaim; exit 3 backlog).
  lane-dispose             Inspect and repair terminal lane branches (dry-run by default).
  lane-census              Census non-terminal remote lanes and plan typed repairs (dry-run by default).
  remote-sandbox-reap      Sweep remote VM sandboxes repo-wide (dry-run by default).
  pass-rescue              Inspect or recover one terminal offload pass.
  lane-reclaim-scan        Scan terminal on-disk lanes for reclaim candidates (optional nudge).
  orphan-reclaim           Reclaim merged branches with no live task or lane owner.
  branch-reclaim-release   Release a dead-lettered branch-reclaim queue item.
  list-backends            List available AI backends.
  metrics                  Print ACE metrics summary.
  ace-reflect              Apply pending ACE counter updates.
  ace-curation-report      Print ACE curation report.
  ace-metrics              Build full ACE metrics snapshot.
  ace-trends               Print ACE metrics sparklines.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from workbay_handoff_mcp import record_decision
from workbay_handoff_mcp.config import RuntimeConfig
from workbay_protocol import BRAND_NAME

from .api import (
    build_orchestrator_mcp,
    configure_runtime,
    dispatch_lane_work,
    get_metrics_summary,
    list_available_backends,
    manage_orchestrator,
    manage_worker,
    rescue_offload_pass,
    run_doctor,
    run_tools_snapshot,
)
from .lane_reaping import _branch_bundle_key, _bundle_dir
from .lanes import (
    _DEFAULT_BUNDLE_RETENTION_COUNT as DEFAULT_BUNDLE_RETENTION_COUNT,
)
from .lanes import (
    DEFAULT_BLOCKED_LANE_REAP_BATCH,
    backfill_worker_report_acks,
    get_lane_activity,
    lane_communication,
    manage_worktree_lane,
    reap_blocked_lanes,
    unreap_lane_branch,
    worker_reports,
)
from .lanes_support import _get_db_connection, _workspace_root
from .orchestration.branch_reclaim_delete import delete_authorized_branch
from .orchestration.branch_reclaim_queue import (
    branch_reclaim_dead_letter_release_decision_id,
    release_dead_letter_by_identity,
)
from .orchestration.lane_postmerge import reap_preserved_lanes_cross_session
from .orchestration.lane_reclaim import (
    NudgeFailure,
    lane_branch_reclaimable,
    nudge_reclaim_candidate,
    scan_terminal_lanes,
)
from .orchestration.lane_terminal_dispose import (
    BranchDisposition,
    classify_terminal_lane,
    dispose_terminal_lane,
)

# OBS-08 / REVHARD-EXIT-CODE-NOT-PINNED-01: incomplete scan must exit this exact
# nonzero code (distinct from clean-empty exit 0 and from generic handler error 1).
_LANE_RECLAIM_SCAN_DEGRADED_EXIT = 2
_ORPHAN_RECLAIM_DEGRADED_EXIT = 2
# Advisory: unintegrated backlog is not reclaimable; operators still need the signal.
_CROSS_SESSION_BACKLOG_EXIT = 3


def _positive_int_arg(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def _nonnegative_int_arg(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return value


def _build_config(
    workspace_root: Path,
    state_dir: Path | None = None,
    current_task_path: Path | None = None,
    exports_dir: Path | None = None,
) -> RuntimeConfig:
    return RuntimeConfig.for_repo(
        workspace_root,
        state_dir=state_dir,
        current_task_path=current_task_path,
        exports_dir=exports_dir,
    )


def _print_json(payload: Any) -> None:
    if isinstance(payload, dict):
        print(json.dumps(payload, indent=2))
        return
    print(payload)


def _emit_lane_payload(payload: Any) -> None:
    """Print lane-data JSON and propagate business-level failure to the shell."""
    _print_json(payload)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise SystemExit(1)


_LANE_DISPOSE_GIT_TIMEOUT_S = 20.0
_LANE_DISPOSE_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
_LANE_DISPOSE_SHA_LENGTH = 40


def _lane_dispose_git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a bounded read probe for the operator-only disposal surface."""

    command = ["git", "-C", str(root), *args]
    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=_LANE_DISPOSE_GIT_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return subprocess.CompletedProcess(command, 124, "", stderr or "git timeout")
    except OSError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))


def _lane_dispose_short_branch(branch: object) -> str:
    value = str(branch or "").strip()
    if value.startswith("refs/heads/"):
        return value.removeprefix("refs/heads/")
    return value


def _lane_dispose_sha(value: object) -> str | None:
    normalized = str(value or "").strip().lower()
    if len(normalized) != _LANE_DISPOSE_SHA_LENGTH or any(char not in "0123456789abcdef" for char in normalized):
        return None
    return normalized


def _lane_dispose_ref_sha(root: Path, ref: str) -> str | None:
    proc = _lane_dispose_git(root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if proc.returncode != 0:
        return None
    return _lane_dispose_sha(proc.stdout)


def _lane_dispose_unmerged_branches(root: Path, integration_ref: str) -> tuple[list[str], str | None]:
    proc = _lane_dispose_git(root, "branch", "--no-merged", integration_ref, "--format=%(refname:short)")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "branch listing failed").strip()
        return [], detail
    branches = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
    return branches, None


def _lane_dispose_notes(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _lane_dispose_review_subject(row: Mapping[str, Any]) -> Mapping[str, Any]:
    notes = _lane_dispose_notes(row.get("notes"))
    subject = notes.get("review_subject")
    return subject if isinstance(subject, Mapping) else {}


def _lane_dispose_receipt_has_evidence(
    state_dir: Path,
    task_ref: str,
    lane_id: str,
    *,
    recorded_only: bool = False,
) -> bool:
    """Read a recorded review receipt without treating an absent receipt as evidence."""

    if not state_dir.is_dir():
        return False
    for path in sorted(state_dir.rglob("review-receipt-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            continue
        if not isinstance(payload, Mapping):
            continue
        identity = payload.get("identity")
        kwargs = payload.get("kwargs")
        identity = identity if isinstance(identity, Mapping) else {}
        kwargs = kwargs if isinstance(kwargs, Mapping) else {}
        saved_task = identity.get("task_ref") or kwargs.get("task_ref")
        saved_lane = identity.get("lane_id") or kwargs.get("lane_id")
        if saved_task != task_ref or saved_lane != lane_id:
            continue
        if payload.get("status") == "recorded":
            return True
        if recorded_only:
            continue
        harvest = payload.get("harvest")
        if not isinstance(harvest, Mapping):
            continue
        dropped = harvest.get("dropped", 0)
        if harvest.get("status") == "recorded" and type(dropped) is int and dropped == 0:
            return True
        if harvest.get("reason") == "no_valid_findings" and type(dropped) is int and dropped == 0:
            return True
    return False


def _lane_dispose_review_findings_counts(conn: Any) -> dict[tuple[str, str], int]:
    try:
        rows = conn.execute(
            "SELECT task_ref, lane_id, COUNT(*) AS count FROM review_findings GROUP BY task_ref, lane_id"
        ).fetchall()
    except Exception:  # noqa: BLE001 - an absent/old findings table is no evidence
        return {}
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        task_ref = str(row["task_ref"] or "").strip()
        lane_id = str(row["lane_id"] or "").strip()
        if task_ref and lane_id:
            counts[(task_ref, lane_id)] = int(row["count"] or 0)
    return counts


def _lane_dispose_rows(task_ref: str | None = None) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    with _get_db_connection() as conn:
        if task_ref is None:
            rows = conn.execute("SELECT * FROM worktree_lanes ORDER BY id DESC").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? ORDER BY id DESC",
                (task_ref,),
            ).fetchall()
        findings = _lane_dispose_review_findings_counts(conn)
    return [dict(row) for row in rows], findings


def _lane_dispose_branch_tip(root: Path, row: Mapping[str, Any]) -> str | None:
    branch = _lane_dispose_short_branch(row.get("branch"))
    if branch:
        current = _lane_dispose_ref_sha(root, f"refs/heads/{branch}")
        if current is not None:
            return current
    return _lane_dispose_sha(row.get("branch_tip_sha"))


def _lane_dispose_ancestor(root: Path, tip_sha: str, integration_ref: str) -> bool | None:
    proc = _lane_dispose_git(root, "merge-base", "--is-ancestor", tip_sha, integration_ref)
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _lane_dispose_no_commits(root: Path, row: Mapping[str, Any], tip_sha: str) -> bool | None:
    subject = _lane_dispose_review_subject(row)
    base_ref = subject.get("base_ref") or row.get("review_base_ref")
    tip_ref = subject.get("tip_ref") or row.get("review_tip_ref")
    if not isinstance(base_ref, str) or not base_ref.strip():
        return None
    base_sha = _lane_dispose_ref_sha(root, base_ref.strip())
    if base_sha is None:
        return None
    observed_tip = tip_sha
    if isinstance(tip_ref, str) and tip_ref.strip():
        observed_tip = _lane_dispose_ref_sha(root, tip_ref.strip()) or observed_tip
    return base_sha == observed_tip


def _lane_dispose_worktree_live(root: Path, value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    try:
        return path.is_dir() or path.is_symlink()
    except OSError:
        return True


def _lane_dispose_bundle_path(
    root: Path,
    branch: str,
    tip_sha: str | None,
    bundle_dir: str | None = None,
) -> str | None:
    if tip_sha is None:
        return None
    try:
        directory = _bundle_dir(root, bundle_dir)
    except OSError:
        return None
    return str(directory / f"{_branch_bundle_key(branch)}-{tip_sha}.bundle")


def _lane_dispose_verdict(
    root: Path,
    row: Mapping[str, Any] | None,
    *,
    branch: str,
    integration_ref: str,
) -> Any:
    if row is None:
        return SimpleNamespace(
            reclaimable=False,
            reason="lane_row_missing",
            observed={
                "branch": branch,
                "survivor_refs": [],
                "production_paths": [],
                "unpreserved_paths": [],
            },
        )
    task_ref = str(row.get("task_ref") or "").strip()
    lane_id = str(row.get("lane_id") or "").strip()
    if not task_ref or not lane_id:
        return SimpleNamespace(
            reclaimable=False,
            reason="lane_identity_missing",
            observed={"branch": branch, "survivor_refs": []},
        )
    try:
        return lane_branch_reclaimable(
            orchestrator_root=root,
            task_ref=task_ref,
            lane_id=lane_id,
            integration_ref=integration_ref,
        )
    except Exception as exc:  # noqa: BLE001 - one unreadable row is a typed refusal
        return SimpleNamespace(
            reclaimable=False,
            reason="evaluator_failed",
            observed={"branch": branch, "survivor_refs": [], "error": str(exc)},
        )


def _lane_dispose_disposition(
    root: Path,
    row: Mapping[str, Any] | None,
    verdict: Any,
    *,
    branch: str,
    tip_sha: str | None,
    integration_ref: str,
    review_evidence: bool = False,
    review_findings_count: int | None = None,
) -> BranchDisposition:
    if row is None:
        return BranchDisposition.UNDECIDABLE
    is_ancestor = _lane_dispose_ancestor(root, tip_sha, integration_ref) if tip_sha else None
    no_commits = _lane_dispose_no_commits(root, row, tip_sha) if tip_sha else None
    return classify_terminal_lane(
        verdict,
        row,
        is_ancestor=is_ancestor,
        no_commits=no_commits,
        review_evidence=review_evidence,
        review_findings_count=review_findings_count,
        integration_ref=integration_ref,
        branch=branch,
    )


def _lane_dispose_survivor_refs(verdict: Any) -> list[str]:
    observed = getattr(verdict, "observed", {})
    raw = observed.get("survivor_refs") if isinstance(observed, Mapping) else None
    if isinstance(raw, Mapping):
        values = list(raw.keys())
    elif isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        values = []
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _lane_dispose_scan(
    *,
    root: Path,
    state_dir: Path,
    integration_ref: str,
    task_ref: str | None,
    bundle_dir: str | None,
) -> dict[str, Any]:
    rows, findings = _lane_dispose_rows(task_ref)
    by_branch: dict[str, dict[str, Any]] = {}
    for row in rows:
        branch = _lane_dispose_short_branch(row.get("branch"))
        if branch and branch not in by_branch:
            by_branch[branch] = row

    branches, listing_error = _lane_dispose_unmerged_branches(root, integration_ref)
    if listing_error is not None:
        return {
            "ok": False,
            "command": "lane-dispose scan",
            "error": "branch_list_failed",
            "detail": listing_error,
            "rows": [],
            "notice": "scan may write only additive refs/reclaimed/* pins and nothing else",
        }

    report_rows: list[dict[str, Any]] = []
    for branch in branches:
        row = by_branch.get(branch)
        tip_sha = _lane_dispose_ref_sha(root, f"refs/heads/{branch}")
        verdict = _lane_dispose_verdict(root, row, branch=branch, integration_ref=integration_ref)
        task = str(row.get("task_ref") or "") if row else ""
        lane_id = str(row.get("lane_id") or "") if row else ""
        evidence = _lane_dispose_receipt_has_evidence(state_dir, task, lane_id) if row else False
        finding_count = findings.get((task, lane_id), 0) if row else 0
        disposition = _lane_dispose_disposition(
            root,
            row,
            verdict,
            branch=branch,
            tip_sha=tip_sha,
            integration_ref=integration_ref,
            review_evidence=evidence,
            review_findings_count=finding_count,
        )
        observed = getattr(verdict, "observed", {})
        reason = getattr(verdict, "reason", None)
        report_rows.append(
            {
                "branch": branch,
                "tip_sha": tip_sha,
                "task_ref": task or None,
                "lane_id": lane_id or None,
                "disposition": str(disposition),
                "survivor_refs": _lane_dispose_survivor_refs(verdict),
                "bundle_path": _lane_dispose_bundle_path(root, branch, tip_sha, bundle_dir),
                "verdict_reason": reason,
                "reclaimable": getattr(verdict, "reclaimable", False) is True,
                "observed": dict(observed) if isinstance(observed, Mapping) else {},
            }
        )
    return {
        "ok": True,
        "command": "lane-dispose scan",
        "integration_ref": integration_ref,
        "rows": report_rows,
        "notice": "scan may write only additive refs/reclaimed/* pins and nothing else",
    }


def _print_lane_dispose_scan(payload: Mapping[str, Any], *, json_output: bool) -> None:
    if json_output:
        _print_json(dict(payload))
        return
    print("lane-dispose scan")
    print("notice=scan may write only additive refs/reclaimed/* pins and nothing else")
    if payload.get("ok") is not True:
        print(f"error={payload.get('error')} detail={payload.get('detail', '')}")
        return
    for row in payload.get("rows", []):
        survivor_refs = ",".join(row.get("survivor_refs") or []) or "-"
        print(
            f"branch={row.get('branch')} tip_sha={row.get('tip_sha') or '-'} "
            f"disposition={row.get('disposition')} survivor_refs={survivor_refs} "
            f"bundle_path={row.get('bundle_path') or '-'}"
        )


def _handle_lane_dispose_scan(args: argparse.Namespace, *, root: Path, state_dir: Path) -> None:
    payload = _lane_dispose_scan(
        root=root,
        state_dir=state_dir,
        integration_ref=args.integration_ref,
        task_ref=args.task_ref,
        bundle_dir=args.bundle_dir,
    )
    _print_lane_dispose_scan(payload, json_output=bool(args.json_output))
    if payload.get("ok") is not True:
        raise SystemExit(1)


def _lane_dispose_backfill_rows(
    *,
    root: Path,
    state_dir: Path,
    task_ref: str | None,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    rows, findings = _lane_dispose_rows(task_ref)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if row.get("lane_kind") != "review":
            continue
        if row.get("status") in _LANE_DISPOSE_TERMINAL_STATUSES:
            continue
        task = str(row.get("task_ref") or "").strip()
        lane_id = str(row.get("lane_id") or "").strip()
        finding_count = findings.get((task, lane_id), 0)
        has_receipt = _lane_dispose_receipt_has_evidence(
            state_dir, task, lane_id, recorded_only=True
        )
        candidate = dict(row)
        candidate["_has_receipt"] = has_receipt
        candidate["_finding_count"] = finding_count
        candidate["_eligible"] = has_receipt or finding_count > 0
        candidate["_worktree_live"] = _lane_dispose_worktree_live(root, row.get("worktree_path"))
        candidates.append(candidate)
    return candidates, findings


def _lane_dispose_backfill(
    *,
    root: Path,
    state_dir: Path,
    task_ref: str | None,
    apply: bool,
) -> dict[str, Any]:
    candidates, _findings = _lane_dispose_backfill_rows(root=root, state_dir=state_dir, task_ref=task_ref)
    report_rows: list[dict[str, Any]] = []
    refusals: list[dict[str, Any]] = []
    for row in candidates:
        task = str(row.get("task_ref") or "")
        lane_id = str(row.get("lane_id") or "")
        branch = _lane_dispose_short_branch(row.get("branch"))
        tip_sha = _lane_dispose_branch_tip(root, row)
        evidence = "recorded_receipt" if row["_has_receipt"] else "findings"
        item: dict[str, Any] = {
            "task_ref": task,
            "lane_id": lane_id,
            "branch": branch,
            "tip_sha": tip_sha,
            "finding_count": row["_finding_count"],
            "receipt_recorded": row["_has_receipt"],
            "status": row.get("status"),
        }
        if not row["_eligible"]:
            item["action"] = "skipped"
            item["reason"] = "review_evidence_missing"
            report_rows.append(item)
            continue
        if row["_worktree_live"]:
            refusal = {**item, "action": "refused", "reason": "live_worktree"}
            refusals.append(refusal)
            report_rows.append(refusal)
            continue
        if tip_sha is None:
            refusal = {**item, "action": "refused", "reason": "branch_tip_unavailable"}
            refusals.append(refusal)
            report_rows.append(refusal)
            continue
        item["evidence"] = evidence
        if not apply:
            item["action"] = "would_close_and_enqueue"
            report_rows.append(item)
            continue

        close = manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            status="closed",
            notes=f"lane-dispose backfill: {evidence}",
            task_ref=task,
            branch_tip_sha=tip_sha,
        )
        if not isinstance(close, Mapping) or close.get("ok") is not True:
            refusal = {**item, "action": "refused", "reason": "close_failed", "detail": close}
            refusals.append(refusal)
            report_rows.append(refusal)
            continue
        queued = dispose_terminal_lane(
            task,
            lane_id,
            branch,
            tip_sha=tip_sha,
            outcome="review_complete",
        )
        if not queued:
            refusal = {**item, "action": "refused", "reason": "queue_insert_failed"}
            refusals.append(refusal)
            report_rows.append(refusal)
            continue
        item["action"] = "closed_and_enqueued"
        report_rows.append(item)
    return {
        "ok": not refusals,
        "command": "lane-dispose backfill",
        "apply": apply,
        "rows": report_rows,
        "refusals": refusals,
    }


def _print_lane_dispose_backfill(payload: Mapping[str, Any], *, json_output: bool) -> None:
    if json_output:
        _print_json(dict(payload))
        return
    print(f"lane-dispose backfill apply={payload.get('apply')}")
    for row in payload.get("rows", []):
        print(
            f"task_ref={row.get('task_ref')} lane_id={row.get('lane_id')} "
            f"action={row.get('action')} reason={row.get('reason', '-')}"
        )
    for refusal in payload.get("refusals", []):
        print(f"refusal lane_id={refusal.get('lane_id')} reason={refusal.get('reason')}")


def _handle_lane_dispose_backfill(args: argparse.Namespace, *, root: Path, state_dir: Path) -> None:
    payload = _lane_dispose_backfill(
        root=root,
        state_dir=state_dir,
        task_ref=args.task_ref,
        apply=bool(args.apply),
    )
    _print_lane_dispose_backfill(payload, json_output=bool(args.json_output))
    if payload.get("ok") is not True:
        raise SystemExit(1)


def _lane_dispose_find_lane(*, lane_id: str, task_ref: str | None) -> tuple[dict[str, Any] | None, str | None]:
    with _get_db_connection() as conn:
        if task_ref is not None:
            rows = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref = ? AND lane_id = ? ORDER BY id DESC",
                (task_ref, lane_id),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM worktree_lanes WHERE lane_id = ? ORDER BY id DESC",
                (lane_id,),
            ).fetchall()
    if not rows:
        return None, "lane_not_found"
    if task_ref is None and len(rows) > 1:
        return None, "lane_id_ambiguous_requires_task_ref"
    return dict(rows[0]), None


def _lane_dispose_record_abandon(row: Mapping[str, Any], tip_sha: str) -> dict[str, Any]:
    task_ref = str(row.get("task_ref") or "")
    lane_id = str(row.get("lane_id") or "")
    branch = _lane_dispose_short_branch(row.get("branch"))
    digest = hashlib.sha256(f"{task_ref}:{lane_id}:{tip_sha}".encode("utf-8")).hexdigest()[:16]
    try:
        return record_decision(
            session="lane-dispose-operator",
            decision=f"lane_dispose_abandon_{digest}",
            rationale=(
                "Operator accepted unique work loss for lane-dispose abandon; "
                f"lane={lane_id} branch={branch} tip_sha={tip_sha} "
                "confirmation=--i-accept-unique-work-loss."
            ),
            actor={
                "agent": "operator",
                "branch": branch,
                "commit_sha": tip_sha,
                "lane_id": lane_id,
            },
            task_ref=task_ref,
            event_id=f"lane-dispose-abandon:{task_ref}:{lane_id}:{tip_sha}",
            decision_origin="system",
            refresh_rationale_on_conflict=True,
        )
    except Exception as exc:  # noqa: BLE001 - provenance failure blocks deletion
        return {"ok": False, "error": f"operator_provenance_failed: {exc}"}


def _lane_dispose_delete_payload(result: Any) -> dict[str, Any]:
    if isinstance(result, Mapping):
        return dict(result)
    fields = (
        "deleted",
        "reason",
        "branch",
        "authorized_sha",
        "reclaim_ref",
        "detail",
        "recovery_config",
        "disposition",
    )
    return {field: getattr(result, field, None) for field in fields}


def _handle_lane_dispose_abandon(args: argparse.Namespace, *, root: Path) -> None:
    lane_id = str(args.lane_id or "").strip()
    task_ref = args.task_ref.strip() if isinstance(args.task_ref, str) and args.task_ref.strip() else None
    if not lane_id:
        _emit_lane_payload({"ok": False, "command": "lane-dispose abandon", "reason": "blank_lane_id"})
        return
    row, lookup_error = _lane_dispose_find_lane(lane_id=lane_id, task_ref=task_ref)
    if row is None:
        _emit_lane_payload({"ok": False, "command": "lane-dispose abandon", "reason": lookup_error})
        return
    if not args.i_accept_unique_work_loss:
        _emit_lane_payload(
            {
                "ok": False,
                "command": "lane-dispose abandon",
                "lane_id": lane_id,
                "reason": "confirmation_required",
                "detail": "pass --i-accept-unique-work-loss to authorize unique-work loss",
                "deleted": False,
            }
        )
        return
    branch = _lane_dispose_short_branch(row.get("branch"))
    tip_sha = _lane_dispose_branch_tip(root, row)
    if not branch or tip_sha is None:
        _emit_lane_payload(
            {
                "ok": False,
                "command": "lane-dispose abandon",
                "lane_id": lane_id,
                "reason": "branch_identity_unavailable",
                "deleted": False,
            }
        )
        return
    provenance = _lane_dispose_record_abandon(row, tip_sha)
    if provenance.get("ok") is not True:
        _emit_lane_payload(
            {
                "ok": False,
                "command": "lane-dispose abandon",
                "lane_id": lane_id,
                "reason": "operator_provenance_failed",
                "provenance": provenance,
                "deleted": False,
            }
        )
        return
    # The actuator owns the fresh C1-C7 proof, pin, bundle, and expected-tip
    # delete. The CLI deliberately does not reproduce its env allow-list.
    result = delete_authorized_branch(
        orchestrator_root=root,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=tip_sha,
        apply=True,
        task_ref=str(row.get("task_ref") or task_ref or ""),
        integration_ref=args.integration_ref,
    )
    result_payload = _lane_dispose_delete_payload(result)
    payload = {
        "ok": bool(result_payload.get("deleted")),
        "command": "lane-dispose abandon",
        "lane_id": lane_id,
        "provenance": provenance,
        "result": result_payload,
    }
    _print_json(payload)
    if payload["ok"] is not True:
        raise SystemExit(1)


def _handle_lane_reap_cross_session(args: argparse.Namespace) -> None:
    """Print the cross-session reap report; exit 0 / 3 as documented in --help."""
    if args.task_ref is not None and args.task_ref.strip() == "":
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "blank_task_ref: --task-ref was provided but empty/whitespace; "
                    "an empty scope would silently widen the reap. "
                    "--cross-session requires a real task_ref."
                ),
            }
        )
        return
    if args.task_ref is None or not str(args.task_ref).strip():
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "cross_session_requires_task_ref: --cross-session needs a non-blank "
                    "--task-ref for the in-progress task whose preserved lanes should be reaped."
                ),
            }
        )
        return
    if args.max_batch <= 0:
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "non_positive_max_batch: --max-batch must be greater than zero; "
                    "a non-positive batch would silently widen or skip the reap."
                ),
            }
        )
        return
    hours = 0.0 if args.min_age_hours < 0 else float(args.min_age_hours)
    payload = reap_preserved_lanes_cross_session(
        task_ref=args.task_ref,
        root=args.workspace_root,
        integration_refs=args.integration_refs,
        apply=bool(args.apply),
        max_batch=args.max_batch,
        age_floor_seconds=int(round(hours * 3600)),
        orchestrator_root=args.workspace_root,
    )
    _print_json(payload)
    if not isinstance(payload, dict):
        raise SystemExit(1)
    if payload.get("ok") is False or payload.get("error"):
        raise SystemExit(1)
    probe_failures = payload.get("probe_failures")
    if isinstance(probe_failures, int) and probe_failures > 0:
        raise SystemExit(1)
    backlog = payload.get("backlog") if isinstance(payload.get("backlog"), list) else []
    unintegrated = payload.get("unintegrated_backlog")
    count_raw = unintegrated.get("count") if isinstance(unintegrated, dict) else 0
    try:
        backlog_count = int(count_raw or 0)
    except (TypeError, ValueError):
        backlog_count = 0
    if backlog or backlog_count > 0:
        raise SystemExit(_CROSS_SESSION_BACKLOG_EXIT)


def _handle_lane_reclaim_scan(
    *,
    orchestrator_root: Path,
    task_ref: str | None,
    all_tasks: bool,
    lane_id: str | None,
    limit: int | None,
    offset: int,
) -> None:
    """Run scan_terminal_lanes and optional nudge_reclaim_candidate.

    Never-raise wrapper (RES-07/AGT-10): the whole post-validation body is under
    one try/except so non-conforming returns and symbol faults become an
    ``ok: false`` JSON report and exit 1 (no traceback). Incomplete scans
    (``ScanResult.complete`` is False) exit with the pinned degraded code 2
    (OBS-08), distinct from generic error exit 1. Clean complete scans exit 0.
    """
    # Blank/whitespace --task-ref refused before any symbol call (mirror lane-reap).
    if task_ref is not None and task_ref.strip() == "":
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "blank_task_ref: --task-ref was provided but empty/whitespace; "
                    "an empty scope is not a valid reclaim scan. "
                    "Pass a real task_ref."
                ),
            }
        )
        return

    if all_tasks and lane_id is not None:
        _emit_lane_payload({"ok": False, "error": "all_tasks_lane_id: --lane-id requires a scoped --task-ref scan."})
        return

    # Blank/whitespace --lane-id refused the same way when the flag is present.
    if lane_id is not None and lane_id.strip() == "":
        _emit_lane_payload(
            {
                "ok": False,
                "error": (
                    "blank_lane_id: --lane-id was provided but empty/whitespace; "
                    "an empty lane_id is not a valid reclaim nudge. "
                    "Pass a real lane_id or omit the flag."
                ),
            }
        )
        return

    # Whole post-validation body under never-raise (REAPCONV-RECLAIMWIRE-NEVERRAISE-WRAP-INCOMPLETE-01):
    # scan, complete/candidate access, iteration, and nudge result rendering.
    # SystemExit (degraded=2 or _emit_lane_payload=1) is BaseException — not caught here.
    try:

        def _progress(rows_done: int, rows_total: int | None = None) -> None:
            total = "?" if rows_total is None else str(rows_total)
            print(f"lane-reclaim-scan progress rows={rows_done}/{total}", file=sys.stderr)

        scan = scan_terminal_lanes(
            orchestrator_root=orchestrator_root,
            limit=limit,
            offset=offset,
            progress=_progress,
            **({"all_tasks": True} if all_tasks else {"task_ref": task_ref}),
        )

        if not scan.complete:
            reason = scan.refusal_reason if scan.refusal_reason is not None else "scan_incomplete"
            print(f"refusal_reason={reason}")
            raise SystemExit(_LANE_RECLAIM_SCAN_DEGRADED_EXIT)

        print(f"candidates={len(scan)}")
        # Per-lane faults do not flip complete (OBS-08 / PARTIAL-SCAN-FIELDS): surface counts.
        failed_n = len(scan.failed_lanes)
        unrecorded_n = len(scan.unrecorded_lanes)
        if failed_n or unrecorded_n:
            print(f"failed_lanes={failed_n} unrecorded_lanes={unrecorded_n}")

        for cand in scan:
            verdict = cand.verdict
            print(
                f"candidate lane_id={cand.lane_id} "
                f"reclaimable={verdict.reclaimable} "
                f"reason={verdict.reason} "
                f"recorded={cand.recorded}"
            )
            branch_verdict = cand.branch_verdict
            if branch_verdict is not None:
                print(
                    f"branch_candidate task_ref={cand.task_ref} lane_id={cand.lane_id} "
                    f"reclaimable={branch_verdict.reclaimable} "
                    f"reason={branch_verdict.reason} "
                    f"recorded={cand.branch_recorded}"
                )

        if lane_id is None:
            return

        nudged = nudge_reclaim_candidate(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
        )

        if nudged is None:
            print(f"nudge lane_id={lane_id} not-a-candidate")
        elif isinstance(nudged, NudgeFailure):
            print(f"nudge lane_id={nudged.lane_id} failure reason={nudged.reason}")
        else:
            verdict = nudged.verdict
            print(
                f"nudge lane_id={nudged.lane_id} "
                f"reclaimable={verdict.reclaimable} "
                f"reason={verdict.reason} "
                f"recorded={nudged.recorded}"
            )
    except Exception as exc:  # noqa: BLE001 — CLI never-raise surface
        # Unify error surface with blank-flag refusal: JSON ok:false, exit 1.
        _emit_lane_payload(
            {
                "ok": False,
                "error": f"lane-reclaim-scan error: {exc}",
            }
        )


def _handle_orphan_reclaim(
    *,
    orchestrator_root: Path,
    state_dir: Path,
    apply: bool,
    integration_ref: str,
    json_output: bool,
) -> None:
    """Run the orphan branch scan without allowing an exception traceback."""

    try:
        from .orchestration.orphan_reclaim import (  # noqa: PLC0415
            apply_orphan_reclaim,
            scan_orphan_branches,
        )

        scan = scan_orphan_branches(orchestrator_root, integration_ref=integration_ref)
        reclaim = apply_orphan_reclaim(scan, apply=apply, state_dir=state_dir)
        payload: dict[str, Any] = {
            "ok": bool(reclaim.get("ok", True)) and not scan.error and not scan.unknown_rows,
            "command": "orphan-reclaim",
            "apply": apply,
            "integration_ref": integration_ref,
            "orphan_scan_summary": scan.summary(),
            "scan": scan.to_dict(),
            "reclaim": reclaim,
        }
        if json_output:
            _print_json(payload)
        else:
            print(f"orphan-reclaim apply={apply} integration_ref={integration_ref}")
            for classification, count in scan.summary().items():
                print(f"{classification}: {count}")
            if reclaim.get("applied"):
                print(f"applied: {', '.join(reclaim['applied'])}")
            if reclaim.get("errors"):
                print(f"errors: {reclaim['errors']}")
        if scan.error or scan.unknown_rows:
            raise SystemExit(_ORPHAN_RECLAIM_DEGRADED_EXIT)
        if reclaim.get("errors"):
            raise SystemExit(1)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - CLI never-raise contract
        payload = {
            "ok": False,
            "command": "orphan-reclaim",
            "error": f"{type(exc).__name__}: {exc}",
        }
        if json_output:
            _print_json(payload)
        else:
            print(payload["error"], file=sys.stderr)
        raise SystemExit(1) from None


def _resolve_playbook_paths(paths: list[str] | None) -> list[str]:
    """Resolve ACE playbook declarations: explicit --playbook-file wins, else fall
    back to the canonical WORKBAY_ACE_PLAYBOOK_FILES env var.

    The env var is read through ``workbay_protocol.resolve_env_alias`` so it is
    the single canonical resolution seam (blank == unset) and renames in lockstep
    with every other resolve_env_alias call site under the WorkBay rebrand
    (implementation note). ``Makefile.d/ace.mk`` still expands the same declaration into
    explicit --playbook-file flags for the operator surface.
    """
    if paths:
        return paths
    from workbay_protocol import resolve_env_alias  # noqa: PLC0415

    declared = resolve_env_alias("WORKBAY_ACE_PLAYBOOK_FILES", default="")
    return [token for token in declared.split() if token]


def _coerce_playbook_paths(raw_paths: list[str], workspace_root: Path) -> list[Path]:
    resolved: list[Path] = []
    for path in raw_paths:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = workspace_root / candidate
        resolved.append(candidate)
    return resolved


def _validated_playbook_files(command: str, paths: list[str] | None, workspace_root: Path) -> list[Path]:
    from workbay_orchestrator_mcp.orchestration.ace_reflect import (  # noqa: PLC0415
        PlaybookValidationError,
        validate_playbook_files,
    )

    playbook_files = _coerce_playbook_paths(_resolve_playbook_paths(paths), workspace_root)
    try:
        validate_playbook_files(playbook_files)
    except PlaybookValidationError as exc:
        print(f"{command}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    return playbook_files


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp-workbay-orchestrator",
        description=f"{BRAND_NAME} Orchestrator MCP server.",
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=Path.cwd(),
        help="Workspace root directory (default: cwd).",
    )
    parser.add_argument(
        "--state-dir", type=Path, default=None, help="State directory (default: <workspace-root>/.task-state)."
    )
    parser.add_argument(
        "--current-task-path",
        type=Path,
        default=None,
        help="CURRENT_TASK.json path (default: <workspace-root>/CURRENT_TASK.json).",
    )
    parser.add_argument(
        "--exports-dir", type=Path, default=None, help="Exports directory (default: <state-dir>/exports)."
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- serve ---
    subparsers.add_parser("serve", help="Start the MCP server (default).")
    subparsers.add_parser("serve-stdio", help="Start the MCP server over stdio (alias for serve).")

    # --- doctor ---
    doctor_p = subparsers.add_parser("doctor", help="Print server diagnostics.")
    doctor_p.add_argument("--json", dest="json_output", action="store_true")

    # --- tools snapshot ---
    snapshot_p = subparsers.add_parser("tools-snapshot", help="Capture a normalized tools/list snapshot.")
    snapshot_p.add_argument("--output", type=Path, default=None)
    snapshot_p.add_argument("--json", dest="json_output", action="store_true")

    # --- orchestrator daemon ---
    ostart = subparsers.add_parser("orchestrator-start", help="Start the orchestrator daemon.")
    ostart.add_argument("--task-ref", required=True)
    ostart.add_argument("--backend", default="codex-cli")
    ostart.add_argument("--poll-interval", type=int, default=60)
    ostart.add_argument("--single-pass", action="store_true", default=False)
    ostart.add_argument("--worker-start-mode", default="mcp")
    ostart.add_argument("--worker-reasoning-effort", default="auto")
    ostart.add_argument("--model", default=None)

    subparsers.add_parser("orchestrator-status", help="Print orchestrator daemon status.")
    subparsers.add_parser("orchestrator-pause", help="Pause the orchestrator daemon.")
    subparsers.add_parser("orchestrator-resume", help="Resume the orchestrator daemon.")

    ostop = subparsers.add_parser("orchestrator-stop", help="Stop the orchestrator daemon.")
    ostop.add_argument("--force", action="store_true", default=False)
    ostop.add_argument("--wait", type=float, default=5.0, dest="wait_seconds")

    ocycle = subparsers.add_parser("orchestrator-cycle", help="Run one orchestrator cycle synchronously.")
    ocycle.add_argument("--task-ref", required=True)
    ocycle.add_argument("--backend", default="codex-cli")
    ocycle.add_argument("--dry-run", action="store_true", default=False)
    ocycle.add_argument("--timeout", type=float, default=300.0, dest="timeout_seconds")
    ocycle.add_argument("--worker-start-mode", default="mcp")
    ocycle.add_argument("--worker-reasoning-effort", default="auto")
    ocycle.add_argument("--model", default=None)

    # --- worker daemon ---
    wstart = subparsers.add_parser("worker-start", help="Start a worker daemon for a lane.")
    wstart.add_argument("--task-ref", required=True)
    wstart.add_argument("--lane-id", required=True)
    wstart.add_argument("--backend", default="codex-subagent")
    wstart.add_argument("--poll-interval", type=int, default=30)
    wstart.add_argument("--single-pass", action="store_true", default=False)
    wstart.add_argument("--session", default=None)
    wstart.add_argument("--session-mode", default="fresh_turn")
    wstart.add_argument("--reasoning-effort", default="inherit")
    wstart.add_argument("--model", default=None)

    wstatus = subparsers.add_parser("worker-status", help="Print worker daemon status for a lane.")
    wstatus.add_argument("--task-ref", required=True)
    wstatus.add_argument("--lane-id", required=True)

    wstop = subparsers.add_parser("worker-stop", help="Stop a worker daemon for a lane.")
    wstop.add_argument("--task-ref", required=True)
    wstop.add_argument("--lane-id", required=True)
    wstop.add_argument("--force", action="store_true", default=False)

    wresume = subparsers.add_parser("worker-resume", help="Resume a worker daemon for a lane.")
    wresume.add_argument("--task-ref", required=True)
    wresume.add_argument("--lane-id", required=True)

    wall = subparsers.add_parser("worker-start-all", help="Start worker daemons for all lanes in a task.")
    wall.add_argument("--task-ref", required=True)
    wall.add_argument("--backend", default="codex-subagent")
    wall.add_argument("--poll-interval", type=int, default=30)
    wall.add_argument("--single-pass", action="store_true", default=False)
    wall.add_argument("--session-mode", default="fresh_turn")
    wall.add_argument("--reasoning-effort", default="inherit")
    wall.add_argument("--model", default=None)

    wevents = subparsers.add_parser("worker-events", help="Print worker event history for a lane.")
    wevents.add_argument("--task-ref", required=True)
    wevents.add_argument("--lane-id", required=True)
    wevents.add_argument("--limit", type=int, default=50)
    wevents.add_argument("--event-name", default=None)

    # --- dispatch ---
    dispatch_p = subparsers.add_parser("dispatch", help="Dispatch (upsert) work for a lane.")
    dispatch_p.add_argument("--lane-id", required=True)
    dispatch_p.add_argument("--task-ref", default=None)
    dispatch_p.add_argument("--model", default=None)
    dispatch_p.add_argument("--backend", default=None)
    dispatch_p.add_argument("--reasoning-effort", default=None)
    dispatch_p.add_argument("--start-worker", action="store_true", default=False)

    # --- lane data (bash-callable adapters over lanes.py) ---
    lane_upsert_p = subparsers.add_parser("lane-upsert", help="Upsert worktree lane metadata.")
    lane_upsert_p.add_argument("--lane-id", required=True)
    lane_upsert_p.add_argument("--worktree-path", required=True)
    lane_upsert_p.add_argument("--branch", required=True)
    lane_upsert_p.add_argument("--owner-agent", default=None)
    lane_upsert_p.add_argument("--status", default="planned")
    lane_upsert_p.add_argument("--title", default=None)
    lane_upsert_p.add_argument("--objective", default=None)
    lane_upsert_p.add_argument("--notes", default=None)
    lane_upsert_p.add_argument("--task-ref", default=None)

    lane_close_p = subparsers.add_parser(
        "lane-close",
        help="Close a worktree lane (terminal status via close operation).",
    )
    lane_close_p.add_argument("--lane-id", required=True)
    lane_close_p.add_argument("--status", default="closed")
    lane_close_p.add_argument("--notes", default=None)
    lane_close_p.add_argument("--task-ref", default=None)

    lane_list_p = subparsers.add_parser("lane-list", help="List worktree lanes.")
    lane_list_p.add_argument("--task-ref", default=None)
    lane_list_p.add_argument("--status", default="all")
    lane_list_p.add_argument("--limit", type=int, default=100)
    lane_list_p.add_argument("--offset", type=int, default=0)

    lane_activity_p = subparsers.add_parser("lane-activity", help="Read lane activity summary.")
    lane_activity_p.add_argument("--lane-id", required=True)
    lane_activity_p.add_argument("--task-ref", default=None)

    lane_message_p = subparsers.add_parser("lane-message", help="Record a lane message.")
    lane_message_p.add_argument("--task-ref", default=None)
    lane_message_p.add_argument("--lane-id", required=True)
    lane_message_p.add_argument("--session", required=True)
    lane_message_p.add_argument("--direction", required=True)
    lane_message_p.add_argument("--message", required=True)
    lane_message_p.add_argument("--status", default="open")
    lane_message_p.add_argument("--subject", default=None)

    lane_message_list_p = subparsers.add_parser("lane-message-list", help="List lane messages.")
    lane_message_list_p.add_argument("--task-ref", default=None)
    lane_message_list_p.add_argument("--lane-id", default=None)
    lane_message_list_p.add_argument("--status", default="all")
    lane_message_list_p.add_argument("--limit", type=int, default=20)
    lane_message_list_p.add_argument("--offset", type=int, default=0)

    lane_message_update_p = subparsers.add_parser("lane-message-update", help="Update lane message status.")
    lane_message_update_p.add_argument("--task-ref", default=None)
    lane_message_update_p.add_argument("--message-id", type=int, required=True)
    lane_message_update_p.add_argument("--status", required=True)

    lane_report_p = subparsers.add_parser("lane-report", help="Record a worker lane report.")
    lane_report_p.add_argument("--task-ref", default=None)
    lane_report_p.add_argument("--lane-id", required=True)
    lane_report_p.add_argument("--session", required=True)
    lane_report_p.add_argument(
        "--delivery-id",
        default=None,
        help=(
            "Stable delivery identifier; it must be unique across all lanes and tasks. "
            "Re-submitting the same identifier is an idempotent replay of the original report."
        ),
    )
    lane_report_p.add_argument("--summary", required=True)
    lane_report_p.add_argument("--status", default="submitted")
    lane_report_p.add_argument("--outcome", default=None)
    lane_report_p.add_argument("--merge-ready", action="store_true", default=False)
    lane_report_p.add_argument("--changed-file", action="append", default=None)
    lane_report_p.add_argument("--test-command", action="append", default=None)
    lane_report_p.add_argument("--blocker", action="append", default=None)

    lane_report_list_p = subparsers.add_parser("lane-report-list", help="List worker lane reports.")
    lane_report_list_p.add_argument("--task-ref", default=None)
    lane_report_list_p.add_argument("--lane-id", default=None)
    lane_report_list_p.add_argument("--limit", type=int, default=20)
    lane_report_list_p.add_argument("--offset", type=int, default=0)

    lane_report_ack_backfill_p = subparsers.add_parser(
        "lane-report-ack-backfill",
        help="Backfill stranded status=submitted worker reports to acknowledged (CAS, idempotent).",
    )
    lane_report_ack_backfill_p.add_argument("--task-ref", default=None)
    lane_report_ack_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Count stranded rows without updating.",
    )
    lane_report_ack_backfill_p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max rows to process (default: all).",
    )

    lane_reap_p = subparsers.add_parser(
        "lane-reap",
        help="Reap conclusive-dead non-terminal lane rows (dry-run by default).",
    )
    lane_reap_p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="CAS-close conclusive-dead rows (default is dry-run report only).",
    )
    lane_reap_p.add_argument(
        "--task-ref",
        default=None,
        help="Scope candidates and closes to one task_ref (default: repo-wide).",
    )
    lane_reap_p.add_argument(
        "--max-batch",
        type=int,
        default=DEFAULT_BLOCKED_LANE_REAP_BATCH,
        help=f"Max candidate rows per invocation (default: {DEFAULT_BLOCKED_LANE_REAP_BATCH}).",
    )
    lane_reap_p.add_argument(
        "--min-age-hours",
        type=float,
        default=24.0,
        help="Grace floor for non-blocked candidates in hours (default: 24.0).",
    )
    lane_reap_p.add_argument(
        "--bundle-retention-count",
        type=int,
        default=DEFAULT_BUNDLE_RETENTION_COUNT,
        help=(
            "Rollback bundles kept per reaped branch "
            f"(default: {DEFAULT_BUNDLE_RETENTION_COUNT}; <=0 keeps every bundle)."
        ),
    )

    def _nonblank_bundle_dir(value: str) -> str:
        if not value.strip():
            raise argparse.ArgumentTypeError("blank_bundle_dir: --bundle-dir must be a nonblank archive path")
        return value

    lane_reap_p.add_argument(
        "--bundle-dir",
        type=_nonblank_bundle_dir,
        default=None,
        help="Rollback bundle archive directory (overrides WORKBAY_BUNDLE_DIR and the repository default).",
    )
    lane_reap_p.add_argument(
        "--reclaim-worktrees",
        action="store_true",
        default=False,
        help=(
            "Operator opt-in: attempt guarded git worktree remove for the "
            "merged-branch/live-worktree deadlock (default off; daemon never "
            "sets this)."
        ),
    )
    lane_reap_p.add_argument(
        "--cross-session",
        action="store_true",
        default=False,
        help=(
            "Reap preserved, clean, unowned lanes of an in-progress task "
            "(keyed to lane completion, not task finish). Prints would_reclaim / "
            "backlog / kept with reasons. Exit 0 when nothing to reclaim; "
            "exit 3 when unintegrated backlog is non-empty (advisory)."
        ),
    )
    lane_reap_p.add_argument(
        "--integration-ref",
        action="append",
        dest="integration_refs",
        default=None,
        help=(
            "Integration ref treated as a preservation target (repeatable). "
            "Default: main plus the task manifest integration_branch."
        ),
    )

    lane_dispose_p = subparsers.add_parser(
        "lane-dispose",
        help="Scan, backfill, or explicitly abandon terminal lane branches (dry-run by default).",
    )
    lane_dispose_subparsers = lane_dispose_p.add_subparsers(dest="dispose_command", required=True)

    lane_dispose_scan_p = lane_dispose_subparsers.add_parser(
        "scan",
        help="Classify every local branch not merged into the integration ref without deleting refs.",
    )
    lane_dispose_scan_p.add_argument("--task-ref", default=None, help="Limit lane evidence to one task_ref.")
    lane_dispose_scan_p.add_argument("--integration-ref", default="main")
    lane_dispose_scan_p.add_argument("--bundle-dir", default=None, help="Override the rollback bundle directory.")
    lane_dispose_scan_p.add_argument("--json", dest="json_output", action="store_true", default=False)

    lane_dispose_backfill_p = lane_dispose_subparsers.add_parser(
        "backfill",
        help="Backfill review-evidence rows (dry-run unless --apply is explicit).",
    )
    lane_dispose_backfill_mode = lane_dispose_backfill_p.add_mutually_exclusive_group()
    lane_dispose_backfill_mode.add_argument("--dry-run", action="store_true", default=False)
    lane_dispose_backfill_mode.add_argument("--apply", action="store_true", default=False)
    lane_dispose_backfill_p.add_argument("--task-ref", default=None, help="Limit review rows to one task_ref.")
    lane_dispose_backfill_p.add_argument("--json", dest="json_output", action="store_true", default=False)

    lane_dispose_abandon_p = lane_dispose_subparsers.add_parser(
        "abandon",
        help="Explicitly abandon a lane only after accepting unique-work loss.",
    )
    lane_dispose_abandon_p.add_argument("--lane", dest="lane_id", required=True)
    lane_dispose_abandon_p.add_argument("--task-ref", default=None)
    lane_dispose_abandon_p.add_argument("--integration-ref", default="main")
    lane_dispose_abandon_p.add_argument(
        "--i-accept-unique-work-loss",
        action="store_true",
        default=False,
        help="Required acknowledgement before the fresh-proof actuator may run.",
    )

    lane_unreap_p = subparsers.add_parser(
        "lane-unreap",
        help="Restore a reaped branch from its verified rollback bundle.",
    )
    lane_unreap_p.add_argument(
        "branch",
        help="Branch name to restore; refused if the ref already exists.",
    )
    lane_unreap_p.add_argument(
        "--bundle-path",
        default=None,
        help="Restore from this bundle instead of the branch's newest one.",
    )
    lane_unreap_p.add_argument(
        "--repo-root",
        default=None,
        help="Repository to restore into (default: the current workspace root).",
    )

    branch_reclaim_release_p = subparsers.add_parser(
        "branch-reclaim-release",
        help="Release a dead-lettered branch-reclaim queue item after the cause is repaired.",
    )
    branch_reclaim_release_p.add_argument("--task-ref", required=True)
    branch_reclaim_release_p.add_argument("--lane-id", required=True)
    branch_reclaim_release_p.add_argument(
        "--sha",
        required=True,
        help="Full 40-hex authorized SHA (prefix match is refused).",
    )
    branch_reclaim_release_p.add_argument(
        "--decision",
        required=True,
        help="Operator rationale for the release; blank/whitespace is refused.",
    )

    lane_census_p = subparsers.add_parser(
        "lane-census",
        help="Census non-terminal remote lanes and plan typed repairs (dry-run by default).",
    )
    lane_census_p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply typed repairs and reap harvested remote sandboxes (default is dry-run report only).",
    )
    lane_census_p.add_argument(
        "--task-ref",
        required=False,
        help="Task to census (default: all tasks in this repository).",
    )
    lane_census_p.add_argument(
        "--max-batch",
        type=int,
        default=50,
        help="Max repair-eligible rows per invocation (default: 50). Observe-only sinks fill spare slots.",
    )
    lane_census_p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=False,
        help="Emit the census report as JSON (always on for this command; accepted for parser parity).",
    )

    remote_reap_p = subparsers.add_parser(
        "remote-sandbox-reap",
        help="Sweep remote VM lane sandboxes repo-wide (dry-run by default).",
    )
    remote_reap_p.add_argument("--task-ref", required=False, help="Decision task (default: active task).")
    remote_reap_p.add_argument("--apply", action="store_true", default=False, help="Delete eligible sandboxes.")
    remote_reap_p.add_argument("--idle-seconds", type=int, default=None, help="Override the VM idle threshold.")
    remote_reap_p.add_argument("--json", dest="json_output", action="store_true", default=False)

    lane_reclaim_scan_p = subparsers.add_parser(
        "lane-reclaim-scan",
        help="Scan terminal on-disk lanes for reclaim candidates; optional single-lane nudge.",
    )
    lane_reclaim_scope = lane_reclaim_scan_p.add_mutually_exclusive_group(required=True)
    lane_reclaim_scope.add_argument(
        "--task-ref",
        help="Task scope for the reclaim scan (required; blank/whitespace refused).",
    )
    lane_reclaim_scope.add_argument(
        "--all-tasks",
        action="store_true",
        help="Explicitly scan terminal lane rows across every task.",
    )
    lane_reclaim_scan_p.add_argument(
        "--lane-id",
        default=None,
        help="When set, also run nudge_reclaim_candidate for this lane after the scan.",
    )
    lane_reclaim_scan_p.add_argument(
        "--limit",
        type=_positive_int_arg,
        default=None,
        help="Evaluate at most N registry rows in this invocation.",
    )
    lane_reclaim_scan_p.add_argument(
        "--offset",
        type=_nonnegative_int_arg,
        default=0,
        help="Skip N registry rows before evaluating this page (default: 0).",
    )

    orphan_reclaim_p = subparsers.add_parser(
        "orphan-reclaim",
        help="Reclaim merged branches and idle worktrees with no live task or lane owner.",
    )
    orphan_reclaim_p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply eligible reclaims (default is a dry-run report).",
    )
    orphan_reclaim_p.add_argument(
        "--integration-ref",
        default="main",
        help="Integration ref used for reachability (default: main).",
    )
    orphan_reclaim_p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=False,
        help="Emit the scan and reclaim report as JSON.",
    )

    pass_rescue_p = subparsers.add_parser(
        "pass-rescue",
        help="Inspect a terminal pass and optionally salvage/re-arm its original dispatch.",
    )
    pass_rescue_p.add_argument("pass_id")
    pass_rescue_p.add_argument("--apply", action="store_true", default=False)
    pass_rescue_p.add_argument("--json", dest="json_output", action="store_true", default=False)

    # --- list-backends ---
    list_backends_p = subparsers.add_parser("list-backends", help="List available AI backends.")
    list_backends_p.add_argument(
        "--probe",
        action="store_true",
        default=False,
        help="Probe live availability per backend (may shell out to codex/claude and import optional bridges).",
    )

    # --- metrics ---
    metrics_p = subparsers.add_parser("metrics", help="Print ACE metrics summary.")
    metrics_p.add_argument("--task-ref", default=None)
    metrics_p.add_argument("--format", dest="output_format", default="markdown", choices=["markdown", "json"])

    ace_playbook: dict[str, Any] = {
        "action": "append",
        "dest": "playbook_files",
        "help": (
            "Playbook file with ACE strategy bullets (repeatable). Falls back to "
            "the WORKBAY_ACE_PLAYBOOK_FILES env var when omitted."
        ),
    }

    ace_reflect_p = subparsers.add_parser("ace-reflect", help="Apply pending ACE counter updates.")
    ace_reflect_p.add_argument("--playbook-file", **ace_playbook)
    ace_reflect_p.add_argument("--dry-run", action="store_true")
    ace_reflect_p.add_argument("--model-curation-backend", default=None)
    ace_reflect_p.add_argument("--model-curation-model", default=None)
    ace_reflect_p.add_argument("--model-curation-reasoning-effort", default=None)
    ace_reflect_p.add_argument("--model-curation-threshold", type=int, default=5)
    ace_reflect_p.add_argument("--model-curation-budget-tokens", type=int, default=20000)

    ace_report_p = subparsers.add_parser("ace-curation-report", help="Print ACE curation report.")
    ace_report_p.add_argument("--playbook-file", **ace_playbook)

    ace_metrics_p = subparsers.add_parser("ace-metrics", help="Build full ACE metrics snapshot.")
    ace_metrics_p.add_argument("--task-ref", required=True)
    ace_metrics_p.add_argument("--playbook-file", **ace_playbook)
    ace_metrics_p.add_argument("--logs-dir", default="logs")
    ace_metrics_p.add_argument("--format", dest="output_format", default="markdown", choices=["markdown", "json"])

    ace_trends_p = subparsers.add_parser("ace-trends", help="Print ACE metrics sparklines.")
    ace_trends_p.add_argument("--task-ref", required=True)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = _build_config(
        args.workspace_root,
        state_dir=args.state_dir,
        current_task_path=args.current_task_path,
        exports_dir=args.exports_dir,
    )
    configure_runtime(config)

    cmd = args.command

    # --- serve ---
    if cmd in (None, "serve", "serve-stdio"):
        mcp = build_orchestrator_mcp(config)
        mcp.run()
        return

    # --- doctor ---
    if cmd == "doctor":
        result = run_doctor(config)
        if getattr(args, "json_output", False):
            print(json.dumps(result, indent=2))
        else:
            print(f"server: {result.get('server', 'mcp-workbay-orchestrator')}")
            print(f"tool_count: {result.get('tool_count', '?')}")
            for name in sorted(result.get("tools", [])):
                print(f"  - {name}")
        return

    if cmd == "tools-snapshot":
        output_path = args.output
        if output_path is None:
            output_path = config.state_dir / "tools-list-snapshot.json"
        result = run_tools_snapshot(config, output_path=output_path)
        if getattr(args, "json_output", False):
            print(json.dumps(result, indent=2))
        else:
            print(f"server: {result['server']}")
            print(f"tool_count: {result['tool_count']}")
            print(
                "estimated_tools_list_tokens: "
                f"{result['estimated_tools_list_tokens']} ({result['token_estimation_method']})"
            )
            print(f"tools_list_bytes: {result['tools_list_bytes']}")
            print(f"output_path: {result['output_path']}")
        return

    # --- orchestrator daemon ---
    if cmd == "orchestrator-start":
        _print_json(
            manage_orchestrator(
                operation="start",
                task_ref=args.task_ref,
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "orchestrator-status":
        _print_json(manage_orchestrator(operation="status"))
        return

    if cmd == "orchestrator-pause":
        _print_json(manage_orchestrator(operation="pause"))
        return

    if cmd == "orchestrator-resume":
        _print_json(manage_orchestrator(operation="resume"))
        return

    if cmd == "orchestrator-stop":
        _print_json(manage_orchestrator(operation="stop", force=args.force, wait_seconds=args.wait_seconds))
        return

    if cmd == "orchestrator-cycle":
        _print_json(
            manage_orchestrator(
                operation="single_cycle",
                task_ref=args.task_ref,
                backend=args.backend,
                dry_run=args.dry_run,
                timeout_seconds=args.timeout_seconds,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
            )
        )
        return

    # --- worker daemon ---
    if cmd == "worker-start":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                action="start",
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                session=args.session,
                session_mode=args.session_mode,
                reasoning_effort=args.reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "worker-status":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="status"))
        return

    if cmd == "worker-stop":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="stop", force=args.force))
        return

    if cmd == "worker-resume":
        _print_json(manage_worker(task_ref=args.task_ref, lane_id=args.lane_id, action="resume"))
        return

    if cmd == "worker-start-all":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                action="start_all",
                backend=args.backend,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                session_mode=args.session_mode,
                reasoning_effort=args.reasoning_effort,
                model=args.model,
            )
        )
        return

    if cmd == "worker-events":
        _print_json(
            manage_worker(
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                action="event_history",
                limit=args.limit,
                event_name=args.event_name,
            )
        )
        return

    # --- dispatch ---
    if cmd == "dispatch":
        _print_json(
            dispatch_lane_work(
                lane_id=args.lane_id,
                model=args.model,
                backend=args.backend,
                reasoning_effort=args.reasoning_effort,
                task_ref=args.task_ref,
                start_worker=args.start_worker,
            )
        )
        return

    # --- lane data ---
    if cmd == "lane-upsert":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="upsert",
                lane_id=args.lane_id,
                worktree_path=args.worktree_path,
                branch=args.branch,
                owner_agent=args.owner_agent,
                status=args.status,
                title=args.title,
                objective=args.objective,
                notes=args.notes,
                task_ref=args.task_ref,
            )
        )
        return

    if cmd == "lane-close":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="close",
                lane_id=args.lane_id,
                status=args.status,
                notes=args.notes,
                task_ref=args.task_ref,
            )
        )
        return

    if cmd == "lane-list":
        _emit_lane_payload(
            manage_worktree_lane(
                operation="list",
                task_ref=args.task_ref,
                status=args.status,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-activity":
        _emit_lane_payload(get_lane_activity(lane_id=args.lane_id, task_ref=args.task_ref))
        return

    if cmd == "lane-message":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="record",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                direction=args.direction,
                message=args.message,
                status=args.status,
                subject=args.subject,
            )
        )
        return

    if cmd == "lane-message-list":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="list",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                status=args.status,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-message-update":
        _emit_lane_payload(
            lane_communication(
                kind="message",
                operation="update",
                task_ref=args.task_ref,
                message_id=args.message_id,
                status=args.status,
            )
        )
        return

    if cmd == "lane-report":
        _emit_lane_payload(
            worker_reports(
                operation="record",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                session=args.session,
                delivery_id=args.delivery_id,
                summary=args.summary,
                status=args.status,
                outcome=args.outcome,
                merge_ready=args.merge_ready,
                changed_files=args.changed_file,
                test_commands=args.test_command,
                blockers=args.blocker,
            )
        )
        return

    if cmd == "lane-report-list":
        _emit_lane_payload(
            worker_reports(
                operation="list",
                task_ref=args.task_ref,
                lane_id=args.lane_id,
                limit=args.limit,
                offset=args.offset,
            )
        )
        return

    if cmd == "lane-report-ack-backfill":
        _emit_lane_payload(
            backfill_worker_report_acks(
                task_ref=args.task_ref,
                dry_run=args.dry_run,
                limit=args.limit,
            )
        )
        return

    if cmd == "lane-reap":
        if getattr(args, "cross_session", False):
            _handle_lane_reap_cross_session(args)
            return
        # AGT-14: no confirmation prompt — safety is dry-run default + --task-ref.
        # Blank/whitespace --task-ref would silently widen to repo-wide via reaper
        # normalization; refuse at the CLI shell instead (do not call reaper).
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload(
                {
                    "ok": False,
                    "error": (
                        "blank_task_ref: --task-ref was provided but empty/whitespace; "
                        "an empty scope would silently widen the reap to repo-wide. "
                        "Omit the flag for an explicit repo-wide dry run, or pass a real task_ref."
                    ),
                }
            )
            return
        previous_bundle_dir = os.environ.get("WORKBAY_BUNDLE_DIR")
        if args.bundle_dir is not None:
            os.environ["WORKBAY_BUNDLE_DIR"] = args.bundle_dir
        try:
            payload = reap_blocked_lanes(
                apply=args.apply,
                task_ref=args.task_ref,
                max_batch=args.max_batch,
                min_age_hours=args.min_age_hours,
                reclaim_worktrees=args.reclaim_worktrees,
                bundle_retention_count=args.bundle_retention_count,
            )
        finally:
            if args.bundle_dir is not None:
                if previous_bundle_dir is None:
                    os.environ.pop("WORKBAY_BUNDLE_DIR", None)
                else:
                    os.environ["WORKBAY_BUNDLE_DIR"] = previous_bundle_dir
        # OBS-08: reap_blocked_lanes is never-raise (RES-07) and may return ok:true
        # with an error key or non-empty failed list. Print payload verbatim, exit
        # nonzero on degraded sweeps so make/automation cannot treat failure as success.
        if isinstance(payload, dict):
            failed = payload.get("failed")
            # ``registry_sweep_degraded`` carries the nested sweep's refusals --
            # including every ``verified_bundle_required`` -- which would
            # otherwise print inside ``registry_sweep`` and exit 0 [AGT-21].
            if (
                payload.get("error")
                or (isinstance(failed, list) and failed)
                or payload.get("registry_sweep_degraded")
                or payload.get("telemetry_incomplete")
                or payload.get("bundle_refusals")
            ):
                if payload.get("bundle_refusals"):
                    payload["ok"] = False
                _print_json(payload)
                raise SystemExit(1)
        _emit_lane_payload(payload)
        return

    if cmd == "lane-dispose":
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload(
                {
                    "ok": False,
                    "command": "lane-dispose",
                    "reason": "blank_task_ref",
                    "detail": "--task-ref was provided but empty/whitespace.",
                }
            )
            return
        if args.dispose_command == "scan":
            _handle_lane_dispose_scan(args, root=config.workspace_root, state_dir=config.state_dir)
            return
        if args.dispose_command == "backfill":
            _handle_lane_dispose_backfill(args, root=config.workspace_root, state_dir=config.state_dir)
            return
        if args.dispose_command == "abandon":
            _handle_lane_dispose_abandon(args, root=config.workspace_root)
            return
        _emit_lane_payload(
            {
                "ok": False,
                "command": "lane-dispose",
                "reason": "unknown_dispose_command",
            }
        )
        return

    if cmd == "lane-unreap":
        payload = unreap_lane_branch(
            args.repo_root or _workspace_root(),
            args.branch,
            bundle_path=args.bundle_path,
        )
        _print_json(payload)
        if not payload.get("ok"):
            raise SystemExit(1)
        return

    if cmd == "branch-reclaim-release":
        try:
            branch_reclaim_dead_letter_release_decision_id(
                lane_id=args.lane_id,
                authorized_sha=args.sha,
            )
        except ValueError:
            payload = {
                "ok": False,
                "outcome": "invalid_identity",
                "task_ref": args.task_ref,
                "lane_id": args.lane_id,
                "branch": "",
                "sha": args.sha,
                "decision_id": "",
                "release_count": 0,
            }
            _print_json(payload)
            raise SystemExit(1)
        if not str(args.decision).strip():
            payload = {
                "ok": False,
                "outcome": "invalid_decision",
                "task_ref": args.task_ref,
                "lane_id": args.lane_id,
                "branch": "",
                "sha": args.sha,
                "decision_id": "",
                "release_count": 0,
            }
            _print_json(payload)
            raise SystemExit(1)
        payload = release_dead_letter_by_identity(
            task_ref=args.task_ref,
            lane_id=args.lane_id,
            authorized_sha=args.sha,
            operator_decision=args.decision,
        )
        _print_json(payload)
        if not payload.get("ok"):
            raise SystemExit(1)
        return

    if cmd == "pass-rescue":
        payload = rescue_offload_pass(args.pass_id, apply=args.apply)
        if args.json_output:
            _print_json(payload)
        else:
            for key in (
                "pass_id",
                "lane_id",
                "classification",
                "sandbox_state",
                "differing_paths",
                "salvage_paths",
                "actions",
                "rearm_ready",
                "lane_spec",
                "refused_reason",
            ):
                print(f"{key}: {payload.get(key)}")
        if payload.get("ok") is False:
            raise SystemExit(1)
        return

    if cmd == "remote-sandbox-reap":
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload({"ok": False, "error": "blank_task_ref: omit --task-ref or pass a real task_ref."})
            return
        if args.idle_seconds is not None and args.idle_seconds < 0:
            _emit_lane_payload({"ok": False, "error": "--idle-seconds must be non-negative."})
            return
        from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
            run_remote_sandbox_reap,
        )

        _emit_lane_payload(
            run_remote_sandbox_reap(
                root=config.workspace_root,
                task_ref=args.task_ref,
                apply=args.apply,
                idle_seconds=args.idle_seconds,
            )
        )
        return

    if cmd == "lane-census":
        # AGT-14: no confirmation prompt — safety is dry-run default.
        if args.task_ref is not None and args.task_ref.strip() == "":
            _emit_lane_payload(
                {
                    "ok": False,
                    "error": ("blank_task_ref: --task-ref was provided but empty/whitespace; pass a real task_ref."),
                }
            )
            return
        from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
            census_lanes,
            report_payload,
        )

        try:
            report = census_lanes(
                args.task_ref,
                root=config.workspace_root,
                apply=args.apply,
                max_batch=args.max_batch,
            )  # apply=True also runs the acting remote-sandbox reaper
        except Exception as exc:  # noqa: BLE001 — CLI never-raise
            _emit_lane_payload({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
            return
        _emit_lane_payload(report_payload(report, task_ref=args.task_ref, apply=args.apply))
        return

    if cmd == "lane-reclaim-scan":
        _handle_lane_reclaim_scan(
            orchestrator_root=config.workspace_root,
            task_ref=args.task_ref,
            all_tasks=args.all_tasks,
            lane_id=args.lane_id,
            limit=args.limit,
            offset=args.offset,
        )
        return

    if cmd == "orphan-reclaim":
        _handle_orphan_reclaim(
            orchestrator_root=config.workspace_root,
            state_dir=config.state_dir,
            apply=args.apply,
            integration_ref=args.integration_ref,
            json_output=args.json_output,
        )
        return

    # --- list-backends ---
    if cmd == "list-backends":
        _print_json(list_available_backends(probe=args.probe))
        return

    # --- metrics ---
    if cmd == "metrics":
        print(get_metrics_summary(task_ref=args.task_ref, output_format=args.output_format))
        return

    if cmd == "ace-reflect":
        from workbay_orchestrator_mcp.orchestration.ace_reflect import run_ace_reflect  # noqa: PLC0415

        raise SystemExit(
            run_ace_reflect(
                state_dir=config.state_dir,
                playbook_files=_validated_playbook_files("ace-reflect", args.playbook_files, config.workspace_root),
                dry_run=args.dry_run,
                model_curation_backend=args.model_curation_backend,
                model_curation_model=args.model_curation_model,
                model_curation_reasoning_effort=args.model_curation_reasoning_effort,
                model_curation_threshold=args.model_curation_threshold,
                model_curation_budget_tokens=args.model_curation_budget_tokens,
            )
        )

    if cmd == "ace-curation-report":
        from workbay_orchestrator_mcp.orchestration.ace_reflect import run_curation_report  # noqa: PLC0415

        raise SystemExit(
            run_curation_report(
                playbook_files=_validated_playbook_files(
                    "ace-curation-report", args.playbook_files, config.workspace_root
                )
            )
        )

    if cmd == "ace-metrics":
        from workbay_orchestrator_mcp.orchestration.ace_metrics import (  # noqa: PLC0415
            _append_snapshot,
            build_snapshot,
            render_markdown,
        )

        playbook_files = _validated_playbook_files("ace-metrics", args.playbook_files, config.workspace_root)
        snapshot = build_snapshot(
            task_ref=args.task_ref,
            state_dir=config.state_dir,
            logs_dir=config.workspace_root / args.logs_dir,
            instruction_files=playbook_files,
        )
        _append_snapshot(config.state_dir, snapshot)
        if args.output_format == "json":
            print(json.dumps(snapshot, indent=2))
        else:
            print(render_markdown(snapshot))
        return

    if cmd == "ace-trends":
        from workbay_orchestrator_mcp.orchestration.ace_metrics import render_sparklines  # noqa: PLC0415

        print(render_sparklines(config.state_dir, args.task_ref))
        return

    parser.error(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
