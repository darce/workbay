"""Receipt-bearing, idempotent landing of one worktree lane."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4

_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_SUCCESS = frozenset({"landed", "already_landed", "landed_then_moved", "contained_reaped"})
EXIT_CODES = {
    "landed": 0,
    "already_landed": 0,
    "landed_then_moved": 3,
    "lock_held": 4,
    "moved": 5,
    "gate_missing": 6,
    "dirty_target": 7,
    "conflict": 8,
    "contained_no_receipt": 9,
    "merge_refused": 10,
    "indeterminate": 11,
    "lane_unresolved": 12,
    "invalid_request": 2,
    "cleanup_pending": 13,
    "contained_reaped": 0,
}
_NEXT_ACTIONS = {
    "landed_then_moved": "the branch has new commits; land the new tip separately",
    "lock_held": "retry after the current landing finishes",
    "moved": "re-read the branch tip and pass it as expected_tip",
    "gate_missing": "record a passing gate at the tip",
    "dirty_target": "clean or switch the integration checkout",
    "conflict": "rebase or fix the lane",
    "contained_no_receipt": "use backfill / lane_disposition, not land",
    "indeterminate": "re-run with the same run_id",
    "lane_unresolved": "register the lane row",
    "invalid_request": "fix the arguments",
    "cleanup_pending": "re-run with the same run_id to finish cleanup",
    "contained_reaped": "none",
}


def _envelope(
    outcome: str,
    *,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    run_id: str | None,
    detail: str | None = None,
    next_action: str | None = None,
    gate_id: int | None = None,
    landing_commit: str | None = None,
    integration_before: str | None = None,
    pin_ref: str | None = None,
    bundle_path: str | None = None,
    projection_pending: bool = False,
    retire_outcome: str | None = None,
    cleanup_pending: bool | None = None,
    conflicting_paths: list[str] | None = None,
) -> dict[str, object]:
    pending = outcome == "cleanup_pending" if cleanup_pending is None else cleanup_pending
    if next_action is None:
        next_action = _NEXT_ACTIONS.get(outcome)
    if outcome == "merge_refused" and next_action is None:
        next_action = detail or "inspect the refusal detail"
    if outcome in {"landed", "already_landed"} and projection_pending:
        next_action = "re-run to heal the projection"
    return {
        "ok": outcome in _SUCCESS and not pending,
        "outcome": outcome,
        "detail": detail,
        "next_action": next_action or "none",
        "task_ref": task_ref,
        "lane_id": lane_id,
        "expected_tip": expected_tip,
        "run_id": run_id,
        "gate_id": gate_id,
        "landing_commit": landing_commit,
        "integration_before": integration_before,
        "pin_ref": pin_ref,
        "bundle_path": bundle_path,
        "projection_pending": projection_pending,
        "retire_outcome": retire_outcome,
        "cleanup_pending": pending,
        "integrated": outcome in _SUCCESS or pending,
        "done": outcome in _SUCCESS and not pending,
        "conflicting_paths": conflicting_paths or [],
    }


def _raw_object(raw: object) -> dict[str, object] | None:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return None
    return raw if isinstance(raw, dict) else None


def _data(raw: object) -> dict[str, object] | None:
    payload = _raw_object(raw)
    if payload is None or payload.get("ok") is False:
        return None
    data = payload.get("data", payload)
    return data if isinstance(data, dict) else None


def _lane_row(raw: object) -> dict[str, object] | None:
    data = _data(raw)
    if data is None:
        return None
    value = data.get("lane", data.get("row"))
    if value is None and isinstance(data.get("branch"), str):
        value = data
    return value if isinstance(value, dict) else None


def _verified_test(raw: object, expected_tip: str) -> tuple[int | None, bool]:
    data = _data(raw)
    if data is None:
        return None, False
    tests = data.get("tests")
    if not isinstance(tests, list):
        return None, False
    if not tests:
        return None, True
    row = tests[0]
    if not isinstance(row, dict):
        return None, False
    if row.get("passed") is not True or row.get("commit_sha") != expected_tip:
        return None, True
    test_id = row.get("id")
    if isinstance(test_id, bool) or not isinstance(test_id, int) or test_id <= 0:
        return None, False
    return test_id, True


def _ref_oid(orchestrator_lanes: object, root: Path, ref: str) -> tuple[str | None, str | None]:
    try:
        proc = orchestrator_lanes._run_no_ff_git(
            root,
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            f"{ref}^{{commit}}",
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, "probe_failed"
    if proc.returncode == 1:
        return None, None
    if proc.returncode != 0:
        return None, "probe_failed"
    value = proc.stdout.strip()
    if _HEX40.fullmatch(value) is None:
        return None, "probe_failed"
    return value, None


def _conflicting_paths(stdout: str, stderr: str) -> list[str]:
    found: set[str] = set()
    lines = stdout.splitlines()
    for line in lines[1:]:
        value = line.strip()
        if value and _HEX40.fullmatch(value) is None and not value.startswith(("Auto-merging ", "CONFLICT ")):
            found.add(value)
    for line in stderr.splitlines():
        match = re.search(r"(?:Merge conflict in |CONFLICT[^\n]* in )(.+)$", line)
        if match:
            found.add(match.group(1).strip())
    return sorted(found)


def _receipt_verdict(landing_log: object, root: Path, commit: str, integration_ref: str) -> object | None:
    try:
        return landing_log.verify_receipt(root, commit, main_ref=integration_ref)
    except Exception:
        return None


def _find_natural_landing(
    landing_log: object,
    root: Path,
    *,
    task_ref: str,
    lane_id: str,
    tip: str,
    integration_ref: str,
) -> tuple[str, object | None, str | None]:
    try:
        verdict = landing_log.find_landing(
            root,
            task_ref=task_ref,
            lane_id=lane_id,
            tip=tip,
            main_ref=integration_ref,
        )
    except Exception as exc:
        return "indeterminate", None, f"unverified_receipt:scan_failed:{type(exc).__name__}"
    if verdict is None:
        return "none", None, None
    commit = getattr(verdict, "commit", None)
    failed = getattr(verdict, "failed_check", None)
    if getattr(verdict, "verified", False):
        return "verified", verdict, commit if isinstance(commit, str) else None
    if failed == "scan_timeout":
        return "indeterminate", verdict, "scan_timeout"
    return "indeterminate", verdict, f"unverified_receipt:{failed or 'unknown'}"


def _latest_landing_sha(raw: object) -> str | None:
    data = _data(raw)
    if data is None:
        return None
    landing = data.get("landing")
    if isinstance(landing, dict):
        sha = landing.get("commit_sha")
        return sha if isinstance(sha, str) else None
    return None


def _project(
    *,
    lanes: object,
    lanes_recording: object,
    orchestrator_lanes: object,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    landing_commit: str,
    integration_ref: str,
    replay: bool,
) -> bool:
    """Apply the idempotent handoff projection; return whether it remains pending."""
    try:
        recorded = False
        if replay:
            try:
                latest = lanes_recording.latest_lane_landing(lane_id=lane_id, task_ref=task_ref)
                recorded = _latest_landing_sha(latest) == landing_commit
            except Exception:
                recorded = False
        if not recorded:
            recorded = bool(orchestrator_lanes.record_lane_landing(task_ref, lane_id, landing_commit, integration_ref))
        if not recorded:
            return True

        try:
            current = lanes_recording.get_lane(lane_id=lane_id, task_ref=task_ref)
            row = _lane_row(current)
        except Exception:
            row = None
        if (
            row
            and row.get("status") == "merged"
            and row.get("landing_commit_sha") == landing_commit
            and row.get("branch_tip_sha") == expected_tip
        ):
            return False

        closed = lanes.close_worktree_lane(
            lane_id=lane_id,
            status="merged",
            task_ref=task_ref,
            landing_commit_sha=landing_commit,
            branch_tip_sha=expected_tip,
        )
        payload = _raw_object(closed)
        return payload is None or payload.get("ok") is not True
    except Exception:
        return True


def _registered_integration_ref(
    *,
    root: Path,
    task_ref: str,
    requested_ref: str | None,
    manifest_root: str | Path | None = None,
) -> tuple[str | None, str | None]:
    try:
        from .. import lanes  # noqa: PLC0415
        from .lane_postmerge import _read_manifest_integration_branch  # noqa: PLC0415

        declared, lookup_error = lanes._handoff_target_branch(task_ref)
        if lookup_error is not None:
            return None, "task_integration_ref_unresolved:target_lookup_failed"
        external_branch = None
        if manifest_root is not None:
            from . import lane_manifest

            manifest = lane_manifest.load_manifest(task_ref, manifest_dir=Path(manifest_root))
            if manifest.get("task_ref") != task_ref:
                return None, "manifest_task_ref_mismatch"
            external_branch = manifest.get("integration_branch")
            if external_branch is not None and not isinstance(external_branch, str):
                return None, "manifest_integration_ref_invalid"
        if declared is None:
            declared = external_branch
        if declared is None:
            declared = _read_manifest_integration_branch(task_ref, repo=root, orchestrator_root=None)
    except Exception as exc:  # noqa: BLE001 — unreadable task evidence fails closed
        return None, f"task_integration_ref_unresolved:{type(exc).__name__}"
    selected = declared or "main"
    if requested_ref is not None:
        requested = requested_ref.removeprefix("refs/heads/")
        # An explicit main request is the operator's release override.
        if requested == "main":
            return requested, None
        if requested != selected.removeprefix("refs/heads/"):
            return None, "integration_ref_not_task_registered"
    return selected, None


def _repair_stale_registration_tip(
    *,
    lanes: object,
    lanes_recording: object,
    verified_tests: object,
    landing_log: object,
    orchestrator_lanes: object,
    root: Path,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    integration_ref: str,
    lane: Mapping[str, object] | None,
    verdict: object,
) -> bool:
    """CAS-repair only the historical registration-base projection.

    Receipt position/topology and gate binding are re-proved independently of
    the stale row tip; a live branch that moved after landing blocks repair.
    """
    if (
        lane is None
        or lane.get("task_ref") != task_ref
        or lane.get("lane_id") != lane_id
        or lane.get("status") != "merged"
        or lane.get("branch_tip_source") != "registration"
        or lane.get("landing_commit_sha") != getattr(verdict, "commit", None)
    ):
        return False
    observed_tip = lane.get("branch_tip_sha")
    landing_commit = getattr(verdict, "commit", None)
    receipt = getattr(verdict, "receipt", None)
    gate_id = getattr(receipt, "gate_id", None)
    if (
        not isinstance(observed_tip, str)
        or _HEX40.fullmatch(observed_tip) is None
        or not isinstance(landing_commit, str)
        or _HEX40.fullmatch(landing_commit) is None
        or isinstance(gate_id, bool)
        or not isinstance(gate_id, int)
        or getattr(receipt, "task_ref", None) != task_ref
        or getattr(receipt, "lane_id", None) != lane_id
        or getattr(receipt, "tip", None) != expected_tip
        or getattr(receipt, "run_id", None) is None
    ):
        return False

    try:
        carrier = landing_log.find_carrier(root, landing_commit, main_ref=integration_ref)
        if not getattr(carrier, "contained", False) or getattr(carrier, "error", None):
            return False
        topology = orchestrator_lanes._run_no_ff_git(root, "rev-list", "--parents", "-n", "1", landing_commit)
        fields = topology.stdout.split()
        if topology.returncode != 0 or len(fields) != 3 or fields[2] != expected_tip:
            return False
        gate_raw = verified_tests.get_verified_test_by_id(gate_id)
        gate_data = _data(gate_raw)
        gate = gate_data.get("test", gate_data.get("verified_test", gate_data)) if gate_data else None
        if (
            not isinstance(gate, dict)
            or gate.get("passed") not in (True, 1)
            or gate.get("commit_sha") != expected_tip
            or gate.get("id", gate_id) != gate_id
        ):
            return False
        old_is_ancestor = orchestrator_lanes._run_no_ff_git(
            root, "merge-base", "--is-ancestor", observed_tip, expected_tip
        )
        if old_is_ancestor.returncode != 0:
            return False
        branch = lane.get("branch")
        if not isinstance(branch, str) or not branch.strip():
            return False
        branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
        live_tip, branch_error = _ref_oid(orchestrator_lanes, root, branch_ref)
        if branch_error or (live_tip is not None and live_tip != expected_tip):
            return False
        current = _lane_row(lanes_recording.get_lane(lane_id=lane_id, task_ref=task_ref))
        if (
            current is None
            or current.get("status") != "merged"
            or current.get("landing_commit_sha") != landing_commit
            or current.get("branch_tip_sha") != observed_tip
            or current.get("branch_tip_source") != "registration"
        ):
            return False
        return bool(
            lanes._repair_registration_branch_tip_cas(
                task_ref=task_ref,
                lane_id=lane_id,
                landing_commit_sha=landing_commit,
                expected_branch_tip_sha=observed_tip,
                expected_branch_tip_source="registration",
                branch_tip_sha=expected_tip,
            )
        )
    except Exception:
        return False


def _retire(
    *,
    lanes: object,
    orchestrator_lanes: object,
    root: Path,
    branch: str,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    integration_ref: str,
    landing_commit: str,
    landing_run_id: str,
    contained_landing: bool,
    replay: bool,
    projection_pending: bool,
    manifest_root: str | Path | None = None,
) -> tuple[str, str, str | None, str | None]:
    if projection_pending:
        return "landed", "skipped:projection_pending", None, None
    branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    current, error = _ref_oid(orchestrator_lanes, root, branch_ref)
    if error:
        return "landed", "skipped:branch_probe_failed", None, None
    if current != expected_tip:
        if current is None and replay:
            pass
        elif current is None:
            return "landed", "branch_absent_before_cleanup", None, None
        else:
            return "landed_then_moved", "tip_moved_after_landing", None, None
    try:
        raw = lanes._retire_worktree_lane(
            lane_id=lane_id,
            task_ref=task_ref,
            apply=True,
            delete_merged_branch=True,
            landing_integration_ref=integration_ref,
            landing_commit=landing_commit,
            landing_expected_tip=expected_tip,
            landing_run_id=landing_run_id,
            landing_is_contained=contained_landing,
            landing_repo_root=root,
            landing_manifest_root=manifest_root,
        )
    except Exception as exc:
        return "landed", f"skipped:retire_error:{type(exc).__name__}", None, None
    payload = _raw_object(raw)
    if payload is None:
        return "landed", "skipped:unreadable_envelope", None, None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return "landed", "skipped:unreadable_envelope", None, None
    outcome = str(data.get("outcome") or data.get("error_kind") or "unknown")
    bundle_path = data.get("bundle_path") if isinstance(data.get("bundle_path"), str) else None
    if payload.get("ok") is True or data.get("ok") is True:
        if outcome == "retired" and data.get("bundle_tip_sha") != expected_tip:
            return "landed", "skipped:bundle_tip_unverified", None, None
        return "landed", outcome, bundle_path, None
    if outcome == "tip_moved_after_bundle":
        return "landed_then_moved", outcome, bundle_path, None
    if replay and outcome == "already_retired":
        return "landed", outcome, bundle_path, None
    detail = data.get("detail")
    return "landed", f"skipped:{outcome}", bundle_path, detail[:300] if isinstance(detail, str) else None


def _persist_intent(root: Path, landing_cleanup: object, intent: dict, **updates: object) -> dict | None:
    try:
        return landing_cleanup.update_intent(
            root,
            str(intent["intent_id"]),
            expected_revision=int(intent["revision"]),
            **updates,
        )
    except Exception:
        return None


def _bundle_proves_tip(root: Path, lane_reaping: object, bundle_path: object, expected_tip: str) -> bool:
    if not isinstance(bundle_path, str) or not bundle_path.strip():
        return False
    try:
        return bool(lane_reaping._verified_bundle_contains(root, Path(bundle_path), expected_tip))
    except Exception:
        return False


def _all_registered_lane_rows(lanes_recording: object) -> list[dict[str, object]] | None:
    """Read a bounded, complete cross-task ownership snapshot for remote reap."""
    try:
        raw = lanes_recording.list_lanes(all_tasks=True, status="all", limit=10_000, after_id=None)
    except Exception:
        return None
    data = _data(raw)
    if data is None or not isinstance(data.get("lanes"), list) or data.get("has_more") is not False:
        return None
    return [row for row in data["lanes"] if isinstance(row, dict)]


def _finish_cleanup(
    *,
    root: Path,
    identity: Mapping[str, str],
    intent: dict,
    lane_row: Mapping[str, object],
    lanes: object,
    lanes_recording: object,
    orchestrator_lanes: object,
    lane_reaping: object,
    projection_pending: bool,
    replay: bool,
    contained_landing: bool = False,
    manifest_root: str | Path | None = None,
) -> dict[str, object]:
    """Resume local and remote cleanup from its durable per-landing intent."""
    from . import landing_cleanup

    local_status = intent.get("local_status")
    remote_status = intent.get("remote_status")
    local_result = intent.get("local_result") if isinstance(intent.get("local_result"), dict) else {}
    remote_result = intent.get("remote_result") if isinstance(intent.get("remote_result"), dict) else {}
    branch = lane_row.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        return {
            "complete": False,
            "detail": "captured_branch_unavailable",
            "local_status": local_status,
            "remote_status": remote_status,
        }
    branch = branch.strip()
    backend = lane_row.get("backend")
    if isinstance(backend, str) and backend.strip() and remote_status not in {"complete", "skipped"}:
        try:
            from .worker_daemon_ctl import is_remote_worker_backend

            is_remote_backend = is_remote_worker_backend(backend)
        except Exception:
            is_remote_backend = None
        if is_remote_backend is False:
            saved = _persist_intent(
                root,
                landing_cleanup,
                intent,
                remote_status="complete",
                remote_result={"ok": True, "status": "complete", "reason": "local_backend"},
            )
            if saved is None:
                return {
                    "complete": False,
                    "detail": "cleanup_intent_update_failed",
                    "local_status": local_status,
                    "remote_status": "pending",
                }
            intent = saved
            remote_status = "complete"
    if projection_pending:
        return {
            "complete": False,
            "detail": "projection_pending",
            "local_status": local_status,
            "remote_status": remote_status,
            "retire_outcome": "skipped:projection_pending",
        }

    retire_outcome: str | None = None
    bundle_path = local_result.get("bundle_path") if isinstance(local_result.get("bundle_path"), str) else None
    if local_status not in {"complete", "skipped"}:
        current, branch_error = _ref_oid(
            orchestrator_lanes, root, branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
        )
        if branch_error:
            return {
                "complete": False,
                "detail": "branch_probe_failed",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": "skipped:branch_probe_failed",
            }
        if current is not None and current != identity["expected_tip"]:
            return {
                "complete": False,
                "detail": "tip_moved_after_landing",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": "tip_moved_after_landing",
            }

        if current is not None:
            try:
                bundle = lane_reaping._bundle_before_reap(root, branch)
            except Exception as exc:
                bundle = {"ok": False, "error": type(exc).__name__}
            if (
                not isinstance(bundle, dict)
                or bundle.get("ok") is not True
                or bundle.get("tip_sha") != identity["expected_tip"]
                or not _bundle_proves_tip(root, lane_reaping, bundle.get("bundle_path"), identity["expected_tip"])
            ):
                return {
                    "complete": False,
                    "detail": "exact_tip_bundle_unverified",
                    "local_status": "pending",
                    "remote_status": remote_status,
                    "retire_outcome": "skipped:bundle_unverified",
                }
            local_result = {
                "phase": "bundle_verified",
                "bundle_path": bundle.get("bundle_path"),
                "bundle_tip_sha": bundle.get("tip_sha"),
            }
            saved = _persist_intent(root, landing_cleanup, intent, local_result=local_result)
            if saved is None:
                return {
                    "complete": False,
                    "detail": "cleanup_intent_update_failed",
                    "local_status": "pending",
                    "remote_status": remote_status,
                    "retire_outcome": "skipped:intent_update_failed",
                }
            intent = saved
        elif (
            local_result.get("phase") not in {"bundle_verified", "local_pending"}
            or local_result.get("bundle_tip_sha") != identity["expected_tip"]
            or not _bundle_proves_tip(root, lane_reaping, local_result.get("bundle_path"), identity["expected_tip"])
        ):
            return {
                "complete": False,
                "detail": "absent_branch_without_durable_bundle_proof",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": "skipped:bundle_proof_missing",
            }

        final_outcome, retire_outcome, retired_bundle, retire_detail = _retire(
            manifest_root=manifest_root,
            lanes=lanes,
            orchestrator_lanes=orchestrator_lanes,
            root=root,
            branch=branch,
            task_ref=identity["task_ref"],
            lane_id=identity["lane_id"],
            expected_tip=identity["expected_tip"],
            integration_ref=identity["integration_ref"],
            landing_commit=identity["landing_commit"],
            landing_run_id=identity["run_id"],
            contained_landing=contained_landing,
            replay=replay,
            projection_pending=False,
        )
        retire_outcome = retire_outcome
        bundle_path = retired_bundle or local_result.get("bundle_path")
        branch_after, branch_after_error = _ref_oid(
            orchestrator_lanes, root, branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
        )
        worktree_path = lane_row.get("worktree_path")
        path_present = False
        if isinstance(worktree_path, str) and worktree_path.strip():
            path = Path(worktree_path.strip())
            path_present = path.exists() or path.is_symlink()
        if (
            final_outcome != "landed"
            or retire_outcome not in {"retired", "already_retired"}
            or branch_after_error is not None
            or branch_after is not None
            or path_present
            or not _bundle_proves_tip(root, lane_reaping, bundle_path, identity["expected_tip"])
        ):
            failed = {
                "phase": "local_pending",
                "outcome": retire_outcome,
                "bundle_path": bundle_path,
                "bundle_tip_sha": local_result.get("bundle_tip_sha"),
            }
            saved = _persist_intent(root, landing_cleanup, intent, local_result=failed)
            if saved is not None:
                intent = saved
                local_status = intent.get("local_status")
            return {
                "complete": False,
                "detail": (f"local_cleanup_pending:{retire_outcome}" + (f":{retire_detail}" if retire_detail else ""))[
                    :300
                ],
                "local_status": local_status or "pending",
                "remote_status": remote_status,
                "retire_outcome": retire_outcome,
                "bundle_path": bundle_path,
            }
        completed_local = {
            "ok": True,
            "status": "complete",
            "phase": "complete",
            "outcome": retire_outcome,
            "bundle_path": bundle_path,
            "bundle_tip_sha": identity["expected_tip"],
        }
        saved = _persist_intent(
            root,
            landing_cleanup,
            intent,
            local_status="complete",
            local_result=completed_local,
        )
        if saved is None:
            return {
                "complete": False,
                "detail": "cleanup_intent_update_failed",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": retire_outcome,
                "bundle_path": bundle_path,
            }
        intent = saved
        local_status = "complete"
        local_result = completed_local
    else:
        retire_outcome = str(local_result.get("outcome") or "already_retired")
        bundle_path = local_result.get("bundle_path") if isinstance(local_result.get("bundle_path"), str) else None

    if remote_status not in {"complete", "skipped"}:
        backend = lane_row.get("backend")
        if not isinstance(backend, str) or not backend.strip():
            # The immutable intent can predate a routing repair. Recover only
            # routing, never the landed identity or a nonempty pinned backend.
            try:
                current_lane = _lane_row(
                    lanes_recording.get_lane(lane_id=identity["lane_id"], task_ref=identity["task_ref"])
                )
            except Exception:
                current_lane = None
            if (
                current_lane is not None
                and current_lane.get("task_ref") == identity["task_ref"]
                and current_lane.get("lane_id") == identity["lane_id"]
                and current_lane.get("branch") == branch
                and current_lane.get("branch_tip_sha") == identity["expected_tip"]
                and current_lane.get("landing_commit_sha") == identity["landing_commit"]
                and current_lane.get("status") == "merged"
            ):
                backend = current_lane.get("backend")
        if not isinstance(backend, str) or not backend.strip():
            pending_remote = {"status": "pending", "reason": "lane_backend_unavailable"}
            saved = _persist_intent(root, landing_cleanup, intent, remote_result=pending_remote)
            if saved is not None:
                intent = saved
            return {
                "complete": False,
                "detail": "remote_backend_unavailable",
                "local_status": local_status,
                "remote_status": "pending",
                "retire_outcome": retire_outcome,
                "bundle_path": bundle_path,
            }
        try:
            from .worker_daemon_ctl import is_remote_worker_backend

            remote_backend = is_remote_worker_backend(backend)
        except Exception:
            remote_backend = None
        if remote_backend is None:
            pending_remote = {"status": "pending", "reason": "remote_backend_classification_failed"}
            saved = _persist_intent(root, landing_cleanup, intent, remote_result=pending_remote)
            if saved is not None:
                intent = saved
            return {
                "complete": False,
                "detail": "remote_backend_classification_failed",
                "local_status": local_status,
                "remote_status": "pending",
                "retire_outcome": retire_outcome,
                "bundle_path": bundle_path,
            }
        if not remote_backend:
            saved = _persist_intent(
                root,
                landing_cleanup,
                intent,
                remote_status="complete",
                remote_result={"ok": True, "status": "complete", "reason": "local_backend"},
            )
            if saved is None:
                return {
                    "complete": False,
                    "detail": "cleanup_intent_update_failed",
                    "local_status": local_status,
                    "remote_status": "pending",
                    "retire_outcome": retire_outcome,
                    "bundle_path": bundle_path,
                }
            intent = saved
            remote_status = "complete"
            remote_result = intent.get("remote_result") if isinstance(intent.get("remote_result"), dict) else {}
        else:
            current_raw = lanes_recording.get_lane(lane_id=identity["lane_id"], task_ref=identity["task_ref"])
            current_lane = _lane_row(current_raw)
            if (
                current_lane is None
                or current_lane.get("task_ref") != identity["task_ref"]
                or current_lane.get("lane_id") != identity["lane_id"]
                or current_lane.get("branch_tip_sha") != identity["expected_tip"]
                or current_lane.get("status") != "merged"
                or current_lane.get("landing_commit_sha") != identity["landing_commit"]
                or current_lane.get("branch") != branch
            ):
                return {
                    "complete": False,
                    "detail": "remote_cleanup_landing_projection_unverified",
                    "local_status": local_status,
                    "remote_status": "pending",
                    "retire_outcome": retire_outcome,
                    "bundle_path": bundle_path,
                }
            rows = _all_registered_lane_rows(lanes_recording)
            if rows is None:
                pending_remote = {"status": "pending", "reason": "lane_registry_listing_failed"}
                saved = _persist_intent(root, landing_cleanup, intent, remote_result=pending_remote)
                if saved is not None:
                    intent = saved
                return {
                    "complete": False,
                    "detail": "lane_registry_listing_failed",
                    "local_status": local_status,
                    "remote_status": "pending",
                    "retire_outcome": retire_outcome,
                    "bundle_path": bundle_path,
                }
            rows = [
                row
                for row in rows
                if not (row.get("task_ref") == identity["task_ref"] and row.get("lane_id") == identity["lane_id"])
            ]
            rows.append(current_lane)
            try:
                from . import remote_sandbox_reap

                marker = remote_sandbox_reap.write_merged_marker(
                    ({"branch": branch, "receipt_sha": identity["landing_commit"]},), repo_root=root
                )
                if getattr(marker, "ok", False) is not True:
                    raise RuntimeError(f"merged_marker_write_unverified:{getattr(marker, 'error', '')}")
                remote = remote_sandbox_reap.reap_remote_lane_sandbox(
                    identity["task_ref"],
                    lane_id=identity["lane_id"],
                    branch=branch,
                    expected_tip=identity["expected_tip"],
                    primary_repo=root,
                    rows=rows,
                )
            except Exception as exc:
                message = " ".join(str(exc).splitlines())[:300]
                remote = {
                    "ok": False,
                    "status": "pending",
                    "reason": f"remote_cleanup_failed:{type(exc).__name__}:{message}",
                }
            remote_result = (
                remote if isinstance(remote, dict) else {"status": "pending", "reason": "unreadable_remote_result"}
            )
            if remote_result.get("ok") is True and remote_result.get("status") == "complete":
                saved = _persist_intent(
                    root, landing_cleanup, intent, remote_status="complete", remote_result=remote_result
                )
                if saved is None:
                    return {
                        "complete": False,
                        "detail": "cleanup_intent_update_failed",
                        "local_status": local_status,
                        "remote_status": "pending",
                        "retire_outcome": retire_outcome,
                        "bundle_path": bundle_path,
                    }
                intent = saved
                remote_status = "complete"
            else:
                saved = _persist_intent(root, landing_cleanup, intent, remote_result=remote_result)
                if saved is not None:
                    intent = saved
                return {
                    "complete": False,
                    "detail": f"remote_cleanup_pending:{remote_result.get('reason') or 'unknown'}",
                    "local_status": local_status,
                    "remote_status": "pending",
                    "retire_outcome": retire_outcome,
                    "bundle_path": bundle_path,
                }

    complete = local_status in {"complete", "skipped"} and remote_status in {"complete", "skipped"}
    return {
        "complete": complete,
        "detail": None if complete else "cleanup_pending",
        "local_status": local_status,
        "remote_status": remote_status,
        "retire_outcome": retire_outcome,
        "bundle_path": bundle_path,
    }


def land(
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    run_id: str | None = None,
    *,
    integration_ref: str | None = None,
    orchestrator_root: str | Path | None = None,
    manifest_root: str | Path | None = None,
) -> dict:
    """Append one receipt-bearing no-ff landing and converge its projections."""
    task_value = task_ref.strip() if isinstance(task_ref, str) else ""
    lane_value = lane_id.strip() if isinstance(lane_id, str) else ""
    tip_value = expected_tip if isinstance(expected_tip, str) else ""
    supplied_run_id = run_id if isinstance(run_id, str) else None
    if (
        not task_value
        or not lane_value
        or "/" in task_value
        or "/" in lane_value
        or any(ch.isspace() for ch in task_value + lane_value)
        or _HEX40.fullmatch(tip_value) is None
    ):
        return _envelope(
            "invalid_request",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=supplied_run_id,
        )
    if run_id is not None and (not isinstance(run_id, str) or _HEX32.fullmatch(run_id) is None):
        return _envelope(
            "invalid_request",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=run_id if isinstance(run_id, str) else None,
        )
    request_run_id = run_id or uuid4().hex
    if integration_ref is not None and (not isinstance(integration_ref, str) or not integration_ref.strip()):
        return _envelope(
            "invalid_request",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
        )
    try:
        # These imports stay local so lane management can import this module
        # without creating a lanes -> lane_land -> lanes import cycle.
        import workbay_handoff_mcp.lanes_recording as lanes_recording
        import workbay_handoff_mcp.verified_tests as verified_tests

        from .. import lane_reaping, lanes
        from . import harness_protocol, landing_log, orchestrator_lanes
    except Exception as exc:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail=f"dependencies_unavailable:{type(exc).__name__}",
        )

    try:
        root = (
            Path(orchestrator_root).expanduser().resolve()
            if orchestrator_root is not None
            else Path(lanes._workspace_root())
        )
    except Exception as exc:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail=f"orchestrator_root_unavailable:{type(exc).__name__}",
        )

    integration_value, integration_error = _registered_integration_ref(
        root=root,
        task_ref=task_value,
        requested_ref=integration_ref,
        manifest_root=manifest_root,
    )
    if integration_error is not None or integration_value is None:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail=integration_error or "task_integration_ref_unresolved",
        )

    try:
        policy = harness_protocol.load_landing_policy(root)
        require_gate_receipt = policy.require_gate_receipt if policy is not None else None
        require_review_verdict = policy.require_review_verdict if policy is not None else None
    except Exception:
        policy = None
        require_gate_receipt = None
        require_review_verdict = None
    if policy is None:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="landing_policy_unavailable",
        )
    if require_gate_receipt is not True:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="policy_unsupported:require_gate_receipt",
        )
    if require_review_verdict is not False:
        return _envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="policy_unsupported:require_review_verdict",
        )

    try:
        lock_context = orchestrator_lanes._landing_mutation_lock(root)
        with lock_context as acquired:
            if acquired is None:
                return _envelope(
                    "lock_held",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="landing_lock_unavailable",
                )
            if acquired is False:
                return _envelope(
                    "lock_held",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="mutation_lock_held",
                )

            lane_raw = lanes_recording.get_lane(lane_id=lane_value, task_ref=task_value)
            lane = _lane_row(lane_raw)
            branch_value = lane.get("branch") if lane is not None else None
            if lane is None or not isinstance(branch_value, str) or not branch_value.strip():
                return _envelope(
                    "lane_unresolved",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                )
            branch = branch_value.strip()
            branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"

            # Dedupe before checking the branch ref: replay normally sees a
            # retired branch, while the verified receipt remains authoritative.
            state, verdict, receipt_value = _find_natural_landing(
                landing_log,
                root,
                task_ref=task_value,
                lane_id=lane_value,
                tip=tip_value,
                integration_ref=integration_value,
            )
            if state == "indeterminate" and getattr(verdict, "failed_check", None) == "v3_identity":
                parsed_receipt = getattr(verdict, "receipt", None)
                landing_commit = getattr(verdict, "commit", None)
                if (
                    lane is not None
                    and lane.get("status") == "merged"
                    and lane.get("landing_commit_sha") == landing_commit
                    and lane.get("branch_tip_sha") is None
                    and isinstance(landing_commit, str)
                    and _HEX40.fullmatch(landing_commit) is not None
                    and getattr(parsed_receipt, "task_ref", None) == task_value
                    and getattr(parsed_receipt, "lane_id", None) == lane_value
                    and getattr(parsed_receipt, "tip", None) == tip_value
                    and isinstance(getattr(parsed_receipt, "gate_id", None), int)
                ):
                    pending = _project(
                        lanes=lanes,
                        lanes_recording=lanes_recording,
                        orchestrator_lanes=orchestrator_lanes,
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        landing_commit=landing_commit,
                        integration_ref=integration_value,
                        replay=True,
                    )
                    if not pending:
                        state, verdict, receipt_value = _find_natural_landing(
                            landing_log,
                            root,
                            task_ref=task_value,
                            lane_id=lane_value,
                            tip=tip_value,
                            integration_ref=integration_value,
                        )
            if (
                state == "indeterminate"
                and getattr(verdict, "failed_check", None) == "v3_identity"
                and _repair_stale_registration_tip(
                    lanes=lanes,
                    lanes_recording=lanes_recording,
                    verified_tests=verified_tests,
                    landing_log=landing_log,
                    orchestrator_lanes=orchestrator_lanes,
                    root=root,
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    integration_ref=integration_value,
                    lane=lane,
                    verdict=verdict,
                )
            ):
                state, verdict, receipt_value = _find_natural_landing(
                    landing_log,
                    root,
                    task_ref=task_value,
                    lane_id=lane_value,
                    tip=tip_value,
                    integration_ref=integration_value,
                )
            if state == "indeterminate":
                commit = getattr(verdict, "commit", None) if verdict is not None else None
                detail = receipt_value or "unverified_receipt:unknown"
                return _envelope(
                    "indeterminate",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail=detail,
                    landing_commit=commit if isinstance(commit, str) else None,
                )

            if state == "verified":
                try:
                    pending_heads = orchestrator_lanes._no_ff_merge_heads(root)
                    finalized = not pending_heads or orchestrator_lanes._merge_lane_no_ff(
                        root,
                        task_value,
                        lane_value,
                        tested_sha=tip_value,
                        run_id=request_run_id,
                        _lock_held=True,
                    )
                except Exception:
                    finalized = False
                if not finalized:
                    return _envelope(
                        "cleanup_pending",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        landing_commit=receipt_value,
                        detail="merge_finalization_unverified",
                    )

            branch_absent = False
            if state != "verified":
                current_branch, branch_error = _ref_oid(orchestrator_lanes, root, branch_ref)
                if branch_error:
                    return _envelope(
                        "merge_refused",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="branch_probe_failed",
                    )
                if current_branch is None:
                    branch_absent = True
                elif current_branch != tip_value:
                    return _envelope(
                        "moved",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail=f"tip:{current_branch}",
                    )

            integration_before, integration_error = _ref_oid(orchestrator_lanes, root, integration_value)
            if state != "verified" and (integration_error or integration_before is None):
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="integration_ref_unresolved",
                )
            pin_ref = f"refs/reclaimed/{lane_value}/{tip_value}"

            if state == "verified":
                landing_commit = receipt_value
                assert isinstance(landing_commit, str)
                parsed_receipt = getattr(verdict, "receipt", None)
                gate_id = getattr(parsed_receipt, "gate_id", None)
                landing_run_id = getattr(parsed_receipt, "run_id", None)
                if (
                    not isinstance(gate_id, int)
                    or not isinstance(landing_run_id, str)
                    or _HEX32.fullmatch(landing_run_id) is None
                ):
                    return _envelope(
                        "cleanup_pending",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="cleanup_intent_identity_unavailable",
                        landing_commit=landing_commit,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                    )
                from . import landing_cleanup

                cleanup_identity = {
                    "task_ref": task_value,
                    "lane_id": lane_value,
                    "expected_tip": tip_value,
                    "integration_ref": integration_value,
                    "landing_commit": landing_commit,
                    "run_id": landing_run_id,
                }
                try:
                    cleanup_intent = landing_cleanup.ensure_intent(
                        root,
                        identity=cleanup_identity,
                        lane_row=lane,
                    )
                except Exception as exc:
                    cleanup_intent = None
                    cleanup_error = f"cleanup_intent_write_failed:{type(exc).__name__}"
                else:
                    cleanup_error = None
                projection_pending = _project(
                    lanes=lanes,
                    lanes_recording=lanes_recording,
                    orchestrator_lanes=orchestrator_lanes,
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    landing_commit=landing_commit,
                    integration_ref=integration_value,
                    replay=True,
                )
                cleanup_result = (
                    _finish_cleanup(
                        manifest_root=manifest_root,
                        root=root,
                        identity=cleanup_identity,
                        intent=cleanup_intent,
                        lane_row=cleanup_intent.get("lane_row")
                        if isinstance(cleanup_intent.get("lane_row"), dict)
                        else lane,
                        lanes=lanes,
                        lanes_recording=lanes_recording,
                        orchestrator_lanes=orchestrator_lanes,
                        lane_reaping=lane_reaping,
                        projection_pending=projection_pending,
                        replay=True,
                    )
                    if cleanup_intent is not None
                    else {"complete": False, "detail": cleanup_error or "cleanup_intent_unavailable"}
                )
                cleanup_complete = cleanup_result.get("complete") is True
                return _envelope(
                    "already_landed" if cleanup_complete else "cleanup_pending",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    gate_id=gate_id,
                    landing_commit=landing_commit,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                    detail=None if cleanup_complete else str(cleanup_result.get("detail") or "cleanup_pending"),
                    bundle_path=cleanup_result.get("bundle_path")
                    if isinstance(cleanup_result.get("bundle_path"), str)
                    else None,
                    projection_pending=projection_pending,
                    retire_outcome=cleanup_result.get("retire_outcome")
                    if isinstance(cleanup_result.get("retire_outcome"), str)
                    else None,
                    cleanup_pending=not cleanup_complete,
                )

            carrier = landing_log.find_carrier(root, tip_value, main_ref=integration_value)
            if getattr(carrier, "error", None):
                if branch_absent and getattr(carrier, "error", None) == "commit_missing":
                    return _envelope(
                        "moved",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="branch_absent",
                    )
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail=f"ancestry_probe_failed:{carrier.error}",
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )
            if getattr(carrier, "contained", False):
                containing_tip = getattr(carrier, "main_tip", None)
                recorded_container = lane.get("landing_commit_sha")
                if isinstance(recorded_container, str) and _HEX40.fullmatch(recorded_container) is not None:
                    try:
                        recorded_proof = landing_log.find_carrier(root, tip_value, main_ref=recorded_container)
                        still_on_integration = orchestrator_lanes._run_no_ff_git(
                            root, "merge-base", "--is-ancestor", recorded_container, integration_value
                        )
                    except (OSError, subprocess.TimeoutExpired):
                        recorded_proof = None
                        still_on_integration = None
                    if (
                        recorded_proof is not None
                        and getattr(recorded_proof, "contained", False)
                        and getattr(recorded_proof, "error", None) is None
                        and still_on_integration is not None
                        and still_on_integration.returncode == 0
                    ):
                        containing_tip = recorded_container
                if not isinstance(containing_tip, str) or _HEX40.fullmatch(containing_tip) is None:
                    return _envelope(
                        "contained_no_receipt",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="containing_tip_unavailable",
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                    )
                from . import landing_cleanup

                contained_identity = {
                    "task_ref": task_value,
                    "lane_id": lane_value,
                    "expected_tip": tip_value,
                    "integration_ref": integration_value,
                    "landing_commit": containing_tip,
                    "run_id": request_run_id,
                }
                try:
                    contained_intent = landing_cleanup.find_intent(root, identity=contained_identity)
                except Exception:
                    contained_intent = None
                captured_row = (
                    contained_intent.get("lane_row")
                    if isinstance(contained_intent, dict) and isinstance(contained_intent.get("lane_row"), dict)
                    else None
                )
                if contained_intent is not None and captured_row is not None:
                    current_raw = lanes_recording.get_lane(lane_id=lane_value, task_ref=task_value)
                    current_lane = _lane_row(current_raw)
                    if (
                        current_lane is not None
                        and current_lane.get("status") == "merged"
                        and current_lane.get("landing_commit_sha") == containing_tip
                        and current_lane.get("branch") == branch
                        and current_lane.get("worktree_path") == captured_row.get("worktree_path")
                        and captured_row.get("branch") == branch
                        and captured_row.get("branch_tip_sha") == tip_value
                    ):
                        cleanup_result = _finish_cleanup(
                            manifest_root=manifest_root,
                            root=root,
                            identity=contained_identity,
                            intent=contained_intent,
                            lane_row=captured_row,
                            lanes=lanes,
                            lanes_recording=lanes_recording,
                            orchestrator_lanes=orchestrator_lanes,
                            lane_reaping=lane_reaping,
                            projection_pending=False,
                            replay=True,
                            contained_landing=True,
                        )
                        cleanup_complete = cleanup_result.get("complete") is True
                        return _envelope(
                            "contained_reaped" if cleanup_complete else "cleanup_pending",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=None if cleanup_complete else str(cleanup_result.get("detail") or "cleanup_pending"),
                            landing_commit=containing_tip,
                            integration_before=integration_before,
                            pin_ref=pin_ref,
                            bundle_path=(
                                cleanup_result.get("bundle_path")
                                if isinstance(cleanup_result.get("bundle_path"), str)
                                else None
                            ),
                            retire_outcome=(
                                cleanup_result.get("retire_outcome")
                                if isinstance(cleanup_result.get("retire_outcome"), str)
                                else None
                            ),
                            cleanup_pending=not cleanup_complete,
                        )
                return _envelope(
                    "contained_no_receipt",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            if branch_absent:
                return _envelope(
                    "moved",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="branch_absent",
                )

            symbolic = orchestrator_lanes._git_stdout(root, "symbolic-ref", "--quiet", "HEAD")
            target_ref = (
                integration_value if integration_value.startswith("refs/heads/") else f"refs/heads/{integration_value}"
            )
            if symbolic != target_ref:
                return _envelope(
                    "dirty_target",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="head_not_integration_branch",
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )
            try:
                status = orchestrator_lanes._run_no_ff_git(root, "status", "--porcelain", "--untracked-files=no")
            except (OSError, subprocess.TimeoutExpired):
                status = None
            if status is None or status.returncode != 0:
                return _envelope(
                    "dirty_target",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="tracked_status_unavailable",
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )
            if status.stdout.strip():
                return _envelope(
                    "dirty_target",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="tracked_changes",
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            try:
                gate_raw = verified_tests.get_verified_tests(
                    task_ref=task_value,
                    commit_sha=tip_value,
                    passed=True,
                    limit=1,
                )
            except Exception:
                gate_raw = None
            gate_id, gate_readable = _verified_test(gate_raw, tip_value)
            if gate_id is None:
                return _envelope(
                    "gate_missing",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail=None if gate_readable else "verified_tests_unreadable",
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            try:
                merge_tree = orchestrator_lanes._run_no_ff_git(
                    root,
                    "merge-tree",
                    "--write-tree",
                    "--name-only",
                    integration_value,
                    tip_value,
                )
            except (OSError, subprocess.TimeoutExpired):
                merge_tree = None
            if merge_tree is None or merge_tree.returncode not in {0, 1}:
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="merge_tree_probe_failed",
                    gate_id=gate_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )
            if merge_tree.returncode == 1:
                return _envelope(
                    "conflict",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    gate_id=gate_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                    conflicting_paths=_conflicting_paths(merge_tree.stdout, merge_tree.stderr),
                )

            # Validate the merge effect, allowing removal of legacy reviewer scratch.
            scratch_detail = None
            try:
                merged_tree = merge_tree.stdout.splitlines()[0].strip()
                scratch_probe = orchestrator_lanes._run_no_ff_git(
                    root,
                    "diff",
                    "--name-only",
                    "--no-renames",
                    "--diff-filter=AMTCR",
                    integration_before,
                    merged_tree,
                    "--",
                    ".review",
                    ".review-input",
                )
                if scratch_probe.returncode != 0:
                    scratch_detail = "review_scratch_probe_failed"
                elif scratch_probe.stdout.strip():
                    paths = scratch_probe.stdout.splitlines()[:10]
                    scratch_detail = "review_scratch_in_merge_effect:" + ",".join(paths)
                    payload_probe = orchestrator_lanes._run_no_ff_git(
                        root, "log", "--format=%H%x09%s", f"{integration_before}..{tip_value}"
                    )
                    if payload_probe.returncode != 0:
                        scratch_detail = "review_scratch_probe_failed"
                    else:
                        payload_sha = next(
                            (
                                line.partition("\t")[0][:10]
                                for line in payload_probe.stdout.splitlines()
                                if line.partition("\t")[2] == "chore(review): procure governed context payload"
                            ),
                            "none",
                        )
                        scratch_detail += f";payload_commit={payload_sha}"
            except (OSError, subprocess.TimeoutExpired, IndexError):
                scratch_detail = "review_scratch_probe_failed"
            if scratch_detail is not None:
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail=scratch_detail,
                    gate_id=gate_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            try:
                pin_current, pin_error = _ref_oid(orchestrator_lanes, root, pin_ref)
                if pin_error:
                    pin_ok = False
                elif pin_current == tip_value:
                    pin_ok = True
                elif pin_current is not None:
                    pin_ok = False
                else:
                    pin_proc = orchestrator_lanes._run_no_ff_git(
                        root,
                        "update-ref",
                        pin_ref,
                        tip_value,
                        "0" * 40,
                    )
                    pin_ok = pin_proc.returncode == 0
            except (OSError, subprocess.TimeoutExpired):
                pin_ok = False
            if not pin_ok:
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="pin_conflict",
                    gate_id=gate_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            try:
                bundle = lane_reaping._bundle_before_reap(root, branch)
            except Exception as exc:
                bundle = {"ok": False, "error": type(exc).__name__}
            if not isinstance(bundle, dict) or bundle.get("ok") is not True:
                detail = bundle.get("error") if isinstance(bundle, dict) else "unreadable_envelope"
                return _envelope(
                    "merge_refused",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail=f"bundle_failed:{detail or 'unknown'}",
                    gate_id=gate_id,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )
            bundle_path = bundle.get("bundle_path") if isinstance(bundle.get("bundle_path"), str) else None

            captured: dict[str, object] = {}

            def capture(_level: str, event: str, **fields: object) -> None:
                if event == "lane_no_ff_merge_fallback":
                    captured.update(fields)

            append_started = True
            helper_error: Exception | None = None
            try:
                merged = orchestrator_lanes._merge_lane_no_ff(
                    root,
                    task_value,
                    lane_value,
                    tested_sha=tip_value,
                    trailers=landing_log.format_landing_trailers(
                        task_ref=task_value,
                        lane_id=lane_value,
                        tip=tip_value,
                        gate_id=gate_id,
                        run_id=request_run_id,
                    ),
                    run_id=request_run_id,
                    _lock_held=True,
                    log=capture,
                )
            except Exception as exc:
                merged = False
                helper_error = exc

            if captured.get("reason") == "merge_finalization_unverified":
                return _envelope(
                    "cleanup_pending",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="merge_finalization_unverified",
                )

            try:
                current_head = orchestrator_lanes._git_stdout(root, "rev-parse", "HEAD")
                landing_commit: str | None = None
                landing_run_id: str | None = None
                if merged and current_head:
                    first_parent = orchestrator_lanes._git_stdout(root, "rev-parse", "HEAD^1")
                    second_parent = orchestrator_lanes._git_stdout(root, "rev-parse", "HEAD^2")
                    receipt = _receipt_verdict(landing_log, root, current_head, integration_value)
                    parsed = getattr(receipt, "receipt", None) if receipt is not None else None
                    if (
                        first_parent == integration_before
                        and second_parent == tip_value
                        and getattr(receipt, "verified", False)
                        and getattr(parsed, "task_ref", None) == task_value
                        and getattr(parsed, "lane_id", None) == lane_value
                        and getattr(parsed, "tip", None) == tip_value
                    ):
                        landing_commit = current_head
                        parsed_run_id = getattr(parsed, "run_id", None)
                        landing_run_id = parsed_run_id if isinstance(parsed_run_id, str) else None

                if landing_commit is None:
                    state_after, after_verdict, after_value = _find_natural_landing(
                        landing_log,
                        root,
                        task_ref=task_value,
                        lane_id=lane_value,
                        tip=tip_value,
                        integration_ref=integration_value,
                    )
                    if state_after == "verified" and isinstance(after_value, str):
                        landing_commit = after_value
                        parsed = getattr(after_verdict, "receipt", None)
                        found_gate_id = getattr(parsed, "gate_id", None)
                        if isinstance(found_gate_id, int):
                            gate_id = found_gate_id
                        parsed_run_id = getattr(parsed, "run_id", None)
                        landing_run_id = parsed_run_id if isinstance(parsed_run_id, str) else None

                if landing_commit is None:
                    head_after = orchestrator_lanes._git_stdout(root, "rev-parse", "HEAD")
                    if head_after == integration_before:
                        reason = captured.get("reason")
                        conflicts = captured.get("conflicting_paths")
                        detail = str(
                            reason
                            or (
                                f"merge_helper_failed:{type(helper_error).__name__}"
                                if helper_error
                                else "merge_refused"
                            )
                        )
                        if detail == "merge_conflict":
                            return _envelope(
                                "conflict",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                gate_id=gate_id,
                                integration_before=integration_before,
                                pin_ref=pin_ref,
                                bundle_path=bundle_path,
                                conflicting_paths=conflicts if isinstance(conflicts, list) else [],
                            )
                        if detail == "integration_dirty_or_unresolved":
                            return _envelope(
                                "dirty_target",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail=detail,
                                gate_id=gate_id,
                                integration_before=integration_before,
                                pin_ref=pin_ref,
                                bundle_path=bundle_path,
                            )
                        if detail == "merge_blocked_git_index_lock_held":
                            return _envelope(
                                "lock_held",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail=detail,
                                gate_id=gate_id,
                                integration_before=integration_before,
                                pin_ref=pin_ref,
                                bundle_path=bundle_path,
                            )
                        return _envelope(
                            "merge_refused",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=detail,
                            gate_id=gate_id,
                            integration_before=integration_before,
                            pin_ref=pin_ref,
                            bundle_path=bundle_path,
                        )
                    if state_after == "indeterminate":
                        return _envelope(
                            "indeterminate",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=after_value or "unverified_receipt:unknown",
                            gate_id=gate_id,
                            integration_before=integration_before,
                            pin_ref=pin_ref,
                            bundle_path=bundle_path,
                        )
                    return _envelope(
                        "indeterminate",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail=f"head_after_append:{head_after or 'unreadable'}",
                        next_action=f"re-run with the same run_id {request_run_id}",
                        gate_id=gate_id,
                        landing_commit=head_after,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                        bundle_path=bundle_path,
                    )

                if not isinstance(landing_run_id, str) or _HEX32.fullmatch(landing_run_id) is None:
                    return _envelope(
                        "indeterminate",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="verified_receipt_run_id_unavailable",
                        gate_id=gate_id,
                        landing_commit=landing_commit,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                        bundle_path=bundle_path,
                    )
                from . import landing_cleanup

                cleanup_identity = {
                    "task_ref": task_value,
                    "lane_id": lane_value,
                    "expected_tip": tip_value,
                    "integration_ref": integration_value,
                    "landing_commit": landing_commit,
                    "run_id": landing_run_id,
                }
                try:
                    cleanup_intent = landing_cleanup.ensure_intent(
                        root,
                        identity=cleanup_identity,
                        lane_row=lane,
                    )
                except Exception as cleanup_exc:
                    cleanup_intent = None
                    cleanup_error = f"cleanup_intent_write_failed:{type(cleanup_exc).__name__}"
                else:
                    cleanup_error = None

                projection_pending = _project(
                    lanes=lanes,
                    lanes_recording=lanes_recording,
                    orchestrator_lanes=orchestrator_lanes,
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    landing_commit=landing_commit,
                    integration_ref=integration_value,
                    replay=False,
                )
                cleanup_result = (
                    _finish_cleanup(
                        manifest_root=manifest_root,
                        root=root,
                        identity=cleanup_identity,
                        intent=cleanup_intent,
                        lane_row=cleanup_intent.get("lane_row")
                        if isinstance(cleanup_intent.get("lane_row"), dict)
                        else lane,
                        lanes=lanes,
                        lanes_recording=lanes_recording,
                        orchestrator_lanes=orchestrator_lanes,
                        lane_reaping=lane_reaping,
                        projection_pending=projection_pending,
                        replay=False,
                    )
                    if cleanup_intent is not None
                    else {"complete": False, "detail": cleanup_error or "cleanup_intent_unavailable"}
                )
                cleanup_complete = cleanup_result.get("complete") is True
                return _envelope(
                    "landed" if cleanup_complete else "cleanup_pending",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    gate_id=gate_id,
                    landing_commit=landing_commit,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                    detail=None if cleanup_complete else str(cleanup_result.get("detail") or "cleanup_pending"),
                    bundle_path=(
                        cleanup_result.get("bundle_path")
                        if isinstance(cleanup_result.get("bundle_path"), str)
                        else bundle_path
                    ),
                    projection_pending=projection_pending,
                    retire_outcome=(
                        cleanup_result.get("retire_outcome")
                        if isinstance(cleanup_result.get("retire_outcome"), str)
                        else None
                    ),
                    cleanup_pending=not cleanup_complete,
                )
            except Exception as exc:
                # The append has started; resolve its natural key while still serialized.
                try:
                    head_after = orchestrator_lanes._git_stdout(root, "rev-parse", "HEAD")
                    state_after, verdict_after, value_after = _find_natural_landing(
                        landing_log,
                        root,
                        task_ref=task_value,
                        lane_id=lane_value,
                        tip=tip_value,
                        integration_ref=integration_value,
                    )
                    if state_after == "verified" and isinstance(value_after, str):
                        parsed_after = getattr(verdict_after, "receipt", None)
                        recovered_run_id = getattr(parsed_after, "run_id", None)
                        if not isinstance(recovered_run_id, str) or _HEX32.fullmatch(recovered_run_id) is None:
                            return _envelope(
                                "cleanup_pending",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail="cleanup_intent_identity_unavailable",
                                landing_commit=value_after,
                            )
                        from . import landing_cleanup

                        recovery_identity = {
                            "task_ref": task_value,
                            "lane_id": lane_value,
                            "expected_tip": tip_value,
                            "integration_ref": integration_value,
                            "landing_commit": value_after,
                            "run_id": recovered_run_id,
                        }
                        recovery_intent = landing_cleanup.ensure_intent(
                            root,
                            identity=recovery_identity,
                            lane_row=lane,
                        )
                        pending = _project(
                            lanes=lanes,
                            lanes_recording=lanes_recording,
                            orchestrator_lanes=orchestrator_lanes,
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            landing_commit=value_after,
                            integration_ref=integration_value,
                            replay=True,
                        )
                        recovery_result = _finish_cleanup(
                            manifest_root=manifest_root,
                            root=root,
                            identity=recovery_identity,
                            intent=recovery_intent,
                            lane_row=recovery_intent.get("lane_row")
                            if isinstance(recovery_intent.get("lane_row"), dict)
                            else lane,
                            lanes=lanes,
                            lanes_recording=lanes_recording,
                            orchestrator_lanes=orchestrator_lanes,
                            lane_reaping=lane_reaping,
                            projection_pending=pending,
                            replay=True,
                        )
                        outcome = "already_landed" if recovery_result.get("complete") is True else "cleanup_pending"
                        recovered_gate = getattr(parsed_after, "gate_id", None)
                        return _envelope(
                            outcome,
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            gate_id=recovered_gate if isinstance(recovered_gate, int) else None,
                            landing_commit=value_after,
                            integration_before=integration_before if "integration_before" in locals() else None,
                            pin_ref=pin_ref if "pin_ref" in locals() else None,
                            detail=None
                            if recovery_result.get("complete") is True
                            else str(recovery_result.get("detail") or "cleanup_pending"),
                            bundle_path=recovery_result.get("bundle_path")
                            if isinstance(recovery_result.get("bundle_path"), str)
                            else None,
                            projection_pending=pending,
                            retire_outcome=recovery_result.get("retire_outcome")
                            if isinstance(recovery_result.get("retire_outcome"), str)
                            else None,
                            cleanup_pending=recovery_result.get("complete") is not True,
                        )
                    old_head = integration_before if "integration_before" in locals() else None
                    if head_after is not None and head_after == old_head:
                        reason = captured.get("reason") if "captured" in locals() else None
                        conflicts = captured.get("conflicting_paths") if "captured" in locals() else None
                        detail = str(reason or f"append_recovery_failed:{type(exc).__name__}")
                        if detail == "merge_conflict":
                            return _envelope(
                                "conflict",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                gate_id=gate_id if "gate_id" in locals() else None,
                                integration_before=old_head,
                                pin_ref=pin_ref if "pin_ref" in locals() else None,
                                bundle_path=bundle_path if "bundle_path" in locals() else None,
                                conflicting_paths=conflicts if isinstance(conflicts, list) else [],
                            )
                        if detail == "integration_dirty_or_unresolved":
                            return _envelope(
                                "dirty_target",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail=detail,
                                gate_id=gate_id if "gate_id" in locals() else None,
                                integration_before=old_head,
                                pin_ref=pin_ref if "pin_ref" in locals() else None,
                                bundle_path=bundle_path if "bundle_path" in locals() else None,
                            )
                        if detail == "merge_blocked_git_index_lock_held":
                            return _envelope(
                                "lock_held",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail=detail,
                                gate_id=gate_id if "gate_id" in locals() else None,
                                integration_before=old_head,
                                pin_ref=pin_ref if "pin_ref" in locals() else None,
                                bundle_path=bundle_path if "bundle_path" in locals() else None,
                            )
                        return _envelope(
                            "merge_refused",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=detail,
                            gate_id=gate_id if "gate_id" in locals() else None,
                            integration_before=old_head,
                            pin_ref=pin_ref if "pin_ref" in locals() else None,
                            bundle_path=bundle_path if "bundle_path" in locals() else None,
                        )
                    return _envelope(
                        "indeterminate",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail=value_after or f"append_recovery_failed:{type(exc).__name__}",
                        next_action=f"re-run with the same run_id {request_run_id}",
                        landing_commit=getattr(verdict_after, "commit", None) or head_after,
                        integration_before=integration_before if "integration_before" in locals() else None,
                        pin_ref=pin_ref if "pin_ref" in locals() else None,
                        bundle_path=bundle_path if "bundle_path" in locals() else None,
                    )
                except Exception as recovery_exc:
                    return _envelope(
                        "indeterminate",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail=f"append_recovery_failed:{type(recovery_exc).__name__}",
                        next_action=f"re-run with the same run_id {request_run_id}",
                        integration_before=integration_before if "integration_before" in locals() else None,
                        pin_ref=pin_ref if "pin_ref" in locals() else None,
                        bundle_path=bundle_path if "bundle_path" in locals() else None,
                    )
    except Exception as exc:
        if "append_started" not in locals():
            return _envelope(
                "merge_refused",
                task_ref=task_value,
                lane_id=lane_value,
                expected_tip=tip_value,
                run_id=request_run_id,
                detail=f"operation_failed:{type(exc).__name__}",
            )
        return _envelope(
            "indeterminate",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail=f"landing_lock_exit_failed:{type(exc).__name__}",
            next_action=f"re-run with the same run_id {request_run_id}",
            integration_before=integration_before if "integration_before" in locals() else None,
            pin_ref=pin_ref if "pin_ref" in locals() else None,
            bundle_path=bundle_path if "bundle_path" in locals() else None,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lane_land")
    parser.add_argument("--workspace-root", default=".")
    parser.add_argument("--task", required=True)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--expected-tip", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--integration-ref")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    workspace_root = Path(args.workspace_root).expanduser().resolve()
    from ..api import configure_runtime
    from ..cli import _build_config

    configure_runtime(_build_config(workspace_root))
    result = land(
        args.task,
        args.lane,
        args.expected_tip,
        run_id=args.run_id,
        integration_ref=args.integration_ref,
        orchestrator_root=workspace_root,
    )
    print(json.dumps(result, sort_keys=True))
    return EXIT_CODES.get(str(result.get("outcome")), 10)


if __name__ == "__main__":
    sys.exit(main())
