"""Receipt-bearing, idempotent landing of one worktree lane."""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import logging
import re
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_SUCCESS = frozenset({"landed", "already_landed", "landed_then_moved", "contained_reaped", "contained_retired"})
EXIT_CODES = {
    "landed": 0,
    "already_landed": 0,
    "landed_then_moved": 3,
    "lock_held": 4,
    "moved": 5,
    "gate_missing": 6,
    "dirty_target": 7,
    "conflict": 8,
    "contained_no_receipt": 14,
    "contained_kept": 14,
    "merge_refused": 10,
    "probe_unavailable": 15,
    "indeterminate": 11,
    "lane_unresolved": 12,
    "invalid_request": 2,
    "cleanup_pending": 13,
    "contained_reaped": 0,
    "contained_retired": 0,
}
_NEXT_ACTIONS = {
    "landed_then_moved": "the branch has new commits; land the new tip separately",
    "lock_held": "retry after the current landing finishes",
    "moved": "re-read the branch tip and pass it as expected_tip",
    "gate_missing": "record a passing gate at the tip",
    "dirty_target": "clean or switch the integration checkout",
    "conflict": "rebase or fix the lane",
    "merge_refused": "inspect the refusal detail",
    "probe_unavailable": "retry with the same run_id",
    "contained_no_receipt": "retry after the recorded safety refusal clears",
    "contained_kept": "retry after the recorded safety refusal clears",
    "contained_retired": "none",
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
    retire_detail: str | None = None,
    error_kind: str | None = None,
    lock_path: str | None = None,
    lock_kind: str | None = None,
    age_s: int | float | None = None,
    cleanup_pending: bool | None = None,
    fence_release: list[dict[str, object]] | None = None,
    landed_marker: str | None = None,
    landed_marker_blockers: list[str] | None = None,
    conflicting_paths: list[str] | None = None,
    predecessors_retired: list[dict[str, object]] | None = None,
    predecessors_kept: list[dict[str, object]] | None = None,
    supersede_tombstones: list[dict[str, object]] | None = None,
    sweep_wake: dict[str, object] | None = None,
    supersede_tip_binding: str | None = None,
    waited_seconds: float | None = None,
    lock_holder: dict[str, object] | None = None,
    include_lock_holder: bool = False,
    lock_holder_reason: str | None = None,
) -> dict[str, object]:
    """Build a land response with a stable marker shape.

    ``landed_marker`` is always a string or null; ``landed_marker_blockers``
    contains lane IDs when a stale marker could not be removed safely.
    """
    pending = outcome == "cleanup_pending" if cleanup_pending is None else cleanup_pending
    if next_action is None:
        next_action = _NEXT_ACTIONS.get(outcome)
        if outcome == "merge_refused":
            next_action = detail or next_action
    if outcome in {"landed", "already_landed"} and projection_pending:
        next_action = "re-run to heal the projection"
    response = {
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
        "retire_detail": retire_detail,
        "error_kind": error_kind,
        "lock_path": lock_path,
        "lock_kind": lock_kind,
        "age_s": age_s,
        "landed_marker": landed_marker,
        "landed_marker_blockers": landed_marker_blockers or [],
        "cleanup_pending": pending,
        "integrated": outcome in _SUCCESS or pending,
        "done": outcome in _SUCCESS and not pending,
        "conflicting_paths": conflicting_paths or [],
        "predecessors_retired": predecessors_retired or [],
        "predecessors_kept": predecessors_kept or [],
        "supersede_tombstones": supersede_tombstones or [],
        "sweep_wake": sweep_wake,
        "supersede_tip_binding": supersede_tip_binding,
    }
    if waited_seconds is not None:
        response["waited_seconds"] = waited_seconds
    if fence_release:
        response["fence_release"] = [dict(record) for record in fence_release]
    if include_lock_holder:
        response["lock_holder"] = lock_holder
        response["lock_holder_reason"] = lock_holder_reason
    return response


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


def _cleanup_lane_snapshot(lane: Mapping[str, object], identity: Mapping[str, str]) -> dict[str, object]:
    """Pin the verified landing identity into the cleanup journal snapshot."""

    snapshot = dict(lane)
    snapshot.update(
        {
            "task_ref": identity["task_ref"],
            "lane_id": identity["lane_id"],
            "branch_tip_sha": identity["expected_tip"],
            "landing_commit_sha": identity["landing_commit"],
            "status": "merged",
        }
    )
    return snapshot


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


def _mark_manifest_landed(
    root: Path,
    manifest_root: str | Path | None,
    task_ref: str,
    lane_id: str,
    landing_commit: str | None,
    *,
    branch_ref: str | None,
    expected_tip: str,
    orchestrator_lanes: object,
) -> tuple[str | None, list[str]]:
    """Project a verified receipt into advisory manifest metadata."""
    from . import lane_manifest

    marker_status: str | None = None

    def probe(ref: str | None) -> tuple[str | None, str | None]:
        if ref is None or orchestrator_lanes is None:
            return None, "probe_failed"
        return _ref_oid(orchestrator_lanes, root, ref)

    try:
        path = lane_manifest.manifest_dir(orchestrator_root=str(root), manifest_dir=manifest_root) / f"{task_ref}.json"
        manifest = lane_manifest.load_manifest(task_ref, manifest_dir=path.parent)
        previous = manifest["lanes"][lane_id].get("landed")
        if landing_commit is None and "landed" not in manifest["lanes"][lane_id]:
            return None, []
        if isinstance(previous, dict) and previous.get("sha") == landing_commit and previous.get("tip") == expected_tip:
            current, branch_error = probe(branch_ref)
            if branch_error is None and current is None:
                return None, []

        def mutate(data: dict) -> None:
            nonlocal marker_status
            lane = data["lanes"][lane_id]
            if landing_commit is None:
                lane.pop("landed", None)
                return
            if branch_ref is None:
                current, branch_error = None, "probe_failed"
            else:
                current, branch_error = probe(branch_ref)
            if branch_error:
                marker_status = "withheld_branch_probe_failed"
                lane.pop("landed", None)
                return
            if current is not None and current != expected_tip:
                marker_status = "withheld_tip_moved"
                lane.pop("landed", None)
                return
            previous = lane.get("landed")
            if not isinstance(previous, dict) or previous.get("sha") != landing_commit:
                previous = {"sha": landing_commit, "at": datetime.now(timezone.utc).isoformat()}
            else:
                previous = dict(previous)
                if not isinstance(previous.get("at"), str):
                    previous["at"] = datetime.now(timezone.utc).isoformat()
            previous["tip"] = expected_tip
            lane["landed"] = previous

        lane_manifest.atomic_update_manifest(path, mutate)
        if landing_commit is not None and marker_status is None:
            current, branch_error = probe(branch_ref)
            if branch_error:
                marker_status = "withheld_branch_probe_failed"
            elif current is not None and current != expected_tip:
                marker_status = "withheld_tip_moved"

            if marker_status is not None:

                def withdraw(data: dict) -> None:
                    lane = data["lanes"][lane_id]
                    landed = lane.get("landed")
                    if (
                        isinstance(landed, dict)
                        and landed.get("sha") == landing_commit
                        and landed.get("tip") == expected_tip
                    ):
                        lane.pop("landed", None)

                lane_manifest.atomic_update_manifest(path, withdraw)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        logging.getLogger(__name__).warning(
            "Skipping advisory landed marker for task %s lane %s", task_ref, lane_id, exc_info=True
        )
        match = re.search(r"owned_paths collision between lanes '([^']+)' and '([^']+)'", str(exc))
        if match and lane_id in match.groups():
            return "removal_blocked", sorted(match.groups())
        return marker_status, []
    return marker_status, []


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
    root: Path,
    manifest_root: str | Path | None,
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
) -> tuple[str, str, str | None, str | None, dict[str, object]]:
    if projection_pending:
        return "landed", "skipped:projection_pending", None, None, {}
    branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    current, error = _ref_oid(orchestrator_lanes, root, branch_ref)
    if error:
        return "landed", "skipped:branch_probe_failed", None, None, {}
    if current != expected_tip:
        if current is None and replay:
            pass
        elif current is None:
            return "landed", "branch_absent_before_cleanup", None, None, {}
        else:
            return "landed_then_moved", "tip_moved_after_landing", None, None, {}
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
            # _finish_cleanup runs inside _land_once's landing lock.
            landing_lock_held=True,
        )
    except Exception as exc:
        return "landed", f"skipped:retire_error:{type(exc).__name__}", None, None, {}
    payload = _raw_object(raw)
    if payload is None:
        return "landed", "skipped:unreadable_envelope", None, None, {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return "landed", "skipped:unreadable_envelope", None, None, {}
    outcome = str(data.get("outcome") or data.get("error_kind") or "unknown")
    if data.get("error_kind") == outcome and outcome.startswith("refused_"):
        # Keep the stable probe detail without repeating the refusal code as
        # both the outcome and an error-kind suffix.
        data = dict(data)
        data.pop("error_kind", None)
    bundle_path = data.get("bundle_path") if isinstance(data.get("bundle_path"), str) else None
    if payload.get("ok") is True or data.get("ok") is True:
        if outcome == "retired" and data.get("bundle_tip_sha") != expected_tip:
            return "landed", "skipped:bundle_tip_unverified", None, None, _retire_release_fields(data)
        return "landed", outcome, bundle_path, None, _retire_release_fields(data)
    detail, retire_fields = _retire_refusal_detail(data, outcome)
    if outcome == "tip_moved_after_bundle":
        return "landed_then_moved", outcome, bundle_path, detail, retire_fields
    if replay and outcome == "already_retired":
        return "landed", outcome, bundle_path, detail, retire_fields
    return "landed", f"skipped:{outcome}", bundle_path, detail, retire_fields


def _retire_release_fields(*sources: object) -> dict[str, object]:
    """Keep release effects independently of outcomes; landing's pending flag is boolean."""
    records: list[dict[str, object]] = []
    pending = False
    for source in sources:
        if not isinstance(source, dict):
            continue
        releases = source.get("fence_release")
        if isinstance(releases, list):
            for record in releases:
                if isinstance(record, dict) and record not in records:
                    records.append(dict(record))
        pending = pending or bool(source.get("cleanup_pending", bool(releases)))
    if not records and not pending:
        return {}
    return {**({"fence_release": records} if records else {}), "cleanup_pending": pending}


def _retire_fence_recovered(
    lanes_recording: object, identity: Mapping[str, str], lane_row: Mapping[str, object]
) -> bool:
    """Observe recovery on the same durable row; never release or take over a fence here."""
    if type(lane_row.get("id")) is not int:
        return False
    try:
        current = _lane_row(lanes_recording.get_lane(task_ref=identity["task_ref"], lane_id=identity["lane_id"]))
    except Exception:
        return False
    return (
        current is not None
        and current.get("task_ref") == identity["task_ref"]
        and current.get("lane_id") == identity["lane_id"]
        and all(current.get(key) == lane_row.get(key) for key in ("id", "identity_seq", "branch", "worktree_path"))
        and "reclaim_fence" in current
        and current["reclaim_fence"] is None
    )


def _retire_refusal_detail(data: dict[str, object], outcome: str) -> tuple[str, dict[str, object]]:
    detail = next(
        (
            value.strip()
            for key in ("detail", "worktree_reclaim_detail", "error")
            if isinstance((value := data.get(key)), str) and value.strip()
        ),
        outcome,
    )
    fields = _retire_release_fields(data)
    suffix_parts = []
    error_kind = data.get("error_kind")
    if isinstance(error_kind, str) and error_kind.strip():
        error_kind = error_kind.strip()
        fields["error_kind"] = error_kind
        suffix_parts.append(f"error_kind={error_kind}")
    for key in ("lock_path", "lock_kind", "age_s"):
        value = data.get(key)
        if key in {"lock_path", "lock_kind"}:
            if not isinstance(value, str) or not value.strip():
                continue
            value = value.strip()
        elif isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        fields[key] = value
        suffix_parts.append(f"{key}={value}")
    if not suffix_parts:
        return detail[:300], fields
    suffix = "; ".join(suffix_parts)
    detail_limit = 300 - len(suffix) - 2
    if detail_limit <= 0:
        return suffix[:300], fields
    return f"{detail[:detail_limit]}; {suffix}"[:300], fields


def _retire_envelope_fields(cleanup_result: object) -> dict[str, object]:
    if not isinstance(cleanup_result, dict):
        return {}
    fields = _retire_release_fields(cleanup_result)
    # Public landing call sites already derive this boolean from completion.
    fields.pop("cleanup_pending", None)
    detail = cleanup_result.get("retire_detail")
    if isinstance(detail, str):
        fields["retire_detail"] = detail
    error_kind = cleanup_result.get("error_kind")
    if isinstance(error_kind, str):
        fields["error_kind"] = error_kind
    for key in ("lock_path", "lock_kind"):
        value = cleanup_result.get(key)
        if isinstance(value, str):
            fields[key] = value
    age_s = cleanup_result.get("age_s")
    if isinstance(age_s, (int, float)) and not isinstance(age_s, bool):
        fields["age_s"] = age_s
    return fields


def _landed_marker_envelope_fields(cleanup_result: object) -> dict[str, object]:
    if not isinstance(cleanup_result, dict):
        return {}
    marker = cleanup_result.get("landed_marker")
    blockers = cleanup_result.get("landed_marker_blockers")
    return {
        "landed_marker": marker if isinstance(marker, str) else None,
        "landed_marker_blockers": [lane_id for lane_id in blockers if isinstance(lane_id, str)]
        if isinstance(blockers, list)
        else [],
    }


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


def _find_matching_contained_intent(root: Path, identity: Mapping[str, str], landing_cleanup: object) -> dict | None:
    """Find a durable containment intent even when the caller used another run ID."""

    try:
        exact = landing_cleanup.find_intent(root, identity=identity)
    except Exception:
        exact = None
    if isinstance(exact, dict):
        return exact

    cursor: str | None = None
    matches: list[dict] = []
    try:
        for _ in range(20):
            page = landing_cleanup.read_pending_intents(
                root,
                task_ref=identity["task_ref"],
                integration_ref=identity["integration_ref"],
                cursor=cursor,
                limit=1_000,
                budget_seconds=0.2,
                include_completed=True,
            )
            intents = page.get("intents")
            if not isinstance(intents, list):
                return None
            for intent in intents:
                if not isinstance(intent, dict) or not isinstance(intent.get("identity"), Mapping):
                    continue
                saved = intent["identity"]
                if all(
                    saved.get(key) == identity.get(key)
                    for key in ("task_ref", "lane_id", "expected_tip", "integration_ref", "landing_commit")
                ):
                    matches.append(intent)
            if page.get("has_more") is not True:
                break
            next_cursor = page.get("cursor")
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                return None
            cursor = next_cursor
    except Exception:
        return None
    if not matches:
        return None
    matches.sort(
        key=lambda item: (
            item.get("local_status") == "complete" and item.get("remote_status") == "complete",
            item.get("created_at_ns") if type(item.get("created_at_ns")) is int else 0,
        ),
        reverse=True,
    )
    return matches[0]


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
    cleanup_deadline: float | None = None,
) -> dict[str, object]:
    """Update the ownership exemption only after guarded cleanup succeeds."""
    if replay and intent.get("remote_status") == "pending":
        attempts = intent.get("attempts", 0)
        created_at_ns = intent.get("created_at_ns")
        age_seconds = max(0.0, time.time() - created_at_ns / 1_000_000_000) if type(created_at_ns) is int else 0.0
        if (type(attempts) is int and attempts >= 8) or age_seconds > 24 * 60 * 60:
            return {
                "complete": False,
                "detail": "remote_cleanup_stale",
                "local_status": intent.get("local_status", "pending"),
                "remote_status": "pending",
                "retire_outcome": "cleanup_pending",
                **_retire_release_fields(intent.get("local_result")),
            }
        next_attempt_at = intent.get("next_attempt_at")
        if isinstance(next_attempt_at, (int, float)) and not isinstance(next_attempt_at, bool):
            if float(next_attempt_at) > time.time():
                return {
                    "complete": False,
                    "detail": "remote_cleanup_not_due",
                    "local_status": intent.get("local_status", "pending"),
                    "remote_status": "pending",
                    "retire_outcome": "cleanup_pending",
                    **_retire_release_fields(intent.get("local_result")),
                }
    result = _finish_cleanup_impl(
        root=root,
        identity=identity,
        intent=intent,
        lane_row=lane_row,
        lanes=lanes,
        lanes_recording=lanes_recording,
        orchestrator_lanes=orchestrator_lanes,
        lane_reaping=lane_reaping,
        projection_pending=projection_pending,
        replay=replay,
        contained_landing=contained_landing,
        manifest_root=manifest_root,
        cleanup_deadline=cleanup_deadline,
    )
    complete = result.get("complete") is True
    if complete and orchestrator_lanes is not None:
        branch = lane_row.get("branch")
        if not isinstance(branch, str) or not branch.strip():
            complete = False
            result["detail"] = "captured_branch_unavailable"
            result["retire_outcome"] = "skipped:branch_unavailable"
        else:
            branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
            current, branch_error = _ref_oid(orchestrator_lanes, root, branch_ref)
            if branch_error:
                complete = False
                result["detail"] = "branch_probe_failed_after_cleanup"
                result["retire_outcome"] = "skipped:branch_probe_failed"
            elif current is not None and current != identity["expected_tip"]:
                complete = False
                result["detail"] = "tip_moved_after_landing"
                result["retire_outcome"] = "tip_moved_after_landing"
    branch = lane_row.get("branch")
    branch_ref = (
        branch
        if isinstance(branch, str) and branch.startswith("refs/heads/")
        else (f"refs/heads/{branch}" if isinstance(branch, str) and branch.strip() else None)
    )
    marker_result, marker_blockers = _mark_manifest_landed(
        root,
        manifest_root,
        identity["task_ref"],
        identity["lane_id"],
        identity["landing_commit"] if complete else None,
        branch_ref=branch_ref,
        expected_tip=identity["expected_tip"],
        orchestrator_lanes=orchestrator_lanes,
    )
    if marker_result is not None:
        result["landed_marker"] = marker_result
    if marker_blockers:
        result["landed_marker_blockers"] = marker_blockers
    if marker_result == "withheld_tip_moved":
        complete = False
        result["detail"] = "tip_moved_after_landing"
        result["retire_outcome"] = "tip_moved_after_landing"
    elif marker_result == "withheld_branch_probe_failed":
        complete = False
        result["detail"] = "branch_probe_failed_after_marker_check"
        result["retire_outcome"] = "skipped:branch_probe_failed"
    elif marker_result == "removal_blocked":
        complete = False
        result.setdefault("detail", "landed_marker_removal_blocked")
    if not complete:
        if marker_result is None:
            moved = "tip_moved" in str(result.get("retire_outcome"))
            result["landed_marker"] = "withheld_tip_moved" if moved else "withheld_cleanup_pending"
        result["complete"] = False
    return result


def _finish_cleanup_impl(
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
    cleanup_deadline: float | None = None,
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
            **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
                    "complete": False,
                    "detail": "cleanup_intent_update_failed",
                    "local_status": local_status,
                    "remote_status": "pending",
                }
            intent = saved
            remote_status = "complete"
    if projection_pending:
        return {
            **_retire_release_fields(local_result),
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
                **_retire_release_fields(local_result),
                "complete": False,
                "detail": "branch_probe_failed",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": "skipped:branch_probe_failed",
            }
        if current is not None and current != identity["expected_tip"]:
            return {
                **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
                    "complete": False,
                    "detail": "exact_tip_bundle_unverified",
                    "local_status": "pending",
                    "remote_status": remote_status,
                    "retire_outcome": "skipped:bundle_unverified",
                }
            local_result = {
                **_retire_release_fields(local_result),
                "phase": "bundle_verified",
                "bundle_path": bundle.get("bundle_path"),
                "bundle_tip_sha": bundle.get("tip_sha"),
            }
            saved = _persist_intent(root, landing_cleanup, intent, local_result=local_result)
            if saved is None:
                return {
                    **_retire_release_fields(local_result),
                    "complete": False,
                    "detail": "cleanup_intent_update_failed",
                    "local_status": "pending",
                    "remote_status": remote_status,
                    "retire_outcome": "skipped:intent_update_failed",
                }
            intent = saved
            bundle_path = local_result.get("bundle_path") if isinstance(local_result.get("bundle_path"), str) else None
        elif (
            local_result.get("phase") not in {"bundle_verified", "local_pending"}
            or local_result.get("bundle_tip_sha") != identity["expected_tip"]
            or not _bundle_proves_tip(root, lane_reaping, local_result.get("bundle_path"), identity["expected_tip"])
        ):
            return {
                **_retire_release_fields(local_result),
                "complete": False,
                "detail": "absent_branch_without_durable_bundle_proof",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": "skipped:bundle_proof_missing",
            }

        if cleanup_deadline is not None and cleanup_deadline - time.monotonic() <= 0:
            retire_outcome = "cleanup_deadline_exhausted"
            failed = {
                **_retire_release_fields(local_result),
                "phase": "local_pending",
                "outcome": retire_outcome,
                "bundle_path": bundle_path,
                "bundle_tip_sha": local_result.get("bundle_tip_sha"),
                "detail": retire_outcome,
            }
            saved = _persist_intent(root, landing_cleanup, intent, local_result=failed)
            if saved is not None:
                intent = saved
                local_status = intent.get("local_status")
            return {
                **_retire_release_fields(local_result),
                "complete": False,
                "detail": retire_outcome,
                "error_kind": retire_outcome,
                "local_status": local_status or "pending",
                "remote_status": remote_status,
                "retire_outcome": retire_outcome,
                "retire_detail": retire_outcome,
                "bundle_path": bundle_path,
            }

        final_outcome, retire_outcome, retired_bundle, retire_detail, retire_fields = _retire(
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
        fresh_release_fields = _retire_release_fields(retire_fields)
        release_fields = _retire_release_fields(local_result, fresh_release_fields)
        if (
            release_fields.get("cleanup_pending") is True
            and fresh_release_fields.get("cleanup_pending") is not True
            and _retire_fence_recovered(lanes_recording, identity, lane_row)
        ):
            release_fields["cleanup_pending"] = False
        retire_fields.update(release_fields)
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
            or retire_fields.get("cleanup_pending") is True
            or not _bundle_proves_tip(root, lane_reaping, bundle_path, identity["expected_tip"])
        ):
            failed = {
                "phase": "local_pending",
                "outcome": retire_outcome,
                "bundle_path": bundle_path,
                "bundle_tip_sha": local_result.get("bundle_tip_sha"),
                **({"detail": retire_detail} if retire_detail is not None else {}),
                **retire_fields,
            }
            saved = _persist_intent(root, landing_cleanup, intent, local_status="pending", local_result=failed)
            if saved is not None:
                intent = saved
                local_status = intent.get("local_status")
            return {
                **_retire_release_fields(local_result),
                "complete": False,
                "detail": (f"local_cleanup_pending:{retire_outcome}" + (f":{retire_detail}" if retire_detail else ""))[
                    :300
                ],
                "local_status": local_status or "pending",
                "remote_status": remote_status,
                "retire_outcome": retire_outcome,
                "retire_detail": retire_detail,
                **retire_fields,
                "bundle_path": bundle_path,
            }
        completed_local = {
            **retire_fields,
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
                **_retire_release_fields(local_result),
                "complete": False,
                "detail": "cleanup_intent_update_failed",
                "local_status": "pending",
                "remote_status": remote_status,
                "retire_outcome": retire_outcome,
                **retire_fields,
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
                **_retire_release_fields(local_result),
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
                **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
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
            remote_runner = None
            if cleanup_deadline is not None:
                remaining = cleanup_deadline - time.monotonic()
                if remaining <= 0:
                    pending_remote = {"ok": False, "status": "pending", "reason": "cleanup_deadline_exhausted"}
                    saved = _persist_intent(root, landing_cleanup, intent, remote_result=pending_remote)
                    if saved is not None:
                        intent = saved
                    return {
                        **_retire_release_fields(local_result),
                        "complete": False,
                        "detail": "cleanup_deadline_exhausted",
                        "error_kind": "cleanup_deadline_exhausted",
                        "local_status": local_status,
                        "remote_status": "pending",
                        "retire_outcome": retire_outcome,
                        "bundle_path": bundle_path,
                    }
                remote_runner = landing_cleanup._budgeted_remote_runner(root, cleanup_deadline)
            try:
                from . import remote_sandbox_reap

                marker_kwargs = {"repo_root": root}
                if remote_runner is not None:
                    marker_kwargs["runner"] = remote_runner
                marker = remote_sandbox_reap.write_merged_marker(
                    ({"branch": branch, "receipt_sha": identity["landing_commit"]},), **marker_kwargs
                )
                if getattr(marker, "ok", False) is not True:
                    raise RuntimeError(f"merged_marker_write_unverified:{getattr(marker, 'error', '')}")
                remote_kwargs = {
                    "lane_id": identity["lane_id"],
                    "branch": branch,
                    "expected_tip": identity["expected_tip"],
                    "primary_repo": root,
                    "rows": rows,
                }
                if remote_runner is not None:
                    remote_kwargs["ssh_runner"] = remote_runner
                remote = remote_sandbox_reap.reap_remote_lane_sandbox(identity["task_ref"], **remote_kwargs)
            except Exception as exc:
                message = " ".join(str(exc).splitlines())[:300]
                reason = (
                    "cleanup_deadline_exhausted"
                    if cleanup_deadline is not None and "cleanup_deadline_exhausted" in message
                    else f"remote_cleanup_failed:{type(exc).__name__}:{message}"
                )
                remote = {
                    "ok": False,
                    "status": "pending",
                    "reason": reason,
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
                        **_retire_release_fields(local_result),
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
                    **_retire_release_fields(local_result),
                    "complete": False,
                    "detail": f"remote_cleanup_pending:{remote_result.get('reason') or 'unknown'}",
                    "local_status": local_status,
                    "remote_status": "pending",
                    "retire_outcome": retire_outcome,
                    "bundle_path": bundle_path,
                }

    complete = local_status in {"complete", "skipped"} and remote_status in {"complete", "skipped"}
    return {
        **_retire_release_fields(local_result),
        "complete": complete,
        "detail": None if complete else "cleanup_pending",
        "local_status": local_status,
        "remote_status": remote_status,
        "retire_outcome": retire_outcome,
        "bundle_path": bundle_path,
    }


class _UnsafeSupersedeIdentity(ValueError):
    def __init__(self, field: str):
        super().__init__(field)
        self.field = field


def _safe_identity_component(value: object) -> bool:
    """Accept a non-empty printable ASCII identity confined to one component."""
    return (
        isinstance(value, str)
        and bool(value.strip())
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
        and all(32 <= ord(char) <= 126 for char in value)
    )


def _unsafe_supersede_identity_record(field: str, predecessor: object = None) -> dict[str, object]:
    logging.getLogger(__name__).warning("Unsafe supersede authorization identity: %s", field)
    return {
        "lane": predecessor if isinstance(predecessor, str) else None,
        "branch": None,
        "ok": False,
        "reason": "unsafe_identity",
        "field": field,
    }


def _validate_supersede_authorization_path(root: Path, path: Path) -> Path:
    """Early refusal only; descriptor-based store operations mediate each access."""
    try:
        repository_root = root.resolve()
        authorization_root = repository_root / ".task-state" / "lane-supersede-authorizations"
        relative = path.relative_to(repository_root)
        component = repository_root
        for name in relative.parts:
            component = component / name
            if component.is_symlink():
                raise _UnsafeSupersedeIdentity("authorization_path")
        if not path.resolve().is_relative_to(authorization_root):
            raise _UnsafeSupersedeIdentity("authorization_path")
    except (OSError, RuntimeError, ValueError) as exc:
        raise _UnsafeSupersedeIdentity("authorization_path") from exc
    return path


def _supersede_authorization_path(
    root: Path,
    task_ref: str,
    successor: str,
    predecessor: str,
    *,
    landing_commit: str,
    integration_ref: str,
) -> Path:
    """Key authorization by its original landing, retaining the lexical store anchor."""
    for field, value in (("task_ref", task_ref), ("lane_id", successor), ("predecessor_lane", predecessor)):
        if not _safe_identity_component(value):
            raise _UnsafeSupersedeIdentity(field)
    identity = json.dumps([landing_commit, integration_ref], separators=(",", ":"))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    path = (
        root.resolve()
        / ".task-state"
        / "lane-supersede-authorizations"
        / task_ref
        / successor
        / f"{predecessor}.{digest}.json"
    )
    return _validate_supersede_authorization_path(root, path)


def _replay_supersede_tombstone(
    root: Path,
    *,
    branch: str,
    landing_commit: str,
    integration_ref: str,
    current_tip: str,
) -> tuple[bool, str]:
    """Require an existing, verified tombstone for the persisted authorization."""
    from . import landing_log

    scan = landing_log.scan_first_parent(root, main_ref=integration_ref)
    if not scan.complete:
        return False, "probe_failed"
    branch_name = branch.removeprefix("refs/heads/")
    for _, tombstone in scan.tombstones:
        if (
            tombstone.branch != branch_name
            or tombstone.ref != landing_commit
            or tombstone.disposition != "superseded"
            or tombstone.kind != "superseded_by_integration"
        ):
            continue
        verdict = landing_log.verify_tombstone(
            root,
            tombstone,
            main_ref=integration_ref,
            current_tip=current_tip,
        )
        if verdict.failed_check == "probe_failed":
            return False, "probe_failed"
        if verdict.failed_check == "tip_moved":
            # Preserve the moved-tip refusal after reclaiming the record.
            # This is a refusal only; mismatching tombstones grant no authority.
            return False, "predecessor_moved"
        if verdict.verified:
            return True, "already_recorded"
    return False, "supersede_tombstone_missing"


@dataclass
class _SupersedeContext:
    root: Path
    task_ref: str
    lane_id: str
    landing_commit: str
    integration_ref: str
    replay: bool


class _SupersedeStepComplete(Exception):
    """Carry a typed result when this predecessor needs no further work."""

    def __init__(self, record: dict[str, object]):
        super().__init__(str(record["reason"]))
        self.record = record


def _supersede_record(lane=None, branch=None, *, reason: str, ok: bool = False, **details) -> dict[str, object]:
    return {"lane": lane, "branch": branch, "ok": ok, "reason": reason, **details}


def _load_supersede_manifest(ctx: _SupersedeContext, manifest_root: str | Path | None, deadline: float) -> dict:
    """Read the declaration within the phase deadline; a stalled store cannot hold the landing lock."""
    from . import lane_manifest, lane_supersede_auth

    manifest = lane_supersede_auth.read_declaration_manifest(
        ctx.root, ctx.task_ref, deadline=deadline, manifest_dir=manifest_root
    )
    if isinstance(manifest, lane_supersede_auth.ManifestUnavailable):
        reason = manifest.reason
        if reason == "manifest_missing":
            reason = "manifest_unavailable"
        elif reason.startswith("manifest_unreadable:"):
            reason = f"supersede_setup_failed:{reason.removeprefix('manifest_unreadable:')}"
        raise _SupersedeStepComplete(_supersede_record(reason=reason))
    try:
        # Lexical path for messages only; validation performs no storage IO.
        directory = Path(manifest_root) if manifest_root is not None else ctx.root / "config" / "lane-orchestration"
        return lane_manifest.validate_manifest(manifest, directory / f"{ctx.task_ref}.json")
    except Exception as exc:
        raise _SupersedeStepComplete(_supersede_record(reason=f"supersede_setup_failed:{type(exc).__name__}")) from exc


# Typed refusals for declarations or records without a declaration-time tip.
_UNPINNED_SUPERSEDE_REASONS = frozenset(
    {"declared_tip_missing", "declaration_unbound", "authorization_unbound", "authorization_declaration_mismatch"}
)
# The largest declared supersedes list is 3 among 3,972 manifest entries.
_MAX_SUPERSEDE_PREDECESSORS = 8
# Real-Git fixture measured 0.101s with verification/reclaim; earlier samples
# before reclaim ranged 0.071-0.271s. Ten seconds is >4 times eight times
# the observed maximum (about 2.17s), with checks between complete writes.
_SUPERSEDE_PHASE_BUDGET_SECONDS = 10.0


def _declared_supersede_predecessors(ctx: _SupersedeContext, manifest: dict) -> tuple[dict, dict, list]:
    lanes = manifest.get("lanes")
    entry = lanes.get(ctx.lane_id) if isinstance(lanes, dict) else None
    if not isinstance(entry, dict):
        raise _SupersedeStepComplete(_supersede_record(reason="landing_lane_entry_missing"))
    predecessors = entry.get("supersedes")
    if predecessors is None:
        return lanes, entry, []
    if not isinstance(predecessors, list):
        raise _SupersedeStepComplete(_supersede_record(reason="supersedes_invalid"))
    if len(predecessors) > _MAX_SUPERSEDE_PREDECESSORS:
        logging.getLogger(__name__).warning(
            "supersede_cap_exceeded task=%s lane=%s declared_count=%s cap=%s",
            ctx.task_ref,
            ctx.lane_id,
            len(predecessors),
            _MAX_SUPERSEDE_PREDECESSORS,
        )
        raise _SupersedeStepComplete(
            _supersede_record(
                reason="supersede_cap_exceeded",
                declared_count=len(predecessors),
                cap=_MAX_SUPERSEDE_PREDECESSORS,
            )
        )
    return lanes, entry, predecessors


def _supersede_predecessor_branch(lanes: dict, predecessor: str) -> str:
    entry = lanes.get(predecessor)
    if entry is None:
        raise _SupersedeStepComplete(_supersede_record(predecessor, reason="predecessor_entry_missing"))
    if not isinstance(entry, dict):
        raise _SupersedeStepComplete(_supersede_record(predecessor, reason="predecessor_entry_invalid"))
    branch = entry.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        raise _SupersedeStepComplete(_supersede_record(predecessor, reason="predecessor_branch_missing"))
    return branch.strip()


def _supersede_predecessor_tip(ctx: _SupersedeContext, predecessor: str, branch: str) -> str:
    from . import orchestrator_lanes

    branch_ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    tip, error = _ref_oid(orchestrator_lanes, ctx.root, branch_ref)
    if error:
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason="predecessor_probe_failed"))
    if tip is None and not ctx.replay:
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason="branch_missing"))
    return tip or ""


def _load_supersede_authorization(ctx: _SupersedeContext, predecessor: str, branch: str, path: Path):
    from . import lane_supersede_auth

    saved = lane_supersede_auth.load(ctx.root, path.relative_to(ctx.root.resolve()).parts)
    if isinstance(saved, lane_supersede_auth.RecordError):
        if saved.reason == "record_unsafe_path":
            raise _UnsafeSupersedeIdentity("authorization_path")
        reason = (
            "predecessor_authorization_missing"
            if saved.reason == "record_missing"
            else f"predecessor_authorization_{saved.reason}"
        )
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason=reason))
    if saved.successor != ctx.lane_id or saved.predecessor != predecessor or saved.predecessor_branch != branch:
        raise _SupersedeStepComplete(
            _supersede_record(predecessor, branch, reason="predecessor_authorization_identity_mismatch")
        )
    return saved


def _authorize_supersede_replay(ctx: _SupersedeContext, predecessor: str, branch: str, path: Path, entry: dict):
    from . import lane_supersede_auth

    _validate_supersede_authorization_path(ctx.root, path)
    declared_tip = lane_supersede_auth.resolve_declared_tip(entry, predecessor)
    if isinstance(declared_tip, lane_supersede_auth.Unbound):
        # Neither a tombstone nor a record from an earlier landing-time binding
        # can stand in for a declaration that never pinned this predecessor.
        refused = lane_supersede_auth.unpinned_declaration_refusal(declared_tip)
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason=refused.reason))
    tip = _supersede_predecessor_tip(ctx, predecessor, branch)
    if tip != declared_tip:
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason="predecessor_moved"))
    ok, reason = _replay_supersede_tombstone(
        ctx.root,
        branch=branch,
        landing_commit=ctx.landing_commit,
        integration_ref=ctx.integration_ref,
        current_tip=tip,
    )
    if ok:
        record = _supersede_record(predecessor, branch, ok=True, reason=reason)
        # Replays report no tip binding when only the tombstone was consulted.
        raise _SupersedeStepComplete(_reclaim_supersede_authorization(ctx, predecessor, branch, path, tip, record))
    if reason != "supersede_tombstone_missing":
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, ok=ok, reason=reason))
    saved = _load_supersede_authorization(ctx, predecessor, branch, path)
    authorized = lane_supersede_auth.check_replay(saved, tip)
    if isinstance(authorized, lane_supersede_auth.Refused):
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason=authorized.reason))
    # A failed writer may leave durable authorization. Reuse it only after
    # checking the tip and the caller's verified original landing.
    return authorized, tip


def _authorize_supersede_initial(ctx: _SupersedeContext, predecessor: str, branch: str, path: Path, entry: dict):
    from . import lane_supersede_auth

    tip = _supersede_predecessor_tip(ctx, predecessor, branch)
    declared_tip = lane_supersede_auth.resolve_declared_tip(entry, predecessor)
    authorized = lane_supersede_auth.authorize_initial(
        declared_tip,
        tip,
        successor=ctx.lane_id,
        predecessor=predecessor,
        predecessor_branch=branch,
        recorded_at=datetime.now(timezone.utc).isoformat(),
    )
    if isinstance(authorized, lane_supersede_auth.Refused):
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason=authorized.reason))
    # Existing verified tombstones need no recovery authorization.
    _plan_authorized_supersede(ctx, predecessor, branch, authorized)
    _validate_supersede_authorization_path(ctx.root, path)
    persisted = lane_supersede_auth.persist(ctx.root, path.relative_to(ctx.root.resolve()).parts, authorized.record)
    if isinstance(persisted, lane_supersede_auth.RecordError):
        if persisted.reason == "record_unsafe_path":
            raise _UnsafeSupersedeIdentity("authorization_path")
        raise _SupersedeStepComplete(
            _supersede_record(predecessor, branch, reason=f"predecessor_authorization_{persisted.reason}")
        )
    return authorized, tip


def _plan_authorized_supersede(ctx: _SupersedeContext, predecessor: str, branch: str, authorized):
    from . import lane_tombstone

    planned = lane_tombstone.plan_supersede(
        ctx.root,
        branch=branch,
        superseded_by=ctx.landing_commit,
        integration_ref=ctx.integration_ref,
        allow_main=True,
    )
    if isinstance(planned, lane_tombstone.SupersedeResult) and not planned.ok:
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, reason=planned.reason))
    if planned.tip != authorized.record.authorized_tip:
        raise _SupersedeStepComplete(
            _supersede_record(predecessor, branch, reason="predecessor_moved" if ctx.replay else "predecessor_advanced")
        )
    if isinstance(planned, lane_tombstone.SupersedeResult):
        raise _SupersedeStepComplete(_supersede_record(predecessor, branch, ok=planned.ok, reason=planned.reason))
    return planned


def _apply_authorized_supersede(ctx: _SupersedeContext, predecessor: str, branch: str, authorized, tip: str) -> dict:
    from . import lane_tombstone

    planned = _plan_authorized_supersede(ctx, predecessor, branch, authorized)
    result = lane_tombstone.supersede(
        ctx.root,
        branch=branch,
        superseded_by=ctx.landing_commit,
        integration_ref=ctx.integration_ref,
        apply=True,
        allow_main=True,
        plan_digest=planned.plan_digest,
        # The declaration is this tombstone's authority; landing never mints operator review.
        operator_review=False,
    )
    reason = result.reason
    if not result.ok and result.reason == "plan_changed" and result.proof.get("current_tip") != tip:
        reason = "predecessor_moved" if ctx.replay else "predecessor_advanced"
    return _supersede_record(
        predecessor,
        branch,
        ok=result.ok,
        reason=reason,
        **({"replayed_write": True} if ctx.replay and result.ok else {}),
    )


def _record_supersede_predecessor(ctx: _SupersedeContext, lanes: dict, entry: dict, predecessor) -> dict:
    if not _safe_identity_component(predecessor):
        return _unsafe_supersede_identity_record("predecessor_lane", predecessor)
    branch = None
    try:
        branch = _supersede_predecessor_branch(lanes, predecessor)
        path = _supersede_authorization_path(
            ctx.root,
            ctx.task_ref,
            ctx.lane_id,
            predecessor,
            landing_commit=ctx.landing_commit,
            integration_ref=ctx.integration_ref,
        )
        if ctx.replay:
            authorized, tip = _authorize_supersede_replay(ctx, predecessor, branch, path, entry)
        else:
            authorized, tip = _authorize_supersede_initial(ctx, predecessor, branch, path, entry)
        record = _apply_authorized_supersede(ctx, predecessor, branch, authorized, tip)
        if record["ok"] is True:
            record = _reclaim_supersede_authorization(ctx, predecessor, branch, path, tip, record)
        return record
    except _SupersedeStepComplete as completed:
        return completed.record
    except _UnsafeSupersedeIdentity as exc:
        return _unsafe_supersede_identity_record(exc.field, predecessor)
    except Exception as exc:
        return _supersede_record(predecessor, branch, reason=f"supersede_error:{type(exc).__name__}")


def _reclaim_supersede_authorization(ctx, predecessor, branch, path, tip, record) -> dict:
    from . import lane_supersede_auth

    detail = None
    try:
        ok, reason = _replay_supersede_tombstone(
            ctx.root,
            branch=branch,
            landing_commit=ctx.landing_commit,
            integration_ref=ctx.integration_ref,
            current_tip=tip,
        )
        if not ok:
            detail = {"ok": False, "reason": f"tombstone_unverified:{reason}"}
        else:
            deleted = lane_supersede_auth.delete(ctx.root, path.relative_to(ctx.root.resolve()).parts)
            if isinstance(deleted, lane_supersede_auth.RecordError):
                detail = {"ok": False, "reason": deleted.reason, "detail": deleted.detail}
                if deleted.reason == "record_unsafe_path":
                    detail.update(reason="unsafe_identity", field="authorization_path")
    except Exception as exc:
        detail = {"ok": False, "reason": f"record_delete_error:{type(exc).__name__}", "detail": str(exc)}
    if detail is not None:
        record["authorization_reclaim"] = detail
        logging.getLogger(__name__).warning(
            "Supersede authorization reclaim failed task=%s lane=%s predecessor=%s: %s",
            ctx.task_ref,
            ctx.lane_id,
            predecessor,
            detail["reason"],
        )
    # Fail safe: if the tombstone is later lost, replay has no record and
    # refuses with predecessor_authorization_missing; it cannot mint authority.
    return record


def _schedule_supersede_sweep_wake(ctx: _SupersedeContext) -> dict[str, object]:
    from . import lane_retire_sweep

    try:
        raw = lane_retire_sweep.schedule_sweep_wake(ctx.root, wake_at=time.time(), task_ref=ctx.task_ref)
        receipt = dict(raw) if isinstance(raw, dict) else {}
        if receipt.get("ok") is True:
            receipt.setdefault("reason", receipt.get("outcome", "scheduled"))
        else:
            outcome = receipt.get("outcome")
            receipt["ok"] = False
            receipt["reason"] = f"sweep_wake_failed:{outcome if isinstance(outcome, str) else 'invalid_receipt'}"
        return receipt
    except Exception as exc:
        return {
            "ok": False,
            "outcome": "spawn_failed",
            "reason": f"sweep_wake_error:{type(exc).__name__}",
            "error": str(exc),
        }


def _record_supersede_sweep_wake(ctx: _SupersedeContext, records: list[dict[str, object]]) -> dict[str, object]:
    # The post-merge hook may have started a sweep before these commits existed.
    receipt = _schedule_supersede_sweep_wake(ctx)
    if receipt.get("ok") is not True:
        records.append(_supersede_record(reason=receipt["reason"], error=receipt.get("error")))
        logging.getLogger(__name__).warning(
            "Supersede tombstone sweep wake failed for task %s lane %s: %s (%s)",
            ctx.task_ref,
            ctx.lane_id,
            receipt["reason"],
            receipt.get("error"),
        )
    return receipt


def _supersede_phase_deadline(cleanup_deadline: float | None) -> float:
    deadline = time.monotonic() + _SUPERSEDE_PHASE_BUDGET_SECONDS
    return deadline if cleanup_deadline is None else min(deadline, cleanup_deadline)


def _record_supersede_predecessors(ctx, lanes, entry, predecessors, deadline) -> list[dict[str, object]]:
    records = []
    exhausted = False
    for predecessor in predecessors:
        if time.monotonic() >= deadline:
            if not exhausted:
                logging.getLogger(__name__).warning(
                    "supersede_deadline_exhausted task=%s lane=%s: manual review required; "
                    "replay cannot authorize skipped predecessors (predecessor_authorization_missing)",
                    ctx.task_ref,
                    ctx.lane_id,
                )
                exhausted = True
            records.append(
                _supersede_record(
                    predecessor,
                    reason="supersede_deadline_exhausted",
                    manual_review_required=True,
                    replay_reason="predecessor_authorization_missing",
                )
            )
        else:
            # Check only between predecessors, never during an authorized write.
            records.append(_record_supersede_predecessor(ctx, lanes, entry, predecessor))
    return records


def _record_supersede_tombstones(
    root: Path,
    *,
    task_ref: str,
    lane_id: str,
    landing_commit: str,
    integration_ref: str,
    manifest_root: str | Path | None,
    replay: bool = False,
    cleanup_deadline: float | None = None,
) -> tuple[list[dict[str, object]], dict[str, object] | None, str | None]:
    """Record declared predecessor tombstones and return their wake receipt."""
    for field, value in (("task_ref", task_ref), ("lane_id", lane_id)):
        if not _safe_identity_component(value):
            return [_unsafe_supersede_identity_record(field)], None, None
    ctx = _SupersedeContext(root, task_ref, lane_id, landing_commit, integration_ref, replay)
    deadline = _supersede_phase_deadline(cleanup_deadline)
    try:
        manifest = _load_supersede_manifest(ctx, manifest_root, deadline)
        lanes, entry, predecessors = _declared_supersede_predecessors(ctx, manifest)
    except _SupersedeStepComplete as completed:
        return [completed.record], None, None
    if not predecessors:
        return [], None, None
    from . import landing_log

    # Verifications here and in lane_tombstone share the phase's remaining budget.
    with landing_log.tombstone_verification_deadline(deadline):
        records = _record_supersede_predecessors(ctx, lanes, entry, predecessors, deadline)
    unpinned = [
        f"{record['lane']}:{record['reason']}" for record in records if record["reason"] in _UNPINNED_SUPERSEDE_REASONS
    ]
    if unpinned:
        logging.getLogger(__name__).warning(
            "Supersede authority refused for task %s lane %s without a pinned predecessor tip: %s; "
            "manual review required",
            task_ref,
            lane_id,
            ", ".join(unpinned),
        )
    receipt = _record_supersede_sweep_wake(ctx, records)
    # Landing never binds an undeclared tip; the envelope key is kept for callers.
    return records, receipt, None


def _land_once(
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    run_id: str | None = None,
    *,
    integration_ref: str | None = None,
    orchestrator_root: str | Path | None = None,
    manifest_root: str | Path | None = None,
    _cleanup_deadline: float | None = None,
    landing_lock_wait_seconds: float | None = None,
) -> dict:
    """Append one receipt-bearing no-ff landing and converge its projections."""
    lock_details: dict[str, object] = {}

    def _land_envelope(outcome: str, **kwargs: Any) -> dict[str, object]:
        response = _envelope(outcome, **kwargs)
        if "waited_seconds" in lock_details:
            response["waited_seconds"] = lock_details["waited_seconds"]
        if "lock_wait_bound_unhonoured" in lock_details:
            response["lock_wait_bound_unhonoured"] = lock_details["lock_wait_bound_unhonoured"]
        return response

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
        return _land_envelope(
            "invalid_request",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=supplied_run_id,
        )
    if run_id is not None and (not isinstance(run_id, str) or _HEX32.fullmatch(run_id) is None):
        return _land_envelope(
            "invalid_request",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=run_id if isinstance(run_id, str) else None,
        )
    request_run_id = run_id or uuid4().hex
    if integration_ref is not None and (not isinstance(integration_ref, str) or not integration_ref.strip()):
        return _land_envelope(
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
        return _land_envelope(
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
        return _land_envelope(
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
        return _land_envelope(
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
        return _land_envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="landing_policy_unavailable",
        )
    if require_gate_receipt is not True:
        return _land_envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="policy_unsupported:require_gate_receipt",
        )
    if require_review_verdict is not False:
        return _land_envelope(
            "merge_refused",
            task_ref=task_value,
            lane_id=lane_value,
            expected_tip=tip_value,
            run_id=request_run_id,
            detail="policy_unsupported:require_review_verdict",
        )

    # Invalid caller bounds must retain their typed diagnostic, rather than
    # being converted into a generic merge failure by the operation handler.
    lock_wait_bound = orchestrator_lanes._landing_lock_wait_bound(landing_lock_wait_seconds)
    if _cleanup_deadline is not None:
        lock_wait_bound = min(lock_wait_bound, max(0.0, _cleanup_deadline - time.monotonic()))
    try:
        lock_factory = orchestrator_lanes._landing_mutation_lock
        lock_kwargs = {
            "wait_seconds": lock_wait_bound,
            "operation": "land",
            "lane": lane_value,
            "lock_details": lock_details,
        }
        lock_signature = None
        try:
            lock_signature = inspect.signature(lock_factory)
            lock_signature.bind(root, **lock_kwargs)
        except (TypeError, ValueError):
            # Keep simple context-manager test doubles and compatibility
            # wrappers callable, but report the dropped wait contract.
            missing = []
            for name, value in lock_kwargs.items():
                try:
                    if lock_signature is None:
                        raise ValueError("signature unavailable")
                    lock_signature.bind_partial(root, **{name: value})
                except (TypeError, ValueError):
                    missing.append(name)
            diagnostic = {
                "requested": lock_wait_bound,
                "reason": "lock_factory_signature",
                "missing": missing,
                "explicit": landing_lock_wait_seconds is not None,
            }
            lock_details["lock_wait_bound_unhonoured"] = diagnostic
            logging.getLogger(__name__).warning(
                "lock_wait_bound_unhonoured requested=%s reason=%s missing=%s explicit=%s",
                diagnostic["requested"],
                diagnostic["reason"],
                diagnostic["missing"],
                diagnostic["explicit"],
            )
            lock_context = lock_factory(root)
        else:
            lock_context = lock_factory(root, **lock_kwargs)
        with lock_context as acquired:
            if acquired is None:
                return _land_envelope(
                    "lock_held",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="landing_lock_unavailable",
                    waited_seconds=lock_details.get("waited_seconds", 0.0),
                    lock_holder=None,
                    include_lock_holder=True,
                    lock_holder_reason="landing_lock_unavailable",
                )
            if acquired is False:
                return _land_envelope(
                    "lock_held",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="mutation_lock_held",
                    waited_seconds=lock_details.get("waited_seconds", 0.0),
                    lock_holder=lock_details.get("lock_holder"),
                    include_lock_holder=True,
                    lock_holder_reason=lock_details.get("lock_holder_reason", "holder_unknown"),
                )

            lane_raw = lanes_recording.get_lane(lane_id=lane_value, task_ref=task_value)
            lane = _lane_row(lane_raw)
            branch_value = lane.get("branch") if lane is not None else None
            if lane is None or not isinstance(branch_value, str) or not branch_value.strip():
                return _land_envelope(
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
                        root=root,
                        manifest_root=manifest_root,
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
                return _land_envelope(
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
                    return _land_envelope(
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
                    return _land_envelope(
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
                    return _land_envelope(
                        "moved",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail=f"tip:{current_branch}",
                    )

            integration_before, integration_error = _ref_oid(orchestrator_lanes, root, integration_value)
            if state != "verified" and (integration_error or integration_before is None):
                return _land_envelope(
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
                supersede_tombstones, sweep_wake, supersede_tip_binding = _record_supersede_tombstones(
                    root,
                    task_ref=task_value,
                    lane_id=lane_value,
                    landing_commit=landing_commit,
                    integration_ref=integration_value,
                    manifest_root=manifest_root,
                    cleanup_deadline=_cleanup_deadline,
                    replay=True,
                )
                parsed_receipt = getattr(verdict, "receipt", None)
                gate_id = getattr(parsed_receipt, "gate_id", None)
                landing_run_id = getattr(parsed_receipt, "run_id", None)
                if (
                    not isinstance(gate_id, int)
                    or not isinstance(landing_run_id, str)
                    or _HEX32.fullmatch(landing_run_id) is None
                ):
                    return _land_envelope(
                        "cleanup_pending",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="cleanup_intent_identity_unavailable",
                        landing_commit=landing_commit,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                        supersede_tombstones=supersede_tombstones,
                        sweep_wake=sweep_wake,
                        supersede_tip_binding=supersede_tip_binding,
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
                        lane_row=_cleanup_lane_snapshot(lane, cleanup_identity),
                    )
                except Exception as exc:
                    cleanup_intent = None
                    cleanup_error = f"cleanup_intent_write_failed:{type(exc).__name__}"
                else:
                    cleanup_error = None
                projection_pending = _project(
                    root=root,
                    manifest_root=manifest_root,
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
                        cleanup_deadline=_cleanup_deadline,
                    )
                    if cleanup_intent is not None
                    else {"complete": False, "detail": cleanup_error or "cleanup_intent_unavailable"}
                )
                cleanup_complete = cleanup_result.get("complete") is True
                return _land_envelope(
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
                    **_landed_marker_envelope_fields(cleanup_result),
                    **_retire_envelope_fields(cleanup_result),
                    supersede_tombstones=supersede_tombstones,
                    sweep_wake=sweep_wake,
                    supersede_tip_binding=supersede_tip_binding,
                )

            carrier = landing_log.find_carrier(root, tip_value, main_ref=integration_value)
            if getattr(carrier, "error", None):
                carrier_error = getattr(carrier, "error", None)
                if branch_absent and carrier_error == "commit_missing":
                    return _land_envelope(
                        "moved",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="branch_absent",
                    )
                probe_outcome = (
                    "probe_unavailable"
                    if carrier_error == "timeout"
                    or (isinstance(carrier_error, str) and carrier_error.startswith("git_failed:"))
                    else "merge_refused"
                )
                return _land_envelope(
                    probe_outcome,
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
                    return _land_envelope(
                        "contained_kept",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="containing_tip_unavailable",
                        cleanup_pending=True,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                    )
                if lane.get("branch_tip_sha") != tip_value:
                    return _land_envelope(
                        "contained_no_receipt",
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        run_id=request_run_id,
                        detail="recorded_tip_identity_unavailable",
                        cleanup_pending=True,
                        landing_commit=containing_tip,
                        integration_before=integration_before,
                        pin_ref=pin_ref,
                    )
                from . import landing_cleanup, lane_landing_drain

                contained_identity = {
                    "task_ref": task_value,
                    "lane_id": lane_value,
                    "expected_tip": tip_value,
                    "integration_ref": integration_value,
                    "landing_commit": containing_tip,
                    "run_id": request_run_id,
                }
                contained_intent = _find_matching_contained_intent(root, contained_identity, landing_cleanup)
                created_intent = contained_intent is None
                if contained_intent is None:
                    try:
                        contained_intent = landing_cleanup.ensure_intent(
                            root,
                            identity=contained_identity,
                            lane_row=_cleanup_lane_snapshot(lane, contained_identity),
                        )
                    except Exception as exc:
                        return _land_envelope(
                            "contained_kept",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=f"cleanup_intent_write_failed:{type(exc).__name__}",
                            cleanup_pending=True,
                            landing_commit=containing_tip,
                            integration_before=integration_before,
                            pin_ref=pin_ref,
                        )
                saved_identity = contained_intent.get("identity")
                if isinstance(saved_identity, Mapping):
                    contained_identity = {
                        key: str(saved_identity[key])
                        for key in (
                            "task_ref",
                            "lane_id",
                            "expected_tip",
                            "integration_ref",
                            "landing_commit",
                            "run_id",
                        )
                        if isinstance(saved_identity.get(key), str)
                    }
                captured_row = (
                    contained_intent.get("lane_row")
                    if isinstance(contained_intent, dict) and isinstance(contained_intent.get("lane_row"), dict)
                    else None
                )
                current_raw = lanes_recording.get_lane(lane_id=lane_value, task_ref=task_value)
                current_lane = _lane_row(current_raw)
                intent_complete = contained_intent.get("local_status") in {
                    "complete",
                    "skipped",
                } and contained_intent.get("remote_status") in {"complete", "skipped"}
                if not intent_complete and current_lane is not None:
                    try:
                        from workbay_handoff_mcp.runtime import get_runtime_config

                        state_dir = get_runtime_config().state_dir
                    except Exception:
                        state_dir = root / ".task-state"
                    prepared, prepare_error, prepared_sha = lane_landing_drain._prepare_contained_landing(
                        root=root,
                        state_dir=Path(state_dir).expanduser().resolve(),
                        task_ref=task_value,
                        lane_id=lane_value,
                        expected_tip=tip_value,
                        integration_ref=integration_value,
                        run_id=contained_identity["run_id"],
                        lane_row=lane,
                        existing_containing_sha=containing_tip,
                    )
                    if not prepared or prepared_sha != containing_tip:
                        reason = prepare_error or "contained_landing_unverified"
                        latest = _find_matching_contained_intent(root, contained_identity, landing_cleanup)
                        if latest is not None:
                            _persist_intent(root, landing_cleanup, latest, last_error=reason)
                        return _land_envelope(
                            "contained_kept",
                            task_ref=task_value,
                            lane_id=lane_value,
                            expected_tip=tip_value,
                            run_id=request_run_id,
                            detail=reason,
                            cleanup_pending=True,
                            **_retire_envelope_fields(contained_intent.get("local_result")),
                            landing_commit=containing_tip,
                            integration_before=integration_before,
                            pin_ref=pin_ref,
                        )
                    contained_intent = _find_matching_contained_intent(root, contained_identity, landing_cleanup)
                    captured_row = (
                        contained_intent.get("lane_row")
                        if isinstance(contained_intent, dict) and isinstance(contained_intent.get("lane_row"), dict)
                        else None
                    )
                    current_raw = lanes_recording.get_lane(lane_id=lane_value, task_ref=task_value)
                    current_lane = _lane_row(current_raw)
                if contained_intent is not None and captured_row is not None:
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
                            cleanup_deadline=_cleanup_deadline,
                        )
                        cleanup_complete = cleanup_result.get("complete") is True
                        return _land_envelope(
                            ("contained_retired" if created_intent else "contained_reaped")
                            if cleanup_complete
                            else "contained_kept",
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
                            **_landed_marker_envelope_fields(cleanup_result),
                            **_retire_envelope_fields(cleanup_result),
                        )
                return _land_envelope(
                    "contained_kept",
                    task_ref=task_value,
                    lane_id=lane_value,
                    expected_tip=tip_value,
                    run_id=request_run_id,
                    detail="contained_disposition_evidence_unavailable",
                    cleanup_pending=True,
                    **_retire_envelope_fields(contained_intent.get("local_result")),
                    landing_commit=containing_tip,
                    integration_before=integration_before,
                    pin_ref=pin_ref,
                )

            if branch_absent:
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                return _land_envelope(
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
                            return _land_envelope(
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
                            return _land_envelope(
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
                            return _land_envelope(
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
                        return _land_envelope(
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
                        return _land_envelope(
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
                    return _land_envelope(
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

                supersede_tombstones, sweep_wake, supersede_tip_binding = _record_supersede_tombstones(
                    root,
                    task_ref=task_value,
                    lane_id=lane_value,
                    landing_commit=landing_commit,
                    integration_ref=integration_value,
                    manifest_root=manifest_root,
                    cleanup_deadline=_cleanup_deadline,
                )
                if not isinstance(landing_run_id, str) or _HEX32.fullmatch(landing_run_id) is None:
                    return _land_envelope(
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
                        supersede_tombstones=supersede_tombstones,
                        sweep_wake=sweep_wake,
                        supersede_tip_binding=supersede_tip_binding,
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
                        lane_row=_cleanup_lane_snapshot(lane, cleanup_identity),
                    )
                except Exception as cleanup_exc:
                    cleanup_intent = None
                    cleanup_error = f"cleanup_intent_write_failed:{type(cleanup_exc).__name__}"
                else:
                    cleanup_error = None

                projection_pending = _project(
                    root=root,
                    manifest_root=manifest_root,
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
                        cleanup_deadline=_cleanup_deadline,
                    )
                    if cleanup_intent is not None
                    else {"complete": False, "detail": cleanup_error or "cleanup_intent_unavailable"}
                )
                cleanup_complete = cleanup_result.get("complete") is True
                return _land_envelope(
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
                    **_landed_marker_envelope_fields(cleanup_result),
                    **_retire_envelope_fields(cleanup_result),
                    supersede_tombstones=supersede_tombstones,
                    sweep_wake=sweep_wake,
                    supersede_tip_binding=supersede_tip_binding,
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
                        supersede_tombstones, sweep_wake, supersede_tip_binding = _record_supersede_tombstones(
                            root,
                            task_ref=task_value,
                            lane_id=lane_value,
                            landing_commit=value_after,
                            integration_ref=integration_value,
                            manifest_root=manifest_root,
                            cleanup_deadline=_cleanup_deadline,
                            replay=True,
                        )
                        parsed_after = getattr(verdict_after, "receipt", None)
                        recovered_run_id = getattr(parsed_after, "run_id", None)
                        if not isinstance(recovered_run_id, str) or _HEX32.fullmatch(recovered_run_id) is None:
                            return _land_envelope(
                                "cleanup_pending",
                                task_ref=task_value,
                                lane_id=lane_value,
                                expected_tip=tip_value,
                                run_id=request_run_id,
                                detail="cleanup_intent_identity_unavailable",
                                landing_commit=value_after,
                                supersede_tombstones=supersede_tombstones,
                                sweep_wake=sweep_wake,
                                supersede_tip_binding=supersede_tip_binding,
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
                            lane_row=_cleanup_lane_snapshot(lane, recovery_identity),
                        )
                        pending = _project(
                            root=root,
                            manifest_root=manifest_root,
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
                            cleanup_deadline=_cleanup_deadline,
                        )
                        outcome = "already_landed" if recovery_result.get("complete") is True else "cleanup_pending"
                        recovered_gate = getattr(parsed_after, "gate_id", None)
                        return _land_envelope(
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
                            **_landed_marker_envelope_fields(recovery_result),
                            **_retire_envelope_fields(recovery_result),
                            supersede_tombstones=supersede_tombstones,
                            sweep_wake=sweep_wake,
                            supersede_tip_binding=supersede_tip_binding,
                        )
                    old_head = integration_before if "integration_before" in locals() else None
                    if head_after is not None and head_after == old_head:
                        reason = captured.get("reason") if "captured" in locals() else None
                        conflicts = captured.get("conflicting_paths") if "captured" in locals() else None
                        detail = str(reason or f"append_recovery_failed:{type(exc).__name__}")
                        if detail == "merge_conflict":
                            return _land_envelope(
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
                            return _land_envelope(
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
                            return _land_envelope(
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
                        return _land_envelope(
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
                    return _land_envelope(
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
                    return _land_envelope(
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
            return _land_envelope(
                "merge_refused",
                task_ref=task_value,
                lane_id=lane_value,
                expected_tip=tip_value,
                run_id=request_run_id,
                detail=f"operation_failed:{type(exc).__name__}",
            )
        return _land_envelope(
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


def land(
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    run_id: str | None = None,
    *,
    integration_ref: str | None = None,
    orchestrator_root: str | Path | None = None,
    manifest_root: str | Path | None = None,
    _drain_cleanup: bool = True,
    _cleanup_deadline: float | None = None,
    landing_lock_wait_seconds: float | None = None,
) -> dict:
    """Drain durable cleanup obligations, then land one lane."""

    cleanup_drain: dict[str, object] | None = None
    if _drain_cleanup:
        try:
            if orchestrator_root is not None:
                root = Path(orchestrator_root).expanduser().resolve()
            else:
                from .. import lanes

                root = Path(lanes._workspace_root()).expanduser().resolve()
            from . import landing_cleanup

            cleanup_drain = landing_cleanup.drain_pending_cleanup_intents(root)
        except Exception as exc:
            error = " ".join(str(exc).splitlines())[:300]
            cleanup_drain = {
                "ok": False,
                "error": f"{type(exc).__name__}:{error}",
                "local_completed": 0,
                "local_still_pending": 0,
                "skipped_budget": 0,
            }

    result = _land_once(
        task_ref,
        lane_id,
        expected_tip,
        run_id=run_id,
        integration_ref=integration_ref,
        orchestrator_root=orchestrator_root,
        manifest_root=manifest_root,
        _cleanup_deadline=_cleanup_deadline,
        landing_lock_wait_seconds=landing_lock_wait_seconds,
    )
    if cleanup_drain is not None:
        result["cleanup_drain"] = cleanup_drain
    result.setdefault("predecessors_retired", [])
    result.setdefault("predecessors_kept", [])
    if (
        _drain_cleanup
        and result.get("integrated") is True
        and result.get("outcome") in {"landed", "already_landed", "landed_then_moved", "cleanup_pending"}
        and isinstance(result.get("landing_commit"), str)
    ):
        try:
            if orchestrator_root is not None:
                root = Path(orchestrator_root).expanduser().resolve()
            else:
                from .. import lanes

                root = Path(lanes._workspace_root()).expanduser().resolve()
            from workbay_handoff_mcp.runtime import get_runtime_config

            from . import lane_landing_drain

            resolved_integration, _integration_error = _registered_integration_ref(
                root=root,
                task_ref=task_ref,
                requested_ref=integration_ref,
                manifest_root=manifest_root,
            )
            prepared = lane_landing_drain.prepare_contained_predecessors(
                root=root,
                task_ref=task_ref,
                lane_id=lane_id,
                expected_tip=expected_tip,
                integration_ref=str(resolved_integration or integration_ref or "main"),
                landing_commit=str(result["landing_commit"]),
                run_id=str(result.get("run_id") or run_id or ""),
                manifest_root=manifest_root,
                state_dir=get_runtime_config().state_dir,
            )
            result["predecessors_retired"] = prepared.get("predecessors_retired", [])
            result["predecessors_kept"] = prepared.get("predecessors_kept", [])
            result["predecessor_cleanup_drain"] = prepared.get("cleanup_drain")
        except Exception as exc:
            result["predecessors_kept"] = [
                {"lane_id": lane_id, "reason": f"predecessor_drain_failed:{type(exc).__name__}"}
            ]
            result["predecessor_cleanup_drain"] = {"ok": False, "error": f"{type(exc).__name__}"}
    return result


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
