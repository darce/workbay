"""Bounded drain for durable, verified lane tips.

The drain is deliberately a coordinator of existing authorities: the manifest
defines the task's lane order, handoff rows preserve the observed lane tip,
verified tests authorize that exact tip, and :mod:`lane_land` owns every Git
mutation and retirement decision.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_SUCCESS_OUTCOMES = frozenset({"landed", "already_landed"})
_CLEANUP_COMPLETE = frozenset({"reclaimed", "already_retired", "skipped_no_worktree_path"})
_PAGE_SIZE = 200


@dataclass(frozen=True, slots=True)
class LaneDrainOutcome:
    """One lane's eligibility or actuator result."""

    lane_id: str
    status: str
    expected_tip: str | None = None
    run_id: str | None = None
    detail: str | None = None
    landing: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class LandingDrainResult:
    """Results and outstanding obligations for one bounded drain pass."""

    task_ref: str
    integration_ref: str
    outcomes: tuple[LaneDrainOutcome, ...]
    remaining: tuple[str, ...]
    pending: tuple[str, ...]
    pending_cleanup: tuple[str, ...]
    admission_allowed: bool
    error: str | None = None


def _raw_object(raw: object) -> dict[str, object] | None:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return None
    return raw if isinstance(raw, dict) else None


def _data(raw: object) -> dict[str, object] | None:
    payload = _raw_object(raw)
    if payload is None or payload.get("ok") is not True:
        return None
    value = payload.get("data", payload)
    return value if isinstance(value, dict) else None


def _task_manifest(task_ref: str, manifest_root: Path) -> dict[str, object]:
    from . import lane_manifest

    manifest = lane_manifest.load_manifest(task_ref, manifest_dir=manifest_root)
    if manifest.get("task_ref") != task_ref:
        raise ValueError("manifest_task_ref_mismatch")
    lanes = manifest.get("lanes")
    order = manifest.get("merge_order")
    if not isinstance(lanes, dict) or not isinstance(order, list):
        raise ValueError("manifest_lane_order_unreadable")
    lane_ids = [lane_id for lane_id in order if isinstance(lane_id, str) and lane_id in lanes]
    lane_ids.extend(sorted(lane_id for lane_id in lanes if isinstance(lane_id, str) and lane_id not in lane_ids))
    if len(lane_ids) != len(lanes) or len(set(lane_ids)) != len(lane_ids):
        raise ValueError("manifest_lane_order_invalid")

    # GRPH-01/31: retain the declared legal order and check dependency edges;
    # this serial, shared-ref pass does not invent another lane priority/schedule.
    dependencies = manifest.get("depends_on", {})
    if isinstance(dependencies, dict):
        positions = {lane_id: index for index, lane_id in enumerate(lane_ids)}
        for lane_id, prerequisites in dependencies.items():
            if lane_id not in positions or not isinstance(prerequisites, list):
                continue
            if any(
                isinstance(prerequisite, str)
                and prerequisite in positions
                and positions[prerequisite] > positions[lane_id]
                for prerequisite in prerequisites
            ):
                raise ValueError("manifest_merge_order_violates_dependencies")
    manifest["_drain_lane_ids"] = lane_ids
    return manifest


def _list_durable_lanes(task_ref: str) -> dict[str, dict[str, object]]:
    from workbay_handoff_mcp import lanes_recording

    # PERF-10: discover rows with one task-scoped page scan instead of one
    # independent row read per manifest lane; exact-tip gates stay narrow below.
    rows_by_id: dict[str, dict[str, object]] = {}
    offset = 0
    while True:
        raw = lanes_recording.list_lanes(task_ref=task_ref, status="all", limit=_PAGE_SIZE, offset=offset)
        data = _data(raw)
        rows = data.get("lanes") if data is not None else None
        if not isinstance(rows, list):
            raise RuntimeError("lane_rows_unreadable")
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("lane_id"), str):
                rows_by_id[str(row["lane_id"])] = row
        has_more = data.get("has_more") is True
        if not has_more:
            break
        if not rows:
            raise RuntimeError("lane_rows_pagination_stalled")
        offset += len(rows)
    return rows_by_id


def _passing_gate_id(task_ref: str, expected_tip: str) -> tuple[int | None, str | None]:
    from workbay_handoff_mcp import verified_tests

    try:
        raw = verified_tests.get_verified_tests(
            task_ref=task_ref,
            commit_sha=expected_tip,
            passed=True,
            limit=1,
        )
    except Exception as exc:  # an unreadable gate is uncertainty, never a pass
        return None, f"verified_tests_read_failed:{type(exc).__name__}"
    data = _data(raw)
    tests = data.get("tests") if data is not None else None
    if not isinstance(tests, list):
        return None, "verified_tests_unreadable"
    for row in tests:
        if not isinstance(row, dict) or row.get("passed") is not True or row.get("commit_sha") != expected_tip:
            continue
        test_id = row.get("id")
        if isinstance(test_id, int) and not isinstance(test_id, bool) and test_id > 0:
            return test_id, None
    return None, None


def _writer_lock_state(state_dir: Path, lane_id: str) -> tuple[bool | None, str | None]:
    """Probe the worker's kernel lock; lock contents/status strings are not authority."""

    lock_path = state_dir / f"worker-{lane_id}.lock"
    fd: int | None = None
    try:
        # O_CREAT gives a stable inode for future dispatches and makes the
        # non-blocking flock the evidence. The file is retained: unlinking it
        # after a successful probe could split concurrent lockers across inodes.
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False, "worker_lock_held"
        finally:
            # Unlock below only when this process acquired the flock.
            pass
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True, None
    except OSError as exc:
        return None, f"worker_lock_probe_failed:{type(exc).__name__}"
    finally:
        if fd is not None:
            os.close(fd)


def _stable_run_id(task_ref: str, lane_id: str, expected_tip: str, integration_ref: str) -> str:
    # DATA-13: bind retries to the full causal identity at the side-effect sink.
    identity = json.dumps(
        [task_ref, lane_id, expected_tip, integration_ref],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()[:32]


def _git_ref_present(root: Path, branch: str) -> tuple[bool | None, str | None]:
    import subprocess

    ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"branch_probe_failed:{type(exc).__name__}"
    if proc.returncode == 1:
        return False, None
    if proc.returncode != 0:
        return None, "branch_probe_failed"
    return True, None


def _valid_local_feature_branch(root: Path, branch: str) -> str | None:
    """Require an explicit, well-formed feature branch present in this checkout."""

    import subprocess

    if branch == "main" or not branch.startswith("feature/"):
        return "integration_ref_must_be_explicit_feature_branch"
    ref = f"refs/heads/{branch}"
    try:
        syntax = subprocess.run(
            ["git", "-C", str(root), "check-ref-format", ref],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "integration_ref_probe_failed"
    if syntax.returncode != 0:
        return "integration_ref_must_be_explicit_feature_branch"
    present, probe_error = _git_ref_present(root, ref)
    if probe_error:
        return "integration_ref_probe_failed"
    if present is not True:
        return "integration_ref_not_a_local_branch"
    return None


def _worktree_registered(root: Path, worktree_path: str | None) -> tuple[bool | None, str | None]:
    import subprocess

    if not worktree_path:
        return False, None
    expected = str(Path(worktree_path).expanduser().resolve())
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"worktree_probe_failed:{type(exc).__name__}"
    if proc.returncode != 0:
        return None, "worktree_probe_failed"
    paths = {
        line.removeprefix("worktree ").strip() for line in proc.stdout.splitlines() if line.startswith("worktree ")
    }
    return expected in paths, None


def _cleanup_complete(
    *,
    root: Path,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    integration_ref: str,
    run_id: str,
    intent_landing_commit: str | None,
    row: Mapping[str, object],
    actuator: Mapping[str, object],
) -> tuple[bool, str | None]:
    from . import landing_cleanup

    landing_commit = actuator.get("landing_commit")
    if not isinstance(landing_commit, str) or _FULL_SHA.fullmatch(landing_commit) is None:
        landing_commit = intent_landing_commit
    if not isinstance(landing_commit, str) or _FULL_SHA.fullmatch(landing_commit) is None:
        return False, "cleanup_intent_landing_commit_missing"
    identity = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "expected_tip": expected_tip,
        "integration_ref": integration_ref,
        "landing_commit": landing_commit,
        "run_id": run_id,
    }
    try:
        intent = landing_cleanup.read_verified_intent(root, identity=identity)
    except Exception as exc:
        return False, f"cleanup_intent_unreadable:{type(exc).__name__}"
    if intent is None:
        return False, "cleanup_intent_missing"
    if intent.get("local_status") != "complete" or intent.get("remote_status") != "complete":
        return False, "cleanup_intent_pending"
    if intent.get("completed_at") is None:
        return False, "cleanup_intent_completion_unverified"
    if actuator.get("projection_pending") is True:
        return False, "projection_pending"
    retire_outcome = actuator.get("retire_outcome")
    if not isinstance(retire_outcome, str) or retire_outcome.startswith("skipped:"):
        return False, f"retirement_unresolved:{retire_outcome or 'missing'}"
    registered, worktree_error = _worktree_registered(
        root,
        row.get("worktree_path") if isinstance(row.get("worktree_path"), str) else None,
    )
    if worktree_error or registered is not False:
        return False, worktree_error or "worktree_still_registered"
    branch = row.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        return False, "branch_identity_missing"
    present, branch_error = _git_ref_present(root, branch)
    if branch_error or present is not False:
        return False, branch_error or "lane_branch_still_present"
    if retire_outcome not in _CLEANUP_COMPLETE and not bool(actuator.get("bundle_path")):
        return False, f"retirement_unverified:{retire_outcome}"
    return True, None


def _pending_cleanup_intents(
    *,
    root: Path,
    task_ref: str,
    integration_ref: str,
    deadline: float,
) -> list[dict[str, object]]:
    """Read every unresolved identity for this target within the drain budget."""

    from . import landing_cleanup

    records: list[dict[str, object]] = []
    cursor: str | None = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise landing_cleanup.CleanupIntentError("cleanup_intent_scan_budget_exhausted")
        page = landing_cleanup.read_pending_intents(
            root,
            task_ref=task_ref,
            integration_ref=integration_ref,
            cursor=cursor,
            limit=100,
            budget_seconds=min(0.1, remaining),
        )
        intents = page.get("intents")
        if not isinstance(intents, list):
            raise landing_cleanup.CleanupIntentError("cleanup_intent_page_invalid")
        for intent in intents:
            if not isinstance(intent, dict):
                raise landing_cleanup.CleanupIntentError("cleanup_intent_page_invalid")
            records.append(intent)
        if page.get("has_more") is not True:
            return records
        next_cursor = page.get("cursor")
        if not isinstance(next_cursor, str) or next_cursor == cursor:
            raise landing_cleanup.CleanupIntentError("cleanup_intent_pagination_stalled")
        cursor = next_cursor


def _commit_for_ref(root: Path, integration_ref: str) -> tuple[str | None, str | None]:
    import subprocess

    ref = integration_ref if integration_ref.startswith("refs/heads/") else f"refs/heads/{integration_ref}"
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"integration_tip_probe_failed:{type(exc).__name__}"
    if proc.returncode != 0 or _FULL_SHA.fullmatch(proc.stdout.strip()) is None:
        return None, "integration_tip_probe_failed"
    return proc.stdout.strip(), None


def _is_ancestor(root: Path, ancestor: str, descendant: str) -> bool | None:
    import subprocess

    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _prepare_contained_landing(
    *,
    root: Path,
    state_dir: Path,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    integration_ref: str,
    run_id: str,
    lane_row: Mapping[str, object],
    existing_containing_sha: str | None = None,
) -> tuple[bool, str | None, str | None]:
    """Persist actual containment and close the durable lane before replay."""

    from . import landing_cleanup, offload_pass

    branch = lane_row.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        return False, "contained_lane_branch_missing", None
    if lane_row.get("branch_tip_sha") != expected_tip:
        return False, "contained_lane_snapshot_tip_mismatch", None
    lock_free, lock_detail = _writer_lock_state(state_dir, lane_id)
    if lock_free is not True:
        return False, lock_detail or "worker_lock_state_unknown", None

    current_sha, tip_error = _commit_for_ref(root, integration_ref)
    if tip_error is not None or current_sha is None:
        return False, tip_error or "integration_tip_probe_failed", None
    containing_sha = existing_containing_sha or current_sha
    if _FULL_SHA.fullmatch(containing_sha) is None:
        return False, "contained_landing_sha_invalid", None
    if _is_ancestor(root, containing_sha, current_sha) is not True:
        return False, "contained_landing_sha_not_in_integration_ref", None
    if _is_ancestor(root, expected_tip, containing_sha) is not True:
        return False, "lane_tip_not_ancestor_of_containing_sha", None

    identity = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "expected_tip": expected_tip,
        "integration_ref": integration_ref,
        "landing_commit": containing_sha,
        "run_id": run_id,
    }
    try:
        landing_cleanup.ensure_intent(root, identity=identity, lane_row=lane_row)
    except Exception as exc:
        return False, f"contained_cleanup_intent_unavailable:{type(exc).__name__}", containing_sha

    try:
        close_result = offload_pass.close_offload_lane_merged(
            task_ref=task_ref,
            lane_id=lane_id,
            notes="Closed after exact lane tip was verified inside the explicit feature integration ref.",
            force=True,
            orchestrator_root=root,
            lane_row=dict(lane_row),
            landed_sha=containing_sha,
        )
    except Exception as exc:
        return False, f"contained_lane_close_failed:{type(exc).__name__}", containing_sha
    if not isinstance(close_result, Mapping) or close_result.get("ok") is not True:
        reason = close_result.get("error") if isinstance(close_result, Mapping) else None
        return False, f"contained_lane_close_failed:{reason or 'unreadable_result'}", containing_sha
    return True, None, containing_sha


def _valid_inputs(
    *,
    task_ref: str,
    integration_root: Path,
    integration_ref: str,
    manifest_root: Path,
    max_lanes: int,
    budget_seconds: float,
) -> str | None:
    if not task_ref or "/" in task_ref or any(char.isspace() for char in task_ref):
        return "invalid_task_ref"
    if not integration_ref:
        return "integration_ref_must_be_explicit_feature_branch"
    if type(max_lanes) is not int or max_lanes <= 0:
        return "max_lanes_must_be_positive_int"
    if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)):
        return "budget_seconds_must_be_positive_finite_number"
    if not math.isfinite(float(budget_seconds)) or budget_seconds <= 0:
        return "budget_seconds_must_be_positive_finite_number"
    if not integration_root.is_dir() or not (integration_root / ".git").exists():
        return "integration_root_not_git_checkout"
    ref_error = _valid_local_feature_branch(integration_root, integration_ref)
    if ref_error is not None:
        return ref_error
    if not manifest_root.is_dir():
        return "manifest_root_not_directory"
    return None


def drain_completed_lanes(
    *,
    task_ref: str,
    integration_root: str | Path,
    integration_ref: str,
    manifest_root: str | Path,
    state_dir: str | Path,
    max_lanes: int,
    budget_seconds: float,
) -> LandingDrainResult:
    """Land eligible durable lane tips into the explicit feature integration ref.

    ``manifest_root`` is the canonical directory containing ``<task_ref>.json``;
    it is intentionally independent of ``integration_root``. A passing test
    recorded for the exact durable tip and a free worker flock are both required
    before calling the real landing actuator. Worker report classifications and
    lane status labels do not determine eligibility.

    Calls are serial because every candidate mutates the same integration ref
    [GRPH-09]. The caller's manifest order is checked against dependency edges
    [GRPH-01]; each call has an explicit input/output contract [GRPH-33]. The
    lane count and elapsed-time budget bound one pass [RES-06].
    """

    from . import lane_land

    root = Path(integration_root).expanduser().resolve()
    manifests = Path(manifest_root).expanduser().resolve()
    workers = Path(state_dir).expanduser().resolve()
    normalized_ref = integration_ref.strip() if isinstance(integration_ref, str) else ""
    normalized_ref = normalized_ref.removeprefix("refs/heads/")
    error = _valid_inputs(
        task_ref=task_ref,
        integration_root=root,
        integration_ref=normalized_ref,
        manifest_root=manifests,
        max_lanes=max_lanes,
        budget_seconds=budget_seconds,
    )
    if error is not None:
        return LandingDrainResult(task_ref, normalized_ref, (), (), (), (), False, error)

    workers.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + float(budget_seconds)
    outcomes: list[LaneDrainOutcome] = []
    pending: set[str] = set()
    pending_cleanup: set[str] = set()
    remaining: list[str] = []

    try:
        manifest = _task_manifest(task_ref, manifests)
        configured_target = manifest.get("integration_branch")
        if isinstance(configured_target, str) and configured_target.removeprefix(
            "refs/heads/"
        ) != normalized_ref.removeprefix("refs/heads/"):
            raise ValueError("integration_ref_does_not_match_manifest")
        lane_ids = manifest.get("_drain_lane_ids")
        if not isinstance(lane_ids, list):
            raise ValueError("manifest_lane_order_invalid")
        rows = _list_durable_lanes(task_ref)
        cleanup_intents = _pending_cleanup_intents(
            root=root,
            task_ref=task_ref,
            integration_ref=normalized_ref,
            deadline=deadline,
        )
    except Exception as exc:
        return LandingDrainResult(
            task_ref,
            normalized_ref,
            (),
            (),
            (),
            (),
            False,
            f"eligibility_scan_failed:{type(exc).__name__}:{exc}",
        )

    pending_by_lane: dict[str, list[dict[str, object]]] = {}
    for intent in cleanup_intents:
        identity = intent.get("identity")
        if not isinstance(identity, dict) or not isinstance(identity.get("lane_id"), str):
            return LandingDrainResult(
                task_ref,
                normalized_ref,
                (),
                (),
                (),
                (),
                False,
                "cleanup_intent_identity_corrupt",
            )
        pending_by_lane.setdefault(identity["lane_id"], []).append(intent)

    eligible: list[tuple[str, dict[str, object], str, str, bool, str | None]] = []
    by_lane = manifest.get("lanes")
    assert isinstance(by_lane, dict)
    manifest_lane_ids = {lane_id for lane_id in lane_ids if isinstance(lane_id, str)}
    for index, lane_id in enumerate(lane_ids):
        if time.monotonic() >= deadline:
            remaining.extend(str(value) for value in lane_ids[index:])
            pending.update(remaining)
            outcomes.extend(LaneDrainOutcome(str(value), "deferred_budget") for value in lane_ids[index:])
            break
        if not isinstance(lane_id, str):
            continue
        if not lane_id or "/" in lane_id or any(char.isspace() for char in lane_id):
            outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", detail="invalid_lane_id"))
            pending.add(lane_id)
            continue
        lane_intents = pending_by_lane.get(lane_id, [])
        if len(lane_intents) > 1:
            outcomes.append(
                LaneDrainOutcome(lane_id, "eligibility_uncertain", detail="multiple_pending_cleanup_identities")
            )
            pending.add(lane_id)
            pending_cleanup.add(lane_id)
            continue
        resume_intent = lane_intents[0] if lane_intents else None
        row = rows.get(lane_id)
        if resume_intent is not None:
            snapshot = resume_intent.get("lane_row")
            identity = resume_intent.get("identity")
            if not isinstance(snapshot, dict) or not isinstance(identity, dict):
                outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", detail="cleanup_intent_corrupt"))
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
                continue
            expected_tip = identity.get("expected_tip")
            branch = snapshot.get("branch")
            candidate_row = dict(snapshot)
            if row is not None and row.get("branch") != branch:
                outcomes.append(
                    LaneDrainOutcome(
                        lane_id, "eligibility_uncertain", expected_tip, detail="cleanup_intent_branch_mismatch"
                    )
                )
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
                continue
            retry_run_id = identity.get("run_id")
        else:
            if row is None:
                outcomes.append(LaneDrainOutcome(lane_id, "no_durable_lane_row"))
                continue
            candidate_row = row
            expected_tip = row.get("branch_tip_sha")
            branch = row.get("branch")
            retry_run_id = None
        manifest_lane = by_lane.get(lane_id)
        manifest_branch = manifest_lane.get("branch") if isinstance(manifest_lane, dict) else None
        if not isinstance(expected_tip, str) or _FULL_SHA.fullmatch(expected_tip) is None:
            if resume_intent is not None:
                outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", detail="cleanup_intent_tip_invalid"))
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
            else:
                outcomes.append(LaneDrainOutcome(lane_id, "no_durable_tip"))
            continue
        if not isinstance(branch, str) or not branch.strip() or branch != manifest_branch:
            detail = "manifest_lane_row_branch_mismatch"
            outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", expected_tip, detail=detail))
            pending.add(lane_id)
            if resume_intent is not None:
                pending_cleanup.add(lane_id)
            continue

        if resume_intent is None:
            gate_id, gate_error = _passing_gate_id(task_ref, expected_tip)
            if gate_error:
                outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", expected_tip, detail=gate_error))
                pending.add(lane_id)
                continue
            if gate_id is None:
                outcomes.append(LaneDrainOutcome(lane_id, "waiting_for_exact_tip_gate", expected_tip))
                continue

        lock_free, lock_detail = _writer_lock_state(workers, lane_id)
        if lock_free is False:
            outcomes.append(LaneDrainOutcome(lane_id, "live_writer", expected_tip, detail=lock_detail))
            pending.add(lane_id)
            continue
        if lock_free is None:
            outcomes.append(LaneDrainOutcome(lane_id, "eligibility_uncertain", expected_tip, detail=lock_detail))
            pending.add(lane_id)
            continue
        if resume_intent is not None and not isinstance(retry_run_id, str):
            outcomes.append(
                LaneDrainOutcome(lane_id, "eligibility_uncertain", expected_tip, detail="cleanup_run_id_invalid")
            )
            pending.add(lane_id)
            pending_cleanup.add(lane_id)
            continue
        run_id = (
            retry_run_id
            if isinstance(retry_run_id, str)
            else _stable_run_id(task_ref, lane_id, expected_tip, normalized_ref)
        )
        retry_landing_commit = None
        if resume_intent is not None:
            resume_identity = resume_intent.get("identity")
            if isinstance(resume_identity, dict) and isinstance(resume_identity.get("landing_commit"), str):
                retry_landing_commit = resume_identity["landing_commit"]
        eligible.append((lane_id, candidate_row, expected_tip, run_id, resume_intent is not None, retry_landing_commit))

    for lane_id, intents in pending_by_lane.items():
        if lane_id not in manifest_lane_ids:
            outcomes.append(
                LaneDrainOutcome(lane_id, "cleanup_pending", detail="pending_cleanup_lane_missing_from_manifest")
            )
            pending.add(lane_id)
            pending_cleanup.add(lane_id)

    acted = 0
    for position, (lane_id, row, expected_tip, run_id, resuming_cleanup, resume_landing_commit) in enumerate(eligible):
        if acted >= max_lanes or time.monotonic() >= deadline:
            remainder = [candidate[0] for candidate in eligible[position:]]
            remaining.extend(remainder)
            pending.update(remainder)
            outcomes.extend(
                LaneDrainOutcome(candidate_id, "deferred_budget", candidate_tip, candidate_run_id)
                for candidate_id, _candidate_row, candidate_tip, candidate_run_id, _resume, _commit in eligible[
                    position:
                ]
            )
            break

        acted += 1
        try:
            # GRPH-33: all actuator inputs, especially integration_ref, are
            # explicit; never inherit lane_land's default target of main.
            raw = lane_land.land(
                task_ref,
                lane_id,
                expected_tip,
                run_id,
                integration_ref=normalized_ref,
                orchestrator_root=root,
                manifest_root=manifests,
            )
        except Exception as exc:
            outcomes.append(LaneDrainOutcome(lane_id, "actuator_uncertain", expected_tip, run_id, type(exc).__name__))
            pending.add(lane_id)
            continue

        actuator = _raw_object(raw)
        if actuator is None:
            outcomes.append(
                LaneDrainOutcome(lane_id, "actuator_uncertain", expected_tip, run_id, "unreadable_envelope")
            )
            pending.add(lane_id)
            continue
        outcome = actuator.get("outcome")
        detail = actuator.get("detail") if isinstance(actuator.get("detail"), str) else None
        intent_landing_commit: str | None = None
        if resuming_cleanup:
            intent_landing_commit = resume_landing_commit

        if outcome == "contained_no_receipt":
            candidate_intent = pending_by_lane.get(lane_id, [])
            existing_containing_sha: str | None = None
            if candidate_intent:
                saved_identity = candidate_intent[0].get("identity")
                if isinstance(saved_identity, dict):
                    saved_sha = saved_identity.get("landing_commit")
                    if isinstance(saved_sha, str):
                        existing_containing_sha = saved_sha
            prepared, prepare_error, containing_sha = _prepare_contained_landing(
                root=root,
                state_dir=workers,
                task_ref=task_ref,
                lane_id=lane_id,
                expected_tip=expected_tip,
                integration_ref=normalized_ref,
                run_id=run_id,
                lane_row=row,
                existing_containing_sha=existing_containing_sha,
            )
            if not prepared:
                outcomes.append(
                    LaneDrainOutcome(
                        lane_id,
                        "contained_no_receipt",
                        expected_tip,
                        run_id,
                        prepare_error or detail or "contained_landing_unverified",
                        actuator,
                    )
                )
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
                continue
            intent_landing_commit = containing_sha
            try:
                # The supported lane-land actuator consumes the durable
                # containment identity and performs the same bounded cleanup.
                raw = lane_land.land(
                    task_ref,
                    lane_id,
                    expected_tip,
                    run_id,
                    integration_ref=normalized_ref,
                    orchestrator_root=root,
                    manifest_root=manifests,
                )
            except Exception as exc:
                outcomes.append(
                    LaneDrainOutcome(lane_id, "actuator_uncertain", expected_tip, run_id, type(exc).__name__)
                )
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
                continue
            actuator = _raw_object(raw)
            if actuator is None:
                outcomes.append(
                    LaneDrainOutcome(lane_id, "actuator_uncertain", expected_tip, run_id, "unreadable_envelope")
                )
                pending.add(lane_id)
                pending_cleanup.add(lane_id)
                continue
            outcome = actuator.get("outcome")
            detail = actuator.get("detail") if isinstance(actuator.get("detail"), str) else None
        if outcome not in _SUCCESS_OUTCOMES:
            status = str(outcome or "actuator_refused")
            outcomes.append(LaneDrainOutcome(lane_id, status, expected_tip, run_id, detail, actuator))
            pending.add(lane_id)
            if (
                resuming_cleanup
                or outcome in {"cleanup_pending", "contained_no_receipt"}
                or actuator.get("cleanup_pending") is True
            ):
                pending_cleanup.add(lane_id)
            continue

        cleanup_done, cleanup_reason = _cleanup_complete(
            root=root,
            task_ref=task_ref,
            lane_id=lane_id,
            expected_tip=expected_tip,
            integration_ref=normalized_ref,
            run_id=run_id,
            intent_landing_commit=intent_landing_commit,
            row=row,
            actuator=actuator,
        )
        status = str(outcome) if cleanup_done else "cleanup_pending"
        outcomes.append(LaneDrainOutcome(lane_id, status, expected_tip, run_id, cleanup_reason, actuator))
        if not cleanup_done:
            pending.add(lane_id)
            pending_cleanup.add(lane_id)

    # DDIA durable causal identity: the branch tip, passing gate, lane row, and
    # receipt let a later bounded pass rediscover and replay incomplete cleanup.
    # RES-06: no in-call retry loop; return the obligation for a later pass.
    admission_allowed = not pending and not remaining and not pending_cleanup
    return LandingDrainResult(
        task_ref=task_ref,
        integration_ref=normalized_ref,
        outcomes=tuple(outcomes),
        remaining=tuple(remaining),
        pending=tuple(sorted(pending)),
        pending_cleanup=tuple(sorted(pending_cleanup)),
        admission_allowed=admission_allowed,
    )
