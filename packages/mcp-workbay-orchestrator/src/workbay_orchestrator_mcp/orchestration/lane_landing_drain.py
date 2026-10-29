"""Bounded drain for durable, verified lane tips.

The drain is deliberately a coordinator of existing authorities: the manifest
defines the task's lane order, handoff rows preserve the observed lane tip,
verified tests authorize that exact tip, and :mod:`lane_land` owns every Git
mutation and retirement decision.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held

_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_SUCCESS_OUTCOMES = frozenset({"landed", "already_landed", "contained_reaped", "contained_retired"})
_CLEANUP_COMPLETE = frozenset({"reclaimed", "already_retired", "skipped_no_worktree_path"})
_PAGE_SIZE = 200
_LOGGER = logging.getLogger(__name__)
# Statuses a refused retirement may restore. The close compare-and-set returns
# whichever of these the live row actually held; an earlier snapshot must not.
_RESTORABLE_PREDECESSOR_STATUSES = frozenset({"planned", "active", "blocked", "review"})


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
    quarantined_cleanup_intents: list[dict[str, str]] = field(default_factory=list)
    quarantined_cleanup_intent_count: int = 0
    quarantined_cleanup_intents_by_reason: dict[str, int] = field(default_factory=dict)


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
    """Probe through the shared kernel oracle, failing closed on uncertainty."""
    from .worker_lock import lock_is_held

    if lock_is_held(state_dir / f"worker-{lane_id}.lock"):
        return False, "worker_lock_held"
    return True, None


def _stable_run_id(task_ref: str, lane_id: str, expected_tip: str, integration_ref: str) -> str:
    # DATA-13: bind retries to the full causal identity at the side-effect sink.
    identity = json.dumps(
        [task_ref, lane_id, expected_tip, integration_ref],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()[:32]


def _git_ref_present(root: Path, branch: str) -> tuple[bool | None, str | None]:
    assert_no_write_lock_held("subprocess")
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
    assert_no_write_lock_held("subprocess")
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
    assert_no_write_lock_held("subprocess")
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
    integration_ref: str | None,
    deadline: float,
    quarantine_report: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    """Read every unresolved identity for this target within the drain budget."""

    from . import landing_cleanup

    records: list[dict[str, object]] = []
    quarantined: list[dict[str, str]] = []
    quarantined_count = 0
    quarantined_by_reason: dict[str, int] = {}
    cursor: str | None = None
    try:
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
                quarantine_invalid=True,
            )
            intents = page.get("intents")
            if not isinstance(intents, list):
                raise landing_cleanup.CleanupIntentError("cleanup_intent_page_invalid")
            for intent in intents:
                if not isinstance(intent, dict):
                    raise landing_cleanup.CleanupIntentError("cleanup_intent_page_invalid")
                records.append(intent)
            page_quarantined = page.get("quarantined_intents")
            if not isinstance(page_quarantined, list) or any(
                not isinstance(item, dict)
                or not isinstance(item.get("intent_id"), str)
                or not isinstance(item.get("reason"), str)
                for item in page_quarantined
            ):
                raise landing_cleanup.CleanupIntentError("cleanup_intent_page_invalid")
            quarantined_count += len(page_quarantined)
            for item in page_quarantined:
                reason = item["reason"]
                quarantined_by_reason[reason] = quarantined_by_reason.get(reason, 0) + 1
                if len(quarantined) < 50:
                    quarantined.append(item)
            if page.get("has_more") is not True:
                break
            next_cursor = page.get("cursor")
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise landing_cleanup.CleanupIntentError("cleanup_intent_pagination_stalled")
            cursor = next_cursor
        return records
    finally:
        if quarantine_report is not None and quarantined_count:
            quarantine_report.update(
                {
                    "quarantined_cleanup_intents": quarantined,
                    "quarantined_cleanup_intent_count": quarantined_count,
                    "quarantined_cleanup_intents_by_reason": dict(sorted(quarantined_by_reason.items())),
                }
            )
        if quarantined_count:
            _LOGGER.warning(
                "Quarantined %d invalid cleanup intents during landing drain; reasons=%s",
                quarantined_count,
                dict(sorted(quarantined_by_reason.items())),
            )


def _commit_for_ref(root: Path, ref_name: str, *, error_prefix: str) -> tuple[str | None, str | None]:
    assert_no_write_lock_held("subprocess")
    import subprocess

    ref = ref_name if ref_name.startswith("refs/heads/") else f"refs/heads/{ref_name}"
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"{error_prefix}_probe_failed:{type(exc).__name__}"
    raw_output = proc.stdout
    output = raw_output.strip() if isinstance(raw_output, str) else ""
    if proc.returncode == 1 and raw_output == "":
        return None, f"{error_prefix}_absent"
    if proc.returncode != 0 or _FULL_SHA.fullmatch(output) is None:
        return None, f"{error_prefix}_probe_failed"
    return output, None


def _is_ancestor(root: Path, ancestor: str, descendant: str) -> bool | None:
    assert_no_write_lock_held("subprocess")
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


def _undo_contained_predecessor_merge(
    *,
    task_ref: str,
    lane_id: str,
    prior_status: str,
    branch: object,
    worktree_path: object,
    branch_tip_sha: object,
) -> bool:
    """Put back the status a compare-and-set close claimed, if that claim is still merged."""

    import sqlite3

    from workbay_orchestrator_mcp.lanes_support import _get_db_connection

    try:
        with _get_db_connection(begin_immediate=True) as conn:
            cursor = conn.execute(
                """
                UPDATE worktree_lanes
                SET status = ?,
                    updated_at = datetime('now')
                WHERE task_ref = ? AND lane_id = ?
                  AND status = 'merged'
                  AND branch IS ?
                  AND worktree_path IS ?
                  AND branch_tip_sha IS ?
                """,
                (prior_status, task_ref, lane_id, branch, worktree_path, branch_tip_sha),
            )
            return int(cursor.rowcount or 0) == 1
    except sqlite3.Error:
        return False


def _cas_close_contained_predecessor(
    *,
    root: Path,
    task_ref: str,
    lane_id: str,
    lane_row: Mapping[str, object],
    containing_sha: str,
) -> tuple[bool, str | None, dict[str, object] | None]:
    """Compare-and-set the live predecessor row to merged.

    The expected status is the row's actual status at close time, never the
    caller's earlier snapshot. On success the returned mapping's ``status`` is
    the value that close replaced. A conflicting write leaves the row untouched.
    """

    import sqlite3

    from workbay_orchestrator_mcp.lanes_support import _get_db_connection

    from . import offload_pass

    expected_branch = lane_row.get("branch")
    expected_worktree = lane_row.get("worktree_path")
    expected_tip = lane_row.get("branch_tip_sha")
    try:
        with _get_db_connection(begin_immediate=True) as conn:
            probed = conn.execute(
                """
                SELECT status, branch, worktree_path, branch_tip_sha
                FROM worktree_lanes
                WHERE task_ref = ? AND lane_id = ?
                """,
                (task_ref, lane_id),
            ).fetchone()
            if probed is None:
                return False, "contained_lane_close_failed:lane_missing", None
            replaced_status = probed["status"]
            if not isinstance(replaced_status, str) or not replaced_status:
                return False, "contained_lane_close_failed:status_unreadable", None
            if (
                probed["branch"] != expected_branch
                or probed["worktree_path"] != expected_worktree
                or probed["branch_tip_sha"] != expected_tip
            ):
                return False, "contained_lane_close_failed:predecessor_identity_changed", None
            cursor = conn.execute(
                """
                UPDATE worktree_lanes
                SET status = 'merged',
                    updated_at = datetime('now')
                WHERE task_ref = ? AND lane_id = ?
                  AND status = ?
                  AND branch IS ?
                  AND worktree_path IS ?
                  AND branch_tip_sha IS ?
                """,
                (
                    task_ref,
                    lane_id,
                    replaced_status,
                    probed["branch"],
                    probed["worktree_path"],
                    probed["branch_tip_sha"],
                ),
            )
            if int(cursor.rowcount or 0) != 1:
                return False, "contained_lane_close_failed:predecessor_status_changed", None
    except sqlite3.Error as exc:
        return False, f"contained_lane_close_failed:{type(exc).__name__}", None

    replaced = {
        "status": replaced_status,
        "branch": expected_branch,
        "worktree_path": expected_worktree,
        "branch_tip_sha": expected_tip,
    }
    live_row = dict(lane_row)
    live_row["status"] = replaced_status
    try:
        close_result = offload_pass.close_offload_lane_merged(
            task_ref=task_ref,
            lane_id=lane_id,
            notes="Closed after exact lane tip was verified inside the explicit feature integration ref.",
            force=True,
            orchestrator_root=root,
            lane_row=live_row,
            landed_sha=containing_sha,
        )
    except Exception as exc:
        if not _undo_contained_predecessor_merge(
            task_ref=task_ref,
            lane_id=lane_id,
            prior_status=replaced_status,
            branch=expected_branch,
            worktree_path=expected_worktree,
            branch_tip_sha=expected_tip,
        ):
            return False, "contained_lane_close_failed:predecessor_open_row_restore_failed", None
        return False, f"contained_lane_close_failed:{type(exc).__name__}", None
    if not isinstance(close_result, Mapping) or close_result.get("ok") is not True:
        reason = close_result.get("error") if isinstance(close_result, Mapping) else None
        if not _undo_contained_predecessor_merge(
            task_ref=task_ref,
            lane_id=lane_id,
            prior_status=replaced_status,
            branch=expected_branch,
            worktree_path=expected_worktree,
            branch_tip_sha=expected_tip,
        ):
            return False, "contained_lane_close_failed:predecessor_open_row_restore_failed", None
        return False, f"contained_lane_close_failed:{reason or 'unreadable_result'}", None
    return True, None, replaced


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
    replaced_status: dict[str, object] | None = None,
) -> tuple[bool, str | None, str | None]:
    """Persist containment, preflight retirement, then close before replay.

    When ``replaced_status`` is supplied and the close commits, it receives the
    status the compare-and-set replaced plus the branch and worktree that close
    bound. Callers that reopen after a refused retirement must use that value.
    """

    from .. import lane_reaping
    from . import landing_cleanup

    branch = lane_row.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        return False, "contained_lane_branch_missing", None
    if lane_row.get("branch_tip_sha") != expected_tip:
        return False, "contained_lane_snapshot_tip_mismatch", None
    lock_free, lock_detail = _writer_lock_state(state_dir, lane_id)
    if lock_free is not True:
        return False, lock_detail or "worker_lock_state_unknown", None

    current_sha, tip_error = _commit_for_ref(root, integration_ref, error_prefix="integration_tip")
    if tip_error is not None or current_sha is None:
        return False, tip_error or "integration_tip_probe_failed", None
    containing_sha = existing_containing_sha or current_sha
    if _FULL_SHA.fullmatch(containing_sha) is None:
        return False, "contained_landing_sha_invalid", None
    if _is_ancestor(root, containing_sha, current_sha) is not True:
        return False, "contained_landing_sha_not_in_integration_ref", None
    if _is_ancestor(root, expected_tip, containing_sha) is not True:
        return False, "lane_tip_not_ancestor_of_containing_sha", None

    branch_tip, branch_tip_error = _commit_for_ref(root, branch, error_prefix="predecessor_branch")
    if branch_tip_error is not None or branch_tip != expected_tip:
        return False, branch_tip_error or "contained_lane_recorded_tip_mismatch", containing_sha

    try:
        from dataclasses import replace

        from . import lane_disposition
        from .lane_terminal_dispose import BranchDisposition

        snapshot = lane_disposition.collect_snapshot(root, branch, integration_ref=containing_sha)
        # The lane's attached worktree is the object under consideration;
        # keep the canonical liveness probe in the retire actuator, not in
        # this disposition preview. Project only this exact lane row into its
        # contained terminal state so the precedence table can reach its
        # MERGED_ANCESTRY rule; the durable close follows after the verdict.
        projected_rows = tuple(
            {
                **row,
                "status": "merged",
            }
            if row.get("task_ref") == task_ref and row.get("lane_id") == lane_id
            else row
            for row in snapshot.lane_rows
        )
        disposition = lane_disposition.decide(replace(snapshot, worktree_paths=(), lane_rows=projected_rows))
    except Exception as exc:
        return False, f"contained_disposition_probe_failed:{type(exc).__name__}", containing_sha
    if disposition.kind is not BranchDisposition.MERGED_ANCESTRY or disposition.branch_action != "delete_d":
        return (
            False,
            f"contained_disposition_refused:{disposition.kind.value}:{disposition.branch_action}",
            containing_sha,
        )

    identity = {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "expected_tip": expected_tip,
        "integration_ref": integration_ref,
        "landing_commit": containing_sha,
        "run_id": run_id,
    }
    try:
        intent = landing_cleanup.ensure_intent(root, identity=identity, lane_row=lane_row)
    except Exception as exc:
        return False, f"contained_cleanup_intent_unavailable:{type(exc).__name__}", containing_sha

    worktree_path = lane_row.get("worktree_path")
    try:
        retire_outcome, retire_detail = lane_reaping._reclaim_lane_worktree(
            worktree_path=worktree_path,
            branch=branch,
            repo_root=root,
            apply=False,
            integration_ref=containing_sha,
            task_ref=task_ref,
            lane_id=lane_id,
        )
    except Exception as exc:
        return False, f"predecessor_retire_preflight_probe_failed:{type(exc).__name__}", containing_sha
    if retire_outcome != "would_reclaim":
        detail = " ".join(retire_detail.splitlines())[:300]
        refusal = f"predecessor_retire_preflight_refused:{retire_outcome}"
        if detail:
            refusal = f"{refusal}:{detail}"
        return False, refusal, containing_sha

    try:
        landing_cleanup.update_intent(
            root,
            str(intent["intent_id"]),
            expected_revision=intent["revision"],
            last_error=None,
            next_attempt_at=None,
        )
    except Exception as exc:
        return False, f"predecessor_retire_preflight_state_reset_failed:{type(exc).__name__}", containing_sha

    closed, close_error, replaced = _cas_close_contained_predecessor(
        root=root,
        task_ref=task_ref,
        lane_id=lane_id,
        lane_row=lane_row,
        containing_sha=containing_sha,
    )
    if not closed or replaced is None:
        return False, close_error or "contained_lane_close_failed:unreadable_result", containing_sha
    if replaced_status is not None:
        replaced_status.clear()
        replaced_status.update(replaced)
    return True, None, containing_sha


def _predecessor_reason(intent: Mapping[str, object] | None) -> str:
    if not isinstance(intent, Mapping):
        return "cleanup_intent_unavailable"
    error = intent.get("last_error")
    if isinstance(error, str) and error.strip():
        normalized = " ".join(error.splitlines())
        for marker in ("predecessor_retire_preflight_refused:", "predecessor_retire_preflight_probe_failed:"):
            if normalized.startswith(marker):
                typed_reason = normalized[len(marker) :]
                if typed_reason.startswith(("integration_tip_", "predecessor_branch_")) or typed_reason in {
                    "predecessor_branch_rewritten",
                    "predecessor_has_uncontained_commits",
                    "predecessor_tip_refresh_conflict",
                    "predecessor_ancestry_probe_failed",
                }:
                    normalized = typed_reason
                break
        if normalized.startswith("argv:"):
            return f"live_owner:{normalized}"
        if "refused_owned" in normalized or "refused_owner_unknown" in normalized:
            detail = normalized.partition("refused_owned:")[2] or normalized.partition("refused_owner_unknown:")[2]
            return f"live_owner:{detail.strip(': ') or 'owner_probe_refused'}"
        if "refused_dirty" in normalized or "dirty worktree" in normalized or normalized.startswith("dirty:"):
            return "dirty_worktree"
        if "refused_ignored" in normalized or "ignored" in normalized:
            return "ignored_content"
        if (
            "branch_tip_mismatch" in normalized
            or "tip_moved" in normalized
            or normalized == "predecessor_recorded_tip_mismatch"
        ):
            return "recorded_tip_mismatch"
        return normalized[:300]
    local_result = intent.get("local_result")
    if isinstance(local_result, Mapping):
        detail = local_result.get("detail")
        if isinstance(detail, str) and detail.strip():
            return " ".join(detail.splitlines())[:300]
        reason = local_result.get("reason")
        if isinstance(reason, str) and reason.strip():
            return " ".join(reason.splitlines())[:300]
        outcome = local_result.get("outcome")
        if isinstance(outcome, str) and outcome.strip():
            return outcome[:300]
    return "cleanup_pending"


def _is_retire_preflight_refusal(intent: Mapping[str, object] | None) -> bool:
    if not isinstance(intent, Mapping):
        return False
    error = intent.get("last_error")
    return isinstance(error, str) and error.startswith("predecessor_retire_preflight_")


def _reopen_predecessor_after_refusal(
    intent: Mapping[str, object],
    *,
    attempt_row: Mapping[str, object] | None,
    task_ref: str,
    lane_id: str,
    reason: str,
) -> tuple[bool, str | None]:
    """Undo only this attempt's close, restoring the status that close replaced.

    ``attempt_row["status"]`` is the value the compare-and-set close returned.
    Restore it only while the row is still the merged identity that close
    produced (same branch, worktree, and tip). Any other state fails closed.
    """

    if attempt_row is None:
        return False, "predecessor_open_row_not_closed_by_attempt"
    lane_row = attempt_row
    prior_status = lane_row.get("status")
    if prior_status not in _RESTORABLE_PREDECESSOR_STATUSES:
        return True, None
    worktree_path = lane_row.get("worktree_path")
    branch = lane_row.get("branch")
    if not isinstance(worktree_path, str) or not isinstance(branch, str) or not Path(worktree_path).exists():
        return True, None

    try:
        from workbay_handoff_mcp import lanes_recording

        current = _data(lanes_recording.get_lane(lane_id=lane_id, task_ref=task_ref))
    except Exception as exc:
        return False, f"predecessor_open_row_probe_failed:{type(exc).__name__}"
    current_lane = current.get("lane") if isinstance(current, Mapping) else None
    if not isinstance(current_lane, Mapping):
        return False, "predecessor_open_row_probe_failed"
    if current_lane.get("status") != "merged":
        return False, "predecessor_open_row_status_changed"
    identity = intent.get("identity")
    if not isinstance(identity, Mapping):
        return False, "predecessor_open_row_identity_unavailable"
    if (
        current_lane.get("branch") != branch
        or current_lane.get("worktree_path") != worktree_path
        or current_lane.get("branch_tip_sha") != identity.get("expected_tip")
        or current_lane.get("landing_commit_sha") != identity.get("landing_commit")
    ):
        return False, "predecessor_open_row_identity_changed"

    try:
        reopened = lanes_recording.open_lane(
            lane_id=lane_id,
            worktree_path=worktree_path,
            branch=branch,
            task_ref=task_ref,
            status=str(prior_status),
            expected_status="merged",
            reopen_reason=f"contained predecessor retirement refused: {reason[:240]}",
        )
    except Exception as exc:
        return False, f"predecessor_open_row_restore_failed:{type(exc).__name__}"
    if not isinstance(reopened, Mapping) or reopened.get("ok") is not True:
        return False, "predecessor_open_row_restore_failed"
    return True, None


_PREDECESSOR_IDENTITY_FIELDS = ("task_ref", "lane_id", "branch", "worktree_path")
_OLDER_REFUSAL_SCAN_BUDGET_S = 5.0


def _checked_predecessor_identity(row: Mapping[str, object]) -> dict[str, object]:
    """The row fields that name which lane, branch and worktree a retirement acts on."""

    return {field: row.get(field) for field in _PREDECESSOR_IDENTITY_FIELDS}


def _settle_older_refusal_intents(
    root: Path,
    *,
    task_ref: str,
    settled: Mapping[str, tuple[Mapping[str, object], Mapping[str, object]]],
) -> dict[str, object]:
    """Complete earlier landings' pending retire refusals for lanes this landing settled.

    ``landing_cleanup`` hashes the whole intent identity, including the landing
    commit, run id and expected tip, so a refusal journaled by an earlier landing
    is never found again under this landing's identity and the drain never
    replays it. Once this landing's own intent for the lane is complete on both
    halves, any still-pending ``predecessor_retire_preflight_*`` intent for the
    same lane is moot, and is completed as skipped. An older intent is touched
    only when its snapshot names the same lane, branch and worktree this landing
    checked; anything else stays pending. Failures are reported, never raised.
    """

    from . import landing_cleanup

    completed = 0
    errors: list[str] = []
    superseded_by: dict[str, str] = {}
    for lane_id, (identity, _row) in settled.items():
        try:
            current = landing_cleanup.find_intent(root, identity=identity)
        except Exception as exc:
            errors.append(f"older_refusal_current_intent_unreadable:{lane_id}:{type(exc).__name__}")
            continue
        if (
            isinstance(current, Mapping)
            and current.get("local_status") == "complete"
            and current.get("remote_status") == "complete"
        ):
            superseded_by[lane_id] = str(current["intent_id"])
    if not superseded_by:
        return {"completed": completed, "errors": errors}

    quarantine_report: dict[str, object] = {}
    try:
        pending = _pending_cleanup_intents(
            root=root,
            task_ref=task_ref,
            integration_ref=None,
            deadline=time.monotonic() + _OLDER_REFUSAL_SCAN_BUDGET_S,
            quarantine_report=quarantine_report,
        )
    except Exception as exc:
        errors.append(f"older_refusal_scan_failed:{type(exc).__name__}:{' '.join(str(exc).splitlines())[:200]}")
        return {"completed": completed, "errors": errors, **quarantine_report}

    for intent in pending:
        identity = intent.get("identity")
        lane_id = identity.get("lane_id") if isinstance(identity, Mapping) else None
        if not isinstance(lane_id, str) or lane_id not in superseded_by:
            continue
        if intent.get("local_status") != "pending" or not _is_retire_preflight_refusal(intent):
            continue
        snapshot = intent.get("lane_row")
        checked = settled[lane_id][1]
        if not isinstance(snapshot, Mapping) or any(
            snapshot.get(field) != checked.get(field) for field in ("lane_id", "branch", "worktree_path")
        ):
            continue
        evidence = {
            "ok": True,
            "status": "complete",
            "outcome": "skipped",
            "reason": "predecessor_already_retired",
            "superseded_by": superseded_by[lane_id],
        }
        updates: dict[str, object] = {
            "local_status": "complete",
            "local_result": dict(evidence),
            "last_error": None,
            "next_attempt_at": None,
        }
        if intent.get("remote_status") == "pending":
            updates.update(remote_status="complete", remote_result=dict(evidence))
        try:
            landing_cleanup.update_intent(
                root, str(intent["intent_id"]), expected_revision=intent["revision"], **updates
            )
        except Exception as exc:
            errors.append(f"older_refusal_complete_failed:{lane_id}:{type(exc).__name__}")
            continue
        completed += 1
    return {"completed": completed, "errors": errors, **quarantine_report}


def prepare_contained_predecessors(
    *,
    root: Path,
    task_ref: str,
    lane_id: str,
    expected_tip: str,
    integration_ref: str,
    landing_commit: str,
    run_id: str,
    manifest_root: str | Path | None,
    state_dir: str | Path,
) -> dict[str, object]:
    """Journal and drain same-task predecessor lanes contained by one landing."""

    from . import landing_cleanup

    manifest_dir = (
        Path(manifest_root).expanduser().resolve()
        if manifest_root is not None
        else root / "config" / "lane-orchestration"
    )
    try:
        manifest = _task_manifest(task_ref, manifest_dir)
        manifest_lanes = manifest.get("lanes")
        dependencies = manifest.get("depends_on", {})
        if not isinstance(manifest_lanes, dict) or not isinstance(dependencies, dict):
            raise ValueError("manifest_predecessor_map_unreadable")
        target_lane = manifest_lanes.get(lane_id)
        if not isinstance(target_lane, Mapping):
            raise ValueError("manifest_landing_lane_missing")
        rows = _list_durable_lanes(task_ref)
    except Exception as exc:
        return {
            "predecessors_retired": [],
            "predecessors_kept": [{"lane_id": lane_id, "reason": f"predecessor_scan_failed:{type(exc).__name__}"}],
            "cleanup_drain": {"ok": False, "error": str(exc)[:300]},
        }

    candidates: set[str] = set()
    todo = (
        [value for value in dependencies.get(lane_id, []) if isinstance(value, str)]
        if isinstance(dependencies.get(lane_id, []), list)
        else []
    )
    while todo:
        predecessor_id = todo.pop()
        if predecessor_id == lane_id or predecessor_id in candidates or predecessor_id not in manifest_lanes:
            continue
        candidates.add(predecessor_id)
        prerequisites = dependencies.get(predecessor_id, [])
        if isinstance(prerequisites, list):
            todo.extend(value for value in prerequisites if isinstance(value, str))

    target_base_sha = target_lane.get("base_sha")
    target_base_refs = {
        str(target_lane[key]).removeprefix("refs/heads/")
        for key in ("base_branch", "base_ref", "base")
        if isinstance(target_lane.get(key), str) and str(target_lane[key]).strip()
    }
    base_ref_candidates = {
        candidate_id
        for candidate_id, row in rows.items()
        if candidate_id != lane_id
        and isinstance(row.get("branch"), str)
        and str(row["branch"]).removeprefix("refs/heads/") in target_base_refs
    }
    candidates.update(base_ref_candidates)
    if not base_ref_candidates and isinstance(target_base_sha, str):
        matching_base_tips = [
            candidate_id
            for candidate_id, row in rows.items()
            if candidate_id != lane_id and row.get("branch_tip_sha") == target_base_sha
        ]
        # A commit can be the tip of several unrelated same-task lanes. Only
        # use it as a base identity when it uniquely identifies a lane row.
        if len(matching_base_tips) == 1:
            candidates.add(matching_base_tips[0])

    queued: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    closed_by_attempt: dict[str, dict[str, object]] = {}
    retired: list[dict[str, object]] = []
    kept: dict[str, str] = {}
    # Lanes this landing has retired (or found already retired), with the identity checked for each.
    settled: dict[str, tuple[dict[str, object], Mapping[str, object]]] = {}

    def mark_retired(predecessor_id: str, identity: Mapping[str, object], row: Mapping[str, object]) -> None:
        retired.append({"lane_id": predecessor_id, "branch": row.get("branch")})
        settled[predecessor_id] = (dict(identity), row)

    def predecessor_identity(predecessor_id: str, expected_tip: str) -> dict[str, object]:
        return {
            "task_ref": task_ref,
            "lane_id": predecessor_id,
            "expected_tip": expected_tip,
            "integration_ref": integration_ref,
            "landing_commit": landing_commit,
            "run_id": run_id,
        }

    def record_refusal(
        predecessor_id: str,
        row: Mapping[str, object],
        expected_tip: str,
        reason: str,
        *,
        probe_failed: bool = False,
    ) -> None:
        identity = predecessor_identity(predecessor_id, expected_tip)
        try:
            existing = landing_cleanup.find_intent(root, identity=identity)
            intent = existing or landing_cleanup.ensure_intent(root, identity=identity, lane_row=row)
            if (
                existing is not None
                and existing.get("local_status") in {"complete", "skipped"}
                and existing.get("remote_status") in {"complete", "skipped"}
            ):
                mark_retired(predecessor_id, identity, row)
                return
            marker = (
                "predecessor_retire_preflight_probe_failed" if probe_failed else "predecessor_retire_preflight_refused"
            )
            updated = landing_cleanup.update_intent(
                root,
                str(intent["intent_id"]),
                expected_revision=intent["revision"],
                last_error=f"{marker}:{reason}",
            )
            kept[predecessor_id] = _predecessor_reason(updated)
        except Exception as exc:
            kept[predecessor_id] = f"cleanup_intent_write_failed:{type(exc).__name__}"

    for predecessor_id in sorted(candidates):
        row = rows.get(predecessor_id)
        manifest_lane = manifest_lanes.get(predecessor_id)
        if row is None or not isinstance(manifest_lane, Mapping):
            kept[predecessor_id] = "predecessor_row_or_manifest_missing"
            continue
        branch = row.get("branch")
        predecessor_tip = row.get("branch_tip_sha")
        if not isinstance(branch, str) or not branch.strip():
            kept[predecessor_id] = "predecessor_branch_missing"
            continue
        if branch != manifest_lane.get("branch"):
            kept[predecessor_id] = "manifest_lane_row_branch_mismatch"
            continue
        if not isinstance(predecessor_tip, str) or _FULL_SHA.fullmatch(predecessor_tip) is None:
            kept[predecessor_id] = "predecessor_recorded_tip_invalid"
            continue

        branch_tip, branch_error = _commit_for_ref(root, branch, error_prefix="predecessor_branch")
        if branch_error == "predecessor_branch_absent":
            identity = predecessor_identity(predecessor_id, predecessor_tip)
            try:
                existing_intent = landing_cleanup.find_intent(root, identity=identity)
                intent = existing_intent or landing_cleanup.ensure_intent(root, identity=identity, lane_row=row)
            except Exception as exc:
                kept[predecessor_id] = f"cleanup_intent_write_failed:{type(exc).__name__}"
                continue
            if (
                existing_intent is not None
                and existing_intent.get("local_status") in {"complete", "skipped"}
                and (existing_intent.get("remote_status") in {"complete", "skipped"})
            ):
                mark_retired(predecessor_id, identity, row)
                continue
            worktree_path = row.get("worktree_path")
            worktree_present = isinstance(worktree_path, str) and Path(worktree_path).expanduser().exists()
            if worktree_present:
                record_refusal(
                    predecessor_id,
                    row,
                    predecessor_tip,
                    "predecessor_branch_absent_worktree_present",
                )
                continue
            if intent.get("local_status") == "pending":
                try:
                    intent = landing_cleanup.update_intent(
                        root,
                        str(intent["intent_id"]),
                        expected_revision=intent["revision"],
                        local_status="complete",
                        local_result={
                            "ok": True,
                            "status": "complete",
                            "outcome": "skipped",
                            "reason": "predecessor_already_retired",
                        },
                        last_error=None,
                        next_attempt_at=None,
                    )
                except Exception as exc:
                    kept[predecessor_id] = f"cleanup_intent_write_failed:{type(exc).__name__}"
                    continue
            queued[predecessor_id] = (identity, dict(row))
            continue
        if branch_error is not None or branch_tip is None:
            record_refusal(
                predecessor_id,
                row,
                predecessor_tip,
                branch_error or "predecessor_branch_probe_failed",
                probe_failed=True,
            )
            continue

        stored_tip_is_ancestor = _is_ancestor(root, predecessor_tip, branch_tip)
        if stored_tip_is_ancestor is None:
            record_refusal(
                predecessor_id,
                row,
                predecessor_tip,
                "predecessor_ancestry_probe_failed",
                probe_failed=True,
            )
            continue
        if not stored_tip_is_ancestor:
            record_refusal(predecessor_id, row, predecessor_tip, "predecessor_branch_rewritten")
            continue

        branch_tip_is_contained = _is_ancestor(root, branch_tip, landing_commit)
        if branch_tip_is_contained is None:
            record_refusal(
                predecessor_id,
                row,
                predecessor_tip,
                "predecessor_ancestry_probe_failed",
                probe_failed=True,
            )
            continue
        if not branch_tip_is_contained:
            record_refusal(predecessor_id, row, predecessor_tip, "predecessor_has_uncontained_commits")
            continue

        if branch_tip != predecessor_tip:
            if row.get("branch_tip_source") != "branch":
                record_refusal(predecessor_id, row, predecessor_tip, "predecessor_recorded_tip_mismatch")
                continue
            from workbay_handoff_mcp import lanes_recording

            # The compare-and-set fences task, lane, tip and source, not the branch or
            # worktree this loop checked. Keep what was checked and refuse any drift.
            checked_identity = _checked_predecessor_identity(row)
            expected_source = row.get("branch_tip_source")
            refresh_result = lanes_recording.update_lane(
                task_ref=task_ref,
                lane_id=predecessor_id,
                branch_tip_sha=branch_tip,
                branch_tip_source="branch",
                expected_branch_tip_sha=predecessor_tip,
                expected_branch_tip_source=expected_source if isinstance(expected_source, str) else None,
            )
            refresh_data = _data(refresh_result)
            refreshed_row = refresh_data.get("lane") if refresh_data is not None else None
            if not isinstance(refreshed_row, Mapping) or refreshed_row.get("branch_tip_sha") != branch_tip:
                record_refusal(predecessor_id, row, predecessor_tip, "predecessor_tip_refresh_conflict")
                continue
            if _checked_predecessor_identity(refreshed_row) != checked_identity:
                # Nothing is journaled or closed for an identity this loop never checked.
                kept[predecessor_id] = "predecessor_identity_changed"
                continue
            row = dict(refreshed_row)
            predecessor_tip = branch_tip

        identity = predecessor_identity(predecessor_id, predecessor_tip)
        try:
            existing_intent = landing_cleanup.find_intent(root, identity=identity)
            intent = existing_intent or landing_cleanup.ensure_intent(root, identity=identity, lane_row=row)
        except Exception as exc:
            kept[predecessor_id] = f"cleanup_intent_write_failed:{type(exc).__name__}"
            continue
        if existing_intent is not None:
            if existing_intent.get("local_status") in {"complete", "skipped"} and existing_intent.get(
                "remote_status"
            ) in {"complete", "skipped"}:
                mark_retired(predecessor_id, identity, row)
                continue
            if not _is_retire_preflight_refusal(existing_intent):
                queued[predecessor_id] = (identity, dict(row))
                continue

        replaced_status: dict[str, object] = {}
        prepared, prepare_error, containing_sha = _prepare_contained_landing(
            root=root,
            state_dir=Path(state_dir).expanduser().resolve(),
            task_ref=task_ref,
            lane_id=predecessor_id,
            expected_tip=predecessor_tip,
            integration_ref=integration_ref,
            run_id=run_id,
            lane_row=row,
            existing_containing_sha=landing_commit,
            replaced_status=replaced_status,
        )
        if not prepared or containing_sha != landing_commit:
            reason = prepare_error or "contained_landing_unverified"
            if not reason.startswith("predecessor_retire_preflight_") and not reason.startswith(
                "contained_lane_close_failed:"
            ):
                reason = f"predecessor_retire_preflight_refused:{reason}"
            latest = landing_cleanup.find_intent(root, identity=identity)
            if latest is not None:
                try:
                    updated = landing_cleanup.update_intent(
                        root,
                        str(latest["intent_id"]),
                        expected_revision=latest["revision"],
                        last_error=reason,
                    )
                    reason = _predecessor_reason(updated)
                except Exception:
                    pass
            kept[predecessor_id] = reason
            continue
        queued[predecessor_id] = (identity, dict(row))
        # Recovery must restore the status this close replaced. The row captured
        # at scan time can already be stale by the time the compare-and-set runs.
        if replaced_status.get("status") in _RESTORABLE_PREDECESSOR_STATUSES:
            closed_by_attempt[predecessor_id] = dict(replaced_status)

    try:
        drain = landing_cleanup.drain_pending_cleanup_intents(root, max_intents=10, budget_seconds=30)
    except Exception as exc:
        drain = {"ok": False, "error": f"{type(exc).__name__}:{str(exc)[:300]}"}

    for predecessor_id, (identity, row) in queued.items():
        try:
            intent = landing_cleanup.find_intent(root, identity=identity)
        except Exception:
            intent = None
        if (
            isinstance(intent, Mapping)
            and intent.get("local_status") in {"complete", "skipped"}
            and intent.get("remote_status") in {"complete", "skipped"}
        ):
            mark_retired(predecessor_id, identity, row)
        else:
            reason = _predecessor_reason(intent)
            if isinstance(intent, Mapping) and intent.get("local_status") == "pending":
                reopened, reopen_error = _reopen_predecessor_after_refusal(
                    intent,
                    attempt_row=closed_by_attempt.get(predecessor_id),
                    task_ref=task_ref,
                    lane_id=predecessor_id,
                    reason=reason,
                )
                if not reopened:
                    reason = reopen_error or "predecessor_open_row_restore_failed"

                last_error = intent.get("last_error")
                if not isinstance(last_error, str) or not last_error.startswith("predecessor_retire_preflight_"):
                    last_error = f"predecessor_retire_preflight_refused:{last_error or reason}"
                try:
                    latest = landing_cleanup.find_intent(root, identity=identity)
                    if latest is not None:
                        updated = landing_cleanup.update_intent(
                            root,
                            str(latest["intent_id"]),
                            expected_revision=latest["revision"],
                            last_error=last_error,
                            next_attempt_at=None,
                        )
                        if reopened:
                            reason = _predecessor_reason(updated)
                except Exception as exc:
                    reason = f"predecessor_refusal_persist_failed:{type(exc).__name__}"
            kept[predecessor_id] = reason

    if settled and isinstance(drain, dict):
        older_refusals = _settle_older_refusal_intents(root, task_ref=task_ref, settled=settled)
        if (
            older_refusals["completed"]
            or older_refusals["errors"]
            or older_refusals.get("quarantined_cleanup_intent_count", 0)
        ):
            drain["older_refusals"] = older_refusals

    return {
        "predecessors_retired": retired,
        "predecessors_kept": [{"lane_id": key, "reason": value} for key, value in sorted(kept.items())],
        "cleanup_drain": drain,
    }


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


def _non_success_drain_status(outcome: object, actuator: Mapping[str, object]) -> str:
    """Name the drain status for an actuator outcome outside the success set.

    ``contained_kept`` is the safety-preflight keep: the row stays open and the
    status must stay ``contained_kept``. The same actuator name is also used
    after a contained parent is merged and local removal is still outstanding.
    That obligation carries ``local_cleanup_pending`` (or a withheld cleanup
    marker) and is the cleanup-pending contract, same as a receipt landing
    whose cleanup did not finish.
    """

    status = str(outcome or "actuator_refused")
    if status != "contained_kept" or actuator.get("cleanup_pending") is not True:
        return status
    detail = actuator.get("detail")
    if actuator.get("landed_marker") == "withheld_cleanup_pending" or (
        isinstance(detail, str) and detail.startswith("local_cleanup_pending")
    ):
        return "cleanup_pending"
    return status


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

    Quarantined intents have unknown cleanup state and cannot be trusted to
    identify a lane or target. They deny admission for this pass, while valid
    lanes may still land, and remain reported without changing their files.
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
    quarantine_report: dict[str, Any] = {}

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
            quarantine_report=quarantine_report,
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
            **quarantine_report,
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
                **quarantine_report,
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
            status = _non_success_drain_status(outcome, actuator)
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
    admission_allowed = (
        not pending
        and not remaining
        and not pending_cleanup
        and not quarantine_report.get("quarantined_cleanup_intent_count", 0)
    )
    return LandingDrainResult(
        task_ref=task_ref,
        integration_ref=normalized_ref,
        outcomes=tuple(outcomes),
        remaining=tuple(remaining),
        pending=tuple(sorted(pending)),
        pending_cleanup=tuple(sorted(pending_cleanup)),
        admission_allowed=admission_allowed,
        error=",".join(sorted(quarantine_report.get("quarantined_cleanup_intents_by_reason", {}))) or None,
        **quarantine_report,
    )
