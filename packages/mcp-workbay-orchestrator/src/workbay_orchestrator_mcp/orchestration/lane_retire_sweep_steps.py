"""Cohesive units of one retire-sweep pass, coordinated by ``_sweep_locked``.

Contract: :mod:`lane_retire_sweep_context`. Every unit takes the read-only
``SweepInputs`` and the pass-owned ``SweepState``; per-candidate units also
take a ``CandidateTurn``. Collaborators and tunables are always read through
``inputs.deps`` (the ``lane_retire_sweep`` module) at call time, never bound
to module-level aliases here, so monkeypatched seams keep working.

Units are listed in the order the coordinator runs them. Each docstring names
what the unit reads, writes and which external effects it performs.
"""

from __future__ import annotations

import functools
import math
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .lane_retire_sweep_context import (
    BREAK,
    CONTINUE,
    Candidate,
    CandidateTurn,
    Control,
    LaneKey,
    SweepInputs,
    SweepState,
)

if TYPE_CHECKING:
    from .lane_disposition import SharedReads


def _lane_key(row: dict[str, Any]) -> LaneKey:
    return (str(row.get("task_ref") or ""), str(row.get("lane_id") or ""))


def add_error(inputs: SweepInputs, state: SweepState, value: str) -> None:
    """Append one bounded instrument error."""
    if len(state.instrument_errors) < inputs.deps._DETAIL_CAP:
        state.instrument_errors.append(value)


def _refuse_row(
    inputs: SweepInputs,
    state: SweepState,
    lane_id: object,
    task_ref: object,
    status: object,
    reason: str,
    *,
    detail: str | None = None,
) -> None:
    inputs.deps._add_unactionable(
        lane_id=lane_id,
        task_ref=task_ref,
        status=status,
        reason=reason,
        could_not_act=state.could_not_act,
        skip_counts=state.skip_counts,
        detail=detail,
    )


# --- Scan -------------------------------------------------------------------


def budget_stop(inputs: SweepInputs, state: SweepState, phase: str, *, with_tombstones: bool = False) -> dict[str, Any]:
    """Return the typed budget stop for ``phase``; reads scan state only."""
    deps = inputs.deps
    if not with_tombstones:
        return deps._budget_exhausted_summary(
            apply=inputs.apply,
            task_ref=inputs.task_ref,
            phase=phase,
            started=inputs.started,
            phase_timings=state.phase_timings,
            lane_progress=state.lane_progress,
            outcome=inputs.scan_exhausted_outcome,
        )
    return deps._budget_exhausted_summary(
        apply=inputs.apply,
        task_ref=inputs.task_ref,
        phase=phase,
        started=inputs.started,
        phase_timings=state.phase_timings,
        lane_progress=state.lane_progress,
        outcome=inputs.scan_exhausted_outcome,
        tombstones_drained=state.tombstones_drained,
        tombstones_pending=state.tombstones_pending,
        tombstone_could_not_act=state.could_not_act,
        tombstone_skip_counts=dict(state.skip_counts),
        tombstone_fence_release=state.fence_release,
    )


def load_lane_rows(inputs: SweepInputs, state: SweepState) -> dict[str, Any] | None:
    """Read lane rows within the scan deadline.

    Writes ``rows``, ``lane_progress``, ``lane_rows_failed`` and the
    ``lane_rows_s`` timing. Returns a budget stop or ``None`` to continue.
    """
    deps = inputs.deps
    phase_started = deps._monotonic()
    if deps._monotonic() >= inputs.budget_deadline:
        state.phase_timings["lane_rows_s"] = max(0.0, deps._monotonic() - phase_started)
        return budget_stop(inputs, state, "lane_pagination")
    try:
        state.rows = deps._load_lane_rows(
            inputs.repo_root, inputs.task_ref, deadline=inputs.scan_deadline, progress=state.lane_progress
        )
    except deps._BudgetExhausted as exc:
        state.lane_progress.update(
            {
                "pages_fetched": exc.pages_fetched,
                "rows_fetched": exc.rows_fetched,
                "total_matching": exc.total_matching,
            }
        )
        state.phase_timings["lane_rows_s"] = max(0.0, deps._monotonic() - phase_started)
        return budget_stop(inputs, state, "lane_pagination")
    except Exception as exc:  # noqa: BLE001 — an unknown row universe cannot authorize retirement
        state.rows = []
        state.lane_rows_failed = True
        if deps._monotonic() >= inputs.scan_deadline:
            state.phase_timings["lane_rows_s"] = max(0.0, deps._monotonic() - phase_started)
            return budget_stop(inputs, state, "lane_pagination")
        add_error(inputs, state, f"lane_list_failed:{type(exc).__name__}:{exc}")
    if not state.lane_progress.get("pages_fetched") and len(state.rows) > 0:
        state.lane_progress["rows_fetched"] = len(state.rows)
    state.phase_timings["lane_rows_s"] = max(0.0, deps._monotonic() - phase_started)
    if deps._monotonic() >= inputs.budget_deadline:
        return budget_stop(inputs, state, "lane_pagination")
    return None


def drain_tombstones(inputs: SweepInputs, state: SweepState) -> None:
    """Apply drains tombstone intents; dry run only counts pending ones.

    Writes the tombstone counters, refusal sinks and ``tombstone_drain_s``.
    """
    deps = inputs.deps
    tombstone_phase_started = deps._monotonic()
    try:
        if inputs.apply:
            drain_result = inputs.worktree_tombstone.drain_intents(inputs.repo_root, deadline=inputs.scan_deadline)
            state.tombstones_drained = int(drain_result.get("drained") or 0)
            state.tombstones_pending = int(drain_result.get("pending") or 0)
            state.tombstone_drain_budget_exhausted = drain_result.get("budget_exhausted") is True
            for blocked in drain_result.get("could_not_act", []):
                if not isinstance(blocked, dict):
                    continue
                _refuse_row(
                    inputs,
                    state,
                    blocked.get("lane_id"),
                    blocked.get("task_ref"),
                    "tombstone",
                    str(blocked.get("reason") or "tombstone_drain_failed"),
                )
            _collect_tombstone_fence_release(inputs, state, drain_result.get("fence_release"))
        else:
            state.tombstones_pending = inputs.worktree_tombstone.pending_intents(inputs.repo_root)
    except Exception as exc:  # noqa: BLE001 — incomplete journal recovery must not authorize deletion
        add_error(inputs, state, f"tombstone_drain_failed:{type(exc).__name__}:{exc}")
        try:
            state.tombstones_pending = inputs.worktree_tombstone.pending_intents(inputs.repo_root)
        except Exception:
            state.tombstones_pending = 0
    state.phase_timings["tombstone_drain_s"] = max(0.0, deps._monotonic() - tombstone_phase_started)


def _collect_tombstone_fence_release(inputs: SweepInputs, state: SweepState, records: object) -> None:
    """Keep each drained step's unreleased fence as a typed record [AGT-10].

    ``drain_intents`` reports a step that ran but left its row fenced (or not ours)
    both as a ``could_not_act`` entry, which the loop above already keeps, and as a
    ``fence_release`` record carrying ``reap_fence_release`` and ``drain_state``. The
    record is kept verbatim with its step, so a completed removal stays drained and
    its release failure stays visible beside it. Bounded like ``could_not_act``.
    """
    if not isinstance(records, list):
        return
    for record in records:
        if isinstance(record, dict) and len(state.fence_release) < inputs.deps._DETAIL_CAP:
            state.fence_release.append({"step": "tombstone_drain", **record})


def inventory_worktrees(inputs: SweepInputs, state: SweepState) -> dict[str, Any] | None:
    """List linked worktrees; writes ``linked`` and ``worktree_inventory_failed``."""
    deps = inputs.deps
    phase_started = deps._monotonic()
    try:
        remaining = inputs.scan_deadline - deps._monotonic()
        if remaining <= 0:
            state.phase_timings["worktree_inventory_s"] = max(0.0, deps._monotonic() - phase_started)
            return budget_stop(inputs, state, "worktree_inventory", with_tombstones=True)
        state.linked = deps._list_linked_worktrees(
            inputs.repo_root, timeout_s=min(deps._GIT_STATUS_TIMEOUT_S, remaining)
        )
    except Exception as exc:  # noqa: BLE001 — unknown Git registry fails closed
        state.linked = {}
        state.worktree_inventory_failed = True
        if deps._monotonic() < inputs.budget_deadline and not (
            inputs.scan_reservation_enabled and deps._monotonic() >= inputs.scan_deadline
        ):
            add_error(inputs, state, f"worktree_list_failed:{type(exc).__name__}:{exc}")
    state.phase_timings["worktree_inventory_s"] = max(0.0, deps._monotonic() - phase_started)
    if (
        state.worktree_inventory_failed
        and inputs.scan_reservation_enabled
        and inputs.scan_deadline <= deps._monotonic() < inputs.budget_deadline
    ):
        return budget_stop(inputs, state, "worktree_inventory", with_tombstones=True)
    if deps._monotonic() >= inputs.budget_deadline:
        return budget_stop(inputs, state, "worktree_inventory", with_tombstones=True)
    return None


def inventory_branches(inputs: SweepInputs, state: SweepState) -> dict[str, Any] | None:
    """Read the branch inventory; writes ``branches`` (``None`` when unknown)."""
    deps = inputs.deps
    state.branches = None
    phase_started = deps._monotonic()
    remaining = inputs.scan_deadline - deps._monotonic()
    if remaining > 0:
        try:
            state.branches = deps._branch_inventory(inputs.repo_root, timeout_s=remaining)
        except Exception as exc:  # noqa: BLE001 — branch-only recovery needs positive branch evidence
            if not (inputs.scan_reservation_enabled and deps._monotonic() >= inputs.scan_deadline):
                add_error(inputs, state, f"branch_inventory_failed:{type(exc).__name__}:{exc}")
        state.phase_timings["branch_inventory_s"] = max(0.0, deps._monotonic() - phase_started)
        return None
    state.phase_timings["branch_inventory_s"] = max(0.0, deps._monotonic() - phase_started)
    if deps._monotonic() >= inputs.budget_deadline:
        return budget_stop(inputs, state, "branch_inventory", with_tombstones=True)
    return None


# --- Selection --------------------------------------------------------------


def global_retry_identities(inputs: SweepInputs, state: SweepState) -> set[tuple[str, str]] | None:
    """At most one cross-task read per pass, and only when a TTL or cap decision needs it."""
    if "identities" not in state.global_identity_read:
        identities, error = inputs.deps._authoritative_lane_identities(inputs.repo_root, deadline=inputs.scan_deadline)
        state.global_identity_read.update(identities=identities, error=error)
    return state.global_identity_read["identities"]


def _filter_priority(inputs: SweepInputs, state: SweepState, row: dict[str, Any]) -> tuple[Any, ...]:
    deps = inputs.deps
    status = str(row.get("status") or "").strip().lower()
    key = _lane_key(row)
    stamp = deps._parse_timestamp(row.get("updated_at"))
    # Use the verified inventory for objective class before expensive
    # identity probes. Truncated scans must not repeatedly see old heads.
    try:
        worktree_present = deps._resolved_path(row.get("worktree_path"), relative_to=inputs.repo_root) in state.linked
    except deps._PathUnresolvable:
        # Unknown is not absence: an unresolvable path sorts with the present worktrees.
        worktree_present = True
    return (
        not worktree_present,
        state.scan_retry_entries.get(key, {}).get("attempted_at") or -math.inf,
        stamp.timestamp() if stamp is not None else -math.inf,
        0 if status in {"closed", "closed_stale"} else 2 if status == "merged" else 1,
        *key,
    )


def prepare_selection(inputs: SweepInputs, state: SweepState) -> float:
    """Pre-filter rows and order them for the identity filter.

    Reads scan state; writes the root identity, ``nonterminal``, the
    pre-filtered rows, retained retry keys, scan-order retry entries and the
    pass-record lane index. Performs the first retry-state read. Returns the
    identity phase start for its timing.
    """
    deps = inputs.deps
    phase_started = deps._monotonic()
    if inputs.scan_progress is not None:
        inputs.scan_progress.update(identity_started=phase_started, phase_timings=state.phase_timings)
    state.root_identity = deps._resolved_path(inputs.repo_root)
    state.root_branch = str((state.linked.get(state.root_identity or "") or {}).get("branch") or "").removeprefix(
        "refs/heads/"
    )
    state.nonterminal = {
        (str(row.get("task_ref")), str(row.get("lane_id")))
        for row in state.rows
        if str(row.get("status") or "").strip().lower() not in inputs.terminal_statuses
    }
    for row in state.rows:
        status = str(row.get("status") or "").strip().lower()
        if (
            status in inputs.terminal_statuses
            and row.get("lane_id")
            and row.get("task_ref")
            and not str(row.get("worktree_path") or "").strip()
            and not str(row.get("branch") or "").strip()
        ):
            state.missing_worktree_and_branch += 1
            state.skip_counts["missing_worktree_and_branch"] += 1
            continue
        state.filter_rows.append(row)
    if inputs.scan_progress is not None:
        inputs.scan_progress["filter_rows"] = len(state.filter_rows)

    state.retained_retry_keys = {_lane_key(row) for row in state.rows}
    _old_cursor, _old_priority, state.scan_retry_entries, _old_pruned = deps._load_retry_state(
        inputs.repo_root,
        now_epoch=deps.time.time(),
        retained_keys=state.retained_retry_keys,
        scope_task_ref=inputs.task_ref,
        global_identities=functools.partial(global_retry_identities, inputs, state),
    )
    if inputs.scan_reservation_enabled:
        state.filter_rows.sort(key=functools.partial(_filter_priority, inputs, state))

    for row in state.filter_rows:
        lane_id = str(row.get("lane_id") or "")
        row_task = str(row.get("task_ref") or "")
        if lane_id and row_task:
            state.pass_lanes_by_task.setdefault(row_task, set()).add(lane_id)
    state.identity_filter_timing = deps._empty_identity_filter_timing()
    if inputs.scan_progress is not None:
        inputs.scan_progress["identity_filter_timing"] = state.identity_filter_timing
    return phase_started


def latest_lane_pass_cached(inputs: SweepInputs, state: SweepState, task: str, lane: str) -> dict[str, Any] | None:
    """Read a lane's latest pass through one lazily opened pass-record reader."""
    if state.pass_records_reader is None:
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        from .offload_pass_index import TaskLanePassRecordsReader  # noqa: PLC0415

        state.pass_records_reader = TaskLanePassRecordsReader(
            RuntimeConfig.for_repo(inputs.repo_root).state_dir,
            lanes_by_task=state.pass_lanes_by_task,
        )
    return inputs.deps._latest_lane_pass(inputs.repo_root, task, lane, state.pass_records_reader)


def integration_branch_names(inputs: SweepInputs, state: SweepState, task: str) -> set[str]:
    """Return a task's protected integration branch names (manifest and refs)."""
    deps = inputs.deps
    if task not in state.manifest_cache:
        state.manifest_cache[task] = deps._read_task_manifest(inputs.repo_root, task)
    manifest = state.manifest_cache.get(task)
    names: set[str] = set()
    if isinstance(manifest, dict):
        for key in ("integration_branch", "integration_ref", "target_branch"):
            value = manifest.get(key)
            if isinstance(value, str) and value.strip():
                names.add(value.strip().removeprefix("refs/heads/"))
    try:
        refs, _source = deps._integration_refs_cached(inputs.repo_root, task, state.integration_refs_cache)
        names.update(str(ref).strip().removeprefix("refs/heads/") for ref in refs if str(ref).strip())
    except Exception:
        # The primary worktree stays protected even when its task manifest is unreadable.
        pass
    return names


def timed_identity_probe(inputs: SweepInputs, state: SweepState, family: str, probe: Callable[[], Any]) -> Any:
    """Run one identity probe and charge its time to ``family``."""
    deps = inputs.deps
    probe_started = deps._monotonic()
    try:
        return probe()
    finally:
        family_timing = state.identity_filter_timing[family]
        family_timing["calls"] += 1
        family_timing["seconds"] += max(0.0, deps._monotonic() - probe_started)


def _identity_filter_should_stop(inputs: SweepInputs, state: SweepState, filter_index: int) -> bool:
    deps = inputs.deps
    hard_deadline_expired = deps._monotonic() >= inputs.budget_deadline
    if hard_deadline_expired or (inputs.scan_reservation_enabled and deps._monotonic() >= inputs.scan_deadline):
        if hard_deadline_expired or state.candidates or state.filter_overrun_probe_used:
            state.identity_filter_stopped = True
            state.identity_filter_unvisited = len(state.filter_rows) - filter_index
            for pending_row in state.filter_rows[filter_index:]:
                _refuse_row(
                    inputs,
                    state,
                    pending_row.get("lane_id"),
                    pending_row.get("task_ref"),
                    pending_row.get("status"),
                    "budget_exhausted",
                )
            return True
        state.filter_overrun_probe_used = True
    return False


def filter_identities(inputs: SweepInputs, state: SweepState) -> None:
    """Visit pre-filtered rows in order until the scan budget stops the filter.

    Writes candidates, candidate maps, identity flags and refusals; all reads
    are bounded identity probes (pass records, Git facts, absorbed landing).
    """
    tombstone_shared_cache: dict[str, tuple[SharedReads | None, str | None, str | None]] = {}
    for filter_index, row in enumerate(state.filter_rows):
        if _identity_filter_should_stop(inputs, state, filter_index):
            break
        state.identity_filter_timing["rows_visited"] += 1
        _select_row(inputs, state, row, tombstone_shared_cache)


def _select_row(
    inputs: SweepInputs,
    state: SweepState,
    row: dict[str, Any],
    tombstone_shared_cache: dict[str, tuple[SharedReads | None, str | None, str | None]],
) -> None:
    status = str(row.get("status") or "").strip().lower()
    lane_id = row.get("lane_id")
    row_task = row.get("task_ref")
    if not lane_id or not row_task:
        _refuse_row(inputs, state, lane_id, row_task, status, "lane_identity_missing")
        return
    if "branch kept: review_payload_only_over_preserved_base" in str(row.get("notes") or ""):
        _refuse_row(inputs, state, lane_id, row_task, status, "payload_branch_kept")
        return
    located = _locate_row_worktree(inputs, state, row, status)
    if located is None:
        return
    resolved, path_exists = located
    if not _row_linkage_eligible(inputs, state, row, status, resolved, path_exists):
        return
    if _select_no_work(inputs, state, row, status, path_exists):
        return
    _select_by_landing(inputs, state, row, status, path_exists, tombstone_shared_cache)


def _locate_row_worktree(
    inputs: SweepInputs, state: SweepState, row: dict[str, Any], status: str
) -> tuple[str, bool] | None:
    """Resolve and probe a row's worktree path; records its candidate state."""
    deps = inputs.deps
    lane_id = row.get("lane_id")
    row_task = row.get("task_ref")
    try:
        resolved = deps._resolved_path(row.get("worktree_path"), relative_to=inputs.repo_root)
    except deps._PathUnresolvable:
        # A failed resolve is unknown identity, like an unreadable probe: the scan can never complete on it.
        state.identity_evidence_unknown = True
        _refuse_row(inputs, state, lane_id, row_task, status, "worktree_path_unresolvable")
        return None
    if resolved is None:
        _refuse_row(inputs, state, lane_id, row_task, status, "worktree_path_missing")
        return None
    if resolved == state.root_identity:
        _refuse_row(inputs, state, lane_id, row_task, status, "primary_worktree")
        return None

    path_exists = deps._worktree_path_exists(resolved)
    if path_exists is None:
        state.identity_evidence_unknown = True
        _refuse_row(inputs, state, lane_id, row_task, status, "worktree_path_unknown")
        return None

    row_branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    state.candidate_states[(str(row_task), str(lane_id))] = {
        "worktree_exists": path_exists,
        "branch_exists": row_branch in state.branches if state.branches is not None else None,
        "status": status,
    }
    return resolved, path_exists


def _row_linkage_eligible(
    inputs: SweepInputs,
    state: SweepState,
    row: dict[str, Any],
    status: str,
    resolved: str,
    path_exists: bool,
) -> bool:
    """Check Git linkage of a present worktree, or branch evidence of an absent one."""
    lane_id = row.get("lane_id")
    row_task = row.get("task_ref")
    if path_exists:
        linked_entry = state.linked.get(resolved)
        if linked_entry is None:
            _refuse_row(inputs, state, lane_id, row_task, status, "not_linked_worktree")
            return False
        recorded_branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
        linked_branch = str(linked_entry.get("branch") or "").strip().removeprefix("refs/heads/")
        if not recorded_branch or not linked_branch or recorded_branch != linked_branch:
            _refuse_row(inputs, state, lane_id, row_task, status, "worktree_branch_mismatch")
            return False
        protected_branches = {state.root_branch} if state.root_branch else set()
        protected_branches.update(integration_branch_names(inputs, state, str(row_task)))
        if linked_branch in protected_branches:
            _refuse_row(inputs, state, lane_id, row_task, status, "integration_worktree")
            return False
        return True
    if _no_work_recovery_candidate(row):
        # Only the release entrypoint can prove the dead holder's inode/epoch.
        return True
    if status == "planned":
        # A planned lane with no checkout is already in the desired
        # state. It is intentionally not a retirement candidate.
        return False
    if status not in inputs.terminal_statuses:
        _refuse_row(inputs, state, lane_id, row_task, status, "nonterminal_worktree_missing")
        return False
    branch = str(row.get("branch") or "").strip().removeprefix("refs/heads/")
    if state.branches is None:
        _refuse_row(inputs, state, lane_id, row_task, status, "branch_inventory_unknown")
        return False
    if not branch or branch not in state.branches:
        state.missing_worktree_and_branch += 1
        state.skip_counts["missing_worktree_and_branch"] += 1
        return False
    return True


def _no_work_recovery_candidate(row: dict[str, Any]) -> bool:
    """Routing hint only: never authority to clear a fence or remove a checkout."""
    import json  # noqa: PLC0415

    try:
        holder = json.loads(str(row.get("reclaim_fence")))
        return isinstance(holder, dict) and holder.get("purpose") == "no_work_release"
    except (ValueError, RecursionError):
        return False


def _select_no_work(
    inputs: SweepInputs, state: SweepState, row: dict[str, Any], status: str, path_exists: bool
) -> bool:
    """Admit a live-status lane whose terminal pass produced no commits."""
    deps = inputs.deps
    if not path_exists and _no_work_recovery_candidate(row):
        state.no_work_candidates[(str(row.get("task_ref")), str(row.get("lane_id")))] = {"recovery": True}
        state.candidates.append((row, str(row.get("worktree_path")), False, None))
        return True
    if not (status in {"active", "blocked", "planned"} and path_exists and row.get("lane_kind") != "review"):
        return False
    lane_id = str(row.get("lane_id"))
    row_task = str(row.get("task_ref"))
    try:
        no_work_pass = timed_identity_probe(
            inputs, state, "pass_lookup", lambda: latest_lane_pass_cached(inputs, state, row_task, lane_id)
        )
    except Exception:  # noqa: BLE001 — unreadable pass history cannot authorize release
        no_work_pass = None
        state.identity_evidence_unknown = True
    manifest = state.manifest_cache.get(row_task)
    superseded_by_manifest = isinstance(manifest, dict) and deps._manifest_supersedes_lane(manifest, lane_id)
    if not (deps._no_work_terminal_pass(no_work_pass) and not superseded_by_manifest):
        return False
    own_commits, own_detail = timed_identity_probe(
        inputs, state, "git_facts", lambda: deps._no_work_commit_count(inputs.repo_root, row, no_work_pass)
    )
    if own_commits is None:
        state.identity_evidence_unknown = True
    if own_commits != 0:
        return False
    state.no_work_candidates[(row_task, lane_id)] = {
        "pass_record": no_work_pass,
        "commit_count": own_commits,
        "commit_detail": own_detail,
    }
    state.candidates.append((row, str(row.get("worktree_path")), path_exists, None))
    return True


def _select_by_landing(
    inputs: SweepInputs,
    state: SweepState,
    row: dict[str, Any],
    status: str,
    path_exists: bool,
    tombstone_shared_cache: dict[str, tuple[SharedReads | None, str | None, str | None]],
) -> None:
    """Admit terminal rows, absorbed live rows, or work-complete live rows."""
    deps = inputs.deps
    landing: str | None = None
    if status not in inputs.terminal_statuses:
        landing, reason = timed_identity_probe(
            inputs,
            state,
            "absorbed_landing",
            lambda: deps._absorbed_landing(
                inputs.repo_root,
                row,
                state.rows,
                deadline=inputs.scan_deadline,
                manifest_cache=state.manifest_cache,
                integration_refs_cache=state.integration_refs_cache,
            ),
        )
        if reason:
            _select_unlanded(inputs, state, row, status, path_exists, reason, tombstone_shared_cache)
            return
        state.absorbed[(str(row.get("task_ref")), str(row.get("lane_id")))] = str(landing)
    state.candidates.append((row, str(row.get("worktree_path")), path_exists, landing))


def _select_unlanded(
    inputs: SweepInputs,
    state: SweepState,
    row: dict[str, Any],
    status: str,
    path_exists: bool,
    reason: str,
    tombstone_shared_cache: dict[str, tuple[SharedReads | None, str | None, str | None]],
) -> None:
    # _absorbed_landing reports unknown merge evidence with these reasons; the others are known states.
    if reason in {"merge_probe_failed", "budget_exhausted"}:
        state.identity_evidence_unknown = True
    special: dict[str, Any] | None = None
    probe_detail: str | None = None
    if str(row.get("lane_kind") or "").strip().lower() == "review":
        special = {"rule": "review_harvested"}
    else:
        handled, special, reason = _superseded_special(inputs, state, row, status, reason)
        if handled:
            return
        if special is None and reason in inputs.deps._TOMBSTONE_ADMISSION_REASONS:
            # Routing only: collect_snapshot/decide must verify the tombstone on the current tip.
            names = sorted(integration_branch_names(inputs, state, str(row.get("task_ref")))) or ["main"]
            admitted, probe_error, probe_detail = inputs.deps._tombstone_admission(
                inputs.repo_root,
                branch=str(row.get("branch") or "").strip().removeprefix("refs/heads/"),
                integration_ref=names[0],
                deadline=inputs.scan_deadline,
                shared_cache=tombstone_shared_cache,
            )
            if probe_error is not None:
                # Unknown scans keep the universe incomplete, including its truncated-sample cost penalty.
                state.identity_evidence_unknown = True
                reason = f"keep:{probe_error}"
            if admitted:
                special = {"rule": "superseded_work"}
    if special is not None:
        state.work_complete_candidates[(str(row.get("task_ref")), str(row.get("lane_id")))] = special
        state.candidates.append((row, str(row.get("worktree_path")), path_exists, None))
        return
    _refuse_row(
        inputs,
        state,
        row.get("lane_id"),
        row.get("task_ref"),
        status,
        reason if str(reason).startswith("keep:") else f"absorbed_skip:{reason}",
        detail=probe_detail,
    )


def _superseded_special(
    inputs: SweepInputs, state: SweepState, row: dict[str, Any], status: str, reason: str
) -> tuple[bool, dict[str, Any] | None, str]:
    """Probe work-complete facts of a manifest-superseded lane.

    Returns ``(handled, special, reason)``; ``handled`` means a refusal was
    already recorded and the row is done.
    """
    deps = inputs.deps
    lane_id = row.get("lane_id")
    row_task = row.get("task_ref")
    manifest = state.manifest_cache.get(str(row_task))
    if not (isinstance(manifest, dict) and deps._manifest_supersedes_lane(manifest, str(lane_id))):
        return False, None, reason
    pass_record = timed_identity_probe(
        inputs, state, "pass_lookup", lambda: latest_lane_pass_cached(inputs, state, str(row_task), str(lane_id))
    )
    names = sorted(integration_branch_names(inputs, state, str(row_task)))
    remaining_s = inputs.scan_deadline - deps._monotonic()
    if remaining_s <= 0:
        state.identity_evidence_unknown = True
        _refuse_row(inputs, state, lane_id, row_task, status, "budget_exhausted")
        return True, None, reason
    facts, fact_error = timed_identity_probe(
        inputs,
        state,
        "git_facts",
        lambda: deps._work_complete_git_facts(
            inputs.repo_root,
            row,
            manifest,
            pass_record,
            names,
            timeout_s=min(deps._ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining_s),
        ),
    )
    if fact_error:
        # superseded_branch_missing is a confirmed absence; every other fact error is an unknown probe.
        if fact_error != "superseded_branch_missing":
            state.identity_evidence_unknown = True
        return False, None, f"keep:{fact_error}"
    if facts is not None:
        return (
            False,
            {
                "rule": "work_complete_empty" if facts["own_commits"] == 0 else "superseded_work",
                "facts": facts,
                "pass_record": pass_record,
            },
            reason,
        )
    return False, None, reason


def record_scan_cost(inputs: SweepInputs, state: SweepState, identity_started: float) -> None:
    """Write the one scan-cost sample of this pass (both modes)."""
    deps = inputs.deps
    state.phase_timings["identity_and_linkage_filter_s"] = max(0.0, deps._monotonic() - identity_started)
    state.scan_cost_unreadable = None
    # A failed or unknown row, worktree, branch or identity-probe read leaves the universe partly measured: never complete.
    scan_universe_complete = not (
        state.lane_rows_failed
        or state.worktree_inventory_failed
        or state.branches is None
        or state.identity_evidence_unknown
    )
    try:
        state.scan_cost_unreadable = deps._record_scan_cost(
            inputs.repo_root,
            inputs.task_ref,
            phase_timings=state.phase_timings,
            truncated=state.identity_filter_stopped or not scan_universe_complete,
            filter_rows=len(state.filter_rows),
            visited_rows=state.identity_filter_timing["rows_visited"],
            ceiling_limited=inputs.scan_ceiling_limited,
        )
    except OSError as exc:
        add_error(inputs, state, f"scan_cost_write_failed:{type(exc).__name__}:{exc}")
    if inputs.scan_progress is not None:
        inputs.scan_progress["recorded"] = True


def candidate_priority(state: SweepState, item: Candidate) -> int:
    """Rank a candidate's disposition class (0 cheapest/most closed)."""
    row = item[0]
    key = _lane_key(row)
    status = str(row.get("status") or "").strip().lower()
    if key in state.no_work_candidates:
        return 0
    special = state.work_complete_candidates.get(key)
    if status in {"closed_stale", "closed"} or (
        isinstance(special, dict) and special.get("rule") == "review_harvested"
    ):
        return 0
    if status == "merged":
        return 2
    return 1


def compute_next_grace_expiry(inputs: SweepInputs, state: SweepState) -> None:
    """Write ``now_epoch`` and the earliest future grace expiry of a candidate."""
    deps = inputs.deps
    state.now_epoch = deps.time.time()
    state.next_grace_expiry_at = None
    for row, _worktree_path, _path_exists, _landing in state.candidates:
        stamp = deps._parse_timestamp(row.get("updated_at"))
        key = _lane_key(row)
        if key in state.no_work_candidates:
            continue
        if stamp is None:
            continue
        if key in state.work_complete_candidates:
            work_complete_grace_s, grace_error = deps._work_complete_grace_s()
            if grace_error:
                continue
            candidate_grace_s = max(inputs.grace_s, float(work_complete_grace_s or 0.0))
        else:
            candidate_grace_s = inputs.grace_s
        expiry = stamp.timestamp() + candidate_grace_s
        if expiry > state.now_epoch and (state.next_grace_expiry_at is None or expiry < state.next_grace_expiry_at):
            state.next_grace_expiry_at = expiry


def _candidate_order(inputs: SweepInputs, state: SweepState, item: Candidate) -> tuple[Any, ...]:
    row = item[0]
    key = _lane_key(row)
    stamp = inputs.deps._parse_timestamp(row.get("updated_at"))
    return (
        not item[2],
        state.retry_entries.get(key, {}).get("attempted_at") or -math.inf,
        stamp.timestamp() if stamp is not None else -math.inf,
        candidate_priority(state, item),
        *key,
    )


def order_candidates(inputs: SweepInputs, state: SweepState) -> None:
    """Second retry-state read, then the fairness order of admission.

    Writes ``cursor``, ``cursor_priority``, ``retry_entries``,
    ``retry_state_pruned``, the sorted candidates, the initial
    ``has_more``/``remaining_count`` and the candidate index map.
    """
    deps = inputs.deps
    state.cursor, state.cursor_priority, state.retry_entries, state.retry_state_pruned = deps._load_retry_state(
        inputs.repo_root,
        now_epoch=state.now_epoch,
        candidate_states=state.candidate_states,
        retained_keys=state.retained_retry_keys,
        scope_task_ref=inputs.task_ref,
        global_identities=functools.partial(global_retry_identities, inputs, state),
    )
    state.candidates.sort(key=functools.partial(_candidate_order, inputs, state))
    state.has_more = state.tombstone_drain_budget_exhausted or state.identity_filter_stopped
    state.remaining_count = (
        state.tombstones_pending if state.tombstone_drain_budget_exhausted else 0
    ) + state.identity_filter_unvisited
    for position, (candidate_row, *_candidate_context) in enumerate(state.candidates):
        state.candidate_index_by_key.setdefault(_lane_key(candidate_row), position)


# --- Admission and retry bookkeeping -----------------------------------------


def retry_candidate_state_changed(retry_entry: dict[str, Any], candidate_state: dict[str, Any]) -> bool:
    previous_state = retry_entry.get("candidate_state")
    return isinstance(previous_state, dict) and previous_state != candidate_state


def retire_admission_exhausted(inputs: SweepInputs, remaining_s: float) -> bool:
    return remaining_s < inputs.allowance_s


def record_unvisited(inputs: SweepInputs, state: SweepState, start_index: int, reason: str) -> None:
    """Refuse every candidate from ``start_index`` on with ``reason``."""
    if start_index < len(state.candidates) and any(index < start_index for index in state.deferred_indices):
        state.cursor_advanced = True
    for pending_row, *_pending_context in state.candidates[start_index:]:
        pending_key = _lane_key(pending_row)
        pending_reason = f"keep:{reason}" if pending_key in state.work_complete_candidates else reason
        _refuse_row(
            inputs,
            state,
            pending_row.get("lane_id"),
            pending_row.get("task_ref"),
            pending_row.get("status"),
            pending_reason,
        )


def defer_refused(
    inputs: SweepInputs,
    state: SweepState,
    key: LaneKey,
    candidate_state: dict[str, Any],
    *,
    reason: str | None = None,
) -> None:
    """Back off a refused candidate (apply only, once per candidate per pass)."""
    deps = inputs.deps
    # Keyed by the candidate, not the enclosing loop: keep_work_complete_candidate reaches here from other paths.
    candidate_index = state.candidate_index_by_key[key]
    if not inputs.apply or candidate_index in state.deferred_indices:
        return
    state.deferred_indices.add(candidate_index)
    prior = state.retry_entries.get(key, {})
    attempt_epoch = prior.get("attempted_at", deps.time.time())
    if retry_candidate_state_changed(prior, candidate_state):
        prior = {}
    # A streak is one refusal kind. A budget refusal says this pass ran out of time, not that the
    # lane failed, so it never joins a timeout streak. Alternating kinds therefore reset the streak.
    refusal_kind = reason.split(":", 1)[0] if reason is not None else ""
    if str(prior.get("reason", "")).split(":", 1)[0] != refusal_kind:
        prior = {}
    count = prior.get("count", 0.0) + 1
    delay = min(deps._RETRY_BASE_S * (2 ** min(int(count) - 1, 8)), deps._RETRY_MAX_S)
    if refusal_kind == "retire_timeout" and count >= 3:
        delay = deps._RETRY_MAX_S
        _refuse_row(
            inputs,
            state,
            key[1],
            key[0],
            candidate_state["status"],
            f"retire_timeout_repeated:{reason.split(':', 1)[-1]}",
        )
    state.retry_entries[key] = {
        "count": count,
        "attempted_at": attempt_epoch,
        "retry_after": deps.time.time() + delay,
        "candidate_state": candidate_state,
    }
    if reason is not None:
        state.retry_entries[key]["reason"] = reason


def keep_work_complete_candidate(
    inputs: SweepInputs, state: SweepState, row: dict[str, Any], reason: str, *, detail: str | None = None
) -> None:
    """Refuse a work-complete candidate with a typed ``keep:`` reason."""
    typed_reason = reason if reason.startswith("keep:") else f"keep:{reason}"
    _refuse_row(inputs, state, row.get("lane_id"), row.get("task_ref"), row.get("status"), typed_reason, detail=detail)
    key = _lane_key(row)
    if key in state.admitted_keys:
        # A pre-close refusal is an admitted non-timeout attempt. It
        # must yield its turn and interrupt any previous timeout streak.
        defer_refused(inputs, state, key, state.candidate_states[key])


def record_admitted_attempt(
    inputs: SweepInputs, state: SweepState, key: LaneKey, candidate_state: dict[str, Any], priority: int
) -> bool:
    """Persist an admitted apply attempt before any pre-close work."""
    deps = inputs.deps
    # Strictly increasing timestamps survive clock repetition/backwards
    # steps; a wall-clock tie must not break admission-opportunity fairness.
    last_attempt = max((entry.get("attempted_at", 0.0) for entry in state.retry_entries.values()), default=0.0)
    attempt_epoch = max(deps.time.time(), math.nextafter(last_attempt, math.inf))
    previous = state.retry_entries.get(key, {})
    if retry_candidate_state_changed(previous, candidate_state):
        previous = {}
    state.retry_entries[key] = {**previous, "attempted_at": attempt_epoch, "candidate_state": candidate_state}
    try:
        deps._save_retry_state(
            inputs.repo_root,
            cursor=key,
            cursor_priority=priority,
            entries=state.retry_entries,
            now_epoch=deps.time.time(),
            candidate_states=state.candidate_states,
            retained_keys=state.retained_retry_keys,
            scope_task_ref=inputs.task_ref,
            global_identities=functools.partial(global_retry_identities, inputs, state),
        )
    except OSError as exc:
        add_error(inputs, state, f"retry_state_write_failed:{type(exc).__name__}:{exc}")
        return False
    return True


def emit_review_event(inputs: SweepInputs, state: SweepState, event: dict[str, Any]) -> None:
    """Journal a review event before retirement, then report it."""
    inputs.deps._journal_review_event(inputs.repo_root, event)
    state.events.append(event)


# --- Per-candidate admission and disposition ----------------------------------


def _refuse(
    inputs: SweepInputs, state: SweepState, turn: CandidateTurn, reason: str, *, detail: str | None = None
) -> None:
    _refuse_row(inputs, state, turn.lane_id, turn.row_task, turn.status, reason, detail=detail)


def _refuse_or_keep(
    inputs: SweepInputs, state: SweepState, turn: CandidateTurn, keep_reason: str, reason: str
) -> Control:
    if turn.special is not None:
        keep_work_complete_candidate(inputs, state, turn.row, keep_reason)
    else:
        _refuse(inputs, state, turn, reason)
    return CONTINUE


def _stop_pass(
    inputs: SweepInputs,
    state: SweepState,
    turn: CandidateTurn,
    *,
    reason: str = "budget_exhausted",
    restore_cursor: bool = True,
    blocked: bool = False,
) -> Control:
    """End admission at this candidate; the rest are refused with ``reason``."""
    if blocked:
        state.admission_blocked = True
    if restore_cursor:
        state.cursor = turn.cursor_before_candidate
    state.has_more = True
    state.remaining_count = len(state.candidates) - turn.index
    record_unvisited(inputs, state, turn.index, reason)
    return BREAK


def begin_turn(inputs: SweepInputs, state: SweepState, index: int, item: Candidate) -> CandidateTurn:
    """Open one candidate's turn from the selection snapshot (no effects)."""
    row, worktree_path, path_exists, landing = item
    lane_id = str(row.get("lane_id") or "")
    row_task = str(row.get("task_ref") or "")
    key = (row_task, lane_id)
    return CandidateTurn(
        index=index,
        row=row,
        worktree_path=worktree_path,
        path_exists=path_exists,
        landing=landing,
        lane_id=lane_id,
        row_task=row_task,
        key=key,
        cursor_before_candidate=state.cursor,
        candidate_phase_timings={phase: 0.0 for phase in inputs.deps._RETIRE_PHASE_TIMING_KEYS},
        status=row.get("status"),
        special=state.work_complete_candidates.get(key),
        no_work=state.no_work_candidates.get(key),
    )


def gate_pass_limits(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Budget ceiling, lane cap, hard deadline and incomplete-scan stops.

    Moves the cursor to this candidate when it may proceed.
    """
    deps = inputs.deps
    if inputs.budget_ceiling_reason is not None:
        return _stop_pass(inputs, state, turn, reason=inputs.budget_ceiling_reason, restore_cursor=False)
    max_lanes_reached = turn.no_work is None and state.attempted >= inputs.max_lanes
    if max_lanes_reached or deps._monotonic() >= inputs.budget_deadline:
        if inputs.scan_reservation_enabled and state.attempted == 0 and deps._monotonic() >= inputs.budget_deadline:
            state.admission_blocked = True
        return _stop_pass(
            inputs,
            state,
            turn,
            reason="max_lanes_reached" if max_lanes_reached else "budget_exhausted",
            restore_cursor=False,
        )
    if not turn.path_exists and state.identity_filter_stopped:
        # An incomplete identity scan cannot prove that no eligible
        # worktree remains. Do not spend its last admission on a branch.
        state.admission_blocked = True
        state.has_more = True
        state.remaining_count += len(state.candidates) - turn.index
        record_unvisited(inputs, state, turn.index, "budget_exhausted")
        return BREAK
    state.cursor = turn.key
    return None


def gate_grace(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Keep candidates whose age is unknown or still within grace."""
    deps = inputs.deps
    stamp = deps._parse_timestamp(turn.row.get("updated_at"))
    if turn.no_work is not None:
        return None
    if stamp is None:
        return _refuse_or_keep(inputs, state, turn, "work_complete_age_unknown", "updated_at_unknown")
    age_s = (deps.datetime.now(deps.timezone.utc) - stamp).total_seconds()
    if turn.special is not None:
        work_complete_grace_s, grace_error = deps._work_complete_grace_s()
        if grace_error:
            keep_work_complete_candidate(inputs, state, turn.row, "work_complete_grace_invalid")
            return CONTINUE
        minimum_grace_s = max(inputs.grace_s, float(work_complete_grace_s or 0.0))
    else:
        minimum_grace_s = inputs.grace_s
    if age_s < minimum_grace_s:
        if turn.special is not None:
            _refuse(inputs, state, turn, "keep:work_complete_within_grace")
        else:
            _refuse(inputs, state, turn, "within_grace")
        return CONTINUE
    return None


def gate_retry_backoff(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Honor retry backoff unless the physical candidate state changed."""
    turn.candidate_state = state.candidate_states[turn.key]
    retry_entry = state.retry_entries.get(turn.key)
    if turn.no_work is None and retry_entry is not None and retry_entry.get("retry_after", 0.0) > state.now_epoch:
        if retry_candidate_state_changed(retry_entry, turn.candidate_state):
            # A verified Git/worktree transition means the refusal belongs
            # to an earlier physical state. Replay immediately so a crash
            # after worktree removal can finish deleting the branch.
            state.retry_entries.pop(turn.key, None)
        else:
            return _refuse_or_keep(inputs, state, turn, "retry_deferred", "retry_deferred")
    return None


def gate_worktree_clean(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Require a clean present worktree; a proven torn removal may be restored (apply)."""
    deps = inputs.deps
    remaining_s = inputs.budget_deadline - deps._monotonic()
    if remaining_s <= 0:
        return _stop_pass(inputs, state, turn)
    if not turn.path_exists:
        return None
    if remaining_s < deps._GIT_STATUS_TIMEOUT_S:
        return _stop_pass(inputs, state, turn)
    status_output = deps._git_status(turn.worktree_path, timeout_s=deps._GIT_STATUS_TIMEOUT_S)
    if status_output is None:
        if turn.special is not None:
            keep_work_complete_candidate(inputs, state, turn.row, "work_complete_status_unknown")
        else:
            _refuse(inputs, state, turn, "status_unknown")
        add_error(inputs, state, f"git_status_failed:{turn.row_task}:{turn.lane_id}")
        return CONTINUE
    if not status_output:
        return None
    if not deps._only_deleted_tracked(status_output):
        return _refuse_or_keep(inputs, state, turn, "work_complete_dirty", "dirty")
    return _gate_deleted_only(inputs, state, turn, status_output)


def _gate_deleted_only(inputs: SweepInputs, state: SweepState, turn: CandidateTurn, status_output: str) -> Control:
    deps = inputs.deps
    torn_removal_evidence = False
    if turn.status in inputs.terminal_statuses or turn.special is not None:
        torn_removal_evidence = deps._has_torn_removal_evidence(
            inputs.repo_root,
            turn.row,
            turn.worktree_path,
            work_complete=turn.special is not None,
            timeout_receipts=state.timeout_receipt_keys,
            timeout_s=deps._GIT_STATUS_TIMEOUT_S,
        )
        remaining_s = inputs.budget_deadline - deps._monotonic()
        if inputs.apply and torn_removal_evidence and remaining_s > 0:
            restored_status = _restore_torn_under_fence(inputs, state, turn, status_output, remaining_s)
            if restored_status is None:
                return _refuse_or_keep(inputs, state, turn, "work_complete_status_unknown", "status_unknown")
            status_output = restored_status
    if status_output:
        if deps._only_deleted_tracked(status_output):
            reason = "torn_removal" if torn_removal_evidence else "dirty"
        else:
            reason = "dirty"
        _refuse(inputs, state, turn, reason)
        return CONTINUE
    return None


def _restore_torn_under_fence(
    inputs: SweepInputs, state: SweepState, turn: CandidateTurn, status_output: str | None, remaining_s: float
) -> str | None:
    """Re-prove and restore a torn worktree under the landing fence.

    Returns the fresh status (``None`` when unknown); an unavailable fence
    leaves ``status_output`` unchanged.
    """
    deps = inputs.deps
    try:
        with deps._retire_owner_lock(inputs.repo_root) as owner_acquired:
            if owner_acquired is True:
                fresh_status = deps._git_status(
                    turn.worktree_path,
                    timeout_s=min(deps._GIT_STATUS_TIMEOUT_S, remaining_s),
                )
                if fresh_status is None:
                    status_output = None
                elif deps._only_deleted_tracked(fresh_status) and deps._has_torn_removal_evidence(
                    inputs.repo_root,
                    turn.row,
                    turn.worktree_path,
                    work_complete=turn.special is not None,
                    timeout_receipts=state.timeout_receipt_keys,
                    timeout_s=min(deps._GIT_STATUS_TIMEOUT_S, remaining_s),
                ):
                    deps._restore_torn_worktree(turn.worktree_path, timeout_s=remaining_s)
                    status_output = deps._git_status(
                        turn.worktree_path,
                        timeout_s=min(
                            deps._GIT_STATUS_TIMEOUT_S,
                            max(0.01, inputs.budget_deadline - deps._monotonic()),
                        ),
                    )
                else:
                    status_output = fresh_status
    except Exception:  # noqa: BLE001 — an unavailable mutation fence cannot authorize restore
        pass
    return status_output


def gate_liveness(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """One process snapshot per pass, then process, worker-lock and session liveness."""
    deps = inputs.deps
    if not state.process_probed:
        remaining_s = inputs.budget_deadline - deps._monotonic()
        if remaining_s <= 0:
            return _stop_pass(inputs, state, turn)
        state.process_snapshot = deps._process_snapshot(timeout_s=remaining_s)
        state.process_probed = True
    if deps._monotonic() >= inputs.budget_deadline:
        return _stop_pass(inputs, state, turn)
    if state.process_snapshot is None:
        return _refuse_or_keep(inputs, state, turn, "liveness_unknown", "liveness_unknown")
    if deps._has_live_process(turn.worktree_path, state.process_snapshot):
        return _refuse_or_keep(inputs, state, turn, "live_process", "live_process")
    try:
        driver_live = inputs.lock_is_held(inputs.repo_root / ".task-state" / f"worker-{turn.lane_id}.lock")
    except Exception:
        driver_live = True
    if driver_live:
        return _refuse_or_keep(inputs, state, turn, "live_worker_driver", "live_worker_driver")
    try:
        session_live = inputs.session_live(orchestrator_root=inputs.repo_root, worktree=Path(turn.worktree_path))
    except Exception:
        session_live = True
    if session_live is not False:
        return _refuse_or_keep(inputs, state, turn, "live_session_owner", "live_session_owner")
    if deps._monotonic() >= inputs.budget_deadline:
        return _stop_pass(inputs, state, turn)
    return None


def gate_admission(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Admit before any pre-close collect_snapshot/decide work.

    Probes the landing fence (the actuator reacquires it before mutation),
    counts the admission and, under apply, persists the attempt first.
    """
    deps = inputs.deps
    if turn.no_work is not None:
        return None
    remaining_s = inputs.budget_deadline - deps._monotonic()
    if retire_admission_exhausted(inputs, remaining_s):
        return _stop_pass(inputs, state, turn, blocked=True)
    try:
        with deps._retire_owner_lock(inputs.repo_root) as admission_owner:
            pass
    except Exception as exc:  # noqa: BLE001 — uncertainty cannot admit a mutation
        admission_owner = None
        add_error(inputs, state, f"retire_owner_probe_failed:{turn.row_task}:{turn.lane_id}:{type(exc).__name__}:{exc}")
    if admission_owner is not True:
        _refuse(
            inputs,
            state,
            turn,
            "keep:retire_owner_active" if admission_owner is False else "keep:retire_owner_unverified",
        )
        if admission_owner is False:
            state.has_more = True
            state.remaining_count += 1
        return CONTINUE
    if retire_admission_exhausted(inputs, inputs.budget_deadline - deps._monotonic()):
        return _stop_pass(inputs, state, turn, blocked=True)
    state.attempted += 1
    if inputs.apply and not record_admitted_attempt(
        inputs,
        state,
        turn.key,
        turn.candidate_state,
        candidate_priority(state, (turn.row, turn.worktree_path, turn.path_exists, turn.landing)),
    ):
        state.has_more = True
        state.remaining_count = len(state.candidates) - turn.index
        return BREAK
    state.admitted_keys.add(turn.key)
    return None


def close_review_lane(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Close (apply) or preview a harvested review lane; events in both modes."""
    if turn.row.get("lane_kind") != "review":
        return None
    review_result = inputs.deps.retire_preserved_review_lane(
        repo_root=inputs.repo_root,
        row=turn.row,
        apply=inputs.apply,
        retire=False,
        deadline=inputs.budget_deadline,
        emit_event=functools.partial(emit_review_event, inputs, state),
    )
    if review_result.get("ok") is not True:
        evidence = review_result.get("evidence") or {}
        detail = evidence.get("unknown_pass_outcome")
        if evidence.get("reason") in {"review_pass_index_unavailable", "review_pass_lookup_failed"}:
            # Lookup evidence contains only a status or exception class.
            detail = evidence.get("detail")
        keep_work_complete_candidate(
            inputs, state, turn.row, str(review_result.get("reason") or "review_retire_failed"), detail=detail
        )
        return CONTINUE
    review_reason = str(review_result.get("reason") or "review_harvested")
    if not inputs.apply:
        state.outcome_counts[f"would_close:{review_reason}"] += 1
        return CONTINUE
    if turn.status not in inputs.terminal_statuses:
        state.outcome_counts[f"closed:{review_reason}"] += 1
    turn.row["status"] = review_result["status"]
    turn.status = review_result["status"]
    turn.special = None
    turn.landing = None
    return None


def release_no_work(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Release a no-work lane after re-reading its latest pass at the final guard."""
    deps = inputs.deps
    if turn.no_work is None:
        return None
    try:
        # The filter snapshot cannot authorize release after a new pass
        # starts. Recheck only candidates that reach this final guard.
        latest_pass = deps._latest_lane_pass(inputs.repo_root, turn.row_task, turn.lane_id)
    except Exception:  # noqa: BLE001 — latest-pass uncertainty preserves the lane
        latest_pass = None
    initial_pass = turn.no_work.get("pass_record")
    if not turn.no_work.get("recovery") and (
        not deps._no_work_terminal_pass(latest_pass)
        or not isinstance(initial_pass, dict)
        or not isinstance(latest_pass, dict)
        or latest_pass.get("pass_id") != initial_pass.get("pass_id")
    ):
        _refuse(inputs, state, turn, "no_work_pass_changed")
        return CONTINUE

    from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
        release_no_work_lane_worktree,
    )

    from .branch_reclaim_delete import exception_fence_release, fence_release_of  # noqa: PLC0415

    release_state = ""
    try:
        outcome, detail = release_no_work_lane_worktree(
            row=turn.row,
            pass_record=latest_pass if isinstance(latest_pass, dict) else {},
            repo_root=inputs.repo_root,
            apply=inputs.apply,
        )
    except Exception as exc:  # noqa: BLE001 — release probes fail closed per lane
        outcome, detail = "no_work_release_failed", f"{type(exc).__name__}: {exc}"
        release_state = exception_fence_release(exc)
    release_state = release_state or fence_release_of(detail)
    if release_state in {"not_ours", "release_failed"} and len(state.fence_release) < deps._DETAIL_CAP:
        state.fence_release.append(
            {"step": "no_work_release", "task_ref": turn.row_task, "lane_id": turn.lane_id, "release": release_state}
        )
    state.outcome_counts[outcome] += 1
    if outcome == "released_no_work":
        turn.row["status"] = "planned"
        state.retry_entries.pop(turn.key, None)
    elif outcome == "already_released":
        state.retry_entries.pop(turn.key, None)
    elif outcome not in {"would_release_no_work"}:
        _refuse(inputs, state, turn, outcome, detail=detail)
    return CONTINUE


def _work_complete_pass_guard(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Cached filter evidence cannot authorize a close after a new pass starts."""
    deps = inputs.deps
    try:
        # Read fresh at the final pass-state guard.
        pass_record = deps._latest_lane_pass(inputs.repo_root, turn.row_task, turn.lane_id)
    except Exception:  # noqa: BLE001 — pass-state uncertainty cannot authorize a close
        keep_work_complete_candidate(inputs, state, turn.row, "live_pass")
        return CONTINUE
    if not isinstance(pass_record, dict):
        return None
    result_payload = pass_record.get("result")
    raw_outcome = result_payload.get("outcome") if isinstance(result_payload, dict) else None
    from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
        PASS_OUTCOMES,
    )
    from workbay_orchestrator_mcp.orchestration.pass_terminality import (  # noqa: PLC0415
        pass_outcome_is_terminal,
    )

    if pass_record.get("status") != "done":
        keep_work_complete_candidate(inputs, state, turn.row, "live_pass")
        return CONTINUE
    if isinstance(raw_outcome, str) and raw_outcome in PASS_OUTCOMES and not pass_outcome_is_terminal(raw_outcome):
        keep_work_complete_candidate(inputs, state, turn.row, "live_pass")
        return CONTINUE
    if not isinstance(raw_outcome, str) or raw_outcome not in PASS_OUTCOMES:
        keep_work_complete_candidate(
            inputs, state, turn.row, "unknown_pass_outcome", detail=deps._pass_outcome_detail(raw_outcome)
        )
        return CONTINUE
    return None


def _work_complete_disposition(
    inputs: SweepInputs, state: SweepState, turn: CandidateTurn
) -> tuple[Any, Any, str, str] | Control:
    """Decide the close disposition: ``(disposition, snapshot, target_status, rule)`` or a control."""
    deps = inputs.deps
    special = turn.special
    assert special is not None
    rule_name = str(special["rule"])
    if rule_name == "work_complete_empty":
        facts = special.get("facts")
        if not isinstance(facts, dict):
            keep_work_complete_candidate(inputs, state, turn.row, "superseded_evidence_unavailable")
            return CONTINUE
        snapshot = deps._work_complete_snapshot(
            turn.row,
            tip=str(facts["tip"]),
            integration_ref=str(facts["integration_ref"]),
            integration_tip=str(facts["integration_tip"]),
            work_complete_empty=True,
            evidence=facts,
        )
        from workbay_orchestrator_mcp.orchestration.lane_disposition import decide  # noqa: PLC0415

        return decide(snapshot), snapshot, "closed_stale", rule_name
    # Unique commits must qualify under an existing substantive
    # disposition rule before their row may be terminalized.
    from workbay_orchestrator_mcp.orchestration import lane_disposition  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_terminal_dispose import (  # noqa: PLC0415
        BranchDisposition,
    )

    names = sorted(integration_branch_names(inputs, state, turn.row_task)) or ["main"]
    remaining_s = inputs.budget_deadline - deps._monotonic()
    if remaining_s <= 0:
        return _stop_pass(inputs, state, turn)
    snapshot = None
    try:
        snapshot = lane_disposition.collect_snapshot(
            inputs.repo_root,
            str(turn.row.get("branch") or "").strip().removeprefix("refs/heads/"),
            integration_ref=names[0],
            timeout_s=min(deps._ACTIVE_MERGED_PROBE_TIMEOUT_S, remaining_s),
        )
        snapshot = replace(
            snapshot,
            worktree_paths=(),
            lane_rows=tuple({**lane_row, "status": "closed"} for lane_row in snapshot.lane_rows),
        )
        disposition = lane_disposition.decide(snapshot)
    except Exception:  # noqa: BLE001 — an unknown disposition preserves unique work
        disposition = None
    allowed = {
        BranchDisposition.CONTENT_LANDED,
        BranchDisposition.REVIEW_PERSISTED,
        BranchDisposition.SUPERSEDED_BY_SIBLING,
        BranchDisposition.SUPERSEDED_TOMBSTONE,
    }
    if disposition is None or disposition.kind not in allowed or disposition.branch_action != "delete_D_after_pin":
        keep_work_complete_candidate(inputs, state, turn.row, "superseded_unique_work")
        return CONTINUE
    return disposition, snapshot, "closed", rule_name


def close_work_complete(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Close a work-complete row by CAS on its authorized tip (apply only)."""
    deps = inputs.deps
    if turn.special is None:
        return None
    control = _work_complete_pass_guard(inputs, state, turn)
    if control is not None:
        return control
    decided = _work_complete_disposition(inputs, state, turn)
    if not isinstance(decided, tuple):
        return decided
    disposition, snapshot, target_status, rule_name = decided
    if (
        disposition is None
        or disposition.branch_action != "delete_D_after_pin"
        or (rule_name == "work_complete_empty" and disposition.reason != "superseded_empty")
        or (rule_name == "review_harvested" and disposition.kind.value != "review_persisted")
    ):
        keep_work_complete_candidate(inputs, state, turn.row, "disposition_not_closeable")
        return CONTINUE
    if not inputs.apply:
        state.outcome_counts[f"would_close:{rule_name}"] += 1
        return CONTINUE
    close_started = deps._monotonic()
    close_result = deps._close_lane_cas(
        turn.row,
        target_status=target_status or "closed",
        note=f"retire sweep close: {disposition.reason}",
        deadline=inputs.budget_deadline,
        rule=rule_name,
        disposition_kind=disposition.kind.value,
        authorized_tip=snapshot.tip,
        evidence=disposition.evidence,
        integration_ref=snapshot.integration_ref,
        integration_tip=snapshot.integration_tip,
    )
    turn.candidate_phase_timings["handoff_row_writes_s"] += max(0.0, deps._monotonic() - close_started)
    if close_result.get("ok") is not True:
        close_reason = str(close_result.get("reason") or "close_failed")
        keep_work_complete_candidate(
            inputs,
            state,
            turn.row,
            "lane_row_changed"
            if "revision_changed" in close_reason
            else "deadline_exceeded"
            if close_reason == "deadline_exceeded"
            else "close_failed",
        )
        defer_refused(inputs, state, turn.key, turn.candidate_state)
        return CONTINUE
    state.outcome_counts[f"closed:{rule_name}"] += 1
    turn.row["status"] = target_status
    turn.status = target_status
    return None


def close_absorbed(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Durably close an absorbed row before retirement (apply only)."""
    deps = inputs.deps
    if turn.landing is None:
        turn.landing = state.absorbed.get(turn.key)
    if turn.landing is None:
        return None
    if not inputs.apply:
        state.outcome_counts["would_absorb"] += 1
        return CONTINUE
    try:
        close_started = deps._monotonic()
        closed = deps._close_absorbed_lane(turn.row, turn.landing)
    except Exception as exc:  # noqa: BLE001 — leave the branch intact
        closed = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        turn.candidate_phase_timings["handoff_row_writes_s"] += max(0.0, deps._monotonic() - close_started)
    if not isinstance(closed, dict) or closed.get("ok") is not True:
        _refuse(inputs, state, turn, "absorbed_skip:close_failed")
        defer_refused(inputs, state, turn.key, turn.candidate_state)
        return CONTINUE
    # Durable close precedes retire: a crash is recovered by the terminal sweep.
    turn.row["status"] = "closed"
    return None


def retire_candidate(inputs: SweepInputs, state: SweepState, turn: CandidateTurn) -> Control:
    """Run the retire actuator under the landing fence, then account the result."""
    deps = inputs.deps
    remaining_s = inputs.budget_deadline - deps._monotonic()
    if retire_admission_exhausted(inputs, remaining_s):
        defer_refused(inputs, state, turn.key, turn.candidate_state, reason="budget_exhausted:pre_close")
        return _stop_pass(inputs, state, turn, restore_cursor=False, blocked=True)
    owner_acquired: bool | None = None
    owner_error: Exception | None = None
    retire_started: float | None = None
    result: Any = None
    elapsed = 0.0
    try:
        with deps._retire_owner_lock(inputs.repo_root) as owner_acquired:
            if owner_acquired is True:
                remaining_s = inputs.budget_deadline - deps._monotonic()
                if retire_admission_exhausted(inputs, remaining_s):
                    defer_refused(inputs, state, turn.key, turn.candidate_state, reason="budget_exhausted:pre_close")
                    return _stop_pass(inputs, state, turn, restore_cursor=False, blocked=True)

                retire_started = deps._monotonic()
                result = deps._retire_lane_bounded(
                    lane_id=turn.lane_id,
                    task_ref=turn.row_task,
                    apply=inputs.apply,
                    remaining_s=remaining_s,
                    repo_root=inputs.repo_root,
                )
                elapsed = max(0.0, deps._monotonic() - retire_started)
                state.retire_seconds += elapsed
    except Exception as exc:  # noqa: BLE001 — an unverified owner fence cannot authorize retirement
        if retire_started is not None:
            raise
        owner_error = exc

    if owner_error is not None:
        owner_acquired = None
        add_error(
            inputs,
            state,
            f"retire_owner_probe_failed:{turn.row_task}:{turn.lane_id}:{type(owner_error).__name__}:{owner_error}",
        )

    if owner_acquired is not True:
        keep_reason = "retire_owner_active" if owner_acquired is False else "retire_owner_unverified"
        if turn.special is not None:
            keep_work_complete_candidate(inputs, state, turn.row, keep_reason)
        else:
            _refuse(inputs, state, turn, f"keep:{keep_reason}")
        defer_refused(inputs, state, turn.key, turn.candidate_state)
        if owner_acquired is False:
            state.has_more = True
            state.remaining_count += 1
        return CONTINUE
    record_retire_result(inputs, state, turn, result, elapsed)
    return None


def _budget_truncated_timeout(inputs: SweepInputs, result_payload: dict[str, Any]) -> bool:
    deps = inputs.deps
    result_outcome = result_payload.get("outcome")
    effective_timeout = result_payload.get("effective_timeout_s")
    return (
        result_outcome == "budget_exhausted"
        or (
            result_outcome == "retire_timeout"
            and not isinstance(effective_timeout, bool)
            and isinstance(effective_timeout, (int, float))
            and math.isfinite(float(effective_timeout))
            and float(effective_timeout) < deps._RETIRE_OPERATION_TIMEOUT_S
        )
        or (
            result_outcome == "retire_session_startup_timeout"
            and result_payload.get("startup_budget_capped") is True
            and result_payload.get("startup_deadline_expired") is True
        )
    )


def record_retire_result(
    inputs: SweepInputs, state: SweepState, turn: CandidateTurn, result: Any, elapsed: float
) -> None:
    """Account one actuator result: timing, instrument, release records, outcome and backoff."""
    deps = inputs.deps
    result_payload = result if isinstance(result, dict) else {}
    detail = deps._result_detail(result_payload)
    phase_instrument = result_payload.get("phase_instrument", "missing")
    if not isinstance(phase_instrument, str) or phase_instrument not in {"ok", "missing", "malformed"}:
        phase_instrument = "malformed"
    if phase_instrument != "ok":
        state.phase_instrument_counts[phase_instrument] += 1
    result_outcome = result_payload.get("outcome")
    budget_truncated_timeout = _budget_truncated_timeout(inputs, result_payload)
    retire_phase_timings = deps._bounded_phase_timings(result_payload.get("phase_timings_s"))
    retire_phase_timings["handoff_row_writes_s"] = min(
        deps._RETIRE_PHASE_TIMING_MAX_S,
        retire_phase_timings["handoff_row_writes_s"] + turn.candidate_phase_timings["handoff_row_writes_s"],
    )
    recorded_outcome = "budget_exhausted" if budget_truncated_timeout else str(result_outcome or "retire_malformed")
    if len(state.lane_timings) < deps._DETAIL_CAP:
        timing = {
            "task_ref": turn.row_task,
            "lane_id": turn.lane_id,
            "outcome": recorded_outcome,
            "elapsed_s": elapsed + turn.candidate_phase_timings["handoff_row_writes_s"],
            "phase_timings_s": retire_phase_timings,
            "phase_instrument": phase_instrument,
        }
        if detail:
            timing["detail"] = detail
        state.lane_timings.append(timing)
    _collect_retire_fence_release(inputs, state, turn, result_payload.get("fence_release"))
    if not isinstance(result, dict) or not isinstance(result.get("outcome"), str):
        state.outcome_counts["retire_malformed"] += 1
        _refuse(inputs, state, turn, "retire_malformed", detail=detail)
        add_error(inputs, state, f"retire_result_malformed:{turn.row_task}:{turn.lane_id}")
        defer_refused(inputs, state, turn.key, turn.candidate_state)
        return
    if budget_truncated_timeout:
        state.outcome_counts["budget_exhausted"] += 1
        _refuse(inputs, state, turn, "budget_exhausted", detail=detail)
        timeout_phase = str(retire_phase_timings.get("timed_out_in") or "unknown")
        defer_refused(inputs, state, turn.key, turn.candidate_state, reason=f"budget_exhausted:{timeout_phase}")
        return
    _account_retire_outcome(inputs, state, turn, result, detail, retire_phase_timings)


def _collect_retire_fence_release(inputs: SweepInputs, state: SweepState, turn: CandidateTurn, records: object) -> None:
    """Keep each retire step that left its fence unreleased as a typed record [AGT-10].

    The retire envelope reports a fenced step (``worktree_reclaim``, ``branch_delete``) that
    ran but left its row fenced (``release_failed``) or found its holder replaced
    (``not_ours``) as a ``fence_release`` record, on success and failure alike. Each record is
    kept with a ``retire:`` step and the lane's identity, beside the retire accounting and
    never folded into it. Bounded like ``could_not_act``.
    """
    if not isinstance(records, list):
        return
    for record in records:
        if isinstance(record, dict) and len(state.fence_release) < inputs.deps._DETAIL_CAP:
            state.fence_release.append(
                {
                    "step": f"retire:{record.get('step')}",
                    "task_ref": turn.row_task,
                    "lane_id": turn.lane_id,
                    "release": record.get("release"),
                }
            )


def _account_retire_outcome(
    inputs: SweepInputs,
    state: SweepState,
    turn: CandidateTurn,
    result: dict[str, Any],
    detail: str | None,
    retire_phase_timings: dict[str, Any],
) -> None:
    outcome = str(result["outcome"])
    state.outcome_counts[outcome] += 1
    if inputs.apply and outcome in {"retired", "already_retired"}:
        state.retired_lane_ids.append(turn.lane_id)
        if turn.key in state.absorbed:
            state.absorbed_retired += 1
    failed = result.get("ok") is not True or (
        inputs.apply and turn.landing is not None and outcome not in {"retired", "already_retired"}
    )
    if failed:
        _refuse(inputs, state, turn, outcome, detail=detail)
        timeout_reason = None
        if "timeout" in outcome:
            timeout_reason = f"retire_timeout:{retire_phase_timings.get('timed_out_in') or 'unknown'}"
        defer_refused(inputs, state, turn.key, turn.candidate_state, reason=timeout_reason)
    elif inputs.apply:
        state.retry_entries[turn.key] = {
            "count": 0,
            "attempted_at": state.retry_entries[turn.key]["attempted_at"],
            "candidate_state": turn.candidate_state,
        }


def process_candidate(inputs: SweepInputs, state: SweepState, index: int, item: Candidate) -> Control:
    """Run one candidate through admission and disposition, in fixed order."""
    turn = begin_turn(inputs, state, index, item)
    control = gate_pass_limits(inputs, state, turn)
    if control is not None:
        return control
    control = gate_grace(inputs, state, turn)
    if control is not None:
        return control
    control = gate_retry_backoff(inputs, state, turn)
    if control is not None:
        return control
    control = gate_worktree_clean(inputs, state, turn)
    if control is not None:
        return control
    control = gate_liveness(inputs, state, turn)
    if control is not None:
        return control
    control = gate_admission(inputs, state, turn)
    if control is not None:
        return control
    control = close_review_lane(inputs, state, turn)
    if control is not None:
        return control
    control = release_no_work(inputs, state, turn)
    if control is not None:
        return control
    control = close_work_complete(inputs, state, turn)
    if control is not None:
        return control
    control = close_absorbed(inputs, state, turn)
    if control is not None:
        return control
    return retire_candidate(inputs, state, turn)


# --- Reporting --------------------------------------------------------------


def summarize_retry_deferrals(state: SweepState) -> tuple[int, float | None]:
    """Count candidates still deferred by backoff and the earliest retry time."""
    retry_deferred_count = 0
    next_retry_at: float | None = None
    for retry_key, retry_entry in state.retry_entries.items():
        candidate_state = state.candidate_states.get(retry_key)
        if candidate_state is None or retry_entry.get("retry_after", 0.0) <= state.now_epoch:
            continue
        if retry_candidate_state_changed(retry_entry, candidate_state):
            continue
        retry_deferred_count += 1
        retry_at = retry_entry.get("retry_after")
        if (
            not isinstance(retry_at, bool)
            and isinstance(retry_at, (int, float))
            and math.isfinite(float(retry_at))
            and (next_retry_at is None or float(retry_at) < next_retry_at)
        ):
            next_retry_at = float(retry_at)
    return retry_deferred_count, next_retry_at


def persist_retry_cursor(inputs: SweepInputs, state: SweepState) -> None:
    """Record retire time; under apply, persist retry entries and the cursor."""
    deps = inputs.deps
    state.phase_timings["retire_s"] = state.retire_seconds
    try:
        persisted_cursor_priority = state.cursor_priority
        if state.cursor is not None:
            persisted_cursor_priority = next(
                (candidate_priority(state, item) for item in state.candidates if _lane_key(item[0]) == state.cursor),
                persisted_cursor_priority,
            )
        if inputs.apply:
            state.retry_state_pruned += deps._save_retry_state(
                inputs.repo_root,
                cursor=state.cursor,
                cursor_priority=persisted_cursor_priority,
                entries=state.retry_entries,
                now_epoch=deps.time.time(),
                candidate_states=state.candidate_states,
                retained_keys=state.retained_retry_keys,
                scope_task_ref=inputs.task_ref,
                global_identities=functools.partial(global_retry_identities, inputs, state),
            )
    except OSError as exc:
        add_error(inputs, state, f"retry_state_write_failed:{type(exc).__name__}:{exc}")


def probe_active_but_merged(inputs: SweepInputs, state: SweepState) -> tuple[int, bool]:
    """Count live rows whose work already merged; incomplete probes keep has_more."""
    deps = inputs.deps
    phase_started = deps._monotonic()
    active_but_merged, active_errors, active_complete = deps._count_active_but_merged(
        inputs.repo_root,
        state.rows,
        deadline=inputs.budget_deadline,
        integration_refs_cache=state.integration_refs_cache,
        branch_inventory=state.branches,
    )
    for error in active_errors:
        add_error(inputs, state, error)
    state.phase_timings["active_merged_probe_s"] = max(0.0, deps._monotonic() - phase_started)
    if not active_complete:
        state.has_more = True
        state.remaining_count += sum(
            1 for row in state.rows if str(row.get("status") or "").strip().lower() not in inputs.terminal_statuses
        )
    return active_but_merged, active_complete


def _admission_starved(inputs: SweepInputs, state: SweepState) -> bool:
    made_progress = bool(state.retired_lane_ids) or any(
        count > 0 and (outcome.startswith("closed:") or outcome == "released_no_work")
        for outcome, count in state.outcome_counts.items()
    )
    return (
        state.attempted == 0
        and not made_progress
        and not state.instrument_errors
        and (
            state.identity_filter_stopped
            or state.admission_blocked
            or (inputs.scan_reservation_enabled and state.skip_counts.get("budget_exhausted", 0) > 0)
        )
    )


def _summary_outcome(state: SweepState, retire_admission_starved: bool, retry_deferred_count: int) -> str:
    if state.instrument_errors:
        return "instrument_error"
    if retire_admission_starved:
        return "retire_admission_starved"
    if retry_deferred_count:
        return "deferred"
    return "bounded" if state.has_more else "completed"


def build_summary(
    inputs: SweepInputs,
    state: SweepState,
    *,
    retry_deferred_count: int,
    next_retry_at: float | None,
    active_but_merged: int,
    active_complete: bool,
) -> dict[str, Any]:
    """Assemble the pass summary; every field of the inline body is preserved."""
    deps = inputs.deps
    retire_admission_starved = _admission_starved(inputs, state)
    if retire_admission_starved:
        state.skip_counts["retire_admission_starved"] += 1
    skip_detail_count = sum(
        count for reason, count in state.skip_counts.items() if reason != "missing_worktree_and_branch"
    )
    summary = {
        "ok": not state.instrument_errors and not retire_admission_starved,
        "outcome": _summary_outcome(state, retire_admission_starved, retry_deferred_count),
        "apply": bool(inputs.apply),
        "task_ref": inputs.task_ref,
        "candidates": len(state.candidates),
        "attempted": state.attempted,
        "outcome_counts": dict(sorted(state.outcome_counts.items())),
        "phase_instrument_counts": dict(sorted(state.phase_instrument_counts.items())),
        "skip_counts": dict(sorted(state.skip_counts.items())),
        "retired_lane_ids": state.retired_lane_ids,
        "events": state.events,
        "journaled_events": state.events,
        "tombstones_drained": state.tombstones_drained,
        "tombstones_pending": state.tombstones_pending,
        "could_not_act": state.could_not_act,
        "could_not_act_truncated": skip_detail_count > len(state.could_not_act),
        "missing_worktree_and_branch": state.missing_worktree_and_branch,
        "lane_timings": state.lane_timings,
        "phase_timings_s": {**state.phase_timings, "total_s": max(0.0, deps._monotonic() - inputs.started)},
        "identity_filter_timing": deps._rounded_identity_filter_timing(state.identity_filter_timing),
        "absorbed_retired": state.absorbed_retired,
        "absorbed_skipped": sum(
            (entry["task_ref"], entry["lane_id"]) in state.nonterminal for entry in state.could_not_act
        ),
        "active_but_merged": active_but_merged,
        "active_but_merged_complete": active_complete,
        "cursor_advanced": state.cursor_advanced,
        "retry_deferred": retry_deferred_count,
        "retry_state_pruned": state.retry_state_pruned,
        "next_retry_at": next_retry_at,
        "next_grace_expiry_at": state.next_grace_expiry_at,
        "has_more": state.has_more,
        "remaining": state.remaining_count,
        "instrument_errors": state.instrument_errors[: deps._DETAIL_CAP],
    }
    summary.update(deps._fence_release_fields(state.fence_release))
    if state.scan_cost_unreadable is not None:
        summary["scan_cost_unreadable"] = state.scan_cost_unreadable
    if state.global_identity_read.get("error") is not None:
        # Other tasks' retry history was kept without proof it is live, so the pass says so.
        summary["retry_identity_unknown"] = state.global_identity_read["error"]
    return summary


def report(inputs: SweepInputs, state: SweepState) -> dict[str, Any]:
    """Reporting phase: deferrals, persisted cursor, active-merged probe, summary."""
    retry_deferred_count, next_retry_at = summarize_retry_deferrals(state)
    persist_retry_cursor(inputs, state)
    active_but_merged, active_complete = probe_active_but_merged(inputs, state)
    return build_summary(
        inputs,
        state,
        retry_deferred_count=retry_deferred_count,
        next_retry_at=next_retry_at,
        active_but_merged=active_but_merged,
        active_complete=active_complete,
    )
