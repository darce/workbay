#!/usr/bin/env python3
"""Orchestrator daemon: dispatch open issues, intake merge-ready lanes, refresh dependents.

Usage:
    python3 scripts/mcp/orchestrator_daemon.py \
        --orchestrator-root . --task-ref <task> \
        [--single-pass] [--poll-interval 60] [--dry-run]
"""

from __future__ import annotations

import argparse
import collections.abc
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from dataclasses import field as _dc_field
from importlib import import_module
from itertools import islice
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Graceful shutdown flag (set by SIGTERM handler)
# ---------------------------------------------------------------------------

_shutdown_requested: bool = False

# implementation note S2 — process-local conflict-gate skip counters (increment on refuse,
# reset on successful claim / release). WORKBAY_CONFLICT_MAX_SKIPS resolved here.
_CONFLICT_SKIP_COUNT: dict[str, int] = {}
_CONFLICT_DEFAULT_MAX_SKIPS = 100
_TERMINAL_LANE_STATUSES = frozenset({"merged", "closed", "closed_stale"})


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


def _conflict_max_skips() -> int:
    raw = os.environ.get("WORKBAY_CONFLICT_MAX_SKIPS")
    if raw is None or str(raw).strip() == "":
        return _CONFLICT_DEFAULT_MAX_SKIPS
    try:
        return max(0, int(str(raw).strip()))
    except ValueError:
        return _CONFLICT_DEFAULT_MAX_SKIPS


# ---------------------------------------------------------------------------
# Re-export submodule symbols for backward compatibility (tests load this
# module via importlib and access everything through ``mod.X``).
# ---------------------------------------------------------------------------
from orchestrator_guidance import (  # noqa: F401
    GUIDANCE_STALL_THRESHOLD,
    GuidanceResolution,
    GuidanceResolutionKind,
    _apply_guidance_resolution,
    _classify_guidance,
    _dedupe_worker_guidance_messages,
    _lane_activity,
    _lane_row,
    _latest_lane_report,
    _list_open_dispatch_messages,
    _list_open_worker_guidance,
    _pending_lane_actions,
    _resolve_guidance_cycle,
    _resolve_next_assignment,
)
from orchestrator_helpers import (  # noqa: F401
    _combined_text,
    _json_list_text,
    _log,
    _message_timestamp,
    _normalize_text,
    _report_timestamp,
    _require_dict_payload,
    _require_ok_dict_payload,
)

_handoff_read_shapes = import_module(f"{__package__}.handoff_read_shapes" if __package__ else "handoff_read_shapes")
from orchestrator_lanes import (  # noqa: F401
    REASON_DEPENDENCY_CHECK_FAILED,
    _complete_lane_plan_cursor,
    _count_dependency_refusal,
    _git_is_ancestor,
    _git_stdout,
    _intake_lane,
    _is_full_commit_sha,
    _lane_branch_contained_in,
    _lane_has_capacity,
    _lane_has_unmerged_commits,
    _latest_worker_report_outcome,
    _provision_fresh_worktree,
    _refresh_downstream,
    _resolve_lane_branch,
    _resolve_lane_worktree,
    _run_handoff_dispatch,
    _sort_by_manifest_merge_order,
    _task_branch_landing,
    allow_empty_dependency_graph,
    collect_unsatisfied_dependencies,
    depends_on_ancestors,
    depends_on_scheduling_active,
    lane_dependency_satisfied,
    load_manifest_scheduling_state,
    log_dependency_refusal_summary,
    record_lane_landing,
)

# ---------------------------------------------------------------------------
# Thresholds for stall detection
# ---------------------------------------------------------------------------

PLAN_STALL_THRESHOLD = 3
ATTENTION_STALL_THRESHOLD = 3
# Consecutive daemon cycles of unknown unmerged-state before emitting the
# persistent (vs per-occurrence) typed event [OBS-04 / FW2-WV04C-R2].
UNMERGED_UNKNOWN_CONSECUTIVE_THRESHOLD = 2
# Max report-read failures since the last successful structural read or true
# gap before a lane's streak key is treated as gone for reconcile. The field
# is still named consecutive_read_failures for operator continuity, but with
# unreached-lane preservation (mid-poll abort) the true semantics are
# "failures since last success or true gap", not strictly consecutive
# observed cycles. The counter itself remains monotonic across the
# exhaustion boundary (it is not reset by exhaustion) so operators can
# distinguish three failures from three thousand [OBS-04 / OBS-08 / CLM-04 /
# DATA-01]. Preservation of the *streak key* is a bounded transient
# tolerance, not an indefinite exemption from gap-drop.
UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE = 2


# ---------------------------------------------------------------------------
# Orchestration-level lane queries (stay here so tests can patch siblings)
# ---------------------------------------------------------------------------


def _note_unmerged_commits_unknown_streak(
    lane_id: str,
    *,
    task_ref: str,
    consumer: str,
    log: Any | None,
    streaks: dict[str, int] | None,
    emitted: set[str] | None = None,
) -> None:
    """Bump a lane's consecutive-unknown count; emit persistent event once at threshold.

    Observability only — must never change lane disposition or control flow.
    Callers still emit the per-occurrence ``unmerged_commits_unknown`` event.

    The persistent event is one-shot per streak run: after a successful emit
    for this lane the event will not fire again until the streak key is
    cleared/reconciled away and the spend marker is discarded. Threshold reach
    and log availability are separate: the spend marker is recorded only after
    a successful log call, so if ``log`` is not callable on the threshold cycle
    the event remains pending and can still fire on a later cycle when a
    callable log is available (recoverable).
    """
    if streaks is None:
        return
    count = int(streaks.get(lane_id, 0) or 0) + 1
    streaks[lane_id] = count
    if count < UNMERGED_UNKNOWN_CONSECUTIVE_THRESHOLD:
        return
    if emitted is not None and lane_id in emitted:
        return
    # Threshold decision is independent of log availability (OBS-04).
    if not callable(log):
        return
    log(
        "WARNING",
        "unmerged_commits_unknown_persistent",
        lane_id=lane_id,
        task_ref=task_ref,
        consecutive_unknown_count=count,
        consumer=consumer,
    )
    if emitted is not None:
        emitted.add(lane_id)


def _clear_unmerged_commits_unknown_streak(
    lane_id: str,
    streaks: dict[str, int] | None,
    emitted: set[str] | None = None,
    read_failures: dict[str, int] | None = None,
) -> None:
    """Reset consecutive-unknown count when the predicate returns a known bool."""
    if streaks is not None:
        streaks.pop(lane_id, None)
    if emitted is not None:
        emitted.discard(lane_id)
    if read_failures is not None:
        read_failures.pop(lane_id, None)


def _reconcile_unmerged_unknown_streaks(
    unmerged_unknown_streaks: dict[str, int] | None,
    *,
    observed_this_cycle: set[str],
    read_failed_this_cycle: set[str],
    emitted: set[str] | None = None,
    read_failures: dict[str, int] | None = None,
    read_failure_any_this_cycle: set[str] | None = None,
    unreached_lanes: set[str] | None = None,
) -> None:
    """Drop gap keys; preserve observed keys and briefly-tolerated read failures.

    Read failure is a distinct third state (not observation, not gap): the
    streak key is carried forward without increment for at most
    ``UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE`` failures since the last
    successful structural read or true gap, so a short report-read blip does
    not silently reset consecutive-unknown progress. Once that bound is
    exceeded the streak key is treated as gone and dropped on this path
    [OBS-08 / OBS-04 / CLM-04]. Unreached mid-poll cycles do not count as
    gaps and do not reset the failure counter (CLM-04: "consecutive" in the
    log field means failures-since-success-or-gap, not every daemon cycle).

    The read-failure counter is independent: it stays live across the
    exhaustion boundary (monotonic) while failures continue, and is
    gap-cleaned only when the lane was neither observed nor counted as a
    read-failure this cycle. Counters for never-streaked permanent failures
    share that same lifecycle [DATA-01].

    Lanes listed in ``unreached_lanes`` (any id whose loop body did not reach
    a definitive outcome this cycle — including the aborting lane and later
    ids never entered) are not gap-dropped: they were never given a chance
    to be observed [OBS-08].
    """
    unreached = unreached_lanes if unreached_lanes is not None else set()
    rf_any = (
        read_failure_any_this_cycle
        if read_failure_any_this_cycle is not None
        else read_failed_this_cycle
    )

    # Lanes whose streak key was gap-dropped this reconcile. Their counters
    # are cleared on this path only (not by the orphan loop below) so each
    # pop site is independently load-bearing [CARD-07 / DEFECT TWO vs THREE].
    dropped_streak_lanes: set[str] = set()

    if unmerged_unknown_streaks is not None:
        for stale_lane in list(unmerged_unknown_streaks):
            if stale_lane in unreached:
                continue
            if stale_lane in observed_this_cycle or stale_lane in read_failed_this_cycle:
                continue
            unmerged_unknown_streaks.pop(stale_lane, None)
            dropped_streak_lanes.add(stale_lane)
            if emitted is not None:
                emitted.discard(stale_lane)
            # True gap (not still-failing / exhausted this cycle): clear the
            # third piece of per-lane state alongside streak + spend marker.
            # Exhausted permanent failures stay in rf_any so their monotonic
            # counter is not reset here [DEFECT TWO / FOUR].
            if read_failures is not None and stale_lane not in rf_any:
                read_failures.pop(stale_lane, None)

    # Symmetric lifecycle for counters that never acquired a streak key
    # (permanent pure read-failure lanes) [DEFECT THREE / DATA-01].
    # Skips dropped_streak_lanes so the streak-gap pop above remains the
    # sole cleaner for streaked true gaps [CARD-07 / load-bearing].
    if read_failures is not None:
        for stale_lane in list(read_failures):
            if stale_lane in unreached:
                continue
            if stale_lane in observed_this_cycle or stale_lane in rf_any:
                continue
            if stale_lane in dropped_streak_lanes:
                continue
            read_failures.pop(stale_lane, None)


def _record_merge_ready_read_failure(
    lane_id: str,
    *,
    task_ref: str,
    reason: str,
    log: Any | None,
    read_failed_this_cycle: set[str],
    read_failure_any_this_cycle: set[str],
    read_failures: dict[str, int] | None,
) -> None:
    """Classify a report-read failure; preserve streak only within the bound.

    Provenance of the failure reason is caller-supplied (structural), not
    inferred from exception type [OBS-08]. Streak-key preservation is bounded
    so a permanently failing read cannot retain a streak key forever [OBS-04].
    The consecutive_read_failures counter counts failures since the last
    successful structural read or true gap (unreached mid-poll cycles do not
    reset it). It is monotonic across the exhaustion boundary: exceeding the
    tolerance stops streak preservation but does not reset the counter
    [DATA-01 / CLM-04].
    """
    if read_failures is not None:
        count: int | None = int(read_failures.get(lane_id, 0) or 0) + 1
        read_failures[lane_id] = count
        tracked = True
    else:
        # No cross-cycle tracker: cannot maintain a real consecutive count.
        # Poll entry fails closed when streaks are wired without this tracker,
        # so this branch is only the no-tracking path [OBS-04 / CLM-04].
        count = None
        tracked = False
    # Always mark as counted this cycle so reconcile does not gap-reset the
    # monotonic counter while failures continue (including past tolerance).
    read_failure_any_this_cycle.add(lane_id)
    # Within bound (or untracked single-call path): preserve any existing streak.
    if count is None or count <= UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE:
        read_failed_this_cycle.add(lane_id)
    if callable(log):
        log(
            "WARNING",
            "unmerged_commits_unknown_read_failed",
            lane_id=lane_id,
            task_ref=task_ref,
            reason=reason,
            consumer="merge_ready_poll",
            consecutive_read_failures=count,
            preserved=(
                count is None or count <= UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE
            ),
            untracked=not tracked,
        )
        if count is not None and count > UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE:
            # Operator-visible: streak bound exceeded; counter stays monotonic.
            log(
                "WARNING",
                "unmerged_commits_unknown_read_failure_exhausted",
                lane_id=lane_id,
                task_ref=task_ref,
                consecutive_read_failures=count,
                tolerance=UNMERGED_UNKNOWN_READ_FAILURE_TOLERANCE,
                consumer="merge_ready_poll",
            )


def _poll_merge_ready_lanes(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    *,
    log: Any | None = None,
    unmerged_unknown_streaks: dict[str, int] | None = None,
    unmerged_unknown_persistent_emitted: set[str] | None = None,
    unmerged_unknown_read_failures: dict[str, int] | None = None,
) -> list[str]:
    """Return lane IDs that have a merge-ready worker report and unmerged commits.

    Lanes reporting ``merge_ready`` without unmerged commits are intentionally
    excluded here; ``_complete_already_satisfied_merge_ready_lanes`` advances
    their plan cursor instead of thrashing intake/merge.

    The unmerged-unknown streak dict, its one-shot spend marker set, and the
    read-failure counters (failures since last success or true gap) are owned
    exclusively by this poll: only here may they be incremented, cleared on a
    known bool, or reconciled. The noop/cursor-complete consumer must not
    mutate them.

    Fail closed: when ``unmerged_unknown_streaks`` is provided, both the spend
    marker set and the read-failure counter dict are required. Omitting either
    would silently disable one-shot suppression or the tolerance bound
    [OBS-04 / CLM-04 / CARD-07].
    """
    if unmerged_unknown_streaks is not None:
        if unmerged_unknown_persistent_emitted is None:
            raise ValueError(
                "unmerged_unknown_persistent_emitted is required when "
                "unmerged_unknown_streaks is provided (fail closed: omission "
                "would disable one-shot persistent suppression)"
            )
        if unmerged_unknown_read_failures is None:
            raise ValueError(
                "unmerged_unknown_read_failures is required when "
                "unmerged_unknown_streaks is provided (fail closed: omission "
                "would disable the read-failure tolerance bound)"
            )

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from workbay_orchestrator_mcp.lanes import worker_reports

    ready: list[str] = []
    # Lanes that reached the unmerged tri-state this cycle (merge-ready report
    # and predicate evaluated). Keys not in this set and not in the
    # within-tolerance read-failure set are gaps (skipped / closed / not
    # merge-ready) and are dropped at end-of-poll reconcile, subject to
    # unreached-lane preservation when the loop aborts mid-iteration.
    observed_this_cycle: set[str] = set()
    # Report-read failures within tolerance: preserve existing streak keys
    # without incrementing [OBS-08 / DATA-01 / OBS-04].
    read_failed_this_cycle: set[str] = set()
    # Any read-failure this cycle (including past tolerance): protect the
    # monotonic read-failure counter from gap-reset.
    read_failure_any_this_cycle: set[str] = set()
    # Lanes that reached a definitive outcome this cycle: classified/observed
    # (including not-merge-ready structural success) OR a read failure was
    # recorded. The aborting lane that raised before either is NOT here, so
    # its streak is preserved on the same footing as never-entered later
    # lanes [OBS-08 / F1]. Read-failed lanes must be here so DEFECT FOUR's
    # tolerance bound still applies (they must never be treated as unreached).
    reached_definitive_this_cycle: set[str] = set()
    emitted = unmerged_unknown_persistent_emitted
    read_failures = unmerged_unknown_read_failures
    try:
        for lane_id in lane_ids:
            # Reader call is intentionally outside the payload-shape guard so a
            # programming error inside the reader (e.g. TypeError from a bad
            # keyword) is not relabelled as non_dict_payload [OBS-08 / OBS-04].
            # Catch at per-lane scope to attribute the failure, then re-raise so
            # the daemon still escalates [OBS-04 loud + OBS-08 attributable].
            # Do NOT mark definitive here: a raise leaves the lane unreached so
            # its unknown streak survives for later threshold emission [F1].
            try:
                raw_payload = worker_reports(
                    operation="list",
                    task_ref=task_ref,
                    lane_id=lane_id,
                    limit=1,
                    fields="merge_ready",
                )
            except Exception as exc:
                if callable(log):
                    reason = (
                        "reader_type_error"
                        if isinstance(exc, TypeError)
                        else f"reader_{type(exc).__name__}"
                    )
                    try:
                        log(
                            "ERROR",
                            "merge_ready_poll_reader_failed",
                            lane_id=lane_id,
                            task_ref=task_ref,
                            reason=reason,
                            consumer="merge_ready_poll",
                        )
                    except Exception:  # noqa: BLE001 - observability must not abort cycle
                        pass
                raise
            # Structural / provenance-based classification of the already-returned
            # value only — not exception-type-based wrapping of the call.
            # Leave except TypeError: only — broadening would swallow unrelated
            # programming errors as non_dict_payload [DEFECT FIVE / F2 trap].
            try:
                payload = _require_dict_payload(
                    raw_payload,
                    source=f"worker_reports(list merge-ready:{lane_id})",
                )
            except TypeError:
                # Malformed (non-dict) payload must not abort the whole cycle
                # or leak later-lane keys; treat as read-failure third state.
                # Mark definitive BEFORE the helper: the outcome is decided
                # here. The helper's log sink is not raise-safe (OSError /
                # BrokenPipeError); if it raises after the counter bump but
                # before this mark, the lane lands in unreached and the
                # tolerance bound never fires [DVR4-F1 / DEFECT FOUR / DATA-01].
                reached_definitive_this_cycle.add(lane_id)
                _record_merge_ready_read_failure(
                    lane_id,
                    task_ref=task_ref,
                    reason="non_dict_payload",
                    log=log,
                    read_failed_this_cycle=read_failed_this_cycle,
                    read_failure_any_this_cycle=read_failure_any_this_cycle,
                    read_failures=read_failures,
                )
                continue
            if payload.get("ok") is not True:
                # Same mark-before-helper ordering as non_dict_payload above
                # [DVR4-F1 / DEFECT FOUR / DATA-01].
                reached_definitive_this_cycle.add(lane_id)
                _record_merge_ready_read_failure(
                    lane_id,
                    task_ref=task_ref,
                    reason="report_read_not_ok",
                    log=log,
                    read_failed_this_cycle=read_failed_this_cycle,
                    read_failure_any_this_cycle=read_failure_any_this_cycle,
                    read_failures=read_failures,
                )
                continue
            # Successful structural read — reset read-failure count.
            if read_failures is not None:
                read_failures.pop(lane_id, None)
            reports = payload.get("reports", [])
            if reports and isinstance(reports[0], dict) and reports[0].get("merge_ready"):
                # Only schedule a real merge when commits are known present.
                # Unknown (None) must not enter ready — that would fail-open into
                # a merge (FW2-WV04-N3 / REF-37). Identity vs True is load-bearing
                # for non-bool mutants (CARD-07 / TEST-15).
                # Attribute predicate failures per-lane (consumer distinct from
                # the reader catch) then re-raise; do not mark definitive so
                # the aborting lane's streak is preserved [F1 / F2 / OBS-04].
                try:
                    unmerged = _lane_has_unmerged_commits(
                        orchestrator_root, task_ref, lane_id
                    )
                except Exception as exc:
                    if callable(log):
                        reason = (
                            "predicate_type_error"
                            if isinstance(exc, TypeError)
                            else f"predicate_{type(exc).__name__}"
                        )
                        try:
                            log(
                                "ERROR",
                                "merge_ready_poll_predicate_failed",
                                lane_id=lane_id,
                                task_ref=task_ref,
                                reason=reason,
                                consumer="merge_ready_poll_predicate",
                            )
                        except Exception:  # noqa: BLE001 - observability must not abort cycle
                            pass
                    raise
                observed_this_cycle.add(lane_id)
                if unmerged is True:
                    ready.append(lane_id)
                    _clear_unmerged_commits_unknown_streak(
                        lane_id,
                        unmerged_unknown_streaks,
                        emitted=emitted,
                        read_failures=read_failures,
                    )
                elif unmerged is None:
                    # OBS-04: per-occurrence unknown (transient noise possible).
                    if callable(log):
                        log(
                            "WARNING",
                            "unmerged_commits_unknown",
                            lane_id=lane_id,
                            task_ref=task_ref,
                            reason="predicate_returned_none",
                            consumer="merge_ready_poll",
                        )
                    # OBS-04 / FW2-WV04C-R2: consecutive-cycle unknown is distinct.
                    _note_unmerged_commits_unknown_streak(
                        lane_id,
                        task_ref=task_ref,
                        consumer="merge_ready_poll",
                        log=log,
                        streaks=unmerged_unknown_streaks,
                        emitted=emitted,
                    )
                else:
                    _clear_unmerged_commits_unknown_streak(
                        lane_id,
                        unmerged_unknown_streaks,
                        emitted=emitted,
                        read_failures=read_failures,
                    )
            # Structural processing completed without raise: definitive for
            # gap reconcile (observed tri-state or not-merge-ready gap).
            reached_definitive_this_cycle.add(lane_id)
    finally:
        # Always reconcile, even if a lane raised before per-lane protection
        # or an unexpected error escapes the loop (DATA-01 / no key leak).
        # Lanes that did not reach a definitive outcome (aborting lane and
        # later never-entered ids) keep their streaks so consecutive-unknown
        # progress can still reach threshold [OBS-08 / F1].
        unreached = set(lane_ids) - reached_definitive_this_cycle
        _reconcile_unmerged_unknown_streaks(
            unmerged_unknown_streaks,
            observed_this_cycle=observed_this_cycle,
            read_failed_this_cycle=read_failed_this_cycle,
            emitted=emitted,
            read_failures=read_failures,
            read_failure_any_this_cycle=read_failure_any_this_cycle,
            unreached_lanes=unreached,
        )
    return ready


def _complete_already_satisfied_merge_ready_lanes(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> list[str]:
    """Advance plan cursors for merge-ready lanes that produced no new commits."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from workbay_orchestrator_mcp.lanes import consume_lane_worker_reports, plan_cursor, worker_reports

    completed: list[str] = []
    for lane_id in lane_ids:
        payload = _require_dict_payload(
            worker_reports(
                operation="list",
                task_ref=task_ref,
                lane_id=lane_id,
                limit=1,
                fields="id,merge_ready,created_at,status",
            ),
            source=f"worker_reports(list noop-merge-ready:{lane_id})",
        )
        if payload.get("ok") is not True:
            continue
        reports = payload.get("reports", [])
        if not reports or not isinstance(reports[0], dict) or not reports[0].get("merge_ready"):
            continue
        # True or None: treat as not-noop-ready. Only False (known-zero
        # unmerged) may complete the cursor (FW2-WV04-N3 / REF-37).
        unmerged = _lane_has_unmerged_commits(orchestrator_root, task_ref, lane_id)
        if unmerged is not False:
            if unmerged is None and callable(log):
                # OBS-04: per-occurrence unknown only. Streak dict is owned
                # exclusively by the merge-ready poll so one cycle cannot
                # double-count or clobber both consumers.
                log(
                    "WARNING",
                    "unmerged_commits_unknown",
                    lane_id=lane_id,
                    task_ref=task_ref,
                    reason="predicate_returned_none",
                    consumer="noop_cursor_complete",
                )
            continue
        report = reports[0]
        report_created_at = report.get("created_at")
        cursor_payload = _require_dict_payload(
            plan_cursor(
                operation="list",
                task_ref=task_ref,
                state="dispatched",
                lane_id=lane_id,
                limit=1,
                fields="dispatched_at,state",
            ),
            source=f"plan_cursor(list noop-freshness:{lane_id})",
        )
        if cursor_payload.get("ok") is not True:
            continue
        cursors = cursor_payload.get("cursors", [])
        if not cursors or not isinstance(cursors[0], dict):
            continue
        dispatched_at = cursors[0].get("dispatched_at")
        if not report_created_at or not dispatched_at or report_created_at <= dispatched_at:
            continue
        if dry_run:
            completed.append(lane_id)
            continue
        cursor = _complete_lane_plan_cursor(task_ref, lane_id)
        if cursor is not None:
            report_id = report.get("id")
            consume_lane_worker_reports(
                lane_id,
                report_id=int(report_id) if report_id is not None else None,
                task_ref=task_ref,
            )
            completed.append(lane_id)
            if callable(log):
                log(
                    "INFO",
                    "plan_cursor_noop_completed",
                    lane=lane_id,
                    plan_item_id=cursor.get("plan_item_id"),
                )
    return completed


def _run_cross_lane_verify(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Run ``make lane-check`` from the lane worktree for the intaken lane."""
    if dry_run:
        return True
    lane_worktree = _resolve_lane_worktree(orchestrator_root, task_ref, lane_id)
    if lane_worktree is None or not lane_worktree.is_dir():
        return False
    cmd = [
        "make",
        "lane-check",
        f"TASK={task_ref}",
        f"LANE={lane_id}",
    ]
    result = subprocess.run(
        cmd,
        cwd=lane_worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _has_open_plan_action(task_ref: str, plan_item_id: str) -> bool:
    from workbay_handoff_mcp import list_next_actions
    from workbay_handoff_mcp.enums import ActionStatus  # noqa: PLC0415

    marker = f"[plan:{plan_item_id}]"
    payload = _require_dict_payload(
        list_next_actions(task_ref=task_ref, status=ActionStatus.PENDING, limit=200),
        source=f"list_next_actions({task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list next actions for {task_ref}.")
    for row in payload.get("actions", []):
        if isinstance(row, dict) and marker in str(row.get("action") or ""):
            return True
    return False


def _has_open_plan_message(task_ref: str, plan_item_id: str) -> bool:
    from workbay_handoff_mcp.enums import MessageStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication

    marker = f"[plan:{plan_item_id}]"
    payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            status=MessageStatus.OPEN,
            limit=200,
            fields="subject,message",
        ),
        source=f"lane_communication(list plan messages:{task_ref})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {task_ref}.")
    for row in payload.get("messages", []):
        if not isinstance(row, dict):
            continue
        haystack = f"{row.get('subject') or ''} {row.get('message') or ''}"
        if marker in haystack:
            return True
    return False


def _escalate_plan_item(
    task_ref: str,
    *,
    plan_item_id: str,
    summary: str,
    heading: str,
    dry_run: bool = False,
    log: Any | None = None,
) -> None:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    if dry_run:
        return
    _require_ok_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=plan_item_id,
            state=PlanCursorState.ESCALATED,
            summary=summary,
            source_heading=heading or None,
        ),
        source=f"plan_cursor(upsert escalate:{plan_item_id})",
    )
    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=f"Escalated plan item {plan_item_id} for human review.",
        rationale="Task plan item could not be mapped to a single lane from explicit annotations or manifest routing metadata.",
    )
    if callable(log):
        log("WARN", "task_plan_item_escalated", plan_item_id=plan_item_id, heading=heading)


def _dispatch_plan_item(
    task_ref: str,
    *,
    lane_id: str,
    plan_item_id: str,
    summary: str,
    heading: str,
    resolved_plan: Path,
    owned_paths_override: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    from workbay_handoff_mcp import record_decision, update_next_actions  # noqa: PLC0415
    from workbay_handoff_mcp.api import WriteActorInput  # noqa: PLC0415
    from workbay_handoff_mcp.enums import MessageStatus, PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import lane_communication, plan_cursor  # noqa: PLC0415

    marker = f"[plan:{plan_item_id}]"
    lane_actor = WriteActorInput(lane_id=lane_id)
    result = {
        "plan_item_id": plan_item_id,
        "lane_id": lane_id,
        "summary": summary,
        "heading": heading,
    }
    if dry_run:
        return result

    action_payload = _require_ok_dict_payload(
        update_next_actions(
            operation="add",
            action=f"{marker} {summary}",
            priority=100,
            actor=lane_actor,
        ),
        source=f"update_next_actions(add:{plan_item_id})",
    )
    action = action_payload.get("action", {})
    action_id_raw = action.get("id") if isinstance(action, dict) else None
    action_id = int(action_id_raw) if action_id_raw is not None else None

    _require_ok_dict_payload(
        lane_communication(
            kind="message",
            operation="record",
            lane_id=lane_id,
            session=f"{task_ref}-orchestrator-plan",
            direction="orchestrator_to_worker",
            subject=f"{lane_id} plan assignment",
            message=f"{marker} {summary}",
            status=MessageStatus.OPEN,
            payload={"owned_paths_override": owned_paths_override} if owned_paths_override else None,
        ),
        source=f"lane_communication(record:{plan_item_id})",
    )

    _require_ok_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=plan_item_id,
            state=PlanCursorState.DISPATCHED,
            lane_id=lane_id,
            mcp_action_id=action_id,
            summary=summary,
            source_heading=heading or None,
        ),
        source=f"plan_cursor(upsert dispatch:{plan_item_id})",
    )

    record_decision(
        session=f"{task_ref}-orchestrator-daemon",
        decision=f"Dispatched plan item {plan_item_id} to {lane_id}.",
        rationale=f"Selected the next unchecked task-plan item from {resolved_plan.name} and routed it via manifest-owned lane metadata.",
        actor=lane_actor,
    )
    return result


def _dispatch_from_task_plan(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> dict[str, Any] | None:
    # Per-slice offload dispatch reuses the existing plan_cursor machinery as-is
    # (no schema change): each unchecked plan item maps to one bounded single_pass
    # cycle via DISPATCHED → COMPLETED cursor advancement.
    from lane_manifest import load_manifest, task_plan_path
    from task_plan_parser import map_plan_item_to_lane, normalize_plan_item, parse_task_plan
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    plan_path = task_plan_path(task_ref, orchestrator_root=str(orchestrator_root))
    if not isinstance(plan_path, str) or not plan_path.strip():
        return None
    resolved_plan = Path(plan_path)
    if not resolved_plan.exists():
        raise RuntimeError(f"Task plan path does not exist for {task_ref}: {resolved_plan}")

    manifest = load_manifest(task_ref)
    if not isinstance(manifest, dict):
        return None
    items = parse_task_plan(resolved_plan)
    m_order = manifest.get("merge_order", [])

    def _sort_key(item) -> int:
        n = normalize_plan_item(item)
        lane_id = map_plan_item_to_lane(n, manifest=manifest)
        if lane_id and lane_id in m_order:
            return m_order.index(lane_id)
        return len(m_order)

    items.sort(key=_sort_key)

    for item in items:
        if item.checked:
            continue
        normalized = normalize_plan_item(item)
        cursor_payload = _require_dict_payload(
            plan_cursor(operation="get", task_ref=task_ref, plan_item_id=normalized.plan_item_id),
            source=f"plan_cursor(get:{normalized.plan_item_id})",
        )
        if cursor_payload.get("ok") is not True:
            raise RuntimeError(f"Failed to read plan cursor for {normalized.plan_item_id}.")
        cursor = cursor_payload.get("cursor")
        if isinstance(cursor, dict) and str(cursor.get("state") or "") in {
            PlanCursorState.DISPATCHED,
            PlanCursorState.COMPLETED,
            PlanCursorState.SKIPPED,
            PlanCursorState.ESCALATED,
        }:
            continue

        lane_id = map_plan_item_to_lane(normalized, manifest=manifest)
        if lane_id is None:
            try:
                _escalate_plan_item(
                    task_ref,
                    plan_item_id=normalized.plan_item_id,
                    summary=normalized.summary,
                    heading=normalized.heading,
                    dry_run=dry_run,
                    log=log,
                )
            except RuntimeError as exc:
                # OBS-08: contain refused escalate so later plan items still run.
                if callable(log):
                    log(
                        "ERROR",
                        "plan_item_escalate_refused",
                        plan_item_id=normalized.plan_item_id,
                        task_ref=task_ref,
                        error=str(exc),
                    )
            continue

        if _has_open_plan_action(task_ref, normalized.plan_item_id) or _has_open_plan_message(
            task_ref, normalized.plan_item_id
        ):
            continue
        if not _lane_has_capacity(task_ref, lane_id):
            continue

        try:
            result = _dispatch_plan_item(
                task_ref,
                lane_id=lane_id,
                plan_item_id=normalized.plan_item_id,
                summary=normalized.summary,
                heading=normalized.heading,
                resolved_plan=resolved_plan,
                dry_run=dry_run,
            )
        except RuntimeError as exc:
            # OBS-08: contain refused dispatch so later plan items still run.
            if callable(log):
                log(
                    "ERROR",
                    "plan_item_dispatch_refused",
                    plan_item_id=normalized.plan_item_id,
                    lane_id=lane_id,
                    task_ref=task_ref,
                    error=str(exc),
                )
            continue
        result["line_start"] = normalized.line_start
        return result
    return None


# ---------------------------------------------------------------------------
# salvage_and_close_lane: freeze a failed lane and classify its changed files
# ---------------------------------------------------------------------------


def salvage_and_close_lane(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
) -> dict[str, Any]:
    """Freeze a failed lane, classify its changed files by ownership, and close it.

    Returns a dict with keys:
    - ``lane_id``: the lane that was closed
    - ``this_lane``: files in the lane's own owned_paths
    - ``other_lanes``: dict mapping lane IDs to files belonging to those lanes
    - ``unclassified``: files that don't match any lane's owned_paths
    - ``worktree_preserved``: str path to the preserved worktree
    - ``dry_run``: whether mutation was skipped
    """
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    from lane_exec import _matches_any_owned_path
    from lane_manifest import load_manifest

    worktree = _resolve_lane_worktree(orchestrator_root, task_ref, lane_id)

    # Collect all changed + untracked files in the lane's worktree
    changed: list[str] = []
    if worktree is not None and worktree.is_dir():
        for git_args in (
            ["git", "-C", str(worktree), "diff", "--name-only", "HEAD"],
            ["git", "-C", str(worktree), "ls-files", "--others", "--exclude-standard"],
        ):
            try:
                result = subprocess.run(git_args, capture_output=True, text=True, check=False, timeout=15)
                for line in (result.stdout or "").splitlines():
                    f = line.strip()
                    if f:
                        changed.append(f)
            except (subprocess.TimeoutExpired, OSError):
                pass

    # Load manifest to retrieve owned_paths for every lane
    manifest = load_manifest(task_ref)
    all_lanes: dict[str, Any] = manifest.get("lanes", {}) if isinstance(manifest, dict) else {}

    this_owned: list[str] = []
    if isinstance(all_lanes.get(lane_id), dict):
        this_owned = list(all_lanes[lane_id].get("owned_paths") or [])

    # Classify each changed file
    this_lane_files: list[str] = []
    other_lane_files: dict[str, list[str]] = {}
    unclassified: list[str] = []

    for f in sorted(set(changed)):
        if _matches_any_owned_path(f, this_owned):
            this_lane_files.append(f)
            continue
        matched_lanes = [
            lid
            for lid, cfg in all_lanes.items()
            if lid != lane_id
            and isinstance(cfg, dict)
            and _matches_any_owned_path(f, list(cfg.get("owned_paths") or []))
        ]
        if matched_lanes:
            for m in matched_lanes:
                other_lane_files.setdefault(m, []).append(f)
        else:
            unclassified.append(f)

    salvage: dict[str, Any] = {
        "lane_id": lane_id,
        "this_lane": this_lane_files,
        "other_lanes": other_lane_files,
        "unclassified": unclassified,
        "worktree_preserved": str(worktree) if worktree else None,
        "dry_run": dry_run,
    }

    if not dry_run:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415
        from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        # Close via the close operation (not open/upsert): terminal statuses
        # are refused by open_lane, and _require_dict_payload alone would
        # swallow ok:false then record a false "closed" decision (FW2-WV04-N2).
        _require_ok_dict_payload(
            manage_worktree_lane(
                operation="close",
                lane_id=lane_id,
                status=LaneStatus.CLOSED,
                task_ref=task_ref,
                notes=(
                    f"salvage_and_close: {len(this_lane_files)} owned files preserved; "
                    f"{len(unclassified)} unclassified."
                ),
            ),
            source=f"manage_worktree_lane(close salvage:{lane_id})",
        )
        record_decision(
            session=f"{task_ref}-orchestrator-daemon",
            decision=f"salvage_and_close: lane {lane_id} closed. Worktree preserved at {worktree}.",
            rationale=json.dumps(salvage, indent=2, default=str),
        )

    if callable(log):
        log(
            "INFO",
            "salvage_and_close_complete",
            lane_id=lane_id,
            this_lane_count=len(this_lane_files),
            other_lanes_count=sum(len(v) for v in other_lane_files.values()),
            unclassified_count=len(unclassified),
            dry_run=dry_run,
        )

    return salvage


# ---------------------------------------------------------------------------
# Exclusive orchestrator lock
# ---------------------------------------------------------------------------


class OrchestratorLock:
    """flock-based exclusive lock so only one orchestrator daemon runs at a time."""

    def __init__(self, state_dir: Path) -> None:
        self._lock_path = state_dir / "orchestrator.lock"
        self._fh: Any = None

    def acquire(self) -> bool:
        """Try to acquire the lock.  Returns True on success."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._lock_path.open("w")
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fh.write(json.dumps({"pid": os.getpid()}))
            self._fh.flush()
            return True
        except OSError:
            self._fh.close()
            self._fh = None
            return False

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
        except Exception:
            pass
        self._fh = None
        try:
            self._lock_path.unlink(missing_ok=True)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Pause/resume surface
# ---------------------------------------------------------------------------


def _pause_path(state_dir: Path) -> Path:
    return state_dir / "daemon-paused"


def _is_paused(state_dir: Path) -> bool:
    return _pause_path(state_dir).exists()


def daemon_pause(state_dir: Path) -> None:
    """Create the pause sentinel."""
    import datetime

    state_dir.mkdir(parents=True, exist_ok=True)
    _pause_path(state_dir).write_text(
        json.dumps({"paused_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    )


def daemon_resume(state_dir: Path) -> None:
    """Remove the pause sentinel."""
    p = _pause_path(state_dir)
    if p.exists():
        p.unlink()


# ---------------------------------------------------------------------------
# Status query
# ---------------------------------------------------------------------------


def daemon_status(state_dir: Path, log_dir: Path) -> dict[str, Any]:
    """Return a status dict for the daemon-status target."""
    lock_path = state_dir / "orchestrator.lock"
    lock_info: dict[str, Any] = {"held": False}
    if lock_path.exists():
        try:
            lock_info = {**json.loads(lock_path.read_text()), "held": True}
        except (json.JSONDecodeError, OSError):
            lock_info = {"held": True, "pid": "unknown"}

    paused = _is_paused(state_dir)

    log_path = log_dir / "orchestrator.jsonl"
    last_cycle: dict[str, Any] | None = None
    last_verify: dict[str, Any] | None = None
    if log_path.exists():
        for line in reversed(log_path.read_text().splitlines()):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if last_cycle is None and entry.get("event") == "cycle_end":
                last_cycle = entry
            if last_verify is None and entry.get("event") == "verify_complete":
                last_verify = entry
            if last_cycle and last_verify:
                break

    return {
        "mode": "singleton",
        "state_dir": str(state_dir),
        "log_dir": str(log_dir),
        "lock": lock_info,
        "paused": paused,
        "last_cycle": last_cycle,
        "last_verify": last_verify,
    }


# ---------------------------------------------------------------------------
# Orchestration queries (restored for backward compatibility and logic)
# ---------------------------------------------------------------------------


def _resolve_task_ref(orchestrator_root: Path, task_ref: str | None) -> str:
    """Infer the task reference from active state or manifests if not provided."""
    if task_ref:
        return task_ref
    # 1. Try active task from MCP. read_handoff_state validates the
    # envelope through workbay_protocol.ActiveTask before returning.
    try:
        envelope = _handoff_read_shapes.read_handoff_state(**_handoff_read_shapes.active_task_identity_kwargs())
        state = _require_dict_payload(envelope, source="get_handoff_state(identity)")
        if state.get("ok") and state.get("task_ref"):
            return state["task_ref"]
    except Exception:
        pass
    # 2. Try sole manifest in docs/tasks/
    from lane_manifest import list_manifest_tasks

    tasks = list_manifest_tasks(orchestrator_root=str(orchestrator_root))
    if len(tasks) == 1:
        return tasks[0]
    if not tasks:
        raise RuntimeError("No task manifests found in docs/tasks/.")
    raise RuntimeError(f"Task reference is ambiguous. Available manifests: {', '.join(tasks)}.")


def _lane_work_in_flight(rows: list[dict[str, Any]], *, stale_attention_lanes: set[str] | None = None) -> bool:
    """True if any lane is running or needs attention (and is not stale)."""
    for row in rows:
        if row.get("running"):
            return True
        if row.get("action") == "skip" and row.get("reason") == "attention_required":
            lane_id = _normalize_text(row.get("lane_id"))
            if stale_attention_lanes and lane_id in stale_attention_lanes:
                continue
            return True
    return False


def _remaining_plan_work(task_ref: str) -> list[dict[str, Any]]:
    """Return a list of plan items that are not yet dispatched or completed."""
    # This is primarily for stall detection. In this implementation, we rely on
    # _dispatch_from_task_plan returning None to detect when the plan is empty
    # or stalled. Tests mock this to return non-empty when they want to simulate
    # a stall.
    return []


def _check_lane_health(status: dict[str, Any]) -> tuple[str, str | None]:
    """Compute health from a worker status dict.

    Returns ``(health, recommended_action)`` where *health* is one of
    ``"healthy"``, ``"degraded"``, or ``"unhealthy"`` and *recommended_action*
    is an operator hint string or ``None``.
    """
    attention = bool(status.get("attention_required"))
    worker_state = status.get("worker_state")

    status_record = status.get("status_record") or {}
    streak_info = status_record.get("exhaustion_streak")
    streak = int(streak_info.get("count") or 0) if isinstance(streak_info, dict) else 0

    obs = status.get("observability") or {}
    history = obs.get("history") or []
    scope_violations = sum(1 for e in history if isinstance(e, dict) and e.get("phase") == "scope_check")

    latest_obs = obs.get("latest") or {}
    ctx = status.get("context_utilization_latest") or latest_obs.get("context_utilization") or {}
    pressure = str(ctx.get("pressure") or "normal")

    if worker_state == "unhealthy" or streak >= 2 or attention:
        action: str | None = "promote_model" if streak >= 2 else "close_lane"
        return "unhealthy", action

    if scope_violations > 0:
        return "degraded", "fresh_worktree"

    if pressure == "high":
        return "degraded", "split_lane"

    if pressure == "elevated":
        return "degraded", None

    return "healthy", None


def _ensure_lane_workers(
    orchestrator_root: Path,
    task_ref: str,
    lane_ids: list[str],
    *,
    backend: str = "codex-cli",
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
    dry_run: bool = False,
    log: Any = None,
    prev_health: "dict[str, str] | None" = None,
    state_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Status all lanes and optionally start missing workers via MCP.

    When the manifest's total ``depends_on`` edge set is non-empty
    (or ``WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1``), dispatch is gated by the
    completion predicate over transitive ancestors. With total edges == 0
    and no override, only ``_check_lane_health`` gates starts (legacy).
    Manifest missing/unparseable degrades to legacy (never raises).

    implementation note S2: base-freshness then ``try_claim`` immediately before
    ``manage_worker(start)``; terminal-observation release for held claims.
    """
    import random  # noqa: PLC0415

    from workbay_orchestrator_mcp.api import manage_worker  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration import conflict_gate  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.conflict_gate import (  # noqa: PLC0415
        check_lane_base_freshness,
        load_conflict_graph,
        release_claim,
        try_claim,
    )

    resolved_state_dir = Path(state_dir) if state_dir is not None else orchestrator_root / ".task-state"

    # Scheduling edge source — load once per call (manifest unreadable → legacy).
    # Returns (depends_on, total_edges, scheduling_active) in that order.
    try:
        depends_on, total_edges, scheduling_active = load_manifest_scheduling_state(
            task_ref,
            orchestrator_root=orchestrator_root,
        )
    except Exception:  # noqa: BLE001 — mirror salvage_skipped_no_manifest shape
        depends_on, total_edges, scheduling_active = {}, 0, False
        if log is not None:
            log(
                "WARNING",
                "dependency_gate_skipped_no_manifest",
                task_ref=task_ref,
            )

    # Full manifest for conflict graph + base_sha (best-effort; missing → empty graph).
    full_manifest: dict[str, Any] = {}
    try:
        from lane_manifest import load_manifest  # noqa: PLC0415

        loaded = load_manifest(task_ref, orchestrator_root=str(orchestrator_root))
        if isinstance(loaded, dict):
            full_manifest = loaded
    except Exception:  # noqa: BLE001 — conflict gate degrades to no edges
        full_manifest = {}

    conflict_graph = load_conflict_graph(full_manifest)

    # Fan-out order when the graph has real edges (implementation note S2). Build a NEW
    # local list — never mutate lane_ids / ctx.m_order (re-read after this call).
    if scheduling_active and total_edges > 0:
        from lane_ready_set import dispatch_order  # noqa: PLC0415

        ordered = dispatch_order(list(lane_ids), depends_on if isinstance(depends_on, dict) else {})
    else:
        ordered = list(lane_ids)

    # Hoist one worktree-lane list for per-lane backend fallback (avoid O(n)
    # manage_worktree_lane list calls via _lane_row inside the spawn loop).
    lane_rows_by_id: dict[str, dict[str, Any]] = {}
    try:
        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        listed_raw = manage_worktree_lane(
            operation="list",
            task_ref=task_ref,
            status="all",
            limit=500,
        )
        if isinstance(listed_raw, str):
            listed_raw = json.loads(listed_raw)
        if isinstance(listed_raw, dict) and listed_raw.get("ok") is True:
            for row in listed_raw.get("lanes") or []:
                if isinstance(row, dict) and row.get("lane_id") is not None:
                    lane_rows_by_id[str(row["lane_id"])] = row
    except Exception:  # noqa: BLE001 — backend fallback degrades; never abort spawn
        lane_rows_by_id = {}

    # Per-cycle terminal-observation release (row 28d): held claims whose
    # lane-row status entered {merged, closed, closed_stale} are released.
    # Registry keys are (task_ref, lane_id); release scans by fd value.
    for held_key, held_fd in list(getattr(conflict_gate, "_CLAIM_REGISTRY", {}).items()):
        if isinstance(held_key, tuple) and len(held_key) == 2:
            held_task, held_lane = str(held_key[0]), str(held_key[1])
            if held_task != task_ref:
                continue
        else:
            held_lane = str(held_key)
        row = lane_rows_by_id.get(held_lane) or {}
        status = str(row.get("status") or "").strip().lower()
        if status in _TERMINAL_LANE_STATUSES:
            release_claim(held_lane, held_fd)
            _CONFLICT_SKIP_COUNT.pop(held_lane, None)
            if log is not None:
                log(
                    "INFO",
                    "conflict_claim_released_terminal",
                    lane_id=held_lane,
                    status=status,
                )

    def _active_probe(probe_lane_id: str) -> bool:
        try:
            payload = _require_dict_payload(
                manage_worker(task_ref=task_ref, lane_id=probe_lane_id, action="status"),
                source=f"manage_worker(status:{probe_lane_id})",
            )
        except Exception:  # noqa: BLE001
            return False
        return payload.get("running") is True

    def _neighbour_landings(lane_id: str) -> dict[str, str]:
        landings: dict[str, str] = {}
        neighbours = conflict_graph.get(lane_id, set())
        if not neighbours:
            return landings
        try:
            from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415
        except ImportError:
            return landings
        for neighbour in neighbours:
            try:
                raw = latest_lane_landing(lane_id=neighbour, task_ref=task_ref)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(raw, dict):
                continue
            # Envelope shapes: {ok, data:{landing:{commit_sha}}} or {landing:{commit_sha}}.
            data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
            landing = data.get("landing") if isinstance(data, dict) else None
            if not isinstance(landing, dict):
                continue
            sha = landing.get("commit_sha")
            if isinstance(sha, str) and _is_full_commit_sha(sha):
                landings[neighbour] = sha.strip()
        return landings

    def _record_base_sha(lane_id: str, worktree: Path | None) -> None:
        if worktree is None or dry_run:
            return
        head = _git_stdout(worktree, "rev-parse", "HEAD")
        if not head or not _is_full_commit_sha(head):
            return
        try:
            from lane_manifest import atomic_update_manifest  # noqa: PLC0415
            from lane_manifest import manifest_dir as resolve_manifest_dir

            mpath = resolve_manifest_dir(orchestrator_root=str(orchestrator_root)) / f"{task_ref}.json"
            if not mpath.exists():
                return

            def _mutate(manifest: dict[str, Any]) -> None:
                lanes = manifest.setdefault("lanes", {})
                if not isinstance(lanes, dict):
                    return
                lane = lanes.get(lane_id)
                if isinstance(lane, dict):
                    lane["base_sha"] = head

            atomic_update_manifest(mpath, _mutate)
        except Exception as exc:  # noqa: BLE001 — base_sha pin is best-effort
            if log is not None:
                log("WARNING", "base_sha_record_failed", lane_id=lane_id, error=str(exc))

    rows: list[dict[str, Any]] = []
    for lane_id in ordered:
        status_payload = _require_dict_payload(
            manage_worker(task_ref=task_ref, lane_id=lane_id, action="status"),
            source=f"manage_worker(status:{lane_id})",
        )
        if status_payload.get("ok") is not True:
            continue

        # Merge with lane identity
        status_payload["lane_id"] = lane_id

        if status_payload.get("running"):
            rows.append(status_payload)
            if prev_health is not None:
                prev_health[lane_id] = "healthy"
            continue

        # Gate: skip lanes that are unhealthy
        health, recommended_action = _check_lane_health(status_payload)

        # Emit lane_health_changed when health transitions between cycles.
        if prev_health is not None:
            previous = prev_health.get(lane_id)
            if previous is not None and previous != health and log is not None:
                log(
                    "INFO",
                    "lane_health_changed",
                    lane_id=lane_id,
                    previous=previous,
                    current=health,
                    recommended_action=recommended_action,
                )
            prev_health[lane_id] = health

        if health == "unhealthy":
            status_payload["worker_state"] = "unhealthy"
            status_payload["reason"] = "attention_required"
            if log is not None:
                status_record = status_payload.get("status_record") or {}
                streak_info = status_record.get("exhaustion_streak")
                streak = int(streak_info.get("count") or 0) if isinstance(streak_info, dict) else 0
                log(
                    "WARNING",
                    "lane_unhealthy",
                    lane_id=lane_id,
                    exhaustion_streak=streak,
                    attention_required=bool(status_payload.get("attention_required")),
                    recommended_action=recommended_action,
                )
            if recommended_action == "close_lane":
                try:
                    salvage_and_close_lane(
                        orchestrator_root,
                        task_ref,
                        lane_id,
                        dry_run=dry_run,
                        log=log,
                    )
                except FileNotFoundError:
                    if log is not None:
                        log(
                            "WARNING",
                            "salvage_skipped_no_manifest",
                            lane_id=lane_id,
                            task_ref=task_ref,
                        )
                except RuntimeError as exc:
                    # OBS-08: refused close must not abort the lane loop (DATA-01
                    # still holds: the strict helper raised, no false-closed write).
                    if log is not None:
                        log(
                            "WARNING",
                            "salvage_close_refused",
                            lane_id=lane_id,
                            task_ref=task_ref,
                            error=str(exc),
                        )
            rows.append(status_payload)
            continue

        # Provision a fresh worktree when health is degraded by scope violations
        if recommended_action == "fresh_worktree":
            from lane_manifest import get_lane_config

            lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
            if isinstance(lane_cfg, dict) and lane_cfg.get("redispatch_mode") == "fresh_worktree":
                fresh_path = _provision_fresh_worktree(orchestrator_root, task_ref, lane_id, dry_run=dry_run)
                if fresh_path is not None:
                    # implementation note S2 writer (b): pin base_sha from the provisioned
                    # worktree HEAD so implement lanes outside the freshness
                    # branch also carry the cache (materialize (a) + freshness
                    # reprovision (c) already record via atomic_update_manifest).
                    _record_base_sha(lane_id, fresh_path)
                    if log is not None:
                        log("INFO", "fresh_worktree_provisioned", lane_id=lane_id, worktree_path=str(fresh_path))
                elif log is not None:
                    log("WARNING", "fresh_worktree_provision_failed", lane_id=lane_id)

        # depends_on completion gate (internal). Legacy path: health only.
        # Per-lane try/except: a predicate fault refuses this lane only (fail-closed),
        # never aborts the poll cycle or the remaining lanes (mirror slice-2 shape).
        if scheduling_active:
            try:
                blocked_by, dep_reason = collect_unsatisfied_dependencies(
                    orchestrator_root,
                    task_ref,
                    lane_id,
                    depends_on if isinstance(depends_on, dict) else {},
                    log=log,
                )
            except Exception as exc:  # noqa: BLE001 — fail closed for this lane
                if log is not None:
                    log(
                        "WARNING",
                        "lane_dependency_check_failed",
                        lane_id=lane_id,
                        task_ref=task_ref,
                        error=str(exc),
                    )
                try:
                    _count_dependency_refusal(REASON_DEPENDENCY_CHECK_FAILED)
                except Exception:  # noqa: BLE001 — observability must not abort cycle
                    pass
                status_payload["started"] = False
                status_payload["skipped"] = True
                status_payload["reason"] = REASON_DEPENDENCY_CHECK_FAILED
                status_payload["blocked_by"] = []
                status_payload["worker_state"] = "blocked_upstream"
                rows.append(status_payload)
                continue
            if blocked_by:
                status_payload["started"] = False
                status_payload["skipped"] = True
                status_payload["reason"] = dep_reason or "unresolved_upstream_dependencies"
                status_payload["blocked_by"] = blocked_by
                status_payload["worker_state"] = "blocked_upstream"
                rows.append(status_payload)
                continue

        # Decide if we should start it
        if worker_start_mode == "mcp" and not dry_run:
            # Per-lane backend (implementation note S2): preferred_backend pin, else lane row.
            # Do NOT use the function-level/ctx.backend for twins that carry their own.
            lane_backend: str | None = None
            try:
                from lane_manifest import get_lane_config  # noqa: PLC0415

                pin = get_lane_config(
                    task_ref,
                    lane_id,
                    orchestrator_root=str(orchestrator_root),
                )
                if isinstance(pin, dict):
                    preferred = pin.get("preferred_backend")
                    if isinstance(preferred, str) and preferred.strip():
                        lane_backend = preferred.strip()
            except Exception:  # noqa: BLE001 — fall through to lane-row backend
                pass
            if not lane_backend:
                lane_row = lane_rows_by_id.get(lane_id)
                if lane_row is None:
                    try:
                        lane_row = _lane_row(task_ref, lane_id)
                    except Exception:  # noqa: BLE001 — _lane_row raises on miss
                        lane_row = None
                row_backend = (lane_row or {}).get("backend")
                if isinstance(row_backend, str) and row_backend.strip():
                    lane_backend = row_backend.strip()
            if not lane_backend:
                lane_backend = backend

            neighbours = set(conflict_graph.get(lane_id, set()))

            # Base-freshness BEFORE try_claim (never hold a claim across re-provision).
            # Only meaningful when the lane has conflict neighbours (row 19/19b).
            # Resolve the worktree only when needed: _resolve_lane_worktree raises
            # FileNotFoundError when the on-disk lane manifest is absent, and a
            # crash here would abort the whole daemon cycle (every other lane too).
            # Fail closed to freshness="refuse" for this lane — same degrade shape
            # as worker_daemon on a missing manifest.
            if neighbours:
                try:
                    worktree = _resolve_lane_worktree(orchestrator_root, task_ref, lane_id)
                    freshness = check_lane_base_freshness(
                        lane_id,
                        full_manifest,
                        worktree,
                        _neighbour_landings(lane_id),
                    )
                except Exception as exc:  # noqa: BLE001 — fail closed for this lane
                    if log is not None:
                        log(
                            "WARNING",
                            "base_freshness_check_failed",
                            lane_id=lane_id,
                            error=str(exc),
                        )
                    freshness = "refuse"

                if freshness == "refuse":
                    status_payload["started"] = False
                    status_payload["skipped"] = True
                    status_payload["reason"] = "base_dirty"
                    status_payload["worker_state"] = "blocked_conflict"
                    rows.append(status_payload)
                    continue

                if freshness == "reprovision":
                    fresh_path = _provision_fresh_worktree(orchestrator_root, task_ref, lane_id, dry_run=dry_run)
                    if fresh_path is None:
                        status_payload["started"] = False
                        status_payload["skipped"] = True
                        status_payload["reason"] = "base_stale_reprovision_failed"
                        status_payload["worker_state"] = "blocked_conflict"
                        rows.append(status_payload)
                        continue
                    worktree = fresh_path
                    _record_base_sha(lane_id, worktree)
                    if log is not None:
                        log(
                            "INFO",
                            "base_freshness_reprovisioned",
                            lane_id=lane_id,
                            worktree_path=str(fresh_path),
                        )
                    try:
                        from lane_manifest import load_manifest as _reload  # noqa: PLC0415

                        reloaded = _reload(task_ref, orchestrator_root=str(orchestrator_root))
                        if isinstance(reloaded, dict):
                            full_manifest = reloaded
                    except Exception:  # noqa: BLE001
                        pass

            # Uniform 0–250 ms re-offer jitter after a prior conflict refuse.
            if _CONFLICT_SKIP_COUNT.get(lane_id, 0) > 0:
                time.sleep(random.uniform(0.0, 0.250))

            claim_fd = try_claim(
                task_ref,
                lane_id,
                neighbours,
                _active_probe,
                resolved_state_dir,
            )
            if claim_fd is None:
                skips = _CONFLICT_SKIP_COUNT.get(lane_id, 0) + 1
                _CONFLICT_SKIP_COUNT[lane_id] = skips
                status_payload["started"] = False
                status_payload["skipped"] = True
                status_payload["reason"] = "conflict_active"
                status_payload["worker_state"] = "blocked_conflict"
                status_payload["conflict_skip_count"] = skips
                max_skips = _conflict_max_skips()
                if skips > max_skips:
                    blocker = sorted(neighbours)[:8]
                    if log is not None:
                        log(
                            "WARNING",
                            "conflict_max_skips_exceeded",
                            lane_id=lane_id,
                            skip_count=skips,
                            max_skips=max_skips,
                            blockers=blocker,
                        )
                    try:
                        from workbay_handoff_mcp import record_decision  # noqa: PLC0415

                        record_decision(
                            session=f"{task_ref}-orchestrator-daemon",
                            decision=f"conflict_max_skips:{lane_id}",
                            rationale=(
                                f"lane {lane_id} exceeded WORKBAY_CONFLICT_MAX_SKIPS={max_skips} "
                                f"(skip_count={skips}); blockers={blocker}"
                            ),
                        )
                    except Exception:  # noqa: BLE001 — decision sink must not abort cycle
                        pass
                rows.append(status_payload)
                continue

            # Successful claim resets skip counter (row 18).
            _CONFLICT_SKIP_COUNT.pop(lane_id, None)

            # Bracket manage_worker(start): not-ok OR raise releases the claim
            # before the error propagates (r8 RES-20 every-path audit).
            try:
                start_payload = _require_dict_payload(
                    manage_worker(
                        task_ref=task_ref,
                        lane_id=lane_id,
                        action="start",
                        backend=lane_backend,
                        reasoning_effort=worker_reasoning_effort,
                        model=model,
                    ),
                    source=f"manage_worker(start:{lane_id})",
                )
                if start_payload.get("ok"):
                    status_payload["running"] = True
                    status_payload["worker_state"] = "spawned"
                    status_payload["pid"] = start_payload.get("pid")
                    status_payload["conflict_claim_held"] = True
                else:
                    release_claim(lane_id, claim_fd)
                    _CONFLICT_SKIP_COUNT.pop(lane_id, None)
                    status_payload["started"] = False
                    status_payload["reason"] = "worker_start_failed"
            except Exception:
                release_claim(lane_id, claim_fd)
                _CONFLICT_SKIP_COUNT.pop(lane_id, None)
                raise

        rows.append(status_payload)
    return rows


# ---------------------------------------------------------------------------
# Main orchestrator loop
# ---------------------------------------------------------------------------


@dataclass
class OrchestratorContext:
    """Shared state object threaded through each phase of the orchestrator loop."""

    __module__ = "builtins"
    # Immutable configuration
    orchestrator_root: Path
    task_ref: str
    state_dir: Path
    log_dir: Path
    m_order: list[str]
    poll_interval: int
    single_pass: bool
    dry_run: bool
    backend: str
    worker_start_mode: str
    worker_reasoning_effort: str
    model: "str | None"
    log: Any
    # Mutable counters / per-loop state
    dispatch_failure_count: int = 0
    runtime_failure_count: int = 0
    plan_stall_count: int = 0
    guidance_stalls: "dict[str, tuple[int, int]]" = _dc_field(default_factory=dict)
    attention_stalls: "dict[str, int]" = _dc_field(default_factory=dict)
    # Per-lane consecutive cycles where unmerged-commits predicate returned None
    # (carried across cycles like attention_stalls) [OBS-04 / FW2-WV04C-R2].
    unmerged_unknown_streaks: "dict[str, int]" = _dc_field(default_factory=dict)
    # One-shot spend marker for unmerged_commits_unknown_persistent (lane ids
    # that already delivered the event for the current streak run). Kept as a
    # dedicated set so the streak dict remains a true dict[str, int] [DATA-01].
    unmerged_unknown_persistent_emitted: "set[str]" = _dc_field(default_factory=set)
    # Per-lane report-read failures since last success or true gap (bounded
    # transient tolerance; unreached mid-poll cycles do not reset) [CLM-04].
    unmerged_unknown_read_failures: "dict[str, int]" = _dc_field(default_factory=dict)
    lane_health_prev: "dict[str, str]" = _dc_field(default_factory=dict)
    # Per-cycle outputs written by phases and consumed by the main loop
    guidance_results: list = _dc_field(default_factory=list)
    plan_dispatch: "dict[str, Any] | None" = None
    autostart_results: list = _dc_field(default_factory=list)
    ordered_ready: list = _dc_field(default_factory=list)
    has_in_flight: bool = False
    ready_to_close: bool = False


def _dispatch_phase(ctx: OrchestratorContext) -> None:
    """Step 1: Dispatch open issues to lanes and emit ACE advisory if needed."""
    try:
        dispatch_result = _run_handoff_dispatch(
            ctx.orchestrator_root,
            ctx.task_ref,
            dry_run=ctx.dry_run,
        )
        ctx.dispatch_failure_count = 0
        ctx.log("INFO", "dispatch_complete", result=dispatch_result)

        # ACE advisory was removed — ACE is now project-local (scripts/ace/).
        # The PostToolUse hook (scripts/hooks/ace-detect.py) handles detection;
        # operators run 'make ace-reflect' to apply counter updates.
    except Exception as exc:
        ctx.dispatch_failure_count += 1
        ctx.log("ERROR", "dispatch_failed", error=str(exc))
        if ctx.single_pass or ctx.dispatch_failure_count >= 3:
            raise


def _guidance_phase(ctx: OrchestratorContext) -> None:
    """Step 2: Resolve worker guidance handoffs and update guidance stall counters."""
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    ctx.guidance_results = _resolve_guidance_cycle(
        ctx.orchestrator_root,
        ctx.task_ref,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    for resolution in ctx.guidance_results:
        if resolution.kind == GuidanceResolutionKind.FATAL_ERROR:
            previous = ctx.guidance_stalls.get(resolution.lane_id)
            if previous and previous[0] == resolution.worker_message_id:
                ctx.guidance_stalls[resolution.lane_id] = (resolution.worker_message_id, previous[1] + 1)
            else:
                ctx.guidance_stalls[resolution.lane_id] = (resolution.worker_message_id, 1)
            stall_count = ctx.guidance_stalls[resolution.lane_id][1]
            ctx.log(
                "ERROR",
                "guidance_failed",
                lane=resolution.lane_id,
                error=resolution.error,
                stall_count=stall_count,
            )
            if stall_count >= GUIDANCE_STALL_THRESHOLD:
                ctx.log("ERROR", "terminal_error", lane=resolution.lane_id, reason="guidance_stall")
                raise RuntimeError(f"guidance_stall: lane={resolution.lane_id}")
            if ctx.single_pass:
                # Continue cycle to intake other lanes, but mark for exit
                ctx.dispatch_failure_count = 999
            continue
        ctx.guidance_stalls.pop(resolution.lane_id, None)
        event_name = "guidance_resolved"
        if resolution.kind == GuidanceResolutionKind.REDISPATCH:
            event_name = "guidance_redispatched"
        elif resolution.lane_status == LaneStatus.BLOCKED:
            event_name = "guidance_escalated"
        ctx.log(
            "INFO",
            event_name,
            lane=resolution.lane_id,
            kind=resolution.kind,
            latest_report_id=resolution.latest_report_id,
        )


def _plan_dispatch_phase(ctx: OrchestratorContext) -> None:
    """Step 3: Derive new work from the task plan when backlog is otherwise empty."""
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    ctx.plan_dispatch = _dispatch_from_task_plan(
        ctx.orchestrator_root,
        ctx.task_ref,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    if ctx.plan_dispatch is not None:
        if not ctx.dry_run:
            record_decision(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                decision="Per-slice offload dispatch reuses plan_cursor without schema changes.",
                rationale=(
                    "Each unchecked task-plan item maps to one bounded single_pass cycle; "
                    "plan_cursor state DISPATCHED tracks the active slice and advances to COMPLETED on intake."
                ),
            )
        ctx.log("INFO", "task_plan_dispatch", **ctx.plan_dispatch)


# Telemetry emission caps for lane-reap wide events / WARNs (OBS-06).
# Pathological reaper payloads must not write unbounded JSONL records.
REAP_ERROR_CAP = 512  # chars for error= / id / nested string values (prefix)
REAP_LIST_CAP = 50  # elements for closed_lane_ids / failed / nested sequences
REAP_NEST_DEPTH = 4  # max container nesting for _reap_cap_value (RES-07)
# OBS-08: capped strings must be distinguishable from genuinely short ones.
REAP_TRUNCATION_MARKER = "...[truncated]"
REAP_DEPTH_CAPPED_MARKER = "(depth-capped)"
REAP_CYCLE_MARKER = "(cycle)"
# Intermediate-work margin for the bytes/bytearray pre-slice before repr
# (REVF-DAEMRESID3-K-05). Final emitted length is always
# REAP_ERROR_CAP + len(REAP_TRUNCATION_MARKER); this margin only bounds
# intermediate allocation so a multi-MB blob never materializes full text.
# Not part of the output contract — insurance only.
REAP_BYTES_PRE_BOUND_MARGIN = 32


def _reap_result_telemetry_fields(result: dict) -> dict[str, object]:
    """Count/id payload shared by both lane-reap maintenance arms (OBS-02/06/12).

    Output key contract is frozen: callers depend on these names/types; do not
    rename or add keys here.

    ``closed`` counts every closed-row entry (including id-less dicts).
    ``closed_lane_ids`` is the id-bearing subset sliced to ``REAP_LIST_CAP``;
    ``closed_lane_ids_total`` is the uncapped id-bearing count
    (``len(closed_lane_ids)`` before the slice), not ``len(closed)`` and not
    the length of the capped list after slicing. Id-bearing means: dicts with a
    non-None ``lane_id`` or ``id``, or non-dict non-None items.

    Bounds (OBS-06): ``closed_lane_ids`` length ≤ ``REAP_LIST_CAP``; each id
    string char-capped via ``_reap_cap_str`` (``REAP_ERROR_CAP`` + truncation
    marker); error strings similarly capped; ``max_batch`` coerced to int or
    None. Non-list ``closed`` is treated as empty for both count and id
    extraction (never iterated).

    Work bound (REVB-DAEMRESID-006): extract raw ids first, slice to
    ``REAP_LIST_CAP``, then stringify/char-cap only the slice so work scales
    with the emitted list size, not full ``closed`` length. Total still
    reflects the uncapped id-bearing count.
    """
    closed_raw = result.get("closed") or []
    # Non-list closed must not be iterated (keys-as-ids / char-split / TypeError).
    closed: list = closed_raw if isinstance(closed_raw, list) else []
    would_close = result.get("would_close") or []
    reported = result.get("reported") or []
    ambiguous = result.get("ambiguous") or []
    alive = result.get("alive") or []
    triage = result.get("triage") or []
    failed = result.get("failed") or []
    # Collect raw ids first (no stringify) so total is cheap and capping
    # only runs on the REAP_LIST_CAP slice (REVB-DAEMRESID-006).
    closed_lane_ids_raw: list[object] = []
    for item in closed:
        if isinstance(item, dict):
            lid = item.get("lane_id")
            if lid is None:
                lid = item.get("id")
            if lid is not None:
                closed_lane_ids_raw.append(lid)
        elif item is not None:
            closed_lane_ids_raw.append(item)
    closed_lane_ids: list[str] = [
        _reap_cap_error_text(lid) for lid in closed_lane_ids_raw[:REAP_LIST_CAP]
    ]
    # Degraded marker on the INFO wide event so ok:True+error is not
    # structurally identical to a clean pass when WARNs are filtered (OBS-08/02).
    raw_error = result.get("error")
    error: str | None = _reap_cap_error_text(raw_error) if raw_error else None
    return {
        "applied": bool(result.get("applied")),
        "reported": len(reported) if isinstance(reported, list) else 0,
        "closed": len(closed),
        "would_close": len(would_close) if isinstance(would_close, list) else 0,
        "ambiguous": len(ambiguous) if isinstance(ambiguous, list) else 0,
        "alive": len(alive) if isinstance(alive, list) else 0,
        "triage": len(triage) if isinstance(triage, list) else 0,
        "failed": len(failed) if isinstance(failed, list) else 0,
        "truncated": bool(result.get("truncated")),
        "max_batch": _reap_bound_max_batch(result.get("max_batch")),
        "closed_lane_ids": closed_lane_ids,
        "closed_lane_ids_total": len(closed_lane_ids_raw),
        "degraded": bool(raw_error),
        "error": error,
    }


def _reap_truncation_note(*, arm: str, degraded: bool) -> str:
    """Honest truncation note; suppress "next cycle continues" when degraded (C7)."""
    if arm == "archived_orphan":
        base = "batch cap hit; more archived-orphan lanes may remain"
    else:
        base = "batch cap hit; more non-terminal lanes may remain"
    if degraded:
        return f"{base} — pass degraded, do not assume next cycle clears backlog"
    return f"{base} — next cycle continues"


def _reap_outcome_counts(result: dict) -> dict[str, object]:
    """Cheap count snapshot for reap_telemetry_failed when full field build fails."""
    out: dict[str, object] = {}
    for key in (
        "reported",
        "closed",
        "would_close",
        "ambiguous",
        "alive",
        "triage",
        "failed",
    ):
        val = result.get(key) or []
        out[key] = len(val) if isinstance(val, list) else 0
    return out


def _reap_cap_str(value: object, *, limit: int = REAP_ERROR_CAP) -> object:
    """Cap a single string to *limit* chars with a visible truncation marker (OBS-08).

    Strings at or under *limit* return unchanged. Longer strings return
    ``value[:limit] + REAP_TRUNCATION_MARKER`` so a capped value is
    distinguishable from a genuinely short one. Non-strings pass through.
    """
    if isinstance(value, str):
        if len(value) <= limit:
            return value
        return value[:limit] + REAP_TRUNCATION_MARKER
    return value


def _reap_cap_error_text(value: object, *, limit: int = REAP_ERROR_CAP) -> str:
    """Cap any value to a log-safe error/id string with truncation marker (OBS-06/08)."""
    text = str(value)
    capped = _reap_cap_str(text, limit=limit)
    return capped if isinstance(capped, str) else text[:limit]


# Magnitude bound for max_batch emission (OBS-06 / REVB-DAEMRESID-005).
# Integers with abs > this bound become None rather than embedding huge digits.
REAP_MAX_BATCH_ABS = 10**9


def _reap_bound_max_batch(value: object) -> int | None:
    """Coerce max_batch for emission (OBS-06).

    Rules:
    - ``bool`` is not an int for emission purposes → ``None`` (True/False must
      not surface as 1/0).
    - real ``int`` with ``abs(value) <= REAP_MAX_BATCH_ABS`` → pass through.
    - real ``int`` outside that magnitude bound → ``None`` (never emit a
      multi-hundred-digit integer into INFO/WARN kwargs; REVB-DAEMRESID-005).
    - every other type → ``None``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if abs(value) > REAP_MAX_BATCH_ABS:
            return None
        return value
    return None


def _reap_cap_value(
    value: object,
    *,
    limit: int = REAP_ERROR_CAP,
    depth: int = 0,
    _seen: set[int] | None = None,
) -> object:
    """Cap every string in *value*, including nested dicts/lists/tuples (OBS-06).

    Bounds enforced (copy-on-cap — never mutates *value*):
    - string length → prefix of ``limit`` + ``REAP_TRUNCATION_MARKER`` (OBS-08)
    - dict keys → capped as strings (not preserved verbatim)
    - dict key collisions (distinct source keys → same capped key) → suffix
      ``#2``, ``#3``, ... so both entries remain visible (REVB-DAEMRESID-004);
      after any ``#n`` suffix the key is re-capped so every emitted key length
      is ≤ ``limit + len(REAP_TRUNCATION_MARKER)`` (REVD-DAEMRESID2-004)
    - nested list/tuple/dict cardinality → ``REAP_LIST_CAP`` (+ ``_omitted``)
    - dict overflow accounting: write ``_omitted`` only when that key is absent
      after item processing; if a real payload key already owns ``_omitted``,
      walk ``_omitted#acct``, ``_omitted#acct#2``, ... until free so both real
      values and the count survive (REVB-DAEMRESID-003 / REVD-DAEMRESID2-002)
    - nesting depth → ``REAP_NEST_DEPTH``; beyond → ``REAP_DEPTH_CAPPED_MARKER``
    - cycles → ``REAP_CYCLE_MARKER`` via in-progress ``id()`` set
    - unknown / non-JSON-native types (set/bytes/custom/...) → capped ``repr``
      (REVB-DAEMRESID-002); never pass an unbounded object through to JSONL.
      Intermediate work is pre-sliced for bytes/bytearray/set/sequence/mapping
      fallthroughs (REVD-DAEMRESID2-003) and for the primary dict path
      (``islice(value.items(), REAP_LIST_CAP)``; REVF-DAEMRESID3-C-02).

    Scalars that are not strings (int/float/bool/None) pass through unchanged.
    Depth/cycle guards return typed markers and never raise RecursionError (RES-07).
    """
    if depth >= REAP_NEST_DEPTH:
        return REAP_DEPTH_CAPPED_MARKER
    if isinstance(value, str):
        return _reap_cap_str(value, limit=limit)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if _seen is None:
        _seen = set()
    if isinstance(value, dict):
        oid = id(value)
        if oid in _seen:
            return REAP_CYCLE_MARKER
        _seen.add(oid)
        try:
            # REVF-DAEMRESID3-C-02: never materialize all items before slicing.
            # len(dict) is O(1); islice stops after REAP_LIST_CAP yields.
            omitted = max(0, len(value) - REAP_LIST_CAP)
            out: dict[str, object] = {}
            # Emitted keys must stay within the same budget as capped strings.
            max_key_len = limit + len(REAP_TRUNCATION_MARKER)
            for key, val in islice(value.items(), REAP_LIST_CAP):
                key_str = key if isinstance(key, str) else str(key)
                capped_key = _reap_cap_error_text(key_str, limit=limit)
                # REVB-DAEMRESID-004 / REVD-DAEMRESID2-004: colliding capped
                # keys get a short disambiguator; after any #n suffix re-cap so
                # the key stays ≤ max_key_len (suffix kept visible) and re-walk
                # free slots so re-capping cannot re-introduce a collision.
                if capped_key in out:
                    n = 2
                    while True:
                        suffix = f"#{n}"
                        budget = max_key_len - len(suffix)
                        if budget < 1:
                            base = capped_key[:1] if capped_key else ""
                        else:
                            base = capped_key[:budget]
                        candidate = base + suffix
                        if len(candidate) > max_key_len:
                            # Keep suffix visible; never let re-cap eat it.
                            candidate = candidate[: max_key_len - len(suffix)] + suffix
                        if candidate not in out:
                            capped_key = candidate
                            break
                        n += 1
                out[capped_key] = _reap_cap_value(
                    val, limit=limit, depth=depth + 1, _seen=_seen
                )
            if omitted:
                # REVB-DAEMRESID-003 / REVD-DAEMRESID2-002: never overwrite a
                # real payload key; walk collision-safe accounting keys until free.
                if "_omitted" not in out:
                    out["_omitted"] = omitted
                else:
                    acct_key = "_omitted#acct"
                    n = 2
                    while acct_key in out:
                        acct_key = f"_omitted#acct#{n}"
                        n += 1
                    out[acct_key] = omitted
            return out
        finally:
            _seen.discard(oid)
    if isinstance(value, list):
        oid = id(value)
        if oid in _seen:
            return REAP_CYCLE_MARKER
        _seen.add(oid)
        try:
            capped_list = [
                _reap_cap_value(item, limit=limit, depth=depth + 1, _seen=_seen)
                for item in value[:REAP_LIST_CAP]
            ]
            omitted = max(0, len(value) - REAP_LIST_CAP)
            if omitted:
                capped_list.append({"_omitted": omitted})
            return capped_list
        finally:
            _seen.discard(oid)
    if isinstance(value, tuple):
        oid = id(value)
        if oid in _seen:
            return REAP_CYCLE_MARKER
        _seen.add(oid)
        try:
            capped_items = [
                _reap_cap_value(item, limit=limit, depth=depth + 1, _seen=_seen)
                for item in value[:REAP_LIST_CAP]
            ]
            omitted = max(0, len(value) - REAP_LIST_CAP)
            if omitted:
                # Overflow forces list so _omitted is visible (OBS-08).
                return capped_items + [{"_omitted": omitted}]
            return tuple(capped_items)
        finally:
            _seen.discard(oid)
    # Unknown / non-JSON-native fallthrough (REVB-DAEMRESID-002 / REVD-DAEMRESID2-003).
    # Pre-slice types where intermediate materialization can be bounded; arbitrary
    # object __repr__ cannot be bounded without calling it — keep capped-repr there.
    if isinstance(value, (bytes, bytearray)):
        # Slice raw bytes before repr so a multi-MB blob never allocates full text.
        # REAP_BYTES_PRE_BOUND_MARGIN is intermediate-work insurance only (K-05);
        # emitted length remains limit + marker after _reap_cap_str.
        pre_bound = limit + len(REAP_TRUNCATION_MARKER) + REAP_BYTES_PRE_BOUND_MARGIN
        if len(value) > pre_bound:
            value = bytes(value[:pre_bound]) if isinstance(value, bytearray) else value[:pre_bound]
        return _reap_cap_str(repr(value), limit=limit)
    if isinstance(value, (set, frozenset)):
        # islice elements rather than repr-ing the whole container.
        sample = list(islice(value, REAP_LIST_CAP))
        return _reap_cap_str(repr(type(value)(sample)), limit=limit)
    if isinstance(value, collections.abc.Mapping):
        sample_items = list(islice(value.items(), REAP_LIST_CAP))
        return _reap_cap_str(repr(dict(sample_items)), limit=limit)
    if isinstance(value, collections.abc.Sequence):
        # Non-list/tuple sequences (str/bytes already handled above).
        sample = list(islice(value, REAP_LIST_CAP))
        return _reap_cap_str(repr(sample), limit=limit)
    # Arbitrary object: __repr__ cannot be pre-bounded without calling it.
    return _reap_cap_str(repr(value), limit=limit)


def _reap_failed_element_capped(item: object) -> object:
    """Cap every string field on a failed-row element, nested containers included (OBS-06)."""
    return _reap_cap_value(item)


def _reap_failed_list_capped(result: dict) -> list[object]:
    """Capped failed-row list for rows_failed WARN emission (OBS-06 structural bounds).

    Length is sliced to ``REAP_LIST_CAP``. Every string on each element —
    including strings nested in dicts/lists/tuples — is capped to
    ``REAP_ERROR_CAP`` with a truncation marker (OBS-08). Dict keys, nested
    sequence lengths, and nesting depth are also bounded via ``_reap_cap_value``
    so pathological payloads cannot inflate a single rows_failed JSONL record
    without bound. Unknown containers (set/bytes) become capped repr strings.
    Always copy-on-cap: the reaper result object is never mutated.
    """
    failed = result.get("failed") or []
    if not isinstance(failed, list):
        return []
    return [_reap_failed_element_capped(item) for item in failed[:REAP_LIST_CAP]]


def _reap_blocked_lanes_maintenance(ctx: OrchestratorContext) -> None:
    """Per-cycle conclusive-close pass for non-terminal lanes ([RES-07] Slice-3).

    Arm 1 runs the widened non-terminal sweep (not only aged blocked lanes): any
    non-terminal status whose worktree is gone and branch is merged/deleted can
    close to ``closed_stale``. The AGING report is still read-only on the
    dashboard; this is the write side on the same lane-heartbeat cadence that
    already polls merge-ready lanes.

    ``apply`` is gated on ``dry_run`` (a dry-run daemon reports would-close only
    and writes nothing). The reaper never raises, but each arm is defensively
    wrapped so a maintenance hiccup can never take down a cycle. Telemetry
    emission is separately wrapped (C5): a logging/field-build failure must
    WARN as ``reap_telemetry_failed`` and must never be reported as
    ``*_reap_failed`` after lanes may already have closed (OBS-08).

    Both arms always run: an exception or degraded result in one arm must not
    skip the other (defense-in-depth).

    Event rate (A-03/D3, accepted trade): each arm emits one INFO wide event per
    cycle even on a zero-row result so a broken reaper is distinguishable from a
    clean registry (OBS-08 silence-is-not-success). At poll_interval=60 that is
    2 INFO records/cycle (~120/hour). Rotate or compress daemon JSONL retention
    so emission volume does not force unbounded log growth; do not drop zero-row
    emission to "save" retention.
    """
    # Arm 1 — non-terminal / blocked-lane reaper (widened candidate set).
    try:
        from workbay_orchestrator_mcp.lanes import reap_blocked_lanes  # noqa: PLC0415

        result = reap_blocked_lanes(apply=not ctx.dry_run)
        if isinstance(result, dict):
            # Telemetry emission is separately wrapped: field build / log failure
            # must not surface as blocked_lane_reap_failed after the reaper ran.
            try:
                fields = _reap_result_telemetry_fields(result)
                # Degraded path returns ok:True + error without raising (OBS-08, AGT-10).
                error = result.get("error")
                if error:
                    ctx.log(
                        "WARN",
                        "blocked_lane_reap_degraded",
                        error=_reap_cap_error_text(error),
                        reported=fields["reported"],
                        closed=fields["closed"],
                        would_close=fields["would_close"],
                        ambiguous=fields["ambiguous"],
                        alive=fields["alive"],
                        triage=fields["triage"],
                        failed=fields["failed"],
                    )
                # C6: partial row failures without a top-level error still WARN.
                failed_raw = result.get("failed") or []
                if isinstance(failed_raw, list) and failed_raw:
                    ctx.log(
                        "WARN",
                        "blocked_lane_reap_rows_failed",
                        failed=_reap_failed_list_capped(result),
                        failed_total=len(failed_raw),
                    )
                # Unconditional wide event — zero-row passes still record reported=0.
                # fields includes degraded/error so INFO alone distinguishes clean vs degraded.
                # Event rate: 1 INFO/arm/cycle; see maintenance docstring (A-03/D3).
                ctx.log("INFO", "blocked_lane_reap", **fields)
                # PMH-F12: batch-cap hit leaves a backlog; surface it (parity with arm 2).
                if result.get("truncated"):
                    ctx.log(
                        "WARN",
                        "blocked_lane_reap_truncated",
                        max_batch=_reap_bound_max_batch(result.get("max_batch")),
                        note=_reap_truncation_note(
                            arm="blocked",
                            degraded=bool(error) or bool(fields.get("degraded")),
                        ),
                    )
            except Exception as tel_exc:  # noqa: BLE001 — telemetry must not fail the arm
                # Guard the failure-report log itself: if ctx.log raises here the
                # exception would escape to the arm outer except and be relabeled
                # blocked_lane_reap_failed AFTER the reaper already returned
                # (C5 / RES-07 / OBS-08). Swallow — never re-raise.
                try:
                    ctx.log(
                        "WARN",
                        "reap_telemetry_failed",
                        error=_reap_cap_error_text(tel_exc),
                        arm="blocked_lane",
                        **_reap_outcome_counts(result),
                    )
                except Exception:  # noqa: BLE001 — reporting failure must not promote to reap_failed
                    pass
        else:
            # Non-dict envelope is a broken reaper, not a skipped phase (OBS-08).
            # Guard emission: if ctx.log raises here the exception would escape
            # to the arm outer except and be relabeled blocked_lane_reap_failed
            # (C5 / OBS-08) — same threat model as the telemetry-failure guard.
            try:
                ctx.log(
                    "WARN",
                    "blocked_lane_reap_bad_shape",
                    result_type=type(result).__name__,
                )
            except Exception:  # noqa: BLE001 — log failure must not promote to reap_failed
                pass
    except Exception as exc:  # noqa: BLE001 — maintenance must never break the cycle
        # Guard the failure log itself: if ctx.log raises, the exception must
        # not escape _reap_blocked_lanes_maintenance and skip arm 2
        # ("Both arms always run").
        try:
            ctx.log(
                "WARN",
                "blocked_lane_reap_failed",
                error=_reap_cap_error_text(exc),
            )
        except Exception:  # noqa: BLE001 — outer log failure must not skip arm 2
            pass
        # Do not return: arm 2 must still run.

    # 0112 Bug 2: task-archival-anchored backstop. The two arms overlap by design
    # (reap_blocked_lanes now sweeps every non-terminal status); this arm remains
    # for a different candidate rule — task archived and absent from live handoff
    # state. Daemon-less flows self-heal at task-finish; this is the daemon
    # backstop for the rest. Same dry-run gate; defensively wrapped so it can
    # never take down a cycle. Telemetry separately wrapped (C5) as in arm 1.
    try:
        from workbay_orchestrator_mcp.lanes import reap_task_archived_orphan_lanes  # noqa: PLC0415

        orphan_result = reap_task_archived_orphan_lanes(apply=not ctx.dry_run)
        if isinstance(orphan_result, dict):
            try:
                o_fields = _reap_result_telemetry_fields(orphan_result)
                o_error = orphan_result.get("error")
                if o_error:
                    ctx.log(
                        "WARN",
                        "archived_orphan_lane_reap_degraded",
                        error=_reap_cap_error_text(o_error),
                        reported=o_fields["reported"],
                        closed=o_fields["closed"],
                        would_close=o_fields["would_close"],
                        ambiguous=o_fields["ambiguous"],
                        alive=o_fields["alive"],
                        triage=o_fields["triage"],
                        failed=o_fields["failed"],
                    )
                # C6: partial row failures without a top-level error still WARN.
                o_failed_raw = orphan_result.get("failed") or []
                if isinstance(o_failed_raw, list) and o_failed_raw:
                    ctx.log(
                        "WARN",
                        "archived_orphan_lane_reap_rows_failed",
                        failed=_reap_failed_list_capped(orphan_result),
                        failed_total=len(o_failed_raw),
                    )
                # Event rate: 1 INFO/arm/cycle; see maintenance docstring (A-03/D3).
                ctx.log("INFO", "archived_orphan_lane_reap", **o_fields)
                # PMH-F12: a truncated sweep (batch cap hit) leaves a backlog; surface it
                # so a partial reap is not mistaken for a clean one.
                if orphan_result.get("truncated"):
                    ctx.log(
                        "WARN",
                        "archived_orphan_lane_reap_truncated",
                        max_batch=_reap_bound_max_batch(orphan_result.get("max_batch")),
                        note=_reap_truncation_note(
                            arm="archived_orphan",
                            degraded=bool(o_error) or bool(o_fields.get("degraded")),
                        ),
                    )
            except Exception as tel_exc:  # noqa: BLE001 — telemetry must not fail the arm
                # Guard the failure-report log itself (symmetric with arm 1): a
                # raise here must never be relabeled archived_orphan_lane_reap_failed
                # after the reaper already returned (C5 / RES-07 / OBS-08).
                try:
                    ctx.log(
                        "WARN",
                        "reap_telemetry_failed",
                        error=_reap_cap_error_text(tel_exc),
                        arm="archived_orphan",
                        **_reap_outcome_counts(orphan_result),
                    )
                except Exception:  # noqa: BLE001 — reporting failure must not promote to reap_failed
                    pass
        else:
            # Non-dict envelope is a broken reaper, not a skipped phase (OBS-08).
            # Guard emission (symmetric with arm 1): a ctx.log raise must never
            # be relabeled archived_orphan_lane_reap_failed (C5 / OBS-08).
            try:
                ctx.log(
                    "WARN",
                    "archived_orphan_lane_reap_bad_shape",
                    result_type=type(orphan_result).__name__,
                )
            except Exception:  # noqa: BLE001 — log failure must not promote to reap_failed
                pass
    except Exception as exc:  # noqa: BLE001 — maintenance must never break the cycle
        # Guard the failure log itself (symmetric with arm 1): never re-raise
        # out of the outer handler so a log boom cannot break the cycle.
        try:
            ctx.log(
                "WARN",
                "archived_orphan_lane_reap_failed",
                error=_reap_cap_error_text(exc),
            )
        except Exception:  # noqa: BLE001 — outer log failure must not escape maintenance
            pass


def _recover_stranded_landing_lanes(ctx: OrchestratorContext) -> list[str]:
    """Recover non-MERGED lanes stranded after a real merge ([DOM-06]).

    Two sub-arms (both unreachable via re-intake once the lane tip is in the
    task tip — ``_poll_merge_ready_lanes`` requires unmerged commits):

    1. **Valid landing record present, status not MERGED** — crash between
       ``record_lane_landing`` and the MERGED status write. Re-close MERGED
       when the landing SHA is an ancestor of the task-branch tip **and**
       fully contains the lane branch. A present-but-invalid landing row
       (missing/non-full ``commit_sha``) is treated as absent and falls
       through to sub-arm 2.
    2. **No valid landing record, status not MERGED** — intake withheld
       MERGED after a ledger write failure. Heal only when an **unconsumed**
       ``merge_ready`` worker report remains (consume-after-record leaves it
       submitted on record/close failure) **and** the lane branch is
       contained in the task tip; then re-attempt ``record_lane_landing``
       (fresh tip capture), MERGED, consume the report (mirroring intake),
       and refresh. Vacuous success lanes (``no_work`` / never merge-ready /
       already-consumed report) must not be terminalized.

    Reopened lanes (old landing still ancestral, but new commits on the lane
    branch) are skipped so recovery cannot re-terminalize work in progress.
    Close envelope failures and per-lane exceptions never count as progress and
    never abort the recovery loop for remaining lanes.
    """
    if ctx.dry_run:
        return []

    try:
        from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415
    except ImportError:
        # Reader ships concurrently in the handoff package (internal).
        # Until it merges, recovery is a no-op rather than crashing the daemon.
        return []

    from lane_manifest import downstream_lanes  # noqa: PLC0415
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
        consume_lane_worker_reports,
        manage_worktree_lane,
        worker_reports,
    )

    tip = _git_stdout(ctx.orchestrator_root, "rev-parse", "HEAD")
    if not tip or not _is_full_commit_sha(tip):
        return []

    try:
        listed_raw = manage_worktree_lane(
            operation="list",
            task_ref=ctx.task_ref,
            status="all",
            limit=500,
        )
        if isinstance(listed_raw, str):
            listed_raw = json.loads(listed_raw)
        listed = listed_raw if isinstance(listed_raw, dict) else {}
    except Exception as exc:  # noqa: BLE001 — recovery must never kill the cycle
        ctx.log("ERROR", "landing_recovery_list_failed", error=str(exc))
        return []
    if listed.get("ok") is not True:
        return []

    status_by_lane: dict[str, str] = {}
    branch_by_lane: dict[str, str] = {}
    for row in listed.get("lanes") or []:
        if isinstance(row, dict) and row.get("lane_id") is not None:
            lid = str(row["lane_id"])
            status_by_lane[lid] = str(row.get("status") or "").strip().lower()
            branch_raw = row.get("branch")
            if isinstance(branch_raw, str) and branch_raw.strip():
                branch_by_lane[lid] = branch_raw.strip()

    merged_token = str(getattr(LaneStatus, "MERGED", "merged")).strip().lower()
    if merged_token.startswith("lanestatus."):
        merged_token = "merged"

    def _close_merged(lane_id: str, *, notes: str, sha: str) -> bool:
        close_raw = manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            status=LaneStatus.MERGED,
            notes=notes,
            task_ref=ctx.task_ref,
        )
        if isinstance(close_raw, str):
            close_raw = json.loads(close_raw)
        close_payload = close_raw if isinstance(close_raw, dict) else {}
        # H4: only count actual successful transitions as recovery progress.
        if close_payload.get("ok") is not True:
            ctx.log(
                "ERROR",
                "landing_recovery_close_failed",
                lane=lane_id,
                sha=sha,
                payload=close_payload,
            )
            return False
        return True

    def _refresh_after_recovery(lane_id: str) -> None:
        deps = downstream_lanes(ctx.task_ref, lane_id)
        if deps:
            ctx.log("INFO", "refresh_start", lane=lane_id, downstream=deps, reason="landing_recovery")
            refresh_results = _refresh_downstream(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                deps,
                dry_run=ctx.dry_run,
            )
            ctx.log("INFO", "refresh_complete", lane=lane_id, results=refresh_results)

    def _unconsumed_merge_ready_report(lane_id: str) -> dict[str, Any] | None:
        """Latest unconsumed merge-ready report, or None (fail-closed)."""
        try:
            payload = worker_reports(
                operation="list",
                task_ref=ctx.task_ref,
                lane_id=lane_id,
                limit=1,
                fields="id,merge_ready,status,outcome",
            )
        except Exception:  # noqa: BLE001 — heal eligibility fails closed
            return None
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, json.JSONDecodeError):
                return None
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            return None
        reports = payload.get("reports")
        if not isinstance(reports, list) or not reports:
            return None
        report = reports[0]
        if not isinstance(report, dict) or not report.get("merge_ready"):
            return None
        # Consume ACKs (status out of submitted); rows survive with merge_ready set.
        if str(report.get("status") or "").strip().lower() != "submitted":
            return None
        return report

    recovered: list[str] = []
    for lane_id in ctx.m_order:
        try:
            status = status_by_lane.get(lane_id, "")
            if status == merged_token or status == "merged":
                continue

            try:
                raw = latest_lane_landing(lane_id=lane_id, task_ref=ctx.task_ref)
                if isinstance(raw, str):
                    raw = json.loads(raw)
                env = raw if isinstance(raw, dict) else {}
            except Exception as exc:  # noqa: BLE001 — isolation / mock safety
                ctx.log("ERROR", "landing_recovery_read_failed", lane=lane_id, error=str(exc))
                continue

            data = env.get("data") if isinstance(env.get("data"), dict) else env
            if not isinstance(data, dict):
                continue
            landing = data.get("landing")

            # --- Sub-arm 1: valid record present, status not MERGED ---
            if isinstance(landing, dict):
                sha = str(landing.get("commit_sha") or "").strip()
                if sha and _is_full_commit_sha(sha):
                    if not _git_is_ancestor(ctx.orchestrator_root, sha, tip):
                        ctx.log(
                            "INFO",
                            "landing_recovery_sha_not_ancestor",
                            lane=lane_id,
                            sha=sha,
                            tip=tip,
                        )
                        continue

                    # H5: skip reopened lanes — old landing may still be ancestral to tip,
                    # but new commits on the lane branch mean work is in progress again.
                    lane_branch = _resolve_lane_branch(
                        ctx.orchestrator_root,
                        ctx.task_ref,
                        lane_id,
                        branch_hint=branch_by_lane.get(lane_id),
                    )
                    if not lane_branch:
                        ctx.log(
                            "INFO",
                            "landing_recovery_branch_unresolved",
                            lane=lane_id,
                            sha=sha,
                        )
                        continue
                    contained = _lane_branch_contained_in(ctx.orchestrator_root, sha, lane_branch)
                    if contained is not True:
                        ctx.log(
                            "INFO",
                            "landing_recovery_lane_not_contained",
                            lane=lane_id,
                            sha=sha,
                            branch=lane_branch,
                            contained=contained,
                        )
                        continue

                    if not _close_merged(
                        lane_id,
                        notes="Recovered by orchestrator daemon: landing record present, status not MERGED.",
                        sha=sha,
                    ):
                        continue

                    ctx.log("INFO", "lane_landing_recovered", lane=lane_id, sha=sha)
                    _refresh_after_recovery(lane_id)
                    recovered.append(lane_id)
                    continue

                # Present-but-invalid landing (missing/non-full SHA): treat as
                # absent so the no-record heal path can re-record. continue would
                # permanently strand the lane once tip has no unmerged commits.
                ctx.log(
                    "WARN",
                    "landing_recovery_invalid_landing",
                    lane=lane_id,
                    commit_sha=sha or None,
                )

            # --- Sub-arm 2: no valid landing record, status not MERGED (H3) ---
            # Re-entry via poll requires unmerged commits; post-merge those are
            # gone. Unconsumed merge_ready is the discriminator for a real
            # landed-but-unrecorded intake (consume-after-record leaves it).
            ready_report = _unconsumed_merge_ready_report(lane_id)
            if ready_report is None:
                continue

            lane_branch = _resolve_lane_branch(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                branch_hint=branch_by_lane.get(lane_id),
            )
            if not lane_branch:
                ctx.log(
                    "INFO",
                    "landing_recovery_branch_unresolved",
                    lane=lane_id,
                    sha=tip,
                )
                continue
            contained = _lane_branch_contained_in(ctx.orchestrator_root, tip, lane_branch)
            if contained is not True:
                ctx.log(
                    "INFO",
                    "landing_recovery_lane_not_contained",
                    lane=lane_id,
                    sha=tip,
                    branch=lane_branch,
                    contained=contained,
                )
                continue

            landed_sha, task_branch = _task_branch_landing(ctx.orchestrator_root)
            if landed_sha is None or not _is_full_commit_sha(landed_sha):
                ctx.log(
                    "ERROR",
                    "landing_recovery_sha_unresolved",
                    lane=lane_id,
                )
                continue

            if not record_lane_landing(
                ctx.task_ref,
                lane_id,
                landed_sha,
                task_branch or "",
                log=ctx.log,
            ):
                ctx.log(
                    "ERROR",
                    "landing_recovery_record_failed",
                    lane=lane_id,
                    sha=landed_sha,
                )
                continue

            if not _close_merged(
                lane_id,
                notes="Recovered by orchestrator daemon: re-recorded landing after ledger write failure.",
                sha=landed_sha,
            ):
                continue

            # Close-cycle ack only after record + MERGED succeeded (mirror intake).
            report_id = ready_report.get("id")
            consume_lane_worker_reports(
                lane_id,
                report_id=int(report_id) if report_id is not None else None,
                task_ref=ctx.task_ref,
            )

            ctx.log(
                "INFO",
                "lane_landing_recovered",
                lane=lane_id,
                sha=landed_sha,
                reason="no_record_unconsumed_merge_ready",
            )
            _refresh_after_recovery(lane_id)
            recovered.append(lane_id)
        except Exception as exc:  # noqa: BLE001 — H6: one lane must not abort the cycle
            ctx.log("ERROR", "landing_recovery_lane_failed", lane=lane_id, error=str(exc))
            continue
    return recovered


def _resolve_verify_twin_blockers(ctx: OrchestratorContext) -> None:
    """Resolve admit-time blockers for finished verify-twin lanes (implementation note S2).

    Observer only: never aborts the cycle. Strict outcome gate — only
    ``finished`` resolves (not the broader ``_SUCCESS_WORKER_REPORT_OUTCOMES``
    set used by the propagation predicate). Reads open blockers via the
    unlimited ``handoff_close_check`` items list (not the capped
    ``get_handoff_state.blockers_open``).
    """
    if ctx.dry_run:
        return

    twin_ids: list[str] = [lid for lid in (ctx.m_order or []) if isinstance(lid, str) and "__verify__" in lid]
    if not twin_ids:
        try:
            from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

            listed_raw = manage_worktree_lane(
                operation="list",
                task_ref=ctx.task_ref,
                status="all",
                limit=500,
            )
            if isinstance(listed_raw, str):
                listed_raw = json.loads(listed_raw)
            listed = listed_raw if isinstance(listed_raw, dict) else {}
            if listed.get("ok") is True:
                for row in listed.get("lanes") or []:
                    if not isinstance(row, dict):
                        continue
                    lid = row.get("lane_id")
                    if isinstance(lid, str) and "__verify__" in lid:
                        twin_ids.append(lid)
        except Exception as exc:  # noqa: BLE001 — fail-closed; never abort the cycle
            ctx.log("ERROR", "verify_twin_list_failed", error=str(exc))
            return

    if not twin_ids:
        return

    from workbay_handoff_mcp import handoff_close_check, report_blocker  # noqa: PLC0415

    for twin_id in twin_ids:
        try:
            outcome = _latest_worker_report_outcome(ctx.task_ref, twin_id)
            # STRICT: only "finished" (plan lines 282-285). no_work / no_actionable_work
            # must NOT resolve the task-level admit-time blocker.
            if outcome != "finished":
                continue

            close_raw = handoff_close_check(task_ref=ctx.task_ref)
            if isinstance(close_raw, str):
                close_raw = json.loads(close_raw)
            if not isinstance(close_raw, dict):
                continue
            data = close_raw.get("data") if isinstance(close_raw.get("data"), dict) else close_raw
            checks = data.get("checks") if isinstance(data, dict) else None
            open_blockers = checks.get("open_blockers") if isinstance(checks, dict) else None
            items = open_blockers.get("items") if isinstance(open_blockers, dict) else None
            if not isinstance(items, list):
                continue

            # Map open blocker lane_ids → numeric ids. Status set and not
            # ``open`` is still skipped (same filter as the single-row path).
            open_by_lane: dict[str, int] = {}
            for row in items:
                if not isinstance(row, dict):
                    continue
                row_lane = row.get("lane_id")
                if not isinstance(row_lane, str) or not row_lane:
                    continue
                status = str(row.get("status") or "").strip().lower()
                if status and status != "open":
                    continue
                raw_id = row.get("id")
                if raw_id is None:
                    continue
                try:
                    open_by_lane[row_lane] = int(raw_id)
                except (TypeError, ValueError):
                    continue

            # Lazy import keeps work_graph_compiler out of the daemon module
            # graph (no import cycle). Pure helper — binding from id shape.
            from workbay_orchestrator_mcp.orchestration.work_graph_compiler import (  # noqa: PLC0415
                holds_resolved_by_twin,
            )

            # Resolve every hold this finished twin is bound to, not just the
            # first row keyed on the twin itself (WIDTH-114 shared gate).
            for hold_key in holds_resolved_by_twin(twin_id, open_by_lane.keys()):
                blocker_id = open_by_lane.get(hold_key)
                if blocker_id is None:
                    continue

                resolve_raw = report_blocker(
                    operation="resolve",
                    blocker_id=blocker_id,
                    task_ref=ctx.task_ref,
                    actor={"lane_id": twin_id},
                )
                if isinstance(resolve_raw, str):
                    resolve_raw = json.loads(resolve_raw)
                resolve_result = resolve_raw if isinstance(resolve_raw, dict) else {}
                # report_blocker can return {ok:False} without raising (id race,
                # missing blocker). Log success only when the envelope is ok so a
                # failed resolve is not hidden ([AGT-10] degrade loudly).
                if resolve_result.get("ok") is True:
                    ctx.log(
                        "INFO",
                        "verify_twin_blocker_resolved",
                        lane_id=twin_id,
                        blocker_id=blocker_id,
                    )
                else:
                    err = (
                        (
                            (resolve_result.get("data") or {}).get("error")
                            if isinstance(resolve_result.get("data"), dict)
                            else None
                        )
                        or resolve_result.get("error")
                        or "resolve returned not-ok"
                    )
                    ctx.log(
                        "WARNING",
                        "verify_twin_blocker_resolve_failed",
                        lane_id=twin_id,
                        blocker_id=blocker_id,
                        error=str(err),
                    )
        except Exception as exc:  # noqa: BLE001 — one twin must not abort the cycle
            ctx.log(
                "ERROR",
                "verify_twin_blocker_resolve_failed",
                lane_id=twin_id,
                error=str(exc),
            )
            continue


def _worker_management_phase(ctx: OrchestratorContext) -> None:
    """Step 4: Check worker status / health, start missing workers, poll merge-ready lanes.

    Per-cycle worker bounds (grok ``max_turns`` / wall-clock timeout) and cross-cycle
    ``token_budget`` are unchanged. Junior grok lanes retain ``--no-subagents`` for
    spend containment (see ``grok_cli.py``).
    """
    ctx.autostart_results = _ensure_lane_workers(
        orchestrator_root=ctx.orchestrator_root,
        task_ref=ctx.task_ref,
        lane_ids=ctx.m_order,
        backend=ctx.backend,
        worker_start_mode=ctx.worker_start_mode,
        worker_reasoning_effort=ctx.worker_reasoning_effort,
        model=ctx.model,
        dry_run=ctx.dry_run,
        log=ctx.log,
        prev_health=ctx.lane_health_prev,
        state_dir=ctx.state_dir,
    )
    for row in ctx.autostart_results:
        if isinstance(row, dict) and row.get("reason") == "attention_required":
            ctx.attention_stalls[row["lane_id"]] = ctx.attention_stalls.get(row["lane_id"], 0) + 1
        elif isinstance(row, dict) and row.get("lane_id") in ctx.attention_stalls:
            del ctx.attention_stalls[row["lane_id"]]

    stale_attention = {lane for lane, count in ctx.attention_stalls.items() if count >= 3}

    ctx.has_in_flight = _lane_work_in_flight(ctx.autostart_results, stale_attention_lanes=stale_attention)
    if ctx.has_in_flight:
        ctx.log("INFO", "worker_pool_checked", results=ctx.autostart_results)

    ready_lanes = _poll_merge_ready_lanes(
        ctx.orchestrator_root,
        ctx.task_ref,
        ctx.m_order,
        log=ctx.log,
        unmerged_unknown_streaks=ctx.unmerged_unknown_streaks,
        unmerged_unknown_persistent_emitted=ctx.unmerged_unknown_persistent_emitted,
        unmerged_unknown_read_failures=ctx.unmerged_unknown_read_failures,
    )

    # Explicit recovery arm ([DOM-06] / internal): record-without-MERGED
    # cannot self-heal via re-intake (clean lanes are excluded from ordered_ready).
    # Recovery MUST run before the noop-completer: the no-record heal sub-arm
    # keys on an unconsumed merge-ready report, and the completer consumes that
    # report for clean lanes — completer-first would eat the heal evidence and
    # re-strand a record-failure lane.
    recovered_landings = _recover_stranded_landing_lanes(ctx)
    if recovered_landings:
        ctx.log("INFO", "landing_recovery_completed", lanes=recovered_landings)

    noop_completed = _complete_already_satisfied_merge_ready_lanes(
        ctx.orchestrator_root,
        ctx.task_ref,
        ctx.m_order,
        dry_run=ctx.dry_run,
        log=ctx.log,
    )
    if noop_completed:
        ctx.log("INFO", "merge_ready_noop_completed", lanes=noop_completed)

    ctx.ordered_ready = _sort_by_manifest_merge_order(ready_lanes, ctx.m_order)
    ctx.log("INFO", "poll_complete", ready_lanes=ctx.ordered_ready)

    # Recovery and noop both advance terminal progress; reset the stall tripwire
    # so a recovery-only cycle does not count toward plan_stall_threshold_reached.
    if noop_completed or recovered_landings:
        ctx.plan_stall_count = 0
    elif not ready_lanes and not ctx.guidance_results and not ctx.plan_dispatch and not ctx.has_in_flight:
        ctx.plan_stall_count += 1
        if ctx.plan_stall_count >= 3:
            ctx.log("ERROR", "plan_stall_threshold_reached")
            raise RuntimeError("plan_stall_threshold_reached")
    else:
        ctx.plan_stall_count = 0

    # Write side of the blocked-lane heartbeat: conclusively close aged, dead
    # blocked lanes so they cannot accumulate unbounded ([RES-07] Slice-3).
    _reap_blocked_lanes_maintenance(ctx)


def _lane_intake_phase(ctx: OrchestratorContext) -> None:
    """Step 5: Intake merge-ready lanes, refresh downstream, verify, and check close readiness."""
    from lane_manifest import downstream_lanes  # noqa: PLC0415
    from workbay_handoff_mcp import (  # noqa: PLC0415
        handoff_close_check,
        record_decision,
        record_test_result,
    )

    # Resolve finished verify-twin blockers BEFORE the cycle's close check so
    # the same intake handoff_close_check sees ready_to_close flip (implementation note S2).
    _resolve_verify_twin_blockers(ctx)

    for lane_id in ctx.ordered_ready:
        ctx.log("INFO", "intake_start", lane=lane_id)
        intake_ok = _intake_lane(
            ctx.orchestrator_root,
            ctx.task_ref,
            lane_id,
            dry_run=ctx.dry_run,
        )
        decision_text = (
            f"Orchestrator daemon intaked lane {lane_id} successfully."
            if intake_ok
            else f"Orchestrator daemon failed to intake lane {lane_id}."
        )
        if not ctx.dry_run:
            record_decision(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                decision=decision_text,
                rationale=f"Automated intake cycle for merge-ready lane {lane_id}.",
            )
        ctx.log("INFO", "intake_complete", lane=lane_id, success=intake_ok)

        if not intake_ok:
            continue

        # Propagate refresh/verify only when the lane actually terminalized
        # (MERGED written) or dry_run. Containment / record failures leave
        # write_merged False so dependents are not refreshed from a non-landed tip.
        write_merged = False
        if not ctx.dry_run:
            cursor = _complete_lane_plan_cursor(ctx.task_ref, lane_id)
            if cursor is not None:
                ctx.log("INFO", "plan_cursor_completed", lane=lane_id, plan_item_id=cursor.get("plan_item_id"))
            from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

            from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
                consume_lane_worker_reports,
                manage_worktree_lane,
            )

            # Record-first ([GRPH-14] / [DOM-06]): capture the task-branch tip the
            # lane just landed on BEFORE the terminal MERGED write, so the
            # moment-1 predicate always has evidence for a lane that reached MERGED.
            #
            # Consume is deferred until record + MERGED succeed so a withhold
            # keeps the merge_ready report for poll re-entry when unmerged work
            # remains. Post-merge ledger failure (no unmerged commits) is healed
            # by ``_recover_stranded_landing_lanes`` no-record sub-arm.
            #
            # Failure arms (H3):
            # - No usable SHA (git failed / invalid shape): skip the record and
            #   still write MERGED — post-merge the lane has no unmerged commits
            #   and can never re-enter ordered_ready; withholding MERGED wedges it.
            # - SHA in hand but containment fails: do NOT write MERGED; leave the
            #   merge_ready report unacked for poll re-entry (unmerged commits).
            # - Ledger write fails with a trusted SHA: do NOT write MERGED; the
            #   recovery no-record sub-arm re-attempts record then MERGED when an
            #   unconsumed merge_ready report remains and containment still holds.
            landed_sha, task_branch = _task_branch_landing(ctx.orchestrator_root)
            write_merged = True
            if landed_sha is None:
                ctx.log("ERROR", "landing_sha_unresolved", lane=lane_id)
            else:
                lane_branch = _resolve_lane_branch(ctx.orchestrator_root, ctx.task_ref, lane_id)
                contained = (
                    _lane_branch_contained_in(ctx.orchestrator_root, landed_sha, lane_branch) if lane_branch else None
                )
                if contained is not True:
                    # H1/H2: recipe exit-0 without merge, or unresolvable branch —
                    # record NOTHING. Absence is safe; a false landing is not.
                    ctx.log(
                        "ERROR",
                        "landing_not_contained",
                        lane=lane_id,
                        sha=landed_sha,
                        branch=lane_branch,
                        contained=contained,
                    )
                    write_merged = False
                elif record_lane_landing(
                    ctx.task_ref,
                    lane_id,
                    landed_sha,
                    task_branch,
                    log=ctx.log,
                ):
                    ctx.log(
                        "INFO",
                        "lane_landing_recorded",
                        lane=lane_id,
                        sha=landed_sha,
                        branch=task_branch,
                    )
                else:
                    # H3: valid SHA but ledger write failed — recovery re-attempts.
                    write_merged = False

            if write_merged:
                close_raw = manage_worktree_lane(
                    operation="close",
                    lane_id=lane_id,
                    status=LaneStatus.MERGED,
                    notes="Auto-closed by orchestrator daemon post-intake.",
                    task_ref=ctx.task_ref,
                )
                if isinstance(close_raw, str):
                    close_raw = json.loads(close_raw)
                close_payload = close_raw if isinstance(close_raw, dict) else {}
                # Mirror recovery: only claim terminal success when close ok.
                if close_payload.get("ok") is True:
                    ctx.log("INFO", "lane_auto_merged", lane=lane_id)
                    # Close-cycle ack only after record + MERGED succeeded.
                    consume_lane_worker_reports(lane_id, task_ref=ctx.task_ref)
                else:
                    ctx.log(
                        "ERROR",
                        "lane_auto_merge_failed",
                        lane=lane_id,
                        payload=close_payload,
                    )
                    # Status may still be non-MERGED; recovery heals next cycle.
                    # Do not refresh dependents from a non-terminal close.
                    write_merged = False

        # Gate merge-propagation on the same condition as MERGED ([DOM-06]).
        if not (ctx.dry_run or write_merged):
            continue

        deps = downstream_lanes(ctx.task_ref, lane_id)
        if deps:
            ctx.log("INFO", "refresh_start", lane=lane_id, downstream=deps)
            refresh_results = _refresh_downstream(
                ctx.orchestrator_root,
                ctx.task_ref,
                lane_id,
                deps,
                dry_run=ctx.dry_run,
            )
            ctx.log("INFO", "refresh_complete", lane=lane_id, results=refresh_results)

        ctx.log("INFO", "verify_start", lane=lane_id)
        verify_ok = _run_cross_lane_verify(
            ctx.orchestrator_root,
            ctx.task_ref,
            lane_id,
            dry_run=ctx.dry_run,
        )
        if not ctx.dry_run:
            record_test_result(
                session=f"{ctx.task_ref}-orchestrator-daemon",
                command=f"make lane-check TASK={ctx.task_ref} LANE={lane_id}",
                passed=verify_ok,
                result="Cross-lane verification passed." if verify_ok else "Cross-lane verification failed.",
            )
        ctx.log("INFO", "verify_complete", lane=lane_id, passed=verify_ok)

    close_check = _require_dict_payload(
        handoff_close_check(task_ref=ctx.task_ref),
        source=f"handoff_close_check({ctx.task_ref})",
    )
    # v2 envelopes put ready_to_close under data only (no top-level mirror).
    # Fall back to the raw dict for callers that already pass an unwrapped payload.
    ctx.ready_to_close = bool((close_check.get("data") or close_check).get("ready_to_close"))
    ctx.runtime_failure_count = 0
    ctx.log("INFO", "close_check_complete", ready_to_close=ctx.ready_to_close)
    ctx.log("INFO", "cycle_end", intaked=ctx.ordered_ready, guidance=len(ctx.guidance_results))


def _build_orchestrator_context(
    orchestrator_root: Path,
    task_ref: str,
    poll_interval: int,
    single_pass: bool,
    dry_run: bool,
    backend: str,
    worker_start_mode: str,
    worker_reasoning_effort: str,
    model: str | None,
    state_dir: Path | None = None,
) -> OrchestratorContext:
    """Configure MCP runtime and build an OrchestratorContext ready for the loop."""
    from lane_manifest import merge_order as manifest_merge_order  # noqa: PLC0415
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime  # noqa: PLC0415

    state_dir = state_dir or orchestrator_root / ".task-state"
    log_dir = orchestrator_root / "logs" / "daemon"
    run_id = str(uuid.uuid4())

    def log(level: str, event: str, **kw: object) -> None:
        _log(log_dir, level, event, run_id=run_id, **kw)

    runtime = RuntimeConfig.for_repo(
        orchestrator_root,
        state_dir=state_dir,
        current_task_path=orchestrator_root / "CURRENT_TASK.json",
        exports_dir=state_dir / "exports",
    )
    configure_runtime(runtime)

    log(
        "INFO",
        "daemon_start",
        task_ref=task_ref,
        single_pass=single_pass,
        backend=backend,
        worker_start_mode=worker_start_mode,
        worker_reasoning_effort=worker_reasoning_effort,
        model=model,
    )

    m_order = manifest_merge_order(task_ref)
    log("INFO", "manifest_loaded", merge_order=m_order)

    return OrchestratorContext(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        state_dir=state_dir,
        log_dir=log_dir,
        m_order=m_order,
        poll_interval=poll_interval,
        single_pass=single_pass,
        dry_run=dry_run,
        backend=backend,
        worker_start_mode=worker_start_mode,
        worker_reasoning_effort=worker_reasoning_effort,
        model=model,
        log=log,
    )


def _run_orchestrator_cycle(ctx: OrchestratorContext) -> int:
    """Run one orchestrator cycle. Returns an exit code or ``-1`` to continue."""
    log = ctx.log
    poll_interval = ctx.poll_interval
    task_ref = ctx.task_ref
    single_pass = ctx.single_pass

    # Cycle-scoped dependency-refusal observability: surface counts
    # accumulated since the previous cycle, then reset so each summary
    # covers exactly one cycle regardless of which exit path it takes.
    log_dependency_refusal_summary(log, reset=True, task_ref=task_ref)

    if _shutdown_requested:
        log("INFO", "daemon_stop", reason="sigterm")
        return 0
    if _is_paused(ctx.state_dir):
        log("INFO", "daemon_paused")
        if single_pass:
            return 0
        # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
        time.sleep(poll_interval)
        return -1  # continue

    log("INFO", "cycle_start")

    try:
        _dispatch_phase(ctx)
    except Exception:
        if single_pass or ctx.dispatch_failure_count >= 3:
            return 1

    try:
        _guidance_phase(ctx)
        _plan_dispatch_phase(ctx)
        _worker_management_phase(ctx)
        _lane_intake_phase(ctx)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        ctx.runtime_failure_count += 1
        log("ERROR", "runtime_phase_failed", error=str(exc), failure_count=ctx.runtime_failure_count)
        if single_pass or ctx.runtime_failure_count >= 3:
            log("ERROR", "terminal_error", reason="runtime_failure")
            return 1
        log("INFO", "poll_sleep", interval=poll_interval)
        # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
        time.sleep(poll_interval)
        return -1  # continue

    if ctx.ready_to_close:
        remaining_plan_items = _remaining_plan_work(task_ref)
        if not remaining_plan_items:
            log("INFO", "task_complete", task_ref=task_ref)
            return 0
        log("INFO", "task_close_blocked_by_plan", remaining=len(remaining_plan_items))
        if single_pass:
            return 1

    if single_pass:
        return 0

    log("INFO", "poll_sleep", interval=poll_interval)
    # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
    time.sleep(poll_interval)
    return -1  # continue


def orchestrator_loop(
    *,
    orchestrator_root: Path,
    task_ref: str,
    poll_interval: int = 60,
    single_pass: bool = False,
    dry_run: bool = False,
    backend: str = "codex-cli",
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
    state_dir: Path | None = None,
) -> int:
    """Main daemon loop.  Returns 0 on clean exit, 1 on failure."""
    ctx = _build_orchestrator_context(
        orchestrator_root,
        task_ref,
        poll_interval,
        single_pass,
        dry_run,
        backend,
        worker_start_mode,
        worker_reasoning_effort,
        model,
        state_dir=state_dir,
    )
    from workbay_orchestrator_mcp.orchestration.daemon_startup import (  # noqa: PLC0415
        emit_daemon_startup_warning,
    )

    emit_daemon_startup_warning("orchestrator", poll_interval=poll_interval)
    # Objective 5: empty-graph override is read once at process start; state it.
    if allow_empty_dependency_graph():
        ctx.log(
            "INFO",
            "WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1 activates depends_on scheduling "
            "over an empty graph = unconstrained dispatch",
        )
    while True:
        result = _run_orchestrator_cycle(ctx)
        if result != -1:
            return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orchestrator daemon: dispatch, intake, refresh, verify.")
    sub = parser.add_subparsers(dest="command")

    run_parser = sub.add_parser("run", help="Run the orchestrator loop.")
    run_parser.add_argument("--orchestrator-root", required=True, help="Absolute path to the monorepo root.")
    run_parser.add_argument(
        "--task-ref", required=False, help="MCP task reference. If omitted, infers from active task or manifests."
    )
    run_parser.add_argument("--poll-interval", type=int, default=60, help="Seconds between poll cycles (default: 60).")
    run_parser.add_argument("--single-pass", action="store_true", help="Run one cycle and exit.")
    run_parser.add_argument("--dry-run", action="store_true", help="Skip mutating operations.")
    run_parser.add_argument(
        "--backend", default="codex-cli", help="Execution backend for worker spawning (default: codex-cli)."
    )
    run_parser.add_argument("--worker-start-mode", default="mcp", help="Worker session startup mode (default: mcp).")
    run_parser.add_argument(
        "--worker-reasoning-effort", default="auto", help="Reasoning effort for spawned workers (default: auto)."
    )
    run_parser.add_argument("--model", help="Execution model to use for worker spawning.")
    run_parser.add_argument(
        "--state-dir", default=None, help="State directory. Defaults to <orchestrator-root>/.task-state."
    )

    pause_parser = sub.add_parser("pause", help="Pause the daemon.")
    pause_parser.add_argument("--state-dir", required=True)

    resume_parser = sub.add_parser("resume", help="Resume the daemon.")
    resume_parser.add_argument("--state-dir", required=True)

    status_parser = sub.add_parser("status", help="Show daemon status.")
    status_parser.add_argument("--state-dir", required=True)
    status_parser.add_argument("--log-dir", default=None, help="Log directory. Defaults to <state-dir>/../logs/daemon.")

    salvage_parser = sub.add_parser(
        "salvage-and-close",
        help="Freeze a failed lane, classify its changed files, and close it.",
    )
    salvage_parser.add_argument("--orchestrator-root", required=True, help="Absolute path to the monorepo root.")
    salvage_parser.add_argument("--task-ref", required=True, help="MCP task reference.")
    salvage_parser.add_argument("--lane-id", required=True, help="Lane to salvage and close.")
    salvage_parser.add_argument(
        "--dry-run", action="store_true", help="Print salvage groups without mutating MCP state."
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.command == "pause":
        state_dir = Path(args.state_dir).expanduser().resolve()
        daemon_pause(state_dir)
        print("Daemon paused.")
        return 0

    if args.command == "resume":
        state_dir = Path(args.state_dir).expanduser().resolve()
        daemon_resume(state_dir)
        print("Daemon resumed.")
        return 0

    if args.command == "status":
        state_dir = Path(args.state_dir).expanduser().resolve()
        log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else state_dir.parent / "logs" / "daemon"
        status = daemon_status(state_dir, log_dir)
        print(json.dumps(status, indent=2, default=str))
        return 0

    if args.command == "salvage-and-close":
        orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
        state_dir = orchestrator_root / ".task-state"
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        runtime = RuntimeConfig.for_repo(
            orchestrator_root,
            state_dir=state_dir,
            current_task_path=orchestrator_root / "CURRENT_TASK.json",
            exports_dir=state_dir / "exports",
        )
        configure_runtime(runtime)
        result = salvage_and_close_lane(
            orchestrator_root,
            args.task_ref,
            args.lane_id,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "run":
        orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
        state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else orchestrator_root / ".task-state"

        lock = OrchestratorLock(state_dir)
        if not lock.acquire():
            print("Another orchestrator daemon is already running.", file=sys.stderr)
            return 1

        signal.signal(signal.SIGTERM, _handle_sigterm)
        try:
            resolved_task = _resolve_task_ref(orchestrator_root, args.task_ref)
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 1

        try:
            return orchestrator_loop(
                orchestrator_root=orchestrator_root,
                task_ref=resolved_task,
                poll_interval=args.poll_interval,
                single_pass=args.single_pass,
                dry_run=args.dry_run,
                backend=args.backend,
                worker_start_mode=args.worker_start_mode,
                worker_reasoning_effort=args.worker_reasoning_effort,
                model=args.model,
                state_dir=state_dir,
            )
        finally:
            lock.release()

    # No subcommand -- print help
    _parse_args()
    return 1


if __name__ == "__main__":
    sys.exit(main())
