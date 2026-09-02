"""Deterministic census of non-terminal remote lanes, with typed repairs.

Joins each non-terminal lane row with its latest dispatch, local remote-exec
artifacts, and ONE bounded remote sandbox probe. Verdicts are typed outcomes
(Release It! health checks); repairs are idempotent and keyed on
``(lane_id, dispatch_id)`` (DDIA); the probe is bounded and never silently
coerced into stale (fail-closed). Dry-run is the default.

Cheap classification (status / started_at / local completion evidence)
selects repair-eligible rows before any SSH probe. The cheap_repair
probe loop is capped at ``batch + PROBE_OBSERVE_SPARE`` (repair-scan
budget); observe-only probes use the same spare. Waiting-for-dispatch
(planned) and other sink statuses do not occupy the repair batch.
Rows probed and classified running or dirty-orphan are excluded from
the next cheap_repair prefix so the window advances. Attempted-but-not-applied
cheap_repair ids are persisted as poison-backoff skip, a distinct identity
from observe-after-probe skip, so a later observe-spare writer cannot pop
the backoff. They stay counted in remaining_repairs. Skip-matched backoff
ids do not consume ``PROBE_OBSERVE_SPARE``. Observe-after-probe ids are
persisted for cheap_observe too; skip-matched cheap_observe rows do not
consume the spare before unvisited cheap_observe.
Applied mutations are capped at max_batch; leftover planned repairs
count as remaining work. Unvisited cheap_repair rows count as
``remaining_repairs``. Unvisited skip-mapped non-sinks also count
when the scanned prefix planned a repair. Unvisited cheap_observe
occupancy (in-cap without local completion) counts in both branches
so one planned repair cannot wash out landed commits. When the
scanned prefix planned no repairs, unvisited non-sink rows also
count as remaining work. A row whose idempotency key is already
present is observe-only.
A failed probe is never a clean census.

Tech debt (LOW): commit_count parse fail-open, probe thread leak on
timeout, unread --json flag.
F3 LOW: observe-after-probe skip file is best-effort, not a keyed receipt.
F4 LOW: latest-dispatch sqlite join fail-open.
F5 LOW: truncated observe leftovers do not fail ok.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.backend_registry import (
    _remote_host_is_malformed,
    _resolve_remote_gate_host_for_probe,
)
from workbay_orchestrator_mcp.orchestration.offload_timeout_ssot import (
    CODEX_TIMEOUT_CAP,
    CURSOR_TIMEOUT_CAP,
    GROK_TIMEOUT_CAP,
)
from workbay_orchestrator_mcp.orchestration.openrouter_lane_config import OPENROUTER_TIMEOUT_CAP
from workbay_orchestrator_mcp.orchestration.remote_exec_staging_reaper import STAGING_DIR_PREFIX

# Named per-backend stale clocks (RES-02). GROK remains the unlabeled default.
BACKEND_CAP_S = float(GROK_TIMEOUT_CAP)
GRACE_S = 300.0
CAP_PLUS_GRACE_S = BACKEND_CAP_S + GRACE_S
PROBE_TIMEOUT_S = 20.0
LIVE_SCOPE_UNIT_TEMPLATES = (
    "grok-lane-{slug}.scope",
    "codex-lane-{slug}.scope",
    "cursor-lane-{slug}.scope",
    "openrouter-lane-{slug}.scope",
)
RETRY_BUDGET = 2
DEFAULT_MAX_BATCH = 50
# Small fixed observe-only probe budget. Never walk the whole non-sink population.
PROBE_OBSERVE_SPARE = 8
# Skip-map identities (DDIA typed states). Observe-after-probe is the default
# two-element key; backoff is an optional third field so _extend_observe_skip
# never pops a failed/deferred planned-repair entry.
SKIP_REASON_OBSERVE = "observe"
SKIP_REASON_BACKOFF = "backoff"
CENSUS_SESSION = "lane-census"
CENSUS_WINDOW_FILENAME_PREFIX = "lane-census-window-"
REMOTE_SANDBOX_REAP_LOCK_FILENAME = ".remote-sandbox-reap.lock"
REAP_ERROR_CAP = 512
REAP_LIST_CAP = 50
RETRY_NOTES_RE = re.compile(r"census_retries=(\d+)")
logger = logging.getLogger(__name__)

TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
EXECUTING_STATUSES = frozenset({"active"})
REVIEW_OR_MERGED = frozenset({"review", "merged"})
# Status-level census sinks: already parked, no planned repair, must not pin max_batch.
# planned is waiting for dispatch, not a census repair (leftover staging-dir
# started_at must not re-classify a reset row as stale_no_result).
CENSUS_SINK_STATUSES = frozenset({"blocked", "review", "planned"})

VERDICT_RUNNING = "running"
VERDICT_IN_REVIEW = "in_review"
VERDICT_LANDED_UNADJUDICATED = "landed_unadjudicated"
VERDICT_DEGENERATE_TURN = "degenerate_turn"
VERDICT_STALE_NO_RESULT = "stale_no_result"
VERDICT_ORPHANED_SANDBOX = "orphaned_sandbox"
VERDICT_SUPERSEDED = "superseded"
VERDICT_PROBE_FAILED = "probe_failed"

VERDICT_KINDS = (
    VERDICT_RUNNING,
    VERDICT_IN_REVIEW,
    VERDICT_LANDED_UNADJUDICATED,
    VERDICT_DEGENERATE_TURN,
    VERDICT_STALE_NO_RESULT,
    VERDICT_ORPHANED_SANDBOX,
    VERDICT_SUPERSEDED,
    VERDICT_PROBE_FAILED,
)

REPAIR_RESET_TO_PLANNED = "reset_to_planned"
REPAIR_PROMOTE_TO_REVIEW = "promote_to_review"
REPAIR_CLOSE_SUPERSEDED = "close_superseded"

REPAIR_FOR_KIND: dict[str, str | None] = {
    VERDICT_RUNNING: None,
    VERDICT_IN_REVIEW: None,
    VERDICT_LANDED_UNADJUDICATED: REPAIR_PROMOTE_TO_REVIEW,
    VERDICT_DEGENERATE_TURN: REPAIR_RESET_TO_PLANNED,
    VERDICT_STALE_NO_RESULT: REPAIR_RESET_TO_PLANNED,
    VERDICT_ORPHANED_SANDBOX: None,
    VERDICT_SUPERSEDED: REPAIR_CLOSE_SUPERSEDED,
    VERDICT_PROBE_FAILED: None,
}

# update_lane returns ok:false after the SQL row write when CURRENT_TASK.md
# regeneration fails. The mutation is committed; do not treat it as a no-op.
COMMITTED_SIDE_EFFECT_ERROR_TYPE = "current_task_side_effect_failed"

ProbeFn = Callable[[Mapping[str, Any]], Any]
ListRowsFn = Callable[[str], Sequence[Mapping[str, Any]]]
ListArtifactsFn = Callable[[str], Mapping[str, Any]]
UpdateRowFn = Callable[..., Any]
RecordDecisionFn = Callable[..., Any]
ReapSandboxesFn = Callable[..., Any]
LogFn = Callable[..., Any]


@dataclass(frozen=True)
class LaneCensusVerdict:
    lane_id: str
    dispatch_id: str
    kind: str
    evidence: dict[str, Any]
    repair: str | None
    applied: bool


@dataclass(frozen=True)
class CensusReport:
    verdicts: list[LaneCensusVerdict]
    counts: dict[str, int] = field(default_factory=dict)
    truncated: bool = False
    remaining_repairs: int = 0
    list_error: dict[str, Any] | None = None


class CensusListError(Exception):
    """Failed list_lanes read; callers must not treat this as an empty census."""

    def __init__(self, message: str, *, error_type: str = "list_error") -> None:
        super().__init__(message)
        self.error_type = error_type

    def as_payload(self) -> dict[str, str]:
        return {"error_type": self.error_type, "error": str(self)}


@dataclass(frozen=True)
class _ProbeOk:
    marker_mtime: float | None
    live_process: bool
    commit_count: int
    dirty: bool


@dataclass(frozen=True)
class _ProbeFailed:
    error: str


PreparedRow = tuple[dict[str, Any], dict[str, Any], _ProbeOk | _ProbeFailed]


def repair_decision_id(task_ref: str, lane_id: str, dispatch_id: str) -> str:
    """Stable decision key for one (lane, dispatch) repair."""
    return f"census_repair:{task_ref}:{lane_id}:{dispatch_id}"


def backend_cap_s(backend: Any) -> float:
    """Wall-clock cap for *backend*; unlabeled rows use the grok default."""
    name = str(backend or "").strip().lower()
    if name.startswith("codex"):
        return float(CODEX_TIMEOUT_CAP)
    if name.startswith("cursor"):
        return float(CURSOR_TIMEOUT_CAP)
    if name.startswith("openrouter"):
        return float(OPENROUTER_TIMEOUT_CAP)
    if name.startswith("grok") or not name:
        return float(GROK_TIMEOUT_CAP)
    return float(max(CODEX_TIMEOUT_CAP, GROK_TIMEOUT_CAP, CURSOR_TIMEOUT_CAP, OPENROUTER_TIMEOUT_CAP))


def live_process_probe_script(slug: str) -> str:
    """Occupancy snippet: every backend scope unit plus the backend-agnostic lease."""
    units = " ".join(f'"{template.format(slug=slug)}"' for template in LIVE_SCOPE_UNIT_TEMPLATES)
    return f"""
live=0
for unit in {units}; do
  if systemctl is-active --quiet "$unit" 2>/dev/null; then
    live=1
    break
  fi
done
if [ "$live" -eq 0 ] && [ -f "$ROOT/.lane-live-{slug}" ]; then
  expiry=$(awk -F= '/^expiry=/ {{ print $2 }}' "$ROOT/.lane-live-{slug}" 2>/dev/null || true)
  now=$(date +%s)
  case "$expiry" in
    ''|*[!0-9]*) live=1 ;;
    *) if [ "$now" -lt "$expiry" ]; then live=1; fi ;;
  esac
fi
echo "live_process=$live"
"""


def report_payload(report: CensusReport, *, task_ref: str, apply: bool) -> dict[str, Any]:
    """JSON-ready census payload (CLI / make)."""
    remaining = max(0, int(report.remaining_repairs or 0))
    list_error = report.list_error
    repair_failed = False
    if apply:
        repair_failed = any(
            bool(
                item.evidence.get("update_error")
                or item.evidence.get("decision_error")
                or item.evidence.get("peek_error")
            )
            for item in report.verdicts
        )
    probe_failed = any(item.kind == VERDICT_PROBE_FAILED for item in report.verdicts)
    payload: dict[str, Any] = {
        "ok": not repair_failed and remaining == 0 and not list_error and not probe_failed,
        "applied": bool(apply) and not probe_failed,
        "task_ref": task_ref,
        "truncated": report.truncated,
        "remaining_repairs": remaining,
        "counts": dict(report.counts),
        "verdicts": [asdict(item) for item in report.verdicts],
    }
    if list_error:
        payload["list_error"] = list_error
    return payload


def census_lanes(
    task_ref: str,
    *,
    root: Path | str,
    apply: bool = False,
    probe: ProbeFn | None = None,
    list_rows: ListRowsFn | None = None,
    list_dispatch_artifacts: ListArtifactsFn | None = None,
    update_row: UpdateRowFn | None = None,
    record_decision: RecordDecisionFn | None = None,
    now: Any = None,
    max_batch: int = DEFAULT_MAX_BATCH,
    reap_sandboxes: ReapSandboxesFn | None = None,
    log: LogFn | None = None,
) -> CensusReport:
    """Census every non-terminal lane row of *task_ref*.

    Collaborators default lazily so tests inject fakes and never touch the
    network. ``apply=False`` is dry-run: verdicts and repair plans are
    reported, nothing is written. ``apply=True`` also runs the acting
    remote-sandbox reaper.
    """
    resolved_task = str(task_ref or "").strip()
    workspace = Path(root)
    clock = _epoch(now)
    batch = max(1, int(max_batch))
    rows_fn = list_rows if list_rows is not None else (lambda _ref: _default_list_rows(_ref))
    artifacts_fn = (
        list_dispatch_artifacts
        if list_dispatch_artifacts is not None
        else (lambda lane_id: _default_list_dispatch_artifacts(workspace, lane_id))
    )
    probe_fn = probe if probe is not None else _default_probe
    update_fn = update_row if update_row is not None else _default_update_row
    decision_fn = record_decision if record_decision is not None else _default_record_decision
    reap_fn = reap_sandboxes if reap_sandboxes is not None else _default_reap_sandboxes
    log_fn = log if log is not None else _default_census_log

    try:
        raw_rows = list(rows_fn(resolved_task) or ())
    except CensusListError as exc:
        counts = {kind: 0 for kind in VERDICT_KINDS}
        counts["repairs_applied"] = 0
        counts["remaining_repairs"] = 0
        return CensusReport(
            verdicts=[],
            counts=counts,
            truncated=False,
            remaining_repairs=0,
            list_error=exc.as_payload(),
        )
    non_terminal = [dict(row) for row in raw_rows if str(row.get("status") or "") not in TERMINAL_STATUSES]
    ordered = sorted(non_terminal, key=_row_sort_key)
    # F1: bound the repair batch to rows whose planned repair is not None and
    # whose status is not already a census sink (blocked, review, planned).
    # Already-keyed rows are observe-only so a leftover receipt cannot pin
    # max_batch. Observe-only kinds fill PROBE_OBSERVE_SPARE so a blocked-only
    # census still reports (RES-19) without walking the whole population.
    candidates = [row for row in ordered if str(row.get("status") or "") not in CENSUS_SINK_STATUSES]
    sinks = [row for row in ordered if str(row.get("status") or "") in CENSUS_SINK_STATUSES]
    exists_fn = getattr(decision_fn, "exists", None)

    def _decision_exists(key: str) -> bool:
        if callable(exists_fn):
            return bool(exists_fn(key))
        return _default_decision_exists(key, task_ref=resolved_task)

    artifacts_cache: dict[str, dict[str, Any]] = {}

    def _artifacts_for(lane_id: str) -> dict[str, Any]:
        if lane_id not in artifacts_cache:
            artifacts_cache[lane_id] = dict(artifacts_fn(lane_id) or {}) if lane_id else {}
        return artifacts_cache[lane_id]

    def _probe_one(
        row: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], _ProbeOk | _ProbeFailed]:
        lane_id = str(row.get("lane_id") or "").strip()
        return (row, _artifacts_for(lane_id), _invoke_probe(probe_fn, row))

    skip_map = _load_observe_skip(workspace, resolved_task)
    cheap_repair: list[dict[str, Any]] = []
    cheap_observe: list[dict[str, Any]] = []
    for row in candidates:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        artifacts = _artifacts_for(lane_id)
        if _decision_key_present(
            task_ref=resolved_task,
            row=row,
            artifacts=artifacts,
            decision_exists=_decision_exists,
        ):
            cheap_observe.append(row)
            continue
        if _observe_skip_matches(row, skip_map):
            # Already probed as running / dirty-orphan; do not pin the prefix.
            cheap_observe.append(row)
            continue
        if _cheap_may_need_repair(row, artifacts, now=clock):
            cheap_repair.append(row)
        else:
            cheap_observe.append(row)

    # Live-past-cap workers look repair-eligible until probed. Bound the
    # cheap_repair scan at batch + spare so 80 live rows cannot cost 80 SSH
    # round-trips (Latency: never an unbounded buffer). The skip map keeps
    # the window moving past observe-after-probe rows on the next run.
    repair_scan_budget = batch + PROBE_OBSERVE_SPARE
    scanned_repair: list[PreparedRow] = []
    for row in cheap_repair:
        if len(scanned_repair) >= repair_scan_budget:
            break
        scanned_repair.append(_probe_one(row))
        if (
            _count_planned_repairs(
                scanned_repair,
                now=clock,
                task_ref=resolved_task,
                decision_exists=_decision_exists,
            )
            >= batch
        ):
            break
    scanned_repair_ids = {str(item[0].get("lane_id") or "") for item in scanned_repair}
    skip_map = _extend_observe_skip(
        skip_map,
        scanned_repair,
        now=clock,
        task_ref=resolved_task,
        decision_exists=_decision_exists,
    )
    live_ids = {str(row.get("lane_id") or "").strip() for row in ordered if str(row.get("lane_id") or "").strip()}
    _save_observe_skip(workspace, resolved_task, skip_map, live_ids=live_ids)

    repair_prepared, observe_from_repair_scan = _split_repair_and_observe(
        scanned_repair,
        now=clock,
        task_ref=resolved_task,
        decision_exists=_decision_exists,
    )
    prepared: list[PreparedRow] = list(repair_prepared[:batch])
    observe_budget = PROBE_OBSERVE_SPARE
    scanned_observe: list[PreparedRow] = []

    def _append_observe(item: PreparedRow) -> None:
        nonlocal observe_budget
        prepared.append(item)
        scanned_observe.append(item)
        observe_budget -= 1

    for item in observe_from_repair_scan:
        if observe_budget <= 0:
            break
        _append_observe(item)
    unvisited_observe = [
        row
        for row in cheap_observe
        if str(row.get("lane_id") or "").strip() and not _observe_skip_matches(row, skip_map)
    ]
    skip_matched_observe = [
        row
        for row in cheap_observe
        if str(row.get("lane_id") or "").strip()
        and _observe_skip_matches(row, skip_map)
        and _skip_reason(skip_map.get(str(row.get("lane_id") or "").strip())) != SKIP_REASON_BACKOFF
    ]
    if observe_budget > 0:
        for row in unvisited_observe:
            if observe_budget <= 0:
                break
            _append_observe(_probe_one(row))
        for row in skip_matched_observe:
            if observe_budget <= 0:
                break
            _append_observe(_probe_one(row))
        for row in sinks:
            if observe_budget <= 0:
                break
            if not str(row.get("lane_id") or "").strip():
                continue
            _append_observe(_probe_one(row))
    skip_map = _extend_observe_skip(
        skip_map,
        scanned_observe,
        now=clock,
        task_ref=resolved_task,
        decision_exists=_decision_exists,
    )
    _save_observe_skip(workspace, resolved_task, skip_map, live_ids=live_ids)
    prepared_ids = {
        str(item[0].get("lane_id") or "").strip() for item in prepared if str(item[0].get("lane_id") or "").strip()
    }
    planned_repairs = _count_planned_repairs(
        scanned_repair,
        now=clock,
        task_ref=resolved_task,
        decision_exists=_decision_exists,
    )
    unvisited_cheap_repair = sum(
        1
        for row in cheap_repair
        if str(row.get("lane_id") or "").strip() and str(row.get("lane_id") or "") not in scanned_repair_ids
    )
    visited_ids = prepared_ids | {item for item in scanned_repair_ids if item}
    unvisited_skip_mapped = sum(
        1
        for row in candidates
        if str(row.get("lane_id") or "").strip()
        and _observe_skip_matches(row, skip_map)
        and str(row.get("lane_id") or "").strip() not in visited_ids
    )
    unvisited_cheap_observe_occupancy = sum(
        1
        for row in cheap_observe
        if str(row.get("lane_id") or "").strip()
        and str(row.get("lane_id") or "").strip() not in visited_ids
        and not _decision_key_present(
            task_ref=resolved_task,
            row=row,
            artifacts=_artifacts_for(str(row.get("lane_id") or "").strip()),
            decision_exists=_decision_exists,
        )
        and not _observe_skip_matches(row, skip_map)
    )
    if planned_repairs > 0:
        remaining_repairs = unvisited_cheap_repair + unvisited_skip_mapped + unvisited_cheap_observe_occupancy
    else:
        # F2: in-cap occupancy without local completion is cheap_observe.
        # Unvisited non-sinks are remaining work so a landed remote commit
        # cannot hide behind the observe spare with ok:true. Scanned
        # live/dirty rows are not remaining repairs (F1: prefix produced
        # zero planned repairs).
        remaining_repairs = sum(
            1
            for row in candidates
            if str(row.get("lane_id") or "").strip() and str(row.get("lane_id") or "").strip() not in visited_ids
        )
    truncated = remaining_repairs > 0 or len(ordered) > len(prepared)

    running_ids = {
        str(row.get("lane_id") or "")
        for row, artifacts, probe_result in prepared
        if isinstance(probe_result, _ProbeOk)
        and _probe_shows_running(row=row, artifacts=artifacts, probe_result=probe_result, now=clock)
    }
    # CON-05: liveness + SHA + this census batch are compared together.
    superseded_ids = _superseded_lane_ids(
        [row for row, _artifacts, _probe in prepared],
        protected_ids=running_ids,
    )

    verdicts: list[LaneCensusVerdict] = []
    applied_slots = 0
    deferred_repairs = 0
    for row, artifacts, probe_result in prepared:
        lane_id = str(row.get("lane_id") or "").strip()
        dispatch_id = str(row.get("dispatch_id") or artifacts.get("dispatch_id") or "").strip()
        verdict = _classify(
            row=row,
            artifacts=artifacts,
            probe_result=probe_result,
            superseded=lane_id in superseded_ids,
            now=clock,
        )
        evidence = dict(verdict.evidence)
        evidence["status"] = str(row.get("status") or "")
        evidence["branch_tip_sha"] = row.get("branch_tip_sha")
        evidence["updated_at"] = row.get("updated_at")
        evidence["result_present"] = bool(artifacts.get("result_present"))
        evidence["turn_patch_size"] = _as_int(artifacts.get("turn_patch_size"), default=0)
        if "tool_call_count" not in evidence:
            evidence["tool_call_count"] = artifacts.get("tool_call_count")
        repair = REPAIR_FOR_KIND.get(verdict.kind)
        if str(row.get("status") or "") in CENSUS_SINK_STATUSES or _decision_key_present(
            task_ref=resolved_task,
            row=row,
            artifacts=artifacts,
            decision_exists=_decision_exists,
        ):
            repair = None
        applied = False
        if repair is not None:
            if applied_slots >= batch:
                deferred_repairs += 1
                # Observe extend may have run before this arm. Re-persist
                # poison-backoff after that writer so a deferred planned
                # repair cannot leave the map empty (typed skip identity).
                if _skip_reason(skip_map.get(lane_id)) == SKIP_REASON_BACKOFF:
                    skip_map[lane_id] = _serialize_backoff_key(row)
            else:
                applied_slots += 1
                if apply:
                    applied = _apply_repair(
                        task_ref=resolved_task,
                        lane_id=lane_id,
                        dispatch_id=dispatch_id,
                        kind=verdict.kind,
                        repair=repair,
                        row=row,
                        evidence=evidence,
                        update_row=update_fn,
                        record_decision=decision_fn,
                        decision_exists=_decision_exists,
                    )
                    if not applied:
                        # Failed mutation is not a sink. Persist backoff so
                        # the next cheap_repair prefix can move past this head
                        # without requiring a successful write (Latency:
                        # throttle the producer; Release It!: bulkhead).
                        skip_map[lane_id] = _serialize_backoff_key(row)
        verdicts.append(
            LaneCensusVerdict(
                lane_id=lane_id,
                dispatch_id=dispatch_id,
                kind=verdict.kind,
                evidence=evidence,
                repair=repair,
                applied=applied,
            )
        )

    if apply:
        _save_observe_skip(workspace, resolved_task, skip_map, live_ids=live_ids)
    remaining_repairs += deferred_repairs
    truncated = remaining_repairs > 0 or len(ordered) > len(prepared)
    counts = {kind: 0 for kind in VERDICT_KINDS}
    counts["repairs_applied"] = 0
    counts["remaining_repairs"] = remaining_repairs
    for item in verdicts:
        counts[item.kind] = counts.get(item.kind, 0) + 1
        if item.applied:
            counts["repairs_applied"] += 1

    def _emit_census_gauge(
        *,
        phase: str,
        reclaimed: int,
        reaper_ran: bool,
        skipped: int = 0,
        lock_held: int = 0,
        failed: int = 0,
        skip_reason: str | None = None,
    ) -> None:
        try:
            log_fn(
                "INFO",
                "lane_census_gauge",
                phase=phase,
                reclaimed=reclaimed,
                reaper_ran=reaper_ran,
                skipped=skipped,
                lock_held=lock_held,
                failed=failed,
                skip_reason=skip_reason,
                remaining_repairs=remaining_repairs,
                repairs_applied=counts.get("repairs_applied", 0),
                truncated=truncated,
            )
        except Exception as gauge_exc:  # noqa: BLE001 — gauge must not fail the census
            try:
                log_fn(
                    "WARN",
                    "lane_census_gauge_failed",
                    error=_cap_reap_error(gauge_exc),
                    phase=phase,
                )
            except Exception:  # noqa: BLE001 — gauge-failure signal must never raise or collide with reap_failed
                pass

    _emit_census_gauge(phase="pre_reclaim", reclaimed=0, reaper_ran=False)
    reclaimed = 0
    reaper_ran = False
    skipped = 0
    lock_held = 0
    failed = 0
    skip_reason: str | None = None
    if apply:
        lock_handle: Any | None = None
        try:
            lock_handle = _try_acquire_remote_sandbox_reap_lock(workspace)
        except OSError as lock_exc:
            # PermissionError / UnsupportedOperation / ENOTSUP: local fault,
            # not contention. Do not retry as if a peer held the lock.
            try:
                log_fn(
                    "WARN",
                    "remote_sandbox_reap_lock_fault",
                    error=_cap_reap_error(lock_exc),
                )
            except Exception:  # noqa: BLE001 — lock fault notice must not fail census
                pass
            skip_reason = "local_lock_fault"
        else:
            if lock_handle is None:
                try:
                    log_fn("WARN", "remote_sandbox_reap_lock_held")
                except Exception:  # noqa: BLE001 — lock notice must not fail census
                    pass
                skip_reason = "local_lock_held"
            else:
                try:
                    try:
                        result = reap_fn(resolved_task, rows=raw_rows, root=workspace)
                        if not _is_remote_sandbox_reap_result(result):
                            reaper_ran = True
                            try:
                                log_fn(
                                    "WARN",
                                    "remote_sandbox_reap_bad_shape",
                                    result_type=type(result).__name__,
                                )
                            except Exception:  # noqa: BLE001 — shape warn must not become reap_failed
                                pass
                        else:
                            reaper_ran = True
                            reclaimed = len(_remote_sandbox_reaped_keys(result))
                            gauge = _remote_sandbox_reap_gauge_fields(result)
                            skipped = int(gauge["skipped"])
                            lock_held = int(gauge["lock_held"])
                            failed = int(gauge["failed"])
                            skip_reason = gauge.get("skip_reason")
                            try:
                                fields = _remote_sandbox_reap_telemetry_fields(result)
                                failed_raw = fields.get("failed") or []
                                if isinstance(failed_raw, list) and failed_raw:
                                    log_fn(
                                        "WARN",
                                        "remote_sandbox_reap_rows_failed",
                                        failed=failed_raw,
                                        failed_total=int(fields.get("failed_total") or len(failed_raw)),
                                    )
                                log_fn("INFO", "remote_sandbox_reap", **fields)
                            except Exception as tel_exc:  # noqa: BLE001 — telemetry never becomes reap_failed
                                try:
                                    log_fn(
                                        "WARN",
                                        "remote_sandbox_reap_telemetry_failed",
                                        error=_cap_reap_error(tel_exc),
                                        reaped=reclaimed,
                                    )
                                except Exception:  # noqa: BLE001 — telemetry warn must never raise
                                    pass
                    except Exception as exc:  # noqa: BLE001 — reaper never fails the census
                        try:
                            log_fn(
                                "WARN",
                                "remote_sandbox_reap_failed",
                                error=_cap_reap_error(exc),
                            )
                        except Exception:  # noqa: BLE001 — reap-failure signal must never raise
                            pass
                finally:
                    _release_remote_sandbox_reap_lock(lock_handle)
    _emit_census_gauge(
        phase="post_reclaim",
        reclaimed=reclaimed,
        reaper_ran=reaper_ran,
        skipped=skipped,
        lock_held=lock_held,
        failed=failed,
        skip_reason=skip_reason,
    )
    if reaper_ran:
        counts["sandboxes_reclaimed"] = reclaimed
    return CensusReport(
        verdicts=verdicts,
        counts=counts,
        truncated=truncated,
        remaining_repairs=remaining_repairs,
    )


def _default_census_log(level: str, event: str, **kwargs: Any) -> None:
    log_level = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logger.log(log_level, "%s %s", event, kwargs)


def _cap_reap_error(exc: BaseException | str) -> str:
    text = str(exc)
    if len(text) <= REAP_ERROR_CAP:
        return text
    return f"{text[:REAP_ERROR_CAP]}…"


def _is_remote_sandbox_reap_result(result: Any) -> bool:
    if isinstance(result, dict):
        return any(key in result for key in ("reaped", "failed", "verdicts"))
    if result is None or isinstance(result, (str, bytes, int, float, bool)):
        return False
    return any(hasattr(result, key) for key in ("reaped", "failed", "verdicts"))


def _remote_sandbox_reaped_keys(result: Any) -> list[str]:
    raw = result.get("reaped") if isinstance(result, dict) else getattr(result, "reaped", None)
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    try:
        return [str(item) for item in raw]
    except TypeError:
        return [str(raw)]


def _remote_sandbox_failed_list(result: Any) -> list[Any]:
    raw = result.get("failed") if isinstance(result, dict) else getattr(result, "failed", None)
    if raw:
        if isinstance(raw, str):
            return [raw]
        try:
            return list(raw)
        except TypeError:
            return [raw]
    verdicts = result.get("verdicts") if isinstance(result, dict) else getattr(result, "verdicts", None)
    failed: list[Any] = []
    for verdict in verdicts or ():
        kind = verdict.get("kind") if isinstance(verdict, Mapping) else getattr(verdict, "kind", None)
        if kind != "probe_failed":
            continue
        key = verdict.get("sandbox_key") if isinstance(verdict, Mapping) else getattr(verdict, "sandbox_key", "")
        reason = verdict.get("reason") if isinstance(verdict, Mapping) else getattr(verdict, "reason", "")
        failed.append(key or reason or "probe_failed")
    if failed:
        return failed
    probe_error = result.get("probe_error") if isinstance(result, dict) else getattr(result, "probe_error", None)
    if probe_error:
        return [str(probe_error)]
    return []


def _remote_sandbox_verdicts(result: Any) -> list[Any]:
    raw = result.get("verdicts") if isinstance(result, dict) else getattr(result, "verdicts", None)
    if not raw:
        return []
    try:
        return list(raw)
    except TypeError:
        return []


def _remote_sandbox_reap_gauge_fields(result: Any) -> dict[str, Any]:
    """Cause-separated post-reclaim counts. Zero-reclaimed is not a sink."""
    skipped_kinds = frozenset({"sandbox_live", "sandbox_unharvested", "sandbox_unmapped"})
    lock_held = 0
    skipped = 0
    for verdict in _remote_sandbox_verdicts(result):
        kind = verdict.get("kind") if isinstance(verdict, Mapping) else getattr(verdict, "kind", None)
        if kind == "lock_held":
            lock_held += 1
        elif kind in skipped_kinds:
            skipped += 1
    failed = _remote_sandbox_failed_list(result)
    skipped_flag = result.get("skipped") if isinstance(result, dict) else getattr(result, "skipped", False)
    skip_reason = result.get("skip_reason") if isinstance(result, dict) else getattr(result, "skip_reason", None)
    if skipped_flag:
        skipped = max(skipped, 1)
    return {
        "skipped": skipped,
        "lock_held": lock_held,
        "failed": len(failed),
        "skip_reason": skip_reason,
    }


def _remote_sandbox_reap_telemetry_fields(result: Any) -> dict[str, Any]:
    """Build the acting-reaper telemetry payload. May raise; callers wrap separately."""
    reaped = _remote_sandbox_reaped_keys(result)
    failed = _remote_sandbox_failed_list(result)
    probe_error = result.get("probe_error") if isinstance(result, dict) else getattr(result, "probe_error", None)
    dry_run = result.get("dry_run") if isinstance(result, dict) else getattr(result, "dry_run", False)
    return {
        "reaped": len(reaped),
        "reaped_keys": reaped[:REAP_LIST_CAP],
        "failed": failed[:REAP_LIST_CAP],
        "failed_total": len(failed),
        "probe_error": None if probe_error is None else _cap_reap_error(probe_error),
        "dry_run": bool(dry_run),
    }


def _try_acquire_remote_sandbox_reap_lock(root: Path) -> Any | None:
    """Non-blocking exclusive lock for this workspace checkout.

    None means a peer in THIS checkout holds it (no-op, do not raise).
    The file lives under the local ``.task-state`` directory, so it cannot
    serialise a check-then-act against a shared remote host: two machines
    or two worktrees take two different locks and both proceed. Remote-side
    exclusion is the generated script's ``$ROOT/.reap.lock``; occupancy
    write + materialize coverage of that mutex is a later slice. OSError
    from open/lock is a local fault, not contention — callers must not
    retry it as if a peer held the lock. Only ``BlockingIOError`` is
    contention (same distinction as ``session_heartbeat.py``).
    """
    path = Path(root) / ".task-state" / REMOTE_SANDBOX_REAP_LOCK_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            handle.close()
        except OSError:
            pass
        return None
    except OSError:
        try:
            handle.close()
        except OSError:
            pass
        raise
    return handle


def _release_remote_sandbox_reap_lock(handle: Any) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        handle.close()
    except OSError:
        pass


def _default_sandbox_ssh_runner(
    script: str,
    *,
    timeout: float | None = None,
    workspace_root: Path | str | None = None,
) -> subprocess.CompletedProcess[str]:
    host = str(_resolve_remote_gate_host_for_probe(workspace_root=workspace_root) or "").strip()
    if not host:
        raise RuntimeError("remote_sandbox_reap_unconfigured: remote gate host is unset")
    if _remote_host_is_malformed(host):
        raise RuntimeError(f"remote_sandbox_reap_host_refused: malformed host {host!r}")
    return subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "--",
            host,
            script,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _default_reap_sandboxes(
    task_ref: str,
    *,
    rows: Sequence[Mapping[str, Any] | Any],
    root: Path | str,
    **_: Any,
) -> Any:
    from workbay_orchestrator_mcp.orchestration.remote_sandbox_reap import (  # noqa: PLC0415
        _exclusion_keys_for_rows,
        reap_remote_sandboxes,
    )

    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("default remote sandbox reaper is forbidden under pytest")
    listed = rows
    try:
        listed = _default_list_rows(task_ref, include_terminal=True) or rows
    except Exception:  # noqa: BLE001 — fall back to the census row set
        listed = rows

    def _runner(script: str, *, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
        return _default_sandbox_ssh_runner(script, timeout=timeout, workspace_root=root)

    return reap_remote_sandboxes(
        task_ref,
        ssh_runner=_runner,
        primary_repo=Path(root),
        rows=listed,
        exclude_keys=_exclusion_keys_for_rows(listed),
    )


def _cheap_may_need_repair(
    row: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    *,
    now: float,
) -> bool:
    """True when local evidence can still become a mutating repair (no SSH)."""
    status = str(row.get("status") or "")
    if status in TERMINAL_STATUSES or status in CENSUS_SINK_STATUSES:
        return False
    if _cheap_has_completion_evidence(artifacts):
        return True
    started_at = _optional_epoch(artifacts.get("started_at"))
    if started_at is None:
        return True
    return (now - started_at) > (backend_cap_s(row.get("backend")) + GRACE_S)


def _cheap_has_completion_evidence(artifacts: Mapping[str, Any]) -> bool:
    """Local result or work product: repair-eligible without waiting for stale clock."""
    if bool(artifacts.get("result_present")):
        return True
    if _as_int(artifacts.get("turn_patch_size"), default=0) > 0:
        return True
    tool_call_count = artifacts.get("tool_call_count")
    if tool_call_count is None:
        return False
    return _as_int(tool_call_count, default=0) > 0


def _census_window_path(root: Path, task_ref: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in task_ref) or "task"
    return Path(root) / ".task-state" / f"{CENSUS_WINDOW_FILENAME_PREFIX}{safe}.json"


def _serialize_sort_key(row: Mapping[str, Any]) -> list[Any]:
    updated, lane_id = _row_sort_key(row)
    return [updated, lane_id]


def _serialize_backoff_key(row: Mapping[str, Any]) -> list[Any]:
    return [*_serialize_sort_key(row), SKIP_REASON_BACKOFF]


def _skip_reason(stored: list[Any] | None) -> str | None:
    if not isinstance(stored, list) or len(stored) < 2:
        return None
    if len(stored) >= 3 and str(stored[2] or "").strip() == SKIP_REASON_BACKOFF:
        return SKIP_REASON_BACKOFF
    return SKIP_REASON_OBSERVE


def _observe_skip_matches(row: Mapping[str, Any], skip_map: Mapping[str, list[Any]]) -> bool:
    lane_id = str(row.get("lane_id") or "").strip()
    stored = skip_map.get(lane_id)
    if not lane_id or not isinstance(stored, list) or len(stored) < 2:
        return False
    current = _serialize_sort_key(row)
    try:
        return [float(stored[0]), str(stored[1])] == [float(current[0]), str(current[1])]
    except (TypeError, ValueError):
        return False


def _load_observe_skip(root: Path, task_ref: str) -> dict[str, list[Any]]:
    path = _census_window_path(root, task_ref)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    if str(raw.get("task_ref") or "") != str(task_ref):
        return {}
    payload = raw.get("observe_after_probe")
    if not isinstance(payload, dict):
        return {}
    loaded: dict[str, list[Any]] = {}
    for key, value in payload.items():
        lane_id = str(key or "").strip()
        if not lane_id or not isinstance(value, list) or len(value) < 2:
            continue
        entry: list[Any] = [value[0], value[1]]
        if len(value) >= 3:
            reason = str(value[2] or "").strip()
            if reason:
                entry.append(reason)
        loaded[lane_id] = entry
    return loaded


def _save_observe_skip(
    root: Path,
    task_ref: str,
    skip_map: Mapping[str, list[Any]],
    *,
    live_ids: set[str],
) -> None:
    pruned: dict[str, list[Any]] = {}
    for lane_id, key in skip_map.items():
        if lane_id not in live_ids or not isinstance(key, list) or len(key) < 2:
            continue
        stored: list[Any] = [key[0], key[1]]
        if len(key) >= 3:
            reason = str(key[2] or "").strip()
            if reason:
                stored.append(reason)
        pruned[lane_id] = stored
    path = _census_window_path(root, task_ref)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"task_ref": task_ref, "observe_after_probe": pruned},
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )
    except OSError:
        return


def _extend_observe_skip(
    skip_map: Mapping[str, list[Any]],
    scanned: Sequence[PreparedRow],
    *,
    now: float,
    task_ref: str,
    decision_exists: Callable[[str], bool] | None,
) -> dict[str, list[Any]]:
    """Record probed observe-after-probe rows so cheap_repair and cheap_observe windows move.

    Poison-backoff entries are a distinct skip identity and are never popped
    just because this scan now plans a mutation.
    """
    updated = dict(skip_map)
    _running_ids, superseded_ids = _running_and_superseded(scanned, now=now)
    for row, artifacts, probe_result in scanned:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id or isinstance(probe_result, _ProbeFailed):
            continue
        repair = _planned_repair(
            row=row,
            artifacts=artifacts,
            probe_result=probe_result,
            superseded=lane_id in superseded_ids,
            now=now,
            task_ref=task_ref,
            decision_exists=decision_exists,
        )
        if repair is not None:
            if _skip_reason(updated.get(lane_id)) != SKIP_REASON_BACKOFF:
                updated.pop(lane_id, None)
            continue
        if _skip_reason(updated.get(lane_id)) == SKIP_REASON_BACKOFF:
            continue
        updated[lane_id] = _serialize_sort_key(row)
    return updated


def _decision_key_present(
    *,
    task_ref: str,
    row: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    decision_exists: Callable[[str], bool] | None,
) -> bool:
    """Observe-only when the census receipt for this (lane, dispatch) already exists."""
    if decision_exists is None:
        return False
    lane_id = str(row.get("lane_id") or "").strip()
    dispatch_id = str(row.get("dispatch_id") or artifacts.get("dispatch_id") or "").strip()
    if not lane_id or not dispatch_id or not str(task_ref or "").strip():
        return False
    try:
        return bool(decision_exists(repair_decision_id(task_ref, lane_id, dispatch_id)))
    except Exception:  # noqa: BLE001 — peek fail-closed: still repair-eligible
        return False


def _planned_repair(
    *,
    row: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    probe_result: _ProbeOk | _ProbeFailed,
    superseded: bool,
    now: float,
    task_ref: str = "",
    decision_exists: Callable[[str], bool] | None = None,
) -> str | None:
    if str(row.get("status") or "") in CENSUS_SINK_STATUSES:
        return None
    if _decision_key_present(
        task_ref=task_ref,
        row=row,
        artifacts=artifacts,
        decision_exists=decision_exists,
    ):
        return None
    verdict = _classify(
        row=row,
        artifacts=artifacts,
        probe_result=probe_result,
        superseded=superseded,
        now=now,
    )
    return REPAIR_FOR_KIND.get(verdict.kind)


def _running_and_superseded(
    prepared: Sequence[PreparedRow],
    *,
    now: float,
) -> tuple[set[str], set[str]]:
    running_ids = {
        str(row.get("lane_id") or "")
        for row, artifacts, probe_result in prepared
        if isinstance(probe_result, _ProbeOk)
        and _probe_shows_running(row=row, artifacts=artifacts, probe_result=probe_result, now=now)
    }
    superseded_ids = _superseded_lane_ids(
        [row for row, _artifacts, _probe in prepared],
        protected_ids=running_ids,
    )
    return running_ids, superseded_ids


def _count_planned_repairs(
    prepared: Sequence[PreparedRow],
    *,
    now: float,
    task_ref: str = "",
    decision_exists: Callable[[str], bool] | None = None,
) -> int:
    _running_ids, superseded_ids = _running_and_superseded(prepared, now=now)
    count = 0
    for row, artifacts, probe_result in prepared:
        repair = _planned_repair(
            row=row,
            artifacts=artifacts,
            probe_result=probe_result,
            superseded=str(row.get("lane_id") or "") in superseded_ids,
            now=now,
            task_ref=task_ref,
            decision_exists=decision_exists,
        )
        if repair is not None:
            count += 1
    return count


def _split_repair_and_observe(
    prepared: Sequence[PreparedRow],
    *,
    now: float,
    task_ref: str = "",
    decision_exists: Callable[[str], bool] | None = None,
) -> tuple[list[PreparedRow], list[PreparedRow]]:
    _running_ids, superseded_ids = _running_and_superseded(prepared, now=now)
    repair_prepared: list[PreparedRow] = []
    observe_prepared: list[PreparedRow] = []
    for row, artifacts, probe_result in prepared:
        repair = _planned_repair(
            row=row,
            artifacts=artifacts,
            probe_result=probe_result,
            superseded=str(row.get("lane_id") or "") in superseded_ids,
            now=now,
            task_ref=task_ref,
            decision_exists=decision_exists,
        )
        if repair is not None:
            repair_prepared.append((row, artifacts, probe_result))
        else:
            observe_prepared.append((row, artifacts, probe_result))
    return repair_prepared, observe_prepared


def _classify(
    *,
    row: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    probe_result: _ProbeOk | _ProbeFailed,
    superseded: bool,
    now: float,
) -> LaneCensusVerdict:
    lane_id = str(row.get("lane_id") or "")
    dispatch_id = str(row.get("dispatch_id") or artifacts.get("dispatch_id") or "")
    status = str(row.get("status") or "")
    if isinstance(probe_result, _ProbeFailed):
        return LaneCensusVerdict(
            lane_id=lane_id,
            dispatch_id=dispatch_id,
            kind=VERDICT_PROBE_FAILED,
            evidence={"error": probe_result.error},
            repair=None,
            applied=False,
        )

    marker_mtime = probe_result.marker_mtime
    live_process = bool(probe_result.live_process)
    commit_count = int(probe_result.commit_count)
    dirty = bool(probe_result.dirty)
    started_at = _optional_epoch(artifacts.get("started_at"))
    result_present = bool(artifacts.get("result_present"))
    tool_call_count = artifacts.get("tool_call_count")
    cap_s = backend_cap_s(row.get("backend"))
    cap_plus_grace = cap_s + GRACE_S
    marker_age: float | None = None if marker_mtime is None else max(0.0, now - float(marker_mtime))
    started_within = started_at is not None and (now - started_at) <= cap_plus_grace
    started_stale = started_at is not None and (now - started_at) > cap_plus_grace
    fresh_marker = marker_mtime is not None and marker_age is not None and marker_age <= cap_s
    marker_stale = marker_mtime is None or (marker_age is not None and marker_age > cap_s)
    sandbox_present = marker_mtime is not None
    running = live_process or fresh_marker
    evidence = {
        "live_process": live_process,
        "commit_count": commit_count,
        "dirty": dirty,
        "marker_mtime": marker_mtime,
        "started_at": started_at,
        "started_within_cap_plus_grace": started_within,
        "fresh_marker": fresh_marker,
        "tool_call_count": tool_call_count,
        "result_present": result_present,
        "sandbox_present": sandbox_present,
        "backend_cap_s": cap_s,
        "cap_plus_grace_s": cap_plus_grace,
    }

    # F1: a live process (or fresh heartbeat) is running even past cap+grace.
    # F2: superseded is considered only after liveness, never instead of it.
    if running:
        kind = VERDICT_RUNNING
    elif status == "review":
        # Already adjudicated: a later census must not reset review to planned.
        kind = VERDICT_IN_REVIEW
    elif superseded:
        kind = VERDICT_SUPERSEDED
    elif commit_count > 0 and status not in REVIEW_OR_MERGED:
        kind = VERDICT_LANDED_UNADJUDICATED
    elif dirty and not live_process and commit_count == 0:
        # Uncommitted work is occupancy, not a stale empty sandbox.
        kind = VERDICT_ORPHANED_SANDBOX
    elif result_present and tool_call_count == 0 and commit_count == 0:
        kind = VERDICT_DEGENERATE_TURN
    elif started_stale and not result_present and not live_process and marker_stale:
        kind = VERDICT_STALE_NO_RESULT
    elif sandbox_present and not live_process and status in EXECUTING_STATUSES:
        kind = VERDICT_ORPHANED_SANDBOX
    elif started_stale and not result_present and not live_process:
        kind = VERDICT_STALE_NO_RESULT
    else:
        # Fail closed: never emit a mutating repair from the catch-all.
        kind = VERDICT_ORPHANED_SANDBOX

    return LaneCensusVerdict(
        lane_id=lane_id,
        dispatch_id=dispatch_id,
        kind=kind,
        evidence=evidence,
        repair=REPAIR_FOR_KIND.get(kind),
        applied=False,
    )


def _probe_shows_running(
    *,
    row: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    probe_result: _ProbeOk,
    now: float,
) -> bool:
    _ = artifacts  # started_at is not a liveness signal past cap (F1).
    if probe_result.live_process:
        return True
    marker_mtime = probe_result.marker_mtime
    if marker_mtime is None:
        return False
    marker_age = max(0.0, now - float(marker_mtime))
    return marker_age <= backend_cap_s(row.get("backend"))


def _apply_repair(
    *,
    task_ref: str,
    lane_id: str,
    dispatch_id: str,
    kind: str,
    repair: str,
    row: Mapping[str, Any],
    evidence: dict[str, Any],
    update_row: UpdateRowFn,
    record_decision: RecordDecisionFn,
    decision_exists: Callable[[str], bool] | None = None,
) -> bool:
    decision = repair_decision_id(task_ref, lane_id, dispatch_id)
    retries = _retry_count(row.get("notes"))
    target_status, notes, blocked = _repair_target(repair, kind=kind, notes=row.get("notes"), retries=retries)
    evidence["retry_count"] = retries
    evidence["retry_exhausted"] = blocked
    evidence["target_status"] = target_status
    rationale = json.dumps(
        {
            "kind": kind,
            "repair": repair,
            "target_status": target_status,
            "evidence": evidence,
        },
        sort_keys=True,
        default=str,
    )
    # Peek the receipt without writing it. The key is recorded only after the
    # row mutation succeeds (RES-01). A failed peek must not look like "no key"
    # (fail-closed: do not mutate, surface peek_error).
    try:
        if decision_exists is not None and decision_exists(decision):
            return False
    except Exception as exc:  # noqa: BLE001 — peek fail-closed, do not mutate
        evidence["peek_error"] = f"{type(exc).__name__}: {exc}"
        return False
    try:
        updated = update_row(
            lane_id,
            status=target_status,
            notes=notes,
            task_ref=task_ref,
        )
    except Exception as exc:  # noqa: BLE001 — per-row fail-closed
        evidence["update_error"] = f"{type(exc).__name__}: {exc}"
        return False
    if _envelope_failed(updated) and not _row_write_committed(updated):
        evidence["update_error"] = _envelope_error(updated)
        return False
    if _envelope_failed(updated):
        evidence["error_type"] = _envelope_error_type(updated)
        evidence["side_effect_error"] = _envelope_error(updated)
    try:
        recorded = record_decision(
            decision=decision,
            session=CENSUS_SESSION,
            task_ref=task_ref,
            rationale=rationale,
            lane_id=lane_id,
        )
    except Exception as exc:  # noqa: BLE001 — receipt fail-closed, keep the batch moving
        evidence["decision_error"] = f"{type(exc).__name__}: {exc}"
        return False
    if _envelope_failed(recorded):
        evidence["decision_error"] = _envelope_error(recorded)
        return False
    return True


def _repair_target(
    repair: str,
    *,
    kind: str,
    notes: Any,
    retries: int,
) -> tuple[str, str, bool]:
    current_notes = str(notes or "")
    if repair == REPAIR_PROMOTE_TO_REVIEW:
        return "review", _append_reason(current_notes, f"census:{kind}:{repair}"), False
    if repair == REPAIR_CLOSE_SUPERSEDED:
        return "closed", _append_reason(current_notes, f"census:{kind}:{repair}"), False
    # reset_to_planned, with retry budget then blocked.
    if retries >= RETRY_BUDGET:
        blocked_notes = _with_retries(
            _append_reason(current_notes, f"census:{kind}:blocked retry budget {RETRY_BUDGET} exhausted"),
            retries,
        )
        return "blocked", blocked_notes, True
    next_retries = retries + 1
    planned_notes = _with_retries(
        _append_reason(current_notes, f"census:{kind}:{repair}"),
        next_retries,
    )
    return "planned", planned_notes, False


def _retry_count(notes: Any) -> int:
    match = RETRY_NOTES_RE.search(str(notes or ""))
    if match is None:
        return 0
    try:
        return max(0, int(match.group(1)))
    except ValueError:
        return 0


def _with_retries(notes: str, retries: int) -> str:
    token = f"census_retries={retries}"
    if RETRY_NOTES_RE.search(notes):
        return RETRY_NOTES_RE.sub(token, notes, count=1)
    return _append_reason(notes, token)


def _append_reason(notes: str, reason: str) -> str:
    text = notes.strip()
    if not text:
        return reason
    if reason in text:
        return text
    return f"{text}\n{reason}"


def _decision_is_noop(result: Any) -> bool:
    if _envelope_failed(result):
        return False
    if not isinstance(result, dict):
        return False
    mutation = result.get("mutation")
    if isinstance(mutation, dict) and mutation.get("operation") == "noop":
        return True
    data = result.get("data")
    if isinstance(data, dict) and data.get("idempotent") is True:
        return True
    return False


def _envelope_failed(result: Any) -> bool:
    return isinstance(result, dict) and result.get("ok") is False


def _envelope_error(result: Any) -> str:
    if not isinstance(result, dict):
        return "envelope ok=false"
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        return str(data["error"])
    if result.get("error"):
        return str(result["error"])
    return "envelope ok=false"


def _envelope_error_type(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    data = result.get("data")
    if isinstance(data, dict) and data.get("error_type"):
        return str(data["error_type"])
    raw = result.get("error_type")
    if raw:
        return str(raw)
    return None


def _row_write_committed(result: Any) -> bool:
    """True when the lane-row mutation landed, including typed side-effect failure."""
    if not isinstance(result, dict):
        return not _envelope_failed(result)
    if result.get("ok") is not False:
        return True
    return _envelope_error_type(result) == COMMITTED_SIDE_EFFECT_ERROR_TYPE


def _invoke_probe(probe: ProbeFn, lane: Mapping[str, Any]) -> _ProbeOk | _ProbeFailed:
    holder: dict[str, Any] = {}

    def _run() -> None:
        try:
            holder["result"] = probe(lane)
        except BaseException as exc:  # noqa: BLE001 — fail-closed per lane
            holder["error"] = exc

    worker = threading.Thread(target=_run, name=f"lane-census-probe-{lane.get('lane_id', 'lane')}", daemon=True)
    worker.start()
    worker.join(PROBE_TIMEOUT_S)
    if worker.is_alive():
        return _ProbeFailed(error=f"TimeoutError: probe timed out after {PROBE_TIMEOUT_S:g}s")
    if "error" in holder:
        exc = holder["error"]
        return _ProbeFailed(error=f"{type(exc).__name__}: {exc}")
    return _normalize_probe(holder.get("result"))


def _normalize_probe(raw: Any) -> _ProbeOk | _ProbeFailed:
    if isinstance(raw, _ProbeOk):
        return raw
    if isinstance(raw, _ProbeFailed):
        return raw
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and len(raw) >= 4:
        marker_mtime = _optional_epoch(raw[0])
        live_process = bool(raw[1])
        try:
            commit_count = int(raw[2] or 0)
        except (TypeError, ValueError):
            return _ProbeFailed(error="probe commit_count is not an int")
        dirty = bool(raw[3])
        return _ProbeOk(
            marker_mtime=marker_mtime,
            live_process=live_process,
            commit_count=max(0, commit_count),
            dirty=dirty,
        )
    return _ProbeFailed(error=f"probe returned unsupported result: {type(raw).__name__}")


def _superseded_lane_ids(
    rows: Sequence[Mapping[str, Any]],
    *,
    protected_ids: set[str] | None = None,
) -> set[str]:
    """Older duplicates of (branch, tip SHA) in *this* batch, excluding live rows."""
    protected = protected_ids or set()
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        tip = str(row.get("branch_tip_sha") or "").strip()
        branch = str(row.get("branch") or "").strip()
        if not tip or not branch:
            continue
        groups.setdefault((branch, tip), []).append(row)
    superseded: set[str] = set()
    for members in groups.values():
        if len(members) < 2:
            continue
        keeper = max(members, key=_row_sort_key)
        keeper_id = str(keeper.get("lane_id") or "")
        for member in members:
            member_id = str(member.get("lane_id") or "")
            if member_id and member_id != keeper_id and member_id not in protected:
                superseded.add(member_id)
    return superseded


def _row_sort_key(row: Mapping[str, Any]) -> tuple[float, str]:
    updated = _optional_epoch(row.get("updated_at"))
    return (updated if updated is not None else 0.0, str(row.get("lane_id") or ""))


def _as_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _epoch(value: Any) -> float:
    if value is None:
        return time.time()
    if callable(value):
        return _epoch(value())
    parsed = _optional_epoch(value)
    if parsed is None:
        return time.time()
    return parsed


def _optional_epoch(value: Any) -> float | None:
    if value is None or value is False:
        return None
    if isinstance(value, datetime):
        stamp = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return stamp.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            pass
        iso = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(iso)
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    return None


def sandbox_slug(branch: str) -> str:
    """Mirror remote_agent.sh LANE_KEY: sanitized branch + 8-hex sha256."""
    raw = str(branch or "")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    sanitized = re.sub(r"[^A-Za-z0-9-]", "-", raw)[:40].lstrip("-") or "lane"
    return f"{sanitized}-{digest}"


def _unwrap_list_page(envelope: Any) -> dict[str, Any]:
    """Fail closed on ok:false or an unusable page; never coerce to an empty success."""
    if not isinstance(envelope, dict):
        raise CensusListError(f"unusable_page_shape: list_lanes returned {type(envelope).__name__}")
    if envelope.get("ok") is False:
        raise CensusListError(_envelope_error(envelope))
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    if not isinstance(data, dict):
        raise CensusListError("unusable_page_shape: missing page data")
    if not isinstance(data.get("lanes"), list):
        raise CensusListError("unusable_page_shape: lanes is not a list")
    return data


def _default_list_rows(task_ref: str, *, include_terminal: bool = False) -> list[dict[str, Any]]:
    from workbay_handoff_mcp.lanes_api import list_lanes  # noqa: PLC0415

    collected: list[dict[str, Any]] = []
    after_id: int | None = None
    while True:
        try:
            envelope = list_lanes(task_ref=task_ref, status="all", limit=100, after_id=after_id)
        except CensusListError:
            raise
        except Exception as exc:  # noqa: BLE001 — failed list is not an empty census
            raise CensusListError(f"{type(exc).__name__}: {exc}") from exc
        data = _unwrap_list_page(envelope)
        lanes = data.get("lanes") if isinstance(data.get("lanes"), list) else []
        collected.extend(dict(item) for item in lanes if isinstance(item, Mapping))
        if not data.get("has_more"):
            break
        next_after = data.get("next_after_id")
        if not isinstance(next_after, int):
            raise CensusListError("unusable_page_shape: has_more without int next_after_id")
        after_id = next_after
    dispatches = _latest_dispatch_ids(task_ref)
    for row in collected:
        lane_id = str(row.get("lane_id") or "")
        if lane_id and not row.get("dispatch_id") and lane_id in dispatches:
            row["dispatch_id"] = dispatches[lane_id]
    if include_terminal:
        return collected
    return [row for row in collected if str(row.get("status") or "") not in TERMINAL_STATUSES]


def _latest_dispatch_ids(task_ref: str) -> dict[str, str]:
    try:
        import sqlite3  # noqa: PLC0415
        from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

        db_path = Path(get_runtime_config().db_path)
        if not db_path.is_file():
            return {}
        found: dict[str, str] = {}
        with sqlite3.connect(str(db_path)) as conn:
            rows = conn.execute(
                """
                SELECT lane_id, dispatch_id
                FROM lane_messages
                WHERE task_ref = ? AND dispatch_id IS NOT NULL AND TRIM(dispatch_id) != ''
                ORDER BY id DESC
                """,
                (task_ref,),
            ).fetchall()
        for lane_id, dispatch_id in rows:
            key = str(lane_id or "").strip()
            if key and key not in found:
                found[key] = str(dispatch_id).strip()
        return found
    except Exception:  # noqa: BLE001 — default join is best-effort
        return {}


def _default_list_dispatch_artifacts(root: Path, lane_id: str) -> dict[str, Any]:
    state_dir = Path(root) / ".task-state"
    empty = {
        "dispatch_id": None,
        "started_at": None,
        "result_present": False,
        "turn_patch_size": 0,
        "tool_call_count": None,
        "path": None,
    }
    if not state_dir.is_dir():
        return empty
    safe_lane = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in lane_id) or "lane"
    prefix = f"{STAGING_DIR_PREFIX}{safe_lane}-"
    try:
        candidates = [path for path in state_dir.iterdir() if path.is_dir() and path.name.startswith(prefix)]
    except OSError:
        return empty
    if not candidates:
        return empty
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    result_path = latest / "result.json"
    patch_path = latest / "turn.patch"
    result_present = result_path.is_file()
    turn_patch_size = patch_path.stat().st_size if patch_path.is_file() else 0
    tool_call_count = None
    started_at = latest.stat().st_mtime
    dispatch_id = None
    if result_present:
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            tool_call_count = _extract_tool_call_count(payload)
            dispatch_id = payload.get("dispatch_id")
            started_raw = payload.get("started_at") or payload.get("startedAt")
            parsed_start = _optional_epoch(started_raw)
            if parsed_start is not None:
                started_at = parsed_start
    spec_path = latest / "spec.json"
    if spec_path.is_file() and dispatch_id is None:
        try:
            spec = json.loads(spec_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            spec = None
        if isinstance(spec, dict):
            dispatch_id = spec.get("dispatch_id") or spec.get("dispatchId")
    return {
        "dispatch_id": dispatch_id,
        "started_at": started_at,
        "result_present": result_present,
        "turn_patch_size": int(turn_patch_size),
        "tool_call_count": tool_call_count,
        "path": str(latest),
    }


def _extract_tool_call_count(payload: Mapping[str, Any]) -> int | None:
    raw = payload.get("tool_call_count")
    if raw is None:
        phases = payload.get("phases")
        if isinstance(phases, Mapping):
            raw = phases.get("tool_call_count")
            agent_turn = phases.get("agent_turn")
            if raw is None and isinstance(agent_turn, Mapping):
                raw = agent_turn.get("tool_call_count")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _default_probe(lane: Mapping[str, Any]) -> tuple[float | None, bool, int, bool]:
    """One bounded SSH fact-gathering round-trip. Tests must inject a fake."""
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("default remote probe is forbidden under pytest")
    host = str(os.environ.get("WORKBAY_REMOTE_GATE_HOST") or "").strip()
    if not host:
        raise RuntimeError("remote_probe_unconfigured: WORKBAY_REMOTE_GATE_HOST is unset")
    branch = str(lane.get("branch") or lane.get("lane_id") or "")
    slug = sandbox_slug(branch)
    agent_root = str(
        os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or os.environ.get("WORKBAY_REMOTE_GATE_DIR") or "src/.workbay-agent"
    )
    live_script = live_process_probe_script(slug)
    remote = f"""
set -euo pipefail
ROOT="$HOME/{agent_root}"
SBX="$ROOT/{slug}"
MARKER="$SBX/.workbay-lane-sandbox"
if [ -f "$MARKER" ]; then echo "marker_mtime=$(stat -c %Y "$MARKER")"; else echo "marker_mtime="; fi
{live_script}
if [ -d "$SBX/.git" ]; then
  base=$(git -C "$SBX" rev-list --max-parents=0 HEAD 2>/dev/null | tail -n 1 || true)
  if [ -n "$base" ]; then
    echo "commit_count=$(git -C "$SBX" rev-list --count "$base"..HEAD 2>/dev/null || echo 0)"
  else
    echo "commit_count=0"
  fi
  if git -C "$SBX" status --porcelain 2>/dev/null | grep -q .; then echo "dirty=1"; else echo "dirty=0"; fi
else
  echo "commit_count=0"
  echo "dirty=0"
fi
"""
    proc = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            host,
            remote,
        ],
        capture_output=True,
        text=True,
        timeout=PROBE_TIMEOUT_S,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or f"ssh exit {proc.returncode}"
        raise RuntimeError(f"remote_probe_failed: {detail}")
    parsed: dict[str, str] = {}
    for line in (proc.stdout or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()
    marker_mtime = _optional_epoch(parsed.get("marker_mtime") or None)
    live_process = parsed.get("live_process") == "1"
    try:
        commit_count = int(parsed.get("commit_count") or 0)
    except ValueError:
        commit_count = 0
    dirty = parsed.get("dirty") == "1"
    return marker_mtime, live_process, commit_count, dirty


def _default_update_row(lane_id: str, *, status: str, notes: str | None = None, task_ref: str, **_: Any) -> Any:
    if status in {"closed", "merged"}:
        from workbay_handoff_mcp.lanes_api import close_lane  # noqa: PLC0415

        result = close_lane(lane_id=lane_id, status=status, notes=notes, task_ref=task_ref)
    else:
        from workbay_handoff_mcp.lanes_api import update_lane  # noqa: PLC0415

        result = update_lane(lane_id=lane_id, status=status, notes=notes, task_ref=task_ref)
    return result


def _default_decision_exists(decision: str, *, task_ref: str) -> bool:
    try:
        import sqlite3  # noqa: PLC0415
        from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

        db_path = Path(get_runtime_config().db_path)
        if not db_path.is_file():
            return False
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                """
                SELECT 1 FROM decisions
                WHERE task_ref = ? AND decision = ? AND session = ?
                LIMIT 1
                """,
                (task_ref, decision, CENSUS_SESSION),
            ).fetchone()
        return row is not None
    except Exception as exc:  # noqa: BLE001 — peek must not look like key-absent
        raise RuntimeError(f"{type(exc).__name__}: {exc}") from exc


def _default_record_decision(
    *,
    decision: str,
    session: str,
    task_ref: str,
    rationale: str | None = None,
    lane_id: str | None = None,
    **_: Any,
) -> Any:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415
    from workbay_handoff_mcp.api import WriteActorInput  # noqa: PLC0415

    actor = WriteActorInput(lane_id=lane_id) if lane_id else None
    return record_decision(
        session=session,
        decision=decision,
        rationale=rationale,
        actor=actor,
        task_ref=task_ref,
        decision_origin="system",
    )
