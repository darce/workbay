"""Disposable OBS-07 plan projection. Only the collector performs I/O.

Manifests, cursor/lane rows and managed job intents remain authoritative. The
pure adapters deliberately retain disagreements rather than choosing a winner.
"""

from __future__ import annotations

import copy
import json
import math
import os
import re
import stat
import time
from datetime import datetime, timezone
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.shared_schema import bounded_handoff_read

from .lane_lifecycle_contracts import JOB_ID_RE, job_id_for

_SPEC_FIELDS = (
    "schema_version",
    "job_id",
    "task_ref",
    "lane_id",
    "pass_id",
    "attempt",
    "backend",
    "lane_key",
    "created_at",
    "remote_exec_mode",
)
_CURSOR_FIELDS = (
    "id",
    "task_ref",
    "plan_item_id",
    "lane_id",
    "state",
    "summary",
    "dispatch_count",
    "dispatched_at",
    "completed_at",
    "created_at",
    "updated_at",
    "source_heading",
)
_LANE_FIELDS = ("id", "task_ref", "lane_id", "status", "backend", "model", "created_at", "updated_at")
_OPEN_STAGES = frozenset({"intent", "submitted"})
_CLOSED_STAGES = frozenset({"done", "completed", "dead_letter"})
_SAFE_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_MODE_FIELDS = frozenset({"mode", "backend_id", "source", "repo_root", "config_identity"})


def _iso(now):
    return datetime.fromtimestamp(now, timezone.utc).isoformat() if now is not None else None


def _stamp(value):
    from .fleet_snapshot import _timestamp

    return _timestamp(value)


def _source(state="fresh", reason=None, *, now, complete=True, **fields):
    return dict(state=state, reason=reason, complete=complete, observed_at=_iso(now), read_at=_iso(now), **fields)


def _good(source):
    return source.get("state") in ("fresh", "known") and source.get("complete") is True


def empty_plan(reason="not_observed"):
    return dict(version=1, state="unknown", partial=True, limits_hit=[reason], sources=[], tasks=[])


def _mode(value):
    return value.get("mode") if isinstance(value, dict) else value


def _identity_spec(raw):
    """Retain only typed identity scalars, including inside the mode envelope."""
    spec = {}
    for field in _SPEC_FIELDS:
        value = raw.get(field)
        if field == "remote_exec_mode":
            spec[field] = (
                {
                    name: value
                    for name, value in value.items()
                    if name in _MODE_FIELDS and (value is None or isinstance(value, str))
                }
                if isinstance(value, dict)
                else None
            )
        elif field in ("attempt", "schema_version"):
            if type(value) is int:
                spec[field] = value
        elif isinstance(value, str):
            spec[field] = value
    return spec


def _safe_spec(spec):
    """Validate identity without retaining argv, prompts or environment payloads."""
    if not isinstance(spec, dict):
        return "invalid_host_spec"
    if (
        type(spec.get("schema_version")) is not int
        or spec["schema_version"] != 1
        or not isinstance(spec.get("job_id"), str)
        or not JOB_ID_RE.fullmatch(spec["job_id"])
    ):
        return "invalid_host_identity"
    if any(
        not isinstance(spec.get(k), str) or not spec[k].strip()
        for k in ("task_ref", "lane_id", "pass_id", "backend", "lane_key")
    ):
        return "invalid_host_identity"
    if not isinstance(spec.get("created_at"), str) or _stamp(spec["created_at"]) is None:
        return "invalid_host_created_at"
    key = spec["lane_key"]
    if not _SAFE_KEY.fullmatch(key) or key in (".", ".."):
        return "unsafe_lane_key"
    attempt = spec.get("attempt")
    if type(attempt) is not int or attempt < 0:
        return "invalid_attempt"
    if job_id_for(spec["task_ref"], spec["lane_id"], spec["pass_id"], attempt) != spec["job_id"]:
        return "host_job_identity_mismatch"
    if _mode(spec.get("remote_exec_mode")) != "detached":
        return "execution_mode_unsupported" if spec.get("remote_exec_mode") else "identity_unavailable"
    mode = spec["remote_exec_mode"]
    if not isinstance(mode, dict):
        return "identity_unavailable"
    if (
        any(value is not None and not isinstance(value, str) for key, value in mode.items() if key in _MODE_FIELDS)
        or mode.get("backend_id", spec["backend"]) != spec["backend"]
    ):
        return "invalid_execution_metadata"
    return None


def _vm_reason(vm, now):
    """Why this VM observation cannot support any current presence claim."""
    from .fleet_snapshot import VM_FRESH_SECONDS

    stamp = _stamp(vm.get("observed_at"))
    if now is None or stamp is None or vm.get("state") == "unknown":
        return vm.get("reason") or "inventory_unavailable"
    if stamp > now or now - stamp > VM_FRESH_SECONDS or vm.get("state") == "stale":
        return "inventory_stale"
    return None


def _facet_gap(vm, facet):
    """Why one facet cannot prove an absence; positive exact rows stay usable."""
    if facet == "jobs":
        if vm.get("jobs_state") != "known":
            return vm.get("jobs_reason") or vm.get("jobs_truncation_reason") or "jobs_unavailable"
        if vm.get("jobs_truncated"):
            return vm.get("jobs_truncation_reason") or "jobs_truncated"
        return None
    if facet == "processes":
        census = vm.get("process_census")
        if isinstance(census, dict) and census.get("unclassified_opaque", 0):
            return "processes_opaque"
        if vm.get("processes_state") != "known":
            return "processes_unavailable"
        if "job_processes_state" in vm and vm.get("job_processes_state") != "known":
            return "job_processes_unavailable"
        return None
    if vm.get("sandboxes_state") != "known":
        return "sandboxes_unavailable"
    if vm.get("truncated"):
        return vm.get("truncation_reason") or "inventory_truncated"
    return None


_UNIT_RE = re.compile(r"workbay-lane-([A-Za-z0-9][A-Za-z0-9_.-]*)-job-(j[0-9a-f]{20})\.service\Z")


def _job_lane_key(job):
    key = job.get("lane_key")
    if isinstance(key, str) and key:
        return key
    match = _UNIT_RE.fullmatch(job["unit"]) if isinstance(job.get("unit"), str) else None
    return match[1] if match and match[2] == job.get("job_id") else None


def _job_done(job):
    return job.get("done") is True or job.get("state") in ("done", "lost")


def _job_issue(job, spec, record):
    """Exact native job identity against the producer's host spec."""
    if job.get("identity_state", "known") != "known":
        return job.get("reason") or "job_identity_unavailable"
    unit = f"workbay-lane-{spec.get('lane_key')}-job-{spec.get('job_id')}.service"
    if not isinstance(job.get("unit"), str):
        return "job_identity_unavailable"
    if job["unit"] != unit or _job_lane_key(job) != spec.get("lane_key"):
        return "job_identity_mismatch"
    if any(field in job and job[field] != spec.get(field) for field in ("task_ref", "lane_id", "pass_id", "attempt")):
        return "job_identity_mismatch"
    return None


def _process_start(process, now):
    """Raw start, parsed epoch (None when invalid or in the future) and reason."""
    if "started_at" in process or "start_time" in process:
        raw = process.get("started_at", process.get("start_time"))
        stamp = _stamp(raw)
    elif "lstart" in process:
        raw = process.get("lstart")
        try:
            # The inventory probe runs ps with TZ=UTC; lstart is UTC.
            stamp = datetime.strptime(raw, "%a %b %d %H:%M:%S %Y").replace(tzinfo=timezone.utc).timestamp()
        except (ValueError, TypeError):
            stamp = None
    else:
        return None, None, "process_start_unavailable"
    if stamp is None:
        return raw, None, "invalid_process_start"
    if now is None:
        return raw, None, "reference_time_unavailable"
    if stamp > now:
        return raw, None, "future_process_start"
    return raw, stamp, None


def _worker_age(processes, job, now):
    """Age since the earliest known process start, else a labelled job start.

    Every attributed process must have a valid past start; otherwise the age
    is unknown. A job-start age is only used when no process is attributed.
    """
    if processes:
        starts = [(_process_start(p, now), p) for p in processes]
        bad = next(((raw, reason) for (raw, stamp, reason), _ in starts if stamp is None), None)
        if bad:
            return dict(started_at=bad[0], age_kind="unknown", age_seconds=None, age_reason=bad[1])
        (raw, stamp, _), _ = min(starts, key=lambda pair: (pair[0][1], pair[1].get("pid", 0)))
        return dict(started_at=_iso(stamp), age_kind="process_start", age_seconds=now - stamp, age_reason=None)
    raw = job.get("started_at")
    if raw is None:
        return dict(started_at=None, age_kind="unknown", age_seconds=None, age_reason="start_unavailable")
    stamp = _stamp(raw)
    valid = now is not None and stamp is not None and stamp <= now
    return dict(
        started_at=raw,
        age_kind="job_start" if valid else "unknown",
        age_seconds=now - stamp if valid else None,
        age_reason=None if valid else "invalid_job_start",
    )


def _host_record(record):
    """Collector rows already carry an allowlisted spec; raw ledger rows are reduced."""
    raw = record.get("spec") if isinstance(record, dict) else None
    if not isinstance(raw, dict):
        return dict(spec={}, stage=None, unit=None, reason="invalid_host_spec")
    reason = record["reason"] if "reason" in record else _safe_spec(raw)
    stage = record.get("stage")
    if stage not in _OPEN_STAGES | _CLOSED_STAGES:
        reason = reason or "host_job_stage_unavailable"
    unit = record.get("unit") if isinstance(record.get("unit"), str) else None
    return dict(spec=_identity_spec(raw), stage=stage, unit=unit, reason=reason)


def _processes(vm):
    """Sandbox members plus native job members, once per PID."""
    rows = {}
    for facet in ("processes", "job_processes"):
        for process in vm.get(facet, []) or []:
            if isinstance(process, dict) and type(process.get("pid")) is int:
                rows.setdefault(process["pid"], process)
    return [rows[pid] for pid in sorted(rows)]


def _sandbox_owner(key, jobs, jobs_gap):
    """The single native job that can own lane ``key``'s sandbox processes.

    Every observed job of that lane key is a candidate, open jobs before
    finished ones. With an incomplete jobs facet no owner can be proved.
    """
    if jobs_gap:
        return None, [], jobs_gap
    rows = [job for job in jobs if _job_lane_key(job) == key]
    candidates = [job for job in rows if not _job_done(job)] or rows
    ids = sorted({job.get("job_id") for job in candidates})
    if len(ids) == 1:
        return ids[0], ids, None
    return None, ids, "ambiguous_job_attempts" if ids else "job_not_observed"


def join_worker_presence(task_ref, lane_id, host_jobs, vm, *, now=None, source=None):
    """Pure exact job/spec/process join; one worker row per matching attempt.

    Positive exact evidence (a native job row whose unit matches the producer
    spec plus a process attributed to that job) proves a live worker even when
    unrelated rows are opaque. Absence needs every facet complete. Identity is
    never read from a branch, cwd spelling or command text.
    """
    vm = vm or {}
    now = _stamp(vm.get("observed_at")) if now is None else now
    source = source or {}
    records = [_host_record(record) for record in host_jobs]
    matching = sorted(
        (r for r in records if (r["spec"].get("task_ref"), r["spec"].get("lane_id")) == (task_ref, lane_id)),
        key=lambda r: (str(r["spec"].get("job_id")), str(r["spec"].get("attempt"))),
    )
    own_jobs = {r["spec"].get("job_id") for r in matching}
    vm_reason = _vm_reason(vm, now)
    gaps = [] if vm_reason else [g for g in (_facet_gap(vm, f) for f in ("jobs", "processes", "sandboxes")) if g]
    jobs_gap = vm_reason or _facet_gap(vm, "jobs")
    host_gap = None if not source or _good(source) else source.get("reason") or "host_jobs_unavailable"
    jobs = [] if vm_reason else [j for j in vm.get("jobs", []) or [] if isinstance(j, dict)]
    processes = [] if vm_reason else _processes(vm)
    workers, confirmed = [], False
    for record in matching:
        spec = record["spec"]
        job_id, key = spec.get("job_id"), spec.get("lane_key")
        issue = record["reason"] or vm_reason
        rows = [] if issue else [j for j in jobs if j.get("job_id") == job_id]
        job = rows[0] if len(rows) == 1 else {}
        if not issue and len(rows) > 1:
            issue = "ambiguous_job_observation"
        if not issue and job:
            issue = _job_issue(job, spec, record)
        attributed, attribution, ambiguity, sure = [], None, None, False
        if not issue:
            # Inventory native membership: cwd in, or PGID with, the job directory.
            for process in processes:
                ids = process.get("job_ids") or []
                if isinstance(ids, list) and job_id in ids:
                    attributed.append(process)
                    attribution = "job_directory"
                    if ids != [job_id]:
                        ambiguity = ambiguity or "ambiguous_process_membership"
                        sure = sure or set(ids) <= own_jobs
            # Exact sandbox membership through the producer's lane key, only for
            # processes without native membership and only for its single owner.
            members = [
                process
                for process in processes
                if not process.get("job_ids")
                and any(isinstance(p, str) and Path(p).name == key for p in process.get("sandboxes", []) or [])
            ]
            if members:
                owner, candidates, why = _sandbox_owner(key, jobs, jobs_gap)
                if job and (owner == job_id or job_id in candidates or jobs_gap):
                    attributed.extend(members)
                    attribution = attribution or "sandbox"
                    if owner != job_id:
                        # Possibly this attempt's process, but not provably.
                        ambiguity = ambiguity or why
                        sure = sure or (bool(candidates) and set(candidates) <= own_jobs)
                    if any(len(process.get("sandboxes", [])) != 1 for process in members):
                        ambiguity = ambiguity or "ambiguous_process_membership"
                elif not job and owner is None and why == "job_not_observed":
                    # A process in this lane key's sandbox with no native job.
                    issue = "job_not_observed_with_process"
        attributed.sort(key=lambda process: process["pid"])
        if issue:
            kind, reason = "unknown", issue
        elif attributed and not job:
            kind, reason = "unknown", "job_not_observed_with_process"
        elif attributed and ambiguity:
            kind, reason = "unknown", ambiguity
            confirmed = confirmed or sure
        elif attributed and attribution == "sandbox" and vm.get("processes_state") != "known":
            kind, reason = "unknown", _facet_gap(vm, "processes")
        elif attributed and (record["stage"] in _CLOSED_STAGES or _job_done(job)):
            kind, reason = "unknown", "terminal_job_with_live_process"
            confirmed = True
        elif attributed:
            kind, reason = "live", None
            confirmed = True
        elif job:
            kind, reason = "directory_only", None
        elif jobs_gap:
            kind, reason = "unknown", jobs_gap
        else:
            kind, reason = "absent", None
        # An observed directory or absence is only complete with every facet.
        quality_reason = reason or (None if kind == "live" else (gaps or [host_gap])[0])
        age = _worker_age(attributed, job, now)
        # The oldest attributed process represents the worker; PID breaks ties.
        leader = min(attributed, key=lambda p: (_process_start(p, now)[1] or math.inf, p["pid"]), default={})
        workers.append(
            dict(
                job_id=job_id,
                pass_id=spec.get("pass_id"),
                attempt=spec.get("attempt"),
                task_ref=task_ref,
                lane_id=lane_id,
                lane_key=key,
                backend=spec.get("backend"),
                host_stage=record["stage"],
                presence_kind=kind,
                attribution=attribution if attributed else None,
                pid=leader.get("pid"),
                pgid=leader.get("pgid"),
                pids=[process["pid"] for process in attributed],
                processes=copy.deepcopy(attributed),
                unit=job.get("unit"),
                job_state=job.get("state"),
                job_done=job.get("done"),
                job_started_at=job.get("started_at"),
                created_at=spec.get("created_at"),
                remote_exec_mode=copy.deepcopy(spec.get("remote_exec_mode")),
                observed_at=vm.get("observed_at"),
                **age,
                quality="unknown" if quality_reason else "known",
                reason=quality_reason,
            )
        )
    live = sorted({w["job_id"] for w in workers if w["presence_kind"] == "live"})
    if len(live) > 1:
        for worker in workers:
            if worker["job_id"] in live:
                worker.update(presence_kind="unknown", quality="unknown", reason="ambiguous_attempts")
    # Attempts the host ledger has closed cannot hide a second running worker.
    blocking = [w for w in workers if w["presence_kind"] == "unknown" and w["host_stage"] not in _CLOSED_STAGES]
    running = len(live) == 1 and not blocking and not host_gap
    absent = (
        bool(workers)
        and not vm_reason
        and not gaps
        and not host_gap
        and all(w["presence_kind"] in ("absent", "directory_only") for w in workers)
    )
    reasons = [] if running or absent else [w["reason"] for w in workers if w["reason"]]
    if not running and not absent:
        reasons += ([vm_reason] if vm_reason else []) + gaps + ([host_gap] if host_gap else [])
        if not matching:
            reasons.append("identity_unavailable")
    reasons = sorted(set(reasons))
    return dict(
        state="known" if running or absent else "unknown",
        reason=reasons[0] if reasons else None,
        reasons=reasons,
        workers=workers,
        live=running,
        absence_proved=absent,
        confirmed_live=confirmed,
        observed_at=vm.get("observed_at"),
    )


def _bind(task_ref, item, lane, cursors, items):
    """Decision 6: exact item first, else one exact same-task lane row."""
    item_rows = [r for r in cursors if r.get("task_ref") == task_ref and r.get("plan_item_id") == item]
    lane_rows = [r for r in cursors if r.get("task_ref") == task_ref and r.get("lane_id") == lane]
    if item_rows:
        # Ambiguity keeps every raw row naming this item or this lane.
        raw = item_rows + [r for r in lane_rows if r not in item_rows]
        if len(item_rows) != 1:
            return "ambiguous", raw, ["ambiguous_cursor_binding"]
        if item_rows[0].get("lane_id") not in (None, "", lane):
            return "item_id", item_rows, ["inconsistent_item_lane_identity"]
        return "item_id", item_rows, []
    if any(r.get("plan_item_id") in items for r in lane_rows):
        # Another manifest item's exact cursor names this lane: no fallback.
        return "ambiguous", lane_rows, ["item_lane_identity_conflict"]
    if len(lane_rows) == 1:
        return "lane_id", lane_rows, []
    if lane_rows:
        return "ambiguous", lane_rows, ["ambiguous_cursor_binding"]
    return "missing", [], []


def _display(node, *, cursor_source, lane_source):
    """The ordered decision-3 table; raw observations are never overwritten."""
    cursor, lane = node["cursor_state"], node["lane_status"]
    presence = node["presence"]
    inconsistent = node["inconsistency_reasons"]
    if cursor in ("completed", "skipped") and (lane in ("active", "blocked") or presence["confirmed_live"]):
        inconsistent.append("inconsistent_terminal_cursor_live_evidence")
    if inconsistent:
        return "unknown"
    if not _good(cursor_source) or not _good(lane_source):
        for source, name in ((cursor_source, "cursors"), (lane_source, "lanes")):
            if not _good(source):
                node["unknown_reasons"].append(source.get("reason") or name + "_unavailable")
        return "unknown"
    if cursor in ("completed", "skipped"):
        return "done"
    if cursor in ("escalated", "expired") or lane == "blocked":
        return "blocked"
    if cursor == "dispatched":
        if presence["live"]:
            return "running"
        if presence["absence_proved"]:
            return "waiting"
        node["unknown_reasons"].extend(presence["reasons"] or ["presence_unknown"])
        return "unknown"
    if cursor is None and lane in (None, "planned") and not node["workers"]:
        return "waiting"
    node["unknown_reasons"].append("unmapped_evidence")
    return "unknown"


def project_plan(
    manifests, cursors=(), lanes=(), *, tasks=(), host_jobs=(), vm=None, sources=None, now=None, limits_hit=()
):
    """Pure deterministic projection of complete or explicitly partial inputs.

    ``manifests`` maps task_ref to {manifest, source, path, reason}; source is
    the shared S1 digest status. ``sources`` are named observations (a mapping
    or a list of rows with ``name``); the result lists them sorted by name.
    """
    vm = vm or {}
    now = _stamp(vm.get("observed_at")) if now is None else now
    if isinstance(sources, (list, tuple)):
        sources = {row["name"]: {k: v for k, v in row.items() if k != "name"} for row in sources}
    sources = copy.deepcopy(sources or {})
    identities = set(manifests)
    for row in [*tasks, *cursors, *lanes]:
        ref = row if isinstance(row, str) else row.get("task_ref")
        if isinstance(ref, str) and ref:
            identities.add(ref)
    from .fleet_snapshot import MAX_FILES

    limits = set(limits_hit)
    if len(identities) > MAX_FILES:
        limits.add("tasks_limit")
    result = dict(
        version=1,
        state="fresh",
        partial=False,
        limits_hit=[],
        sources=[dict(name=name, **sources[name]) for name in sorted(sources)],
        observed_at=_iso(now),
        tasks=[],
    )
    cursor_source = sources.get("cursors", {})
    lane_source = sources.get("lanes", {})
    for ref in sorted(identities)[:MAX_FILES]:
        entry = manifests.get(ref, {})
        manifest = entry.get("manifest")
        task = dict(
            task_ref=ref,
            graph_state="no_plan_graph",
            reason="manifest_missing",
            manifest_path=entry.get("path"),
            plan_source=None,
            source=entry.get("source"),
            nodes=[],
            edges=[],
            conflict_edges=[],
            merge_order=[],
        )
        result["tasks"].append(task)
        if manifest is None:
            if entry.get("reason"):
                task.update(graph_state="unknown", reason=entry["reason"])
            elif not _good(sources.get("manifests", {})):
                task.update(
                    graph_state="unknown", reason=sources.get("manifests", {}).get("reason") or "manifests_unavailable"
                )
            continue
        task.update(
            graph_state="available",
            reason=None,
            plan_source=copy.deepcopy(manifest.get("plan_source")),
            source=copy.deepcopy(
                entry.get("source") or {"state": "untracked", "reason": "manifest has no plan_source"}
            ),
            merge_order=list(manifest.get("merge_order", [])),
        )
        declared = manifest.get("lanes", {})
        item_by_lane = {lane: row.get("plan_item_id") or lane for lane, row in declared.items()}
        by_item = {}
        for lane, declared_row in sorted(declared.items(), key=lambda p: (item_by_lane[p[0]], p[0])):
            item = item_by_lane[lane]
            node_id = f"plan:{ref}:{item}"
            method, bound, conflicts = _bind(ref, item, lane, cursors, set(item_by_lane.values()))
            bound = sorted(bound, key=lambda r: (str(r.get("plan_item_id")), str(r.get("id"))))
            raw_lanes = [copy.deepcopy(r) for r in lanes if (r.get("task_ref"), r.get("lane_id")) == (ref, lane)]
            raw_lanes.sort(key=lambda r: str(r.get("id")))
            if len(raw_lanes) > 1:
                conflicts.append("ambiguous_lane_rows")
            if list(item_by_lane.values()).count(item) > 1:
                conflicts.append("ambiguous_plan_item_identity")
            presence = join_worker_presence(ref, lane, host_jobs, vm, now=now, source=sources.get("host_jobs"))
            workers = presence.pop("workers")
            cursor = bound[0] if len(bound) == 1 else {}
            lane_row = raw_lanes[0] if len(raw_lanes) == 1 else {}
            node = dict(
                id=node_id,
                plan_item_id=item,
                title=declared_row.get("title", item),
                lane_id=lane,
                cursor_binding=method,
                cursor_id=cursor.get("plan_item_id"),
                cursor_ids=[r.get("plan_item_id") for r in bound],
                cursor_row_ids=[r.get("id") for r in bound],
                cursors=copy.deepcopy(bound),
                cursor_state=cursor.get("state"),
                dispatched_at=cursor.get("dispatched_at"),
                completed_at=cursor.get("completed_at"),
                lanes=raw_lanes,
                lane_status=lane_row.get("status"),
                backend=lane_row.get("backend"),
                model=lane_row.get("model"),
                workers=workers,
                presence=presence,
                blockers=[],
                waiting_on=[],
                inconsistency_reasons=conflicts,
                unknown_reasons=[],
            )
            node["display_status"] = _display(node, cursor_source=cursor_source, lane_source=lane_source)
            task["nodes"].append(node)
            by_item[item] = node
        for dependent, prerequisites in sorted(manifest.get("depends_on", {}).items()):
            for prerequisite in sorted(prerequisites):
                if prerequisite in item_by_lane and dependent in item_by_lane:
                    task["edges"].append(
                        dict(
                            source=f"plan:{ref}:{item_by_lane[prerequisite]}",
                            target=f"plan:{ref}:{item_by_lane[dependent]}",
                        )
                    )
        for left, right in sorted(manifest.get("conflict_edges", [])):
            task["conflict_edges"].append(
                dict(source=f"plan:{ref}:{item_by_lane[left]}", target=f"plan:{ref}:{item_by_lane[right]}")
            )
        # Prerequisite effects only propagate from complete, noncontradictory
        # evidence. Explicit graph edges, never merge-order adjacency, govern it.
        by_id = {node["id"]: node for node in task["nodes"]}
        for _ in task["nodes"]:
            changed = False
            for node in task["nodes"]:
                required = [by_id[e["source"]] for e in task["edges"] if e["target"] == node["id"]]
                node["blockers"] = sorted(r["id"] for r in required if r["display_status"] == "blocked")
                node["waiting_on"] = sorted(r["id"] for r in required if r["display_status"] != "done")
                if (
                    node["blockers"]
                    and node["cursor_state"] in (None, "dispatched")
                    and not node["inconsistency_reasons"]
                    and _good(cursor_source)
                    and _good(lane_source)
                    and node["display_status"] != "blocked"
                ):
                    # A proved prerequisite is sufficient for blocked; worker
                    # availability is irrelevant to that label.
                    node["display_status"] = "blocked"
                    node["unknown_reasons"] = []
                    changed = True
                if node["display_status"] in ("waiting", "running") and node["blockers"]:
                    node["display_status"] = "blocked"
                    changed = True
                elif node["display_status"] == "waiting" and any(r["display_status"] == "unknown" for r in required):
                    # An unknown prerequisite may be the blocker; a live worker
                    # stays running, since nothing can make it not live.
                    node["display_status"] = "unknown"
                    node["unknown_reasons"].append("prerequisite_unknown")
                    changed = True
            if not changed:
                break
    partial = (
        bool(limits)
        or any(not _good(s) for s in sources.values())
        or any(
            t["graph_state"] == "unknown"
            or (t.get("source") or {}).get("state") in ("drifted", "unknown")
            or any(n["display_status"] == "unknown" for n in t["nodes"])
            for t in result["tasks"]
        )
    )
    result.update(partial=partial, state="partial" if partial else "fresh", limits_hit=sorted(limits))
    return result


def _read_bytes(path, budget):
    """Use inherited Fleet byte bounds, with no-follow at every path component.

    There is no separate host-spec byte policy. This shares Fleet's existing
    per-record/aggregate resource bounds and original absolute deadline.
    """
    from .fleet_snapshot import MAX_RECORD_BYTES, MAX_TOTAL_BYTES

    path = Path(path).absolute()
    budget.check()
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        leaf = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        try:
            info = os.fstat(leaf)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("not_regular_file")
            if info.st_size > MAX_RECORD_BYTES or info.st_size > MAX_TOTAL_BYTES - budget.bytes_read:
                raise ValueError("bytes_limit")
            chunks, length = [], 0
            while True:
                budget.check()
                room = min(MAX_RECORD_BYTES - length, MAX_TOTAL_BYTES - budget.bytes_read)
                if room <= 0:
                    if os.fstat(leaf).st_size > length:
                        raise ValueError("bytes_limit")
                    break
                chunk = os.read(leaf, min(room, 1024**2))
                length += len(chunk)
                budget.bytes_read += len(chunk)
                if not chunk:
                    break
                chunks.append(chunk)
            budget.check()
            return b"".join(chunks)
        finally:
            os.close(leaf)
    finally:
        os.close(fd)


def _scan(directory, budget, limit):
    """Bound entries inspected as well as entries accepted; no symlink roots."""
    paths = []
    budget.check()
    fd = os.open(directory, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        with os.scandir(fd) as entries:
            for index, entry in enumerate(entries):
                budget.check()
                if index >= limit:
                    return sorted(paths), "files_limit"
                if entry.name.endswith(".json"):
                    paths.append(Path(directory) / entry.name)
    finally:
        os.close(fd)
    return sorted(paths), None


def _source_status(root, manifest, budget):
    """The S1 extraction/validation/digest implementation, on bounded bytes."""
    from .generate_lane_manifest import build_manifest_from_plan

    source = manifest.get("plan_source")
    if source is None:
        return dict(state="untracked", reason="manifest has no plan_source")
    try:
        selected = Path(source["path"])
        if selected.is_absolute() or ".." in selected.parts:
            raise ValueError("unsafe_plan_path")
        raw = _read_bytes(root / selected, budget)
        current = build_manifest_from_plan(
            root=root, task_ref=manifest["task_ref"], plan_path=selected, plan_bytes=raw
        )["plan_source"]
        return dict(
            state="matched" if current["decomposition_digest"] == source["decomposition_digest"] else "drifted",
            reason="decomposition digest matches manifest"
            if current["decomposition_digest"] == source["decomposition_digest"]
            else "plan edited since manifest",
            path=source["path"],
            manifest_digest=source["decomposition_digest"],
            current_digest=current["decomposition_digest"],
        )
    except (OSError, ValueError, RuntimeError, TypeError, KeyError) as exc:
        return dict(state="unknown", reason=f"{type(exc).__name__}: {exc}"[:500], path=source.get("path"))


def collect_plan_projection(root, *, vm=None, deadline=None, runtime=None, budget=None, publish=None, now=None):
    """Collect with the caller's budget, or own the same Fleet watchdog slot."""
    from .fleet_snapshot import REFRESH_SECONDS, _bounded

    if budget is not None:
        return _collect_plan_projection(root, vm=vm, runtime=runtime, budget=budget, publish=publish, now=now)
    deadline = deadline if deadline is not None else time.monotonic() + REFRESH_SECONDS

    def collect(budget, update):
        def progress(value):
            update(**value)
            if publish:
                publish(value)

        result = _collect_plan_projection(root, vm=vm, runtime=runtime, budget=budget, publish=progress, now=now)
        if result["partial"]:
            budget.issues.update(result["limits_hit"] or {"incomplete_plan_sources"})

    plan = _bounded(empty_plan(), collect, max(0, deadline - time.monotonic()))
    plan.pop("elapsed_s", None)  # collection duration is not part of the picture
    return plan


def _collect_plan_projection(root, *, vm, runtime, budget, publish, now):
    """Bounded read-only input collection; independent facets survive failures.

    The snapshot caller supplies its original deadline and watchdog. No cache
    is an input. No DB bootstrap, manifest generator publication, cursor writer,
    remote transport or ledger object with create-on-read behavior is called.
    """
    from .fleet_snapshot import MAX_FILES
    from .lane_manifest import validate_manifest

    root = Path(root).absolute()
    budget.check()
    runtime = runtime or RuntimeConfig.for_repo(root)
    now = time.time() if now is None else now
    sources, manifests, db_rows, host_jobs = {}, {}, {}, []
    limits = set()

    def emit():
        projected = project_plan(
            manifests,
            db_rows.get("cursors", []),
            db_rows.get("lanes", []),
            tasks=db_rows.get("tasks", []),
            host_jobs=host_jobs,
            vm=vm,
            sources=sources,
            now=now,
            limits_hit=limits,
        )
        if publish:
            publish(projected)
        return projected

    directory = root / "config/lane-orchestration"
    nodes_read = edges_read = 0
    try:
        paths, reason = _scan(directory, budget, MAX_FILES)
        sources["manifests"] = _source(
            "unknown" if reason else "fresh", reason, now=now, complete=not reason, path=str(directory)
        )
        if reason:
            limits.add(reason)
        for path in paths:
            manifests[path.stem] = dict(path=str(path), reason="manifest_not_read")
        for path in paths:
            ref = path.stem
            entry = dict(path=str(path))
            manifests[ref] = entry
            file_source = sources["manifest:" + ref] = _source(now=now, path=str(path))
            try:
                value = json.loads(_read_bytes(path, budget))
                if not isinstance(value, dict) or value.get("task_ref") != ref:
                    raise ValueError("manifest_task_identity_mismatch")
                valid = validate_manifest(value, path)
                lane_count = len(valid["lanes"])
                edge_count = sum(len(v) for v in valid.get("depends_on", {}).values()) + len(
                    valid.get("conflict_edges") or []
                )
                if nodes_read + lane_count > MAX_FILES or edges_read + edge_count > MAX_FILES:
                    limits.add("graph_rows_limit")
                    raise ValueError("graph_rows_limit")
                entry["manifest"] = valid
                nodes_read += lane_count
                edges_read += edge_count
                entry["source"] = _source_status(root, value, budget)
                if value.get("plan_source"):
                    status = entry["source"]
                    sources["plan_source:" + ref] = _source(
                        "unknown" if status["state"] == "unknown" else "fresh",
                        status["reason"] if status["state"] == "unknown" else None,
                        now=now,
                        complete=status["state"] != "unknown",
                        path=str(root / value["plan_source"]["path"]),
                    )
            except (OSError, ValueError, RuntimeError, TypeError, KeyError, TimeoutError) as exc:
                entry["reason"] = f"manifest_unreadable:{type(exc).__name__}:{exc}"[:500]
                file_source.update(state="unknown", complete=False, reason=entry["reason"])
                limits.add("manifest_unreadable")
                if isinstance(exc, TimeoutError):
                    limits.add("deadline")
                    emit()
                    break
            emit()
    except FileNotFoundError:
        # An absent manifest namespace is a complete observation of no graphs.
        sources["manifests"] = _source(now=now, path=str(directory))
    except (OSError, ValueError, TimeoutError) as exc:
        sources["manifests"] = _source(
            "unknown", f"manifests:{type(exc).__name__}", now=now, complete=False, path=str(directory)
        )
    emit()

    def read(conn):
        result = {}
        conn.execute("BEGIN")
        for name, table, fields in (
            ("tasks", "handoff_state", ("task_ref", "status")),
            ("cursors", "plan_cursors", _CURSOR_FIELDS),
            ("lanes", "worktree_lanes", _LANE_FIELDS),
        ):
            try:
                budget.check()
                rows = conn.execute(
                    f"SELECT * FROM {table} ORDER BY task_ref" + (", id" if name != "tasks" else "") + " LIMIT ?",
                    (MAX_FILES + 1,),
                )
                kept = [{k: row[k] for k in fields if k in row.keys()} for row in rows]
                result[name] = (kept[:MAX_FILES], "db_rows_limit" if len(kept) > MAX_FILES else None)
            except Exception as exc:
                result[name] = ([], f"{name}:{type(exc).__name__}:{exc}"[:500])
        conn.rollback()
        return result

    try:
        observations = bounded_handoff_read(runtime.db_path, read, deadline=budget.deadline)
    except Exception as exc:
        observations = {name: ([], f"db:{type(exc).__name__}:{exc}"[:500]) for name in ("tasks", "cursors", "lanes")}
    for name, (rows, reason) in observations.items():
        db_rows[name] = rows
        sources[name] = _source(
            "unknown" if reason else "fresh", reason, now=now, complete=not reason, path=str(runtime.db_path)
        )
        if reason:
            limits.add(reason)
    emit()
    directory = runtime.state_dir / "remote-jobs/ledger"
    try:
        paths, reason = _scan(directory, budget, MAX_FILES)
        sources["host_jobs"] = _source(
            "unknown", reason or "host_jobs_pending", now=now, complete=False, path=str(directory)
        )
        for path in paths:
            sources["host_job:" + path.stem] = _source(
                "unknown", "host_job_not_read", now=now, complete=False, path=str(path)
            )
        for path in paths:
            file_source = sources["host_job:" + path.stem] = _source(now=now, path=str(path))
            try:
                record = json.loads(_read_bytes(path, budget))
                if not isinstance(record, dict):
                    raise ValueError("invalid_host_record")
                raw = record.get("spec")
                if not isinstance(raw, dict):
                    raise ValueError("invalid_host_spec")
                spec = _identity_spec(raw)
                issue = _safe_spec(raw)
                if record.get("stage") not in ("intent", "submitted", "done", "completed", "dead_letter"):
                    issue = issue or "host_job_stage_unavailable"
                if isinstance(spec.get("job_id"), str) and path.stem != spec["job_id"]:
                    issue = "host_filename_identity_mismatch"
                host_jobs.append(dict(spec=spec, stage=record.get("stage"), unit=record.get("unit"), reason=issue))
                if issue:
                    sources["host_jobs"].update(state="unknown", complete=False, reason=issue)
                    file_source.update(state="unknown", complete=False, reason=issue)
            except (OSError, ValueError, TypeError, KeyError, TimeoutError) as exc:
                file_source.update(
                    state="unknown", complete=False, reason=f"host_jobs:{type(exc).__name__}:{exc}"[:500]
                )
                sources["host_jobs"].update(
                    state="unknown", complete=False, reason=f"host_jobs:{type(exc).__name__}:{exc}"[:500]
                )
                if isinstance(exc, TimeoutError):
                    limits.add("deadline")
                    emit()
                    break
            emit()
        if not reason and all(_good(sources["host_job:" + path.stem]) for path in paths):
            sources["host_jobs"] = _source(now=now, path=str(directory))
        if reason:
            limits.add(reason)
    except FileNotFoundError:
        sources["host_jobs"] = _source(now=now, path=str(directory))
    except (OSError, ValueError, TimeoutError) as exc:
        sources["host_jobs"] = _source(
            "unknown", f"host_jobs:{type(exc).__name__}", now=now, complete=False, path=str(directory)
        )
    vm = vm or {}
    for facet in ("jobs", "processes", "sandboxes"):
        # The VM observation was read once by the snapshot; record its own time.
        reason = _vm_reason(vm, now) or _facet_gap(vm, facet)
        sources["vm_" + facet] = _source(
            "unknown" if reason else "fresh", reason, now=now, complete=not reason, path="vm:" + facet
        )
        sources["vm_" + facet]["observed_at"] = vm.get("observed_at")
    return emit()
