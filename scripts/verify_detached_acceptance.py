#!/usr/bin/env python3
"""Host-side verification wrapper for VRE-06 detached-mode acceptance (PR330-08).

Evaluates ``internal-acceptance.json`` using ``scripts/measure_lane_host_cost.py``
(without modifying its pinned ``BARS``), enforces raw series, job-identity, sensor, and
monotonic-clock invariants, and—for ``--phase flip``—verifies read-only ``turn_metrics``
token telemetry in ``handoff.db`` (``mode=ro`` URI) plus separate ``s9`` real-gate
host-cost evidence.

Exit codes (Canon v0.26.2 [AGT-21] Exit status is the outcome contract):
  0  Accepted outcome for the requested phase
     (``measurement``: valid ``pass`` or ``fail``; ``flip``: valid ``pass`` +
     5 positive-total ``turn_metrics`` rows + passing ``s9`` gate evidence).
  1  Measured bar failure at ``--phase flip`` (computed verdict == ``fail``).
  2  Invalid, incomplete, or pending measurement evidence.
  3  Absent, unreadable, mismatched, or non-positive ``turn_metrics`` rows at ``flip``.
  4  Absent, invalid, or failed separate ``s9`` real-gate evidence at ``flip``.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sqlite3
import statistics
import sys
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RECEIPT_SCHEMA = "workbay.detached_acceptance_receipt.v1"
DEFAULT_ACCEPTANCE_PATH = "docs/assessments/internal-acceptance.json"
EXPECTED_WAVE_LANES = 5
EXPECTED_BARS = {
    "resident_host_processes_per_lane": 0,
    "host_rss_mb_per_inflight_lane_max": 25,
    "time_to_vm_start_p50_s_max": 30,
}

EXIT_OK = 0
EXIT_BAR_FAILURE = 1
EXIT_INVALID_OR_PENDING = 2
EXIT_TELEMETRY_FAILURE = 3
EXIT_GATE_EVIDENCE_FAILURE = 4

TURN_METRICS_QUERY = (
    "SELECT id, task_ref, lane_id, session, cycle, phase, backend, turn_id, "
    "total_tokens, usage_source, created_at "
    "FROM turn_metrics "
    "WHERE lane_id IN (?, ?, ?, ?, ?) "
    "ORDER BY id ASC"
)

_MEASURE_MOD: Any = None


def _load_measure_module() -> Any:
    """Load scripts/measure_lane_host_cost.py without mutating sys.path."""
    global _MEASURE_MOD
    if _MEASURE_MOD is not None:
        return _MEASURE_MOD
    script_path = Path(__file__).resolve().parent / "measure_lane_host_cost.py"
    spec = importlib.util.spec_from_file_location("measure_lane_host_cost", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load measure_lane_host_cost from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _MEASURE_MOD = module
    return module


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_finite_nonnegative(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 0
        and (isinstance(value, int) or math.isfinite(value))
    )


def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _parse_iso_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp requiring explicit timezone information."""
    if not _is_nonempty_str(value):
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def _validate_series(
    result: Mapping[str, Any],
    *,
    label: str,
    require_positive_process_count: bool = False,
    forbid_resident_processes: bool = False,
) -> tuple[str | None, str | None]:
    """Validate raw process-sample series coverage and monotonic sample timestamps."""
    series = result.get("series")
    if not isinstance(series, list) or not series:
        return (
            "missing_raw_series",
            f"{label}.series must be a non-empty list of raw samples",
        )
    samples = result.get("samples")
    if samples is not None:
        if type(samples) is not int or samples != len(series):
            return (
                "raw_series_count_mismatch",
                f"{label}.samples ({samples!r}) does not match len(series) ({len(series)})",
            )

    prev_t: float | None = None
    saw_positive_process = False
    for idx, point in enumerate(series):
        if not isinstance(point, Mapping):
            return (
                "invalid_raw_series",
                f"{label}.series[{idx}] must be an object",
            )
        t_val = point.get("t")
        if not _is_finite_nonnegative(t_val):
            return (
                "invalid_clock",
                f"{label}.series[{idx}].t must be a finite non-negative number",
            )
        if prev_t is not None and float(t_val) < prev_t:
            return (
                "invalid_clock",
                f"{label}.series[{idx}].t ({t_val}) is earlier than previous sample ({prev_t})",
            )
        prev_t = float(t_val)

        proc_count = point.get("process_count")
        if type(proc_count) is not int or proc_count < 0:
            return (
                "invalid_raw_series",
                f"{label}.series[{idx}].process_count must be a non-negative integer",
            )
        if proc_count > 0:
            saw_positive_process = True
            if forbid_resident_processes:
                return (
                    "gate_resident_process_detected",
                    f"{label}.series[{idx}].process_count={proc_count} violates zero resident process bar",
                )

        unreadable = point.get("footprint_unreadable", 0)
        if type(unreadable) is not int or unreadable != 0:
            return (
                "invalid_raw_series",
                f"{label}.series[{idx}].footprint_unreadable must be 0",
            )

    if require_positive_process_count and not saw_positive_process:
        return (
            "attached_raw_series_unmatched",
            f"{label}.series has no sample with process_count > 0",
        )
    return None, None


def _validate_window_clock(result: Mapping[str, Any], *, label: str) -> tuple[str | None, str | None]:
    started_at = _parse_iso_timestamp(result.get("started_at"))
    ended_at = _parse_iso_timestamp(result.get("ended_at"))
    if started_at is None or ended_at is None:
        return (
            "invalid_clock",
            f"{label}.started_at and {label}.ended_at must be valid timezone-aware ISO-8601 timestamps",
        )
    if ended_at < started_at:
        return (
            "invalid_clock",
            f"{label}.ended_at ({result.get('ended_at')}) precedes started_at ({result.get('started_at')})",
        )
    return None, None


def _validate_jobs_and_clocks(
    result: Mapping[str, Any],
    *,
    mode: str,
    expected_lanes: int,
) -> tuple[list[dict[str, Any]] | None, str | None, str | None]:
    """Validate 5-lane raw job/pass identity coverage, needles, and per-job clocks."""
    needles = result.get("needles")
    if not isinstance(needles, list) or not needles or not all(_is_nonempty_str(n) for n in needles):
        return (
            None,
            "missing_job_identity_coverage",
            f"{mode}.needles must be a non-empty list of strings",
        )
    needle_set = {n.strip() for n in needles}

    jobs = result.get("jobs")
    if not isinstance(jobs, list) or len(jobs) != expected_lanes:
        return (
            None,
            "missing_job_identity_coverage",
            f"{mode}.jobs must contain exactly {expected_lanes} job identity records",
        )

    seen_lanes: set[str] = set()
    seen_passes: set[str] = set()
    seen_jobs: set[str] = set()
    normalized_jobs: list[dict[str, Any]] = []
    vm_start_deltas: list[float] = []

    clock_offset_s = result.get("clock_offset_s", 0.0)
    if not isinstance(clock_offset_s, (int, float)) or isinstance(clock_offset_s, bool) or not math.isfinite(
        clock_offset_s
    ):
        return None, "invalid_clock", f"{mode}.clock_offset_s must be a finite number"

    clock_uncertainty_s = result.get("clock_uncertainty_s", 0.0)
    if not _is_finite_nonnegative(clock_uncertainty_s):
        return None, "invalid_clock", f"{mode}.clock_uncertainty_s must be a finite non-negative number"

    for idx, entry in enumerate(jobs):
        if not isinstance(entry, Mapping):
            return (
                None,
                "missing_job_identity_coverage",
                f"{mode}.jobs[{idx}] must be an object",
            )
        lane_id = entry.get("lane_id")
        pass_id = entry.get("pass_id")
        job_id = entry.get("job_id")
        if not (_is_nonempty_str(lane_id) and _is_nonempty_str(pass_id) and _is_nonempty_str(job_id)):
            return (
                None,
                "missing_job_identity_coverage",
                f"{mode}.jobs[{idx}] must include non-empty lane_id, pass_id, and job_id",
            )
        lane_id_s = lane_id.strip()
        pass_id_s = pass_id.strip()
        job_id_s = job_id.strip()

        if lane_id_s in seen_lanes or pass_id_s in seen_passes or job_id_s in seen_jobs:
            return (
                None,
                "duplicate_job_identity",
                f"{mode}.jobs[{idx}] repeats a lane_id, pass_id, or job_id",
            )
        seen_lanes.add(lane_id_s)
        seen_passes.add(pass_id_s)
        seen_jobs.add(job_id_s)

        if lane_id_s not in needle_set:
            return (
                None,
                "missing_job_identity_coverage",
                f"{mode}.jobs[{idx}].lane_id ({lane_id_s}) is not covered by {mode}.needles",
            )

        dispatched_raw = entry.get("dispatched_at")
        vm_started_raw = entry.get("vm_started_at")
        if mode == "detached" or dispatched_raw is not None or vm_started_raw is not None:
            dispatched_at = _parse_iso_timestamp(dispatched_raw)
            vm_started_at = _parse_iso_timestamp(vm_started_raw)
            if dispatched_at is None or vm_started_at is None:
                return (
                    None,
                    "invalid_clock",
                    f"{mode}.jobs[{idx}] requires valid timezone-aware dispatched_at and vm_started_at",
                )
            delta_s = (vm_started_at - dispatched_at).total_seconds() - float(clock_offset_s)
            if delta_s < 0:
                return (
                    None,
                    "invalid_clock",
                    f"{mode}.jobs[{idx}] has reversed clock timestamps (delta={delta_s:.3f}s < 0)",
                )
            vm_start_deltas.append(delta_s)

        normalized_jobs.append(dict(entry))

    if mode == "detached":
        computed_p50 = float(statistics.median(vm_start_deltas))
        reported_p50 = float(result["time_to_vm_start_p50_s"])
        tolerance_s = max(1.0, float(clock_uncertainty_s))
        if abs(computed_p50 - reported_p50) > tolerance_s:
            return (
                None,
                "invalid_clock",
                (
                    f"detached.time_to_vm_start_p50_s ({reported_p50}) diverges from "
                    f"job-timestamp median ({computed_p50:.3f}s, tolerance={tolerance_s}s)"
                ),
            )

    return normalized_jobs, None, None


def _validate_measurement_evidence(
    acceptance: Any,
    measure_mod: Any,
) -> tuple[str, str | None, str | None, list[dict[str, Any]] | None]:
    """Validate acceptance JSON, immutable BARS, verdict_for, sensors, raw series, identities, and clocks."""
    if not isinstance(acceptance, Mapping):
        return "invalid_measurement", "acceptance_file_invalid", "acceptance JSON root must be an object", None

    if getattr(measure_mod, "BARS", None) != EXPECTED_BARS or acceptance.get("bars") != EXPECTED_BARS:
        return (
            "invalid_measurement",
            "bars_modified",
            "acceptance bars or measure_lane_host_cost.BARS do not match immutable plan BARS",
            None,
        )

    # Reject bools masquerading as int in bars (e.g., False == 0 in Python)
    for bars_src in (measure_mod.BARS, acceptance.get("bars")):
        if any(isinstance(v, bool) for v in bars_src.values()):
            return (
                "invalid_measurement",
                "bars_modified",
                "bars values must be numeric, not boolean",
                None,
            )

    verdict = measure_mod.verdict_for(acceptance)
    if verdict == "pending_operator_measurement":
        return (
            verdict,
            "pending_operator_measurement",
            "measurement is pending operator completion (missing attached/detached or detached.time_to_vm_start_p50_s)",
            None,
        )
    if verdict not in ("pass", "fail"):
        return (
            "invalid_measurement",
            "invalid_measurement",
            f"measure_lane_host_cost.verdict_for returned {verdict!r}",
            None,
        )

    attached = acceptance["attached"]
    detached = acceptance["detached"]

    if attached.get("platform") != detached.get("platform") or attached.get("memory_source") != detached.get(
        "memory_source"
    ):
        return (
            "invalid_measurement",
            "sensor_mismatch",
            "attached and detached measurements must use the same platform and memory_source",
            None,
        )

    for mode, result in (("attached", attached), ("detached", detached)):
        err_code, err_detail = _validate_window_clock(result, label=mode)
        if err_code is not None:
            return "invalid_measurement", err_code, err_detail, None

        err_code, err_detail = _validate_series(
            result,
            label=mode,
            require_positive_process_count=(mode == "attached"),
        )
        if err_code is not None:
            return "invalid_measurement", err_code, err_detail, None

        jobs, err_code, err_detail = _validate_jobs_and_clocks(
            result,
            mode=mode,
            expected_lanes=EXPECTED_WAVE_LANES,
        )
        if err_code is not None:
            return "invalid_measurement", err_code, err_detail, None
        if mode == "detached":
            detached_jobs = jobs

    return verdict, None, None, detached_jobs


def _verify_turn_metrics_readonly(
    handoff_db: str | None,
    detached_jobs: list[dict[str, Any]],
    default_task_ref: str | None,
) -> tuple[dict[str, Any], str | None, str | None]:
    """Query handoff.db strictly read-only (mode=ro URI) for 5 positive-total turn_metrics rows."""
    snapshot_at = _utc_now_iso()
    expected_passes = [
        {"lane_id": j["lane_id"], "pass_id": j["pass_id"], "job_id": j["job_id"]}
        for j in detached_jobs
    ]
    base_telemetry: dict[str, Any] = {
        "db_path": handoff_db,
        "db_uri": None,
        "query": TURN_METRICS_QUERY,
        "snapshot_at": snapshot_at,
        "expected_passes": expected_passes,
        "matched_row_ids": [],
        "matched_rows": [],
    }

    if not handoff_db:
        return base_telemetry, "telemetry_db_missing", "--handoff-db is required for --phase flip"

    db_path = Path(handoff_db)
    if not db_path.is_file():
        return base_telemetry, "telemetry_db_missing", f"handoff database does not exist: {handoff_db}"

    db_uri = f"{db_path.resolve().as_uri()}?mode=ro"
    base_telemetry["db_uri"] = db_uri
    lane_ids = [j["lane_id"].strip() for j in detached_jobs]

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(db_uri, uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        rows = [dict(r) for r in conn.execute(TURN_METRICS_QUERY, lane_ids).fetchall()]
    except sqlite3.Error as exc:
        return base_telemetry, "telemetry_query_failed", f"read-only SQLite query failed: {exc}"
    finally:
        if conn is not None:
            conn.close()

    used_row_ids: set[int] = set()
    matched_rows: list[dict[str, Any]] = []
    missing_lanes: list[str] = []
    non_positive_lanes: list[str] = []

    for job in detached_jobs:
        lane_id = job["lane_id"].strip()
        expected_task_ref = job.get("task_ref") or default_task_ref
        candidates: list[dict[str, Any]] = []
        for row in rows:
            if row["id"] in used_row_ids:
                continue
            if row.get("lane_id") != lane_id:
                continue
            if expected_task_ref is not None and row.get("task_ref") != expected_task_ref:
                continue
            if job.get("session") is not None and row.get("session") != job["session"]:
                continue
            if job.get("cycle") is not None and row.get("cycle") != job["cycle"]:
                continue
            if job.get("phase") is not None and row.get("phase") != job["phase"]:
                continue
            if job.get("backend") is not None and row.get("backend") != job["backend"]:
                continue
            if job.get("turn_id") is not None and row.get("turn_id") != job["turn_id"]:
                continue
            candidates.append(row)

        if not candidates:
            missing_lanes.append(lane_id)
            continue

        positive_candidates = [
            row
            for row in candidates
            if type(row.get("total_tokens")) is int and row["total_tokens"] > 0
        ]
        if not positive_candidates:
            non_positive_lanes.append(lane_id)
            continue

        chosen = positive_candidates[0]
        used_row_ids.add(chosen["id"])
        matched_rows.append(
            {
                "lane_id": lane_id,
                "pass_id": job["pass_id"],
                "job_id": job["job_id"],
                "row_id": chosen["id"],
                "task_ref": chosen.get("task_ref"),
                "total_tokens": chosen["total_tokens"],
                "usage_source": chosen.get("usage_source"),
            }
        )

    base_telemetry["matched_row_ids"] = [m["row_id"] for m in matched_rows]
    base_telemetry["matched_rows"] = matched_rows

    if missing_lanes:
        base_telemetry["missing_lane_ids"] = missing_lanes
        return (
            base_telemetry,
            "telemetry_rows_missing_or_mismatched",
            f"missing identity-matched turn_metrics rows for lanes: {', '.join(missing_lanes)}",
        )
    if non_positive_lanes:
        base_telemetry["non_positive_lane_ids"] = non_positive_lanes
        return (
            base_telemetry,
            "telemetry_non_positive_tokens",
            f"turn_metrics rows have non-positive total_tokens for lanes: {', '.join(non_positive_lanes)}",
        )
    return base_telemetry, None, None


def _verify_gate_evidence(gate_evidence_path: str | None) -> tuple[dict[str, Any], str | None, str | None]:
    """Validate separate s9 real-gate detached host-cost evidence."""
    summary: dict[str, Any] = {
        "path": gate_evidence_path,
        "verdict": None,
        "valid": False,
    }
    if not gate_evidence_path:
        return summary, "gate_evidence_missing", "--gate-evidence is required for --phase flip"

    path = Path(gate_evidence_path)
    if not path.is_file():
        return summary, "gate_evidence_missing", f"gate evidence file does not exist: {gate_evidence_path}"

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return summary, "gate_evidence_invalid", f"could not parse gate evidence JSON: {exc}"

    if not isinstance(payload, Mapping):
        return summary, "gate_evidence_invalid", "gate evidence JSON root must be an object"

    verdict = payload.get("verdict")
    summary["verdict"] = verdict
    if verdict != "pass":
        return summary, "gate_evidence_failed", f"gate evidence verdict must be 'pass', got {verdict!r}"

    if payload.get("mode") != "detached":
        return summary, "gate_evidence_invalid", f"gate evidence mode must be 'detached', got {payload.get('mode')!r}"

    identity = next(
        (payload.get(k).strip() for k in ("job_id", "gate_id", "review_run_id", "pass_id") if _is_nonempty_str(payload.get(k))),
        None,
    )
    if identity is None:
        return (
            summary,
            "gate_evidence_invalid",
            "gate evidence must include a non-empty job_id, gate_id, review_run_id, or pass_id",
        )
    summary["identity"] = identity

    err_code, err_detail = _validate_window_clock(payload, label="gate_evidence")
    if err_code is not None:
        return summary, "gate_evidence_invalid", err_detail

    err_code, err_detail = _validate_series(
        payload,
        label="gate_evidence",
        forbid_resident_processes=True,
    )
    if err_code is not None:
        return summary, err_code, err_detail

    for zero_field in ("resident_host_processes_per_lane", "process_count_max"):
        if zero_field in payload:
            val = payload[zero_field]
            if not _is_finite_nonnegative(val) or val != 0:
                return (
                    summary,
                    "gate_resident_process_detected",
                    f"gate_evidence.{zero_field} must be 0, got {val!r}",
                )

    summary["samples"] = len(payload["series"])
    summary["mode"] = payload["mode"]
    summary["valid"] = True
    return summary, None, None


def _summarize_mode_for_receipt(result: Any) -> dict[str, Any] | None:
    if not isinstance(result, Mapping):
        return None
    jobs = result.get("jobs")
    return {
        "mode": result.get("mode"),
        "lanes": result.get("lanes"),
        "platform": result.get("platform"),
        "memory_source": result.get("memory_source"),
        "samples": result.get("samples"),
        "started_at": result.get("started_at"),
        "ended_at": result.get("ended_at"),
        "resident_host_processes_per_lane": result.get("resident_host_processes_per_lane"),
        "host_rss_mb_per_inflight_lane_max": result.get("host_rss_mb_per_inflight_lane_max"),
        "time_to_vm_start_p50_s": result.get("time_to_vm_start_p50_s"),
        "job_ids": [j.get("job_id") for j in jobs if isinstance(j, Mapping)] if isinstance(jobs, list) else [],
        "pass_ids": [j.get("pass_id") for j in jobs if isinstance(j, Mapping)] if isinstance(jobs, list) else [],
    }


def _write_receipt_atomic(receipt: Mapping[str, Any], receipt_path: str) -> None:
    target = Path(receipt_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def verify_acceptance(
    *,
    phase: str,
    acceptance_path: str,
    handoff_db: str | None = None,
    gate_evidence_path: str | None = None,
    measure_mod: Any = None,
) -> tuple[int, dict[str, Any]]:
    """Run the PR330-08 verification contract and return (exit_code, receipt)."""
    measure_mod = measure_mod if measure_mod is not None else _load_measure_module()
    receipt: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "phase": phase,
        "acceptance_path": acceptance_path,
        "verdict": "invalid_measurement",
        "exit_code": EXIT_INVALID_OR_PENDING,
        "reason": "uninitialized",
        "detail": "",
        "measurement_complete": False,
        "flip_permitted": False,
        "checked_at": _utc_now_iso(),
        "bars": dict(EXPECTED_BARS),
        "attached_summary": None,
        "detached_summary": None,
        "telemetry": None,
        "gate_evidence": None,
    }

    path = Path(acceptance_path)
    if not path.is_file():
        receipt["reason"] = "acceptance_file_missing"
        receipt["detail"] = f"acceptance file does not exist: {acceptance_path}"
        return EXIT_INVALID_OR_PENDING, receipt

    try:
        acceptance = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        receipt["reason"] = "acceptance_file_invalid"
        receipt["detail"] = f"could not parse acceptance JSON: {exc}"
        return EXIT_INVALID_OR_PENDING, receipt

    if isinstance(acceptance, Mapping):
        receipt["attached_summary"] = _summarize_mode_for_receipt(acceptance.get("attached"))
        receipt["detached_summary"] = _summarize_mode_for_receipt(acceptance.get("detached"))

    verdict, err_code, err_detail, detached_jobs = _validate_measurement_evidence(acceptance, measure_mod)
    receipt["verdict"] = verdict
    if err_code is not None or detached_jobs is None:
        receipt["exit_code"] = EXIT_INVALID_OR_PENDING
        receipt["reason"] = err_code or "invalid_measurement"
        receipt["detail"] = err_detail or "measurement evidence validation failed"
        return EXIT_INVALID_OR_PENDING, receipt

    receipt["measurement_complete"] = True

    if phase == "measurement":
        receipt["exit_code"] = EXIT_OK
        if verdict == "pass":
            receipt["reason"] = "measurement_passed"
            receipt["detail"] = "five-lane measurement evidence is valid and passed all bars"
        else:
            receipt["reason"] = "measurement_failed_bars"
            receipt["detail"] = (
                "five-lane measurement evidence is complete but failed one or more bars; "
                "default flip is not permitted"
            )
        return EXIT_OK, receipt

    # --phase flip
    if verdict == "fail":
        receipt["exit_code"] = EXIT_BAR_FAILURE
        receipt["reason"] = "bar_failure"
        receipt["detail"] = "detached measurement failed one or more immutable host-cost/start bars"
        return EXIT_BAR_FAILURE, receipt

    default_task_ref = acceptance["detached"].get("task") or acceptance.get("task")
    telemetry, tel_err, tel_detail = _verify_turn_metrics_readonly(
        handoff_db,
        detached_jobs,
        default_task_ref=default_task_ref,
    )
    receipt["telemetry"] = telemetry
    if tel_err is not None:
        receipt["exit_code"] = EXIT_TELEMETRY_FAILURE
        receipt["reason"] = tel_err
        receipt["detail"] = tel_detail or "turn_metrics telemetry verification failed"
        return EXIT_TELEMETRY_FAILURE, receipt

    gate_summary, gate_err, gate_detail = _verify_gate_evidence(gate_evidence_path)
    receipt["gate_evidence"] = gate_summary
    if gate_err is not None:
        receipt["exit_code"] = EXIT_GATE_EVIDENCE_FAILURE
        receipt["reason"] = gate_err
        receipt["detail"] = gate_detail or "s9 real-gate evidence verification failed"
        return EXIT_GATE_EVIDENCE_FAILURE, receipt

    receipt["exit_code"] = EXIT_OK
    receipt["reason"] = "flip_permitted"
    receipt["detail"] = (
        "measurement passed all bars, 5 detached passes matched positive turn_metrics rows, "
        "and separate s9 gate evidence passed"
    )
    receipt["flip_permitted"] = True
    return EXIT_OK, receipt


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify VRE-06 detached-mode acceptance evidence and emit a structured receipt."
    )
    parser.add_argument(
        "--phase",
        required=True,
        choices=("measurement", "flip"),
        help="Verification phase: 'measurement' (s7 completion) or 'flip' (s8 default-flip gate).",
    )
    parser.add_argument(
        "--acceptance",
        default=DEFAULT_ACCEPTANCE_PATH,
        help=f"Path to acceptance JSON (default: {DEFAULT_ACCEPTANCE_PATH}).",
    )
    parser.add_argument(
        "--receipt",
        default=None,
        help="Optional output path for the structured JSON receipt (written atomically).",
    )
    parser.add_argument(
        "--handoff-db",
        default=None,
        help="Path to handoff SQLite database (opened strictly read-only via mode=ro URI for --phase flip).",
    )
    parser.add_argument(
        "--gate-evidence",
        "--gate-receipt",
        dest="gate_evidence",
        default=None,
        help="Path to separate s9 real-gate host-cost evidence JSON (required for --phase flip).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    exit_code, receipt = verify_acceptance(
        phase=args.phase,
        acceptance_path=args.acceptance,
        handoff_db=args.handoff_db,
        gate_evidence_path=args.gate_evidence,
    )
    if args.receipt is not None:
        _write_receipt_atomic(receipt, args.receipt)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
