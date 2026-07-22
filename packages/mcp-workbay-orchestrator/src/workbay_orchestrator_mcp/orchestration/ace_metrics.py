"""ACE observability metrics aggregation.

Reads JSONL worker/orchestrator logs, handoff.db, mcp-artifacts.db, and
instruction files to produce a metrics snapshot (JSON or markdown).

Usage:
    python -m workbay_handoff_mcp.orchestration.ace_metrics \\
        --task-ref <task> \\
        --state-dir .task-state \\
        --logs-dir logs \\
        --output-format markdown

The snapshot is also appended (as JSON) to .task-state/metrics.jsonl.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Sequence

from .ace_reflect import parse_strategy_bullets, validate_playbook_files
from .handoff_read_shapes import hot_state_metric_kwargs

_ACE_RULE_REFERENCE_RE = re.compile(r"\[(?:sr|rg)-\d{3}\]")


# ---------------------------------------------------------------------------
# JSONL log parsing
# ---------------------------------------------------------------------------


def _iter_jsonl(path: Path):
    """Yield parsed JSON objects from a JSONL file; skip malformed lines."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def _collect_worker_events(logs_dir: Path) -> list[dict]:
    events: list[dict] = []
    worker_dir = logs_dir / "worker-daemon"
    if worker_dir.exists():
        for f in sorted(worker_dir.glob("worker-*.jsonl")):
            events.extend(_iter_jsonl(f))
    return events


def _collect_orchestrator_events(logs_dir: Path) -> list[dict]:
    return list(_iter_jsonl(logs_dir / "daemon" / "orchestrator.jsonl"))


def _load_turn_metrics(state_dir: Path, task_ref: str) -> list[dict]:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime, generate_current_task_md  # noqa: PLC0415

    configure_runtime(RuntimeConfig.for_repo(state_dir.parent, state_dir=state_dir))
    payload = generate_current_task_md(task_ref=task_ref, write_file=False)
    if payload.get("ok") is not True:
        error = payload.get("error") or payload.get("data", {}).get("error")
        raise RuntimeError(f"generate_current_task_md failed for {task_ref}: {error}")
    current_task_json = payload.get("data", {}).get("current_task_json")
    if not isinstance(current_task_json, str):
        raise RuntimeError(f"generate_current_task_md returned no current_task_json for {task_ref}")
    state = json.loads(current_task_json)
    rows = state.get("turn_metrics", [])
    if not isinstance(rows, list):
        raise RuntimeError(f"CURRENT_TASK.json returned non-list turn_metrics for {task_ref}")
    return [row for row in rows if isinstance(row, dict)]


# ---------------------------------------------------------------------------
# Token burn aggregation
# ---------------------------------------------------------------------------


def _agent_turn_rows(turn_metrics: list[dict] | None) -> list[dict]:
    """Agent-execution-turn rows only. Excludes ``phase='lane_prep'`` footprint-telemetry
    rows (internal S2): they are token-less, pressure-less
    observability rows the short-lived lane_prompt subprocess self-writes, NOT agent turns,
    so counting them flips ``data_available`` true on a prep-only set and dilutes the pressure
    elevated/high ratios toward ``normal``. ``list_turn_metrics`` still surfaces prep rows for
    operators; only the agent-turn AGGREGATES filter them out.

    Uses ``phase`` as the single canonical discriminator — identical criterion to the
    ``get_turn_metrics_summary`` SQL filter (``COALESCE(phase,'') != 'lane_prep'``) — so the
    two exclusion sites cannot drift apart (record_turn_metric requires a non-null phase)."""
    if not turn_metrics:
        return []
    return [row for row in turn_metrics if str(row.get("phase") or "") != "lane_prep"]


def _token_burn(worker_events: list[dict], turn_metrics: list[dict] | None = None) -> dict:
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    turn_metrics = _agent_turn_rows(turn_metrics)
    if turn_metrics:
        total = 0
        turn_metrics_by_lane: dict[str, int] = {}
        by_source = {"observed": 0, "tokenizer_estimate": 0, "char_estimate": 0}
        for row in turn_metrics:
            tokens = int(row.get("total_tokens") or 0)
            lane = str(row.get("lane_id") or "unknown")
            total += tokens
            turn_metrics_by_lane[lane] = turn_metrics_by_lane.get(lane, 0) + tokens
            source = row.get("usage_source")
            if isinstance(source, str) and source in by_source:
                by_source[source] += 1
        review_events = [e for e in worker_events if e.get("event") == WorkerEventName.REVIEW_COMPLETE]
        converged_cycles = sum(1 for e in review_events if e.get("converged"))
        total_review_cycles = len(review_events)
        tpc = (total // converged_cycles) if converged_cycles > 0 else None
        return {
            "data_available": total > 0 or bool(turn_metrics),
            "total_tokens": total,
            "by_lane": turn_metrics_by_lane,
            "by_usage_source": by_source,
            "converged_cycles": converged_cycles,
            "total_review_cycles": total_review_cycles,
            "tokens_per_converged_cycle": tpc,
        }
    total = 0
    by_lane: dict[str, int] = {}
    converged_cycles = 0
    total_review_cycles = 0

    for e in worker_events:
        event = e.get("event")
        if event == WorkerEventName.SUBAGENT_TURN_OBSERVED:
            tokens = (e.get("token_usage_totals") or {}).get("total_tokens") or 0
            lane = e.get("lane_id", "unknown")
            total += tokens
            by_lane[lane] = by_lane.get(lane, 0) + tokens
        elif event == WorkerEventName.REVIEW_COMPLETE:
            total_review_cycles += 1
            if e.get("converged"):
                converged_cycles += 1

    tpc = (total // converged_cycles) if converged_cycles > 0 else None
    return {
        "data_available": total > 0,
        "total_tokens": total,
        "by_lane": by_lane,
        "by_usage_source": {},
        "converged_cycles": converged_cycles,
        "total_review_cycles": total_review_cycles,
        "tokens_per_converged_cycle": tpc,
    }


# ---------------------------------------------------------------------------
# Context pressure trending
# ---------------------------------------------------------------------------


def _context_pressure(worker_events: list[dict], turn_metrics: list[dict] | None = None) -> dict:
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    turn_metrics = _agent_turn_rows(turn_metrics)
    if turn_metrics:
        metric_counts: dict[str, int] = {"normal": 0, "elevated": 0, "high": 0}
        by_source = {"observed": 0, "tokenizer_estimate": 0, "char_estimate": 0}
        latest = "normal"
        for row in turn_metrics:
            level = str(row.get("pressure_level") or "normal")
            if level in metric_counts:
                metric_counts[level] += 1
                latest = level
            prompt_source = row.get("prompt_token_source")
            if isinstance(prompt_source, str) and prompt_source in by_source:
                by_source[prompt_source] += 1
        total = sum(metric_counts.values())
        return {
            "data_available": total > 0,
            "latest_pressure": latest,
            "elevated_cycle_ratio": round(metric_counts["elevated"] / total, 3) if total else 0.0,
            "high_cycle_ratio": round(metric_counts["high"] / total, 3) if total else 0.0,
            "pressure_by_source": by_source,
        }
    counts: dict[str, int] = {"normal": 0, "elevated": 0, "high": 0}
    latest = "normal"
    for e in worker_events:
        if e.get("event") == WorkerEventName.CONTEXT_PRESSURE:
            level = e.get("pressure_level", "normal")
            if level in counts:
                counts[level] += 1
                latest = level

    total = sum(counts.values())
    return {
        "data_available": total > 0,
        "latest_pressure": latest,
        "elevated_cycle_ratio": round(counts["elevated"] / total, 3) if total else 0.0,
        "high_cycle_ratio": round(counts["high"] / total, 3) if total else 0.0,
        "pressure_by_source": {},
    }


# ---------------------------------------------------------------------------
# Tool attribution
# ---------------------------------------------------------------------------


def _tool_attribution(turn_metrics: list[dict]) -> dict:
    turn_metrics = _agent_turn_rows(turn_metrics)
    tracked_tools = (
        "used_ace_guidance",
        "used_artifact_context",
        "used_slice_packet",
        "used_recent_lane_history",
        "used_global_context",
        "used_ctx7",
    )
    by_tool = {tool: {"turns": 0, "prompt_tokens": 0, "prompt_chars": 0, "total_tokens": 0} for tool in tracked_tools}
    ctx7_query_count_total = 0
    turns_with_ctx7_queries = 0

    for row in turn_metrics:
        attribution = row.get("attribution")
        if not isinstance(attribution, dict):
            continue
        prompt_tokens = int(row.get("prompt_tokens") or 0)
        prompt_chars = int(row.get("prompt_chars") or 0)
        total_tokens = int(row.get("total_tokens") or 0)
        for tool in tracked_tools:
            if attribution.get(tool) is True:
                by_tool[tool]["turns"] += 1
                by_tool[tool]["prompt_tokens"] += prompt_tokens
                by_tool[tool]["prompt_chars"] += prompt_chars
                by_tool[tool]["total_tokens"] += total_tokens
        query_count = attribution.get("ctx7_query_count")
        if isinstance(query_count, int) and query_count > 0:
            ctx7_query_count_total += query_count
            turns_with_ctx7_queries += 1

    return {
        "data_available": bool(turn_metrics),
        "by_tool": by_tool,
        "ctx7_query_count_total": ctx7_query_count_total,
        "turns_with_ctx7_queries": turns_with_ctx7_queries,
    }


def _preflight_observed_drift(turn_metrics: list[dict]) -> dict:
    # Exclude lane_prep rows uniformly at the agent-turn boundary (grok G1). Today prep rows
    # are skipped only implicitly (null prompt_tokens/input_tokens hit the continue below); make
    # the exclusion explicit so a future emitter that fills token fields on prep rows cannot
    # poison the drift ratios while the sibling aggregators stay clean.
    turn_metrics = _agent_turn_rows(turn_metrics)
    comparable_rows: list[dict[str, int | str]] = []
    for row in turn_metrics:
        prompt_tokens = row.get("prompt_tokens")
        input_tokens = row.get("input_tokens")
        if prompt_tokens is None or input_tokens is None:
            continue
        prompt_tokens_int = int(prompt_tokens)
        input_tokens_int = int(input_tokens)
        comparable_rows.append(
            {
                "prompt_tokens": prompt_tokens_int,
                "input_tokens": input_tokens_int,
                "drift_tokens": input_tokens_int - prompt_tokens_int,
                "prompt_token_source": str(row.get("prompt_token_source") or "unknown"),
            }
        )

    if not comparable_rows:
        return {
            "data_available": False,
            "comparable_turns": 0,
            "exact_preflight_turns": 0,
            "estimated_preflight_turns": 0,
            "mean_signed_token_drift": None,
            "mean_absolute_token_drift": None,
            "median_absolute_token_drift": None,
            "max_absolute_token_drift": None,
        }

    drift_values = [int(row["drift_tokens"]) for row in comparable_rows]
    abs_drift_values = [abs(value) for value in drift_values]
    exact_preflight_turns = sum(1 for row in comparable_rows if row["prompt_token_source"] == "observed")
    comparable_turns = len(comparable_rows)
    estimated_preflight_turns = comparable_turns - exact_preflight_turns
    return {
        "data_available": True,
        "comparable_turns": comparable_turns,
        "exact_preflight_turns": exact_preflight_turns,
        "estimated_preflight_turns": estimated_preflight_turns,
        "mean_signed_token_drift": round(sum(drift_values) / comparable_turns, 3),
        "mean_absolute_token_drift": round(sum(abs_drift_values) / comparable_turns, 3),
        "median_absolute_token_drift": round(float(median(abs_drift_values)), 3),
        "max_absolute_token_drift": max(abs_drift_values),
    }


def _ace_model_curation(state_dir: Path) -> dict:
    log_path = state_dir / "ace_curation_log.jsonl"
    entries = list(_iter_jsonl(log_path))
    if not entries:
        return {
            "data_available": False,
            "runs": 0,
            "triggered_runs": 0,
            "total_tokens": 0,
            "latest_status": None,
            "latest_backend": None,
            "latest_model": None,
        }
    latest = entries[-1]
    total_tokens = sum(int(entry.get("token_usage", {}).get("total", {}).get("total_tokens") or 0) for entry in entries)
    return {
        "data_available": True,
        "runs": len(entries),
        "triggered_runs": sum(1 for entry in entries if entry.get("status") == "triggered"),
        "total_tokens": total_tokens,
        "latest_status": latest.get("status"),
        "latest_backend": latest.get("backend"),
        "latest_model": latest.get("model"),
    }


# ---------------------------------------------------------------------------
# ACE process health
# ---------------------------------------------------------------------------


def _ace_process_health(task_ref: str, state_dir: Path, instruction_files: list[Path]) -> dict:
    result: dict[str, object] = {
        "data_available": False,
        "status": "defined",
        "rules_defined": False,
        "reflect_log_exists": False,
        "logged_detection_count": 0,
        "pending_entry_count": 0,
        "processed_entry_count": 0,
        "last_apply_at": None,
        "rule_tagged_findings": 0,
        "backfill_needed": False,
    }

    rules_defined = any(parse_strategy_bullets(path) for path in instruction_files if path.exists())
    result["rules_defined"] = rules_defined
    result["data_available"] = rules_defined
    if not rules_defined:
        return result

    reflect_log = state_dir / "ace_reflect_log.jsonl"
    offset_file = reflect_log.with_name(reflect_log.name + ".offset")
    result["reflect_log_exists"] = reflect_log.exists()

    total_logged = 0
    if reflect_log.exists():
        try:
            total_logged = sum(1 for line in reflect_log.read_text(encoding="utf-8").splitlines() if line.strip())
        except OSError:
            total_logged = 0
    result["logged_detection_count"] = total_logged

    processed = 0
    if offset_file.exists():
        try:
            processed = int(json.loads(offset_file.read_text(encoding="utf-8")).get("processed_line_count", 0))
            result["last_apply_at"] = datetime.fromtimestamp(offset_file.stat().st_mtime, tz=timezone.utc).isoformat()
        except (OSError, ValueError, json.JSONDecodeError):
            processed = 0
    result["processed_entry_count"] = processed
    pending = max(0, total_logged - processed)
    result["pending_entry_count"] = pending

    handoff_db = state_dir / "handoff.db"
    tagged_findings = 0
    if handoff_db.exists():
        try:
            with sqlite3.connect(str(handoff_db)) as conn:
                rows = conn.execute(
                    "SELECT description FROM review_findings WHERE task_ref = ?",
                    (task_ref,),
                ).fetchall()
            tagged_findings = sum(
                1
                for (description,) in rows
                if isinstance(description, str) and _ACE_RULE_REFERENCE_RE.search(description)
            )
        except sqlite3.Error:
            tagged_findings = 0
    result["rule_tagged_findings"] = tagged_findings

    backfill_needed = tagged_findings > 0 and total_logged == 0
    result["backfill_needed"] = backfill_needed

    if total_logged > 0 and pending > 0:
        result["status"] = "detecting"
    elif processed > 0 and pending == 0:
        result["status"] = "applied"
    else:
        result["status"] = "defined"
    return result


# ---------------------------------------------------------------------------
# Lane health aggregation
# ---------------------------------------------------------------------------


def _lane_health(worker_events: list[dict]) -> dict:
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    scope_violations = sum(1 for e in worker_events if e.get("event") == WorkerEventName.SCOPE_VIOLATION)
    max_streak = 0
    for e in worker_events:
        streak = (e.get("exhaustion_streak") or {}).get("count", 0)
        if streak > max_streak:
            max_streak = streak

    review_events = [e for e in worker_events if e.get("event") == WorkerEventName.REVIEW_COMPLETE]
    total_cycles = len(review_events)
    converged = sum(1 for e in review_events if e.get("converged"))
    convergence_rate = round(converged / total_cycles, 3) if total_cycles else 0.0

    return {
        "data_available": len(worker_events) > 0,
        "total_scope_violations": scope_violations,
        "max_exhaustion_streak": max_streak,
        "convergence_rate": convergence_rate,
    }


# ---------------------------------------------------------------------------
# Phase execution timing
# ---------------------------------------------------------------------------


def _phase_timing(worker_events: list[dict]) -> dict:
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    exec_times: list[float] = []
    review_times: list[float] = []

    for e in worker_events:
        if e.get("event") == WorkerEventName.EXEC_COMPLETE:
            t = e.get("exec_seconds")
            if t is not None:
                exec_times.append(float(t))
        elif e.get("event") == WorkerEventName.REVIEW_COMPLETE:
            t = e.get("review_seconds")
            if t is not None:
                review_times.append(float(t))

    def _stats(values: list[float]) -> dict:
        if not values:
            return {"count": 0, "total": 0.0, "mean": 0.0, "max": 0.0}
        return {
            "count": len(values),
            "total": round(sum(values), 2),
            "mean": round(sum(values) / len(values), 2),
            "max": round(max(values), 2),
        }

    return {
        "data_available": len(exec_times) > 0 or len(review_times) > 0,
        "exec": _stats(exec_times),
        "review": _stats(review_times),
    }


def _slice_review_adoption(worker_events: list[dict]) -> dict:
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource, WorkerEventName  # noqa: PLC0415

    review_events = [event for event in worker_events if event.get("event") == WorkerEventName.REVIEW_COMPLETE]
    packet_backed_reviews = 0
    branch_diff_fallback_reviews = 0
    planning_reviews = 0
    branch_reviews = 0

    for event in review_events:
        if event.get("scope_source") == ReviewScopeSource.SLICE_PACKET:
            packet_backed_reviews += 1
        elif event.get("scope_source") == ReviewScopeSource.BRANCH_DIFF:
            branch_diff_fallback_reviews += 1

        if event.get("review_kind") == ReviewKind.PLANNING:
            planning_reviews += 1
        elif event.get("review_kind") == ReviewKind.BRANCH:
            branch_reviews += 1

    total_reviews = len(review_events)
    return {
        "data_available": total_reviews > 0,
        "total_reviews": total_reviews,
        "packet_backed_reviews": packet_backed_reviews,
        "branch_diff_fallback_reviews": branch_diff_fallback_reviews,
        "planning_reviews": planning_reviews,
        "branch_reviews": branch_reviews,
        "packet_backed_adoption_rate": (round(packet_backed_reviews / total_reviews, 3) if total_reviews else None),
        "branch_diff_fallback_rate": (
            round(branch_diff_fallback_reviews / total_reviews, 3) if total_reviews else None
        ),
    }


# ---------------------------------------------------------------------------
# FTS5 retrieval stats (from SQLite databases)
# ---------------------------------------------------------------------------


def _fts5_retrieval(state_dir: Path) -> dict:
    result: dict = {
        "data_available": False,
        "artifact_sources_indexed": 0,
        "artifact_chunks_fts_count": 0,
        "handoff_record_counts": {
            "decisions": 0,
            "findings": 0,
            "blockers": 0,
            "actions": 0,
        },
    }

    handoff_db = state_dir / "handoff.db"
    if handoff_db.exists():
        try:
            with sqlite3.connect(str(handoff_db)) as conn:
                for table, key in [
                    ("decisions", "decisions"),
                    ("review_findings", "findings"),
                    ("blockers", "blockers"),
                    ("next_actions", "actions"),
                ]:
                    try:
                        (count,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        result["handoff_record_counts"][key] = count
                        result["data_available"] = True
                    except sqlite3.OperationalError:
                        pass
        except sqlite3.Error:
            pass

    artifacts_db = state_dir / "mcp-artifacts.db"
    if artifacts_db.exists():
        try:
            with sqlite3.connect(str(artifacts_db)) as conn:
                try:
                    (count,) = conn.execute("SELECT COUNT(*) FROM artifact_sources").fetchone()
                    result["artifact_sources_indexed"] = count
                    result["data_available"] = True
                except sqlite3.OperationalError:
                    pass
                try:
                    (count,) = conn.execute("SELECT COUNT(*) FROM artifact_chunks_fts").fetchone()
                    result["artifact_chunks_fts_count"] = count
                    result["data_available"] = True
                except sqlite3.OperationalError:
                    pass
        except sqlite3.Error:
            pass

    return result


# ---------------------------------------------------------------------------
# Process health metrics
# ---------------------------------------------------------------------------

_MANDATORY_DECISION_HEADINGS = (
    "## Changes",
    "## Verification",
    "## Schema / Contract Changes",
    "## Open Threads",
)

_HOT_STATE_LIMITS = {
    "blockers": 5,
    "actions": 5,
    "decisions": 3,
    "tests": 3,
    "findings": 10,
}

_METRIC_WINDOW_DAYS = 30
_CTX7_LIBRARY_ID_RE = re.compile(r"ctx7 library id:\s*(?P<library_id>/[^\s`]+)", re.IGNORECASE)


def _process_health(task_ref: str, state_dir: Path, workspace_root: Path) -> dict:
    result: dict = {
        "data_available": False,
        "reopened_finding_rate": {
            "value": None,
            "reopened_findings": 0,
            "total_findings": 0,
        },
        "finding_resolution_velocity_hours": {
            "median_hours": None,
            "resolved_findings": 0,
        },
        "handoff_decision_completeness": {
            "value": None,
            "structured_decisions": 0,
            "total_decisions": 0,
        },
        "contract_co_change_signal": _contract_co_change_signal(workspace_root),
    }

    handoff_db = state_dir / "handoff.db"
    if not handoff_db.exists():
        result["data_available"] = result["contract_co_change_signal"]["data_available"]
        return result

    try:
        with sqlite3.connect(str(handoff_db)) as conn:
            conn.row_factory = sqlite3.Row

            finding_rows = conn.execute(
                """
                SELECT reopen_count, created_at, resolved_at, status
                FROM review_findings
                WHERE task_ref = ?
                """,
                (task_ref,),
            ).fetchall()
            total_findings = len(finding_rows)
            reopened_findings = sum(1 for row in finding_rows if int(row["reopen_count"] or 0) >= 1)
            if total_findings:
                result["reopened_finding_rate"] = {
                    "value": round(reopened_findings / total_findings, 3),
                    "reopened_findings": reopened_findings,
                    "total_findings": total_findings,
                }
                result["data_available"] = True

            resolution_durations_hours: list[float] = []
            for row in finding_rows:
                if str(row["status"] or "") != "fixed":
                    continue
                created_at = _parse_metric_datetime(row["created_at"])
                resolved_at = _parse_metric_datetime(row["resolved_at"])
                if created_at is None or resolved_at is None:
                    continue
                resolution_durations_hours.append(round((resolved_at - created_at).total_seconds() / 3600.0, 3))
            if resolution_durations_hours:
                result["finding_resolution_velocity_hours"] = {
                    "median_hours": round(median(resolution_durations_hours), 3),
                    "resolved_findings": len(resolution_durations_hours),
                }
                result["data_available"] = True

            decision_rows = conn.execute(
                """
                SELECT rationale
                FROM decisions
                WHERE task_ref = ?
                """,
                (task_ref,),
            ).fetchall()
            total_decisions = len(decision_rows)
            structured_decisions = sum(
                1 for row in decision_rows if _has_structured_decision_headings(str(row["rationale"] or ""))
            )
            if total_decisions:
                result["handoff_decision_completeness"] = {
                    "value": round(structured_decisions / total_decisions, 3),
                    "structured_decisions": structured_decisions,
                    "total_decisions": total_decisions,
                }
                result["data_available"] = True
    except sqlite3.Error:
        pass

    if result["contract_co_change_signal"]["data_available"]:
        result["data_available"] = True

    return result


def _handoff_memory(task_ref: str, state_dir: Path, workspace_root: Path) -> dict:
    result = {
        "data_available": False,
        "hot_state_size_bytes": 0,
        "total_decisions": 0,
        "total_findings": 0,
        "artifact_source_count": 0,
    }

    handoff_db = state_dir / "handoff.db"
    if not handoff_db.exists():
        return result

    try:
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415
        from workbay_handoff_mcp.runtime import (  # noqa: PLC0415
            configure_runtime,
            get_runtime_config,
        )

        from . import handoff_read_shapes as _handoff_read_shapes  # noqa: PLC0415

        prior_runtime = None
        try:
            prior_runtime = get_runtime_config()
        except RuntimeError:
            prior_runtime = None

        try:
            configure_runtime(RuntimeConfig.for_repo(workspace_root, state_dir=state_dir))
            # Intentional broad read: this metric tracks the serialized hot-state
            # footprint of the default bounded handoff snapshot, not the minimum
            # payload needed by a specific orchestrator caller. Post-internal
            # the handoff handler returns a native dict; we serialise it
            # ourselves below to keep the byte-count metric stable.
            # read_handoff_state validates the envelope against the
            # workbay_protocol contract before returning.
            hot_state = _handoff_read_shapes.read_handoff_state(
                **hot_state_metric_kwargs(task_ref, limits=_HOT_STATE_LIMITS)
            )
        finally:
            if prior_runtime is not None:
                configure_runtime(prior_runtime)

        result["hot_state_size_bytes"] = len(json.dumps(hot_state, sort_keys=True).encode("utf-8"))

        with sqlite3.connect(str(handoff_db)) as conn:
            (decision_count,) = conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            (finding_count,) = conn.execute(
                "SELECT COUNT(*) FROM review_findings WHERE task_ref = ?",
                (task_ref,),
            ).fetchone()
            result["total_decisions"] = int(decision_count)
            result["total_findings"] = int(finding_count)
            result["data_available"] = bool(result["total_decisions"] or result["total_findings"])
    except (json.JSONDecodeError, RuntimeError, sqlite3.Error):
        pass

    artifacts_db = state_dir / "mcp-artifacts.db"
    if artifacts_db.exists():
        try:
            with sqlite3.connect(str(artifacts_db)) as conn:
                (artifact_count,) = conn.execute("SELECT COUNT(*) FROM artifact_sources").fetchone()
                result["artifact_source_count"] = int(artifact_count)
                result["data_available"] = True
        except sqlite3.Error:
            pass

    return result


def _has_structured_decision_headings(text: str) -> bool:
    return all(heading in text for heading in _MANDATORY_DECISION_HEADINGS)


def _parse_metric_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if normalized == "":
        return None
    for parser in (
        lambda raw: datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc),
    ):
        try:
            return parser(normalized)
        except ValueError:
            continue
    return None


def _window_start(days: int = _METRIC_WINDOW_DAYS) -> datetime:
    return datetime.now(tz=timezone.utc).replace(microsecond=0) - timedelta(days=days)


def _planning_drift(task_ref: str, state_dir: Path, window_days: int = _METRIC_WINDOW_DAYS) -> dict:
    from workbay_handoff_mcp.enums import PlanCursorState  # noqa: PLC0415

    result: dict[str, object] = {
        "data_available": False,
        "window_days": window_days,
        "total": 0,
        "terminal": 0,
        "drift": None,
    }
    handoff_db = state_dir / "handoff.db"
    if not handoff_db.exists():
        return result

    terminal_states = (
        PlanCursorState.COMPLETED.value,
        PlanCursorState.SKIPPED.value,
    )
    terminal_placeholders = ", ".join("?" for _ in terminal_states)
    cutoff = _window_start(window_days).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with sqlite3.connect(str(handoff_db)) as conn:
            total_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM plan_cursors
                WHERE task_ref = ? AND updated_at >= ?
                """,
                (task_ref, cutoff),
            ).fetchone()
            terminal_row = conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM plan_cursors
                WHERE task_ref = ? AND updated_at >= ? AND state IN ({terminal_placeholders})
                """,
                (task_ref, cutoff, *terminal_states),
            ).fetchone()
    except sqlite3.Error:
        return result

    total = int(total_row[0]) if total_row else 0
    terminal = int(terminal_row[0]) if terminal_row else 0
    result["data_available"] = total > 0
    result["total"] = total
    result["terminal"] = terminal
    result["drift"] = None if total == 0 else round(1.0 - (terminal / total), 3)
    return result


def _stale_artifact_metrics(state_dir: Path, window_days: int = _METRIC_WINDOW_DAYS) -> dict:
    result: dict[str, object] = {
        "data_available": False,
        "window_days": window_days,
        "total": 0,
        "stale_count": 0,
        "stale_rate": 0.0,
    }
    artifacts_db = state_dir / "mcp-artifacts.db"
    if not artifacts_db.exists():
        return result

    cutoff = _window_start(window_days).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with sqlite3.connect(str(artifacts_db)) as conn:
            total_row = conn.execute("SELECT COUNT(*) FROM artifact_sources").fetchone()
            stale_row = conn.execute(
                "SELECT COUNT(*) FROM artifact_sources WHERE updated_at < ?",
                (cutoff,),
            ).fetchone()
    except sqlite3.Error:
        return result

    total = int(total_row[0]) if total_row else 0
    stale_count = int(stale_row[0]) if stale_row else 0
    result["data_available"] = True
    result["total"] = total
    result["stale_count"] = stale_count
    result["stale_rate"] = round(stale_count / total, 3) if total else 0.0
    return result


def _archive_rate(state_dir: Path, window_days: int = _METRIC_WINDOW_DAYS) -> dict:
    result: dict[str, object] = {
        "data_available": False,
        "window_days": window_days,
        "total_archives": 0,
        "in_window": 0,
        "mean_interval_hours": None,
    }
    handoff_db = state_dir / "handoff.db"
    if not handoff_db.exists():
        return result

    cutoff = _window_start(window_days)
    try:
        with sqlite3.connect(str(handoff_db)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT archived_at FROM task_archives ORDER BY archived_at ASC").fetchall()
    except sqlite3.Error:
        return result

    archive_times = [parsed for row in rows if (parsed := _parse_metric_datetime(row["archived_at"])) is not None]
    result["total_archives"] = len(archive_times)
    result["in_window"] = sum(1 for value in archive_times if value >= cutoff)
    intervals_hours = [
        round((later - earlier).total_seconds() / 3600.0, 3) for earlier, later in zip(archive_times, archive_times[1:])
    ]
    result["mean_interval_hours"] = round(sum(intervals_hours) / len(intervals_hours), 3) if intervals_hours else None
    result["data_available"] = len(rows) > 0
    return result


def _ctx7_adoption(task_ref: str, state_dir: Path) -> dict:
    result: dict[str, object] = {
        "data_available": False,
        "decisions_with_ctx7": 0,
        "unique_library_ids": 0,
        "reuse_ratio": None,
        "library_ids": [],
    }
    handoff_db = state_dir / "handoff.db"
    if not handoff_db.exists():
        return result

    try:
        with sqlite3.connect(str(handoff_db)) as conn:
            rows = conn.execute(
                "SELECT rationale FROM decisions WHERE task_ref = ?",
                (task_ref,),
            ).fetchall()
    except sqlite3.Error:
        return result

    decision_count = 0
    library_ids: list[str] = []
    for row in rows:
        rationale = str(row[0] or "")
        matches = [match.group("library_id") for match in _CTX7_LIBRARY_ID_RE.finditer(rationale)]
        if not matches:
            continue
        decision_count += 1
        library_ids.extend(matches)

    unique_library_ids = sorted(set(library_ids))
    result["data_available"] = len(rows) > 0
    result["decisions_with_ctx7"] = decision_count
    result["unique_library_ids"] = len(unique_library_ids)
    result["reuse_ratio"] = round(len(library_ids) / len(unique_library_ids), 3) if unique_library_ids else None
    result["library_ids"] = unique_library_ids
    return result


def _contract_co_change_signal(workspace_root: Path, commit_limit: int = 20) -> dict:
    from .review_ready import (  # noqa: PLC0415
        BOUNDARY_PREFIXES,
        CONTRACT_CHECKLIST_PATH,
        CONTRACT_PREFIXES,
    )

    result: dict[str, object] = {
        "data_available": False,
        "recent_commits_scanned": 0,
        "boundary_touching_commits": 0,
        "boundary_commits_with_contract_co_change": 0,
        "value": None,
    }

    git_dir = workspace_root / ".git"
    if not workspace_root.exists() or not git_dir.exists():
        return result

    try:
        log_output = subprocess.run(
            [
                "git",
                "-C",
                str(workspace_root),
                "log",
                "--format=commit:%H",
                f"-n{commit_limit}",
                "--name-only",
                "HEAD",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return result

    commits: list[list[str]] = []
    current_files: list[str] = []
    for raw_line in log_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("commit:"):
            if current_files:
                commits.append(current_files)
            current_files = []
            continue
        current_files.append(line)
    if current_files:
        commits.append(current_files)

    boundary_touching_commits = 0
    co_changed_commits = 0
    for changed_files in commits:
        if not changed_files:
            continue
        boundary_changed = any(path.startswith(BOUNDARY_PREFIXES) for path in changed_files)
        if not boundary_changed:
            continue
        boundary_touching_commits += 1
        contract_changed = any(
            path.startswith(CONTRACT_PREFIXES) or path == CONTRACT_CHECKLIST_PATH for path in changed_files
        )
        if contract_changed:
            co_changed_commits += 1

    result["data_available"] = bool(commits)
    result["recent_commits_scanned"] = len(commits)
    result["boundary_touching_commits"] = boundary_touching_commits
    result["boundary_commits_with_contract_co_change"] = co_changed_commits
    if boundary_touching_commits:
        result["value"] = round(co_changed_commits / boundary_touching_commits, 3)
    return result


# ---------------------------------------------------------------------------
# ACE documentation health
# ---------------------------------------------------------------------------


def _ace_documentation(instruction_files: list[Path]) -> dict:
    total_helpful = 0
    total_harmful = 0
    all_bullets: dict[str, dict] = {}
    total_lines = 0

    for fp in instruction_files:
        if fp.exists():
            total_lines += len(fp.read_text(encoding="utf-8").splitlines())
            bullets = parse_strategy_bullets(fp)
            for rule_id, data in bullets.items():
                if rule_id not in all_bullets:
                    all_bullets[rule_id] = data
                    total_helpful += data["helpful"]
                    total_harmful += data["harmful"]

    pruning_candidates = [
        rule_id for rule_id, data in all_bullets.items() if data["helpful"] == 0 and data["harmful"] >= 2
    ]

    return {
        "data_available": len(all_bullets) > 0,
        "total_strategy_bullets": len(all_bullets),
        "pruning_candidates": len(pruning_candidates),
        "pruning_candidate_ids": pruning_candidates,
        "total_helpful": total_helpful,
        "total_harmful": total_harmful,
        "instruction_file_lines": total_lines,
    }


# ---------------------------------------------------------------------------
# Snapshot assembly and persistence
# ---------------------------------------------------------------------------


def build_snapshot(
    task_ref: str,
    state_dir: Path,
    logs_dir: Path,
    instruction_files: list[Path],
) -> dict:
    worker_events = _collect_worker_events(logs_dir)
    turn_metrics = _load_turn_metrics(state_dir, task_ref)
    workspace_root = state_dir.parent

    snapshot = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "task_ref": task_ref,
        "token_burn": _token_burn(worker_events, turn_metrics),
        "context_pressure": _context_pressure(worker_events, turn_metrics),
        "preflight_observed_drift": _preflight_observed_drift(turn_metrics),
        "tool_attribution": _tool_attribution(turn_metrics),
        "ace_process_health": _ace_process_health(task_ref, state_dir, instruction_files),
        "ace_model_curation": _ace_model_curation(state_dir),
        "fts5_retrieval": _fts5_retrieval(state_dir),
        "lane_health": _lane_health(worker_events),
        "process_health": _process_health(task_ref, state_dir, workspace_root),
        "handoff_memory": _handoff_memory(task_ref, state_dir, workspace_root),
        "planning_drift": _planning_drift(task_ref, state_dir),
        "stale_artifact_rate": _stale_artifact_metrics(state_dir),
        "archive_rate": _archive_rate(state_dir),
        "ctx7_adoption": _ctx7_adoption(task_ref, state_dir),
        "phase_timing": _phase_timing(worker_events),
        "slice_review_adoption": _slice_review_adoption(worker_events),
        "ace_documentation": _ace_documentation(instruction_files),
    }
    return snapshot


def _append_snapshot(state_dir: Path, snapshot: dict) -> None:
    metrics_file = state_dir / "metrics.jsonl"
    state_dir.mkdir(parents=True, exist_ok=True)
    with metrics_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot) + "\n")


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _render_token_efficiency(tb: dict) -> list[str]:
    lines = ["", "## Token Efficiency"]
    if tb["data_available"]:
        lines += [
            f"- Total tokens: **{tb['total_tokens']:,}**",
            f"- Converged cycles: {tb['converged_cycles']} / {tb['total_review_cycles']}",
            "- Tokens per converged cycle: "
            + (f"**{tb['tokens_per_converged_cycle']:,}**" if tb["tokens_per_converged_cycle"] else "n/a"),
        ]
        if tb.get("by_usage_source"):
            usage_parts = [f"{source}={count}" for source, count in tb["by_usage_source"].items() if count]
            if usage_parts:
                lines.append(f"- Usage-source coverage: {'  '.join(usage_parts)}")
        if tb["by_lane"]:
            lines.append("- By lane:")
            for lane, tokens in sorted(tb["by_lane"].items()):
                lines.append(f"  - `{lane}`: {tokens:,}")
        return lines
    lines.append("_No worker turn events found. Data not available._")
    return lines


def _render_context_pressure(cp: dict) -> list[str]:
    lines = ["", "## Context Pressure"]
    if cp["data_available"]:
        rendered = lines + [
            f"- Latest pressure: **{cp['latest_pressure']}**",
            f"- Elevated cycle ratio: {cp['elevated_cycle_ratio']:.1%}",
            f"- High cycle ratio: {cp['high_cycle_ratio']:.1%}",
        ]
        if cp.get("pressure_by_source"):
            source_parts = [f"{source}={count}" for source, count in cp["pressure_by_source"].items() if count]
            if source_parts:
                rendered.append(f"- Pressure source coverage: {'  '.join(source_parts)}")
        return rendered
    lines.append("_No context pressure events recorded._")
    return lines


def _render_preflight_observed_drift(drift: dict) -> list[str]:
    lines = ["", "## Prompt Drift"]
    if drift.get("data_available"):
        return lines + [
            f"- Comparable turns: {drift['comparable_turns']}",
            f"- Exact preflight turns: {drift['exact_preflight_turns']}",
            f"- Estimated preflight turns: {drift['estimated_preflight_turns']}",
            "- Mean signed token drift: "
            + (f"{drift['mean_signed_token_drift']}" if drift.get("mean_signed_token_drift") is not None else "n/a"),
            "- Mean absolute token drift: "
            + (
                f"{drift['mean_absolute_token_drift']}" if drift.get("mean_absolute_token_drift") is not None else "n/a"
            ),
            "- Median absolute token drift: "
            + (
                f"{drift['median_absolute_token_drift']}"
                if drift.get("median_absolute_token_drift") is not None
                else "n/a"
            ),
            "- Max absolute token drift: "
            + (f"{drift['max_absolute_token_drift']}" if drift.get("max_absolute_token_drift") is not None else "n/a"),
        ]
    lines.append("_No comparable preflight-vs-observed prompt data recorded yet._")
    return lines


def _render_retrieval_activity(fts: dict) -> list[str]:
    lines = ["", "## Retrieval Activity (FTS5)"]
    if fts["data_available"]:
        hrc = fts["handoff_record_counts"]
        return lines + [
            f"- Artifact sources indexed: {fts['artifact_sources_indexed']}",
            f"- Artifact chunks (FTS): {fts['artifact_chunks_fts_count']}",
            f"- Handoff records: decisions={hrc['decisions']}  findings={hrc['findings']}  blockers={hrc['blockers']}  actions={hrc['actions']}",
        ]
    lines.append("_No database data available._")
    return lines


def _render_tool_attribution(tool_attribution: dict) -> list[str]:
    lines = ["", "## Tool Attribution"]
    if not tool_attribution.get("data_available"):
        lines.append("_No turn-metrics attribution data recorded yet._")
        return lines

    by_tool = tool_attribution.get("by_tool", {})
    rendered_any = False
    for tool, payload in by_tool.items():
        turns = int(payload.get("turns") or 0)
        if turns <= 0:
            continue
        rendered_any = True
        lines.append(
            f"- `{tool}`: turns={turns}  prompt_tokens={int(payload.get('prompt_tokens') or 0):,}  total_tokens={int(payload.get('total_tokens') or 0):,}"
        )
    if tool_attribution.get("turns_with_ctx7_queries"):
        lines.append(
            f"- `ctx7_query_count`: turns={tool_attribution['turns_with_ctx7_queries']}  total_queries={tool_attribution['ctx7_query_count_total']}"
        )
        rendered_any = True
    if not rendered_any:
        lines.append("_No attributed turn usage recorded yet._")
    return lines


def _render_ace_process_health(ace_health: dict) -> list[str]:
    lines = ["", "## ACE Process Health"]
    if not ace_health.get("data_available"):
        lines.append("_ACE rules are not defined in the loaded instruction files._")
        return lines

    lines.extend(
        [
            f"- Status: **{ace_health.get('status', 'defined')}**",
            f"- Rules defined: {'yes' if ace_health.get('rules_defined') else 'no'}",
            f"- Reflect log exists: {'yes' if ace_health.get('reflect_log_exists') else 'no'}",
            f"- Logged detections: {int(ace_health.get('logged_detection_count') or 0)}",
            f"- Pending entries: {int(ace_health.get('pending_entry_count') or 0)}",
            f"- Rule-tagged findings: {int(ace_health.get('rule_tagged_findings') or 0)}",
            f"- Backfill needed: {'yes' if ace_health.get('backfill_needed') else 'no'}",
        ]
    )
    if ace_health.get("last_apply_at"):
        lines.append(f"- Last apply: {ace_health['last_apply_at']}")
    return lines


def _render_ace_model_curation(curation: dict) -> list[str]:
    lines = ["", "## ACE Model Curation"]
    if curation.get("data_available"):
        return lines + [
            f"- Runs: {curation['runs']}",
            f"- Triggered runs: {curation['triggered_runs']}",
            f"- Separate token cost: {curation['total_tokens']:,}",
            f"- Latest status: {curation['latest_status'] or 'n/a'}",
            f"- Latest backend/model: {(curation['latest_backend'] or 'n/a')} / {(curation['latest_model'] or 'n/a')}",
        ]
    lines.append("_No model-backed ACE curation runs recorded. Default ACE remains local-only._")
    return lines


def _render_lane_stability(lh: dict) -> list[str]:
    lines = ["", "## Lane Stability"]
    if lh["data_available"]:
        return lines + [
            f"- Scope violations: {lh['total_scope_violations']}",
            f"- Max exhaustion streak: {lh['max_exhaustion_streak']}",
            f"- Convergence rate: {lh['convergence_rate']:.1%}",
        ]
    lines.append("_No lane event data available._")
    return lines


def _render_process_health(ph: dict) -> list[str]:
    lines = ["", "## Process Health"]
    if ph.get("data_available"):
        reopened = ph["reopened_finding_rate"]
        velocity = ph["finding_resolution_velocity_hours"]
        completeness = ph["handoff_decision_completeness"]
        contract = ph["contract_co_change_signal"]
        return lines + [
            "- Reopened finding rate: "
            + (
                f"{reopened['value']:.1%} ({reopened['reopened_findings']} / {reopened['total_findings']})"
                if reopened["value"] is not None
                else "n/a"
            ),
            "- Finding resolution velocity (median hours): "
            + (
                f"{velocity['median_hours']}h across {velocity['resolved_findings']} resolved findings"
                if velocity["median_hours"] is not None
                else "n/a"
            ),
            "- Structured handoff decision completeness: "
            + (
                f"{completeness['value']:.1%} ({completeness['structured_decisions']} / {completeness['total_decisions']})"
                if completeness["value"] is not None
                else "n/a"
            ),
            "- Contract co-change signal: "
            + (
                f"{contract['value']:.1%} ({contract['boundary_commits_with_contract_co_change']} / {contract['boundary_touching_commits']} boundary-touching commits)"
                if contract.get("value") is not None
                else "n/a"
            ),
        ]
    lines.append("_No process-health data available yet._")
    return lines


def _render_handoff_memory(hm: dict) -> list[str]:
    lines = ["", "## Handoff Memory"]
    if hm.get("data_available"):
        return lines + [
            f"- Hot-state size: {hm['hot_state_size_bytes']} bytes",
            f"- Total decisions: {hm['total_decisions']}",
            f"- Total findings: {hm['total_findings']}",
            f"- Artifact sources indexed: {hm['artifact_source_count']}",
        ]
    lines.append("_No handoff-memory data available yet._")
    return lines


def _render_planning_drift(pd: dict) -> list[str]:
    lines = ["", "## Planning Drift"]
    if pd.get("data_available"):
        return lines + [
            "- Drift ratio: " + (f"{pd['drift']:.1%}" if pd.get("drift") is not None else "n/a"),
            f"- Window: {pd['window_days']} days",
            f"- Plan cursor rows in window: {pd['total']}  terminal={pd['terminal']}",
        ]
    lines.append("_No plan-cursor data available in the evaluation window._")
    return lines


def _render_artifact_staleness(sa: dict) -> list[str]:
    lines = ["", "## Artifact Staleness"]
    if sa.get("data_available"):
        return lines + [
            f"- Window: {sa['window_days']} days",
            f"- Artifact sources: {sa['total']}",
            f"- Stale artifacts: {sa['stale_count']} ({sa['stale_rate']:.1%})",
        ]
    lines.append("_No artifact-index data available yet._")
    return lines


def _render_archive_cadence(ar: dict) -> list[str]:
    lines = ["", "## Archive Cadence"]
    if ar.get("data_available"):
        return lines + [
            f"- Window: {ar['window_days']} days",
            f"- Total archived tasks: {ar['total_archives']}",
            f"- Archived in window: {ar['in_window']}",
            "- Mean interval between archives: "
            + (f"{ar['mean_interval_hours']}h" if ar.get("mean_interval_hours") is not None else "n/a"),
        ]
    lines.append("_No archive history available yet._")
    return lines


def _render_ctx7_adoption(ctx7: dict) -> list[str]:
    lines = ["", "## ctx7 Adoption"]
    if ctx7.get("data_available"):
        lines += [
            f"- Decisions with ctx7 references: {ctx7['decisions_with_ctx7']}",
            f"- Unique library ids: {ctx7['unique_library_ids']}",
            "- Library-id reuse ratio: "
            + (f"{ctx7['reuse_ratio']:.2f}" if ctx7.get("reuse_ratio") is not None else "n/a"),
        ]
        if ctx7.get("library_ids"):
            lines.append(f"- Library ids: {', '.join(ctx7['library_ids'])}")
        return lines
    lines.append("_No decision history available for ctx7 adoption metrics._")
    return lines


def _render_phase_timing(pt: dict) -> list[str]:
    lines = ["", "## Phase Timing"]
    if pt.get("data_available"):
        exec_s = pt["exec"]
        rev_s = pt["review"]
        return lines + [
            f"- Exec cycles: {exec_s['count']}  total={exec_s['total']}s  mean={exec_s['mean']}s  max={exec_s['max']}s",
            f"- Review cycles: {rev_s['count']}  total={rev_s['total']}s  mean={rev_s['mean']}s  max={rev_s['max']}s",
        ]
    lines.append("_No exec/review timing data recorded yet._")
    return lines


def _render_slice_review_adoption(adoption: dict) -> list[str]:
    lines = ["", "## Slice Review Adoption"]
    if adoption.get("data_available"):
        return lines + [
            f"- Total review cycles: {adoption['total_reviews']}",
            "- Packet-backed latest-slice reviews: "
            + (
                f"{adoption['packet_backed_reviews']} ({adoption['packet_backed_adoption_rate']:.1%})"
                if adoption.get("packet_backed_adoption_rate") is not None
                else "n/a"
            ),
            "- Branch-diff fallback reviews: "
            + (
                f"{adoption['branch_diff_fallback_reviews']} ({adoption['branch_diff_fallback_rate']:.1%})"
                if adoption.get("branch_diff_fallback_rate") is not None
                else "n/a"
            ),
            f"- Review kinds: branch={adoption['branch_reviews']} planning={adoption['planning_reviews']}",
        ]
    lines.append("_No review-complete events recorded with scope-source metadata yet._")
    return lines


def _render_documentation_fitness(ace: dict) -> list[str]:
    lines = ["", "## Documentation Fitness (ACE)"]
    if ace["data_available"]:
        lines += [
            f"- Strategy bullets: {ace['total_strategy_bullets']}",
            f"- Total helpful: {ace['total_helpful']}  Total harmful: {ace['total_harmful']}",
            f"- Pruning candidates: {ace['pruning_candidates']}",
            f"- Instruction file lines: {ace['instruction_file_lines']}",
        ]
        if ace["pruning_candidate_ids"]:
            lines.append(f"- Candidate IDs: {', '.join(ace['pruning_candidate_ids'])}")
        return lines
    lines.append("_No ACE strategy bullets found in instruction files._")
    return lines


def render_markdown(snapshot: dict) -> str:
    lines = [
        "# ACE Metrics Snapshot",
        "",
        f"**Task**: `{snapshot['task_ref']}`  **Timestamp**: `{snapshot['timestamp']}`",
    ]
    for section_lines in (
        _render_token_efficiency(snapshot["token_burn"]),
        _render_context_pressure(snapshot["context_pressure"]),
        _render_preflight_observed_drift(snapshot.get("preflight_observed_drift", {})),
        _render_tool_attribution(snapshot.get("tool_attribution", {})),
        _render_ace_process_health(snapshot.get("ace_process_health", {})),
        _render_ace_model_curation(snapshot.get("ace_model_curation", {})),
        _render_retrieval_activity(snapshot["fts5_retrieval"]),
        _render_lane_stability(snapshot["lane_health"]),
        _render_process_health(snapshot.get("process_health", {})),
        _render_handoff_memory(snapshot.get("handoff_memory", {})),
        _render_planning_drift(snapshot.get("planning_drift", {})),
        _render_artifact_staleness(snapshot.get("stale_artifact_rate", {})),
        _render_archive_cadence(snapshot.get("archive_rate", {})),
        _render_ctx7_adoption(snapshot.get("ctx7_adoption", {})),
        _render_phase_timing(snapshot.get("phase_timing", {})),
        _render_slice_review_adoption(snapshot.get("slice_review_adoption", {})),
        _render_documentation_fitness(snapshot["ace_documentation"]),
    ):
        lines.extend(section_lines)
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Sparkline visualization helpers
# ---------------------------------------------------------------------------

_SPARK_CHARS = " \u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"


def _sparkline(values: Sequence[float]) -> str:
    """Convert a list of numeric values into a compact Unicode sparkline."""
    if not values:
        return ""
    lo, hi = min(values), max(values)
    spread = hi - lo or 1.0
    n = len(_SPARK_CHARS) - 1
    return "".join(_SPARK_CHARS[round((v - lo) / spread * n)] for v in values)


def render_sparklines(state_dir: Path, task_ref: str) -> str:
    """Read .task-state/metrics.jsonl and render time-series sparklines."""
    metrics_file = state_dir / "metrics.jsonl"
    if not metrics_file.exists():
        return f"No metrics history found at {metrics_file}\n"

    snapshots: list[dict] = []
    with metrics_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    s = json.loads(line)
                    if task_ref in ("", "unknown", s.get("task_ref", "")):
                        snapshots.append(s)
                except json.JSONDecodeError:
                    continue

    if not snapshots:
        return f"No snapshots found for task_ref={task_ref!r} in {metrics_file}\n"

    lines = [
        "# ACE Metrics Trends",
        "",
        f"**Task**: `{task_ref}`  **Snapshots**: {len(snapshots)}",
        "",
    ]

    token_series = [s.get("token_burn", {}).get("total_tokens", 0) for s in snapshots]
    pressure_series = [
        1
        if s.get("context_pressure", {}).get("latest_pressure") == "elevated"
        else 2
        if s.get("context_pressure", {}).get("latest_pressure") == "high"
        else 0
        for s in snapshots
    ]
    exec_mean_series = [s.get("phase_timing", {}).get("exec", {}).get("mean", 0.0) for s in snapshots]
    review_mean_series = [s.get("phase_timing", {}).get("review", {}).get("mean", 0.0) for s in snapshots]
    convergence_series = [s.get("lane_health", {}).get("convergence_rate", 0.0) for s in snapshots]
    reopened_series = [
        s.get("process_health", {}).get("reopened_finding_rate", {}).get("value") or 0.0 for s in snapshots
    ]
    hot_state_series = [s.get("handoff_memory", {}).get("hot_state_size_bytes", 0) for s in snapshots]
    packet_backed_series = [
        s.get("slice_review_adoption", {}).get("packet_backed_adoption_rate") or 0.0 for s in snapshots
    ]

    lines += [
        "## Token Burn",
        f"  `{_sparkline(token_series)}`",
        f"  latest={token_series[-1]:,}" if token_series else "",
        "",
        "## Context Pressure Level (0=normal 1=elevated 2=high)",
        f"  `{_sparkline(pressure_series)}`",
        "",
        "## Exec Duration (mean seconds per cycle)",
        f"  `{_sparkline(exec_mean_series)}`",
        f"  latest={exec_mean_series[-1]:.1f}s" if exec_mean_series else "",
        "",
        "## Review Duration (mean seconds per cycle)",
        f"  `{_sparkline(review_mean_series)}`",
        f"  latest={review_mean_series[-1]:.1f}s" if review_mean_series else "",
        "",
        "## Lane Convergence Rate",
        f"  `{_sparkline(convergence_series)}`",
        f"  latest={convergence_series[-1]:.1%}" if convergence_series else "",
        "",
        "## Reopened Finding Rate",
        f"  `{_sparkline(reopened_series)}`",
        f"  latest={reopened_series[-1]:.1%}" if reopened_series else "",
        "",
        "## Hot-State Size (bytes)",
        f"  `{_sparkline(hot_state_series)}`",
        f"  latest={hot_state_series[-1]:,}" if hot_state_series else "",
        "",
        "## Packet-Backed Review Adoption",
        f"  `{_sparkline(packet_backed_series)}`",
        f"  latest={packet_backed_series[-1]:.1%}" if packet_backed_series else "",
    ]

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ACE observability metrics aggregation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--task-ref", default="unknown", help="Task reference identifier")
    parser.add_argument("--state-dir", default=".task-state", help="Path to .task-state directory")
    parser.add_argument("--logs-dir", default="logs", help="Path to logs directory")
    parser.add_argument(
        "--output-format",
        choices=["json", "markdown"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--playbook-file",
        action="append",
        required=True,
        dest="playbook_files",
        help="Playbook file containing ACE strategy bullets (repeatable, required).",
    )
    parser.add_argument(
        "--sparklines",
        action="store_true",
        help="Print time-series sparklines from accumulated metrics history and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    state_dir = Path(args.state_dir)
    logs_dir = Path(args.logs_dir)
    playbook_files = [Path(path) for path in args.playbook_files]
    validate_playbook_files(playbook_files)

    if args.sparklines:
        print(render_sparklines(state_dir, args.task_ref))
        return

    snapshot = build_snapshot(
        task_ref=args.task_ref,
        state_dir=state_dir,
        logs_dir=logs_dir,
        instruction_files=playbook_files,
    )

    _append_snapshot(state_dir, snapshot)

    if args.output_format == "json":
        print(json.dumps(snapshot, indent=2))
    else:
        print(render_markdown(snapshot))


if __name__ == "__main__":
    main(sys.argv[1:])
