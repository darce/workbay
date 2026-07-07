#!/usr/bin/env python3
"""Worker daemon: poll for lane work, run implementation/review/fix cycles, emit one final handoff.

Usage:
    python3 scripts/mcp/worker_daemon.py \
        --orchestrator-root . --task-ref <task> --lane-id <lane> \
        --worktree-path ../example-repo-<lane> \
        [--single-pass] [--max-review-cycles 3] [--poll-interval 30] [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime
import fcntl
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
PACKAGE_SRC = SCRIPT_DIR.parents[1]
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from _env import WORKER_REASONING_EFFORT_CHOICES, pythonpath_env
from backend_registry import get_backend_choices
from orchestrator_helpers import _normalize_text, rotate_jsonl_if_needed

_MAX_LOG_BYTES = 1_000_000
_STATUS_FILE_VERSION = 1
_OBSERVABILITY_HISTORY_LIMIT = 20
BACKEND_CHOICES = get_backend_choices()
SESSION_MODE_CHOICES = ("fresh_turn", "shared_lane")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSONL logger
# ---------------------------------------------------------------------------


def _log(lane_id: str, log_dir: Path, level: str, event: str, **extra: Any) -> None:
    """Append one JSONL record to ``<log_dir>/worker-<lane_id>.jsonl``."""
    log_dir.mkdir(parents=True, exist_ok=True)
    entry: dict[str, Any] = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "lane": lane_id,
        "level": level,
        "event": event,
        **extra,
    }
    path = log_dir / f"worker-{lane_id}.jsonl"
    rotate_jsonl_if_needed(path, _MAX_LOG_BYTES)
    with path.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    # Also print for interactive visibility.
    preview_parts: list[str] = []
    for key in ("cycle", "elapsed_seconds", "finding_count", "passed", "interval", "pid"):
        if key in extra:
            preview_parts.append(f"{key}={extra[key]}")
    for key in ("result_path", "error", "stderr_tail", "stdout_tail"):
        value = extra.get(key)
        if not value:
            continue
        text = str(value).replace("\n", " ")
        if len(text) > 160:
            text = text[:157] + "..."
        preview_parts.append(f"{key}={text}")
    suffix = f" {' '.join(preview_parts)}" if preview_parts else ""
    print(f"[{level}] {event}{suffix}", flush=True)


# ---------------------------------------------------------------------------
# Graceful shutdown flag (set by SIGTERM handler)
# ---------------------------------------------------------------------------

_shutdown_requested: bool = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _shutdown_requested
    _shutdown_requested = True


# ---------------------------------------------------------------------------
# Exclusive per-lane lock
# ---------------------------------------------------------------------------


class WorkerLock:
    """flock-based exclusive lock so only one daemon runs per lane."""

    def __init__(self, lane_id: str, state_dir: Path) -> None:
        self._lock_path = state_dir / f"worker-{lane_id}.lock"
        self._fh: Any = None

    def acquire(self) -> bool:
        """Try to acquire the lock.  Returns True on success."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._lock_path.open("w")
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fh.write(str(json.dumps({"pid": __import__("os").getpid()})))
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
# Actionable-work detection
# ---------------------------------------------------------------------------


_NO_WORK_EXIT = 3
_WAITING_EXIT = 4


def poll_lane_state(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> str:
    """Return one of ``actionable``, ``idle``, or ``waiting``.

    Exit code 3 means the lane is idle. Exit code 4 means the worker
    already handed control back to the orchestrator and should remain
    dormant until a new dispatch arrives. Any other non-zero exit
    indicates a runtime/config error and raises ``RuntimeError`` so the
    caller can surface it instead of silently sleeping.
    """
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "lane_prompt.py"),
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
        "--lane-id",
        lane_id,
        "--worktree-path",
        str(worktree_path),
        "--check",
    ]
    env = pythonpath_env(orchestrator_root, task_ref=task_ref, lane_id=lane_id)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if result.returncode == 0:
        return "actionable"
    if result.returncode == _NO_WORK_EXIT:
        return "idle"
    if result.returncode == _WAITING_EXIT:
        return "waiting"
    raise RuntimeError(f"lane_prompt.py --check failed (exit {result.returncode}):\n{result.stderr.strip()}")


def _fetch_mcp_lane_params(orchestrator_root: Path, task_ref: str, lane_id: str) -> dict[str, Any]:
    """Fetch dynamic lane parameters (model, backend, reasoning_effort) from MCP."""
    try:
        # Avoid circular or heavy imports at module level
        from workbay_handoff_mcp import api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

        config = RuntimeConfig.for_repo(orchestrator_root)
        api.configure_runtime(config)

        data = manage_worktree_lane(operation="list", task_ref=task_ref, status="all")
        if not data.get("ok"):
            return {}

        lanes = data.get("lanes", [])
        for lane in lanes:
            if lane.get("lane_id") == lane_id:
                return {
                    "model": lane.get("model"),
                    "backend": lane.get("backend"),
                    "reasoning_effort": lane.get("reasoning_effort"),
                }
    except (ImportError, FileNotFoundError, KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("lane params unavailable for %s/%s: %s", task_ref, lane_id, exc)
    return {}


def has_actionable_work(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> bool:
    """Backwards-compatible bool wrapper used by older callers/tests."""
    return (
        poll_lane_state(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
        )
        == "actionable"
    )


# ---------------------------------------------------------------------------
# Verification (make lane-check)
# ---------------------------------------------------------------------------


def _run_lane_check(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> bool:
    """Run ``make lane-check`` and return True on success."""
    cmd = [
        "make",
        "-f",
        str(orchestrator_root / "Makefile"),
        "-C",
        str(worktree_path),
        "lane-check",
        f"TASK={task_ref}",
        f"LANE={lane_id}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Final handoff (lane_result.py handoff)
# ---------------------------------------------------------------------------


def _run_final_handoff(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    result_path: Path,
    dry_run: bool = False,
    run_id: str | None = None,
    outcome: str | None = None,
) -> int:
    """Call ``lane_result.py handoff`` for the one final report."""
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "lane_result.py"),
        "handoff",
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
        "--lane-id",
        lane_id,
        "--session",
        session,
        "--worktree-path",
        str(worktree_path),
        "--result-file",
        str(result_path),
    ]
    if outcome:
        cmd.extend(["--outcome", outcome])
    if dry_run:
        cmd.append("--dry-run")
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        stderr_tail = (result.stderr or "")[-500:]
        stdout_tail = (result.stdout or "")[-500:]
        log_dir = orchestrator_root / "logs" / "worker-daemon"
        _log(
            lane_id,
            log_dir,
            "ERROR",
            WorkerEventName.HANDOFF_SUBPROCESS_FAILED,
            exit_code=result.returncode,
            run_id=run_id,
            stderr_tail=stderr_tail,
            stdout_tail=stdout_tail,
        )
    return result.returncode


def _record_terminal_outcome(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    summary: str,
    outcome: str,
) -> None:
    """Best-effort terminal report when no result-file handoff path exists."""
    try:
        from workbay_handoff_mcp import api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

        api.configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
        response = worker_reports(
            operation="record",
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            summary=summary,
            outcome=outcome,
            merge_ready=False,
        )
        if isinstance(response, dict) and response.get("ok") is not True:
            logger.warning(
                "terminal outcome recording returned ok=false for %s/%s: %s",
                task_ref,
                lane_id,
                response.get("error") or response,
            )
    except Exception as exc:  # noqa: BLE001 - terminal outcome reporting is best-effort on shutdown
        logger.warning("terminal outcome recording skipped for %s/%s: %s", task_ref, lane_id, exc)


def _maybe_record_lane_jail_denial(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    error_text: str,
    jailed_run: bool,
) -> None:
    """Best-effort ``agent_errors`` row when a jailed lane run trips a write denial.

    implementation note / adoption C telemetry: on a nonzero agent exit whose stderr carries a
    Seatbelt file-write* denial signature (and only where the jail could have been
    active), record an ``error_class='lane_jail_denial'`` row via the same handoff
    ``agent_errors`` sink the server uses. Must never raise: telemetry cannot be
    allowed to disturb the real failure/handoff path.
    """
    try:
        from lane_jail import is_sandbox_denial  # noqa: PLC0415

        if not jailed_run or not is_sandbox_denial(error_text):
            return
        from workbay_handoff_mcp import api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        api.configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
        api.record_agent_error(
            error_class="lane_jail_denial",
            summary=f"lane {lane_id} agent run tripped a Seatbelt write-jail denial",
            detail=error_text,
            task_ref=task_ref,
            harness="worker-daemon",
        )
    except Exception as exc:  # noqa: BLE001 - best-effort telemetry on the failure path
        logger.warning("lane_jail_denial telemetry skipped for %s/%s: %s", task_ref, lane_id, exc)


def _outcome_for_result_file(path: Path) -> str | None:
    try:
        result = _load_result(path)
    except Exception:  # noqa: BLE001 - retry paths should fail closed as a terminal failure
        return "failed"
    action = result.get("handoff_action")
    if action == "merge_ready":
        return "finished"
    if action == "needs_guidance":
        return None
    return "failed"


def _cleanup_result_file(path: Path | None) -> None:
    """Delete a consumed lane result artifact if it still exists."""
    if path is None:
        return
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _require_result_path(result_path: Path | None) -> Path:
    if result_path is None:
        raise RuntimeError("Worker result path is not available for this phase.")
    return result_path


# ---------------------------------------------------------------------------
# Durable worker status
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _status_path(state_dir: Path, lane_id: str) -> Path:
    return state_dir / f"worker-{lane_id}.status.json"


def _read_worker_status(state_dir: Path, lane_id: str) -> dict[str, Any] | None:
    path = _status_path(state_dir, lane_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_worker_status(
    state_dir: Path,
    lane_id: str,
    *,
    task_ref: str | None = None,
    session: str | None = None,
    state: str,
    summary: str,
    result_path: Path | None = None,
    clear_result_path: bool = False,
    failure_stage: str | None = None,
    cycle: int | None = None,
    handoff_action: str | None = None,
    attention_required: bool = False,
    observability: dict[str, Any] | None = None,
    context_utilization: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True)
    previous = _read_worker_status(state_dir, lane_id) or {}
    payload: dict[str, Any] = {
        "version": _STATUS_FILE_VERSION,
        "lane_id": lane_id,
        "task_ref": task_ref or previous.get("task_ref"),
        "session": session or previous.get("session"),
        "state": state,
        "summary": summary,
        "attention_required": attention_required,
        "updated_at": _utcnow_iso(),
        "pid": os.getpid(),
    }
    if result_path is not None:
        payload["result_path"] = str(result_path)
    elif clear_result_path:
        payload.pop("result_path", None)
    elif "result_path" in previous and state != "handoff_failed":
        payload["result_path"] = previous["result_path"]
    if failure_stage is not None:
        payload["failure_stage"] = failure_stage
    if cycle is not None:
        payload["cycle"] = cycle
    if handoff_action is not None:
        payload["handoff_action"] = handoff_action
    if observability is not None:
        payload["observability"] = observability
    elif isinstance(previous.get("observability"), dict):
        payload["observability"] = previous["observability"]
    if context_utilization is not None:
        payload["context_utilization_latest"] = context_utilization
    elif isinstance(previous.get("context_utilization_latest"), dict):
        payload["context_utilization_latest"] = previous["context_utilization_latest"]
    _status_path(state_dir, lane_id).write_text(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def _observability_entry(
    *,
    task_ref: str,
    lane_id: str,
    cycle: int,
    phase: str,
    backend: str,
    model: str | None = None,
    requested_reasoning_effort: str,
    effective_reasoning_effort: str,
    telemetry: dict[str, Any],
    context_utilization: dict[str, Any] | None = None,
) -> dict[str, Any]:
    token_usage = telemetry.get("token_usage")
    total_usage = token_usage.get("total") if isinstance(token_usage, dict) else None
    last_usage = token_usage.get("last") if isinstance(token_usage, dict) else None
    response_model = _normalize_text(telemetry.get("response_model")) or model
    effective_reasoning = _normalize_text(telemetry.get("reasoning_effort")) or effective_reasoning_effort
    entry: dict[str, Any] = {
        "recorded_at": _utcnow_iso(),
        "task_ref": task_ref,
        "lane_id": lane_id,
        "cycle": cycle,
        "phase": phase,
        "backend": backend,
        "model": response_model,
        "requested_reasoning_effort": requested_reasoning_effort,
        "effective_reasoning_effort": effective_reasoning,
        "thread_id": telemetry.get("thread_id"),
        "turn_id": telemetry.get("turn_id"),
        "response_model": response_model,
        "token_usage": token_usage,
        "token_usage_totals": {
            "input_tokens": last_usage.get("input_tokens") if isinstance(last_usage, dict) else None,
            "output_tokens": last_usage.get("output_tokens") if isinstance(last_usage, dict) else None,
            "cached_input_tokens": (last_usage.get("cached_input_tokens") if isinstance(last_usage, dict) else None),
            "total_tokens": total_usage.get("total_tokens") if isinstance(total_usage, dict) else None,
            "reasoning_output_tokens": (
                total_usage.get("reasoning_output_tokens") if isinstance(total_usage, dict) else None
            ),
            "usage_source": token_usage.get("usage_source") if isinstance(token_usage, dict) else None,
            "model_context_window": (
                token_usage.get("model_context_window") if isinstance(token_usage, dict) else None
            ),
        },
    }
    if context_utilization is not None:
        entry["context_utilization"] = context_utilization
    return entry


def _merge_observability(
    existing: dict[str, Any] | None,
    *,
    entry: dict[str, Any],
) -> dict[str, Any]:
    history = (
        list(existing.get("history", []))
        if isinstance(existing, dict) and isinstance(existing.get("history"), list)
        else []
    )
    history.append(entry)
    if len(history) > _OBSERVABILITY_HISTORY_LIMIT:
        history = history[-_OBSERVABILITY_HISTORY_LIMIT:]
    by_phase = (
        dict(existing.get("by_phase", {}))
        if isinstance(existing, dict) and isinstance(existing.get("by_phase"), dict)
        else {}
    )
    phase = str(entry.get("phase") or "").strip()
    if phase:
        by_phase[phase] = entry
    return {
        "latest": entry,
        "by_phase": by_phase,
        "history": history,
    }


@dataclass
class ObservabilityContext:
    __module__ = "builtins"
    requested_reasoning_effort: str
    effective_reasoning_effort: str
    telemetry: dict[str, Any]
    state: str
    summary: str
    result_path: Path | None = None
    handoff_action: str | None = None
    attention_required: bool = False
    context_utilization: dict[str, Any] | None = None


def _accumulate_run_ctx_tokens(run_ctx: "WorkerRunContext | None", entry: dict[str, Any]) -> None:
    if run_ctx is None:
        return
    totals = entry.get("token_usage_totals") or {}
    tokens = int(totals.get("total_tokens") or 0)
    if tokens > 0:
        run_ctx.cumulative_tokens += tokens
        return
    budget = run_ctx.config.token_budget
    if budget is not None and budget > 0 and not run_ctx.token_usage_absent_warned:
        run_ctx.token_usage_absent_warned = True
        logger.warning(
            "token_budget=%s set for %s/%s but a turn reported no token usage; "
            "the per-lane token governor may be unenforceable for this backend/session_mode.",
            budget,
            run_ctx.config.task_ref,
            run_ctx.config.lane_id,
        )


def _record_observability(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    cycle: int,
    phase: str,
    backend: str,
    model: str | None = None,
    obs_ctx: ObservabilityContext,
    run_ctx: "WorkerRunContext | None" = None,
) -> dict[str, Any]:
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    requested_reasoning_effort = obs_ctx.requested_reasoning_effort
    effective_reasoning_effort = obs_ctx.effective_reasoning_effort
    telemetry = obs_ctx.telemetry
    state = obs_ctx.state
    summary = obs_ctx.summary
    result_path = obs_ctx.result_path
    handoff_action = obs_ctx.handoff_action
    attention_required = obs_ctx.attention_required
    context_utilization = obs_ctx.context_utilization
    state_dir = orchestrator_root / ".task-state"
    log_dir = orchestrator_root / "logs" / "worker-daemon"
    previous = _read_worker_status(state_dir, lane_id) or {}
    entry = _observability_entry(
        task_ref=task_ref,
        lane_id=lane_id,
        cycle=cycle,
        phase=phase,
        backend=backend,
        model=model,
        requested_reasoning_effort=requested_reasoning_effort,
        effective_reasoning_effort=effective_reasoning_effort,
        telemetry=telemetry,
        context_utilization=context_utilization,
    )
    observability = _merge_observability(previous.get("observability"), entry=entry)
    _write_worker_status(
        state_dir,
        lane_id,
        task_ref=task_ref,
        session=session,
        state=state,
        summary=summary,
        result_path=result_path,
        cycle=cycle,
        handoff_action=handoff_action,
        attention_required=attention_required,
        observability=observability,
    )
    _log(
        lane_id,
        log_dir,
        "INFO",
        WorkerEventName.SUBAGENT_TURN_OBSERVED,
        cycle=cycle,
        phase=phase,
        backend=backend,
        model=model,
        requested_reasoning_effort=requested_reasoning_effort,
        effective_reasoning_effort=effective_reasoning_effort,
        token_usage=entry["token_usage"],
        token_usage_totals=entry["token_usage_totals"],
        total_tokens=entry["token_usage_totals"]["total_tokens"],
        reasoning_output_tokens=entry["token_usage_totals"]["reasoning_output_tokens"],
        thread_id=entry.get("thread_id"),
        turn_id=entry.get("turn_id"),
    )
    _record_token_usage_to_handoff(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        cycle=cycle,
        phase=phase,
        backend=backend,
        model=model,
        entry=entry,
    )
    _accumulate_run_ctx_tokens(run_ctx, entry)
    return entry


def _record_token_budget_blocker(
    *,
    orchestrator_root: Path,
    task_ref: str,
    description: str,
) -> None:
    """Best-effort MCP blocker for per-lane token-budget governor stops.

    Must never raise: a failure here cannot be allowed to abort the governor's
    real preserve-work / handoff path in ``_handle_token_budget_exceeded``.
    """
    try:
        from workbay_handoff_mcp import api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        config = RuntimeConfig.for_repo(orchestrator_root)
        api.configure_runtime(config)
        api.report_blocker(
            operation="add",
            description=description,
            task_ref=task_ref,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort; must not abort the governor handoff
        logger.warning("token_budget blocker recording skipped for %s: %s", task_ref, exc)


def _record_token_usage_to_handoff(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    cycle: int,
    phase: str,
    backend: str,
    model: str | None,
    entry: dict[str, Any],
) -> None:
    """Best-effort recording of token usage to MCP handoff as a decision."""
    from workbay_handoff_mcp.enums import normalize_model_label  # noqa: PLC0415

    totals = entry.get("token_usage_totals") or {}
    total_tokens = totals.get("total_tokens")
    if not total_tokens:
        return
    try:
        from workbay_handoff_mcp import api  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        config = RuntimeConfig.for_repo(orchestrator_root)
        api.configure_runtime(config)
        from workbay_orchestrator_mcp.lanes import (  # noqa: PLC0415
            PromptMetrics,
            TokenUsage,
            turn_metrics,
        )

        reasoning_tokens = totals.get("reasoning_output_tokens") or 0
        token_usage = entry.get("token_usage") or {}
        last = token_usage.get("last") or {}
        context_utilization = entry.get("context_utilization") or {}
        observed_model = _normalize_text(entry.get("response_model")) or model
        effective_reasoning = _normalize_text(entry.get("effective_reasoning_effort"))
        model_label = normalize_model_label(observed_model)
        actor_payload = api.build_write_actor(
            model=observed_model,
            model_label=model_label,
            reasoning_level=effective_reasoning,
            lane_id=lane_id,
        )
        actor = api.WriteActorInput.model_validate(actor_payload)

        rationale = (
            f"cycle={cycle} phase={phase} backend={backend} model={observed_model or 'default'} "
            f"total_tokens={total_tokens} "
            f"input={last.get('input_tokens', 'n/a')} "
            f"output={last.get('output_tokens', 'n/a')} "
            f"cached={last.get('cached_input_tokens', 'n/a')} "
            f"reasoning={reasoning_tokens} "
            f"context_window={token_usage.get('model_context_window', 'n/a')}"
        )
        api.record_decision(
            session=session,
            decision=f"token_usage_c{cycle}_{phase}",
            rationale=rationale,
            actor=actor,
        )
        turn_metrics(
            operation="record",
            task_ref=task_ref,
            session=session,
            phase=phase,
            backend=backend,
            cycle=cycle,
            lane_id=lane_id,
            model=observed_model,
            thread_id=entry.get("thread_id"),
            turn_id=entry.get("turn_id"),
            token_usage=TokenUsage(
                input_tokens=last.get("input_tokens"),
                output_tokens=last.get("output_tokens"),
                cached_input_tokens=last.get("cached_input_tokens"),
                reasoning_output_tokens=totals.get("reasoning_output_tokens"),
                total_tokens=total_tokens,
                usage_source=token_usage.get("usage_source") or "observed",
            ),
            prompt_metrics=PromptMetrics(
                model_context_window=token_usage.get("model_context_window"),
                prompt_tokens=context_utilization.get("prompt_tokens"),
                prompt_chars=context_utilization.get("prompt_chars"),
                prompt_token_source=context_utilization.get("usage_source"),
                utilization_ratio=context_utilization.get("utilization_ratio"),
                domain_signal_ratio=context_utilization.get("domain_signal_ratio"),
                pressure_level=context_utilization.get("pressure_level") or context_utilization.get("pressure"),
            ),
            attribution=context_utilization.get("attribution"),
            section_sizes=context_utilization.get("section_sizes"),
            raw_usage=token_usage,
            actor=actor_payload,
        )
    except (RuntimeError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
        logger.warning("telemetry turn-metrics logging skipped for %s/%s: %s", task_ref, lane_id, exc)


# ---------------------------------------------------------------------------
# Result file helpers
# ---------------------------------------------------------------------------


def _load_result(path: Path) -> dict[str, Any]:
    raw = path.read_text()
    return json.loads(raw)


def _patch_result(path: Path, overrides: dict[str, Any]) -> None:
    """Merge overrides into the result file on disk."""
    data = _load_result(path)
    data.update(overrides)
    path.write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Per-session hardening helpers
# ---------------------------------------------------------------------------


def _finding_stable_id(finding: dict[str, Any]) -> str:
    """Derive a stable ID for a review finding to track recurrence across cycles."""
    return (
        f"{finding.get('severity', '')}:"
        f"{finding.get('category', '')}:"
        f"{finding.get('file_path', '')}:"
        f"{finding.get('line_start', 0)}"
    )


def _compute_finding_diff(
    prev_finding_ids: set[str],
    current_findings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare current findings against a previous set of stable IDs.

    Returns a dict with three keys:
    - ``new``: findings not present in ``prev_finding_ids``
    - ``recurring``: findings that were already in ``prev_finding_ids``
    - ``resolved_count``: number of IDs from ``prev_finding_ids`` not re-found
    """
    current_ids = {_finding_stable_id(f) for f in current_findings}
    new_findings = [f for f in current_findings if _finding_stable_id(f) not in prev_finding_ids]
    recurring = [f for f in current_findings if _finding_stable_id(f) in prev_finding_ids]
    resolved_count = len(prev_finding_ids - current_ids)
    return {
        "new": new_findings,
        "recurring": recurring,
        "resolved_count": resolved_count,
    }


def _update_exhaustion_streak(state_dir: Path, lane_id: str, run_id: str) -> int:
    """Increment and return the exhaustion streak counter for the current daemon session.

    The counter is scoped to ``run_id`` so a fresh daemon session always
    starts from zero even if the status file persists from a previous run.
    """
    status = _read_worker_status(state_dir, lane_id) or {}
    streak_info = status.get("exhaustion_streak")
    if not isinstance(streak_info, dict) or streak_info.get("run_id") != run_id:
        streak_info = {"run_id": run_id, "count": 0}
    streak_info["count"] = int(streak_info.get("count") or 0) + 1
    status["exhaustion_streak"] = streak_info
    _status_path(state_dir, lane_id).parent.mkdir(parents=True, exist_ok=True)
    _status_path(state_dir, lane_id).write_text(json.dumps(status, indent=2, sort_keys=True))
    return streak_info["count"]


def _reset_exhaustion_streak(state_dir: Path, lane_id: str, run_id: str) -> None:
    """Reset exhaustion streak to zero after successful review convergence."""
    status = _read_worker_status(state_dir, lane_id) or {}
    status["exhaustion_streak"] = {"run_id": run_id, "count": 0}
    _status_path(state_dir, lane_id).parent.mkdir(parents=True, exist_ok=True)
    _status_path(state_dir, lane_id).write_text(json.dumps(status, indent=2, sort_keys=True))


def _check_token_burn(
    *,
    state_dir: Path,
    lane_id: str,
    run_id: str,
    threshold: int,
    log_dir: Path,
) -> bool:
    """Emit a ``token_burn_warning`` event if cumulative token usage exceeds threshold.

    Returns True if the threshold was exceeded.
    """
    status = _read_worker_status(state_dir, lane_id) or {}
    obs = status.get("observability") or {}
    history = obs.get("history") if isinstance(obs, dict) else None
    if not isinstance(history, list):
        return False
    cumulative = sum(
        int((entry.get("token_usage_totals") or {}).get("total_tokens") or 0)
        for entry in history
        if isinstance(entry, dict)
    )
    if cumulative < threshold:
        return False
    entry: dict[str, Any] = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "lane": lane_id,
        "level": "WARNING",
        "event": "token_burn_warning",
        "run_id": run_id,
        "cumulative_tokens": cumulative,
        "threshold": threshold,
    }
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"worker-{lane_id}.jsonl"
    rotate_jsonl_if_needed(path, _MAX_LOG_BYTES)
    with path.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    print(
        f"[WARNING] token_burn_warning cumulative_tokens={cumulative} threshold={threshold}",
        flush=True,
    )
    return True


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------


def _resolve_grok_cycle_bounds(config: WorkerConfig) -> WorkerConfig:
    """Derive grok single-cycle max_turns/timeout from token_budget when unset."""
    if config.backend != "grok-cli" or config.token_budget is None or config.token_budget <= 0:
        return config
    if config.grok_max_turns is not None and config.grok_timeout is not None:
        return config
    from offload_preflight import derive_grok_single_cycle_bounds  # noqa: PLC0415

    bounds = derive_grok_single_cycle_bounds(config.token_budget)
    return replace(
        config,
        grok_max_turns=config.grok_max_turns if config.grok_max_turns is not None else bounds["max_turns"],
        grok_timeout=config.grok_timeout if config.grok_timeout is not None else bounds["timeout"],
    )


@dataclass
class WorkerConfig:
    __module__ = "builtins"
    orchestrator_root: Path
    task_ref: str
    lane_id: str
    session: str
    worktree_path: Path
    max_review_cycles: int = 3
    poll_interval: int = 30
    single_pass: bool = False
    backend: str = "codex-cli"
    session_mode: str = "fresh_turn"
    reasoning_effort: str = "inherit"
    model: str | None = None
    codex_bin: str | None = None
    codex_args: list[str] | None = None
    grok_bin: str | None = None
    grok_args: list[str] | None = None
    grok_max_turns: int | None = None
    grok_timeout: int | None = None
    dry_run: bool = False
    token_budget: int | None = None


@dataclass
class WorkerRunContext:
    """Mutable per-cycle state threaded through the worker phase functions."""

    __module__ = "builtins"
    # Stable references (set once, read by all phases)
    config: WorkerConfig
    log_dir: Path
    state_dir: Path
    run_id: str
    token_burn_threshold: int
    log: Any
    # Dynamic per-cycle params (may be overridden by MCP each cycle)
    backend: str
    model: "str | None"
    reasoning_effort: str
    # Per-cycle mutable state
    cycle: int = 0
    final_result_path: "Path | None" = None
    last_findings: "list[dict[str, Any]]" = field(default_factory=list)
    prev_finding_ids: "set[str]" = field(default_factory=set)
    handoff_exit: int = 1
    previous_run_exhausted: bool = False
    cumulative_tokens: int = 0
    token_budget_tripped: bool = False
    token_usage_absent_warned: bool = False
    # Cycle-level reasoning effort (resolved per-cycle, set by _execute_phase)
    cycle_reasoning_effort: "str | None" = None
    execution_requested_effort: str = "inherit"
    execution_effective_effort: str = "inherit"


def _poll_phase(ctx: WorkerRunContext) -> "str | None":
    """Poll lane state.  Returns the lane_state string, or raises on hard errors.

    Returns ``None`` if the caller should skip to the next poll cycle
    (i.e. lane is not actionable).  Returns ``"actionable"`` to proceed.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = ctx.config
    try:
        lane_state = poll_lane_state(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            worktree_path=config.worktree_path,
        )
    except RuntimeError as exc:
        ctx.log("ERROR", WorkerEventName.POLL_ERROR, error=str(exc))
        return None
    return lane_state


def _execute_phase(ctx: WorkerRunContext) -> bool:
    """Run the implementation pass for the current cycle.

    Returns True on success, False if execution failed (caller should break
    the cycle loop).  On success, ``ctx.final_result_path`` is updated and
    ``ctx.cycle_reasoning_effort`` / effort strings are set.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = ctx.config
    from backend_registry import get_adapter  # noqa: PLC0415
    from lane_exec import build_fix_prompt, run_lane_exec  # noqa: PLC0415

    # Fetch dynamic overrides from MCP at the start of each cycle
    mcp_params = _fetch_mcp_lane_params(config.orchestrator_root, config.task_ref, config.lane_id)
    if mcp_params.get("backend"):
        ctx.log("INFO", WorkerEventName.MCP_BACKEND_OVERRIDE, old=ctx.backend, new=mcp_params["backend"])
        ctx.backend = str(mcp_params["backend"])
    if mcp_params.get("model"):
        ctx.log("INFO", WorkerEventName.MCP_MODEL_OVERRIDE, old=ctx.model, new=mcp_params["model"])
        ctx.model = str(mcp_params["model"])
    if mcp_params.get("reasoning_effort"):
        ctx.log(
            "INFO", WorkerEventName.MCP_EFFORT_OVERRIDE, old=ctx.reasoning_effort, new=mcp_params["reasoning_effort"]
        )
        ctx.reasoning_effort = str(mcp_params["reasoning_effort"])

    ctx.log("INFO", WorkerEventName.CYCLE_START, cycle=ctx.cycle)
    _write_worker_status(
        ctx.state_dir,
        config.lane_id,
        task_ref=config.task_ref,
        session=config.session,
        state="executing",
        summary=f"Worker execution cycle {ctx.cycle + 1} is running.",
        cycle=ctx.cycle,
    )

    # Build fix prompt override for cycles after the first
    prompt_override = None
    if ctx.cycle > 0 and ctx.final_result_path and ctx.last_findings:
        base_prompt_cmd = [
            sys.executable,
            str(SCRIPT_DIR / "lane_prompt.py"),
            "--orchestrator-root",
            str(config.orchestrator_root),
            "--task-ref",
            config.task_ref,
            "--lane-id",
            config.lane_id,
            "--worktree-path",
            str(config.worktree_path),
        ]
        env = pythonpath_env(config.orchestrator_root, task_ref=config.task_ref, lane_id=config.lane_id)
        base_result = subprocess.run(base_prompt_cmd, capture_output=True, text=True, check=False, env=env)
        if base_result.returncode == 0:
            prompt_override = build_fix_prompt(base_result.stdout, ctx.last_findings)
        else:
            ctx.log(
                "WARNING",
                WorkerEventName.FIX_PROMPT_FAILED,
                cycle=ctx.cycle,
                error=(base_result.stderr or base_result.stdout or "").strip()[:200],
            )

    adapter_kwargs: dict[str, Any] = {}
    if ctx.backend == "codex-cli":
        adapter_kwargs = {"codex_bin": config.codex_bin, "codex_args": config.codex_args}
    elif ctx.backend == "grok-cli":
        adapter_kwargs = {"grok_bin": config.grok_bin, "grok_args": config.grok_args}
        if config.grok_max_turns is not None:
            adapter_kwargs["max_turns"] = config.grok_max_turns
        if config.grok_timeout is not None:
            adapter_kwargs["timeout"] = config.grok_timeout

    try:
        adapter = get_adapter(ctx.backend, **adapter_kwargs)
        cycle_reasoning_effort, effort_reasons = adapter.resolve_reasoning_effort(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            requested=ctx.reasoning_effort,
            cycle=ctx.cycle,
            prompt_override=prompt_override,
            previous_run_exhausted=ctx.previous_run_exhausted,
        )
    except Exception as exc:
        ctx.log("ERROR", WorkerEventName.EXEC_FAILED, error=str(exc), cycle=ctx.cycle)
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="execution_failed",
            summary=f"Worker execution failed before launch: {str(exc)[:200]}",
            failure_stage="execute",
            cycle=ctx.cycle,
            attention_required=True,
        )
        return False
    ctx.log(
        "INFO",
        WorkerEventName.REASONING_EFFORT_SELECTED,
        cycle=ctx.cycle,
        requested_reasoning_effort=ctx.reasoning_effort,
        effective_reasoning_effort=cycle_reasoning_effort or "inherit",
        reasons="; ".join(effort_reasons),
    )
    ctx.cycle_reasoning_effort = cycle_reasoning_effort
    ctx.execution_requested_effort = str(ctx.reasoning_effort or "inherit")
    ctx.execution_effective_effort = cycle_reasoning_effort or "inherit"

    def _worker_progress(event: str, **kw: Any) -> None:
        if event == WorkerEventName.SUBAGENT_TURN_COMPLETE:
            phase = str(kw.get("phase") or "execution")
            phase_state = "reviewing" if phase == "review" else "executing"
            _record_observability(
                orchestrator_root=config.orchestrator_root,
                task_ref=config.task_ref,
                lane_id=config.lane_id,
                session=config.session,
                cycle=ctx.cycle,
                phase=phase,
                backend=str(kw.get("backend") or ctx.backend),
                model=ctx.model,
                obs_ctx=ObservabilityContext(
                    requested_reasoning_effort=ctx.execution_requested_effort,
                    effective_reasoning_effort=ctx.execution_effective_effort,
                    telemetry=kw,
                    state=phase_state,
                    summary=f"Worker {phase} telemetry captured for cycle {ctx.cycle + 1}.",
                    result_path=ctx.final_result_path,
                ),
                run_ctx=ctx,
            )
            exceeded = _check_token_burn(
                state_dir=ctx.state_dir,
                lane_id=config.lane_id,
                run_id=ctx.run_id,
                threshold=ctx.token_burn_threshold,
                log_dir=ctx.log_dir,
            )
            if exceeded:
                _write_worker_status(
                    ctx.state_dir,
                    config.lane_id,
                    task_ref=config.task_ref,
                    session=config.session,
                    state=phase_state,
                    summary=(
                        f"Token burn threshold ({ctx.token_burn_threshold:,} tokens) exceeded;"
                        " manual attention may be required."
                    ),
                    result_path=ctx.final_result_path,
                    cycle=ctx.cycle,
                    attention_required=True,
                )
        else:
            ctx.log("INFO", event, cycle=ctx.cycle, **kw)

    _exec_start = time.monotonic()
    try:
        ctx.log("INFO", WorkerEventName.EXEC_START, cycle=ctx.cycle, worktree_path=str(config.worktree_path))
        ctx.final_result_path = run_lane_exec(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            worktree_path=config.worktree_path,
            backend=ctx.backend,
            session_mode=config.session_mode,
            reasoning_effort=ctx.cycle_reasoning_effort,
            model=ctx.model,
            codex_bin=config.codex_bin,
            codex_args=config.codex_args,
            grok_bin=config.grok_bin,
            grok_args=config.grok_args,
            prompt_override=prompt_override,
            progress_callback=_worker_progress,
            dry_run=config.dry_run,
        )
    except Exception as exc:
        ctx.log("ERROR", WorkerEventName.EXEC_FAILED, error=str(exc), cycle=ctx.cycle)
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="execution_failed",
            summary=f"Worker execution failed: {str(exc)[:200]}",
            failure_stage="execute",
            cycle=ctx.cycle,
            attention_required=True,
        )
        _maybe_record_lane_jail_denial(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            error_text=str(exc),
            jailed_run=bool(getattr(exc, "jailed", False)),
        )
        _record_terminal_outcome(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            summary=f"Worker execution failed: {str(exc)[:200]}",
            outcome=None,
        )
        return False

    exec_seconds = round(time.monotonic() - _exec_start, 2)
    ctx.log(
        "INFO",
        WorkerEventName.EXEC_COMPLETE,
        result_path=str(ctx.final_result_path),
        cycle=ctx.cycle,
        exec_seconds=exec_seconds,
    )

    # Emit artifact_indexed event when lane_exec compressed a large details field
    final_result_path = _require_result_path(ctx.final_result_path)
    result = _load_result(final_result_path)
    _details_ref = result.get("details_artifact_ref")
    if _details_ref is not None:
        ctx.log(
            "INFO",
            WorkerEventName.ARTIFACT_INDEXED,
            cycle=ctx.cycle,
            details_artifact_ref=_details_ref,
            lane_id=config.lane_id,
            task_ref=config.task_ref,
        )

    # Emit context_pressure event when the prompt was under elevated or high pressure
    _ctx_util = result.get("context_utilization")
    if isinstance(_ctx_util, dict):
        _pressure = str(_ctx_util.get("pressure") or "normal")
        if _pressure in ("elevated", "high"):
            ctx.log(
                "WARNING",
                WorkerEventName.CONTEXT_PRESSURE,
                cycle=ctx.cycle,
                pressure=_pressure,
                utilization_ratio=_ctx_util.get("utilization_ratio"),
                domain_signal_ratio=_ctx_util.get("domain_signal_ratio"),
                prompt_tokens_approx=_ctx_util.get("prompt_tokens_approx"),
            )
            _write_worker_status(
                ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="executing",
                summary=f"Context pressure is {_pressure}; utilization={_ctx_util.get('utilization_ratio')}.",
                cycle=ctx.cycle,
                context_utilization=_ctx_util,
            )
        _record_observability(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            cycle=ctx.cycle,
            phase="context_freshness",
            backend=ctx.backend,
            model=ctx.model,
            obs_ctx=ObservabilityContext(
                requested_reasoning_effort=ctx.execution_requested_effort,
                effective_reasoning_effort=ctx.execution_effective_effort,
                telemetry={},
                state="executing",
                summary=f"Context-freshness metrics recorded for cycle {ctx.cycle + 1}.",
                result_path=ctx.final_result_path,
                context_utilization=_ctx_util,
            ),
            run_ctx=ctx,
        )

    return True


def _review_phase(ctx: WorkerRunContext) -> "dict[str, Any]":
    """Run the self-review pass.  Returns the review_output dict, or raises on failure.

    Also handles the needs_guidance early-exit path: if the execution result
    already signals ``needs_guidance`` or a scope violation, this function
    performs the handoff and raises ``StopIteration`` so the caller breaks
    the review cycle cleanly.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = ctx.config
    from review_runner import run_review  # noqa: PLC0415

    final_result_path = _require_result_path(ctx.final_result_path)
    result = _load_result(final_result_path)

    # Check for needs_guidance
    if result.get("handoff_action") == "needs_guidance":
        ctx.log("INFO", WorkerEventName.NEEDS_GUIDANCE, cycle=ctx.cycle)
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="handoff",
            summary="Worker is handing a blocked/needs-guidance result back to the orchestrator.",
            result_path=final_result_path,
            cycle=ctx.cycle,
            handoff_action="needs_guidance",
        )
        ctx.handoff_exit = _run_final_handoff(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            worktree_path=config.worktree_path,
            result_path=final_result_path,
            dry_run=config.dry_run,
            run_id=ctx.run_id,
            outcome=None,
        )
        if ctx.handoff_exit == 0:
            _cleanup_result_file(final_result_path)
            _write_worker_status(
                ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="waiting_for_orchestrator",
                summary="Blocked worker handoff submitted; waiting for orchestrator guidance.",
                handoff_action="needs_guidance",
                clear_result_path=True,
            )
        else:
            _write_worker_status(
                ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="handoff_failed",
                summary="The final worker handoff failed; the saved lane result must be retried without re-running execution.",
                result_path=final_result_path,
                failure_stage="final_handoff",
                cycle=ctx.cycle,
                handoff_action="needs_guidance",
                attention_required=True,
            )
            ctx.log("ERROR", WorkerEventName.HANDOFF_FAILED, cycle=ctx.cycle, result_path=str(final_result_path))
        raise StopIteration("needs_guidance")

    # Scope violation gate
    if result.get("scope_violation"):
        scope_violations = result.get("scope_violations", [])
        ctx.log("WARNING", WorkerEventName.SCOPE_VIOLATION, cycle=ctx.cycle, violations=scope_violations)
        _record_observability(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            cycle=ctx.cycle,
            phase="scope_check",
            backend=ctx.backend,
            model=ctx.model or "unknown",
            obs_ctx=ObservabilityContext(
                requested_reasoning_effort=ctx.execution_requested_effort,
                effective_reasoning_effort=ctx.execution_effective_effort,
                telemetry={"scope_violations": scope_violations},
                state="scope_violation",
                summary=f"Scope violation: {len(scope_violations)} file(s) outside owned_paths.",
                result_path=final_result_path,
            ),
            run_ctx=ctx,
        )
        _patch_result(
            final_result_path,
            {
                "handoff_action": "needs_guidance",
                "blockers": [
                    f"Scope violation: {len(scope_violations)} file(s) modified outside owned_paths: "
                    + str(scope_violations[:5])
                ],
            },
        )
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="handoff",
            summary=(
                f"Scope violation detected ({len(scope_violations)} file(s));"
                " handing blocked result back to orchestrator."
            ),
            result_path=final_result_path,
            cycle=ctx.cycle,
            handoff_action="needs_guidance",
            attention_required=True,
        )
        ctx.handoff_exit = _run_final_handoff(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            worktree_path=config.worktree_path,
            result_path=final_result_path,
            dry_run=config.dry_run,
            run_id=ctx.run_id,
            outcome=None,
        )
        if ctx.handoff_exit == 0:
            _cleanup_result_file(final_result_path)
            _write_worker_status(
                ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="waiting_for_orchestrator",
                summary="Scope-violation blocked handoff submitted; waiting for orchestrator guidance.",
                handoff_action="needs_guidance",
                clear_result_path=True,
            )
        else:
            _write_worker_status(
                ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="handoff_failed",
                summary="Scope-violation handoff failed; saved result must be retried.",
                result_path=final_result_path,
                failure_stage="final_handoff",
                cycle=ctx.cycle,
                handoff_action="needs_guidance",
                attention_required=True,
            )
            ctx.log("ERROR", WorkerEventName.HANDOFF_FAILED, cycle=ctx.cycle, result_path=str(final_result_path))
        raise StopIteration("scope_violation")

    # Self-review pass
    ctx.log("INFO", WorkerEventName.REVIEW_START, cycle=ctx.cycle)
    _write_worker_status(
        ctx.state_dir,
        config.lane_id,
        task_ref=config.task_ref,
        session=config.session,
        state="reviewing",
        summary=f"Worker review cycle {ctx.cycle + 1} is checking the latest lane changes.",
        result_path=final_result_path,
        cycle=ctx.cycle,
    )
    _review_start = time.monotonic()
    try:
        review_output = run_review(
            worktree_path=config.worktree_path,
            lane_id=config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            orchestrator_root=config.orchestrator_root,
            backend=ctx.backend,
            reasoning_effort=ctx.cycle_reasoning_effort,
            model=ctx.model,
            # Same per-backend binary/args the execute phase used, so the review
            # pass does not fall back to a different grok build or crash on a
            # non-PATH binary (s6-a-002).
            codex_bin=config.codex_bin,
            codex_args=config.codex_args,
            grok_bin=config.grok_bin,
            grok_args=config.grok_args,
            grok_max_turns=config.grok_max_turns,
            grok_timeout=config.grok_timeout,
            record_findings=True,
            dry_run=config.dry_run,
            progress_callback=lambda event, **kw: (
                ctx.log("INFO", event, cycle=ctx.cycle, **kw)
                if event != WorkerEventName.SUBAGENT_TURN_COMPLETE
                else _record_observability(
                    orchestrator_root=config.orchestrator_root,
                    task_ref=config.task_ref,
                    lane_id=config.lane_id,
                    session=config.session,
                    cycle=ctx.cycle,
                    phase=str(kw.get("phase") or "execution"),
                    backend=str(kw.get("backend") or ctx.backend),
                    model=ctx.model,
                    obs_ctx=ObservabilityContext(
                        requested_reasoning_effort=ctx.execution_requested_effort,
                        effective_reasoning_effort=ctx.execution_effective_effort,
                        telemetry=kw,
                        state="reviewing" if str(kw.get("phase") or "") == "review" else "executing",
                        summary=f"Worker {str(kw.get('phase') or 'execution')} telemetry captured for cycle {ctx.cycle + 1}.",
                        result_path=ctx.final_result_path,
                    ),
                    run_ctx=ctx,
                )
            ),
        )
    except Exception as exc:
        ctx.log("ERROR", WorkerEventName.REVIEW_FAILED, error=str(exc), cycle=ctx.cycle)
        raise

    review_seconds = round(time.monotonic() - _review_start, 2)
    findings = review_output.get("findings", [])
    converged = review_output.get("converged", False)
    ctx.log(
        "INFO",
        WorkerEventName.REVIEW_COMPLETE,
        cycle=ctx.cycle,
        converged=converged,
        finding_count=len(findings),
        review_seconds=review_seconds,
        review_kind=review_output.get("review_kind"),
        scope_source=review_output.get("scope_source"),
        scope_reason=review_output.get("scope_reason"),
    )

    # ACE reflection is now handled by the project-local PostToolUse hook
    # (scripts/hooks/ace-detect.py) rather than embedded in the daemon.
    # See scripts/ace/ace_reflect.py for the extracted logic.

    # Compute finding diff
    if ctx.prev_finding_ids or findings:
        diff = _compute_finding_diff(ctx.prev_finding_ids, findings)
        ctx.log(
            "INFO",
            WorkerEventName.FINDING_DIFF,
            cycle=ctx.cycle,
            new_count=len(diff["new"]),
            recurring_count=len(diff["recurring"]),
            resolved_count=diff["resolved_count"],
        )

    ctx.last_findings = findings
    ctx.prev_finding_ids = {_finding_stable_id(f) for f in findings}

    return review_output


def _verify_phase(ctx: WorkerRunContext, review_output: "dict[str, Any]") -> bool:
    """Run lane-local verification after review convergence.

    Returns True if verification passed, False otherwise.
    Should only be called when ``review_output.get('converged')`` is True.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = ctx.config
    _reset_exhaustion_streak(ctx.state_dir, config.lane_id, ctx.run_id)
    ctx.previous_run_exhausted = False
    ctx.log("INFO", WorkerEventName.VERIFICATION_START)
    _write_worker_status(
        ctx.state_dir,
        config.lane_id,
        task_ref=config.task_ref,
        session=config.session,
        state="verifying",
        summary="Worker review converged; lane-local verification is running.",
        result_path=_require_result_path(ctx.final_result_path),
        cycle=ctx.cycle,
    )
    if config.dry_run:
        check_ok = True
    else:
        check_ok = _run_lane_check(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            worktree_path=config.worktree_path,
        )
    ctx.log("INFO", WorkerEventName.VERIFICATION_COMPLETE, passed=check_ok)
    return check_ok


def _handoff_phase(ctx: WorkerRunContext, check_ok: bool) -> None:
    """Submit the final handoff after verification.

    Updates ``ctx.handoff_exit`` with the result.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = ctx.config
    final_result_path = _require_result_path(ctx.final_result_path)
    if not check_ok:
        _patch_result(
            final_result_path,
            {
                "handoff_action": "needs_guidance",
                "blockers": ["Lane verification failed after review convergence."],
            },
        )

    handoff_action = "needs_guidance" if not check_ok else "merge_ready"
    _write_worker_status(
        ctx.state_dir,
        config.lane_id,
        task_ref=config.task_ref,
        session=config.session,
        state="handoff",
        summary="Worker verification finished; final handoff is being submitted.",
        result_path=final_result_path,
        cycle=ctx.cycle,
        handoff_action=handoff_action,
    )
    ctx.handoff_exit = _run_final_handoff(
        orchestrator_root=config.orchestrator_root,
        task_ref=config.task_ref,
        lane_id=config.lane_id,
        session=config.session,
        worktree_path=config.worktree_path,
        result_path=final_result_path,
        dry_run=config.dry_run,
        run_id=ctx.run_id,
        outcome="finished" if check_ok else None,
    )
    if ctx.handoff_exit == 0:
        _cleanup_result_file(final_result_path)
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="waiting_for_orchestrator",
            summary="Worker handoff submitted successfully; waiting for orchestrator follow-up.",
            handoff_action=handoff_action,
            clear_result_path=True,
        )
    else:
        _write_worker_status(
            ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="handoff_failed",
            summary="The final worker handoff failed after verification; the saved lane result must be retried without re-running execution.",
            result_path=final_result_path,
            failure_stage="final_handoff",
            cycle=ctx.cycle,
            handoff_action=handoff_action,
            attention_required=True,
        )
        ctx.log("ERROR", WorkerEventName.HANDOFF_FAILED, cycle=ctx.cycle, result_path=str(final_result_path))


def _handle_token_budget_exceeded(run_ctx: WorkerRunContext) -> None:
    """Stop the worker at a cycle boundary after the per-lane token budget is crossed."""
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = run_ctx.config
    if run_ctx.token_budget_tripped:
        # Already handled on an earlier boundary; never re-record a duplicate blocker.
        return
    run_ctx.token_budget_tripped = True
    budget = config.token_budget
    cumulative = run_ctx.cumulative_tokens
    run_ctx.log(
        "WARNING",
        WorkerEventName.TOKEN_BUDGET_EXCEEDED,
        cumulative_tokens=cumulative,
        token_budget=budget,
        cycle=run_ctx.cycle,
    )
    blocker_msg = f"token_budget_exceeded: cumulative_tokens={cumulative} exceeded token_budget={budget}"
    _record_token_budget_blocker(
        orchestrator_root=config.orchestrator_root,
        task_ref=config.task_ref,
        description=blocker_msg,
    )
    if run_ctx.final_result_path:
        _patch_result(
            run_ctx.final_result_path,
            {
                "handoff_action": "needs_guidance",
                "blockers": [blocker_msg],
            },
        )
        _write_worker_status(
            run_ctx.state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="handoff",
            summary="Worker stopped after exceeding its per-lane token budget; handing partial work back.",
            result_path=run_ctx.final_result_path,
            handoff_action="needs_guidance",
            attention_required=True,
        )
        run_ctx.handoff_exit = _run_final_handoff(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            worktree_path=config.worktree_path,
            result_path=run_ctx.final_result_path,
            dry_run=config.dry_run,
            run_id=run_ctx.run_id,
            outcome="exhausted",
        )
        if run_ctx.handoff_exit == 0:
            _cleanup_result_file(run_ctx.final_result_path)
            _write_worker_status(
                run_ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="waiting_for_orchestrator",
                summary="Token-budget worker handoff submitted; waiting for orchestrator follow-up.",
                handoff_action="needs_guidance",
                clear_result_path=True,
            )
        else:
            _write_worker_status(
                run_ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="handoff_failed",
                summary="The token-budget worker handoff failed; the saved lane result must be retried without re-running execution.",
                result_path=run_ctx.final_result_path,
                failure_stage="final_handoff",
                handoff_action="needs_guidance",
                attention_required=True,
            )
            run_ctx.log("ERROR", WorkerEventName.HANDOFF_FAILED, result_path=str(run_ctx.final_result_path))


def _run_worker_cycles(run_ctx: WorkerRunContext, single_pass: bool) -> "int | None":
    """Execute the review-cycle loop for one actionable pass.

    Returns an early exit code if single_pass caused an early return inside the
    loop, or None if the loop ran to completion (caller should use run_ctx.handoff_exit).
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = run_ctx.config
    for cycle in range(config.max_review_cycles):
        run_ctx.cycle = cycle
        if (
            config.token_budget is not None
            and config.token_budget > 0
            and run_ctx.cumulative_tokens > config.token_budget
        ):
            _handle_token_budget_exceeded(run_ctx)
            return run_ctx.handoff_exit if single_pass else None
        exec_ok = _execute_phase(run_ctx)
        if not exec_ok:
            break
        try:
            review_output = _review_phase(run_ctx)
        except StopIteration:
            return run_ctx.handoff_exit if single_pass else None
        except (RuntimeError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
            logger.warning("review phase failed for %s/%s cycle %s: %s", config.task_ref, config.lane_id, cycle, exc)
            break
        converged = review_output.get("converged", False)
        if converged:
            check_ok = _verify_phase(run_ctx, review_output)
            _handoff_phase(run_ctx, check_ok)
            return run_ctx.handoff_exit if single_pass else None
        run_ctx.log("INFO", WorkerEventName.FIX_CYCLE_NEEDED, cycle=cycle)
    else:
        run_ctx.log("WARNING", WorkerEventName.REVIEW_EXHAUSTED, max_cycles=config.max_review_cycles)
        run_ctx.previous_run_exhausted = True
        exhaustion_streak = _update_exhaustion_streak(run_ctx.state_dir, config.lane_id, run_ctx.run_id)
        run_ctx.log("WARNING", WorkerEventName.EXHAUSTION_STREAK, streak=exhaustion_streak, lane=config.lane_id)
        if exhaustion_streak >= 3:
            run_ctx.log("WARNING", WorkerEventName.LANE_EXHAUSTION_FORCED_STOP, streak=exhaustion_streak)
        if run_ctx.final_result_path:
            _patch_result(
                run_ctx.final_result_path,
                {
                    "handoff_action": "needs_guidance",
                    "blockers": [f"Review did not converge after {config.max_review_cycles} cycles."],
                },
            )
            _write_worker_status(
                run_ctx.state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="handoff",
                summary="Worker review did not converge; handing the blocked result back to the orchestrator.",
                result_path=run_ctx.final_result_path,
                handoff_action="needs_guidance",
                attention_required=exhaustion_streak >= 2,
            )
            run_ctx.handoff_exit = _run_final_handoff(
                orchestrator_root=config.orchestrator_root,
                task_ref=config.task_ref,
                lane_id=config.lane_id,
                session=config.session,
                worktree_path=config.worktree_path,
                result_path=run_ctx.final_result_path,
                dry_run=config.dry_run,
                run_id=run_ctx.run_id,
                outcome="exhausted",
            )
            if run_ctx.handoff_exit == 0:
                _cleanup_result_file(run_ctx.final_result_path)
                _write_worker_status(
                    run_ctx.state_dir,
                    config.lane_id,
                    task_ref=config.task_ref,
                    session=config.session,
                    state="waiting_for_orchestrator",
                    summary="Non-converged worker handoff submitted; waiting for orchestrator follow-up.",
                    handoff_action="needs_guidance",
                    clear_result_path=True,
                )
            else:
                _write_worker_status(
                    run_ctx.state_dir,
                    config.lane_id,
                    task_ref=config.task_ref,
                    session=config.session,
                    state="handoff_failed",
                    summary="The blocked worker handoff failed; the saved lane result must be retried without re-running execution.",
                    result_path=run_ctx.final_result_path,
                    failure_stage="final_handoff",
                    handoff_action="needs_guidance",
                    attention_required=True,
                )
                run_ctx.log("ERROR", WorkerEventName.HANDOFF_FAILED, result_path=str(run_ctx.final_result_path))
    return None


def _setup_worker_run(config: WorkerConfig) -> "tuple[WorkerRunContext, str | None]":
    """Build WorkerRunContext from a WorkerConfig and write initial status.

    Returns (run_ctx, initial_dormant_state).
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    log_dir = config.orchestrator_root / "logs" / "worker-daemon"
    state_dir = config.orchestrator_root / ".task-state"
    run_id = str(uuid.uuid4())

    def _loop_log(level: str, event: str, **kw: Any) -> None:
        _log(config.lane_id, log_dir, level, event, run_id=run_id, **kw)

    existing_status = _read_worker_status(state_dir, config.lane_id) or {}

    try:
        from lane_manifest import get_lane_config as _get_lane_config  # noqa: PLC0415

        _lane_cfg = (
            _get_lane_config(config.task_ref, config.lane_id, orchestrator_root=str(config.orchestrator_root)) or {}
        )
    except (ImportError, FileNotFoundError, KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("lane config unavailable for %s/%s: %s", config.task_ref, config.lane_id, exc)
        _lane_cfg = {}
    token_burn_threshold = int(_lane_cfg.get("token_burn_threshold") or 2_000_000)

    _loop_log(
        "INFO",
        WorkerEventName.DAEMON_START,
        task_ref=config.task_ref,
        single_pass=config.single_pass,
        max_review_cycles=config.max_review_cycles,
        backend=config.backend,
        session_mode=config.session_mode,
        reasoning_effort=config.reasoning_effort,
        model=config.model,
    )
    if existing_status.get("state") != "handoff_failed":
        _write_worker_status(
            state_dir,
            config.lane_id,
            task_ref=config.task_ref,
            session=config.session,
            state="starting",
            summary="Worker daemon started and is preparing its lane-scoped runtime.",
        )

    run_ctx = WorkerRunContext(
        config=config,
        log_dir=log_dir,
        state_dir=state_dir,
        run_id=run_id,
        token_burn_threshold=token_burn_threshold,
        log=_loop_log,
        backend=config.backend,
        model=config.model,
        reasoning_effort=config.reasoning_effort,
    )
    return run_ctx, None


def worker_loop_from_kwargs(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    max_review_cycles: int = 3,
    poll_interval: int = 30,
    single_pass: bool = False,
    backend: str = "codex-cli",
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
    codex_bin: str | None = None,
    codex_args: list[str] | None = None,
    grok_bin: str | None = None,
    grok_args: list[str] | None = None,
    grok_max_turns: int | None = None,
    grok_timeout: int | None = None,
    dry_run: bool = False,
    token_budget: int | None = None,
) -> int:
    """Keyword-argument adapter for ``worker_loop``.  Builds a ``WorkerConfig`` and delegates."""
    config = WorkerConfig(
        orchestrator_root=orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        worktree_path=worktree_path,
        max_review_cycles=max_review_cycles,
        poll_interval=poll_interval,
        single_pass=single_pass,
        backend=backend,
        session_mode=session_mode,
        reasoning_effort=reasoning_effort,
        model=model,
        codex_bin=codex_bin,
        codex_args=codex_args,
        grok_bin=grok_bin,
        grok_args=grok_args,
        grok_max_turns=grok_max_turns,
        grok_timeout=grok_timeout,
        dry_run=dry_run,
        token_budget=token_budget,
    )
    config = _resolve_grok_cycle_bounds(config)
    return worker_loop(config)


def _handle_handoff_retry_path(
    run_ctx: WorkerRunContext,
    dormant_state: "str | None",
    handoff_retry_count: int,
) -> "tuple[int | None, str | None, int]":
    """Handle the handoff-failed retry path.

    Returns (exit_code_or_None, new_dormant_state, new_handoff_retry_count).
    exit_code_or_None is set when the caller should return immediately or continue;
    None means this path was not taken (persisted state is not handoff_failed).
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = run_ctx.config
    state_dir = run_ctx.state_dir
    poll_interval = config.poll_interval
    single_pass = config.single_pass
    MAX_HANDOFF_RETRIES = 3
    persisted_status = _read_worker_status(state_dir, config.lane_id) or {}
    if persisted_status.get("state") != "handoff_failed":
        return None, dormant_state, handoff_retry_count

    result_path_raw = str(persisted_status.get("result_path") or "").strip()
    result_path = Path(result_path_raw) if result_path_raw else None
    if result_path is not None and result_path.exists() and handoff_retry_count < MAX_HANDOFF_RETRIES:
        backoff = min(poll_interval * (2**handoff_retry_count), 300)
        if handoff_retry_count > 0:
            time.sleep(backoff)
        run_ctx.log(
            "INFO", WorkerEventName.HANDOFF_RETRY_START, result_path=str(result_path), retry=handoff_retry_count + 1
        )
        retry_exit = _run_final_handoff(
            orchestrator_root=config.orchestrator_root,
            task_ref=config.task_ref,
            lane_id=config.lane_id,
            session=config.session,
            worktree_path=config.worktree_path,
            result_path=result_path,
            dry_run=config.dry_run,
            run_id=run_ctx.run_id,
            outcome=_outcome_for_result_file(result_path),
        )
        handoff_retry_count += 1
        if retry_exit == 0:
            _cleanup_result_file(result_path)
            _write_worker_status(
                state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="waiting_for_orchestrator",
                summary="Worker handoff submitted successfully; waiting for orchestrator follow-up.",
                clear_result_path=True,
            )
            run_ctx.log("INFO", WorkerEventName.HANDOFF_RETRY_COMPLETE, result_path=str(result_path))
            exit_code = 0 if single_pass else -1  # -1 = continue polling
            return exit_code, dormant_state, handoff_retry_count
        run_ctx.log("ERROR", WorkerEventName.HANDOFF_RETRY_FAILED, result_path=str(result_path))
    if dormant_state != "handoff_failed":
        run_ctx.log(
            "ERROR",
            WorkerEventName.DORMANT_ENTERED,
            state="handoff_failed",
            interval=poll_interval,
            retry_count=handoff_retry_count,
        )
        dormant_state = "handoff_failed"
    return (1 if single_pass else -1), dormant_state, handoff_retry_count


def _handle_dormant_state(
    run_ctx: WorkerRunContext,
    lane_state: "str | None",
    dormant_state: "str | None",
) -> "tuple[str | None, bool]":
    """Handle non-actionable poll results. Returns (new_dormant_state, should_wake).

    should_wake=True means lane_state is 'actionable' and execution should proceed.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    config = run_ctx.config
    state_dir = run_ctx.state_dir
    if lane_state == "actionable":
        if dormant_state is not None:
            wake_reason = "orchestrator_dispatch" if dormant_state == "waiting" else "new_lane_work"
            run_ctx.log("INFO", WorkerEventName.DORMANT_EXITED, previous_state=dormant_state, reason=wake_reason)
        return None, True
    if dormant_state != lane_state:
        if lane_state == "waiting":
            run_ctx.log(
                "INFO", WorkerEventName.DORMANT_ENTERED, state="waiting_for_orchestrator", interval=config.poll_interval
            )
            _write_worker_status(
                state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="waiting_for_orchestrator",
                summary="Worker already handed off this lane and is waiting for orchestrator follow-up.",
            )
        else:
            run_ctx.log("INFO", WorkerEventName.DORMANT_ENTERED, state="idle", interval=config.poll_interval)
            _write_worker_status(
                state_dir,
                config.lane_id,
                task_ref=config.task_ref,
                session=config.session,
                state="idle",
                summary="No actionable lane inbox items are currently assigned to this worker.",
            )
    return lane_state, False


def worker_loop(config: WorkerConfig) -> int:
    """Main daemon loop.  Accepts a fully-populated WorkerConfig.

    To call from keyword arguments use the ``worker_loop_from_kwargs`` adapter.
    Returns 0 on clean handoff, 1 on failure.
    """
    from workbay_handoff_mcp.enums import WorkerEventName  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.daemon_startup import (  # noqa: PLC0415
        emit_daemon_startup_warning,
    )

    run_ctx, dormant_state = _setup_worker_run(config)
    cfg = run_ctx.config
    emit_daemon_startup_warning("worker", poll_interval=cfg.poll_interval)
    single_pass = cfg.single_pass
    handoff_retry_count = 0
    while True:
        if _shutdown_requested:
            run_ctx.log("INFO", WorkerEventName.DAEMON_STOP, reason="sigterm")
            if dormant_state != "waiting" and run_ctx.handoff_exit != 0:
                _record_terminal_outcome(
                    orchestrator_root=cfg.orchestrator_root,
                    task_ref=cfg.task_ref,
                    lane_id=cfg.lane_id,
                    session=cfg.session,
                    summary="Worker daemon stopped before completing a lane handoff.",
                    outcome="stopped",
                )
            return 0
        exit_code, dormant_state, handoff_retry_count = _handle_handoff_retry_path(
            run_ctx, dormant_state, handoff_retry_count
        )
        if exit_code is not None:
            if exit_code == -1:
                # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
                time.sleep(cfg.poll_interval)
                continue
            return exit_code
        lane_state = _poll_phase(run_ctx)
        if lane_state is None:
            if single_pass:
                return 1
            # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
            time.sleep(cfg.poll_interval)
            continue
        dormant_state, should_wake = _handle_dormant_state(run_ctx, lane_state, dormant_state)
        if not should_wake:
            if single_pass:
                if lane_state == "idle":
                    # internal S1: an empty inbox is a typed no-work
                    # outcome, never exit 0 — the coordinator must be able to
                    # distinguish "did the slice" from "found nothing and quit".
                    # S1-A-005: a dry_run pass must not write a real worker_reports
                    # row, so skip the terminal-outcome DB write under dry_run.
                    # HARM-A-006: use the canonical 'no_actionable_work' name shared
                    # with worker_start / run_offload_pass, not the legacy 'no_work'.
                    if not cfg.dry_run:
                        _record_terminal_outcome(
                            orchestrator_root=cfg.orchestrator_root,
                            task_ref=cfg.task_ref,
                            lane_id=cfg.lane_id,
                            session=cfg.session,
                            summary=(
                                "Single-pass worker found no actionable lane work "
                                "(no open brief); exiting with typed no-work outcome."
                            ),
                            outcome="no_actionable_work",
                        )
                    return _NO_WORK_EXIT
                return 0
            # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
            time.sleep(cfg.poll_interval)
            continue
        run_ctx.final_result_path = None
        run_ctx.handoff_exit = 1
        run_ctx.last_findings = []
        run_ctx.prev_finding_ids = set()
        early_exit = _run_worker_cycles(run_ctx, single_pass)
        if single_pass:
            return run_ctx.handoff_exit if early_exit is None else early_exit
        if run_ctx.token_budget_tripped:
            # Open-circuit: a tripped per-lane token budget stops the daemon in
            # continuous mode too, instead of re-tripping every poll and spamming
            # duplicate blockers (Release It! 5.2 open circuit = stop, inspectable).
            run_ctx.log("INFO", WorkerEventName.DAEMON_STOP, reason="token_budget_exceeded")
            return run_ctx.handoff_exit
        run_ctx.log("INFO", WorkerEventName.POLL_SLEEP, interval=cfg.poll_interval)
        # TODO(internal): Pull-based poll -- see packages/mcp-workbay-orchestrator/docs/reworks/event-driven-daemon-design-note.md
        time.sleep(cfg.poll_interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker daemon: poll, implement, review, verify, handoff.")
    parser.add_argument("--orchestrator-root", required=True, help="Absolute path to the monorepo root.")
    parser.add_argument("--task-ref", required=True, help="MCP task reference (e.g. example-multi-lane-task).")
    parser.add_argument("--lane-id", required=True, help="Lane identifier (e.g. domain).")
    parser.add_argument("--session", default=None, help="MCP session. Defaults to <task>-<lane>.")
    parser.add_argument("--worktree-path", required=True, help="Absolute path to the lane worktree.")
    parser.add_argument(
        "--max-review-cycles", type=int, default=3, help="Max review/fix cycles before declaring blocked (default: 3)."
    )
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between poll cycles (default: 30).")
    parser.add_argument("--single-pass", action="store_true", help="Run one cycle and exit instead of looping.")
    parser.add_argument(
        "--backend", default="codex-cli", choices=BACKEND_CHOICES, help="Execution backend to use (default: codex-cli)."
    )
    parser.add_argument(
        "--session-mode",
        default="fresh_turn",
        choices=SESSION_MODE_CHOICES,
        help="Use a fresh backend session per turn, or preserve continuity within this lane only.",
    )
    parser.add_argument(
        "--reasoning-effort",
        default="inherit",
        choices=WORKER_REASONING_EFFORT_CHOICES,
        help="Worker reasoning mode: inherit existing defaults, auto-tune per cycle, or force a specific effort.",
    )
    parser.add_argument("--codex-bin", default=None, help="Explicit path to the codex binary.")
    parser.add_argument("--codex-args", default=None, help="Extra args for codex exec (space-separated).")
    parser.add_argument("--grok-bin", default=None, help="Explicit path to the grok binary.")
    parser.add_argument("--grok-args", default=None, help="Extra args for grok exec (space-separated).")
    parser.add_argument("--model", default=None, help="Explicit model to use (e.g. gpt-5.4-mini).")
    parser.add_argument(
        "--token-budget",
        type=int,
        default=None,
        help="Optional per-lane token budget enforced at cycle boundaries (default: off).",
    )
    parser.add_argument(
        "--grok-max-turns",
        type=int,
        default=None,
        help="Per-invocation grok --max-turns cap (derived from --token-budget when unset).",
    )
    parser.add_argument(
        "--grok-timeout",
        type=int,
        default=None,
        help="Per-invocation grok wall-clock timeout seconds (derived from --token-budget when unset).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Skip Codex execution and simulate results.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    worktree_path = Path(args.worktree_path).expanduser().resolve()
    session = args.session or f"{args.task_ref}-{args.lane_id}"
    state_dir = orchestrator_root / ".task-state"
    codex_args = args.codex_args.split() if args.codex_args else None
    grok_args = args.grok_args.split() if args.grok_args else None

    # Ensure SCRIPT_DIR is on sys.path so lazy imports work
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    # Per-lane exclusive lock
    lock = WorkerLock(args.lane_id, state_dir)
    if not lock.acquire():
        print(f"Another worker daemon is already running for lane '{args.lane_id}'.", file=sys.stderr)
        return 1

    signal.signal(signal.SIGTERM, _handle_sigterm)
    try:
        config = WorkerConfig(
            orchestrator_root=orchestrator_root,
            task_ref=args.task_ref,
            lane_id=args.lane_id,
            session=session,
            worktree_path=worktree_path,
            max_review_cycles=args.max_review_cycles,
            poll_interval=args.poll_interval,
            single_pass=args.single_pass,
            backend=args.backend,
            session_mode=args.session_mode,
            reasoning_effort=args.reasoning_effort,
            model=args.model,
            codex_bin=args.codex_bin,
            codex_args=codex_args,
            grok_bin=args.grok_bin,
            grok_args=grok_args,
            grok_max_turns=args.grok_max_turns,
            grok_timeout=args.grok_timeout,
            dry_run=args.dry_run,
            token_budget=args.token_budget,
        )
        config = _resolve_grok_cycle_bounds(config)
        return worker_loop(config)
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
