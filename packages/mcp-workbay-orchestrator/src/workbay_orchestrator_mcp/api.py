"""Orchestrator MCP API — lane management, worker daemons, turn metrics, and dispatch."""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

from fastmcp import FastMCP
from workbay_protocol import BRAND_NAME, HARNESS_CONTRACT_RELPATH, INSTRUCTIONS_RELPATH

from workbay_orchestrator_mcp import lanes as _lanes
from workbay_orchestrator_mcp.orchestration.token_estimate import (
    estimate_token_count as _estimate_token_count,
)

if TYPE_CHECKING:
    from workbay_handoff_mcp.config import RuntimeConfig

_logger = logging.getLogger(__name__)

# implementation note S8 / T6: named warn when a grok-cli brief requests subagent steps.
GROK_BRIEF_SUBAGENT_STEPS_WARNING = "grok_brief_subagent_steps"
_GROK_BRIEF_SUBAGENT_STEP_PATTERNS = re.compile(
    r"(?i)(/review-parallel|subagent\s+fan[- ]?out|fan[- ]?out\s+reviews?)",
)


def _handoff_core():
    from workbay_handoff_mcp import core

    return core


class _CoreProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(_handoff_core(), name)


core = _CoreProxy()

_HANDOFF_API_EXPORTS = frozenset(
    {
        "archive",
        "archive_task_state",
        "artifacts",
        "batch_record_review_findings",
        "build_write_actor",
        "close_slice",
        "export_handoff_state",
        "get_handoff_state",
        "handoff_close_check",
        "import_handoff_state",
        "list_next_actions",
        "list_review_findings",
        "next_actions",
        "record_artifact",
        "record_decision",
        "record_event",
        "record_review_finding",
        "record_review_run",
        "record_test_result",
        "render_handoff",
        "report_blocker",
        "review_findings",
        "review_runs",
        "set_handoff_state",
        "update_next_actions",
        "update_review_finding",
    }
)


def __getattr__(name: str) -> Any:
    if name == "RuntimeConfig":
        from workbay_handoff_mcp.config import RuntimeConfig as _RuntimeConfig  # noqa: PLC0415

        return _RuntimeConfig
    if name in _HANDOFF_API_EXPORTS:
        import workbay_handoff_mcp as _handoff  # noqa: PLC0415

        return getattr(_handoff, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _json_response(payload: dict[str, object]) -> dict:
    return _handoff_core()._json_response(payload)


def _get_db_connection():
    return _handoff_core()._get_db_connection()


def _resolve_task_ref(conn: Any, task_ref: str | None) -> str:
    return _handoff_core()._resolve_task_ref(conn, task_ref)


def configure_runtime(config: "RuntimeConfig"):
    from workbay_handoff_mcp.api import configure_runtime as _configure_runtime

    return _configure_runtime(config)


def get_runtime_config():
    from workbay_handoff_mcp.api import get_runtime_config as _get_runtime_config

    return _get_runtime_config()


def reset_runtime_config():
    from workbay_handoff_mcp.api import reset_runtime_config as _reset_runtime_config

    return _reset_runtime_config()


def switch_task(
    task_ref: str,
    objective: str | None = None,
    focus: str | None = None,
    status: str = "in_progress",
    actor: dict[str, Any] | None = None,
    target_branch: str | None = None,
):
    from workbay_handoff_mcp import switch_task as _switch_task
    from workbay_handoff_mcp.api import WriteActor

    return _switch_task(
        task_ref=task_ref,
        objective=objective,
        focus=focus,
        status=status,
        actor=cast(WriteActor | None, actor),
        target_branch=target_branch,
    )


def reconcile_review_findings(task_ref: str | None = None, apply: bool = False):
    from workbay_handoff_mcp.review_findings import reconcile_review_findings as _reconcile_review_findings

    return _reconcile_review_findings(task_ref=task_ref, apply=apply)


def get_review_findings_summary(
    task_ref: str | None = None,
    top_n_open: int = 5,
    top_n_recent_updates: int = 3,
    review_mode: str | None = None,
):
    from workbay_handoff_mcp.review_findings import get_review_findings_summary as _get_review_findings_summary

    return _get_review_findings_summary(
        task_ref=task_ref,
        top_n_open=top_n_open,
        top_n_recent_updates=top_n_recent_updates,
        review_mode=review_mode,
    )


manage_worktree_lane = _lanes.manage_worktree_lane
get_lane_activity = _lanes.get_lane_activity
turn_metrics = _lanes.turn_metrics
lane_communication = _lanes.lane_communication
worker_reports = _lanes.worker_reports
plan_cursor = _lanes.plan_cursor

# Additional tools that belong to the orchestration surface
get_latest_slice_review_packet = _lanes.get_latest_slice_review_packet


def _register_dashboard_extensions() -> None:
    """Register orchestrator-side dashboard extensions at module load time.

    Late-binding imports per rg-014: ``workbay_handoff_mcp`` symbols are
    imported inside this function, not at module top level.
    """
    from workbay_handoff_mcp.dashboard_rendering import register_dashboard_extension  # noqa: PLC0415

    from workbay_orchestrator_mcp.orchestration.dashboard_extension import (  # noqa: PLC0415
        lane_worker_extension,
    )

    register_dashboard_extension(lane_worker_extension)


_register_dashboard_extensions()

TOOL_DESCRIPTIONS: dict[str, str] = {
    "manage_worktree_lane": "Compound tool: create, close, or list worktree lanes in one call. Use operation='upsert'|'close'|'list'.",
    "get_lane_activity": "Read the current activity summary for a lane, including blockers, actions, findings, messages, and tests.",
    "turn_metrics": "Compound tool: record, list, or summarize turn metrics in one call. Use operation='record'|'list'|'summary'.",
    "lane_communication": "Compound tool: record, update, or list lane messages and briefs in one call. Use kind='message'|'brief' and operation='record'|'update'|'list'.",
    "worker_reports": "Compound tool: record or list worker reports in one call. Use operation='record'|'list'.",
    "plan_cursor": "Compound tool: create/update, fetch, or list plan cursors in one call. Use operation='upsert'|'get'|'list'.",
    "switch_task": "Switch the active task in one step: auto-archives the outgoing task and activates the target.",
    "get_latest_slice_review_packet": "Resolve a completed slice review packet for a task. Omit slice selectors for the latest packet; pass slice_decision_id or slice_label to fetch a historical slice.",
    "reconcile_review_findings": "Compare open findings against current files and return a reconciliation summary for review workflows.",
    "get_review_findings_summary": "Return aggregate counts of review findings by status and severity for the active or requested task.",
    "manage_orchestrator": "Compound tool: start, query, pause, resume, stop, or run a single orchestrator cycle in one call. Use operation='start'|'status'|'pause'|'resume'|'stop'|'single_cycle'.",
    "manage_worker": "Compound tool: start, stop, resume, query status, inspect event history, or start all worker daemons in one call. Use action='start'|'stop'|'resume'|'status'|'event_history'|'start_all'.",
    "run_structured_turn": "Execute one synchronous structured bridge turn through a registered non-CLI backend.",
    "dispatch_lane_work": "Update lane dispatch parameters (model, backend, effort) for the next execution cycle. Optional include_context_packet + context_targets append a deterministic codemap lane-context packet to the brief (implementation note S12); auto when CLI present and targets provided. Surfaces packet_bytes/sections on the result; codemap absence degrades typed without failing dispatch.",
    "run_offload_pass": "Run one synchronous offload pass over an actionable lane: bounded execute→review→fix with a commit gate, mandatory positive token_budget + timeout_seconds, and a typed outcome enum (handoff_ready|review_complete|needs_guidance|no_actionable_work|uncommitted_work|token_budget_exceeded|timeout|error|lane_not_found|self_verify_failed|composer_violation_quarantined|checkpoint|server_stale_restart_required|admission_deferred|admission_refused|remote_required; await_offload_pass additionally reports still_running). Every outcome carries commit_landed:bool + failed_stage (execute|self_verify|review|handoff|attestation|null) + findings:list (worker BR-* rows recorded during the pass; may be empty). On grok smoke-review degrade, review may be skipped_unparseable and raw_tail may carry the unparseable payload tail — never bare error after a green self-verify. needs_guidance means the worker submitted a blocked/unverified handoff — never merge-ready. review_complete (implementation note) means a review lane (lane_kind='review') finished cleanly — clean tree, unchanged HEAD, handoff submitted, and a parseable findings block harvested (findings_harvest + findings surface them); a success with failed_stage=null, not a wedged needs_guidance. self_verify_failed means the worker TEST_CMD gate failed before commit. composer_violation_quarantined preserves a self-verified checkpoint when grok-build contamination is detected. checkpoint means max-turns stopped with resumable work. admission_deferred means host memory pressure rose mid-pass so the pass parked (dirty work preserved as a checkpoint) instead of spawning another turn — retryable via a fresh dispatch. remote_required means the bootstrap ledger is execution_mode=remote_only and the caller pinned an explicit local backend (never silently substituted — use grok-remote or repair --no-remote). Backend default is grok-cli under local_ok and grok-remote under remote_only. Never auto-retries; recovery is a new idempotent dispatch.",
    "await_offload_pass": "Bounded continuation for an offload pass that outlived one client call window: long-poll (pass_id, wait_seconds) for the persisted pass outcome (same typed enum + commit_landed + failed_stage + findings + optional review/raw_tail discriminators as run_offload_pass) or still_running with a progress snapshot. One call per wait window, not a poll loop.",
    "await_offload_passes": "Multi-pass join over N offload pass ids: wait for terminal outcomes without coordinator O(N) hand-tracking. mode='all_complete' waits until every pass is terminal (or wait_seconds elapses); mode='first_failure' returns as soon as any pass terminally fails. Always returns a per-pass entry for every requested id (success, failure, empty_result, still_running, unknown). Partial failure does not sink siblings. wait_exhausted is always present so a deadline never silently looks like full completion.",
    "dispatch_wave": "Coordinator-side batch submission of COST_REMOTE/grok-remote lanes through run_offload_pass: ready-frontier wave width, cost-class-branched admission (remote skips heavy-slot claim; gated classes serialise check+claim with reserved_slot_idx ownership), blocking join via await_offload_passes. Returns wave_id + dispatched/deferred/refused. Non-remote members (e.g. claude verify twins) are refused — the daemon owns them.",
    "list_available_backends": "List supported execution backends and their capabilities. By default includes probed is_available plus availability_state/detail per backend so skills can route safely. Pass probe=false for the cheap static declaration-only view.",
    "get_metrics_summary": "Return an ACE metrics snapshot for the active task covering token burn, context pressure, FTS5 retrieval, lane health, phase timing, and documentation fitness.",
    "offload_preflight": "Fail-Fast cross-harness offload pre-checks before dispatch: resolve --agent to its offload profile (grok-cli|grok-remote|cursor-cli|codex-subagent), probe availability, validate --effort and any model pin, require a clean worktree and positive token_budget; returns the selected backend/model/effort and derived single-cycle bounds (grok: max_turns+timeout; cursor-cli: timeout only, since cursor-agent has no --max-turns). Also compares lane payload-rules content hashes vs primary main (docs/workbay/rules/**, packages/workbay-system/**/payload/docs/**) and warns (or fails when strict=true) when stale. Codemap index-freshness gate (implementation note S12): when codebase-memory-mcp CLI is available, warns codemap_stale on lag; CLI absent → codemap_unavailable skip note — never blocks. No fallback.",
    "materialize_offload_lane_manifest": "Write/patch the lane manifest preferred_backend/preferred_model/preferred_reasoning_effort so review_runner pins review to the selected offload backend. Concrete effort only; auto|inherit are resolved at execution and not pinned.",
}

_CONTRACT_RELATIVE_PATH = HARNESS_CONTRACT_RELPATH
# The enable-path in this message MUST name the file the runtime actually reads
# (_daemons_enabled_for_workspace parses only the tracked contract; the legacy
# local/ overlay was removed in implementation note and is never consulted).
_DAEMONS_DISABLED_MESSAGE = (
    "Daemons are opt-in. Enable via `orchestrator.daemons.enabled: true` in "
    f"`{_CONTRACT_RELATIVE_PATH}` (read live per-call; no server restart needed). "
    "See `docs/workbay/consumer-setup.md § Daemons` for token-cost implications."
)


class DaemonsDisabledError(RuntimeError):
    """Raised when a daemon start surface is invoked while daemons are disabled."""


def _strip_yaml_comment(raw_line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    result: list[str] = []
    for char in raw_line:
        if escaped:
            result.append(char)
            escaped = False
            continue
        if char == "\\" and in_double:
            result.append(char)
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            result.append(char)
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            result.append(char)
            continue
        if char == "#" and not in_single and not in_double:
            break
        result.append(char)
    return "".join(result).rstrip()


def _parse_daemons_enabled(contract_path: Path) -> bool | None:
    try:
        lines = contract_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    in_orchestrator = False
    in_daemons = False
    for raw_line in lines:
        stripped = _strip_yaml_comment(raw_line)
        text = stripped.strip()
        if not text:
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if indent == 0:
            in_orchestrator = text == "orchestrator:"
            in_daemons = False
            continue
        if not in_orchestrator:
            continue
        if indent <= 0:
            in_orchestrator = False
            in_daemons = False
            continue
        if indent == 2:
            in_daemons = text == "daemons:"
            continue
        if not in_daemons:
            continue
        if indent <= 2:
            in_daemons = False
            continue
        if indent == 4 and text.startswith("enabled:"):
            value = text.split(":", 1)[1].strip().lower()
            if value == "true":
                return True
            if value == "false":
                return False
            return None
    return None


def _daemons_enabled_for_workspace(workspace_root: Path) -> bool:
    # Contract resolution uses the default path only — the legacy
    # `surfaces.contracts` overlay override was removed (implementation note): it was dead
    # (no production writer; it read a manifest name the installer renames away).
    enabled = _parse_daemons_enabled(workspace_root / _CONTRACT_RELATIVE_PATH)
    return True if enabled is None else enabled


def _ensure_daemons_enabled() -> None:
    runtime = get_runtime_config()
    workspace_root = Path(runtime.workspace_root).expanduser().resolve()
    if _daemons_enabled_for_workspace(workspace_root):
        return
    raise DaemonsDisabledError(_DAEMONS_DISABLED_MESSAGE)


def _apply_tool_descriptions() -> None:
    for name, description in TOOL_DESCRIPTIONS.items():
        tool = globals().get(name)
        if tool is None:
            continue
        existing = getattr(tool, "__doc__", None)
        if existing and existing.strip():
            continue
        tool.__doc__ = description


@dataclass
class ToolEntry:
    """Registry entry for a single MCP tool."""

    name: str
    handler: Callable[..., Any]
    description: str
    deprecated_since: str | None = None  # Version string; non-None appends [DEPRECATED] to description


def _current_tool_entries() -> list[ToolEntry]:
    return [
        ToolEntry("manage_worktree_lane", manage_worktree_lane, TOOL_DESCRIPTIONS["manage_worktree_lane"]),
        ToolEntry("get_lane_activity", get_lane_activity, TOOL_DESCRIPTIONS["get_lane_activity"]),
        ToolEntry("turn_metrics", turn_metrics, TOOL_DESCRIPTIONS["turn_metrics"]),
        ToolEntry("lane_communication", lane_communication, TOOL_DESCRIPTIONS["lane_communication"]),
        ToolEntry("worker_reports", worker_reports, TOOL_DESCRIPTIONS["worker_reports"]),
        ToolEntry("plan_cursor", plan_cursor, TOOL_DESCRIPTIONS["plan_cursor"]),
        ToolEntry("switch_task", switch_task, TOOL_DESCRIPTIONS["switch_task"]),
        ToolEntry(
            "get_latest_slice_review_packet",
            get_latest_slice_review_packet,
            TOOL_DESCRIPTIONS["get_latest_slice_review_packet"],
        ),
        ToolEntry(
            "reconcile_review_findings", reconcile_review_findings, TOOL_DESCRIPTIONS["reconcile_review_findings"]
        ),
        ToolEntry(
            "get_review_findings_summary", get_review_findings_summary, TOOL_DESCRIPTIONS["get_review_findings_summary"]
        ),
        ToolEntry("manage_orchestrator", manage_orchestrator, TOOL_DESCRIPTIONS["manage_orchestrator"]),
        ToolEntry("manage_worker", manage_worker, TOOL_DESCRIPTIONS["manage_worker"]),
        ToolEntry("run_structured_turn", run_structured_turn, TOOL_DESCRIPTIONS["run_structured_turn"]),
        ToolEntry("dispatch_lane_work", dispatch_lane_work, TOOL_DESCRIPTIONS["dispatch_lane_work"]),
        ToolEntry("run_offload_pass", run_offload_pass, TOOL_DESCRIPTIONS["run_offload_pass"]),
        ToolEntry("await_offload_pass", await_offload_pass, TOOL_DESCRIPTIONS["await_offload_pass"]),
        ToolEntry("await_offload_passes", await_offload_passes, TOOL_DESCRIPTIONS["await_offload_passes"]),
        ToolEntry("dispatch_wave", dispatch_wave, TOOL_DESCRIPTIONS["dispatch_wave"]),
        ToolEntry("offload_preflight", offload_preflight, TOOL_DESCRIPTIONS["offload_preflight"]),
        ToolEntry(
            "materialize_offload_lane_manifest",
            materialize_offload_lane_manifest,
            TOOL_DESCRIPTIONS["materialize_offload_lane_manifest"],
        ),
        ToolEntry("list_available_backends", list_available_backends, TOOL_DESCRIPTIONS["list_available_backends"]),
        ToolEntry("get_metrics_summary", get_metrics_summary, TOOL_DESCRIPTIONS["get_metrics_summary"]),
    ]


def _snapshot_registry(phase: str = "current") -> list[ToolEntry]:
    if phase != "current":
        raise ValueError("Unknown snapshot phase. The orchestrator tools snapshot only supports 'current'.")
    return _current_tool_entries()


def _build_tool_registry() -> list[ToolEntry]:
    """Build the orchestrator MCP tool registry (called lazily after all handlers defined)."""
    return _current_tool_entries()


def _orchestration_dir() -> Path:
    """Return the path to the workbay_orchestrator_mcp/orchestration/ package directory."""
    return Path(__file__).resolve().parent / "orchestration"


def _import_orchestration_module(name: str) -> Any:
    """Import a module from workbay_orchestrator_mcp.orchestration by bare name.

    Keeps the orchestration/ directory on sys.path so the orchestration
    scripts that rely on bare sibling imports (e.g. ``backend_adapter``)
    continue to work after being imported as a proper subpackage.
    """
    orchestration_dir = _orchestration_dir()
    if str(orchestration_dir) not in sys.path:
        sys.path.insert(0, str(orchestration_dir))
    bare_module = sys.modules.get(name)
    if bare_module is not None:
        return bare_module
    return importlib.import_module(f"workbay_orchestrator_mcp.orchestration.{name}")


def _runtime_pythonpath() -> str:
    package_root = Path(__file__).resolve().parents[4]
    disallowed_parts = {
        str(package_root / "packages" / "mcp-workbay-handoff" / "src"),
        str(package_root / "packages" / "mcp-workbay-orchestrator" / "src"),
    }
    pythonpath_parts = [
        str(package_root / "packages" / "workbay-codex-bridge" / "src"),
    ]
    existing = os.environ.get("PYTHONPATH")
    if existing:
        pythonpath_parts.extend(part for part in existing.split(":") if part and part not in disallowed_parts)
    return ":".join(part for part in pythonpath_parts if part)


def _daemon_runtime_env() -> dict[str, str]:
    env = dict(os.environ)
    runtime_pythonpath = _runtime_pythonpath()
    if runtime_pythonpath:
        env["PYTHONPATH"] = runtime_pythonpath
    return env


def _orchestrator_paths() -> dict[str, Path]:
    config = get_runtime_config()
    state_dir = config.state_dir
    return {
        "workspace_root": config.workspace_root,
        "state_dir": state_dir,
        "lock_path": state_dir / "orchestrator.lock",
        "pause_path": state_dir / "daemon-paused",
        "log_dir": config.workspace_root / "logs" / "daemon",
        "log_path": config.workspace_root / "logs" / "daemon" / "orchestrator.jsonl",
        "script_path": _orchestration_dir() / "orchestrator_daemon.py",
    }


def _worker_paths() -> dict[str, Path]:
    config = get_runtime_config()
    state_dir = config.state_dir
    log_dir = config.workspace_root / "logs" / "worker-daemon"
    return {
        "workspace_root": config.workspace_root,
        "state_dir": state_dir,
        "log_dir": log_dir,
        "script_path": _orchestration_dir() / "worker_daemon.py",
    }


def _worker_lane_config(task_ref: str, lane_id: str) -> dict[str, Any]:
    lane_manifest = _import_orchestration_module("lane_manifest")
    lane = lane_manifest.get_lane_config(task_ref, lane_id, orchestrator_root=str(get_runtime_config().workspace_root))
    if not isinstance(lane, dict):
        raise RuntimeError(f"Lane '{lane_id}' is not defined in the manifest for task '{task_ref}'.")
    return lane


def _evaluate_host_admission(
    workspace_root: Path,
    *,
    cost_class: str = "heavy",
    exclude_slots: frozenset[int] = frozenset(),
) -> Any:
    """internal: live host-memory admission verdict for a spawn edge.

    Never raises — a probe/registry failure degrades inside the probe (pressure
    ``warn``) rather than crashing the dispatch surface.

    ``WORKBAY_HOSTGOV_DISABLE=1`` bypasses the probe entirely and returns an
    unenforced allow. This keeps the hermetic test suite independent of the
    runner's live memory state (the orchestrator conftest sets it) and gives
    operators an env-level kill switch equivalent to ``enforcement: off``.

    ``exclude_slots`` is forwarded only on the live (non-disabled) path so a
    coordinator that already holds a heavy slot does not self-count it
    (implementation note S3). Ownership is verified inside ``count_held_heavy_slots``.
    """
    from workbay_orchestrator_mcp.orchestration.host_resources import (
        AdmissionDecision,
        HostResources,
        resolve_live_admission,
    )

    if os.environ.get("WORKBAY_HOSTGOV_DISABLE") == "1":
        return AdmissionDecision(
            "allow",
            "admission disabled (WORKBAY_HOSTGOV_DISABLE=1)",
            cost_class,
            0,
            0,
            False,
            HostResources(platform="disabled"),
        )
    return resolve_live_admission(workspace_root, cost_class, exclude_slots=exclude_slots)


def _admission_gate_error(
    admission: Any,
    *,
    override: bool,
    task_ref: str | None,
    workspace_root: Path | None = None,
    surface: str = "dispatch",
    lane_id: str | None = None,
) -> dict[str, Any] | None:
    """Structured recoverable error for a hard-gated spawn, or None to proceed.

    ``allow`` (or an unenforced ``warn_only``/``off`` downgrade, which the
    decision already reports as ``allow``) proceeds. An enforced ``refuse``/
    ``defer`` blocks unless ``override`` is set. Blocks are recorded as
    best-effort handoff decision telemetry (internal D6) when a
    ``workspace_root`` is supplied.
    """
    if override:
        # D5 reset: an explicit operator override also clears the post-crash
        # breaker marker so the next non-overridden spawn is back at full width.
        if workspace_root is not None and task_ref:
            from workbay_orchestrator_mcp.orchestration.host_resources import (
                clear_crash_breaker,
            )

            if clear_crash_breaker(workspace_root, task_ref):
                _logger.info("admission_override cleared the crash-breaker marker for %s", task_ref)
        return None
    if admission.decision == "allow":
        return None
    if workspace_root is not None:
        from workbay_orchestrator_mcp.orchestration.host_resources import (
            record_admission_telemetry,
        )

        record_admission_telemetry(workspace_root, admission, surface=surface, task_ref=task_ref, lane_id=lane_id)
    error_kind = "admission_refused" if admission.decision == "refuse" else "admission_deferred"
    payload: dict[str, Any] = {
        "ok": False,
        # Carry `outcome` as well as `error_kind`: worker_start's sibling
        # fail-fasts (no_actionable_work, missing worktree) all set `outcome`,
        # and dispatch_lane_work reads worker_start_result.get("outcome") to
        # avoid masking a refusal behind its own ok:True — without this key an
        # admission refusal would surface as a *successful* dispatch outcome.
        "outcome": error_kind,
        "error": f"host memory admission {admission.decision}: {admission.reason}",
        "error_kind": error_kind,
        "admission": admission.to_dict(),
    }
    if task_ref:
        payload["task_ref"] = task_ref
    return payload


def _read_lock_pid(lock_path: Path) -> int | None:
    if not lock_path.exists():
        return None
    try:
        payload = json.loads(lock_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    pid = payload.get("pid")
    return int(pid) if isinstance(pid, int) or isinstance(pid, str) and str(pid).isdigit() else None


def _pid_is_running(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _last_log_event(log_path: Path) -> dict[str, Any] | None:
    if not log_path.exists():
        return None
    try:
        for line in reversed(log_path.read_text().splitlines()):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
    except OSError:
        return None
    return None


def _count_log_events(log_path: Path, event_name: str) -> int:
    if not log_path.exists():
        return 0
    try:
        count = 0
        for line in log_path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("event") == event_name:
                count += 1
        return count
    except OSError:
        return 0


def _load_response_payload(payload: dict[str, Any] | str | bytes | bytearray) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    loaded = json.loads(payload)
    if not isinstance(loaded, dict):
        raise TypeError(f"Expected object payload, got {type(loaded).__name__}")
    return loaded


# ---------------------------------------------------------------------------
# Orchestration wrapper tools
# ---------------------------------------------------------------------------


def orchestrator_start(
    task_ref: str,
    backend: str = "codex-cli",
    poll_interval: int = 60,
    single_pass: bool = False,
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
) -> dict:
    paths = _orchestrator_paths()
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        backend_name = backend_registry.validate_backend(backend)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    existing_pid = _read_lock_pid(paths["lock_path"])
    if _pid_is_running(existing_pid):
        return core._json_response(
            {
                "ok": False,
                "error": "Orchestrator daemon is already running.",
                "pid": existing_pid,
                "lock_path": str(paths["lock_path"]),
            }
        )

    env = _daemon_runtime_env()
    cmd = [
        sys.executable,
        str(paths["script_path"]),
        "run",
        "--orchestrator-root",
        str(paths["workspace_root"]),
        "--state-dir",
        str(paths["state_dir"]),
        "--task-ref",
        task_ref,
        "--backend",
        backend_name,
        "--poll-interval",
        str(poll_interval),
        "--worker-start-mode",
        worker_start_mode,
        "--worker-reasoning-effort",
        worker_reasoning_effort,
    ]
    if model:
        cmd.extend(["--model", model])
    if single_pass:
        cmd.append("--single-pass")
    log_dir = paths["log_dir"]
    log_dir.mkdir(parents=True, exist_ok=True)
    stderr_fh = (log_dir / "orchestrator.stderr").open("a")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(paths["workspace_root"]),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_fh,
            start_new_session=True,
        )
    finally:
        stderr_fh.close()
    return core._json_response(
        {
            "ok": True,
            "pid": proc.pid,
            "lock_path": str(paths["lock_path"]),
            "backend": backend_name,
            "single_pass": single_pass,
            "worker_start_mode": worker_start_mode,
            "worker_reasoning_effort": worker_reasoning_effort,
        }
    )


def orchestrator_status() -> dict:
    paths = _orchestrator_paths()
    orchestrator_daemon = _import_orchestration_module("orchestrator_daemon")
    status = orchestrator_daemon.daemon_status(paths["state_dir"], paths["log_dir"])
    pid = None
    lock_info = status.get("lock")
    if isinstance(lock_info, dict):
        raw_pid = lock_info.get("pid")
        if isinstance(raw_pid, int) or isinstance(raw_pid, str) and str(raw_pid).isdigit():
            pid = int(raw_pid)
    running = _pid_is_running(pid)
    last_event = _last_log_event(paths["log_path"])
    task_ref = None
    if isinstance(last_event, dict):
        raw_task_ref = last_event.get("task_ref")
        if isinstance(raw_task_ref, str) and raw_task_ref.strip():
            task_ref = raw_task_ref
    return core._json_response(
        {
            "ok": True,
            "running": running,
            "pid": pid,
            "task_ref": task_ref,
            "cycle_count": _count_log_events(paths["log_path"], "cycle_end"),
            "last_event": last_event,
            "paused": bool(status.get("paused")),
            "lock_path": str(paths["lock_path"]),
            "status": status,
        }
    )


def orchestrator_pause() -> dict:
    paths = _orchestrator_paths()
    orchestrator_daemon = _import_orchestration_module("orchestrator_daemon")
    orchestrator_daemon.daemon_pause(paths["state_dir"])
    return core._json_response(
        {
            "ok": True,
            "paused": True,
            "pause_path": str(paths["pause_path"]),
        }
    )


def orchestrator_resume() -> dict:
    paths = _orchestrator_paths()
    orchestrator_daemon = _import_orchestration_module("orchestrator_daemon")
    orchestrator_daemon.daemon_resume(paths["state_dir"])
    return core._json_response(
        {
            "ok": True,
            "paused": False,
            "pause_path": str(paths["pause_path"]),
        }
    )


def orchestrator_stop(force: bool = False, wait_seconds: float = 5.0) -> dict:
    paths = _orchestrator_paths()
    pid = _read_lock_pid(paths["lock_path"])
    if not _pid_is_running(pid):
        return core._json_response(
            {
                "ok": True,
                "running": False,
                "pid": pid,
                "exit_code": None,
            }
        )

    sig = signal.SIGKILL if force else signal.SIGTERM
    if pid is None:
        return core._json_response({"ok": False, "error": "Orchestrator lock exists but no pid could be read."})
    os.kill(pid, sig)
    deadline = time.monotonic() + max(wait_seconds, 0.0)
    while time.monotonic() < deadline:
        if not _pid_is_running(pid):
            return core._json_response(
                {
                    "ok": True,
                    "running": False,
                    "pid": pid,
                    "exit_code": -int(sig),
                }
            )
        time.sleep(0.05)
    return core._json_response(
        {
            "ok": False,
            "error": f"Orchestrator daemon did not exit after {signal.Signals(sig).name}.",
            "running": True,
            "pid": pid,
        }
    )


def orchestrator_single_cycle(
    task_ref: str,
    backend: str = "codex-cli",
    dry_run: bool = False,
    timeout_seconds: float = 300.0,
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
) -> dict:
    """Run one orchestrator cycle synchronously (dispatch, poll, intake, verify)."""
    paths = _orchestrator_paths()
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        backend_name = backend_registry.validate_backend(backend)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    env = _daemon_runtime_env()
    cmd = [
        sys.executable,
        str(paths["script_path"]),
        "run",
        "--orchestrator-root",
        str(paths["workspace_root"]),
        "--task-ref",
        task_ref,
        "--backend",
        backend_name,
        "--worker-start-mode",
        worker_start_mode,
        "--worker-reasoning-effort",
        worker_reasoning_effort,
        "--single-pass",
    ]
    if model:
        cmd.extend(["--model", model])
    if dry_run:
        cmd.append("--dry-run")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(paths["workspace_root"]),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(timeout_seconds, 1.0),
        )
    except subprocess.TimeoutExpired:
        return core._json_response(
            {
                "ok": False,
                "error": f"Orchestrator single cycle timed out after {timeout_seconds} seconds.",
            }
        )
    return core._json_response(
        {
            "ok": result.returncode == 0,
            "exit_code": result.returncode,
            "backend": backend_name,
            "dry_run": dry_run,
            "worker_start_mode": worker_start_mode,
            "worker_reasoning_effort": worker_reasoning_effort,
            "stderr": result.stderr[-2000:] if result.stderr else "",
        }
    )


def manage_orchestrator(
    operation: str,
    task_ref: str | None = None,
    backend: str = "codex-cli",
    poll_interval: int = 60,
    single_pass: bool = False,
    worker_start_mode: str = "mcp",
    worker_reasoning_effort: str = "auto",
    model: str | None = None,
    force: bool = False,
    wait_seconds: float = 5.0,
    dry_run: bool = False,
    timeout_seconds: float = 300.0,
) -> dict:
    """Compound tool for orchestrator-daemon lifecycle and single-cycle operations."""
    valid_operations = {"pause", "resume", "single_cycle", "start", "status", "stop"}
    if operation not in valid_operations:
        return core._json_response(
            {"ok": False, "error": f"Invalid operation. Valid: {', '.join(sorted(valid_operations))}"}
        )
    if operation in {"start", "single_cycle"} and (task_ref is None or not str(task_ref).strip()):
        return core._json_response({"ok": False, "error": f"Operation '{operation}' requires task_ref."})
    if operation in {"start", "single_cycle"}:
        try:
            _ensure_daemons_enabled()
        except DaemonsDisabledError as exc:
            return core._json_response({"ok": False, "error": str(exc)})
    if operation == "start":
        return orchestrator_start(
            task_ref=str(task_ref),
            backend=backend,
            poll_interval=poll_interval,
            single_pass=single_pass,
            worker_start_mode=worker_start_mode,
            worker_reasoning_effort=worker_reasoning_effort,
            model=model,
        )
    if operation == "status":
        return orchestrator_status()
    if operation == "pause":
        return orchestrator_pause()
    if operation == "resume":
        return orchestrator_resume()
    if operation == "stop":
        return orchestrator_stop(force=force, wait_seconds=wait_seconds)
    return orchestrator_single_cycle(
        task_ref=str(task_ref),
        backend=backend,
        dry_run=dry_run,
        timeout_seconds=timeout_seconds,
        worker_start_mode=worker_start_mode,
        worker_reasoning_effort=worker_reasoning_effort,
        model=model,
    )


def _non_actionable_lane_message(lane_id: str, lane_state: str) -> str:
    """Refusal text for a non-actionable lane, branched on lane_state (S1-A-004).

    'waiting' means the worker already handed a result back and is awaiting the
    orchestrator — advising a fresh brief there is wrong; the caller must resolve
    the pending handoff instead.
    """
    if lane_state == "waiting":
        return (
            f"Lane '{lane_id}' has no actionable work (lane state: waiting): the worker already "
            "handed a result back and is awaiting the orchestrator. Resolve the pending handoff "
            "(review worker_reports / lane_communication and resume), do not record a new brief."
        )
    return (
        f"Lane '{lane_id}' has no actionable work (lane state: {lane_state}). "
        "Record a brief first via dispatch_lane_work(brief=...), then start the worker."
    )


def worker_start(
    task_ref: str,
    lane_id: str,
    backend: str | None = None,
    poll_interval: int = 30,
    single_pass: bool = False,
    session: str | None = None,
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
    token_budget: int | None = None,
    admission_override: bool = False,
) -> dict:
    paths = _worker_paths()
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        offload_profiles = _import_orchestration_module("offload_profiles")
        lane = _worker_lane_config(task_ref, lane_id)
        worker_daemon_ctl = _import_orchestration_module("worker_daemon_ctl")
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    worktree_path = Path(str(lane.get("worktree_path") or "")).expanduser().resolve()
    if not worktree_path.exists():
        return core._json_response(
            {
                "ok": False,
                "error": f"Lane worktree does not exist for lane '{lane_id}': {worktree_path}",
            }
        )

    # implementation note bra1: shared spawn edge consults the bootstrap execution_mode
    # ledger (manage_worker start/start_all, worker_start_all, dispatch
    # start_worker all route here). Explicit local backends under remote_only
    # refuse with typed remote_required; unset backend defaults to grok-remote
    # under remote_only (same helper as run_offload_pass / materialize).
    resolved_backend, remote_required_error = offload_profiles.resolve_offload_backend_for_execution_mode(
        backend,
        repo_root=worktree_path,
    )
    if remote_required_error is not None:
        return core._json_response(
            {
                "ok": False,
                "error": remote_required_error,
                "outcome": "remote_required",
                "backend": resolved_backend,
                "lane_id": lane_id,
            }
        )
    try:
        backend_name = backend_registry.validate_backend(resolved_backend)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    # Fail-closed grok-remote dispatch gate (implementation note H4/M5): the shared daemon
    # spawn edge (dispatch_lane_work(start_worker=True) / manage_worker start /
    # worker_start_all all route here) refuses grok-remote until S3/S5 land.
    _remote_block = backend_registry.grok_remote_dispatch_block_reason(backend_name)
    if _remote_block is not None:
        return core._json_response({"ok": False, "error": _remote_block})

    # internal S1 fail-fast empty inbox: never spawn a worker over a
    # lane with no actionable brief — the process would exit dormant and the
    # caller would read the ok/pid response as success.
    worker_daemon = _import_orchestration_module("worker_daemon")
    try:
        lane_state = worker_daemon.poll_lane_state(
            orchestrator_root=paths["workspace_root"],
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
        )
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    if lane_state != "actionable":
        return core._json_response(
            {
                "ok": False,
                "outcome": "no_actionable_work",
                "lane_state": lane_state,
                "error": _non_actionable_lane_message(lane_id, lane_state),
            }
        )

    # internal D2: host-memory admission at the shared spawn edge. This one
    # gate covers dispatch_lane_work(start_worker=True), manage_worker start,
    # and worker_start_all — all route here. Evaluate-only: the detached worker
    # acquires+holds the slot for its lifetime, so acquiring here (in the
    # returning MCP process) would drop the flock the moment the call returns.
    # Cost class from the backend profile (internal D1): a
    # grok-cli lane is remote-API (small local RSS), not heavy.
    admission = _evaluate_host_admission(
        paths["workspace_root"], cost_class=backend_registry.cost_class_for_backend(backend_name)
    )
    gate = _admission_gate_error(
        admission,
        override=admission_override,
        task_ref=task_ref,
        workspace_root=paths["workspace_root"],
        surface="worker_start",
        lane_id=lane_id,
    )
    if gate is not None:
        gate["lane_id"] = lane_id
        return core._json_response(gate)

    grok_max_turns: int | None = None
    grok_timeout: int | None = None
    if token_budget is not None and token_budget > 0:
        offload_preflight = _import_orchestration_module("offload_preflight")
        if backend_registry.backend_supports_token_budget_cycle_bounds(backend_name):
            bounds = offload_preflight.derive_grok_single_cycle_bounds(token_budget)
            grok_max_turns = bounds["max_turns"]
            grok_timeout = bounds["timeout"]
        elif backend_registry.backend_supports_adapter_timeout_bounds(backend_name):
            # Wall-clock-only family: a timeout, and deliberately no max_turns —
            # its CLI has no flag to enforce one. Sibling derivation site to
            # offload_pass; both must stay on the same union semantics or a lane
            # started through manage_worker runs unbounded by its budget.
            grok_timeout = offload_preflight.derive_adapter_timeout_bounds(token_budget)["timeout"]

    payload = worker_daemon_ctl.daemon_start(
        orchestrator_root=paths["workspace_root"],
        state_dir=paths["state_dir"],
        log_dir=paths["log_dir"],
        task_ref=task_ref,
        lane_id=lane_id,
        worktree_path=worktree_path,
        session=session or f"{task_ref}-{lane_id}",
        python_executable=sys.executable,
        pythonpath=_runtime_pythonpath(),
        backend=backend_name,
        session_mode=session_mode,
        reasoning_effort=reasoning_effort,
        model=model,
        poll_interval=poll_interval,
        single_pass=single_pass,
        token_budget=token_budget,
        grok_max_turns=grok_max_turns,
        grok_timeout=grok_timeout,
    )
    return core._json_response(payload)


def worker_status(task_ref: str, lane_id: str) -> dict:
    paths = _worker_paths()
    worker_daemon_ctl = _import_orchestration_module("worker_daemon_ctl")
    payload = worker_daemon_ctl.daemon_status(
        state_dir=paths["state_dir"],
        log_dir=paths["log_dir"],
        lane_id=lane_id,
        task_ref=task_ref,
    )
    process = payload.get("process")
    running = isinstance(process, dict) and isinstance(process.get("pid"), int)
    payload["running"] = running
    payload["ok"] = True
    return core._json_response(payload)


def worker_event_history(
    task_ref: str,
    lane_id: str,
    limit: int = 50,
    event_name: str | None = None,
) -> dict:
    paths = _worker_paths()
    worker_daemon_ctl = _import_orchestration_module("worker_daemon_ctl")
    payload = worker_daemon_ctl.daemon_event_history(
        state_dir=paths["state_dir"],
        log_dir=paths["log_dir"],
        lane_id=lane_id,
        task_ref=task_ref,
        limit=limit,
        event_name=event_name,
    )
    process = payload.get("process")
    payload["running"] = isinstance(process, dict) and isinstance(process.get("pid"), int)
    payload["ok"] = True
    return core._json_response(payload)


def worker_stop(task_ref: str, lane_id: str, force: bool = False) -> dict:
    paths = _worker_paths()
    worker_daemon_ctl = _import_orchestration_module("worker_daemon_ctl")
    payload = worker_daemon_ctl.daemon_stop(
        state_dir=paths["state_dir"],
        log_dir=paths["log_dir"],
        lane_id=lane_id,
        task_ref=task_ref,
        force=force,
    )
    return core._json_response(payload)


def worker_resume(task_ref: str, lane_id: str) -> dict:
    paths = _worker_paths()
    worker_daemon_ctl = _import_orchestration_module("worker_daemon_ctl")
    # S1-A-002: mirror worker_start's fail-fast preflight — reviving a worker over
    # an idle lane (empty inbox, no actionable brief) would just exit dormant and
    # the caller would read the resume as success.
    try:
        lane = _worker_lane_config(task_ref, lane_id)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    worktree_path = Path(str(lane.get("worktree_path") or "")).expanduser().resolve()
    worker_daemon = _import_orchestration_module("worker_daemon")
    try:
        lane_state = worker_daemon.poll_lane_state(
            orchestrator_root=paths["workspace_root"],
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=worktree_path,
        )
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    if lane_state == "idle":
        return core._json_response(
            {
                "ok": False,
                "outcome": "no_actionable_work",
                "lane_state": lane_state,
                "error": _non_actionable_lane_message(lane_id, lane_state),
            }
        )
    payload = worker_daemon_ctl.daemon_resume(
        state_dir=paths["state_dir"],
        log_dir=paths["log_dir"],
        lane_id=lane_id,
        task_ref=task_ref,
    )
    return core._json_response(payload)


def worker_start_all(
    task_ref: str,
    backend: str = "codex-subagent",
    poll_interval: int = 30,
    single_pass: bool = False,
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
    token_budget: int | None = None,
    admission_override: bool = False,
) -> dict:
    try:
        lane_manifest = _import_orchestration_module("lane_manifest")
        orchestrator_lanes = _import_orchestration_module("orchestrator_lanes")
        merge_order_fn = getattr(lane_manifest, "merge_order", None)
        manifest_order = merge_order_fn(task_ref) if callable(merge_order_fn) else []
        lane_ids = manifest_order or lane_manifest.list_lanes(task_ref)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    # Edge source (implementation note Objective 5 / implementation note): depends_on when the total
    # declared edge set is non-empty (or WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1 →
    # unconstrained depends_on mode). Otherwise legacy merge-order prefix +
    # _lane_has_capacity idleness proxy. Manifest unreadable → total_edges=0.
    orchestrator_root: Path | None = None
    try:
        orchestrator_root = Path(get_runtime_config().workspace_root).expanduser().resolve()
    except Exception:  # noqa: BLE001 — degrade to legacy without a root
        orchestrator_root = None

    depends_on: dict[str, list[str]] = {}
    scheduling_active = False
    load_sched = getattr(orchestrator_lanes, "load_manifest_scheduling_state", None)
    if callable(load_sched) and orchestrator_root is not None:
        try:
            sched = load_sched(
                task_ref,
                orchestrator_root=orchestrator_root,
                lane_manifest_module=lane_manifest,
            )
            if (
                isinstance(sched, tuple)
                and len(sched) == 3
                and isinstance(sched[2], bool)
            ):
                depends_on = sched[0] if isinstance(sched[0], dict) else {}
                scheduling_active = bool(sched[2])
        except Exception:  # noqa: BLE001 — Mock/unreadable → legacy
            depends_on, scheduling_active = {}, False

    collect_unsatisfied = getattr(orchestrator_lanes, "collect_unsatisfied_dependencies", None)
    reason_check_failed = getattr(
        orchestrator_lanes,
        "REASON_DEPENDENCY_CHECK_FAILED",
        "dependency_check_failed",
    )
    count_refusal = getattr(orchestrator_lanes, "_count_dependency_refusal", None)
    log_refusal_summary = getattr(orchestrator_lanes, "log_dependency_refusal_summary", None)
    # Prefer the shared shape validator when present (real module); tests may
    # hand a Mock without it — fall back to the same (list, str|None) contract.
    parse_collect = getattr(orchestrator_lanes, "parse_collect_unsatisfied_result", None)

    def _dep_log(level: str, event: str, **fields: Any) -> None:
        # Match daemon log(level, event, **fields) so collect_unsatisfied
        # refusals are recorded on the operator surface too.
        try:
            log_level = getattr(logging, str(level).upper(), logging.INFO)
            detail = " ".join(f"{k}={v!r}" for k, v in fields.items())
            _logger.log(log_level, "%s %s", event, detail)
        except Exception:  # noqa: BLE001 — observability must not abort start_all
            pass

    def _mark_dependency_check_failed(
        *,
        lane_id: str,
        detail: str,
    ) -> tuple[bool, str, list[str]]:
        reason = (
            reason_check_failed
            if isinstance(reason_check_failed, str)
            else "dependency_check_failed"
        )
        _logger.warning(
            "dependency_check_failed lane_id=%s task_ref=%s: %s",
            lane_id,
            task_ref,
            detail,
        )
        if callable(count_refusal):
            try:
                count_refusal(reason)
            except Exception:  # noqa: BLE001 — observability must not abort start_all
                pass
        return True, reason, []

    results: list[dict[str, Any]] = []
    for lane_id in lane_ids:
        blocked_by: list[str] = []
        refusal_reason = "unresolved_upstream_dependencies"
        dependency_check_failed = False
        if scheduling_active:
            # depends_on scheduling: transitive ancestors via the completion
            # predicate. Empty ancestor set (roots / unconstrained empty graph)
            # starts freely. _lane_has_capacity is backpressure-only and is not
            # consulted for dependency readiness here.
            # Helpers missing, root missing, raise, or non-conforming return
            # all refuse (fail closed) — never fall open under an active graph.
            if not callable(collect_unsatisfied) or orchestrator_root is None:
                dependency_check_failed, refusal_reason, blocked_by = _mark_dependency_check_failed(
                    lane_id=lane_id,
                    detail=(
                        "collect_unsatisfied_dependencies unavailable"
                        if not callable(collect_unsatisfied)
                        else "orchestrator_root unavailable"
                    ),
                )
            else:
                try:
                    collected = collect_unsatisfied(
                        orchestrator_root,
                        task_ref,
                        lane_id,
                        depends_on if isinstance(depends_on, dict) else {},
                        log=_dep_log,
                    )
                    parsed = None
                    if callable(parse_collect):
                        try:
                            candidate = parse_collect(collected)
                        except Exception:  # noqa: BLE001 — stub/mock helpers
                            candidate = None
                        # Accept only a real (list, str|None) 2-tuple; Mock
                        # getattr stubs return non-tuples and must not pass.
                        if (
                            isinstance(candidate, tuple)
                            and len(candidate) == 2
                            and isinstance(candidate[0], list)
                            and (candidate[1] is None or isinstance(candidate[1], str))
                        ):
                            parsed = (
                                [b for b in candidate[0] if isinstance(b, str)],
                                candidate[1]
                                if isinstance(candidate[1], str) and candidate[1]
                                else None,
                            )
                    if parsed is None and (
                        isinstance(collected, tuple)
                        and len(collected) == 2
                        and isinstance(collected[0], list)
                        and (collected[1] is None or isinstance(collected[1], str))
                    ):
                        raw_blocked, raw_reason = collected
                        parsed = (
                            [b for b in raw_blocked if isinstance(b, str)],
                            raw_reason if isinstance(raw_reason, str) and raw_reason else None,
                        )
                    if parsed is None:
                        dependency_check_failed, refusal_reason, blocked_by = (
                            _mark_dependency_check_failed(
                                lane_id=lane_id,
                                detail="collect_unsatisfied_dependencies returned invalid shape",
                            )
                        )
                    else:
                        blocked_by, reason = parsed
                        if isinstance(reason, str) and reason:
                            refusal_reason = reason
                except Exception as exc:  # noqa: BLE001 — fail closed (never fall open)
                    dependency_check_failed, refusal_reason, blocked_by = (
                        _mark_dependency_check_failed(lane_id=lane_id, detail=str(exc))
                    )
        elif lane_id in manifest_order:
            # Legacy merge-order prefix gating (total depends_on edges == 0).
            lane_index = manifest_order.index(lane_id)
            dependency_error: dict[str, Any] | None = None
            for upstream_lane in manifest_order[:lane_index]:
                try:
                    has_capacity = bool(orchestrator_lanes._lane_has_capacity(task_ref, upstream_lane))
                except RuntimeError as exc:
                    dependency_error = {
                        "ok": False,
                        "lane_id": lane_id,
                        "error": f"dependency check failed for upstream lane '{upstream_lane}': {exc}",
                    }
                    break
                if not has_capacity:
                    blocked_by.append(upstream_lane)
            if dependency_error is not None:
                results.append(dependency_error)
                continue
        if blocked_by or dependency_check_failed:
            results.append(
                {
                    "ok": True,
                    "lane_id": lane_id,
                    "started": False,
                    "skipped": True,
                    "reason": refusal_reason,
                    "blocked_by": blocked_by,
                }
            )
            continue
        try:
            result = _load_response_payload(
                worker_start(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    backend=backend,
                    poll_interval=poll_interval,
                    single_pass=single_pass,
                    session_mode=session_mode,
                    reasoning_effort=reasoning_effort,
                    model=model,
                    token_budget=token_budget,
                    admission_override=admission_override,
                )
            )
        except Exception as exc:
            result = {
                "ok": False,
                "lane_id": lane_id,
                "error": f"worker_start raised {type(exc).__name__}: {exc}",
            }
        results.append(result)
    if callable(log_refusal_summary):
        try:
            log_refusal_summary(_dep_log, reset=False, task_ref=task_ref, surface="worker_start_all")
        except Exception:  # noqa: BLE001 — observability must not abort start_all
            pass
    return core._json_response(
        {
            "ok": all(bool(item.get("ok")) for item in results),
            "task_ref": task_ref,
            "backend": backend,
            "session_mode": session_mode,
            "reasoning_effort": reasoning_effort,
            "results": results,
        }
    )


def manage_worker(
    task_ref: str,
    action: str,
    lane_id: str | None = None,
    backend: str = "codex-subagent",
    poll_interval: int = 30,
    single_pass: bool = False,
    session: str | None = None,
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
    token_budget: int | None = None,
    force: bool = False,
    limit: int = 50,
    event_name: str | None = None,
    admission_override: bool = False,
) -> dict:
    """Compound tool for worker-daemon lifecycle, inspection, and bulk starts.

    action values:
    - "start"   — start a lane worker with the given parameters.
    - "stop"    — stop a lane worker (force=True for SIGKILL).
    - "resume"  — resume a stopped lane worker.
    - "status"  — inspect lane-worker runtime status.
    - "event_history" — read recent worker-daemon events for a lane.
    - "start_all" — start workers for every lane declared in the task.
    """
    lane_actions = {"start", "stop", "resume", "status", "event_history"}
    if action in lane_actions and (lane_id is None or not str(lane_id).strip()):
        return core._json_response(
            {
                "ok": False,
                "error": f"Action '{action}' requires lane_id.",
            }
        )
    if action in {"start", "start_all"}:
        try:
            _ensure_daemons_enabled()
        except DaemonsDisabledError as exc:
            return core._json_response({"ok": False, "error": str(exc)})

    if action == "start":
        return worker_start(
            task_ref=task_ref,
            lane_id=str(lane_id),
            backend=backend,
            poll_interval=poll_interval,
            single_pass=single_pass,
            session=session,
            session_mode=session_mode,
            reasoning_effort=reasoning_effort,
            model=model,
            token_budget=token_budget,
            admission_override=admission_override,
        )
    if action == "stop":
        return worker_stop(task_ref=task_ref, lane_id=str(lane_id), force=force)
    if action == "resume":
        return worker_resume(task_ref=task_ref, lane_id=str(lane_id))
    if action == "status":
        return worker_status(task_ref=task_ref, lane_id=str(lane_id))
    if action == "event_history":
        return worker_event_history(
            task_ref=task_ref,
            lane_id=str(lane_id),
            limit=limit,
            event_name=event_name,
        )
    if action == "start_all":
        return worker_start_all(
            task_ref=task_ref,
            backend=backend,
            poll_interval=poll_interval,
            single_pass=single_pass,
            session_mode=session_mode,
            reasoning_effort=reasoning_effort,
            model=model,
            token_budget=token_budget,
            admission_override=admission_override,
        )
    return core._json_response(
        {
            "ok": False,
            "error": (
                f"Unknown action '{action}'. Valid values: start, stop, resume, status, event_history, start_all."
            ),
        }
    )


def _run_in_process_structured_turn(
    backend_registry: Any,
    backend_name: str,
    *,
    prompt: str,
    schema: dict[str, Any],
    cwd: str,
    env: dict[str, str] | None,
    timeout_seconds: float,
) -> dict:
    """Dispatch an in-process backend through its adapter runner seam (internal).

    In-process adapters compose a downstream backend; calling their runner seam
    (not ``execute()``) preserves arbitrary caller schemas verbatim —
    ``BackendResult`` coercion is a worker-lane concern. The downstream
    composition owns the timeout (threaded via the adapter constructor), so no
    second executor layer wraps this call: exactly one timeout layer governs,
    and timeout errors are reported by the downstream invocation that owns
    them.

    Envelope handling is provenance-based, not shape-sniffed: the downstream
    ``{"ok", "backend", "result"|"error"}`` envelope is unwrapped only when the
    adapter reports ``runner_emits_envelope`` (its composed default runner).
    Injected runners pass through verbatim, so caller schemas that happen to
    contain ``ok``/``result`` keys are never corrupted.
    """
    try:
        adapter = backend_registry.get_adapter(backend_name, timeout_seconds=timeout_seconds)
        runner = adapter.resolve_runner()
    except (RuntimeError, ImportError, AttributeError) as exc:
        # get_adapter imports the adapter module and getattrs the class, so a
        # broken adapter_path raises ImportError/AttributeError — map those to
        # the same clean envelope the bridge path produces via resolve_bridge.
        return core._json_response({"ok": False, "error": str(exc), "backend": backend_name})

    # Provenance-based envelope contract: only the adapter's composed default
    # runner is guaranteed to return the downstream run_structured_turn
    # envelope. Injected runners pass through verbatim — a caller schema that
    # merely looks like the envelope must never be unwrapped.
    emits_envelope = bool(getattr(adapter, "runner_emits_envelope", False))

    runner_kwargs: dict[str, Any] = {
        "prompt": prompt,
        "schema": schema,
        "cwd": cwd,
    }
    if env is not None:
        runner_kwargs["env"] = env

    def _invoke_runner() -> Any:
        try:
            return runner(**runner_kwargs)
        except TypeError as exc:
            # Mirror the bridge path's runner-signature tolerance: retry once
            # without env when the runner does not accept it.
            if env is None or "env" not in str(exc):
                raise
            retry_kwargs = dict(runner_kwargs)
            retry_kwargs.pop("env", None)
            return runner(**retry_kwargs)

    try:
        payload = _invoke_runner()
    except (RuntimeError, TypeError) as exc:
        return core._json_response({"ok": False, "error": str(exc), "backend": backend_name})

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            return core._json_response(
                {
                    "ok": False,
                    "error": f"{backend_name} backend returned invalid JSON: {exc}",
                    "backend": backend_name,
                }
            )
    if not isinstance(payload, dict):
        return core._json_response(
            {
                "ok": False,
                "error": f"{backend_name} backend returned non-object payload: {type(payload).__name__}",
                "backend": backend_name,
            }
        )
    if emits_envelope:
        # Default-runner payload is the downstream run_structured_turn
        # envelope by construction; interpret it strictly.
        if payload.get("ok") is False:
            # Downstream error envelope: surface it verbatim, attributed to
            # this backend with the downstream named separately.
            return core._json_response(
                {
                    "ok": False,
                    "error": payload.get("error") or "unknown downstream backend error",
                    "backend": backend_name,
                    "downstream_backend": payload.get("backend"),
                }
            )
        if payload.get("ok") is True and isinstance(payload.get("result"), dict):
            # Downstream success envelope: unwrap to the result.
            payload = payload["result"]
        else:
            return core._json_response(
                {
                    "ok": False,
                    "error": f"{backend_name} downstream composition returned an unexpected envelope shape.",
                    "backend": backend_name,
                }
            )
    return core._json_response({"ok": True, "backend": backend_name, "result": payload})


def run_structured_turn(
    prompt: str,
    schema: dict[str, Any],
    cwd: str,
    backend: str = "codex-subagent",
    env: dict[str, str] | None = None,
    timeout_seconds: float = 120.0,
) -> dict:
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        backend_name = backend_registry.validate_backend(backend)
        spec = backend_registry.get_backend_spec(backend_name)
        if spec.kind == "cli":
            return core._json_response(
                {
                    "ok": False,
                    "error": "CLI backends are not supported for synchronous MCP turns. Use manage_orchestrator(operation='start') or a worker daemon instead.",
                }
            )
        if spec.kind == "in-process":
            return _run_in_process_structured_turn(
                backend_registry,
                backend_name,
                prompt=prompt,
                schema=schema,
                cwd=cwd,
                env=env,
                timeout_seconds=timeout_seconds,
            )
        runner = backend_registry.resolve_bridge(backend_name)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    runner_kwargs: dict[str, Any] = {
        "prompt": prompt,
        "schema": schema,
        "cwd": cwd,
    }
    if env is not None:
        runner_kwargs["env"] = env

    def _invoke_runner() -> Any:
        try:
            return runner(**runner_kwargs)
        except TypeError as exc:
            if env is None or "env" not in str(exc):
                raise
            retry_kwargs = dict(runner_kwargs)
            retry_kwargs.pop("env", None)
            return runner(**retry_kwargs)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_invoke_runner)
            payload = future.result(timeout=max(timeout_seconds, 0.0))
    except concurrent.futures.TimeoutError:
        return core._json_response(
            {
                "ok": False,
                "error": f"Structured turn timed out after {timeout_seconds} seconds.",
                "backend": backend,
            }
        )
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc), "backend": backend})
    except TypeError as exc:
        return core._json_response({"ok": False, "error": str(exc), "backend": backend})

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            return core._json_response(
                {
                    "ok": False,
                    "error": f"{backend_name} backend returned invalid JSON: {exc}",
                    "backend": backend_name,
                }
            )
    if not isinstance(payload, dict):
        return core._json_response(
            {
                "ok": False,
                "error": f"{backend_name} backend returned non-object payload: {type(payload).__name__}",
                "backend": backend_name,
            }
        )
    return core._json_response({"ok": True, "backend": backend_name, "result": payload})


def _grok_brief_subagent_steps_warning(backend: str | None, brief: str | None) -> str | None:
    """Return a named warning when a grok-cli brief requests subagent steps (T6).

    Warn-only: never blocks dispatch. Grok adapters hardcode ``--no-subagents``,
    so briefs that ask for ``/review-parallel`` or subagent fan-out waste tokens.
    """
    resolved = (backend or "").strip().lower()
    if resolved != "grok-cli":
        return None
    text = brief if isinstance(brief, str) else ""
    if not text or not _GROK_BRIEF_SUBAGENT_STEP_PATTERNS.search(text):
        return None
    return (
        f"{GROK_BRIEF_SUBAGENT_STEPS_WARNING}: grok-cli brief mentions "
        "/review-parallel or subagent fan-out; use in-lane /branch-review only "
        "(orchestrator owns the merge-gate /review-parallel)"
    )


# implementation note S2: brief-hygiene warnings. A grok-cli offload pass runs under a hard
# turn/time deadline (~900s); a TEST_CMD that re-runs the whole package suite, or
# a brief that instructs a full re-baseline, is the exact shape that timed out a
# pass in the 0113 grind. Warn-only (mirrors grok_brief_subagent_steps): reshape
# is the operator's call, but the risk is named up-front rather than left to lore.
BRIEF_TEST_CMD_FULL_SUITE_WARNING = "brief_test_cmd_full_suite"
BRIEF_REQUESTS_FULL_REBASELINE_WARNING = "brief_requests_full_rebaseline"
# A scoped pytest run carries a ``-k`` expr, a ``::`` node id, or a specific
# ``.py`` file. Absent all three the invocation targets a whole directory /
# package (or the bare suite) — the timeout-prone shape.
_PYTEST_SELECTOR_PATTERNS = re.compile(r"(?:\s-k(?:\s|=)|::|\S+\.py(?:\b|::))")
_BRIEF_FULL_REBASELINE_PATTERNS = re.compile(
    r"(?i)(re-?baseline|full (?:test )?suite|whole (?:test )?suite|entire (?:test )?suite"
    r"|run all (?:the )?tests|re-?run the (?:full|whole|entire) suite|recapture all .*golden)"
)


def _brief_test_cmd_hygiene_warnings(
    backend: str | None,
    test_cmd: str | None,
    brief: str | None,
) -> list[str]:
    """Return named brief-hygiene warnings for a grok-cli dispatch (T-0127-S2).

    Warn-only. Degrades cleanly for non-grok backends (returns []): the turn/time
    deadline these guard against is the grok-cli single-cycle bound.
    """
    if (backend or "").strip().lower() != "grok-cli":
        return []
    warnings: list[str] = []
    cmd = test_cmd if isinstance(test_cmd, str) else ""
    if "pytest" in cmd and not _PYTEST_SELECTOR_PATTERNS.search(cmd):
        warnings.append(
            f"{BRIEF_TEST_CMD_FULL_SUITE_WARNING}: TEST_CMD runs a whole-package "
            "pytest with no -k/::/file selector; scope it (e.g. `-k <expr>`) so the "
            "grok pass self-verifies inside its turn/time bound instead of timing out"
        )
    text = brief if isinstance(brief, str) else ""
    if text and _BRIEF_FULL_REBASELINE_PATTERNS.search(text):
        warnings.append(
            f"{BRIEF_REQUESTS_FULL_REBASELINE_WARNING}: brief instructs a full "
            "suite re-baseline/re-run; a whole-suite pass exceeds the grok "
            "turn/time deadline — scope the brief to the slice under change"
        )
    return warnings


def _resolve_dispatch_backend(
    backend: str | None, *, worktree_path: str
) -> tuple[str, str | None]:
    """Ledger-aware backend resolution for spawn paths (implementation note S2 / S2R-2 / bra2).

    Returns ``(resolved_backend, remote_required_error)``. When the error is set
    the caller must refuse with typed ``remote_required`` — never return a
    forbidden local backend under ``remote_only`` (bra2).
    """
    offload_profiles = _import_orchestration_module("offload_profiles")
    return offload_profiles.resolve_offload_backend_for_execution_mode(
        backend,
        repo_root=Path(worktree_path),
    )


def dispatch_lane_work(
    lane_id: str,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    task_ref: str | None = None,
    start_worker: bool = False,
    token_budget: int | None = None,
    brief: str | None = None,
    dispatch_id: str | None = None,
    test_cmd: str | None = None,
    include_context_packet: bool | None = None,
    context_targets: list[str] | None = None,
    admission_override: bool = False,
) -> dict:
    if start_worker:
        try:
            _ensure_daemons_enabled()
        except DaemonsDisabledError as exc:
            return core._json_response({"ok": False, "error": str(exc)})
    with core._get_db_connection() as conn:
        resolved_task_ref = core._resolve_task_ref(conn, task_ref)
        lane_row = _lanes._get_lane_row(conn, resolved_task_ref, lane_id)
        if lane_row is None:
            return core._json_response(
                {"ok": False, "error": f"Lane '{lane_id}' not found.", "outcome": "lane_not_found"}
            )

        # implementation note S3 [OBS-08]/T3]: validate/auto-materialize lane manifest
        # before dispatch so bootstrap never fails as bare exit-code-1.
        paths = _worker_paths()
        offload_preflight_mod = _import_orchestration_module("offload_preflight")
        # S3-A-01 [OBS-08]: mirror offload_pass — preflight glue must degrade to a
        # typed error payload, never an uncaught raise out of the MCP tool.
        try:
            ensure = offload_preflight_mod.ensure_lane_manifest_for_offload(
                orchestrator_root=paths["workspace_root"],
                task_ref=resolved_task_ref,
                lane_id=lane_id,
                worktree_path=str(lane_row.get("worktree_path") or paths["workspace_root"]),
                branch=str(lane_row.get("branch") or "").strip() or None,
                preferred_backend=backend or lane_row.get("backend"),
                preferred_model=model or lane_row.get("model"),
                preferred_reasoning_effort=reasoning_effort or lane_row.get("reasoning_effort"),
                auto_materialize=True,
            )
        except Exception as exc:  # noqa: BLE001 — never crash dispatch on preflight glue
            return core._json_response(
                {
                    "ok": False,
                    "outcome": "error",
                    "error": f"no manifest for {lane_id}; run materialize_offload_lane_manifest ({exc})",
                    "failed_stage": "execute",
                }
            )
        if not ensure.get("ok"):
            # S2R-4: preserve a typed policy outcome (remote_required) from the
            # ensure path instead of collapsing it into a generic error — the
            # discriminator is what callers branch on ([API-05]).
            return core._json_response(
                {
                    "ok": False,
                    "outcome": str(ensure.get("outcome") or "error"),
                    "error": str(
                        ensure.get("error") or f"no manifest for {lane_id}; run materialize_offload_lane_manifest"
                    ),
                    "failed_stage": "execute" if not ensure.get("outcome") else None,
                }
            )

        # bra2: prefer ensure/materialize pin over raw lane_row.backend, then
        # re-check execution_mode so a stale DB backend (e.g. codex-subagent)
        # cannot slip through under remote_only when start_worker=True.
        worktree_for_dispatch = str(lane_row.get("worktree_path") or paths["workspace_root"])
        lane_cfg = ensure.get("lane_config")
        pin_backend: str | None = None
        if isinstance(lane_cfg, dict):
            raw_pin = lane_cfg.get("preferred_backend")
            pin_backend = str(raw_pin).strip() if raw_pin else None
        elif lane_cfg is not None:
            raw_pin = getattr(lane_cfg, "preferred_backend", None)
            pin_backend = str(raw_pin).strip() if raw_pin else None
        candidate_backend = backend or pin_backend or lane_row.get("backend")
        resolved_dispatch_backend, remote_required_error = _resolve_dispatch_backend(
            candidate_backend,
            worktree_path=worktree_for_dispatch,
        )
        if remote_required_error is not None:
            return core._json_response(
                {
                    "ok": False,
                    "outcome": "remote_required",
                    "error": remote_required_error,
                    "backend": resolved_dispatch_backend,
                    "failed_stage": None,
                }
            )

        normalized_test_cmd = test_cmd.strip() if isinstance(test_cmd, str) and test_cmd.strip() else None
        # Align the DB lane backend with the ledger-resolved value (bra2), not
        # the raw caller/lane_row local pin that may have been refused above.
        conn.execute(
            """
            UPDATE worktree_lanes
            SET model = COALESCE(?, model),
                backend = COALESCE(?, backend),
                reasoning_effort = COALESCE(?, reasoning_effort),
                test_cmd = COALESCE(?, test_cmd),
                updated_at = datetime('now')
            WHERE task_ref = ? AND lane_id = ?
            """,
            (
                model,
                resolved_dispatch_backend,
                reasoning_effort,
                normalized_test_cmd,
                resolved_task_ref,
                lane_id,
            ),
        )
        updated_lane = _lanes._get_lane_row(conn, resolved_task_ref, lane_id) or lane_row
        normalized_brief = brief.strip() if isinstance(brief, str) and brief.strip() else None
        # implementation note S12 / T25: optional deterministic codemap context packet.
        context_packet_meta: dict[str, Any] | None = None
        targets_list: list[str] | None = None
        if isinstance(context_targets, list):
            targets_list = [str(t).strip() for t in context_targets if str(t).strip()]
            if not targets_list:
                targets_list = None
        try:
            lcp = _import_orchestration_module("lane_context_packet")
            if lcp.should_include_context_packet(
                include_context_packet=include_context_packet,
                targets=targets_list,
            ):
                worktree_for_packet = str(lane_row.get("worktree_path") or paths["workspace_root"])
                built = lcp.build_lane_context_packet(
                    task_ref=resolved_task_ref,
                    lane_id=lane_id,
                    worktree_path=worktree_for_packet,
                    targets=targets_list,
                )
                context_packet_meta = {
                    "packet_bytes": int(built.get("packet_bytes") or 0),
                    "sections": built.get("sections") or {},
                    "notes": list(built.get("notes") or []),
                    "available": bool(built.get("available")),
                    "truncated": bool(built.get("truncated")),
                }
                normalized_brief = lcp.append_packet_to_brief(normalized_brief, built.get("packet"))
        except Exception as exc:  # noqa: BLE001 — never fail dispatch on packet build
            _logger.warning("lane context packet build failed: %s", exc)
            context_packet_meta = {
                "packet_bytes": 0,
                "sections": {},
                "notes": [f"codemap_unavailable:packet_error:{exc}"],
                "available": False,
                "truncated": False,
            }
        normalized_dispatch_id = dispatch_id.strip() if isinstance(dispatch_id, str) and dispatch_id.strip() else None
        message_row = None
        outcome = "params_only"
        actionable = False
        if normalized_brief is not None:
            payload = {"dispatch_id": normalized_dispatch_id} if normalized_dispatch_id is not None else None
            payload_json = json.dumps(payload, sort_keys=True) if payload is not None else None
            ctx = _lanes._resolve_write_actor(conn, None)
            # implementation note R1 [single-active-brief invariant]: a genuinely new
            # dispatch supersedes prior OPEN brief:dispatch rows on the lane. The
            # worker prompt is assembled from OPEN orchestrator→worker messages
            # (lane_prompt._actionable_state), so leaving earlier briefs open let
            # the worker pick from an N-brief set by its own judgment — that
            # hijacked two passes in the 0108 run (executed a superseded brief).
            # The lane_messages CHECK constraint allows only open/acknowledged/
            # closed (no 'superseded'), so a prior brief is marked 'closed' —
            # it leaves the active/open set the prompt reads yet stays in-table
            # for audit. A duplicate re-dispatch (same dispatch_id already
            # recorded) is a pure no-op replay: skip supersession so it never
            # churns lane state.
            is_duplicate_dispatch = False
            if normalized_dispatch_id is not None:
                is_duplicate_dispatch = (
                    conn.execute(
                        "SELECT 1 FROM lane_messages WHERE task_ref = ? AND lane_id = ? "
                        "AND dispatch_id = ? AND subject = 'brief:dispatch'",
                        (resolved_task_ref, lane_id, normalized_dispatch_id),
                    ).fetchone()
                    is not None
                )
            if not is_duplicate_dispatch:
                conn.execute(
                    "UPDATE lane_messages SET status = 'closed', updated_at = datetime('now') "
                    "WHERE task_ref = ? AND lane_id = ? AND subject = 'brief:dispatch' "
                    "AND status = 'open'",
                    (resolved_task_ref, lane_id),
                )
            try:
                cur = conn.execute(
                    """
                    INSERT INTO lane_messages (
                        task_ref, lane_id, session, direction, subject, message, status,
                        dispatch_id, payload_json, agent, branch, commit_sha, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                    """,
                    (
                        resolved_task_ref,
                        lane_id,
                        "dispatch_lane_work",
                        "orchestrator_to_worker",
                        # HARM-A-007: use the 'brief:' subject convention so lane_prompt
                        # (_brief_messages) treats the dispatched brief as a high-signal
                        # brief, not a generic message.
                        "brief:dispatch",
                        normalized_brief,
                        "open",
                        normalized_dispatch_id,
                        payload_json,
                        ctx.agent,
                        ctx.branch,
                        ctx.commit_sha,
                    ),
                )
                message_row = _lanes._row_to_dict(
                    conn.execute("SELECT * FROM lane_messages WHERE id = ?", (cur.lastrowid,)).fetchone()
                )
                outcome = "dispatched"
            except sqlite3.IntegrityError:
                if normalized_dispatch_id is None:
                    raise
                message_row = _lanes._row_to_dict(
                    conn.execute(
                        """
                        SELECT * FROM lane_messages
                        WHERE task_ref = ? AND lane_id = ? AND dispatch_id = ?
                        """,
                        (resolved_task_ref, lane_id, normalized_dispatch_id),
                    ).fetchone()
                )
                outcome = "duplicate_dispatch"
            if message_row is not None:
                message_row = _lanes._decode_lane_message_row_dict(message_row)
            actionable = message_row is not None and message_row.get("status") == "open"
            _lanes._write_current_task_md_for_task(conn, resolved_task_ref)
        elif normalized_dispatch_id is not None:
            # implementation note R2 [continuation contract]: a no-brief re-dispatch that
            # names an existing OPEN brief:dispatch on the lane is a checkpoint
            # continuation signal — after outcome=checkpoint the pass returns that
            # brief's dispatch_id as continuation_dispatch_id, and the brief stays
            # open. Return continuation_armed (idempotent, no re-enqueue) so the
            # recovery is a documented call, not a bare params_only that reads as
            # "nothing to do" (the 0108 run recovered by calling run_offload_pass
            # directly, undocumented). The operator then runs run_offload_pass.
            existing_open = conn.execute(
                "SELECT * FROM lane_messages WHERE task_ref = ? AND lane_id = ? "
                "AND dispatch_id = ? AND subject = 'brief:dispatch' AND status = 'open'",
                (resolved_task_ref, lane_id, normalized_dispatch_id),
            ).fetchone()
            if existing_open is not None:
                message_row = _lanes._decode_lane_message_row_dict(_lanes._row_to_dict(existing_open))
                outcome = "continuation_armed"
                actionable = True
        # implementation note S8 / T6: warn-only when grok brief requests subagent steps.
        # Compute before worker_start so the named warn attaches on both the
        # success path and the worker_start ok=False early-return (BR-0108-S8-03):
        # a bad brief can already be recorded when start refuses.
        resolved_backend = resolved_dispatch_backend
        brief_warning = _grok_brief_subagent_steps_warning(resolved_backend, normalized_brief)
        warnings: list[str] = []
        if brief_warning:
            warnings.append(brief_warning)
            _logger.warning("%s", brief_warning)
        # implementation note S2: brief-hygiene warnings (full-suite TEST_CMD / full
        # re-baseline). Warn-only; grok-only (degrades to [] otherwise).
        for hygiene_warning in _brief_test_cmd_hygiene_warnings(
            resolved_backend, normalized_test_cmd, normalized_brief
        ):
            warnings.append(hygiene_warning)
            _logger.warning("%s", hygiene_warning)
        worker_start_result: dict | None = None
        if start_worker:
            worker_start_result = worker_start(
                task_ref=resolved_task_ref,
                lane_id=lane_id,
                # bra2/S2R-2: spawn uses the ledger-resolved backend (pin or
                # remote_only default), never a raw local lane_row.backend that
                # slipped past ensure when a remote pin already existed.
                backend=resolved_dispatch_backend,
                model=model or lane_row["model"],
                reasoning_effort=reasoning_effort or lane_row["reasoning_effort"] or "inherit",
                token_budget=token_budget,
                admission_override=admission_override,
            )
            # S1-A-001 / HARM-A-004: a fail-fast worker_start refusal (e.g.
            # no_actionable_work) must not be masked by the dispatch's own ok:True.
            # Surface its ok/outcome so the coordinator sees the worker never started.
            if isinstance(worker_start_result, dict) and worker_start_result.get("ok") is False:
                response: dict[str, Any] = {
                    "ok": False,
                    "outcome": worker_start_result.get("outcome", outcome),
                    "actionable": actionable,
                    "lane": updated_lane,
                    "message": message_row,
                    "worker_start": worker_start_result,
                }
                if warnings:
                    response["warnings"] = warnings
                    response["warning"] = warnings[0].split(":", 1)[0].strip()
                if context_packet_meta is not None:
                    response["context_packet"] = context_packet_meta
                    response["packet_bytes"] = context_packet_meta.get("packet_bytes", 0)
                    response["sections"] = context_packet_meta.get("sections") or {}
                return core._json_response(response)
        response = {
            "ok": True,
            "outcome": outcome,
            "actionable": actionable,
            "lane": updated_lane,
            "message": message_row,
            "worker_start": worker_start_result,
        }
        if warnings:
            response["warnings"] = warnings
            response["warning"] = warnings[0].split(":", 1)[0].strip()
        if context_packet_meta is not None:
            response["context_packet"] = context_packet_meta
            response["packet_bytes"] = context_packet_meta.get("packet_bytes", 0)
            response["sections"] = context_packet_meta.get("sections") or {}
        return core._json_response(response)


def _run_offload_pass_impl(
    lane_id: str,
    task_ref: str | None = None,
    backend: str | None = None,
    model: str | None = None,
    reasoning_effort: str = "high",
    token_budget: int | None = None,
    timeout_seconds: float | None = None,
    max_review_cycles: int = 2,
    turn_timeout_seconds: float | None = None,
    session: str | None = None,
    session_mode: str = "fresh_turn",
    dry_run: bool = False,
    pass_id: str | None = None,
    grok_max_turns: int | None = None,
    admission_override: bool = False,
    *,
    reserved_slot_idx: int | None = None,
) -> dict:
    """Internal offload-pass implementation (implementation note S3).

    ``reserved_slot_idx`` is coordinator-only: when set, the host-admission
    gate still runs but excludes that process-owned heavy slot from the held
    count so a wave that already claimed the slot does not self-refuse. Not
    exposed on the MCP tool schema (see the public :func:`run_offload_pass`
    wrapper). Does **not** clear the crash-breaker (unlike ``admission_override``).
    """
    # bool is an int subclass; token_budget=True must not slip through as budget 1.
    if isinstance(token_budget, bool) or not isinstance(token_budget, int) or token_budget <= 0:
        return core._json_response(
            {"ok": False, "error": "run_offload_pass requires a positive token_budget (mandatory, fail-closed)."}
        )
    if timeout_seconds is None or timeout_seconds <= 0:
        return core._json_response(
            {"ok": False, "error": "run_offload_pass requires a positive timeout_seconds (bounded caller wait)."}
        )
    if isinstance(max_review_cycles, bool) or not isinstance(max_review_cycles, int) or max_review_cycles < 1:
        return core._json_response(
            {"ok": False, "error": "run_offload_pass requires max_review_cycles to be a positive integer (>=1)."}
        )
    if turn_timeout_seconds is not None and turn_timeout_seconds > timeout_seconds:
        return core._json_response(
            {
                "ok": False,
                "error": (
                    "turn_timeout_seconds must not exceed timeout_seconds — the pass-level "
                    "timeout bounds all cycles and is the outer kill switch."
                ),
            }
        )
    # Explicit-backend validation stays ahead of the lane lookup: input-shape
    # refusals (unknown backend, unenforceable turn_timeout_seconds) are
    # pre-spend contracts that must not be masked by lane_not_found. Only
    # backend=None defers validation until the lane worktree's ledger resolves
    # the default (implementation note S2).
    _explicit_backend = backend is not None and str(backend).strip() != ""
    if _explicit_backend:
        try:
            backend_registry = _import_orchestration_module("backend_registry")
            _explicit_name = backend_registry.validate_backend(str(backend).strip())
        except RuntimeError as exc:
            return core._json_response({"ok": False, "error": str(exc)})
        if turn_timeout_seconds is not None and not backend_registry.backend_supports_token_budget_cycle_bounds(
            _explicit_name
        ):
            return core._json_response(
                {
                    "ok": False,
                    "error": (
                        f"turn_timeout_seconds is only enforceable on backends with a per-turn kill "
                        f"switch (grok-family); backend '{_explicit_name}' enforces timeout_seconds "
                        "cooperatively between phases. Omit turn_timeout_seconds for this backend."
                    ),
                }
            )
    # Lane lookup: implementation note S2 reads execution_mode from the lane worktree
    # repo root (consumer ledger lives there) to resolve/police the backend.
    with core._get_db_connection() as conn:
        resolved_task_ref = core._resolve_task_ref(conn, task_ref)
        lane_row = _lanes._get_lane_row(conn, resolved_task_ref, lane_id)
    if lane_row is None:
        return core._json_response({"ok": False, "error": f"Lane '{lane_id}' not found.", "outcome": "lane_not_found"})
    worktree_path = Path(str(lane_row.get("worktree_path") or "")).expanduser().resolve()
    if not worktree_path.exists():
        return core._json_response(
            {"ok": False, "error": f"Lane worktree does not exist for lane '{lane_id}': {worktree_path}"}
        )
    paths = _worker_paths()
    offload_profiles = _import_orchestration_module("offload_profiles")
    offload_pass = _import_orchestration_module("offload_pass")
    resolved_backend, remote_required_error = offload_profiles.resolve_offload_backend_for_execution_mode(
        backend,
        repo_root=worktree_path,
    )
    if remote_required_error is not None:
        # Policy refusal before any spend; durable pass-state record matches other
        # typed outcomes so await_offload_pass / recovery can observe it ([AGT-10]).
        import uuid  # noqa: PLC0415

        resolved_pass_id = (str(pass_id).strip() or None) if pass_id is not None else None
        resolved_pass_id = resolved_pass_id or str(uuid.uuid4())
        result = {
            "outcome": "remote_required",
            "pass_id": resolved_pass_id,
            "task_ref": resolved_task_ref,
            "lane_id": lane_id,
            "backend": resolved_backend,
            "model": model or lane_row.get("model"),
            "reasoning_effort": reasoning_effort,
            "commit_landed": False,
            "failed_stage": None,
            "findings": [],
            "error": remote_required_error,
        }
        offload_pass.write_pass_state(
            paths["state_dir"],
            resolved_pass_id,
            {
                "status": "done",
                "task_ref": resolved_task_ref,
                "lane_id": lane_id,
                "result": result,
            },
        )
        return core._json_response({"ok": True, **result})
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        backend_name = backend_registry.validate_backend(resolved_backend)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    # Fail-closed grok-remote dispatch gate (implementation note H4/M5): refuse the pass
    # engine for grok-remote until its S3 admission + S5 concurrency caps land,
    # unless the operator opts in. Pre-spend refusal (no enum outcome).
    _remote_block = backend_registry.grok_remote_dispatch_block_reason(backend_name)
    if _remote_block is not None:
        return core._json_response({"ok": False, "error": _remote_block})
    # turn_timeout_seconds only has teeth on a backend with a per-turn kill switch
    # (grok-family). For others the pass deadline is checked cooperatively
    # between phases, so accepting turn_timeout_seconds would silently no-op —
    # refuse it rather than pretend to enforce a hard per-turn cap.
    if turn_timeout_seconds is not None and not backend_registry.backend_supports_token_budget_cycle_bounds(
        backend_name
    ):
        return core._json_response(
            {
                "ok": False,
                "error": (
                    f"turn_timeout_seconds is only enforceable on backends with a per-turn kill "
                    f"switch (grok-family); backend '{backend_name}' enforces timeout_seconds "
                    "cooperatively between phases. Omit turn_timeout_seconds for this backend."
                ),
            }
        )
    # internal D2: gate at pass start (a real pass spawns a heavy worker).
    # dry_run never spawns, so it is not gated. Evaluate-only — the worker holds
    # the slot. A refuse/defer returns the typed admission outcome.
    # reserved_slot_idx: exclude only this process's already-claimed slot from
    # the held count (implementation note S3). admission_override is UNCHANGED — it still
    # clears the crash-breaker; reserved_slot_idx is correct accounting, not a bypass.
    if not dry_run:
        # Cost class from the RESOLVED backend profile (internal
        # D1): use the validated backend_name, not the raw arg.
        _cost_class = backend_registry.cost_class_for_backend(backend_name)
        _exclude = frozenset({reserved_slot_idx}) if reserved_slot_idx is not None else frozenset()
        admission = _evaluate_host_admission(
            paths["workspace_root"],
            cost_class=_cost_class,
            exclude_slots=_exclude,
        )
        gate = _admission_gate_error(
            admission,
            override=admission_override,
            task_ref=resolved_task_ref,
            workspace_root=paths["workspace_root"],
            surface="run_offload_pass",
            lane_id=lane_id,
        )
        if gate is not None:
            gate["lane_id"] = lane_id
            gate["outcome"] = "admission_refused" if admission.decision == "refuse" else "admission_deferred"
            return core._json_response(gate)
    try:
        result = offload_pass.run_offload_pass_engine(
            orchestrator_root=paths["workspace_root"],
            task_ref=resolved_task_ref,
            lane_id=lane_id,
            session=session or f"{resolved_task_ref}-{lane_id}",
            worktree_path=worktree_path,
            backend=backend_name,
            model=model or lane_row.get("model"),
            reasoning_effort=reasoning_effort,
            token_budget=token_budget,
            timeout_seconds=timeout_seconds,
            max_review_cycles=max_review_cycles,
            turn_timeout_seconds=turn_timeout_seconds,
            session_mode=session_mode,
            dry_run=dry_run,
            pass_id=(str(pass_id).strip() or None) if pass_id is not None else None,
            state_dir=paths["state_dir"],
            grok_max_turns=grok_max_turns,
            test_cmd=str(lane_row.get("test_cmd") or "").strip() or None,
        )
    except ValueError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    return core._json_response({"ok": True, **result})


def run_offload_pass(
    lane_id: str,
    task_ref: str | None = None,
    backend: str | None = None,
    model: str | None = None,
    reasoning_effort: str = "high",
    token_budget: int | None = None,
    timeout_seconds: float | None = None,
    max_review_cycles: int = 2,
    turn_timeout_seconds: float | None = None,
    session: str | None = None,
    session_mode: str = "fresh_turn",
    dry_run: bool = False,
    pass_id: str | None = None,
    grok_max_turns: int | None = None,
    admission_override: bool = False,
) -> dict:
    """Run one synchronous offload pass over an actionable lane (internal S2).

    Returns a typed outcome enum (`handoff_ready | review_complete | needs_guidance |
    no_actionable_work | uncommitted_work | token_budget_exceeded | timeout |
    error | lane_not_found | self_verify_failed | composer_violation_quarantined |
    checkpoint | server_stale_restart_required | remote_required`) with token
    telemetry, checkpoint refs, and always-present discriminators:
    ``commit_landed: bool``, ``failed_stage`` in
    ``execute|self_verify|review|handoff|attestation|null``, and ``findings``
    (worker-recorded BR-* rows for this pass; empty list when none).

    Grok smoke-review may set ``review: skipped_unparseable`` with optional
    ``raw_tail`` when the non-authoritative review output cannot be parsed —
    that degrades the smoke review only and does not by itself turn a green
    self-verify into ``error`` (implementation note S1 / [OBS-08]). Lane-check failure
    after convergence is still ``needs_guidance`` regardless of review
    discriminator. `composer_violation_quarantined` preserves a self-verified
    checkpoint when grok-build contamination is detected (Composer pin
    attestation retired — implementation note S2). `needs_guidance` means the worker
    submitted a blocked or verification-failed handoff — the submission landed
    but the work is NOT merge-ready. Un-budgeted or un-timeboxed calls are
    refused before any spend. The engine never auto-retries after timeout or
    error; recovery is a new explicit dispatch (idempotent on dispatch_id).

    Backend defaulting (implementation note S2): ``backend=None`` resolves to
    ``grok-remote`` when the lane worktree's bootstrap ledger is
    ``execution_mode=remote_only``, else ``grok-cli``. An *explicit* local
    backend under ``remote_only`` returns typed ``remote_required`` (never a
    silent remote substitution).

    `timeout_seconds` is the outer bound on the caller's wait; for backends
    without a per-turn kill switch it is enforced cooperatively (checked between
    phases), so `turn_timeout_seconds` is accepted only for backends that can
    hard-enforce it (grok-family backends) and refused otherwise.

    Pass a caller-supplied ``pass_id`` to make disconnect recovery usable: if the
    client times out or disconnects mid-pass, it can reconnect with
    ``await_offload_pass(pass_id)`` to recover the persisted outcome. When omitted,
    the engine generates one and returns it on the (blocking) result.

    Internal ``reserved_slot_idx`` is deliberately absent from this public
    signature so FastMCP tool-schema introspection never surfaces it (implementation note
    S3 row 17).
    """
    return _run_offload_pass_impl(
        lane_id=lane_id,
        task_ref=task_ref,
        backend=backend,
        model=model,
        reasoning_effort=reasoning_effort,
        token_budget=token_budget,
        timeout_seconds=timeout_seconds,
        max_review_cycles=max_review_cycles,
        turn_timeout_seconds=turn_timeout_seconds,
        session=session,
        session_mode=session_mode,
        dry_run=dry_run,
        pass_id=pass_id,
        grok_max_turns=grok_max_turns,
        admission_override=admission_override,
        reserved_slot_idx=None,
    )


def await_offload_pass(
    pass_id: str,
    wait_seconds: float = 30,
    task_ref: str | None = None,
) -> dict:
    """Bounded continuation for a pass that outlived one client call window.

    Long-polls the persisted pass state for up to ``wait_seconds`` and returns
    the pass outcome (same typed enum as ``run_offload_pass``, including the
    always-present ``commit_landed`` / ``failed_stage`` / ``findings``
    discriminators and optional ``review`` / ``raw_tail``), or ``still_running``
    with a progress snapshot. This is a coarse bounded wait, not a poll loop —
    one call per wait window.
    """
    normalized_pass_id = str(pass_id or "").strip()
    if not normalized_pass_id:
        return core._json_response({"ok": False, "error": "pass_id is required."})
    wait_seconds = max(0.0, float(wait_seconds))
    paths = _worker_paths()
    offload_pass = _import_orchestration_module("offload_pass")
    deadline = time.monotonic() + wait_seconds
    while True:
        state = offload_pass.read_pass_state(paths["state_dir"], normalized_pass_id)
        if state is None:
            return core._json_response(
                {"ok": False, "error": f"Unknown offload pass '{normalized_pass_id}' (no persisted pass state)."}
            )
        # Pass state files are global across tasks in a workspace; if the caller
        # scoped the lookup with task_ref, refuse a cross-task pass_id rather than
        # silently returning another task's outcome.
        if task_ref and str(state.get("task_ref") or "") != str(task_ref):
            return core._json_response(
                {
                    "ok": False,
                    "error": (
                        f"offload pass '{normalized_pass_id}' belongs to task "
                        f"'{state.get('task_ref')}', not '{task_ref}'."
                    ),
                }
            )
        if state.get("status") == "done" and isinstance(state.get("result"), dict):
            return core._json_response({"ok": True, **state["result"]})
        if time.monotonic() >= deadline:
            return core._json_response(
                {
                    "ok": True,
                    "outcome": "still_running",
                    "pass_id": normalized_pass_id,
                    "progress": {k: v for k, v in state.items() if k != "result"},
                }
            )
        time.sleep(min(1.0, max(0.05, deadline - time.monotonic())))


# Outcomes that count as terminal *failure* for first_failure multipass join.
# still_running is non-terminal; handoff_ready / review_complete / no_actionable_work /
# checkpoint are terminal success (do not trip first_failure).
_MULTIPASS_TERMINAL_FAILURE_OUTCOMES = frozenset(
    {
        "timeout",
        "error",
        "self_verify_failed",
        "token_budget_exceeded",
        "uncommitted_work",
        "needs_guidance",
        "lane_not_found",
        "composer_violation_quarantined",
        "admission_refused",
        "remote_required",
        "server_stale_restart_required",
        "admission_deferred",
        "empty_result",
        "unknown_pass",
        "task_mismatch",
    }
)


def _multipass_snapshot_one(
    offload_pass: Any,
    state_dir: Path,
    pass_id: str,
    task_ref: str | None,
) -> dict[str, Any]:
    """Resolve one pass id to a join entry without waiting.

    Empty done-state results (status=done with missing/non-dict/empty result or
    missing outcome) are reported as ``empty_result`` failures — the remote
    wrapper can print a failure line with an empty reason and still exit 0; an
    empty result file is the reliable signal.
    """
    normalized = str(pass_id or "").strip()
    if not normalized:
        return {
            "pass_id": "",
            "outcome": "error",
            "error": "pass_id is required.",
            "commit_landed": False,
            "failed_stage": None,
            "findings": [],
            "terminal": True,
            "failed": True,
        }
    state = offload_pass.read_pass_state(state_dir, normalized)
    if state is None:
        return {
            "pass_id": normalized,
            "outcome": "unknown_pass",
            "error": f"Unknown offload pass '{normalized}' (no persisted pass state).",
            "commit_landed": False,
            "failed_stage": None,
            "findings": [],
            "terminal": True,
            "failed": True,
        }
    if task_ref and str(state.get("task_ref") or "") != str(task_ref):
        return {
            "pass_id": normalized,
            "outcome": "task_mismatch",
            "error": (
                f"offload pass '{normalized}' belongs to task "
                f"'{state.get('task_ref')}', not '{task_ref}'."
            ),
            "commit_landed": False,
            "failed_stage": None,
            "findings": [],
            "terminal": True,
            "failed": True,
        }
    if state.get("status") == "done":
        result = state.get("result")
        # Empty / non-dict / outcome-less result: wrapper-lie failure, not success and not absent.
        if not isinstance(result, dict) or not result or not str(result.get("outcome") or "").strip():
            return {
                "pass_id": normalized,
                "outcome": "empty_result",
                "error": "pass status is done but result is empty or missing outcome",
                "commit_landed": False,
                "failed_stage": None,
                "findings": [],
                "empty_result": True,
                "terminal": True,
                "failed": True,
            }
        entry = dict(result)
        entry.setdefault("pass_id", normalized)
        outcome = str(entry.get("outcome") or "")
        entry["terminal"] = outcome != "still_running"
        entry["failed"] = outcome in _MULTIPASS_TERMINAL_FAILURE_OUTCOMES
        return entry
    return {
        "pass_id": normalized,
        "outcome": "still_running",
        "progress": {k: v for k, v in state.items() if k != "result"},
        "terminal": False,
        "failed": False,
    }


def await_offload_passes(
    pass_ids: list[str] | tuple[str, ...] | str,
    wait_seconds: float = 30,
    mode: str = "all_complete",
    task_ref: str | None = None,
) -> dict:
    """Join N offload pass ids into one bounded wait with per-pass outcomes.

    Parameters
    ----------
    pass_ids:
        One or more pass identifiers the caller already holds (join only — no
        batch dispatch). Order is preserved in the returned ``passes`` list.
    wait_seconds:
        Outer bound on the join wait. When the deadline elapses before the mode
        condition is met, ``wait_exhausted`` is True and unfinished passes are
        reported as ``still_running`` (never silently dropped).
    mode:
        ``all_complete`` — return when every pass is terminal (or wait exhausts).
        ``first_failure`` — return as soon as any pass terminally fails; siblings
        that are still running are reported as such.
    task_ref:
        Optional scope check; a cross-task pass_id is a terminal failure for that
        entry only and does not sink the rest of the join.

    Returns a dict with ``ok``, ``mode``, ``wait_exhausted``, ``join_status``,
    and ``passes``: a list of per-pass outcome dicts (one entry per requested id).
    Partial failure never prevents sibling outcomes from being reported.
    """
    if isinstance(pass_ids, str):
        requested = [pass_ids]
    elif pass_ids is None:
        requested = []
    else:
        requested = list(pass_ids)
    if not requested:
        return core._json_response({"ok": False, "error": "pass_ids is required (one or more pass identifiers)."})
    normalized_mode = str(mode or "all_complete").strip().lower().replace("-", "_")
    if normalized_mode not in ("all_complete", "first_failure"):
        return core._json_response(
            {
                "ok": False,
                "error": "mode must be 'all_complete' or 'first_failure'.",
            }
        )
    wait_seconds = max(0.0, float(wait_seconds))
    paths = _worker_paths()
    offload_pass = _import_orchestration_module("offload_pass")
    state_dir = paths["state_dir"]
    deadline = time.monotonic() + wait_seconds

    def _snapshot() -> list[dict[str, Any]]:
        return [_multipass_snapshot_one(offload_pass, state_dir, pid, task_ref) for pid in requested]

    while True:
        entries = _snapshot()
        any_failed = any(bool(e.get("failed")) for e in entries)
        all_terminal = all(bool(e.get("terminal")) for e in entries)
        if normalized_mode == "first_failure" and any_failed:
            return core._json_response(
                {
                    "ok": True,
                    "mode": normalized_mode,
                    "join_status": "first_failure",
                    "wait_exhausted": False,
                    "pass_count": len(entries),
                    "passes": entries,
                }
            )
        if all_terminal:
            return core._json_response(
                {
                    "ok": True,
                    "mode": normalized_mode,
                    "join_status": "all_complete",
                    "wait_exhausted": False,
                    "pass_count": len(entries),
                    "passes": entries,
                }
            )
        if time.monotonic() >= deadline:
            return core._json_response(
                {
                    "ok": True,
                    "mode": normalized_mode,
                    "join_status": "wait_exhausted",
                    "wait_exhausted": True,
                    "pass_count": len(entries),
                    "passes": entries,
                }
            )
        # Sub-second poll so first_failure can return promptly; single-pass await
        # keeps its coarser 1s cadence unchanged (P4).
        time.sleep(min(0.1, max(0.05, deadline - time.monotonic())))


def dispatch_wave(
    lane_specs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    task_ref: str,
    wave_max_width: int | None = None,
    wait_seconds: float = 0.0,
) -> dict:
    """Coordinator-side batch wave over COST_REMOTE/grok-remote lanes (implementation note S3).

    Blocking-join model: generate each lane's ``pass_id`` up front, dispatch via
    :func:`_run_offload_pass_impl` (with ``reserved_slot_idx`` for gated classes),
    then optionally join with :func:`await_offload_passes`.

    Returns ``{wave_id, dispatched[], deferred[], refused[], wave_max_width}``.
    Non-remote members (e.g. ``{L}__verify__claude`` / COST_HEAVY) are refused —
    the daemon owns them. ``wave_max_width==0`` fails closed into ``deferred[]``
    without constructing ``Semaphore(0)``.
    """
    from workbay_orchestrator_mcp.orchestration.wave_dispatch import (  # noqa: PLC0415
        coordinate_wave,
    )

    if not task_ref or not str(task_ref).strip():
        return core._json_response({"ok": False, "error": "dispatch_wave requires task_ref."})
    if lane_specs is None:
        return core._json_response({"ok": False, "error": "dispatch_wave requires lane_specs."})
    paths = _worker_paths()
    workspace_root = Path(paths["workspace_root"])
    try:
        payload = coordinate_wave(
            list(lane_specs),
            task_ref=str(task_ref).strip(),
            workspace_root=workspace_root,
            run_pass=_run_offload_pass_impl,
            await_passes=await_offload_passes if wait_seconds and float(wait_seconds) > 0 else None,
            wave_max_width=wave_max_width,
            wait_seconds=float(wait_seconds or 0.0),
        )
    except Exception as exc:  # noqa: BLE001 — surface as structured error
        return core._json_response({"ok": False, "error": f"dispatch_wave failed: {exc}"})

    # Best-effort system decision row (180-day retention is the handoff default).
    try:
        import workbay_handoff_mcp as handoff  # noqa: PLC0415

        handoff.record_event(
            event={  # type: ignore[arg-type]
                "event_kind": "decision",
                "session": f"wave-dispatch-{task_ref}",
                "decision": f"dispatch_wave:{payload.get('wave_id')}",
                "rationale": (
                    f"wave_max_width={payload.get('wave_max_width')} "
                    f"dispatched={len(payload.get('dispatched') or [])} "
                    f"deferred={len(payload.get('deferred') or [])} "
                    f"refused={len(payload.get('refused') or [])}"
                ),
                "task_ref": str(task_ref).strip(),
            }
        )
    except Exception:  # noqa: BLE001, S110 — telemetry best-effort
        pass

    return core._json_response(payload)


def offload_preflight(
    *,
    worktree_path: str | Path,
    agent: str,
    token_budget: int | None,
    model: str | None = None,
    reasoning_effort: str | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    strict: bool = False,
    admission_override: bool = False,
) -> dict:
    """Fail-Fast cross-harness offload pre-checks before any dispatch spend."""
    paths = _worker_paths()
    offload_preflight = _import_orchestration_module("offload_preflight")
    backend_registry = _import_orchestration_module("backend_registry")
    try:
        payload = offload_preflight.offload_preflight(
            orchestrator_root=paths["workspace_root"],
            worktree_path=Path(worktree_path),
            agent=agent,
            model=model,
            reasoning_effort=reasoning_effort,
            token_budget=token_budget,
            probe_availability=backend_registry.probe_availability,
            task_ref=task_ref,
            lane_id=lane_id,
            strict=strict,
        )
    except offload_preflight.OffloadPreflightError as exc:
        # implementation note residual R0152-1: probe-failure (and other post-echo raises)
        # may already carry execution_mode + remote_probe_state — surface them on
        # ok:false without a second probe. Do not change the error string.
        failure: dict[str, object] = {"ok": False, "error": str(exc)}
        execution_mode = getattr(exc, "execution_mode", None)
        remote_probe_state = getattr(exc, "remote_probe_state", None)
        if execution_mode is not None:
            failure["execution_mode"] = execution_mode
        if remote_probe_state is not None:
            failure["remote_probe_state"] = remote_probe_state
        return core._json_response(failure)
    # implementation note S1 residual: remote_required already carries execution_mode +
    # remote_probe_state; return before host-admission so policy refusal is not
    # overwritten by admission pressure.
    if isinstance(payload, dict) and payload.get("outcome") == "remote_required":
        if task_ref:
            payload["task_ref"] = task_ref
        return core._json_response(payload)
    if task_ref:
        payload["task_ref"] = task_ref
    # internal D2/PR-010: host-memory admission facet (evaluate-only — the
    # worker holds the slot, preflight only reports + hard-gates). Additive
    # `admission` key; a refuse/defer becomes the structured recoverable error
    # unless the operator passes admission_override. Cost class from the resolved
    # backend profile (internal D1): grok-cli is remote-API.
    admission = _evaluate_host_admission(
        paths["workspace_root"], cost_class=backend_registry.cost_class_for_backend(agent)
    )
    payload["admission"] = admission.to_dict()
    # internal D2b: echo the effective host_memory policy
    # (values + relative source path + warnings) so an operator can confirm a
    # contract edit took effect — and, critically, SEE a misplaced top-level
    # `host_memory:` warning on the same surface that returned admission_refused.
    # Included on BOTH the refuse/defer gate error and the success payload.
    from workbay_orchestrator_mcp.orchestration.host_resources import (
        host_memory_policy_echo,
    )

    host_memory_policy = host_memory_policy_echo(paths["workspace_root"])
    payload["host_memory_policy"] = host_memory_policy
    gate = _admission_gate_error(
        admission,
        override=admission_override,
        task_ref=task_ref,
        workspace_root=paths["workspace_root"],
        surface="offload_preflight",
        lane_id=lane_id,
    )
    if gate is not None:
        gate["host_memory_policy"] = host_memory_policy
        return core._json_response(gate)
    return core._json_response(payload)


def materialize_offload_lane_manifest(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: str,
    branch: str,
    preferred_backend: str | None = None,
    preferred_model: str | None = "",
    preferred_reasoning_effort: str | None = "",
) -> dict:
    """Patch lane manifest reviewer backend/model/effort pin for offload lanes.

    ``preferred_model``/``preferred_reasoning_effort`` use ``""`` as an "omitted"
    sentinel so the grok defaults fill in for a bare grok call, while an explicit
    ``None`` (e.g. codex-subagent with no model) is forwarded and leaves the key
    unpinned.

    Backend defaulting (implementation note S2): ``preferred_backend=None`` resolves to
    ``grok-remote`` when the worktree bootstrap ledger is
    ``execution_mode=remote_only``, else ``grok-cli``. An *explicit* local pin
    under ``remote_only`` is a validation error naming ``remote_required``
    (never a silent substitution).
    """
    paths = _worker_paths()
    offload_preflight = _import_orchestration_module("offload_preflight")
    offload_profiles = _import_orchestration_module("offload_profiles")
    lane_manifest = _import_orchestration_module("lane_manifest")
    repo_root = Path(worktree_path).expanduser().resolve()
    resolved_backend, remote_required_error = offload_profiles.resolve_offload_backend_for_execution_mode(
        preferred_backend,
        repo_root=repo_root,
    )
    if remote_required_error is not None:
        # Standard validation error path (ok:false), naming remote_required semantics.
        return core._json_response(
            {
                "ok": False,
                "error": remote_required_error,
                "outcome": "remote_required",
            }
        )
    call_kwargs: dict[str, Any] = {
        "orchestrator_root": paths["workspace_root"],
        "task_ref": task_ref,
        "lane_id": lane_id,
        "worktree_path": worktree_path,
        "branch": branch,
        "preferred_backend": resolved_backend,
    }
    if preferred_model != "":
        call_kwargs["preferred_model"] = preferred_model
    if preferred_reasoning_effort != "":
        call_kwargs["preferred_reasoning_effort"] = preferred_reasoning_effort
    try:
        manifest_path = offload_preflight.materialize_offload_lane_manifest(**call_kwargs)
    except offload_preflight.OffloadPreflightError as exc:
        return core._json_response({"ok": False, "error": str(exc)})
    cfg = lane_manifest.get_lane_config(task_ref, lane_id, orchestrator_root=str(paths["workspace_root"])) or {}
    return core._json_response(
        {
            "ok": True,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "manifest_path": str(manifest_path),
            "preferred_backend": cfg.get("preferred_backend"),
            "preferred_model": cfg.get("preferred_model"),
            "preferred_reasoning_effort": cfg.get("preferred_reasoning_effort"),
        }
    )


def list_available_backends(probe: bool = True) -> dict:
    """List supported execution backends and their capabilities.

    By default this includes probed availability so MCP callers can distinguish
    "declared" from "actually reachable" without first attempting a failing
    dispatch. This is intentionally safer for skill routing than the old static
    declaration-only default.

    Pass ``probe=False`` to copy the static declaration table only (cheap: no
    subprocess calls and no optional bridge imports). When probing is enabled,
    each entry gains:

    * ``is_available`` — probed reachability (CLI binary on PATH, bridge module
      importable, or in-process). This is reachability, NOT a liveness guarantee:
      a ``reachable`` bridge can still time out at dispatch.
    * ``availability_state`` — one of ``available`` / ``reachable`` /
      ``declared_not_installed`` / ``unavailable`` / ``unknown``. The
      ``declared_not_installed`` state is what flags an optional bridge (e.g.
      ``codex-subagent``) that is declared but whose host module is not importable
      in this runtime.
    * ``availability_detail`` — human-readable explanation.

    Probing MAY shell out to ``codex``/``claude`` and import optional bridge
    modules, so callers that need a declaration-only read should pass
    ``probe=False`` explicitly.
    """
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        # bra3: when the MCP server has a configured workspace, thread it so
        # grok-remote's .workbay/remote-gate.env fallback resolves against the
        # consumer repo root rather than Path.cwd() of the server process.
        # Unconfigured runtimes (and unit tests that never call
        # configure_runtime) keep the pre-bra3 probe call shape so the public
        # return contract stays intact.
        workspace_root = None
        try:
            raw_root = get_runtime_config().workspace_root
            if raw_root is not None:
                workspace_root = Path(raw_root).expanduser().resolve()
        except Exception:
            workspace_root = None
        backends = {}
        for name, spec in backend_registry.BACKENDS.items():
            entry = {
                "kind": spec.kind,
                "description": spec.description,
                "supports_reasoning_effort": spec.capabilities.supports_reasoning_effort,
                "supports_sync_turn": spec.capabilities.supports_sync_turn,
            }
            if probe:
                if workspace_root is not None:
                    probed = backend_registry.probe_availability(name, workspace_root=workspace_root)
                else:
                    probed = backend_registry.probe_availability(name)
                caps = probed["capabilities"]
                entry["is_available"] = probed["is_available"]
                entry["availability_state"] = probed["state"]
                entry["availability_detail"] = probed["detail"]
                # Prefer probed capability flags when probing — e.g. codex-cli
                # reasoning-effort support is only known after inspecting --help.
                entry["supports_reasoning_effort"] = caps.supports_reasoning_effort
                entry["supports_sync_turn"] = caps.supports_sync_turn
                if "downstream" in probed:
                    # internal: in-process adapters annotate their downstream
                    # prerequisite; forward it untouched for probe-first routers.
                    entry["downstream"] = probed["downstream"]
            backends[name] = entry
        return core._json_response({"ok": True, "backends": backends, "probed": probe})
    except Exception as exc:
        return core._json_response({"ok": False, "error": str(exc)})


def get_metrics_summary(
    task_ref: str | None = None,
    output_format: str = "markdown",
) -> dict[str, Any] | str:
    """Return an ACE metrics snapshot for the active task."""
    try:
        from workbay_orchestrator_mcp.orchestration.ace_metrics import (  # noqa: PLC0415
            build_snapshot,
            render_markdown,
        )
    except ImportError as exc:
        return core._json_response({"ok": False, "error": f"ace_metrics module unavailable: {exc}"})

    paths = _orchestrator_paths()
    workspace_root = paths["workspace_root"]
    state_dir = paths["state_dir"]
    logs_dir = workspace_root / "logs"

    resolved_task_ref = task_ref
    if not resolved_task_ref:
        try:
            with core._get_db_connection() as conn:
                resolved_task_ref = core._resolve_task_ref(conn, None)
        except Exception:
            resolved_task_ref = "unknown"

    instruction_files = [workspace_root / INSTRUCTIONS_RELPATH]

    try:
        snapshot = build_snapshot(
            task_ref=resolved_task_ref,
            state_dir=state_dir,
            logs_dir=logs_dir,
            instruction_files=instruction_files,
        )
        if output_format == "json":
            return core._json_response({"ok": True, "snapshot": snapshot})
        return render_markdown(snapshot)
    except Exception as exc:
        return core._json_response({"ok": False, "error": str(exc)})


def build_orchestrator_mcp(config: RuntimeConfig) -> FastMCP:
    configure_runtime(config)
    return _build_mcp_from_registry(_build_tool_registry())


def _build_mcp_from_registry(entries: list[ToolEntry]) -> FastMCP:
    mcp = FastMCP(
        f"{BRAND_NAME} Orchestrator MCP",
        instructions=(
            f"You are connected to the {BRAND_NAME} Orchestrator MCP server. "
            "Use these tools for daemon lifecycle, lane management, worker control, "
            "turn metrics, plan cursors, and backend dispatch."
        ),
    )
    _apply_tool_descriptions()
    for entry in entries:
        base_doc = entry.description
        if entry.deprecated_since is not None:
            entry.handler.__doc__ = f"[DEPRECATED since {entry.deprecated_since}] {base_doc}"
        else:
            entry.handler.__doc__ = base_doc
        mcp.add_tool(entry.handler)
    return mcp


def run_doctor(config: RuntimeConfig) -> dict[str, Any]:
    configure_runtime(config)
    mcp = build_orchestrator_mcp(config)
    if hasattr(mcp, "_tool_manager") and hasattr(mcp._tool_manager, "_tools"):
        tool_names = sorted(mcp._tool_manager._tools.keys())
    else:
        tool_names = sorted(t.name for t in asyncio.run(mcp.list_tools()))
    return {
        "ok": True,
        "server": "mcp-workbay-orchestrator",
        "tool_count": len(tool_names),
        "tools": tool_names,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(mode="json", exclude_none=True))
    return str(value)


def _serialize_tool_snapshot(tool: Any, *, deprecated_since: str | None = None) -> dict[str, Any]:
    raw_tool = tool.to_mcp_tool() if hasattr(tool, "to_mcp_tool") else tool
    if hasattr(raw_tool, "model_dump"):
        snapshot = _json_safe(raw_tool.model_dump(mode="json", exclude_none=True))
    else:
        snapshot = {
            "name": getattr(tool, "name"),
            "description": getattr(tool, "description", None),
            "inputSchema": _json_safe(getattr(tool, "parameters", None)),
        }
    if deprecated_since is not None:
        snapshot["deprecated_since"] = deprecated_since
    return snapshot


def run_tools_snapshot(
    config: RuntimeConfig,
    *,
    phase: str = "current",
    output_path: Path | None = None,
) -> dict[str, Any]:
    configure_runtime(config)
    registry = _snapshot_registry(phase)
    mcp = _build_mcp_from_registry(registry)
    tools = asyncio.run(mcp.list_tools())
    deprecated_map = {entry.name: entry.deprecated_since for entry in registry if entry.deprecated_since is not None}
    tool_snapshots = [
        _serialize_tool_snapshot(tool, deprecated_since=deprecated_map.get(tool.name))
        for tool in sorted(tools, key=lambda item: item.name)
    ]
    tools_list_payload = {"tools": tool_snapshots}
    tools_list_json = json.dumps(tools_list_payload, sort_keys=True, separators=(",", ":"))
    estimated_tokens, estimation_method = _estimate_token_count(tools_list_json)
    snapshot = {
        "ok": True,
        "server": "mcp-workbay-orchestrator",
        "phase": phase,
        "tool_count": len(tool_snapshots),
        "tools": tool_snapshots,
        "tool_names": [tool["name"] for tool in tool_snapshots],
        "tools_list_bytes": len(tools_list_json),
        "estimated_tools_list_tokens": estimated_tokens,
        "token_estimation_method": estimation_method,
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(snapshot, sort_keys=True, indent=2) + "\n")
        snapshot["output_path"] = str(output_path)
    return snapshot
