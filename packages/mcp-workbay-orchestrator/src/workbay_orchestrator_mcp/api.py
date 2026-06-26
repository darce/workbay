"""Orchestrator MCP API — lane management, worker daemons, turn metrics, and dispatch."""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

from fastmcp import FastMCP
from workbay_protocol import BRAND_NAME, HARNESS_CONTRACT_RELPATH, INSTRUCTIONS_RELPATH

from workbay_orchestrator_mcp import lanes as _lanes

if TYPE_CHECKING:
    from workbay_handoff_mcp.config import RuntimeConfig


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
    "get_latest_slice_review_packet": "Resolve the latest completed slice review packet for a task, optionally filtered by lane or review kind.",
    "reconcile_review_findings": "Compare open findings against current files and return a reconciliation summary for review workflows.",
    "get_review_findings_summary": "Return aggregate counts of review findings by status and severity for the active or requested task.",
    "manage_orchestrator": "Compound tool: start, query, pause, resume, stop, or run a single orchestrator cycle in one call. Use operation='start'|'status'|'pause'|'resume'|'stop'|'single_cycle'.",
    "manage_worker": "Compound tool: start, stop, resume, query status, inspect event history, or start all worker daemons in one call. Use action='start'|'stop'|'resume'|'status'|'event_history'|'start_all'.",
    "run_structured_turn": "Execute one synchronous structured bridge turn through a registered non-CLI backend.",
    "dispatch_lane_work": "Update lane dispatch parameters (model, backend, effort) for the next execution cycle.",
    "list_available_backends": "List supported execution backends and their capabilities. By default includes probed is_available plus availability_state/detail per backend so skills can route safely. Pass probe=false for the cheap static declaration-only view.",
    "get_metrics_summary": "Return an ACE metrics snapshot for the active task covering token burn, context pressure, FTS5 retrieval, lane health, phase timing, and documentation fitness.",
}

_CONTRACT_RELATIVE_PATH = HARNESS_CONTRACT_RELPATH
_DAEMONS_DISABLED_MESSAGE = (
    "Daemons are opt-in. Enable via `orchestrator.daemons.enabled: true` in your "
    "`local/harness-protocol.yaml`. See `docs/workbay/consumer-setup.md § Daemons` "
    "for token-cost implications."
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


def worker_start(
    task_ref: str,
    lane_id: str,
    backend: str = "codex-subagent",
    poll_interval: int = 30,
    single_pass: bool = False,
    session: str | None = None,
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
) -> dict:
    paths = _worker_paths()
    try:
        backend_registry = _import_orchestration_module("backend_registry")
        backend_name = backend_registry.validate_backend(backend)
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
) -> dict:
    try:
        lane_manifest = _import_orchestration_module("lane_manifest")
        orchestrator_lanes = _import_orchestration_module("orchestrator_lanes")
        merge_order_fn = getattr(lane_manifest, "merge_order", None)
        manifest_order = merge_order_fn(task_ref) if callable(merge_order_fn) else []
        lane_ids = manifest_order or lane_manifest.list_lanes(task_ref)
    except RuntimeError as exc:
        return core._json_response({"ok": False, "error": str(exc)})

    results: list[dict[str, Any]] = []
    for lane_id in lane_ids:
        blocked_by: list[str] = []
        if lane_id in manifest_order:
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
        if blocked_by:
            results.append(
                {
                    "ok": True,
                    "lane_id": lane_id,
                    "started": False,
                    "skipped": True,
                    "reason": "unresolved_upstream_dependencies",
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
                )
            )
        except Exception as exc:
            result = {
                "ok": False,
                "lane_id": lane_id,
                "error": f"worker_start raised {type(exc).__name__}: {exc}",
            }
        results.append(result)
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
    force: bool = False,
    limit: int = 50,
    event_name: str | None = None,
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


def dispatch_lane_work(
    lane_id: str,
    model: str | None = None,
    backend: str | None = None,
    reasoning_effort: str | None = None,
    task_ref: str | None = None,
    start_worker: bool = False,
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
            return core._json_response({"ok": False, "error": f"Lane '{lane_id}' not found."})

        result = manage_worktree_lane(
            operation="upsert",
            lane_id=lane_id,
            worktree_path=lane_row["worktree_path"],
            branch=lane_row["branch"],
            title=lane_row["title"],
            objective=lane_row["objective"],
            owner_agent=lane_row["owner_agent"],
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            status=lane_row["status"],
            notes=lane_row["notes"],
            task_ref=resolved_task_ref,
        )
        if start_worker:
            worker_start(
                task_ref=resolved_task_ref,
                lane_id=lane_id,
                backend=backend or lane_row["backend"] or "codex-subagent",
                model=model or lane_row["model"],
                reasoning_effort=reasoning_effort or lane_row["reasoning_effort"] or "inherit",
            )
        return result


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
        backends = {}
        for name, spec in backend_registry.BACKENDS.items():
            entry = {
                "kind": spec.kind,
                "description": spec.description,
                "supports_reasoning_effort": spec.capabilities.supports_reasoning_effort,
                "supports_sync_turn": spec.capabilities.supports_sync_turn,
            }
            if probe:
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


def _estimate_token_count(payload_json: str) -> tuple[int, str]:
    try:
        import tiktoken  # type: ignore

        encoding = tiktoken.get_encoding("o200k_base")
        return len(encoding.encode(payload_json)), "tiktoken:o200k_base"
    except Exception:
        return max(1, round(len(payload_json) / 4)), "chars_div_4"


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
