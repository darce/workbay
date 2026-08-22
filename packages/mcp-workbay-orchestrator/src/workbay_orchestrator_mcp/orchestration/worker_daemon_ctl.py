#!/usr/bin/env python3
"""Control helpers for lane-scoped worker daemons."""

from __future__ import annotations

import argparse
import datetime
import fcntl
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.db_writer_liveness import WRITER_HEARTBEAT_STALE_SECONDS


def _lock_path(state_dir: Path, lane_id: str) -> Path:
    return state_dir / f"worker-{lane_id}.lock"


def _log_path(log_dir: Path, lane_id: str) -> Path:
    return log_dir / f"worker-{lane_id}.jsonl"


def _status_path(state_dir: Path, lane_id: str) -> Path:
    return state_dir / f"worker-{lane_id}.status.json"


def _read_lock_info(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"held": False, "expired": False, "path": str(path)}
    if not path.exists():
        return info
    raw = path.read_text(errors="replace").strip()
    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                info.update(payload)
            else:
                info["raw"] = raw
        except json.JSONDecodeError:
            info["raw"] = raw
    # File existence is not liveness. ``held`` is filled in by ``probe_worker_lock``.
    info["held"] = False
    info["expired"] = _lease_expired(info)
    return info


def _lease_expired(lock: dict[str, Any], *, now: float | None = None) -> bool:
    """True when heartbeat is older than TTL. Follows db_writer_liveness dialect."""
    heartbeat = lock.get("heartbeat_ts")
    if not isinstance(heartbeat, (int, float)):
        return False
    ttl = lock.get("lease_ttl_seconds")
    if not isinstance(ttl, (int, float)):
        ttl = WRITER_HEARTBEAT_STALE_SECONDS
    ts = time.time() if now is None else now
    return (ts - float(heartbeat)) > float(ttl)


def probe_worker_lock(path: Path) -> dict[str, Any]:
    """flock is the sole liveness oracle.

    Acquire ``LOCK_EX | LOCK_NB``:
    - success -> nobody holds it -> orphan. Release immediately, report not-held.
    - ``BlockingIOError`` / ``OSError`` -> a live process holds it -> held.

    Never consult ``ps``, ``os.kill``, or file existence to decide held/not-held.
    """
    result: dict[str, Any] = {"held": False, "orphan": False, "path": str(path)}
    try:
        fh = path.open("r+")
    except FileNotFoundError:
        return result
    except OSError:
        return result
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            result["held"] = True
            return result
        result["orphan"] = True
        result["held"] = False
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        return result
    finally:
        fh.close()


def _reclaim_orphan_lock(path: Path) -> bool:
    """Unlink ``path`` only while holding its flock. Returns True if gone or reclaimed."""
    try:
        fh = path.open("r+")
    except FileNotFoundError:
        return True
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return True
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fh.close()


def _ps_info(pid: int) -> dict[str, Any] | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "pid=,ppid=,stat=,etime=,command="],
        capture_output=True,
        text=True,
        check=False,
    )
    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return None
    parts = line.split(None, 4)
    if len(parts) < 4:
        return {"pid": pid, "raw": line}
    info: dict[str, Any] = {
        "pid": int(parts[0]),
        "ppid": int(parts[1]),
        "stat": parts[2],
        "etime": parts[3],
        "command": parts[4] if len(parts) > 4 else "",
    }
    info["stopped"] = "T" in info["stat"]
    return info


def _find_worker_process(*, task_ref: str | None, lane_id: str) -> dict[str, Any] | None:
    pattern = f"worker_daemon.py.*--lane-id {lane_id}"
    if task_ref:
        pattern = f"worker_daemon.py.*--task-ref {task_ref}.*--lane-id {lane_id}"
    result = subprocess.run(
        ["pgrep", "-af", pattern],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    candidates: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if not parts or not parts[0].isdigit():
            continue
        pid = int(parts[0])
        command = parts[1] if len(parts) == 2 else ""
        candidates.append({"pid": pid, "command": command})

    if not candidates:
        return None

    candidates.sort(key=lambda item: 1 if item["command"].startswith("/bin/sh -c") else 0)
    chosen = candidates[0]
    info = _ps_info(int(chosen["pid"]))
    if info is not None:
        info["pid_source"] = "process_scan"
    return info


def _child_pids(pid: int) -> list[int]:
    result = subprocess.run(
        ["pgrep", "-P", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [int(line.strip()) for line in result.stdout.splitlines() if line.strip().isdigit()]


def _process_tree(pid: int) -> list[int]:
    tree: list[int] = []
    for child in _child_pids(pid):
        tree.extend(_process_tree(child))
        tree.append(child)
    tree.append(pid)
    return tree


def _signal_tree(pid: int, sig: signal.Signals) -> list[int]:
    signaled: list[int] = []
    for target in _process_tree(pid):
        try:
            os.kill(target, sig)
        except ProcessLookupError:
            continue
        signaled.append(target)
    return signaled


def _last_log_event(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    for line in reversed(path.read_text(errors="replace").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            return data
    return None


def _read_log_events(path: Path, *, limit: int = 50, event_name: str | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw_lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []

    events: list[dict[str, Any]] = []
    normalized_limit = max(limit, 0)
    for line in reversed(raw_lines):
        if normalized_limit and len(events) >= normalized_limit:
            break
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if event_name and payload.get("event") != event_name:
            continue
        events.append(payload)
    return events


def _read_status_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_status_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _derive_state_summary(state: str, status_record: dict[str, Any] | None) -> str:
    if isinstance(status_record, dict):
        summary = status_record.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
    defaults = {
        "starting": "Worker daemon started and is preparing its lane-scoped runtime.",
        "idle": "No actionable lane inbox items are currently assigned to this worker.",
        "waiting_for_orchestrator": "Worker already submitted a handoff and is waiting for orchestrator follow-up.",
        "executing": "Worker execution is currently running.",
        "reviewing": "Worker self-review is currently running.",
        "verifying": "Worker lane-local verification is currently running.",
        "handoff": "Worker is submitting its final handoff.",
        "handoff_failed": "The final worker handoff failed; retry the saved result instead of rerunning the lane assignment.",
        "paused": "Worker process is paused. Resume it with manage_worker(action='resume') or SIGCONT.",
        "stopped": "Worker daemon is not currently running for this lane.",
    }
    return defaults.get(state, "Worker status is available.")


def _derive_worker_state(
    *,
    process: dict[str, Any] | None,
    status_record: dict[str, Any] | None,
    stale_lock: bool,
) -> tuple[str, str, bool]:
    if isinstance(process, dict) and process.get("stopped") is True:
        return "paused", _derive_state_summary("paused", status_record), False
    state = str(status_record.get("state") or "").strip() if isinstance(status_record, dict) else ""
    if state:
        attention_required = state == "handoff_failed"
        return state, _derive_state_summary(state, status_record), attention_required
    if isinstance(process, dict):
        return "running", "Worker daemon is running.", False
    if stale_lock:
        return "stopped", "Worker daemon is not running but its lock file is stale.", True
    return "stopped", _derive_state_summary("stopped", status_record), False


def daemon_status(*, state_dir: Path, log_dir: Path, lane_id: str, task_ref: str | None = None) -> dict[str, Any]:
    lock_path = _lock_path(state_dir, lane_id)
    lock = _read_lock_info(lock_path)
    probe = probe_worker_lock(lock_path)
    lock["held"] = bool(probe.get("held"))
    lock["expired"] = _lease_expired(lock)
    pid = lock.get("pid")
    # pid / ``ps`` stay in the payload for humans; they must not feed held/not-held.
    process = _ps_info(int(pid)) if isinstance(pid, int) else None
    if process is not None:
        process["pid_source"] = "lock"
    if process is None:
        process = _find_worker_process(task_ref=task_ref, lane_id=lane_id)
    stale_lock = bool(probe.get("orphan"))
    status_record = _read_status_file(_status_path(state_dir, lane_id))
    worker_state, state_summary, attention_required = _derive_worker_state(
        process=process,
        status_record=status_record,
        stale_lock=stale_lock,
    )
    if lock.get("expired"):
        attention_required = True
    return {
        "lane_id": lane_id,
        "task_ref": task_ref,
        "lock": lock,
        "process": process,
        "stale_lock": stale_lock,
        "log_path": str(_log_path(log_dir, lane_id)),
        "status_path": str(_status_path(state_dir, lane_id)),
        "status_record": status_record,
        "observability": status_record.get("observability") if isinstance(status_record, dict) else None,
        "worker_state": worker_state,
        "state_summary": state_summary,
        "attention_required": attention_required,
        "last_event": _last_log_event(_log_path(log_dir, lane_id)),
    }


def daemon_event_history(
    *,
    state_dir: Path,
    log_dir: Path,
    lane_id: str,
    task_ref: str | None = None,
    limit: int = 50,
    event_name: str | None = None,
) -> dict[str, Any]:
    status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
    log_path = _log_path(log_dir, lane_id)
    events = _read_log_events(log_path, limit=limit, event_name=event_name)
    return {
        **status,
        "event_filter": event_name,
        "events": events,
        "returned": len(events),
    }


def daemon_start(
    *,
    orchestrator_root: Path,
    state_dir: Path,
    log_dir: Path,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
    session: str,
    python_executable: str,
    pythonpath: str | None = None,
    backend: str = "codex-cli",
    session_mode: str = "fresh_turn",
    reasoning_effort: str = "inherit",
    model: str | None = None,
    codex_bin: str | None = None,
    codex_args: str | None = None,
    grok_bin: str | None = None,
    grok_args: str | None = None,
    grok_max_turns: int | None = None,
    grok_timeout: int | None = None,
    poll_interval: int = 30,
    single_pass: bool = False,
    token_budget: int | None = None,
) -> dict[str, Any]:
    lock_path = _lock_path(state_dir, lane_id)
    probe = probe_worker_lock(lock_path)
    if probe.get("held"):
        status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
        pid = status.get("lock", {}).get("pid") if isinstance(status.get("lock"), dict) else None
        if not isinstance(pid, int):
            process = status.get("process")
            pid = process.get("pid") if isinstance(process, dict) else None
        return {
            "ok": False,
            "message": f"Worker daemon is already running for lane '{lane_id}'.",
            "pid": pid,
            "lock_path": str(lock_path),
            "log_path": str(_log_path(log_dir, lane_id)),
            "status": status,
        }

    # Orphan lock files may be removed only while holding the flock.
    _reclaim_orphan_lock(lock_path)

    cmd = [
        python_executable,
        str(Path(__file__).resolve().parent / "worker_daemon.py"),
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
        "--backend",
        backend,
        "--session-mode",
        session_mode,
        "--reasoning-effort",
        reasoning_effort,
        "--poll-interval",
        str(poll_interval),
    ]
    if model:
        cmd.extend(["--model", model])
    # Forward per-backend binary/args overrides so a grok (or codex) worker whose
    # binary is not on PATH can be pinned end-to-end — the daemon CLI is the only
    # producer of WorkerConfig.grok_bin/grok_args (s4-a-002 / s6-a-001).
    if codex_bin:
        cmd.extend(["--codex-bin", codex_bin])
    if codex_args:
        cmd.extend(["--codex-args", codex_args])
    if grok_bin:
        cmd.extend(["--grok-bin", grok_bin])
    if grok_args:
        cmd.extend(["--grok-args", grok_args])
    if single_pass:
        cmd.append("--single-pass")
    if token_budget is not None:
        cmd.extend(["--token-budget", str(token_budget)])
    if grok_max_turns is not None:
        cmd.extend(["--grok-max-turns", str(grok_max_turns)])
    if grok_timeout is not None:
        cmd.extend(["--grok-timeout", str(grok_timeout)])

    env = dict(os.environ)
    if pythonpath:
        env["PYTHONPATH"] = pythonpath
    # Bind the spawned worker subprocess to its lane so the MCP server can
    # resolve the worker's task_ref from the env regardless of cwd ambiguity.
    env["WORKBAY_LANE_ID"] = lane_id

    log_dir.mkdir(parents=True, exist_ok=True)
    stderr_fh = (log_dir / f"worker-{lane_id}.stderr").open("a")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(orchestrator_root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_fh,
            start_new_session=True,
        )
    finally:
        stderr_fh.close()
    return {
        "ok": True,
        "pid": proc.pid,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "session": session,
        "backend": backend,
        "session_mode": session_mode,
        "reasoning_effort": reasoning_effort,
        "model": model,
        "poll_interval": poll_interval,
        "single_pass": single_pass,
        "worktree_path": str(worktree_path),
        "lock_path": str(_lock_path(state_dir, lane_id)),
        "log_path": str(_log_path(log_dir, lane_id)),
    }


def _cleanup_lock(state_dir: Path, lane_id: str) -> None:
    """Delete the worker lock file only if this process can flock it."""
    _reclaim_orphan_lock(_lock_path(state_dir, lane_id))


def _emit_stopped_event(log_dir: Path, lane_id: str) -> None:
    """Append a ``worker_stopped`` JSONL event to the worker's log file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    entry: dict = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "lane": lane_id,
        "level": "INFO",
        "event": "worker_stopped",
    }
    log_path = log_dir / f"worker-{lane_id}.jsonl"
    try:
        with log_path.open("a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError:
        pass


def daemon_stop(
    *, state_dir: Path, log_dir: Path, lane_id: str, task_ref: str | None = None, force: bool = False
) -> dict[str, Any]:
    status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
    process = status.get("process")
    pid = process.get("pid") if isinstance(process, dict) else None
    if not isinstance(pid, int):
        return {"ok": False, "message": f"No running worker daemon recorded for lane '{lane_id}'.", "signaled": []}
    sig = signal.SIGKILL if force else signal.SIGTERM
    signaled = _signal_tree(pid, sig)
    status_record = status.get("status_record")
    base_payload = dict(status_record) if isinstance(status_record, dict) else {}
    base_payload.update(
        {
            "lane_id": lane_id,
            "task_ref": task_ref or base_payload.get("task_ref"),
            "state": "stopped",
            "summary": f"Worker daemon stop requested via {sig.name}.",
            "attention_required": False,
        }
    )
    _write_status_file(_status_path(state_dir, lane_id), base_payload)
    _cleanup_lock(state_dir, lane_id)
    _emit_stopped_event(log_dir, lane_id)
    return {"ok": True, "message": f"Sent {sig.name} to worker daemon lane '{lane_id}'.", "signaled": signaled}


def daemon_resume(*, state_dir: Path, log_dir: Path, lane_id: str, task_ref: str | None = None) -> dict[str, Any]:
    status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
    process = status.get("process")
    pid = process.get("pid") if isinstance(process, dict) else None
    if not isinstance(pid, int):
        return {"ok": False, "message": f"No running worker daemon recorded for lane '{lane_id}'.", "signaled": []}
    signaled = _signal_tree(pid, signal.SIGCONT)
    return {"ok": True, "message": f"Sent SIGCONT to worker daemon lane '{lane_id}'.", "signaled": signaled}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect and control lane worker daemons.")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--state-dir", required=True)
    common.add_argument("--log-dir", required=False)
    common.add_argument("--lane-id", required=True)
    common.add_argument("--task-ref")

    sub.add_parser("status", parents=[common])
    stop = sub.add_parser("stop", parents=[common])
    stop.add_argument("--force", action="store_true")
    sub.add_parser("resume", parents=[common])
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    state_dir = Path(args.state_dir).expanduser().resolve()
    log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else state_dir.parent / "logs" / "worker-daemon"

    if args.command == "status":
        print(
            json.dumps(
                daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=args.lane_id, task_ref=args.task_ref),
                indent=2,
            )
        )
        return 0
    if args.command == "stop":
        result = daemon_stop(
            state_dir=state_dir, log_dir=log_dir, lane_id=args.lane_id, task_ref=args.task_ref, force=args.force
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1
    if args.command == "resume":
        result = daemon_resume(state_dir=state_dir, log_dir=log_dir, lane_id=args.lane_id, task_ref=args.task_ref)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
