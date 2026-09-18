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
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.db_writer_liveness import WRITER_HEARTBEAT_STALE_SECONDS

from workbay_orchestrator_mcp.orchestration.remote_sandbox_reap import (
    _validate_probe_process_identity,
    release_remote_lane_lease,
)

_HANDOFF_REPLAY_PREFLIGHT_TIMEOUT_SECONDS = 120
_HANDOFF_REPLAY_SUBMIT_TIMEOUT_SECONDS = 900


def _handoff_replay_timeout_seconds(env_name: str, default: int) -> int:
    """Read a positive-int timeout from env; garbage and non-positive values fail closed."""
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < 1:
        return default
    return value


def _lock_path(state_dir: Path, lane_id: str) -> Path:
    return state_dir / f"worker-{lane_id}.lock"


def _log_path(log_dir: Path, lane_id: str) -> Path:
    return log_dir / f"worker-{lane_id}.jsonl"


def _spawn_worker_process(cmd: list[str], **kwargs: Any) -> subprocess.Popen[Any]:
    """Spawn one worker through a controller-local seam.

    ``daemon_start`` also derives capability identity before this call.  Keeping
    the process edge behind a narrow seam lets consumers replace only the worker
    spawn in tests; patching the process-global ``subprocess.Popen`` would also
    intercept the controller's harmless Git identity probes.
    """
    return subprocess.Popen(cmd, **kwargs)  # noqa: S603


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


def _process_identity(pid: int) -> tuple[int, str] | None:
    """Return a PID plus stable start-time token, or no safe identity."""
    info = _ps_info(pid)
    if info is None or "Z" in str(info.get("stat") or ""):
        return None
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        capture_output=True,
        text=True,
        check=False,
    )
    start_time = result.stdout.strip()
    if result.returncode != 0 or not start_time:
        return None
    return pid, start_time


def _snapshot_process_tree(pid: int) -> dict[int, tuple[int, str]]:
    """Capture owned process identities before any signal is sent."""
    snapshot: dict[int, tuple[int, str]] = {}
    for target in _process_tree(pid):
        if target <= 0 or target == os.getpid():
            continue
        identity = _process_identity(target)
        if identity is not None:
            snapshot[target] = identity
    return snapshot


def _surviving_processes(snapshot: dict[int, tuple[int, str]]) -> dict[int, tuple[int, str]]:
    """Retain only PIDs whose pre-signal start identity still matches."""
    return {pid: identity for pid, identity in snapshot.items() if _process_identity(pid) == identity}


def _signal_process_snapshot(snapshot: dict[int, tuple[int, str]], sig: signal.Signals) -> list[int]:
    """Signal only fenced individual PIDs; never use a caller-owned process group."""
    signaled: list[int] = []
    for pid, identity in snapshot.items():
        if pid <= 0 or pid == os.getpid() or _process_identity(pid) != identity:
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        signaled.append(pid)
    return signaled


def _signal_tree(pid: int, sig: signal.Signals) -> list[int]:
    signaled: list[int] = []
    for target in _process_tree(pid):
        if target <= 0 or target == os.getpid():
            continue
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


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_status_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as temporary_file:
            json.dump(payload, temporary_file, indent=2, sort_keys=True)
            temporary_file.write("\n")
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


class TerminalPersistenceError(RuntimeError):
    """A terminal durability boundary failed; dispatch must stop nonzero."""


_NO_LAUNCH_RECEIPT_CLEANUP_ERROR = "no launch receipt: nothing to reap"
_PENDING_RECEIPT_REMEDY = "mcp-workbay-orchestrator remote-sandbox-reap --apply"
_NO_SANDBOX_REMEDY = "mark cleanup_state=complete after confirming no sandbox exists"


def _malformed_cleanup_receipt_error(event_path: object) -> str:
    return f"malformed launch receipt at {event_path}"


def _cleanup_receipt_is_valid(receipt: object) -> bool:
    """True when a non-null receipt carries the reaper's full process identity.

    DBG-10 / REF-26: one shape check at persist, replay, and remedy. None is the
    no-launch sentinel; do not treat a falsy non-null value as "nothing to reap".
    """
    return bool(_validate_probe_process_identity(receipt, receipt))


def _terminal_event_needs_recovery(event: dict[str, Any]) -> bool:
    if event.get("delivery_state") != "acknowledged" or event.get("cleanup_state") != "complete":
        return True
    receipt = event.get("cleanup_receipt")
    return receipt is not None and not _cleanup_receipt_is_valid(receipt)


def _terminal_cleanup_remedy(event: dict[str, Any]) -> str:
    receipt = event.get("cleanup_receipt")
    if event.get("cleanup_state") == "pending" and receipt is not None:
        if _cleanup_receipt_is_valid(receipt):
            return _PENDING_RECEIPT_REMEDY
        return _malformed_cleanup_receipt_error(event.get("event_path"))
    return _NO_SANDBOX_REMEDY


def _terminal_operator_recovery_message(retained: list[dict[str, Any]]) -> str:
    lines = ["terminal replay requires operator recovery:"]
    for event in retained:
        lines.append(
            "event_path={event_path} lane_id={lane_id} delivery_state={delivery_state} "
            "cleanup_state={cleanup_state} cleanup_error={cleanup_error}".format(
                event_path=event.get("event_path"),
                lane_id=event.get("lane_id"),
                delivery_state=event.get("delivery_state"),
                cleanup_state=event.get("cleanup_state"),
                cleanup_error=event.get("cleanup_error", ""),
            )
        )
        lines.append(f"remedy: {_terminal_cleanup_remedy(event)}")
    return "\n".join(lines)


def _raise_if_terminal_recovery_required(retained: list[dict[str, Any]]) -> None:
    blocked = [event for event in retained if _terminal_event_needs_recovery(event)]
    if blocked:
        raise TerminalPersistenceError(_terminal_operator_recovery_message(blocked))


def _terminal_write(path: Path, record: dict[str, Any]) -> None:
    try:
        _write_status_file(path, record)
        # Persist the directory entry as well when terminal-events was just created.
        _fsync_directory(path.parent.parent)
    except OSError as exc:
        raise TerminalPersistenceError(f"terminal persistence failed at {path}: {exc}") from exc


def _terminal_attempt_path(state_dir: Path, task_ref: str, lane_id: str) -> Path:
    import hashlib

    key = hashlib.sha256(f"{task_ref.strip()}\0{lane_id.strip()}".encode()).hexdigest()
    return state_dir / "terminal-attempts" / f"{key}.json"


def _begin_terminal_attempt(
    *,
    state_dir: Path,
    task_ref: str,
    lane_id: str,
    run_id: str = "",
    cleanup_receipt: dict | None = None,
) -> dict[str, Any]:
    """Called under the worker lane lock, before launching a new attempt."""
    import uuid

    retained = _replay_terminal_events(state_dir=state_dir, task_ref=task_ref, lane_id=lane_id, read_only=True)
    _raise_if_terminal_recovery_required(retained)
    record = {"attempt_id": str(uuid.uuid4()), "run_id": run_id, "cleanup_receipt": cleanup_receipt}
    _terminal_write(_terminal_attempt_path(state_dir, task_ref, lane_id), record)
    return record


def _terminal_payload(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("terminal payload is not an object")
    result = {}
    for key in ("delivery_id", "task_ref", "lane_id", "session", "summary", "outcome"):
        item = value.get(key)
        if not isinstance(item, str) and not (key == "outcome" and item is None):
            raise ValueError(f"invalid terminal {key}")
        result[key] = item
    for key in ("changed_files", "test_commands", "blockers"):
        item = value.get(key, value.get(f"{key}_json", []))
        if isinstance(item, str):
            item = json.loads(item)
        if not isinstance(item, list) or any(not isinstance(part, str) for part in item):
            raise ValueError(f"invalid terminal {key}")
        result[key] = item
    merge_ready = value.get("merge_ready", False)
    if type(merge_ready) not in (bool, int) or merge_ready not in (0, 1):
        raise ValueError("invalid terminal merge_ready")
    result["merge_ready"] = bool(merge_ready)
    return result


def _persist_terminal_event(
    *,
    state_dir: Path,
    task_ref: str,
    lane_id: str,
    report_payload: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    import hashlib

    task_ref, lane_id = task_ref.strip(), lane_id.strip()
    attempt_path = _terminal_attempt_path(state_dir, task_ref, lane_id)
    if attempt_path.exists():
        attempt = json.loads(attempt_path.read_text())
    else:
        attempt = _begin_terminal_attempt(state_dir=state_dir, task_ref=task_ref, lane_id=lane_id)
    event_id = f"terminal:{task_ref}:{lane_id}:{attempt['attempt_id']}"
    path = state_dir / "terminal-events" / f"{hashlib.sha256(event_id.encode()).hexdigest()}.json"
    if path.exists():
        # Lost reply / restart: never derive a replacement payload from live state.
        return path, json.loads(path.read_text())
    payload = _terminal_payload(dict(report_payload, delivery_id=event_id, task_ref=task_ref, lane_id=lane_id))
    receipt = attempt.get("cleanup_receipt")
    if receipt is None:
        cleanup_state = "complete"
        cleanup_error = _NO_LAUNCH_RECEIPT_CLEANUP_ERROR
    elif _cleanup_receipt_is_valid(receipt):
        cleanup_state = "pending"
        cleanup_error = ""
    else:
        cleanup_state = "pending"
        cleanup_error = _malformed_cleanup_receipt_error(path)
    record = dict(
        schema_version=1,
        event_id=event_id,
        task_ref=task_ref,
        lane_id=lane_id,
        run_id=attempt.get("run_id", ""),
        attempt_id=attempt["attempt_id"],
        report_payload=payload,
        cleanup_receipt=receipt,
        cleanup_state=cleanup_state,
        cleanup_error=cleanup_error,
        delivery_state="pending",
        attempts=0,
        next_retry_at=0,
        last_error="",
        report_id=None,
    )
    _terminal_write(path, record)
    return path, record


def _terminal_retirement_ready(event: dict[str, Any], *, ownership_retired: bool = False) -> bool:
    """No age-based deletion: caller must also establish artifact/commit retirement."""
    return bool(
        ownership_retired
        and event.get("event_id")
        and event.get("report_id")
        and event.get("delivery_state") == "acknowledged"
        and event.get("cleanup_state") == "complete"
    )


def _terminal_submit(payload: dict[str, Any]) -> dict[str, Any]:
    """Bound caller wait; an in-doubt commit always keeps its original delivery ID."""
    import contextvars
    import queue
    import threading

    from workbay_orchestrator_mcp.lanes import worker_reports

    result = queue.Queue(maxsize=1)

    def submit() -> None:
        try:
            result.put((True, worker_reports(operation="record", **payload)))
        except Exception as exc:
            result.put((False, exc))

    threading.Thread(
        target=contextvars.copy_context().run, args=(submit,), daemon=True, name="terminal-delivery"
    ).start()
    try:
        ok, response = result.get(timeout=10)
    except queue.Empty as exc:
        raise TimeoutError("terminal delivery deadline exceeded (10s)") from exc
    if not ok:
        raise response
    return response


def _replay_terminal_events(
    *,
    state_dir: Path,
    task_ref: str,
    lane_id: str,
    now: float | None = None,
    repair: bool = False,
    read_only: bool = False,
) -> list[dict[str, Any]]:
    """Replay under the existing lane lock; never mutate the generic sink fingerprint.

    TEST-15 / AGT-21 (https://github.com/darce/heuristics-canon): retain
    evidence of partial failure. A committed but mismatching row is a permanent
    refusal, not transport failure; outcome-mismatch tests falsify this boundary.
    """
    import math

    live_clock = now is None
    now = time.time() if live_clock else now
    task_ref, lane_id = task_ref.strip(), lane_id.strip()
    results = []
    for path in sorted((state_dir / "terminal-events").glob("*.json")):
        try:
            event = json.loads(path.read_text())
            if not isinstance(event, dict):
                raise ValueError("terminal event is not an object")
            if not isinstance(event.get("task_ref"), str) or not isinstance(event.get("lane_id"), str):
                raise ValueError("missing terminal task/lane identity")
            if event.get("task_ref") != task_ref or event.get("lane_id") != lane_id:
                continue
            if event.get("schema_version") != 1:
                raise ValueError("unsupported terminal schema")
            payload = _terminal_payload(event["report_payload"])
            if (payload["delivery_id"], payload["task_ref"], payload["lane_id"]) != (
                event["event_id"],
                task_ref,
                lane_id,
            ):
                raise ValueError("terminal identity mismatch")
            if type(event.get("attempts")) is not int or not 0 <= event["attempts"] <= 3:
                raise ValueError("invalid terminal attempt budget")
            if event.get("delivery_state") not in {"pending", "blocked", "acknowledged"}:
                raise ValueError("invalid terminal delivery state")
            try:
                valid_retry_timestamp = type(event.get("next_retry_at")) in (int, float) and math.isfinite(
                    event["next_retry_at"]
                )
            except OverflowError:
                valid_retry_timestamp = False
            if not valid_retry_timestamp:
                raise ValueError("invalid terminal retry timestamp")
            if event.get("cleanup_state") not in {"unknown", "pending", "blocked", "complete"}:
                raise ValueError("invalid terminal cleanup state")
            receipt = event.get("cleanup_receipt")
            if (
                event.get("cleanup_state") == "complete"
                and receipt is not None
                and not _cleanup_receipt_is_valid(receipt)
            ):
                event.update(cleanup_state="blocked", cleanup_error=_malformed_cleanup_receipt_error(path))
                if not read_only:
                    _terminal_write(path, event)
            if read_only:
                results.append(dict(event, event_path=str(path)))
                continue
            if event.get("cleanup_state") == "blocked" and receipt is None:
                event.update(cleanup_state="complete", cleanup_error=_NO_LAUNCH_RECEIPT_CLEANUP_ERROR)
                _terminal_write(path, event)
            elif event.get("cleanup_state") == "blocked" and not _cleanup_receipt_is_valid(receipt):
                malformed = _malformed_cleanup_receipt_error(path)
                if event.get("cleanup_error") != malformed:
                    event.update(cleanup_error=malformed)
                    _terminal_write(path, event)
            if repair and event["delivery_state"] == "blocked":
                event.update(delivery_state="pending", attempts=0, next_retry_at=0, last_error="")
                _terminal_write(path, event)
            if event["delivery_state"] == "pending" and event["attempts"] >= 3:
                event.update(delivery_state="blocked", last_error="terminal delivery attempts exhausted")
                _terminal_write(path, event)
            if event["delivery_state"] == "pending" and now >= event["next_retry_at"]:
                event["attempts"] += 1
                event["next_retry_at"] = now + (1 if event["attempts"] == 1 else 5)
                _terminal_write(path, event)  # Spend budget before a possibly committed call.
                try:
                    response = _terminal_submit(payload)
                except Exception as exc:
                    event["last_error"] = str(exc)
                    event["next_retry_at"] = (time.time() if live_clock else now) + (1 if event["attempts"] == 1 else 5)
                    if event["attempts"] >= 3:
                        event["delivery_state"] = "blocked"
                else:
                    row = response.get("report") if isinstance(response, dict) else None
                    try:
                        if response.get("ok") is not True or not isinstance(row, dict) or not row.get("id"):
                            raise ValueError(f"terminal sink rejected payload: {response}")
                        if _terminal_payload(row) != payload:
                            raise ValueError("terminal sink full-payload mismatch")
                    except (ValueError, TypeError, AttributeError) as exc:
                        event.update(delivery_state="blocked", last_error=str(exc))
                    else:
                        event.update(delivery_state="acknowledged", report_id=row["id"], last_error="")
                _terminal_write(path, event)
            results.append(dict(event, event_path=str(path)))
        except (ValueError, KeyError, TypeError, OSError) as exc:
            # Preserve malformed bytes for operator repair, and fail dispatch closed.
            results.append(
                dict(event_path=str(path), delivery_state="blocked", cleanup_state="unknown", last_error=str(exc))
            )
    return results


def _drive_terminal_replay(
    *, state_dir: Path, task_ref: str, lane_id: str, repair: bool = False
) -> list[dict[str, Any]]:
    """Drain the persisted three-attempt budget under the caller's lane lock.

    GRPH-29 / TEST-15: a saved due time needs an active recovery consumer.
    No backend is launched while these bounded delivery attempts run.
    """
    for iteration in range(4):
        events = _replay_terminal_events(
            state_dir=state_dir,
            task_ref=task_ref,
            lane_id=lane_id,
            repair=repair and iteration == 0,
        )
        pending = [event for event in events if event["delivery_state"] == "pending"]
        if not pending or iteration == 3:
            return events
        delay = max(0.0, min(event["next_retry_at"] for event in pending) - time.time())
        # Clock rollback or operator-edited timestamps must not hold the lane indefinitely.
        if delay > 5:
            return events
        time.sleep(delay)
    return events


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
        "handoff_failed": (
            "The final worker handoff failed; replay the saved result with "
            "manage_worker action=retry_handoff instead of rerunning the lane assignment."
        ),
        "orphaned": (
            "The worker daemon exited because its supervising orchestrator process died; "
            "the unfinished lane requires operator attention."
        ),
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
        # Orphaned is a named stability event: surface attention, but do not
        # treat the status-file flag as the only source (dormancy writes it
        # explicitly; orphaned may only write state=orphaned).
        attention_required = state in {"handoff_failed", "orphaned"} or bool(status_record.get("attention_required"))
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
    pending_routing: dict[str, Any] | None = None
    effective_routing = status_record.get("routing_effective") if isinstance(status_record, dict) else None
    if isinstance(effective_routing, dict) and task_ref:
        try:
            from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

            listed = manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=10_000)
            rows = listed.get("lanes") if isinstance(listed, dict) else None
            lane_row = next(
                (row for row in rows or [] if isinstance(row, dict) and row.get("lane_id") == lane_id),
                None,
            )
            if lane_row is not None:
                row_routing = {
                    "backend": lane_row.get("backend"),
                    "model": lane_row.get("model"),
                    "effort": lane_row.get("reasoning_effort"),
                    "speed": lane_row.get("speed"),
                    "tier": lane_row.get("tier"),
                }
                if row_routing != effective_routing:
                    pending_routing = row_routing
        except (ImportError, OSError, RuntimeError, TypeError, ValueError):
            pending_routing = None
    terminal_events = _replay_terminal_events(
        state_dir=state_dir,
        task_ref=task_ref or str((status_record or {}).get("task_ref") or ""),
        lane_id=lane_id,
        read_only=True,
    )
    if any(_terminal_event_needs_recovery(event) for event in terminal_events):
        attention_required = True
    return {
        "terminal_events": terminal_events,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "lock": lock,
        "process": process,
        "stale_lock": stale_lock,
        "log_path": str(_log_path(log_dir, lane_id)),
        "status_path": str(_status_path(state_dir, lane_id)),
        "status_record": status_record,
        "observability": status_record.get("observability") if isinstance(status_record, dict) else None,
        "pending_routing": pending_routing,
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


def canonical_capability_speed(backend: str | None, speed: object) -> str | None:
    """Canonical speed hashed into capability receipts and forwarded to workers.

    One owner for mint/identity (REF-26): ``lane_routing.normalize_speed``.
    Codex-remote None/'' -> ``standard``; ``fast`` passes through; other
    backends stay None. Unknown values raise the typed ``InvalidSpeed``.
    """
    from workbay_orchestrator_mcp.orchestration.lane_routing import normalize_speed  # noqa: PLC0415

    return normalize_speed(backend, speed)


def _remote_capability_identity(
    *,
    backend: str,
    model: str | None,
    speed: str | None,
    reasoning_effort: str,
    worktree_path: Path,
    grok_max_turns: int | None,
    grok_timeout: int | None,
    adapter_timeout: int | None,
    token_budget: int | None,
    lane_id: str,
    pass_id: str | None,
    dispatch_id: str | None,
) -> dict[str, Any]:
    """Derive the complete remote identity used by daemon-start authorization."""
    from workbay_orchestrator_mcp.orchestration import preflight_attestation as attest  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.adapters import remote_exec  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.backend_spec import (  # noqa: PLC0415
        build_agent_spec,
        default_effort_for_model,
        resolve_remote_lane_timeout_s,
    )
    from workbay_orchestrator_mcp.orchestration.grok_lane_config import (  # noqa: PLC0415
        GROK_MAX_TURNS_CAP,
        GROK_TIMEOUT_CAP,
    )
    from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
        derive_grok_single_cycle_bounds,
    )

    speed = canonical_capability_speed(backend, speed)
    resolved_worktree = Path(worktree_path).expanduser().resolve()
    row = BACKENDS.get(backend)
    effective_model = ""
    sandbox_flags: list[str] = []
    transport_digest: str | None = None
    single_cycle_bounds: dict[str, Any] = {}
    if row is not None:
        try:
            effective_model = remote_exec.resolve_effective_model(row, backend, model)
        except (RuntimeError, TypeError, ValueError):
            effective_model = str(model or row.allowed_model or "")
        requested_effort = str(reasoning_effort or "").strip().lower()
        spec_effort = (
            requested_effort
            if requested_effort not in {"", "auto", "inherit"}
            else (default_effort_for_model(backend, effective_model) if effective_model else None)
        )
        if row.capabilities.supports_token_budget_cycle_bounds:
            if token_budget is not None and token_budget > 0:
                derived = derive_grok_single_cycle_bounds(token_budget)
            else:
                derived = {"max_turns": GROK_MAX_TURNS_CAP, "timeout": GROK_TIMEOUT_CAP}
            max_turns = int(grok_max_turns if grok_max_turns is not None else derived["max_turns"])
            cycle_timeout = int(grok_timeout if grok_timeout is not None else derived["timeout"])
            single_cycle_bounds = {"max_turns": max_turns, "timeout": cycle_timeout}
        elif row.capabilities.supports_adapter_timeout_bounds:
            cycle_timeout = adapter_timeout if adapter_timeout is not None else grok_timeout
            cycle_timeout = int(cycle_timeout if cycle_timeout is not None else resolve_remote_lane_timeout_s(backend))
            single_cycle_bounds = {"timeout": cycle_timeout}
        else:
            cycle_timeout = None
        if effective_model:
            try:
                spec = build_agent_spec(
                    backend,
                    model=effective_model,
                    effort=spec_effort,
                    speed=speed,
                    prompt="capability receipt daemon preflight",
                    max_turns=(
                        single_cycle_bounds.get("max_turns")
                        if row.capabilities.supports_token_budget_cycle_bounds
                        else None
                    ),
                    agent_turn_timeout_s=(cycle_timeout if row.capabilities.supports_adapter_timeout_bounds else None),
                )
                sandbox_flags = remote_exec._sandbox_attestation_flags(spec.argv)
                resolution = remote_exec.resolve_transport(resolved_worktree, recipe_argv=spec.argv)
                if resolution.source == remote_exec.TRANSPORT_SOURCE_WORKTREE:
                    transport_digest = resolution.transport_digest
            except (
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
                remote_exec.TransportMissingError,
                remote_exec.WorktreeRemoteAgentUnreadableError,
                remote_exec.EmptyRecipePlaceholderSetError,
                remote_exec.RecipeResolverSkewError,
            ):
                # The adapter remains the final transport boundary. Keep the
                # missing dimensions explicit so the validator returns a typed
                # incomplete-identity refusal instead of authorizing a partial
                # controller snapshot.
                pass
        if effective_model and transport_digest:
            config = attest.build_capability_configuration(
                agent=backend,
                model=effective_model,
                reasoning_effort=spec_effort,
                speed=speed,
                single_cycle_bounds=single_cycle_bounds,
                worktree_path=resolved_worktree,
                transport_digest=transport_digest,
            )
            config_digest = attest.configuration_digest(config)
        else:
            config_digest = None
    else:
        config_digest = None

    gate_host = remote_exec._transport_attestation_gate_host(resolved_worktree, os.environ)
    return attest.build_capability_identity(
        lane_id=lane_id,
        pass_id=pass_id,
        dispatch_id=dispatch_id,
        gate_host=gate_host,
        codex_version="",
        sandbox_flags=sandbox_flags,
        worktree_path=resolved_worktree,
        config_digest=config_digest,
        transport_digest=transport_digest,
    )


def is_remote_worker_backend(backend: str | None) -> bool:
    """True for ``remote``, ``codex-remote``, and every ``*-remote`` backend."""
    name = str(backend or "").strip()
    return name in {"remote", "codex-remote"} or name.endswith("-remote")


class WorktreeCapabilityReceipt:
    """Identity-bound receipt minted from the lane worktree transport."""

    __slots__ = (
        "capability_receipt_id",
        "capability_config_digest",
        "pass_id",
        "dispatch_id",
    )

    def __init__(
        self,
        capability_receipt_id: Any,
        capability_config_digest: Any,
        pass_id: str,
        dispatch_id: str,
    ) -> None:
        self.capability_receipt_id = capability_receipt_id
        self.capability_config_digest = capability_config_digest
        self.pass_id = pass_id
        self.dispatch_id = dispatch_id


class WorktreeCapabilityReceiptRefusal:
    """Typed mint refusal. Never an ambient-env substitute."""

    __slots__ = ("reason", "error", "failed_stage")

    def __init__(self, reason: str, error: str, failed_stage: str = "attestation") -> None:
        self.reason = reason
        self.error = error
        self.failed_stage = failed_stage


def mint_worktree_capability_receipt(
    *,
    orchestrator_root: Path,
    backend: str,
    model: str | None,
    speed: str | None,
    reasoning_effort: str,
    worktree_path: Path,
    grok_max_turns: int | None,
    grok_timeout: int | None,
    adapter_timeout: int | None,
    token_budget: int | None,
    lane_id: str,
    pass_id: str,
    dispatch_id: str,
    persist: bool = True,
    persist_receipt: Callable[..., str] | None = None,
    preflight_error_type: type[BaseException] | None = None,
) -> WorktreeCapabilityReceipt | WorktreeCapabilityReceiptRefusal:
    """Mint a worktree-bound capability receipt for a ``*-remote`` worker.

    Identity is derived from the same bounds the worker will carry. An empty
    worktree transport digest is ``transport_missing`` — package/primary
    fallback and ambient ``WORKBAY_CAPABILITY_RECEIPT_ID`` are never substitutes.
    ``persist=False`` still derives identity and reports ``transport_missing``
    but does not replace the current attestation record. The persist path
    always forwards ``lane_id`` so per-lane attestation records stay isolated.
    """
    from workbay_orchestrator_mcp.orchestration.offload_preflight import (  # noqa: PLC0415
        OffloadPreflightError,
        persist_transport_capability_receipt,
    )

    persist_fn = persist_receipt if persist_receipt is not None else persist_transport_capability_receipt
    error_type: type[BaseException] = preflight_error_type or OffloadPreflightError
    speed = canonical_capability_speed(backend, speed)
    identity = _remote_capability_identity(
        backend=backend,
        model=model,
        speed=speed,
        reasoning_effort=reasoning_effort,
        worktree_path=worktree_path,
        grok_max_turns=grok_max_turns,
        grok_timeout=grok_timeout,
        adapter_timeout=adapter_timeout,
        token_budget=token_budget,
        lane_id=lane_id,
        pass_id=pass_id,
        dispatch_id=dispatch_id,
    )
    transport_digest = identity.get("transport_digest")
    if not isinstance(transport_digest, str) or not transport_digest.strip():
        return WorktreeCapabilityReceiptRefusal(
            reason="transport_missing",
            error=("remote worker start could not mint a capability receipt: worktree transport is missing"),
        )
    config_digest = identity.get("config_digest")
    if not persist:
        return WorktreeCapabilityReceipt(
            capability_receipt_id=None,
            capability_config_digest=config_digest if isinstance(config_digest, str) else None,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
        )
    try:
        capability_receipt_id = persist_fn(
            Path(orchestrator_root),
            gate_host=str(identity.get("gate_host") or ""),
            sandbox_flags=identity.get("sandbox_flags") or [],
            lane_id=lane_id,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
            worktree_path=Path(worktree_path),
            config_digest=identity.get("config_digest"),
            transport_digest=transport_digest,
        )
    except Exception as exc:
        if not isinstance(exc, error_type):
            raise
        return WorktreeCapabilityReceiptRefusal(
            reason=str(getattr(exc, "kind", None) or "capability_receipt_unavailable"),
            error=str(exc),
        )
    if not capability_receipt_id:
        return WorktreeCapabilityReceiptRefusal(
            reason="capability_receipt_unavailable",
            error="capability receipt was not persisted",
        )
    return WorktreeCapabilityReceipt(
        capability_receipt_id=capability_receipt_id,
        capability_config_digest=config_digest if isinstance(config_digest, str) else None,
        pass_id=pass_id,
        dispatch_id=dispatch_id,
    )


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
    speed: str | None = None,
    tier: str | None = None,
    codex_bin: str | None = None,
    codex_args: str | None = None,
    grok_bin: str | None = None,
    grok_args: str | None = None,
    grok_max_turns: int | None = None,
    # Grok-family single-cycle clock (or an explicit self-verify bound).
    grok_timeout: int | None = None,
    adapter_timeout: int | None = None,
    poll_interval: int = 30,
    # Consecutive non-actionable polls, not seconds.
    dormant_poll_deadline: int = 120,
    # Absolute dormancy ceiling in seconds; non-positive disables only this ceiling.
    dormant_max_wall_clock_seconds: int = 300,
    single_pass: bool = False,
    token_budget: int | None = None,
    test_cmd: str | None = None,
    capability_receipt_id: str | None = None,
    capability_config_digest: str | None = None,
    pass_id: str | None = None,
    dispatch_id: str | None = None,
) -> dict[str, Any]:
    from workbay_orchestrator_mcp.orchestration.preflight_attestation import (  # noqa: PLC0415
        CAPABILITY_RECEIPT_ENV,
        STALE_WORKER_CONFIGURATION_REASON,
        validate_capability_receipt,
    )

    if backend in {"remote", "codex-remote"} or backend.endswith("-remote") or capability_receipt_id is not None:
        # ``orchestrator_root`` is already the resolved control-plane root. Do
        # not run git here: daemon_start is called with Popen mocked in tests and
        # a live root lookup would consume that mock before the actual spawn.
        speed = canonical_capability_speed(backend, speed)
        expected_identity = _remote_capability_identity(
            backend=backend,
            model=model,
            speed=speed,
            reasoning_effort=reasoning_effort,
            worktree_path=worktree_path,
            grok_max_turns=grok_max_turns,
            grok_timeout=grok_timeout,
            adapter_timeout=adapter_timeout,
            token_budget=token_budget,
            lane_id=lane_id,
            pass_id=pass_id,
            dispatch_id=dispatch_id,
        )
        receipt = validate_capability_receipt(
            Path(orchestrator_root),
            capability_receipt_id,
            expected_identity=expected_identity,
            lane_id=lane_id,
            worktree_path=worktree_path,
            require_complete_identity=True,
            # A preflight-minted receipt is identity-bound transport evidence
            # with probe_timeout=None / UNKNOWN write gates (ecd32aecf9). The
            # in-execute live probe completes those fields before model spawn.
            require_complete_capability=False,
        )
        if not receipt["ok"]:
            return {**receipt, "failed_stage": "attestation", "lane_id": lane_id}
        if capability_config_digest is not None and capability_config_digest != expected_identity.get("config_digest"):
            return {
                "ok": False,
                "reason": STALE_WORKER_CONFIGURATION_REASON,
                "mismatched_fields": ["config_digest"],
                "failed_stage": "attestation",
                "lane_id": lane_id,
            }
        capability_receipt_id = receipt["capability_receipt_id"]
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
        "--dormant-poll-deadline",
        str(dormant_poll_deadline),
        "--dormant-max-wall-clock-seconds",
        str(dormant_max_wall_clock_seconds),
        "--supervisor-pid",
        str(os.getpid()),
    ]
    if model:
        cmd.extend(["--model", model])
    if speed:
        cmd.extend(["--speed", speed])
    if tier:
        cmd.extend(["--tier", tier])
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
        cmd.extend(["--adapter-max-turns", str(grok_max_turns)])
    # Both bounded families use the public adapter-named clock flag. The worker
    # routes it to the capability-specific WorkerConfig field after parsing.
    cycle_timeout = adapter_timeout if adapter_timeout is not None else grok_timeout
    if cycle_timeout is not None:
        cmd.extend(["--adapter-timeout", str(cycle_timeout)])
    if test_cmd is not None:
        cmd.extend(["--test-cmd", test_cmd])
    if capability_receipt_id is not None:
        cmd.extend(["--capability-receipt-id", capability_receipt_id])
    if capability_config_digest is not None:
        cmd.extend(["--capability-config-digest", capability_config_digest])
    if pass_id is not None:
        cmd.extend(["--pass-id", pass_id])
    if dispatch_id is not None:
        cmd.extend(["--dispatch-id", dispatch_id])

    env = dict(os.environ)
    if pythonpath:
        env["PYTHONPATH"] = pythonpath
    # Bind the spawned worker subprocess to its lane so the MCP server can
    # resolve the worker's task_ref from the env regardless of cwd ambiguity.
    env["WORKBAY_LANE_ID"] = lane_id
    # Never inherit another lane's receipt from the launching server. The
    # explicit CLI/config carrier above is authoritative; this env name remains
    # only as a break-glass carrier for independently launched workers.
    env.pop(CAPABILITY_RECEIPT_ENV, None)

    log_dir.mkdir(parents=True, exist_ok=True)
    stderr_fh = (log_dir / f"worker-{lane_id}.stderr").open("a")
    try:
        proc = _spawn_worker_process(
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
        "capability_receipt_id": capability_receipt_id,
        "capability_config_digest": capability_config_digest,
        "pass_id": pass_id,
        "dispatch_id": dispatch_id,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "session": session,
        "backend": backend,
        "session_mode": session_mode,
        "reasoning_effort": reasoning_effort,
        "model": model,
        "speed": speed,
        "tier": tier,
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


def _emit_replayed_event(log_dir: Path, lane_id: str) -> None:
    """Append the operator replay transition to the worker event stream."""
    log_dir.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "lane": lane_id,
        "level": "INFO",
        "event": "worker_handoff_replayed",
    }
    try:
        with _log_path(log_dir, lane_id).open("a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError:
        pass


def _wait_for_worker_exit(
    pid: int,
    grace_seconds: float,
    snapshot: dict[int, tuple[int, str]] | None = None,
) -> bool:
    """Wait for every fenced process in a pre-signal snapshot to disappear."""
    try:
        grace = max(0.0, float(grace_seconds))
    except (TypeError, ValueError):
        grace = 0.0
    owned = snapshot if snapshot is not None else _snapshot_process_tree(pid)
    deadline = time.monotonic() + grace
    while True:
        if not _surviving_processes(owned):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.1, remaining))


def _active_remote_lease(status_record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(status_record, dict):
        return None
    raw = status_record.get("remote_lease")
    if not isinstance(raw, dict) or raw.get("active") is False:
        return None
    lane_key = raw.get("lane_key")
    nonce = raw.get("nonce")
    if not isinstance(lane_key, str) or not lane_key.strip() or not isinstance(nonce, str) or not nonce.strip():
        return None
    return dict(raw)


def daemon_stop(
    *,
    state_dir: Path,
    log_dir: Path,
    lane_id: str,
    task_ref: str | None = None,
    force: bool = False,
    force_grace_seconds: float = 3.0,
    grace_seconds: float | None = None,
) -> dict[str, Any]:
    status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
    process = status.get("process")
    pid = process.get("pid") if isinstance(process, dict) else None
    if not isinstance(pid, int):
        return {"ok": False, "message": f"No running worker daemon recorded for lane '{lane_id}'.", "signaled": []}
    if grace_seconds is not None:
        force_grace_seconds = grace_seconds
    signal_sequence: list[str] = []
    kill_set: list[int] = []
    survivors: list[int] = []
    if force:
        signal_sequence.append(signal.SIGTERM.name)
        owned_processes = _snapshot_process_tree(pid)
        signaled = _signal_process_snapshot(owned_processes, signal.SIGTERM)
        escalated = not _wait_for_worker_exit(pid, force_grace_seconds, owned_processes)
        if escalated:
            signal_sequence.append(signal.SIGKILL.name)
            kill_targets = _surviving_processes(owned_processes)
            kill_set = _signal_process_snapshot(kill_targets, signal.SIGKILL)
            signaled.extend(kill_set)
            _wait_for_worker_exit(pid, 0.5, kill_targets)
            survivors = sorted(_surviving_processes(kill_targets))
    else:
        signal_sequence.append(signal.SIGTERM.name)
        signaled = _signal_tree(pid, signal.SIGTERM)
        escalated = False
    status_record = status.get("status_record")
    base_payload = dict(status_record) if isinstance(status_record, dict) else {}
    lease_release: dict[str, Any] | None = None
    if force and escalated:
        lease = _active_remote_lease(status_record)
        if lease is not None:
            lease_release = release_remote_lane_lease(
                str(lease["lane_key"]),
                str(lease["nonce"]),
                host=lease.get("host") if isinstance(lease.get("host"), str) else None,
                sandbox_root=(lease.get("sandbox_root") if isinstance(lease.get("sandbox_root"), str) else None),
            )
    base_payload.update(
        {
            "lane_id": lane_id,
            "task_ref": task_ref or base_payload.get("task_ref"),
            "state": "stopped",
            "summary": f"Worker daemon stop requested via {signal_sequence[-1]}.",
            "attention_required": bool(survivors),
        }
    )
    response_message = f"Sent {' then '.join(signal_sequence)} to worker daemon lane '{lane_id}'."
    if lease_release is not None:
        if lease_release.get("ok"):
            base_payload["remote_lease_release"] = lease_release
        else:
            expiry = lease_release.get("expiry_epoch")
            expiry_text = str(expiry) if isinstance(expiry, int) else "unknown"
            base_payload["attention_required"] = True
            base_payload["summary"] = (
                f"Worker daemon force stop escalated, but remote lane lease release failed; "
                f"lease expiry epoch={expiry_text}."
            )
            response_message += f" Remote lane lease release failed; lease expiry epoch={expiry_text}."
    _write_status_file(_status_path(state_dir, lane_id), base_payload)
    _cleanup_lock(state_dir, lane_id)
    _emit_stopped_event(log_dir, lane_id)
    result: dict[str, Any] = {
        "ok": True,
        "message": response_message,
        "signaled": signaled,
        "kill_set": kill_set,
        "survivors": survivors,
        "signal_sequence": signal_sequence,
        "escalated": escalated,
    }
    if lease_release is not None:
        result["lease_release"] = lease_release
        if isinstance(lease_release.get("expiry_epoch"), int):
            result["lease_expiry_epoch"] = lease_release["expiry_epoch"]
    return result


def daemon_resume(*, state_dir: Path, log_dir: Path, lane_id: str, task_ref: str | None = None) -> dict[str, Any]:
    status = daemon_status(state_dir=state_dir, log_dir=log_dir, lane_id=lane_id, task_ref=task_ref)
    process = status.get("process")
    pid = process.get("pid") if isinstance(process, dict) else None
    if not isinstance(pid, int):
        return {"ok": False, "message": f"No running worker daemon recorded for lane '{lane_id}'.", "signaled": []}
    signaled = _signal_tree(pid, signal.SIGCONT)
    return {"ok": True, "message": f"Sent SIGCONT to worker daemon lane '{lane_id}'.", "signaled": signaled}


def _max_turns_from_result_file(result_path: Path) -> int | None:
    """Read the execute-path cap from a persisted result envelope, if any.

    Prefer the top-level ``max_turns`` the adapter stamped, then
    ``raw_payload.max_turns``. Missing or non-positive values stay ``None``
    so the argv builder omits ``--max-turns`` rather than inventing a cap.
    """
    from workbay_orchestrator_mcp.orchestration import worker_daemon  # noqa: PLC0415

    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if "max_turns" in payload:
        return worker_daemon._positive_handoff_max_turns(payload.get("max_turns"))
    raw_payload = payload.get("raw_payload")
    if isinstance(raw_payload, dict) and "max_turns" in raw_payload:
        return worker_daemon._positive_handoff_max_turns(raw_payload.get("max_turns"))
    return None


def daemon_retry_handoff(
    *,
    orchestrator_root: Path,
    state_dir: Path,
    log_dir: Path,
    lane_id: str,
    task_ref: str,
) -> dict[str, Any]:
    """Replay one persisted final handoff while owning the worker's lane lock."""
    replay_lock_path = _lock_path(state_dir, lane_id)
    replay_lock_path.parent.mkdir(parents=True, exist_ok=True)
    replay_lock = replay_lock_path.open("a+")
    acquired = False
    try:
        try:
            fcntl.flock(replay_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError as exc:
            return {
                "ok": False,
                "error_code": "retry_already_in_progress",
                "message": f"A worker or handoff replay already owns lane '{lane_id}': {exc}",
            }
        except Exception as exc:  # noqa: BLE001 - operator control verbs must return structured failures
            return {
                "ok": False,
                "error_code": "retry_lock_failed",
                "message": f"Unable to acquire handoff replay lock for lane '{lane_id}': {exc}",
            }
        try:
            return _daemon_retry_handoff_claimed(
                orchestrator_root=orchestrator_root,
                state_dir=state_dir,
                log_dir=log_dir,
                lane_id=lane_id,
                task_ref=task_ref,
            )
        finally:
            try:
                fcntl.flock(replay_lock.fileno(), fcntl.LOCK_UN)
            finally:
                replay_lock.close()
    finally:
        if not acquired:
            replay_lock.close()
        elif not replay_lock.closed:
            try:
                fcntl.flock(replay_lock.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            replay_lock.close()


def _daemon_retry_handoff_claimed(
    *,
    orchestrator_root: Path,
    state_dir: Path,
    log_dir: Path,
    lane_id: str,
    task_ref: str,
) -> dict[str, Any]:
    """Execute a replay from its durable terminal snapshot.

    The attempt and stable delivery id are persisted before submission. A
    later caller may safely resubmit an in-doubt delivery because the receiver
    atomically collapses that id to its first durable effect.
    """
    if (state_dir / "terminal-events").exists():
        from workbay_handoff_mcp import api
        from workbay_handoff_mcp.config import RuntimeConfig

        api.configure_runtime(RuntimeConfig.for_repo(orchestrator_root))
        events = _drive_terminal_replay(state_dir=state_dir, task_ref=task_ref, lane_id=lane_id, repair=True)
        if events:
            return {
                "ok": all(event["delivery_state"] == "acknowledged" for event in events),
                "terminal_events": events,
            }
    status_path = _status_path(state_dir, lane_id)
    if not status_path.exists():
        return {
            "ok": False,
            "error_code": "missing_status_file",
            "message": f"Worker status file is missing for lane '{lane_id}'.",
        }
    status = _read_status_file(status_path)
    if status is None:
        return {
            "ok": False,
            "error_code": "unreadable_status_file",
            "message": f"Worker status file is unreadable for lane '{lane_id}'.",
        }
    persisted_task_ref = str(status.get("task_ref") or "").strip()
    if not persisted_task_ref:
        return {
            "ok": False,
            "error_code": "missing_task_ref",
            "message": f"Worker status for lane '{lane_id}' has no task_ref.",
        }
    if persisted_task_ref != task_ref:
        return {
            "ok": False,
            "error_code": "task_ref_mismatch",
            "message": (
                f"Caller task_ref '{task_ref}' does not match persisted task_ref "
                f"'{persisted_task_ref}' for lane '{lane_id}'."
            ),
        }
    state = str(status.get("state") or "").strip()
    if state == "waiting_for_orchestrator":
        cleanup_raw = str(status.get("cleanup_pending_result_path") or "").strip()
        if cleanup_raw:
            cleanup_path = Path(cleanup_raw).expanduser()
            try:
                updated = dict(status)
                updated.pop("cleanup_pending_result_path", None)
                # Older retry snapshots may have staged this artifact for
                # deletion after an exit-0 replay. Preserve and reattach it:
                # receiver recognition alone does not prove full ceremony.
                updated["result_path"] = str(cleanup_path)
                _write_status_file(status_path, updated)
            except OSError as exc:
                return {
                    "ok": False,
                    "error_code": "status_update_failed",
                    "message": f"Lane '{lane_id}' handoff artifact could not be retained in status: {exc}",
                }
        return {
            "ok": True,
            "message": f"Lane '{lane_id}' is already waiting for orchestrator follow-up; no replay was needed.",
        }
    # These are the only retry-safe states: handoff_failed is the explicit
    # failure snapshot, while handoff and submitting are durable mid-flight
    # snapshots that may remain after a crash around external submission.
    if state not in {"handoff_failed", "handoff", "submitting"}:
        return {
            "ok": False,
            "message": (f"Lane '{lane_id}' is in state '{state or 'unknown'}', not a retryable handoff state."),
        }
    retry_record = status.get("handoff_retry")
    persisted_delivery_id = str(status.get("handoff_delivery_id") or "").strip()
    session = str(status.get("session") or "").strip()
    if not session:
        return {
            "ok": False,
            "error_code": "missing_session",
            "message": f"Worker status for lane '{lane_id}' has no session.",
        }
    persisted_worktree_raw = str(status.get("worktree_path") or "").strip()
    if not persisted_worktree_raw:
        return {
            "ok": False,
            "error_code": "missing_worktree_path",
            "message": f"Worker status for lane '{lane_id}' has no persisted worktree_path.",
        }
    persisted_worktree = Path(persisted_worktree_raw).expanduser().resolve()
    if not persisted_worktree.is_dir():
        return {
            "ok": False,
            "error_code": "missing_worktree",
            "message": f"Recorded worktree path no longer exists for lane '{lane_id}': {persisted_worktree}.",
        }
    result_path_raw = str(status.get("result_path") or "").strip()
    if not result_path_raw:
        return {
            "ok": False,
            "error_code": "missing_result_path",
            "message": f"Worker status for lane '{lane_id}' has no result_path.",
        }
    result_path = Path(result_path_raw).expanduser()
    if not result_path.exists():
        return {
            "ok": False,
            "error_code": "missing_result_file",
            "message": f"Saved result file no longer exists: {result_path}.",
        }

    from workbay_orchestrator_mcp.orchestration import worker_daemon  # noqa: PLC0415

    if not persisted_delivery_id:
        try:
            result_identity = worker_daemon._handoff_result_identity(
                task_ref=persisted_task_ref,
                lane_id=lane_id,
                result_path=result_path,
            )
            persisted_delivery_id = worker_daemon._handoff_delivery_id_for_identity(result_identity)
            migrated = dict(status)
            migrated.update(
                {
                    "handoff_delivery_id": persisted_delivery_id,
                    "handoff_delivery_result_identity": result_identity,
                }
            )
            # Safe for legacy records: this artifact contains the same bytes
            # the original handoff submitted, so its derived id is the claim
            # that submission would have carried under the stable scheme.
            _write_status_file(status_path, migrated)
            status = migrated
        except OSError as exc:
            return {
                "ok": False,
                "error_code": "delivery_id_derivation_failed",
                "message": f"Unable to derive a stable handoff delivery id for lane '{lane_id}': {exc}",
            }
    if isinstance(retry_record, dict) and retry_record.get("phase") in {"submitting", "ambiguous"}:
        attempted_delivery_id = str(retry_record.get("delivery_id") or "").strip()
        if attempted_delivery_id != persisted_delivery_id:
            return {
                "ok": False,
                "error_code": "delivery_id_mismatch",
                "message": (
                    f"Lane '{lane_id}' has an in-doubt handoff whose delivery id does not match "
                    "the stable result snapshot; automatic replay is refused."
                ),
            }
    delivery_id = persisted_delivery_id

    try:
        from workbay_orchestrator_mcp.lanes import handoff_subprocess_env  # noqa: PLC0415

        outcome = worker_daemon._outcome_for_result_file(result_path)
        max_turns = _max_turns_from_result_file(result_path)
        env = handoff_subprocess_env(
            os.environ,
            default_agent=session,
            workspace_root=persisted_worktree,
        )
        preflight_cmd = worker_daemon.build_final_handoff_argv(
            orchestrator_root=orchestrator_root,
            task_ref=persisted_task_ref,
            lane_id=lane_id,
            session=session,
            worktree_path=persisted_worktree,
            result_path=result_path,
            dry_run=True,
            outcome=outcome,
            delivery_id=delivery_id,
            max_turns=max_turns,
        )
        preflight_timeout = _handoff_replay_timeout_seconds(
            "WORKBAY_HANDOFF_REPLAY_PREFLIGHT_TIMEOUT",
            _HANDOFF_REPLAY_PREFLIGHT_TIMEOUT_SECONDS,
        )
        submit_timeout = _handoff_replay_timeout_seconds(
            "WORKBAY_HANDOFF_REPLAY_TIMEOUT",
            _HANDOFF_REPLAY_SUBMIT_TIMEOUT_SECONDS,
        )
        try:
            preflight = subprocess.run(
                preflight_cmd,
                check=False,
                capture_output=True,
                text=True,
                env=env,
                timeout=preflight_timeout,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "error_code": "replay_refused",
                "reason": "preflight_timeout",
                "message": (f"Saved handoff replay preflight for lane '{lane_id}' exceeded {preflight_timeout}s"),
            }
        if preflight.returncode != 0:
            error = (preflight.stderr or preflight.stdout or "handoff replay preflight failed").strip()
            return {
                "ok": False,
                "error_code": "replay_refused",
                "message": (
                    f"Saved handoff replay failed for lane '{lane_id}' with exit {preflight.returncode}: {error[-500:]}"
                ),
            }
        attempt = dict(status)
        attempt["handoff_retry"] = {
            "phase": "submitting",
            "delivery_id": delivery_id,
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _write_status_file(status_path, attempt)
        cmd = worker_daemon.build_final_handoff_argv(
            orchestrator_root=orchestrator_root,
            task_ref=persisted_task_ref,
            lane_id=lane_id,
            session=session,
            worktree_path=persisted_worktree,
            result_path=result_path,
            dry_run=False,
            outcome=outcome,
            delivery_id=delivery_id,
            max_turns=max_turns,
        )
        try:
            completed = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                env=env,
                timeout=submit_timeout,
            )
        except subprocess.TimeoutExpired:
            failed = dict(attempt)
            failed["handoff_retry"] = {
                **attempt["handoff_retry"],
                "phase": "ambiguous",
                "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "exit_code": None,
                "timed_out": True,
                "timeout_seconds": submit_timeout,
            }
            try:
                _write_status_file(status_path, failed)
            except OSError:
                # The durable pre-submit record remains ``submitting``. A later
                # retry reuses the receiver-claimed delivery id and converges.
                pass
            return {
                "ok": False,
                "error_code": "handoff_delivery_ambiguous",
                "message": (
                    f"Saved handoff replay for lane '{lane_id}' timed out after "
                    f"{submit_timeout}s; delivery is ambiguous."
                ),
            }
    except Exception as exc:  # noqa: BLE001 - operator control verbs must return structured failures
        return {"ok": False, "message": f"Unable to replay saved handoff for lane '{lane_id}': {exc}"}
    if completed.returncode != 0:
        error = (completed.stderr or completed.stdout or "handoff command failed").strip()
        failed = dict(attempt)
        failed["handoff_retry"] = {
            **attempt["handoff_retry"],
            "phase": "ambiguous",
            "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "exit_code": completed.returncode,
        }
        try:
            _write_status_file(status_path, failed)
        except OSError:
            # The durable pre-submit record remains ``submitting``. A later
            # retry reuses the receiver-claimed delivery id and converges.
            pass
        return {
            "ok": False,
            "error_code": "handoff_delivery_ambiguous",
            "message": f"Saved handoff replay failed for lane '{lane_id}' with exit {completed.returncode}: {error[-500:]}",
        }

    try:
        updated = dict(status)
        updated.update(
            {
                "state": "waiting_for_orchestrator",
                "summary": "Worker handoff submitted successfully; waiting for orchestrator follow-up.",
                "attention_required": False,
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "last_handoff_delivery_id": delivery_id,
            }
        )
        # Exit 0 may only mean the receiver recognized this delivery id; it is
        # not proof that the entire handoff ceremony completed. Retain the
        # saved result so another reconciliation attempt remains possible.
        updated["result_path"] = str(result_path)
        updated.pop("cleanup_pending_result_path", None)
        updated.pop("failure_stage", None)
        updated.pop("pid", None)
        updated.pop("handoff_retry", None)
        _write_status_file(status_path, updated)
    except OSError as exc:
        return {
            "ok": False,
            "message": f"Handoff replay succeeded but status update failed for lane '{lane_id}': {exc}",
        }
    _emit_replayed_event(log_dir, lane_id)
    return {"ok": True, "message": f"Replayed saved handoff for lane '{lane_id}'; waiting for orchestrator follow-up."}


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
