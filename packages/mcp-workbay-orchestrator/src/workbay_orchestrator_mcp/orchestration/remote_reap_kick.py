"""Coalesce daemon-free reap requests into one detached maintenance child."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .lane_census import (
    REMOTE_REAP_INTERVAL_S,
    _env_nonnegative_float,
    _read_remote_reap_stamp,
    _remote_reap_gauge,
    remote_sandbox_reap_due,
    run_remote_sandbox_reap_maintenance,
)

KICK_LOG_MAX_BYTES = 1_048_576
CHILD_LOCK_FILENAME = "remote-sandbox-reap-kick-child.lock"
PID_FILENAME = "remote-sandbox-reap-kick.pid"
RECEIPT_FILENAME = "remote-sandbox-reap-kick-receipt.json"
_logger = logging.getLogger(__name__)


def _payload(
    outcome: str,
    *,
    skip_reason: str | None = None,
    error: str | None = None,
    gauge: Any = None,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "skip_reason": skip_reason,
        "reaped": 0,
        "gauge": _remote_reap_gauge({"gauge": gauge}),
        "error": error,
    }


def kick_remote_sandbox_reap(
    *,
    root: Path | str,
    state_dir: Path | str,
    task_ref: str,
    reason: str,
    spawner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Start at most one child, doing only local, nonblocking work in the parent.

    The kick lock protects child-lock acquisition and spawn. The child inherits
    ownership until it exits; the pid is diagnostic only. The existing
    maintenance lock still serializes actual remote reclamation.
    """
    if os.environ.get("WORKBAY_REMOTE_REAP_KICK", "1").strip() == "0":
        return _payload("skipped", skip_reason="kick_disabled")
    interval = _env_nonnegative_float("WORKBAY_REMOTE_REAP_INTERVAL_SEC", REMOTE_REAP_INTERVAL_S)
    if interval == 0:
        return _payload("skipped", skip_reason="disabled")
    if not isinstance(task_ref, str) or not task_ref.strip():
        return _payload("skipped_no_task", skip_reason="task_ref_empty")
    try:
        directory = Path(state_dir).expanduser().resolve()
        workspace = Path(root).expanduser().resolve()
        # Validate the explicit attribution in the child's state, without
        # rebinding the caller's runtime or taking either single-flight lock.
        # An unresolved kick must leave a later concrete-task kick free to run.
        try:
            from workbay_handoff_mcp.shared_schema import connect_handoff_db  # noqa: PLC0415

            with closing(
                connect_handoff_db(directory / "handoff.db", read_only=True, deadline=time.monotonic() + 0.1)
            ) as conn:
                task = conn.execute("SELECT 1 FROM handoff_state WHERE task_ref = ? LIMIT 1", (task_ref,)).fetchone()
        except Exception as exc:  # noqa: BLE001 — inability to prove context must fail closed
            _logger.warning("Remote sandbox reap kick task lookup failed: %s: %s", type(exc).__name__, exc)
            return _payload("skipped_no_task", skip_reason="task_lookup_failed")
        if task is None:
            return _payload("skipped_no_task", skip_reason="task_ref_unresolved")
        stamp = _read_remote_reap_stamp(directory)
        if stamp is None:
            return _payload("failed", skip_reason="stamp_unreadable", error="remote_sandbox_reap_stamp_unreadable")
        if not remote_sandbox_reap_due(directory, now=time.time(), interval_seconds=interval):
            return _payload("skipped", skip_reason="cadence_not_due", gauge=stamp.get("last_gauge"))
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "remote-sandbox-reap-kick.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return _payload("skipped", skip_reason="kick_in_progress")
            with (directory / CHILD_LOCK_FILENAME).open("a") as child_lock:
                try:
                    fcntl.flock(child_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return _payload("skipped", skip_reason="child_alive")
                # Close only our copy after spawning: LOCK_UN would also release
                # the child's shared open file description. Kernel close on
                # child exit/crash releases ownership, independent of PID reuse.
                pid_file = directory / PID_FILENAME
                argv = [
                    sys.executable,
                    "-m",
                    "workbay_orchestrator_mcp.orchestration.remote_reap_kick",
                    "--root",
                    str(workspace),
                    "--state-dir",
                    str(directory),
                    "--task-ref",
                    task_ref,
                    "--reason",
                    reason,
                ]
                # One replaced log and receipt per state_dir; repeated passes do not
                # accumulate per-kick artifacts. No remote operation runs here.
                with (directory / "remote-sandbox-reap-kick.out").open("w") as log:
                    child = (spawner or subprocess.Popen)(
                        argv,
                        cwd=workspace,
                        stdin=subprocess.DEVNULL,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                        close_fds=True,
                        pass_fds=(child_lock.fileno(),),
                    )
                _write_json(pid_file, int(child.pid))
                return {**_payload("spawned"), "pid": child.pid, "reason": reason}
    except Exception as exc:  # noqa: BLE001 — a failed kick must never undo closure
        error = f"{type(exc).__name__}: {exc}"[:1000]
        _logger.warning("Remote sandbox reap kick failed: %s", error)
        return _payload("failed", skip_reason="kick_failed", error=error)


def _write_json(path: Path, payload: dict[str, Any] | int) -> None:
    """Replace receipts and pids atomically so interrupted writes cannot wedge kicks."""
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=directory, prefix=".remote-reap-kick-", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None, *, maintenance_runner: Callable[..., Any] | None = None) -> int:
    """Run maintenance in the child and leave a typed local receipt."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--reason", default="kick")
    args = parser.parse_args(argv)
    task_ref = args.task_ref
    try:
        handler = RotatingFileHandler(
            args.state_dir / "remote-sandbox-reap-kick.log",
            maxBytes=KICK_LOG_MAX_BYTES,
            backupCount=1,
            encoding="utf-8",
        )
    except OSError:
        logging.basicConfig(level=logging.INFO)
    else:
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)

    def log(level: str, event: str, **fields: Any) -> None:
        _logger.log(getattr(logging, level.upper(), logging.INFO), "%s %s", event, fields)

    try:
        # A detached interpreter has no in-process handoff runtime. Bind its
        # authorization reads and decision writes to the same state as the kick.
        from workbay_handoff_mcp import configure_runtime  # noqa: PLC0415
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        configure_runtime(RuntimeConfig.for_workspace(args.root, state_dir=args.state_dir))
        if not task_ref:
            from workbay_handoff_mcp.shared_primitives import resolve_active_task_ref  # noqa: PLC0415
            from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

            with _get_db_connection() as conn:
                task_ref = resolve_active_task_ref(conn)
        raw = (maintenance_runner or run_remote_sandbox_reap_maintenance)(
            task_ref,
            root=args.root,
            state_dir=args.state_dir,
            apply=True,
            log=log,
        )
        errors = [str(raw[key]) for key in ("error", "stamp_error", "harvest_error", "evidence_error") if raw.get(key)]
        error = "; ".join(errors)[:1000] or None
        outcome = "failed" if error else "skipped" if raw.get("skipped") else "completed"
        payload = {
            **raw,
            **_payload(outcome, skip_reason=raw.get("skip_reason"), error=error),
            "reaped": int(raw.get("reaped") or 0),
            "gauge": _remote_reap_gauge(raw),
        }
    except Exception as exc:  # noqa: BLE001 — even an unexpected runner failure gets a receipt
        payload = _payload("failed", skip_reason="maintenance_exception", error=f"{type(exc).__name__}: {exc}"[:1000])
    payload.update(reason=args.reason, task_ref=task_ref)
    try:
        _write_json(args.state_dir / RECEIPT_FILENAME, payload)
    except Exception:  # noqa: BLE001 — failure must remain visible in the child log
        _logger.exception("Remote sandbox reap kick receipt failed: %s", payload)
        return 1
    if payload["error"]:
        _logger.warning("Remote sandbox reap kick degraded: %s", payload)
    return 1 if payload["outcome"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
