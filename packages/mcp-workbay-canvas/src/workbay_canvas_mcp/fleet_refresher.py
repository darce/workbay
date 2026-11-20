"""Fleet's single writer: a timer and one bounded, disposable collector child.

A process and JSON round trip per tick buy collection that shutdown can kill.
HTTP handlers remain readers; only this parent publishes collection results.
"""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_orchestrator_mcp.orchestration.fleet_snapshot import (
    NATIVE_RETIRE_FAILED_EXIT,
    acquire_snapshot_writer,
    release_snapshot_writer,
    write_snapshot_cache,
)

logger = logging.getLogger(__name__)
# SIGKILL cannot be handled, so the collector first gets SIGTERM and this long
# to SIGKILL the native groups it started in their own sessions (fleet_cli
# retires them in milliseconds). It fits stop()'s 10 s and a timed-out tick's
# bound; a collector that ignores SIGTERM is still SIGKILLed after it.
STOP_GRACE_S = 2.0


def _document(data):
    def invalid_constant(value):
        raise ValueError(f"non-finite JSON number: {value}")

    document = json.loads(data, parse_constant=invalid_constant)
    if (
        not isinstance(document, dict)
        or type(document.get("schema_version")) is not int
        or document["schema_version"] != 1
    ):
        raise ValueError("expected a schema_version 1 JSON object")
    return document


def _signal_group(child, signum):
    # Even an exited leader can have descendants still holding its output pipes.
    # Only ESRCH means the group is gone; any other failure reaches the caller.
    try:
        os.killpg(child.pid, signum)
    except ProcessLookupError:
        pass


def _stop_failed(child):
    return child.returncode == NATIVE_RETIRE_FAILED_EXIT


class FleetRefresher:
    def __init__(self, root, *, interval_s=60.0, timeout_s=None, collector_argv=None):
        self.root = Path(root)
        self.interval_s = float(interval_s)
        self.timeout_s = min(30.0, self.interval_s * 0.8) if timeout_s is None else float(timeout_s)
        if not math.isfinite(self.interval_s) or self.interval_s <= 0:
            raise ValueError("interval_s must be finite and positive")
        if not math.isfinite(self.timeout_s) or self.timeout_s <= 0:
            raise ValueError("timeout_s must be finite and positive")
        self.collector_argv = (
            list(collector_argv)
            if collector_argv is not None
            else [
                sys.executable,
                "-m",
                "workbay_orchestrator_mcp.orchestration.fleet_cli",
                "--json",
                "--no-cache",
                "--root",
                str(self.root),
            ]
        )
        self._tick_lock = threading.Lock()
        self._publish_lock = threading.RLock()
        self._stop = threading.Event()
        self._closed = False
        self._thread = None
        self._child = None
        self._lease = None

    def start(self):
        """Hold the writer lease and schedule ticks; collection never blocks start."""
        with self._publish_lock:
            if self._closed:
                raise RuntimeError("Fleet refresher is stopped")
            if self._thread is not None:
                return
            if self._lease is None:
                self._lease = acquire_snapshot_writer(self.root)
            self._thread = threading.Thread(target=self._run_timer, name="fleet-refresher", daemon=True)
            try:
                self._thread.start()
            except BaseException:
                self._thread = None
                release_snapshot_writer(self._lease)
                self._lease = None
                raise

    def _run_timer(self):
        next_tick = time.monotonic() + self.interval_s
        while not self._stop.wait(max(0.0, next_tick - time.monotonic())):
            try:
                self.tick()
            except Exception:
                # Publication/storage faults must be visible too, not a silent
                # death of the server's freshness mechanism.
                logger.exception("Fleet refresh failed to publish")
            next_tick += self.interval_s
            now = time.monotonic()
            if next_tick < now:
                # Skip elapsed slots instead of bursting after a slow tick.
                next_tick += (int((now - next_tick) / self.interval_s) + 1) * self.interval_s

    def stop(self):
        """Close publication first, retire collection, then release the lease.

        The collector gets SIGTERM and STOP_GRACE_S to retire what it started,
        then its group is SIGKILLed and reaped. A failed kill, or a collector
        reporting a failed retirement, is raised after the lease is released.
        """
        failure = None
        with self._publish_lock:
            self._closed = True
            self._stop.set()
            child, thread = self._child, self._thread
        try:
            if child is not None:
                try:
                    _signal_group(child, signal.SIGTERM)
                    child.wait(timeout=STOP_GRACE_S)
                except subprocess.TimeoutExpired:
                    pass
                except OSError as exc:
                    failure = exc
                _signal_group(child, signal.SIGKILL)
                child.wait(timeout=3)
                if failure is None and _stop_failed(child):
                    failure = RuntimeError(
                        f"Fleet collector stop failed: exit {child.returncode}; see its refresh error"
                    )
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=4)
        finally:
            with self._publish_lock:
                if self._lease is not None:
                    release_snapshot_writer(self._lease)
                    self._lease = None
        if failure is not None:
            raise failure

    def _prior_observation(self):
        path = RuntimeConfig.for_repo(self.root).state_dir / "fleet" / "snapshot.json"
        try:
            # read_snapshot_cache is an age-derived view, never a publishable
            # observation. Keep unknown/extension fields in the raw document.
            return _document(path.read_bytes())
        except (OSError, ValueError, UnicodeError, RecursionError):
            return {"schema_version": 1, "observed_at": None, "state": "unknown"}

    def _publish(self, observation, refresh):
        with self._publish_lock:
            if self._closed:
                return False
            write_snapshot_cache(self.root, {**observation, "refresh": refresh})
            return True

    def tick(self):
        """Collect synchronously once; overlapping calls do no work."""
        if not self._tick_lock.acquire(blocking=False):
            return "skipped"
        try:
            with self._publish_lock:
                if self._closed:
                    return "skipped"
                if self._lease is None:
                    self._lease = acquire_snapshot_writer(self.root)
                prior = self._prior_observation()
                started = datetime.now(timezone.utc)
                refresh = {
                    "state": "running",
                    "interval_s": self.interval_s,
                    "started_at": started.isoformat(),
                    "ended_at": None,
                    "error": None,
                }
                self._publish(prior, refresh)
                try:
                    # No inherited lease fd, and no arguments appended to a
                    # supplied argv. The default collector uses --no-cache.
                    child = subprocess.Popen(
                        self.collector_argv,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        start_new_session=True,
                    )
                    self._child = child
                except (OSError, ValueError) as exc:
                    child = None
                    error = f"collector_failed: {exc}"
            document = prior
            kill_failure = None
            if child is not None:
                try:
                    stdout, stderr = child.communicate(timeout=self.timeout_s)
                    if child.returncode:
                        error = f"collector_failed: exit {child.returncode}: {stderr.decode('utf-8', errors='replace').strip()}"
                    else:
                        try:
                            document = _document(stdout)
                            error = None
                        except (ValueError, UnicodeError, RecursionError) as exc:
                            error = f"invalid_document: {exc}"
                except subprocess.TimeoutExpired:
                    error = f"timeout: collector exceeded {self.timeout_s:g}s"
                    try:
                        # Bounded: the finally SIGKILLs without reading again.
                        _signal_group(child, signal.SIGTERM)
                        _, stderr = child.communicate(timeout=STOP_GRACE_S)
                        if _stop_failed(child):
                            error += (
                                f"; stop: exit {child.returncode}: {stderr.decode('utf-8', errors='replace').strip()}"
                            )
                    except subprocess.TimeoutExpired:
                        pass
                    except OSError as exc:
                        kill_failure = exc
                        error += f"; stop: {type(exc).__name__}: {exc}"
                except OSError as exc:
                    error = f"collector_failed: {exc}"
                finally:
                    try:
                        # Retire descendants even if the leader exited normally.
                        _signal_group(child, signal.SIGKILL)
                    except OSError as exc:
                        kill_failure = exc
                        error = f"{error or 'collector_failed'}; kill: {type(exc).__name__}: {exc}"
                    try:
                        child.wait(timeout=2)
                    finally:
                        # Never wait again for EOF after a timeout: an escaped
                        # descendant could retain a copy of an output pipe.
                        child.stdout.close()
                        child.stderr.close()
                        with self._publish_lock:
                            self._child = None
            state = "error" if error else "partial" if document.get("partial") is True else "ok"
            published = self._publish(
                prior if error else document,
                {
                    **refresh,
                    "state": state,
                    "ended_at": max(started, datetime.now(timezone.utc)).isoformat(),
                    "error": error,
                },
            )
            if kill_failure is not None and not published:
                raise kill_failure  # stop() closed publication; never swallow it
            return state
        finally:
            with self._publish_lock:
                # A standalone tick owns a lease only for its duration.
                if self._thread is None and self._lease is not None:
                    release_snapshot_writer(self._lease)
                    self._lease = None
            self._tick_lock.release()
