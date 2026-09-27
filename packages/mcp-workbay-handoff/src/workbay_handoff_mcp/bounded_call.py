"""Small runtime seam for abandonable, wall-clock-bounded calls."""

from __future__ import annotations

import atexit
import math
import os
import sys
import threading
import time
import weakref
from collections.abc import Callable
from typing import TextIO, TypeVar

_ResultT = TypeVar("_ResultT")

_ATEXIT_JOIN_BUDGET_SECONDS = 0.5
_abandoned_threads: weakref.WeakSet[threading.Thread] = weakref.WeakSet()
_abandoned_lock = threading.Lock()


def abandoned_thread_count() -> int:
    """Return how many timed-out daemon workers are still alive.

    Used by the doctor and by tests. Finished workers drop out of the
    WeakSet once nothing else holds them; this count only includes threads
    that have not yet exited.
    """
    with _abandoned_lock:
        return sum(1 for thread in list(_abandoned_threads) if thread.is_alive())


def _mark_abandoned(thread: threading.Thread) -> None:
    with _abandoned_lock:
        _abandoned_threads.add(thread)


def _redirect_stdio_to_devnull() -> None:
    # Detach live stdio BEFORE any flush. A full pipe-backed stderr (or a
    # stream whose lock is held by an abandoned writer) can block forever
    # in flush(); hanging here leaves interpreter exit unbounded (LLEMB02RV-001).
    # Do not flush the old streams at all: a daemon flush of a lock held by
    # the abandoned writer is the CPython "could not acquire a buffered-IO
    # lock" SIGABRT during finalization. Losing their buffers at exit is
    # acceptable; hanging or aborting is not (RES-02).
    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
    except OSError:
        return
    try:
        for fd in (1, 2):
            try:
                os.dup2(devnull_fd, fd)
            except OSError:
                pass
        devnull: TextIO = os.fdopen(devnull_fd, "w", encoding="utf-8", errors="replace")
    except Exception:
        try:
            os.close(devnull_fd)
        except OSError:
            pass
        return
    sys.stdout = devnull
    sys.stderr = devnull
    try:
        sys.__stdout__ = devnull
        sys.__stderr__ = devnull
    except Exception:
        pass


def _join_abandoned_threads_at_exit() -> None:
    with _abandoned_lock:
        alive = [thread for thread in list(_abandoned_threads) if thread.is_alive()]
    if not alive:
        return
    deadline = time.monotonic() + _ATEXIT_JOIN_BUDGET_SECONDS
    for thread in alive:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        thread.join(timeout=remaining)
    if any(thread.is_alive() for thread in alive):
        _redirect_stdio_to_devnull()


atexit.register(_join_abandoned_threads_at_exit)


def run_with_daemon_timeout(
    fn: Callable[[], _ResultT],
    *,
    timeout_seconds: float,
    timeout_message: str | None = None,
) -> _ResultT:
    """Run a zero-argument callable in an abandonable daemon worker.

    The caller waits at most ``timeout_seconds`` for the result. A timeout
    raises ``TimeoutError`` without joining the worker; any exception raised by
    the callable is re-raised in the caller thread once the worker finishes.
    """
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout <= 0.0:
        raise ValueError(f"timeout_seconds must be a finite positive number, got {timeout_seconds!r}")

    result: list[_ResultT] = []
    error: list[BaseException] = []
    done = threading.Event()

    def _run() -> None:
        try:
            result.append(fn())
        except BaseException as exc:
            error.append(exc)
        finally:
            done.set()

    worker = threading.Thread(target=_run, name="workbay-bounded-call", daemon=True)
    worker.start()
    if not done.wait(timeout=timeout):
        _mark_abandoned(worker)
        message = timeout_message or f"call timed out after {timeout_seconds}s"
        raise TimeoutError(message)
    if error:
        raise error[0]
    return result[0]
