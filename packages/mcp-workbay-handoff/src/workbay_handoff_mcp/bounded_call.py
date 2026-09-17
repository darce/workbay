"""Small runtime seam for abandonable, wall-clock-bounded calls."""

from __future__ import annotations

import math
import threading
from collections.abc import Callable
from typing import TypeVar

_ResultT = TypeVar("_ResultT")


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
        message = timeout_message or f"call timed out after {timeout_seconds}s"
        raise TimeoutError(message)
    if error:
        raise error[0]
    return result[0]
