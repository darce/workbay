"""Timed-subprocess gateway (implementation note S5)."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

DEFAULT_TIMEOUTS: dict[str, int] = {
    "git": 120,
    "generator": 120,
    "uv_sync": 300,
    "uvx_prewarm": 300,
    "uv_tool_install_gitonly_mcp": 300,
    "grok_cli": 120,
    "handoff_cli": 120,
    "probe": 30,
}

BEST_EFFORT_CALL_CLASSES = frozenset(
    {"uv_sync", "uvx_prewarm", "uv_tool_install_gitonly_mcp"}
)

# Bound on reaping an already-killed child. Keeps the post-kill drain from
# wedging when the killpg fallback path left a grandchild holding pipe FDs.
_REAP_TIMEOUT_SECONDS = 5

# Guarded by _offline_latch_lock: install/repair can run concurrently inside a
# long-lived MCP server process, and an unsynchronized read-modify cycle lets
# one caller's reset clear another caller's trip.
_offline_latch: bool = False
_offline_latch_lock = threading.Lock()


class ExternalCallTimeout(subprocess.TimeoutExpired):
    """Raised when an external call exceeds its configured class timeout."""

    def __init__(
        self,
        *,
        call_class: str,
        cmd: Sequence[str],
        timeout: int,
    ) -> None:
        self.call_class = call_class
        super().__init__(cmd, timeout)
        self.failure_class = "system"


@dataclass(frozen=True)
class DeferredExternalCall(RuntimeError):
    """Best-effort call skipped after the offline latch tripped."""

    call_class: str
    reason: str = "offline"

    @property
    def failure_class(self) -> str:
        return "system"


def reset_offline_latch() -> None:
    global _offline_latch
    with _offline_latch_lock:
        _offline_latch = False


def offline_latch_active() -> bool:
    with _offline_latch_lock:
        return _offline_latch


def timeout_for_call_class(call_class: str, *, override: int | None = None) -> int:
    if override is not None:
        return override
    env_key = f"WORKBAY_TIMEOUT_{call_class.upper()}"
    raw = os.environ.get(env_key, "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(
                f"invalid integer timeout override {env_key}={raw!r}"
            ) from exc
    try:
        return DEFAULT_TIMEOUTS[call_class]
    except KeyError as exc:
        raise ValueError(f"unknown external call_class={call_class!r}") from exc


def _trip_offline_latch(exc: BaseException) -> None:
    global _offline_latch
    if isinstance(exc, (DeferredExternalCall,)):
        return
    if isinstance(exc, (ExternalCallTimeout, subprocess.CalledProcessError, OSError)):
        with _offline_latch_lock:
            _offline_latch = True


def _kill_process_tree(proc: subprocess.Popen[Any], *, use_group: bool) -> None:
    """Kill a timed-out child and, when session-isolated, its whole group.

    Plain ``Popen.kill`` only reaps the direct child; a hanging tool that
    spawned grandchildren (e.g. a wrapper script exec-ing git) would leak them
    past the timeout and keep holding locks.
    """
    if use_group:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            proc.kill()
    else:
        proc.kill()


def run_external(
    cmd: Sequence[str],
    *,
    call_class: str,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = True,
    capture_output: bool = False,
    text: bool = False,
    timeout_override: int | None = None,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    """Run a subprocess with per-class timeout policy and offline latch."""
    if call_class in BEST_EFFORT_CALL_CLASSES and _offline_latch:
        raise DeferredExternalCall(call_class=call_class)

    timeout = timeout_for_call_class(call_class, override=timeout_override)
    argv = list(cmd)
    # POSIX: run the child in its own session so a timeout can reap the whole
    # process tree (grandchildren included), not just the direct child.
    use_group = os.name == "posix"
    try:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            text=text,
            start_new_session=use_group,
        )
    except OSError as exc:
        if call_class in BEST_EFFORT_CALL_CLASSES:
            _trip_offline_latch(exc)
        raise
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc, use_group=use_group)
        try:
            # Bounded reap: when killpg fell back to plain kill(), a surviving
            # grandchild can keep the stdout/stderr pipes open and an untimed
            # communicate() would block forever — defeating the class timeout
            # this reap exists to enforce.
            proc.communicate(timeout=_REAP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()
            try:
                proc.wait(timeout=_REAP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                pass  # direct child was SIGKILLed; do not wedge on a zombie
        timeout_exc = ExternalCallTimeout(
            call_class=call_class, cmd=argv, timeout=timeout
        )
        if call_class in BEST_EFFORT_CALL_CLASSES:
            _trip_offline_latch(timeout_exc)
        raise timeout_exc from None
    completed = subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)
    if proc.returncode != 0:
        failure = subprocess.CalledProcessError(
            proc.returncode, argv, output=stdout, stderr=stderr
        )
        if call_class in BEST_EFFORT_CALL_CLASSES:
            _trip_offline_latch(failure)
        if check:
            raise failure
    return completed