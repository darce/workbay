"""Bounded wake for a review sweep already published by the merge hook."""

from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

_MODULE = "workbay_orchestrator_mcp.orchestration.post_merge_review_worker"
_SCAN_SECONDS = 20.0
_ATTEMPT_SECONDS = 23.0  # Allow the process owner to settle native Git children.
_CANCEL_GRACE_SECONDS = 5.0
_WAKE_SECONDS = 75.0
_RETRY = 75


def dispatch_review_sweep(root: Path) -> None:
    """Start at most one wake per review scope while its retry loop is active."""
    if os.environ.get("WORKBAY_POST_MERGE_RETIRE_SWEEP") == "0" or os.environ.get(
        "WORKBAY_DISABLE_AUTO_REAP", ""
    ).strip().lower() in {"1", "true", "yes"}:
        return
    if sys.platform != "darwin" and (
        sys.platform != "linux"
        or not hasattr(os, "waitid")
        or not Path(f"/proc/self/task/{os.getpid()}/children").is_file()
    ):
        raise RuntimeError("bounded review continuation requires macOS or the Linux process owner")

    from workbay_handoff_mcp.runtime import get_runtime_config

    from .worker_lock import try_acquire_worker_lock

    runtime = get_runtime_config()
    scope = hashlib.sha256(os.fsencode(root)).hexdigest()
    lock = try_acquire_worker_lock(runtime.db_path.with_suffix(f".post-merge-review-{scope}.wake.lock"))
    if lock is None:
        return
    # Transfer the flock via its open file description. Closing our descriptor
    # leaves the supervisor's claim intact. Explicitly unlocking would not.
    with lock:
        fd = lock.fileno()
        deadline = time.monotonic() + _WAKE_SECONDS
        subprocess.Popen(
            [sys.executable, "-P", "-m", _MODULE, str(root), str(runtime.state_dir), str(deadline), str(fd)],
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            pass_fds=(fd,),
        )


def _cooperative_attempt(
    command: list[str], root: Path, attempt_deadline: float, deadline: float, wake_fd: int | None
) -> int | None:
    """Track the native scan while it owns its Git subprocesses."""
    process = subprocess.Popen(command, cwd=root, pass_fds=() if wake_fd is None else (wake_fd,))
    try:
        status = process.wait(timeout=max(0.0, attempt_deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        # The scan unwinds through the native Git wrappers. Killing it would
        # bypass cleanup of mutation groups started in their own sessions.
        process.terminate()
        try:
            status = process.wait(timeout=min(_CANCEL_GRACE_SECONDS, max(0.0, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            return None
    # A signal death or unexpected failure is not a settlement receipt. The
    # scan retains its inherited wake claim while alive, and Git retains the
    # native mutation fence according to the reaper's existing custody rules.
    return status if status in (0, _RETRY) else None


def _supervise(root: Path, state_dir: Path, deadline: float, fd: int) -> int:
    """Run up to three review-only attempts in one finite window."""

    os.set_inheritable(fd, False)

    def attempt(wake_fd: int | None = None) -> int | None:
        now = time.monotonic()
        timeout = min(_ATTEMPT_SECONDS, deadline - now)
        if timeout <= 2:
            return _RETRY
        scan_deadline = now + min(_SCAN_SECONDS, timeout - 2)
        command = [sys.executable, "-P", "-m", _MODULE, str(root), str(state_dir), str(scan_deadline)]
        if sys.platform == "darwin":
            return _cooperative_attempt(command, root, now + timeout, deadline, wake_fd)

        from .adapters.remote_exec import _default_remote_runner

        try:
            result = _default_remote_runner(command, cwd=str(root), env=None, timeout=timeout)
        except subprocess.TimeoutExpired:
            # The owner confirmed settlement, including escaped descendants.
            return _RETRY
        return result.returncode

    with os.fdopen(fd, "r"):
        for delay in (1.0, 2.0):
            status = attempt(fd)
            if status is None:
                return _RETRY
            if status == 0:
                break
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
    # Recheck through the shared consumer after dropping the wake flock. A
    # merge that coalesced while we were finishing can still be pending, and a
    # merge arriving from here on can dispatch its own wake. The native
    # consumer's lock still serializes this pass with every other consumer.
    status = attempt()
    return _RETRY if status is None else status


class _ReviewTimeout(BaseException):
    """Unwind native process ownership even through best-effort handlers."""


def _scan(root: Path, state_dir: Path, deadline: float) -> int:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return _RETRY
    expiring = False

    def expire(_signum: int, _frame: object) -> None:
        nonlocal expiring
        if not expiring:
            expiring = True
            raise _ReviewTimeout

    # A second signal must allow the first cancellation to finish unwinding.
    signals = {signal.SIGALRM, signal.SIGTERM}
    try:
        for signum in signals:
            signal.signal(signum, expire)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, deadline - time.monotonic()))
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        from .post_merge_reap import consume_review_sweep, review_sweep_complete
        from .review_stage_reap import reap_review_stage_worktrees

        runtime = RuntimeConfig.for_repo(root, state_dir=state_dir)
        if runtime.workspace_root != root:
            return _RETRY
        configure_runtime(runtime)
        result = consume_review_sweep(
            root,
            deadline=deadline,
            run=lambda schedule: reap_review_stage_worktrees(
                root, apply=True, deadline=deadline, schedule_candidates=schedule
            ),
        )
    except _ReviewTimeout:
        return _RETRY
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
    # None can be an empty request or contention with foreground/maintenance.
    # A bounded retry handles either without inspecting the pending row here.
    return 0 if isinstance(result, dict) and review_sweep_complete(result) else _RETRY


if __name__ == "__main__":
    repo, state, until, *lock_fd = sys.argv[1:]
    if lock_fd:
        sys.exit(_supervise(Path(repo), Path(state), float(until), int(lock_fd[0])))
    sys.exit(_scan(Path(repo), Path(state), float(until)))
