"""Crash-safe, VM-wide ``flock``-backed multi-slot compute semaphore.

Allows multiple off-box LLM lanes to run concurrently while bounding local
compute-heavy bursts (e.g. ``pytest``, ``eslint``, ``tlc2.TLC``) to a fixed
number of slots. Because slot ownership is backed by kernel ``fcntl.flock``
locks on open file descriptions, an abrupt process crash (``SIGKILL``)
releases the slot automatically via the OS kernel (``[CON-11]``, ``[PERF-04]``,
``[SECD-05]``).
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator, Sequence

DEFAULT_COMPUTE_SLOTS: int = 2
COMPUTE_SLOTS_ENV: str = "WORKBAY_VM_COMPUTE_SLOTS"
COMPUTE_SLOT_DIR_ENV: str = "WORKBAY_VM_COMPUTE_SLOT_DIR"
_DEFAULT_TMP_SLOT_DIR = Path("/tmp/workbay-vm-compute-slots")


def resolve_slot_dir(slot_dir: Path | str | None = None) -> Path:
    """Resolve the compute-semaphore lock directory.

    Precedence:
    1. Explicit ``slot_dir`` argument.
    2. ``WORKBAY_VM_COMPUTE_SLOT_DIR`` environment variable (if non-empty).
    3. ``$XDG_RUNTIME_DIR/workbay-compute-slots`` (if ``XDG_RUNTIME_DIR`` is non-empty).
    4. ``/tmp/workbay-vm-compute-slots``.
    """
    if slot_dir is not None:
        return Path(slot_dir)
    env_dir = os.environ.get(COMPUTE_SLOT_DIR_ENV, "").strip()
    if env_dir:
        return Path(env_dir)
    xdg_runtime = os.environ.get("XDG_RUNTIME_DIR", "").strip()
    if xdg_runtime:
        return Path(xdg_runtime) / "workbay-compute-slots"
    return _DEFAULT_TMP_SLOT_DIR


def resolve_compute_slots(slots: int | None = None) -> int:
    """Resolve and validate the compute slot count (must be an integer >= 1)."""
    if slots is None:
        raw = os.environ.get(COMPUTE_SLOTS_ENV, "").strip()
        if raw:
            try:
                resolved = int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"{COMPUTE_SLOTS_ENV} must be a positive integer, got {raw!r}"
                ) from exc
        else:
            resolved = DEFAULT_COMPUTE_SLOTS
    else:
        if isinstance(slots, bool) or not isinstance(slots, int):
            raise ValueError(f"slots must be an integer >= 1, got {slots!r}")
        resolved = slots

    if resolved < 1:
        raise ValueError(f"slots must be >= 1, got {resolved}")
    return resolved


def _try_lock_slot(lock_path: Path) -> int | None:
    """Open ``lock_path`` and attempt a non-blocking exclusive ``flock``.

    Returns the open file descriptor on success, or ``None`` if the slot is
    currently held by another open file description. Never leaks file
    descriptors on contention or error.
    """
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(lock_path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
            return None
        raise
    except BaseException:
        os.close(fd)
        raise
    return fd


@contextlib.contextmanager
def acquire_compute_slot(
    *,
    slot_dir: Path | str | None = None,
    slots: int | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.02,
) -> Iterator[int]:
    """Acquire an exclusive compute slot backed by ``fcntl.flock``.

    Yields the acquired slot index ``i`` (``0 <= i < slots``). Lock files
    ``slot-{i}.lock`` are intentionally left on disk on release so concurrent
    waiters always lock the same inode (``[CON-11]``).
    """
    resolved_slots = resolve_compute_slots(slots)
    if timeout_s is not None and timeout_s < 0:
        raise ValueError(f"timeout_s must be >= 0 or None, got {timeout_s!r}")
    if poll_interval_s <= 0:
        raise ValueError(f"poll_interval_s must be > 0, got {poll_interval_s!r}")

    resolved_dir = resolve_slot_dir(slot_dir)
    resolved_dir.mkdir(parents=True, exist_ok=True)

    deadline = None if timeout_s is None else (time.monotonic() + timeout_s)
    acquired_fd: int | None = None
    acquired_slot: int | None = None

    while acquired_fd is None:
        for slot_idx in range(resolved_slots):
            lock_path = resolved_dir / f"slot-{slot_idx}.lock"
            fd = _try_lock_slot(lock_path)
            if fd is not None:
                acquired_fd = fd
                acquired_slot = slot_idx
                break

        if acquired_fd is not None:
            break

        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out after {timeout_s}s waiting for a compute slot "
                    f"in {resolved_dir} (slots={resolved_slots})"
                )
            time.sleep(min(poll_interval_s, remaining))
        else:
            time.sleep(poll_interval_s)

    assert acquired_slot is not None
    try:
        yield acquired_slot
    finally:
        try:
            fcntl.flock(acquired_fd, fcntl.LOCK_UN)
        finally:
            os.close(acquired_fd)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m workbay_orchestrator_mcp.orchestration.vm_compute_semaphore",
        description=(
            "Run a command while holding an exclusive VM-wide flock compute slot."
        ),
    )
    parser.add_argument(
        "--slots",
        type=int,
        default=None,
        help=(
            f"Number of compute slots (default: ${COMPUTE_SLOTS_ENV} or "
            f"{DEFAULT_COMPUTE_SLOTS})."
        ),
    )
    parser.add_argument(
        "--slot-dir",
        type=str,
        default=None,
        help=(
            f"Directory for slot lock files (default: ${COMPUTE_SLOT_DIR_ENV}, "
            f"$XDG_RUNTIME_DIR/workbay-compute-slots, or {_DEFAULT_TMP_SLOT_DIR})."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Maximum seconds to wait for a free slot before failing.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint: acquire a compute slot, run ``<cmd> [args...]``, return exit code."""
    raw_args = list(sys.argv[1:] if argv is None else argv)

    if "--" in raw_args:
        sep = raw_args.index("--")
        flag_args = raw_args[:sep]
        cmd_args = raw_args[sep + 1 :]
    else:
        flag_args = raw_args
        cmd_args = []

    parser = _build_parser()
    try:
        parsed = parser.parse_args(flag_args)
    except SystemExit as exc:
        code = exc.code
        return code if isinstance(code, int) else 2

    if not cmd_args:
        print(
            "vm_compute_semaphore: missing command after '--' (usage: ... -- <cmd> [args...])",
            file=sys.stderr,
        )
        return 2

    try:
        with acquire_compute_slot(
            slot_dir=parsed.slot_dir,
            slots=parsed.slots,
            timeout_s=parsed.timeout,
        ):
            proc = subprocess.run(cmd_args, check=False)
    except ValueError as exc:
        print(f"vm_compute_semaphore: invalid configuration: {exc}", file=sys.stderr)
        return 2
    except TimeoutError as exc:
        print(f"vm_compute_semaphore: {exc}", file=sys.stderr)
        return 124
    except FileNotFoundError as exc:
        print(f"vm_compute_semaphore: command not found: {exc}", file=sys.stderr)
        return 127
    except PermissionError as exc:
        print(f"vm_compute_semaphore: permission denied: {exc}", file=sys.stderr)
        return 126

    if proc.returncode < 0:
        return 128 + (-proc.returncode)
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
