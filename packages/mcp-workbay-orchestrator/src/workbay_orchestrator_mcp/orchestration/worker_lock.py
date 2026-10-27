"""Kernel-flock protocol for per-lane worker lock files."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import stat
import time
from pathlib import Path
from typing import TextIO

_logger = logging.getLogger(__name__)
_DEFAULT_STALE_LOCK_AGE_SECONDS = 24 * 60 * 60
_INODE_RECHECK_ATTEMPTS = 3
_OWNED_HANDLES: dict[int, tuple[int, TextIO]] = {}
_OWNED_HANDLES_PID = os.getpid()


def _descriptor_is_open(handle: TextIO) -> bool:
    if handle.closed:
        return False
    try:
        os.fstat(handle.fileno())
    except (OSError, ValueError):
        return False
    return True


def _owned_handles_for_current_process() -> dict[int, tuple[int, TextIO]]:
    """Return this process's live ownership handles, dropping stale entries."""
    global _OWNED_HANDLES_PID

    current_pid = os.getpid()
    if _OWNED_HANDLES_PID != current_pid:
        # A fork copies module globals. Do not inherit the parent's ownership
        # registry as authority in the child process.
        _OWNED_HANDLES.clear()
        _OWNED_HANDLES_PID = current_pid

    for handle_id, (owner_pid, handle) in tuple(_OWNED_HANDLES.items()):
        if owner_pid != current_pid or not _descriptor_is_open(handle):
            _OWNED_HANDLES.pop(handle_id, None)
    return _OWNED_HANDLES


def _same_path_inode(handle: TextIO, path: Path) -> bool:
    try:
        opened = os.fstat(handle.fileno())
        named = path.stat()
    except (OSError, ValueError):
        return False
    return (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino)


def try_acquire_worker_lock(path: Path | str) -> TextIO | None:
    """Take a non-blocking exclusive lock and verify it is still path-named.

    ``None`` means a peer holds the lock or the path was replaced during the
    acquire race. Callers should treat either outcome as a live peer.
    """
    _owned_handles_for_current_process()
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    for _attempt in range(_INODE_RECHECK_ATTEMPTS):
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return None
        except BaseException:
            # Acquisition never transfers ownership when it raises, including
            # unexpected flock failures or interruption of the caller.
            handle.close()
            raise
        if _same_path_inode(handle, lock_path):
            handles = _owned_handles_for_current_process()
            handles[id(handle)] = (os.getpid(), handle)
            return handle
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
    return None


def current_process_holds_worker_lock(path: Path | str, *, claim: str | None = None) -> bool:
    """Recognize only a handle acquired here, still open and naming this inode.

    Payload pids cannot prove ownership. The acquiring pid also prevents a
    forked child from treating an inherited registry entry as its own claim.
    """
    for owner_pid, handle in tuple(_owned_handles_for_current_process().values()):
        if owner_pid != os.getpid() or not _same_path_inode(handle, Path(path)):
            continue
        if claim is None:
            return True
        try:
            handle.seek(0)
            payload = json.load(handle)
        except (OSError, UnicodeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("claim") == claim:
            return True
    return False


def reclaim_worker_lock(path: Path | str) -> bool:
    """Unlink an orphan only under its inode-checked flock; errors refuse."""
    lock_path = Path(path)
    try:
        handle = try_acquire_worker_lock(lock_path)
    except OSError:
        return False
    if handle is None:
        return False
    try:
        if not _same_path_inode(handle, lock_path):
            return False
        lock_path.unlink()
        return True
    except OSError:
        return False
    finally:
        release_worker_lock(handle)


def _pid_is_alive(pid: object) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _report_unlocked_payload_diagnostic(handle: TextIO, path: Path) -> None:
    try:
        handle.seek(0)
        payload = json.loads(handle.read())
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        return
    if not isinstance(payload, dict) or payload.get("released") is True:
        return
    pid = payload.get("pid")
    if _pid_is_alive(pid):
        _logger.warning(
            "payload_pid_alive_but_unlocked path=%s pid=%s",
            path,
            pid,
        )


def lock_is_held(path: Path | str) -> bool:
    """Probe the kernel flock; payload pids are diagnostic only.

    A missing lock is not held. Any other open/probe uncertainty fails closed
    as live. The inode is rechecked after probing so a concurrent purge cannot
    make a probe of an unlinked lock file answer for the current path.
    """
    lock_path = Path(path)
    for _attempt in range(_INODE_RECHECK_ATTEMPTS):
        try:
            handle = lock_path.open("r", encoding="utf-8")
        except FileNotFoundError:
            return False
        except OSError:
            return True

        acquired = False
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                if _same_path_inode(handle, lock_path):
                    return True
                continue
            except OSError:
                return True

            if not _same_path_inode(handle, lock_path):
                continue
            _report_unlocked_payload_diagnostic(handle, lock_path)
            return False
        finally:
            if acquired:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            try:
                handle.close()
            except OSError:
                pass
    return True


def release_worker_lock(handle: TextIO) -> None:
    """Write a released marker while still locked, then unlock and close."""
    _owned_handles_for_current_process().pop(id(handle), None)
    try:
        payload: dict[str, object] = {}
        try:
            handle.seek(0)
            current = json.loads(handle.read())
            if isinstance(current, dict):
                payload.update(current)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            pass
        payload.update({"pid": os.getpid(), "released": True, "released_ts": time.time()})
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(json.dumps(payload))
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:  # noqa: BLE001 — payload writes are best effort; still unlock below
            pass
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:  # noqa: BLE001 — always proceed to close
            pass
        try:
            handle.close()
        except Exception:  # noqa: BLE001 — releasing the flock is best effort
            pass


def _reset_worker_lock_registry_for_tests() -> None:
    """Release tracked handles and reset process-local state between tests."""
    handles = tuple(_owned_handles_for_current_process().values())
    for _owner_pid, handle in handles:
        release_worker_lock(handle)
    _OWNED_HANDLES.clear()


def purge_stale_worker_locks(
    state_dir: Path | str,
    *,
    max_age_seconds: float = _DEFAULT_STALE_LOCK_AGE_SECONDS,
    now: float | None = None,
) -> int:
    """Unlink old unheld ``worker-*.lock`` files while holding each flock.

    The path is checked against the opened inode after acquiring the flock and
    immediately before unlink. Acquirers perform the matching post-lock inode
    check in :func:`try_acquire_worker_lock`.
    """
    directory = Path(state_dir)
    age_limit = float(max_age_seconds)
    if age_limit < 0:
        raise ValueError("max_age_seconds must be non-negative")
    now_epoch = time.time() if now is None else float(now)
    try:
        lock_paths = list(directory.glob("worker-*.lock"))
    except OSError:
        return 0

    removed = 0
    for path in lock_paths:
        try:
            initial = path.lstat()
            if not stat.S_ISREG(initial.st_mode) or now_epoch - initial.st_mtime < age_limit:
                continue
            handle = path.open("r", encoding="utf-8")
        except (FileNotFoundError, OSError):
            continue

        acquired = False
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                continue
            except OSError:
                continue

            try:
                opened = os.fstat(handle.fileno())
                named = path.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(named.st_mode):
                continue
            if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
                continue
            if now_epoch - opened.st_mtime < age_limit:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            except OSError:
                continue
            removed += 1
        finally:
            if acquired:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            try:
                handle.close()
            except OSError:
                pass
    return removed
