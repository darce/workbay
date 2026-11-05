"""Shared writable lifetimes versus a bounded exclusive check/delete fence.

[CON-11] Atomicity violation (OSTEP ch. 32): every sanctioned writable
connection holds SH on the canonical resource's stable sidecar until close.
Removal holds EX across its fresh premises and Git removal, outside SQLite
transactions. SH holders remain concurrent; flock supplies cross-process and
cross-thread exclusion, even within one process (independent open descriptions).

[CON-13] Order your locks (OSTEP ch. 32): worker claim -> DB lifecycle ->
manifest lifecycle/publication -> pass-store lifecycle -> per-pass publication
-> derived index (after per-pass release); short SQLite transactions close
before Git removal. Never upgrade a live SH handle. Nested EX/native connections
may borrow only from the actual process and Thread object owning EX, never a
ContextVar.

[RES-02] Timeout on every blocking call (Release It! ch. 5): nonblocking flock
polls have a monotonic deadline and fail closed. [CON-22] Concurrent correctness
is not a test artifact (OSTEP ch. 26): no writer can open before EX ends, and EX
cannot start before every preexisting writable handle closes.

Contract summary:

* The lock is the stable sidecar ``str(canonical_resource(resource)) +
  ".lifecycle.lock"``. The replaceable data inode is never locked. A held
  descriptor whose path identity no longer matches (sidecar removed or replaced
  while waiting) is dropped and reacquired; participants must not remove,
  rename, or replace sidecars or locked roots during a lifetime.
* Canonical-path locking does NOT unify hard-link aliases. Two hard links to one
  database are two lock namespaces; native DB admission must reject alternate
  hard-linked namespaces rather than rely on this module for their exclusion.
* ``deadline`` is an absolute ``time.monotonic()`` value. The effective wait is
  ``min(now + timeout, deadline)``; an explicit deadline is never extended.
  An already-expired deadline refuses before any sidecar is opened or borrowed.
* Fork children never borrow parent ownership, never call ``LOCK_UN``, and
  never run SQLite close on inherited connections: closing a child's copy of a
  parent's SQLite handle can roll back the parent's hot journal through the
  shared descriptor. Inherited connections are quarantined (kept referenced,
  never closed by this module); the S-side connection must refuse use after
  fork by its own owner-pid check.

Connection-close ABI: ``bind_connection(connection)`` retains the handle and
its lease; the caller owns SQLite close. ``release()`` refuses a live binding
without closing it or surrendering permission. The caller must run SQLite close
first and release only after close succeeds. Forgotten bound leases are retained
rather than finalized underneath a usable handle.
"""

from __future__ import annotations

import errno
import fcntl
import math
import numbers
import os
import sqlite3
import stat
import threading
import time
import weakref
from pathlib import Path

__all__ = [
    "DEFAULT_LIFECYCLE_TIMEOUT_SECONDS",
    "LifecycleLease",
    "LifecycleLeaseTimeout",
    "LifecycleOwnershipError",
    "canonical_resource",
]

DEFAULT_LIFECYCLE_TIMEOUT_SECONDS: float = 1.0
_LOCK_SUFFIX = ".lifecycle.lock"
_POLL_SECONDS = 0.01
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)

_leases: weakref.WeakSet[LifecycleLease] = weakref.WeakSet()
_registry_lock = threading.RLock()
# Strong pins prevent lease finalization from releasing a live bound handle.
_bound_leases: set[LifecycleLease] = set()
# Open-to-publication coverage, including descriptors that are still polling.
# Open/register, close/unregister and publication are atomic with fork's prepare
# hook. The mutex is never held across flock polling or its sleeps [CON-18].
_pending_descriptors: set[int] = set()
# Connections inherited across fork. Held, never closed, in the child.
_fork_quarantine: list[object] = []


class LifecycleLeaseTimeout(TimeoutError):
    """No lifecycle permission was obtained before the deadline."""


class LifecycleOwnershipError(RuntimeError):
    """A lock upgrade, foreign owner, or escaping borrowed handle was refused."""


def canonical_resource(path: Path | str) -> Path:
    """Expand the user and resolve relative, parent-component and symlink aliases."""
    return Path(path).expanduser().resolve()


def _seconds(name: str, value: object) -> float:
    """Finite real seconds. Booleans are not numerical authority."""
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TypeError(f"lifecycle {name} must be a real number of seconds, not {type(value).__name__}")
    seconds = float(value)
    if not math.isfinite(seconds):
        raise ValueError(f"lifecycle {name} must be finite")
    return seconds


def _open_sidecar(lock_path: Path) -> int:
    # O_NOFOLLOW: a symlinked sidecar would lock a file other than the stable one.
    return os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | _O_NOFOLLOW, 0o600)


def _sidecar_is_held(fd: int, lock_path: Path) -> bool:
    """True when the locked descriptor is still the regular sidecar at its path."""
    try:
        current = os.stat(lock_path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    held = os.fstat(fd)
    return stat.S_ISREG(current.st_mode) and (current.st_dev, current.st_ino) == (held.st_dev, held.st_ino)


def _check_deadline(
    end: float, resource: Path, *, immediate: bool = False, deadline: float | None = None,
) -> None:
    now = time.monotonic()
    if (deadline is not None and now >= deadline) or (not immediate and now >= end):
        raise LifecycleLeaseTimeout(f"lifecycle lease timed out: {resource}")


def _close_pending_descriptor(fd: int) -> None:
    with _registry_lock:
        # A child fork hook may already have closed this inherited descriptor.
        if fd in _pending_descriptors:
            try:
                os.close(fd)
            finally:
                _pending_descriptors.discard(fd)


def _acquire_descriptor(
    lock_path: Path, *, exclusive: bool, end: float, resource: Path,
    immediate: bool = False, deadline: float | None = None,
) -> int:
    """Nonblocking flock with bounded polling until ``end`` (monotonic)."""
    mode = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB
    first_attempt = True
    while True:
        _check_deadline(end, resource, immediate=immediate and first_attempt, deadline=deadline)
        with _registry_lock:
            _check_deadline(end, resource, immediate=immediate and first_attempt, deadline=deadline)
            fd = _open_sidecar(lock_path)
            try:
                _pending_descriptors.add(fd)
            except BaseException:
                os.close(fd)
                raise
        try:
            while True:
                _check_deadline(end, resource, immediate=immediate and first_attempt, deadline=deadline)
                try:
                    fcntl.flock(fd, mode)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                        raise
                    first_attempt = False
                    remaining = end - time.monotonic()
                    if remaining <= 0:
                        raise LifecycleLeaseTimeout(f"lifecycle lease timed out: {resource}") from None
                    time.sleep(min(_POLL_SECONDS, remaining))
            # A successful syscall can also resume after the governing deadline.
            _check_deadline(end, resource, immediate=immediate and first_attempt, deadline=deadline)
            if _sidecar_is_held(fd, lock_path):
                _check_deadline(end, resource, immediate=immediate and first_attempt, deadline=deadline)
                return fd
        except BaseException:
            _close_pending_descriptor(fd)
            raise
        # The sidecar was removed or replaced while we waited: this lock sits on a
        # stale inode and conveys nothing for the stable path. Drop it and retry.
        _close_pending_descriptor(fd)
        first_attempt = False
        remaining = end - time.monotonic()
        if remaining <= 0:
            raise LifecycleLeaseTimeout(f"lifecycle lease timed out: {resource}")
        time.sleep(min(_POLL_SECONDS, remaining))


class LifecycleLease:
    """An acquired SH lifetime or EX destructive fence; release is idempotent.

    Construction acquires permission or raises; no object conveys permission
    after a failed acquisition. EX holders may lend nested borrowers on the same
    process and Thread object. An escaping borrower makes the outer exclusive
    release raise and retains exclusion until every borrower closes.
    """

    def __init__(
        self, resource: Path | str, *, exclusive: bool = False,
        timeout: float = DEFAULT_LIFECYCLE_TIMEOUT_SECONDS, deadline: float | None = None,
    ) -> None:
        # Safe defaults first: __del__ must run cleanly if validation refuses.
        self._closed = True
        self._fd: int | None = None
        self._parent: LifecycleLease | None = None
        self._references = 1
        self._connection: object | None = None
        self._pid = os.getpid()
        self._thread = threading.current_thread()
        self._exclusive = bool(exclusive)
        timeout_seconds = _seconds("timeout", timeout)
        if timeout_seconds < 0:
            raise ValueError("lifecycle timeout must be finite and nonnegative")
        deadline_seconds = None if deadline is None else _seconds("deadline", deadline)
        end = time.monotonic() + timeout_seconds
        if deadline_seconds is not None:
            end = min(end, deadline_seconds)
        immediate = timeout_seconds == 0
        self._resource = canonical_resource(resource)
        _check_deadline(end, self._resource, immediate=immediate, deadline=deadline_seconds)

        with _registry_lock:
            _check_deadline(end, self._resource, immediate=immediate, deadline=deadline_seconds)
            owned = [
                lease for lease in _leases
                if not lease._closed and lease._pid == self._pid
                and lease._resource == self._resource and lease._thread is self._thread
            ]
            root = next((lease for lease in owned if lease._exclusive and lease._parent is None), None)
            if root is not None:
                _check_deadline(end, self._resource, immediate=immediate, deadline=deadline_seconds)
                self._parent = root
                root._references += 1
                self._closed = False
                _leases.add(self)
                return
            if self._exclusive and owned:
                raise LifecycleOwnershipError(
                    f"cannot upgrade live lifecycle lease to exclusive: {self._resource}")

        lock_path = Path(str(self._resource) + _LOCK_SUFFIX)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = _acquire_descriptor(
            lock_path, exclusive=self._exclusive, end=end, resource=self._resource,
            immediate=immediate, deadline=deadline_seconds,
        )
        try:
            with _registry_lock:
                self._require_owner("acquire")
                _check_deadline(end, self._resource, immediate=immediate, deadline=deadline_seconds)
                self._fd = fd
                _leases.add(self)
                _pending_descriptors.remove(fd)
                self._closed = False
        except BaseException:
            with _registry_lock:
                self._fd = None
                _leases.discard(self)
                _close_pending_descriptor(fd)
            raise

    @property
    def resource(self) -> Path:
        return self._resource

    @property
    def exclusive(self) -> bool:
        return self._exclusive

    @property
    def borrowed(self) -> bool:
        return self._parent is not None

    def bind_connection(self, connection: object) -> None:
        """Retain a SQLite handle until its caller closes it and releases this lease."""
        with _registry_lock:
            self._require_owner("bind a connection to")
            if self._closed:
                raise LifecycleOwnershipError(f"cannot bind a connection to a released lease: {self._resource}")
            if self._connection is connection:
                return
            if self._bound_connection_is_live():
                raise LifecycleOwnershipError("cannot replace a live lifecycle connection binding")
            self._connection = connection
            _bound_leases.add(self)

    def _bound_connection_is_live(self) -> bool:
        connection = self._connection
        if connection is None:
            return False
        if not isinstance(connection, sqlite3.Connection):
            # An opaque handle has no supported proof of successful SQLite close.
            return True
        try:
            # Use SQLite's base descriptor, not SQL or an overridable subclass
            # method/property. It checks whether the native handle is closed,
            # without statements, authorizers, transactions or connection.close().
            sqlite3.Connection.in_transaction.__get__(connection)
        except sqlite3.ProgrammingError:
            return False
        return True

    def _require_owner(self, action: str) -> None:
        if self._pid != os.getpid() or self._thread is not threading.current_thread():
            raise LifecycleOwnershipError(f"{action} lifecycle lease requires its owning process and thread")

    def _close_fd(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            # close, never LOCK_UN: an inherited fd shares the parent's open
            # description and must not unlock the parent's lease.
            os.close(fd)

    def release(self) -> None:
        """Release after SQLite close; refusal retains all permission and accounting."""
        with _registry_lock:
            if self._closed:
                return
            if self._pid != os.getpid():
                # Inherited handle in a fork child: drop the descriptor, never unlock.
                self._closed = True
                self._close_fd()
                return
            if (
                self._exclusive or self._parent is not None or self._connection is not None
            ) and self._thread is not threading.current_thread():
                raise LifecycleOwnershipError("lifecycle lease release requires its owning thread")
            if self._bound_connection_is_live():
                raise LifecycleOwnershipError("cannot release a live bound SQLite lifecycle connection")
            root = self._parent or self
            if root is self and self._references > 1:
                raise LifecycleOwnershipError(
                    "writable connection or nested lease escaped exclusive lifecycle fence")
            # All refusals precede mutation [CON-11]. A failed release neither
            # consumes this lease's reference nor hides a still-owned EX root.
            self._closed = True
            root._references -= 1
            _leases.discard(self)
            _bound_leases.discard(self)
            self._connection = None
            if root._references == 0:
                _leases.discard(root)
                root._close_fd()
                return

    def __enter__(self) -> LifecycleLease:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()

    def __del__(self) -> None:
        # Finalizers never raise. An unreachable root that was never released and has
        # no outstanding borrowers can no longer be used, so its descriptor is closed.
        try:
            self.release()
        except Exception:
            with _registry_lock:
                if (
                    self._parent is None and not self._closed
                    and self._references == 1 and self._connection is None
                ):
                    self._closed = True
                    self._references = 0
                    _leases.discard(self)
                    self._close_fd()


def _before_fork() -> None:
    _registry_lock.acquire()


def _after_fork_parent() -> None:
    _registry_lock.release()


def _after_fork_child() -> None:
    """Child-side invalidation: no borrowing, no LOCK_UN, no SQLite close."""
    global _registry_lock, _leases, _bound_leases, _pending_descriptors
    inherited = list(_leases)
    inherited_descriptors = set(_pending_descriptors)
    _registry_lock = threading.RLock()
    _leases = weakref.WeakSet()
    for lease in inherited:
        connection = lease._connection
        if connection is not None:
            _fork_quarantine.append(connection)
        lease._closed = True
        if lease._fd is not None:
            inherited_descriptors.add(lease._fd)
            lease._fd = None
    # Quarantine connections before dropping the strong lease registry. Closing
    # pending and published child copies never issues LOCK_UN or SQLite close.
    _bound_leases = set()
    _pending_descriptors = set()
    for fd in inherited_descriptors:
        os.close(fd)


os.register_at_fork(
    before=_before_fork, after_in_parent=_after_fork_parent, after_in_child=_after_fork_child,
)
