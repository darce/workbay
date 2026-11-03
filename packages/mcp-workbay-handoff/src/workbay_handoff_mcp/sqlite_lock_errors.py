"""Typed SQLite lock classification via extended result codes.

Python 3.11+ exposes ``exc.sqlite_errorcode`` (int) and ``exc.sqlite_errorname``
(str) on exceptions raised by the sqlite3 module. Primary result codes alone
cannot separate ``SQLITE_BUSY`` (5) from ``SQLITE_BUSY_SNAPSHOT`` (517): both
surface the message ``database is locked``. String matching therefore cannot
name the condition that fired.

Classification is keyed **only** on ``sqlite_errorcode``. There is no
substring fallback for the primary decision. When the attribute is absent
(older interpreters, or hand-built ``OperationalError`` fixtures), the result
is the explicit :attr:`SqliteLockKind.UNKNOWN` so callers decide degradation
policy rather than silently guessing.

Retryability is a property of the retried **unit**, not of the error code
alone ([RES-01]). :func:`is_retryable_lock_error` therefore takes an explicit
:class:`RetryBoundary` rather than inferring one from ambient state.
"""

from __future__ import annotations

import sqlite3
from enum import Enum
from typing import Final


class SqliteLockKind(str, Enum):
    """Named SQLite lock / contention outcomes.

    Members are stable string values so telemetry can emit the classification
    name without further mapping.
    """

    BUSY = "BUSY"
    BUSY_SNAPSHOT = "BUSY_SNAPSHOT"
    LOCKED = "LOCKED"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


class RetryBoundary(str, Enum):
    """Unit of work that a retry path re-executes ([RES-01]).

    Callers must pass the boundary that matches what they actually re-run.
    Do not sniff the call stack or inherit a module-level default by accident.

    ``STATEMENT``
        Re-execute a single statement (or continue the same open transaction).
        Only peer-writer ``BUSY`` can clear while the same snapshot/txn remains.
    ``WHOLE_CALL``
        Re-invoke a whole handler that opens a fresh connection/transaction
        (e.g. :func:`write_retry.call_with_write_lock_retry`). A new snapshot
        is taken, so ``BUSY_SNAPSHOT`` becomes retryable at this boundary only.
    """

    STATEMENT = "statement"
    WHOLE_CALL = "whole_call"


# Primary result-code mask (SQLite extended codes encode primary in low 8 bits).
_PRIMARY_RC_MASK: Final[int] = 0xFF

# Prefer module constants when present; fall back to documented numeric values
# so classification stays correct on interpreters that expose the exception
# attributes but not every SQLITE_* name on the sqlite3 module.
_SQLITE_BUSY: Final[int] = int(getattr(sqlite3, "SQLITE_BUSY", 5))
_SQLITE_BUSY_SNAPSHOT: Final[int] = int(getattr(sqlite3, "SQLITE_BUSY_SNAPSHOT", 517))
_SQLITE_LOCKED: Final[int] = int(getattr(sqlite3, "SQLITE_LOCKED", 6))


def classify_sqlite_lock_error(exc: BaseException) -> SqliteLockKind:
    """Classify a SQLite exception by ``sqlite_errorcode`` only.

    Returns
    -------
    SqliteLockKind
        ``BUSY`` for primary ``SQLITE_BUSY`` (including extended BUSY forms
        other than SNAPSHOT, e.g. ``BUSY_TIMEOUT`` / ``BUSY_RECOVERY``).
        ``BUSY_SNAPSHOT`` for ``SQLITE_BUSY_SNAPSHOT`` (517).
        ``LOCKED`` for primary ``SQLITE_LOCKED`` (including shared-cache /
        vtab extended forms).
        ``OTHER`` when an errorcode is present but is not a lock/busy code
        (constraint failures, ``no such table``, etc.).
        ``UNKNOWN`` when ``sqlite_errorcode`` is absent — never inferred from
        the exception message.
    """
    code = getattr(exc, "sqlite_errorcode", None)
    if code is None:
        return SqliteLockKind.UNKNOWN
    try:
        code_i = int(code)
    except (TypeError, ValueError):
        return SqliteLockKind.UNKNOWN

    # Extended BUSY_SNAPSHOT must be named before the primary-BUSY bucket so
    # the two "database is locked" outcomes stay distinguishable.
    if code_i == _SQLITE_BUSY_SNAPSHOT:
        return SqliteLockKind.BUSY_SNAPSHOT

    primary = code_i & _PRIMARY_RC_MASK
    if primary == _SQLITE_BUSY:
        return SqliteLockKind.BUSY
    if primary == _SQLITE_LOCKED:
        return SqliteLockKind.LOCKED
    return SqliteLockKind.OTHER


def is_retryable_lock_error(
    exc: BaseException,
    *,
    boundary: RetryBoundary | str = RetryBoundary.STATEMENT,
) -> bool:
    """True when retrying the given *boundary* can clear this lock error.

    Retryability is a property of the retried unit, not of the error alone
    ([RES-01]). Callers must name the boundary that matches what they re-run;
    the default ``STATEMENT`` preserves historical semantics (BUSY only) so
    statement-level callers are not silently widened.

    ``STATEMENT`` boundary
        Only :attr:`SqliteLockKind.BUSY` is retryable. A peer writer holding
        the reserved/pending lock will eventually finish; sleep + re-execute
        the same statement can succeed.

        :attr:`SqliteLockKind.BUSY_SNAPSHOT` is **not** retryable here. It
        means this connection's DEFERRED transaction holds a snapshot a
        concurrent writer has invalidated. Waiting cannot restore that
        snapshot; the open transaction must be rolled back and restarted.
        Re-running the same statement inside the same txn keeps failing.

    ``WHOLE_CALL`` boundary
        :attr:`SqliteLockKind.BUSY` remains retryable (same peer-writer case).
        :attr:`SqliteLockKind.BUSY_SNAPSHOT` is also retryable: the retried
        unit starts a fresh transaction and therefore takes a fresh snapshot.
        This is the case :func:`write_retry.call_with_write_lock_retry`
        implements — it re-invokes the whole handler, not a single statement.

    ``SQLITE_LOCKED`` (primary 6) at every boundary
        Non-retryable by deliberate conservative choice. LOCKED (including
        ``LOCKED_SHAREDCACHE`` / nested-use forms) means another connection in
        the same shared-cache or nested context still owns a conflicting
        table lock. A fresh whole-call invocation does **not** reliably clear
        that condition: the contending connection may be a sibling still open
        in-process, not a peer that finished its write. Historical
        write-retry also never matched ``"database table is locked"``. Leave
        LOCKED non-retryable until a caller can prove its unit of work drops
        the contending holder; undocumented optimism would hide stuck
        shared-cache bugs as "transient busy".

    ``UNKNOWN`` / ``OTHER`` return False so callers must opt into any
    degraded message-based path themselves.
    """
    if isinstance(boundary, str):
        try:
            boundary = RetryBoundary(boundary)
        except ValueError as exc_boundary:
            raise ValueError(
                f"unknown retry boundary {boundary!r}; expected one of {[b.value for b in RetryBoundary]}"
            ) from exc_boundary

    kind = classify_sqlite_lock_error(exc)
    if kind is SqliteLockKind.BUSY:
        return True
    if kind is SqliteLockKind.BUSY_SNAPSHOT:
        return boundary is RetryBoundary.WHOLE_CALL
    # LOCKED / OTHER / UNKNOWN: see docstring — never wait-retryable here.
    return False


def is_lock_contention_error(exc: BaseException) -> bool:
    """True when the exception is a named lock/busy contention code.

    Covers ``BUSY``, ``BUSY_SNAPSHOT``, and ``LOCKED``. Does not treat
    ``UNKNOWN`` as contention — callers that need legacy message matching
    for fixtures without ``sqlite_errorcode`` must do that explicitly.

    This is a separate question from retryability: contention shape does not
    depend on the retry boundary.
    """
    return classify_sqlite_lock_error(exc) in {
        SqliteLockKind.BUSY,
        SqliteLockKind.BUSY_SNAPSHOT,
        SqliteLockKind.LOCKED,
    }
