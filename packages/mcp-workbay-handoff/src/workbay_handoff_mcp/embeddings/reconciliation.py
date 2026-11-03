"""Bounded warm reconciliation of derived embeddings from authoritative rows.

There is no second source queue and no persisted cursor ([DATA-14]). Each pass
asks SQL for source rows with no vector for their current text and model,
newest first, so committed embeddings are the only progress state: a restarted
server resumes past already-current rows without charging them, and a later
write is its source's first candidate on the next pass. A failed row costs at
most one slot per pass and stays pending. Server lifespan owns the worker, never a
short-lived write hook.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from contextvars import copy_context
from dataclasses import dataclass, field
from typing import Any, TypedDict

import numpy as np

from workbay_handoff_mcp.bounded_call import _mark_abandoned
from workbay_handoff_mcp.runtime import configure_runtime
from workbay_handoff_mcp.shared_schema import _get_db_connection

from . import store

_log = logging.getLogger("workbay_handoff_mcp")

# Changed-text marks reach back this far: a write transaction may commit after
# the datetime('now') it stamped into updated_at.
_CHANGED_SINCE_SLACK = "-300 seconds"

# Only reconciliation calls take this fence; embed-on-write/backfill and the
# other concept/source writers do not. It bounds inference, not publication.
# A timed-out worker owns its slot until it returns, across fresh cursors.
_INFERENCE_LIMIT = 2
_inflight_lock = threading.Lock()
_inflight: dict[tuple[str, str, str], threading.Thread] = {}


@dataclass
class _Marks:
    """What a surviving worker already verified for one (kind, model_id)."""

    created: float
    top: int | None = None  # every key in [low, top] was current or attempted
    low: int | None = None  # None while top is set: verified down to the first key
    changed_since: str | None = None


@dataclass
class ReconciliationCursor:
    """Worker memory that only saves cost; source rows and vectors decide progress.

    ``source`` rotates the round-robin start. ``marks`` let a surviving worker
    skip re-proving INTEGER keys and finding updates it verified, until
    ``mark_ttl_seconds`` pass; a fresh cursor rediscovers the same pending rows
    by scanning. Live inference is bounded separately at process scope, so
    replacing this cursor cannot duplicate an outstanding inference.
    """

    source: int = 0
    marks: dict[tuple[str, str], _Marks] = field(default_factory=dict, repr=False)
    mark_ttl_seconds: float = 900.0


class ReconciliationCounts(TypedDict):
    status: str
    scanned: int
    stored: int
    failed: int


@dataclass
class _Source:
    kind: str
    keyed: bool  # INTEGER recency key: marks apply
    lanes: dict[str, list[Any]]  # lane -> keys as returned, newest first
    queue: deque[tuple[Any, Any, Any]]
    newest: int | None = None
    since: str | None = None
    done: set[object] = field(default_factory=set)


def _discover(
    conn: sqlite3.Connection, model_id: str, cursor: ReconciliationCursor, limit: int, stopped
) -> list[_Source]:
    """Read every source's candidates, newest first, in one short read snapshot."""
    now = time.monotonic()
    since = conn.execute("SELECT datetime('now', ?)", (_CHANGED_SINCE_SLACK,)).fetchone()[0]
    sources: list[_Source] = []
    count = len(store.CONCEPT_BACKFILL_SOURCES)
    for offset in range(count):
        if stopped():
            break
        kind, table, key, _column = store.CONCEPT_BACKFILL_SOURCES[(cursor.source + offset) % count]
        mark = cursor.marks.get((kind, model_id))
        if mark is not None and now - mark.created >= cursor.mark_ttl_seconds:
            del cursor.marks[(kind, model_id)]
            mark = None
        src = _Source(kind, store.RECONCILIATION_RECENCY[table] == key, {}, deque(), since=since)
        found: dict[str, list[tuple[object, object, object]]] = {}
        if table in store.CHANGED_TEXT_TABLES:
            changed_since = mark.changed_since if mark is not None and mark.changed_since else ""
            found["changed"] = store.reconciliation_candidates(
                conn, kind, model_id, limit=limit, changed_since=changed_since
            )
        if src.keyed:
            src.newest = conn.execute(f"SELECT max({key}) FROM {table}").fetchone()[0]
        if src.keyed and mark is not None and mark.top is not None:
            found["above"] = store.reconciliation_candidates(conn, kind, model_id, limit=limit, above=mark.top)
            if mark.low is not None:
                found["below"] = store.reconciliation_candidates(conn, kind, model_id, limit=limit, below=mark.low)
        else:
            found["all"] = store.reconciliation_candidates(conn, kind, model_id, limit=limit)
        queued: set[object] = set()
        src.lanes = {lane: [row[0] for row in rows] for lane, rows in found.items()}
        # Reserve opportunities for new keys and changed fields before draining
        # either backlog. A candidate returned by both lanes is charged once.
        lanes = [deque(found[lane]) for lane in ("above", "all", "changed", "below") if lane in found]
        while any(lanes):
            for rows in lanes:
                if not rows:
                    continue
                row = rows.popleft()
                if row[0] not in queued:
                    queued.add(row[0])
                    src.queue.append(row)
        sources.append(src)
    return sources


def _round_robin(sources: list[_Source]):
    """One candidate per source per round, so no source's backlog starves another."""
    while any(src.queue for src in sources):
        for src in sources:
            if src.queue:
                yield src, src.queue.popleft()


def _done_prefix(keys: list[object], done: set[object]) -> int:
    count = 0
    for key in keys:
        if key not in done:
            break
        count += 1
    return count


def _record_marks(cursor: ReconciliationCursor, sources: list[_Source], model_id: str, limit: int) -> None:
    """Extend marks only over keys this pass proved current or attempted."""
    for src in sources:
        mark = cursor.marks.get((src.kind, model_id)) or _Marks(created=time.monotonic())
        changed = src.lanes.get("changed")
        if changed is not None and _done_prefix(changed, src.done) == len(changed) < limit:
            mark.changed_since = src.since
        if src.keyed and "all" in src.lanes:
            keys = src.lanes["all"]
            prefix = _done_prefix(keys, src.done)
            if prefix or not keys:
                mark.top = src.newest
                mark.low = None if prefix == len(keys) < limit else keys[prefix - 1]
        elif src.keyed:
            above = src.lanes["above"]
            if _done_prefix(above, src.done) == len(above) < limit and src.newest is not None:
                mark.top = src.newest
            below = src.lanes.get("below")
            if below is not None:
                prefix = _done_prefix(below, src.done)
                if prefix == len(below) < limit:
                    mark.low = None
                elif prefix:
                    mark.low = below[prefix - 1]
        if mark.top is not None or mark.changed_since is not None:
            cursor.marks[(src.kind, model_id)] = mark


def reconcile_concept_embeddings(
    conn: sqlite3.Connection,
    provider: store.SupportsEmbed | None,
    *,
    max_rows: int = 32,
    max_seconds: float = 120.0,
    cancel: threading.Event | None = None,
    cursor: ReconciliationCursor | None = None,
    row_timeout_seconds: float | None = None,
) -> ReconciliationCounts:
    """Embed at most max_rows pending source fields, publishing one per transaction.

    Candidates come from :func:`store.reconciliation_candidates`: per source,
    interleaved newest keys and changed finding fields, taken round-robin across
    sources. Only rows sent to inference are charged
    against ``max_rows``; a row found current costs nothing. Publication reuses
    backfill's classification, truncation, budget isolation and source
    revalidation. Only warm providers may drain. Caller commits foreign work
    first. Budgets include discovery/inference/SQL; cancellation is checked
    between phases and before publishing a result. Each inference gets at most
    ``row_timeout_seconds`` (default min(30, max_seconds / 4)) and the remaining
    pass time. At most two reconciliation inferences may be live per process,
    including abandoned workers; none can publish from the background.
    """
    if max_rows < 0 or not math.isfinite(max_seconds) or max_seconds < 0:
        raise ValueError("reconciliation budgets must be finite and non-negative")
    if row_timeout_seconds is not None and (not math.isfinite(row_timeout_seconds) or row_timeout_seconds <= 0):
        raise ValueError("row_timeout_seconds must be finite and positive")
    row_timeout = min(30.0, max_seconds / 4) if row_timeout_seconds is None else row_timeout_seconds
    if conn.in_transaction:
        raise RuntimeError("reconciliation refused an open caller transaction")
    cursor = cursor if cursor is not None else ReconciliationCursor()
    deadline = time.monotonic() + max_seconds
    result: ReconciliationCounts = {"status": "bounded", "scanned": 0, "stored": 0, "failed": 0}

    def stopped():
        if cancel is not None and cancel.is_set():
            result["status"] = "cancelled"
            return True
        if time.monotonic() >= deadline:
            result["status"] = "time_budget"
            return True
        return False

    if stopped() or max_rows == 0:
        return result
    if provider is None:
        return {**result, "status": "provider_unavailable"}
    if not store._provider_is_warm(provider):
        return {**result, "status": "provider_cold"}
    model_id = provider.model_id
    previous_busy = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
    # The sanctioned factory's tracked handle refreshes busy_timeout on every
    # statement. Give this phase its own deadline/cap; the short open budget
    # must not expire while a legitimate long inference is running.
    tracked = hasattr(conn, "_handoff_deadline")
    previous_deadline = getattr(conn, "_handoff_deadline", None)
    previous_cap = getattr(conn, "_handoff_busy_timeout_cap_ms", None)
    if tracked:
        setattr(conn, "_handoff_deadline", deadline)
        setattr(conn, "_handoff_busy_timeout_cap_ms", 100)
    sources: list[_Source] = []
    # Live keys are skipped without using the row budget. Read enough extra
    # candidates to fill that budget even when both registry slots are occupied.
    discovery_limit = max_rows + _INFERENCE_LIMIT
    try:
        conn.execute(f"PRAGMA busy_timeout={max(1, min(100, int((deadline - time.monotonic()) * 1000)))}")
        try:
            sources = _discover(conn, model_id, cursor, discovery_limit, stopped)
        finally:
            # Never hold the discovery snapshot across inference.
            store._end_deferred_read_txn(conn)
        cursor.source = (cursor.source + 1) % len(store.CONCEPT_BACKFILL_SOURCES)
        charged = 0
        exhausted = True
        for src, (key, task_ref, text) in _round_robin(sources):
            if charged >= max_rows or stopped():
                exhausted = False
                break
            kind, entity_id = src.kind, str(key)
            result["scanned"] += 1
            conn.execute(f"PRAGMA busy_timeout={max(1, min(100, int((deadline - time.monotonic()) * 1000)))}")
            outcome, source_hash = store.classify_concept_for_backfill(conn, kind, entity_id, text, model_id=model_id)
            store._end_deferred_read_txn(conn)
            if outcome != "pending" or source_hash is None:
                # Became current since discovery (e.g. embed-on-write): free.
                src.done.add(key)
                continue
            if stopped():
                exhausted = False
                break
            to_store = [(kind, entity_id, str(task_ref), str(text), source_hash)]
            done = threading.Event()
            inference_key = (kind, entity_id, model_id)
            batches: list[tuple[list[tuple[int, np.ndarray]], list[int]]] = []
            errors: list[BaseException] = []

            # Capture immutable per-call inputs: a timed-out worker must never
            # see a later loop iteration's row or event.
            def infer(text=str(text), completed=done, outputs=batches, failures=errors, identity=inference_key):
                try:
                    outputs.append(
                        store._provider_embed_backfill_rows(
                            provider, [(0, store.truncate_embed_text(text))], timeout_seconds=None
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)
                finally:
                    with _inflight_lock:
                        if _inflight.get(identity) is threading.current_thread():
                            del _inflight[identity]
                    completed.set()

            try:
                context = copy_context()
                inference = threading.Thread(
                    target=context.run, args=(infer,), name="workbay-embedding-inference", daemon=True
                )
                with _inflight_lock:
                    if inference_key in _inflight:
                        result["status"] = "inference_pending"
                        continue
                    if len(_inflight) >= _INFERENCE_LIMIT:
                        result["status"] = "inference_pending"
                        return result
                    _inflight[inference_key] = inference
                    try:
                        inference.start()
                    except BaseException:
                        del _inflight[inference_key]
                        done.set()
                        raise
                charged += 1
                row_deadline = min(deadline, time.monotonic() + row_timeout)
                timed_out = False
                while not done.wait(min(0.05, max(0.0, row_deadline - time.monotonic()))):
                    if stopped() or time.monotonic() >= row_deadline:
                        _mark_abandoned(inference)
                        result["failed"] += 1
                        src.done.add(key)
                        if result["status"] == "cancelled":
                            return result
                        result["status"] = "timeout"
                        timed_out = True
                        break
                if timed_out:
                    continue
                if stopped():
                    exhausted = False
                    break
                if errors:
                    raise errors[0]
                embedded, over_budget = batches[0]
                if over_budget:
                    result["failed"] += 1
                    src.done.add(key)
                    continue
                conn.execute(f"PRAGMA busy_timeout={max(1, min(100, int((deadline - time.monotonic()) * 1000)))}")
                stored = store._write_backfill_rows(
                    conn,
                    model_id,
                    to_store,
                    embedded,
                    should_stop=stopped,
                )
                result["stored"] += stored
                if stored:
                    src.done.add(key)
            except TimeoutError:
                result["status"] = "timeout"
                result["failed"] += 1
                src.done.add(key)
                continue
            except Exception as exc:
                # Source rows stay authoritative: the row is still pending and a
                # later pass (or a fresh one) rediscovers it. A lock is transient,
                # so leave the row unverified and let the next pass retry it.
                result["failed"] += 1
                if not store._is_sqlite_lock_error(exc):
                    src.done.add(key)
                _log.warning("embedding reconciliation deferred %s/%s", kind, key, exc_info=True)
        if exhausted and result["status"] == "bounded" and charged < max_rows:
            result["status"] = "complete"
    finally:
        _record_marks(cursor, sources, model_id, discovery_limit)
        # Cleanup must work even when either phase's deadline has expired.
        if tracked:
            setattr(conn, "_handoff_deadline", None)
        try:
            conn.execute(f"PRAGMA busy_timeout={previous_busy}")
        finally:
            if tracked:
                setattr(conn, "_handoff_deadline", previous_deadline)
                if previous_cap is None:
                    delattr(conn, "_handoff_busy_timeout_cap_ms")
                else:
                    setattr(conn, "_handoff_busy_timeout_cap_ms", previous_cap)
    return result


def embedding_reconciliation_lifespan(
    config, *, interval_seconds: float = 30.0, max_rows: int = 32, max_seconds: float = 120.0
):
    """Create the serve-only startup/periodic worker and bound shutdown.

    Loading is off the serving/event-loop and script-write paths. The provider's
    shared loader bounds cold attempts; a drain itself always requires warmth.
    Optional extras/provider failures leave source rows pending for later cycles.
    """
    if not math.isfinite(interval_seconds) or interval_seconds <= 0:
        raise ValueError("interval_seconds must be finite and positive")
    if max_rows < 0 or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError("lifespan budgets must be positive and finite")

    @asynccontextmanager
    async def lifespan(_server):
        cancel = threading.Event()
        cursor = ReconciliationCursor()

        def run():
            configure_runtime(config)  # ContextVars do not propagate to threads.
            while not cancel.is_set():
                try:
                    provider = store._resolve_provider()
                    if provider is not None and not store._provider_is_warm(provider):
                        loader = getattr(provider, "_ensure_loaded_with_timeout", None)
                        if callable(loader):
                            loader(
                                timeout_seconds=min(1.0, max_seconds),
                                timeout_message="reconciliation warm-up timed out",
                            )
                    if provider is not None and store._provider_is_warm(provider) and not cancel.is_set():
                        with _get_db_connection(deadline=time.monotonic() + min(1.0, max_seconds)) as conn:
                            counts = reconcile_concept_embeddings(
                                conn,
                                provider,
                                cursor=cursor,
                                cancel=cancel,
                                max_rows=max_rows,
                                max_seconds=max_seconds,
                            )
                        _log.info("embedding reconciliation %s", counts)
                except Exception:
                    _log.warning("embedding reconciliation unavailable; source coverage remains pending", exc_info=True)
                cancel.wait(interval_seconds)

        worker = threading.Thread(target=run, name="workbay-embedding-reconciliation", daemon=True)
        worker.start()
        try:
            yield
        finally:
            cancel.set()
            shutdown_deadline = time.monotonic() + 1.1
            while worker.is_alive() and time.monotonic() < shutdown_deadline:
                await asyncio.sleep(0.01)

    return lifespan
