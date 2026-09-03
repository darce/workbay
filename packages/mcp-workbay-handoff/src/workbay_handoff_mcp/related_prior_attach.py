"""Post-commit advisory related-prior attach for record paths.

Called *after* the essential write commits (and after embed-on-write, when
that hook is present). The lookup is ranking-only: it must never gate,
filter, auto-close, or auto-link the row just written. Failures degrade to a
typed payload and never raise out of the record path.
"""

from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from .shared_primitives import _side_effect_error_class, _surface_side_effect_failure

RELATED_PRIOR_LIMIT = 5
RELATED_PRIOR_DEDUP_WINDOW_S = 300.0
RELATED_PRIOR_DEDUP_CAP = 256
# Tool-path wall-clock budget for the lookup (MCP response availability).
RELATED_PRIOR_LOOKUP_TIMEOUT_S = 5.0

_DEDUP_LOCK = threading.Lock()
# (query_hash, normalized task_ref) -> (monotonic_ts, cached_outcome)
_DEDUP: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_IN_FLIGHT: dict[tuple[str, str], "_InFlightSlot"] = {}

_ERROR_PAYLOAD = {"status": "error", "results": [], "note": "related_prior:error"}
_TIMEOUT_PAYLOAD = {"status": "error", "results": [], "note": "related_prior:timeout"}
_UNAVAILABLE_PAYLOAD = {
    "status": "provider_unavailable",
    "results": [],
    "note": "related_prior:provider_unavailable",
}

_DEDUPED_FLAG = "related_prior:deduped"
_EMBEDDING_FAILED = "embedding_failed"


class _InFlightSlot:
    """One in-flight lookup shared by concurrent identical (query, task_ref) keys."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.payload: dict[str, Any] | None = None
        self.error: BaseException | None = None


def reset_related_prior_dedup_for_tests() -> None:
    """Drop the in-process description-hash window (tests only)."""
    with _DEDUP_LOCK:
        _DEDUP.clear()
        _IN_FLIGHT.clear()


def lookup_related_prior(
    *,
    text: str,
    task_ref: str | None = None,
    limit: int = RELATED_PRIOR_LIMIT,
) -> dict[str, Any]:
    """In-process lookup that backs ``core.find_related_prior_work``.

    Bounded with the same single-thread executor pattern as
    ``embeddings.store._provider_embed_batch``: ``future.result(timeout=...)``
    then ``shutdown(wait=False, cancel_futures=True)``. On timeout return a
    typed degrade instead of hanging the MCP tool response. Other exceptions
    propagate to the attach wrapper.
    """
    # Lazy: concurrent.futures is stdlib but keep the always-on import surface thin.
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FuturesTimeout
    from contextvars import copy_context

    from .core import find_related_prior_work

    def _call() -> dict[str, Any]:
        return find_related_prior_work(text=text, task_ref=task_ref, limit=limit)

    timeout_seconds = RELATED_PRIOR_LOOKUP_TIMEOUT_S
    # wait=False on shutdown so a timed-out hung embed does not block the
    # tool path forever on the orphaned worker. copy_context so the worker
    # sees configure_runtime() (ContextVar, not inherited by ThreadPoolExecutor).
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(copy_context().run, _call)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeout:
            return copy.deepcopy(_TIMEOUT_PAYLOAD)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def attach_related_prior(
    result: dict,
    *,
    query: str,
    task_ref: str | None = None,
) -> None:
    """Attach ``data.related_prior`` after a successful write. Never raises.

    Dedupes identical ``(query, task_ref)`` pairs inside a short in-process
    window so a storm of near-identical records does not re-run the lookup
    each time. A cache hit attaches the cached outcome and adds
    ``related_prior:deduped`` as a flag without clobbering the cached note.
    """
    try:
        _attach_related_prior_unguarded(result, query=query, task_ref=task_ref)
    except ImportError:
        payload = copy.deepcopy(_UNAVAILABLE_PAYLOAD)
        _remember_best_effort(query, task_ref, payload)
        try:
            _set_related_prior(result, payload)
        except Exception:
            pass
    except Exception as exc:
        payload = copy.deepcopy(_ERROR_PAYLOAD)
        _remember_best_effort(query, task_ref, payload)
        try:
            _set_related_prior(result, copy.deepcopy(_ERROR_PAYLOAD))
        except Exception:
            pass
        try:
            _surface_side_effect_failure(
                result,
                effect="related_prior",
                detail=f"related_prior lookup failed: {exc}",
                error_class=_side_effect_error_class(exc),
            )
        except Exception:
            pass


def _attach_related_prior_unguarded(
    result: dict,
    *,
    query: str,
    task_ref: str | None,
) -> None:
    if not isinstance(result, dict) or not result.get("ok"):
        return
    if _is_embedding_failed_record(result):
        return
    resolved_task_ref = _normalize_task_ref(task_ref)
    payload, err, from_cache = _shared_lookup(query, resolved_task_ref)
    if from_cache:
        payload = _with_deduped_flag(payload)
    if err is None:
        _set_related_prior(result, payload)
        return
    try:
        _set_related_prior(result, payload)
    except Exception:
        pass
    if isinstance(err, ImportError):
        return
    raise err


def _shared_lookup(
    query: str,
    task_ref: str | None,
) -> tuple[dict[str, Any], BaseException | None, bool]:
    """Run or join one lookup for ``(query_hash, task_ref)``.

    Returns ``(payload, error, from_cache)``. Typed degrade outcomes are
    remembered in the same window as successes so a failed lookup cannot
    amplify into N cold embeds. Concurrent waiters share the in-flight call.
    """
    key = _dedupe_key(query, task_ref)
    owner = False
    with _DEDUP_LOCK:
        cached = _cached_outcome_locked(key)
        if cached is not None:
            return copy.deepcopy(cached), None, True
        slot = _IN_FLIGHT.get(key)
        if slot is None:
            slot = _InFlightSlot()
            _IN_FLIGHT[key] = slot
            owner = True
    if not owner:
        slot.event.wait()
        if slot.payload is not None:
            return copy.deepcopy(slot.payload), slot.error, False
        with _DEDUP_LOCK:
            cached = _cached_outcome_locked(key)
        if cached is not None:
            return copy.deepcopy(cached), None, False
        return copy.deepcopy(_ERROR_PAYLOAD), None, False

    err: BaseException | None = None
    payload: dict[str, Any] | None = None
    try:
        try:
            raw = lookup_related_prior(
                text=query,
                task_ref=task_ref,
                limit=RELATED_PRIOR_LIMIT,
            )
            payload = _normalize_payload(raw)
        except ImportError as exc:
            payload = copy.deepcopy(_UNAVAILABLE_PAYLOAD)
            err = exc
        except Exception as exc:
            payload = copy.deepcopy(_ERROR_PAYLOAD)
            err = exc
        slot.payload = payload
        slot.error = err
        _remember_best_effort(query, task_ref, payload)
        return copy.deepcopy(payload), err, False
    finally:
        if payload is None:
            fallback = copy.deepcopy(_ERROR_PAYLOAD)
            slot.payload = fallback
            slot.error = err
            _remember_best_effort(query, task_ref, fallback)
        slot.event.set()
        with _DEDUP_LOCK:
            if _IN_FLIGHT.get(key) is slot:
                _IN_FLIGHT.pop(key, None)


def _normalize_payload(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise TypeError("related-prior lookup must return a dict")
    payload = copy.deepcopy(raw)
    results = payload.get("results")
    if results is None:
        payload["results"] = []
    elif not isinstance(results, list):
        payload["results"] = list(results)
    if payload.get("status") == "provider_unavailable":
        payload["note"] = "related_prior:provider_unavailable"
    return payload


def _with_deduped_flag(payload: dict[str, Any]) -> dict[str, Any]:
    """Mark a cache hit without clobbering the cached note."""
    flags = payload.get("flags")
    if not isinstance(flags, list):
        flags = []
    else:
        flags = list(flags)
    if _DEDUPED_FLAG not in flags:
        flags.append(_DEDUPED_FLAG)
    payload["flags"] = flags
    if not isinstance(payload.get("results"), list):
        payload["results"] = list(payload.get("results") or [])
    return payload


def _set_related_prior(result: dict, payload: dict[str, Any]) -> None:
    data = result.get("data")
    if not isinstance(data, dict):
        data = {}
        result["data"] = data
    data["related_prior"] = payload


def _is_embedding_failed_record(result: dict) -> bool:
    data = result.get("data")
    if not isinstance(data, dict):
        return False
    for key in ("agent_error", "blocker"):
        record = data.get(key)
        if isinstance(record, dict) and record.get("error_class") == _EMBEDDING_FAILED:
            return True
    return False


def _normalize_task_ref(task_ref: str | None) -> str | None:
    if isinstance(task_ref, str) and task_ref.strip():
        return task_ref.strip()
    return None


def _query_hash(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8")).hexdigest()


def _dedupe_key(query: str, task_ref: str | None) -> tuple[str, str]:
    return (_query_hash(query), _normalize_task_ref(task_ref) or "")


def _cached_outcome_locked(key: tuple[str, str]) -> dict[str, Any] | None:
    entry = _DEDUP.get(key)
    if entry is None:
        return None
    ts, outcome = entry
    if time.monotonic() - ts > RELATED_PRIOR_DEDUP_WINDOW_S:
        _DEDUP.pop(key, None)
        return None
    return outcome


def _remember_best_effort(query: str, task_ref: str | None, outcome: dict[str, Any]) -> None:
    try:
        _remember(query, task_ref, outcome)
    except Exception:
        pass


def _remember(query: str, task_ref: str | None, outcome: dict[str, Any]) -> None:
    key = _dedupe_key(query, task_ref)
    with _DEDUP_LOCK:
        if key in _DEDUP:
            return
        if len(_DEDUP) >= RELATED_PRIOR_DEDUP_CAP:
            oldest_key = min(_DEDUP, key=lambda item: _DEDUP[item][0])
            _DEDUP.pop(oldest_key, None)
        _DEDUP[key] = (time.monotonic(), copy.deepcopy(outcome))
