"""Deadline-bounded backend availability probes."""

from __future__ import annotations

import inspect
import logging
import math
import os
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

PROBE_TIMEOUT_ENV = "WORKBAY_BACKEND_PROBE_TIMEOUT_S"
PROBE_AGGREGATE_TIMEOUT_ENV = "WORKBAY_BACKEND_PROBE_AGGREGATE_TIMEOUT_S"
DEFAULT_PROBE_TIMEOUT_S = 20.0
DEFAULT_PROBE_AGGREGATE_TIMEOUT_S = 45.0

_logger = logging.getLogger(__name__)

# up0824-ppd-r1-f04: at most one in-flight probe thread per (backend name,
# probe callable). A caller that arrives while a probe for that pair is still
# running joins the same thread instead of starting a duplicate, so a hung
# backend accumulates one daemon thread total (not one per caller) and only
# one thread is ever mutating shared backend_registry caches for that name at
# a time. Keyed on the callable too (not name alone): production always
# passes the same singleton `probe_availability` function for a given name,
# so real calls still coalesce onto one thread, but two calls that
# deliberately pass different probe callables for the same name (as tests
# routinely do) never join each other's thread. Guarded by _inflight_lock;
# entries are evicted by the probe thread itself once done.
_inflight_lock = threading.Lock()
_inflight: dict[tuple[str, Callable[..., dict[str, Any]]], tuple[threading.Event, dict[str, Any]]] = {}


def probe_deadline_from_env(env_var: str, default: float) -> float:
    """Read a finite, positive float deadline (seconds) from ``env_var``.

    Only finite values strictly greater than zero are valid; anything else —
    unset, empty, unparseable, ``"0"``, negative, ``inf``/``nan`` — falls back
    to ``default``. In particular ``"0"`` is invalid (it would disable the
    bound, not tighten it) and ``inf`` is rejected because it would
    reintroduce the unbounded wait these deadlines exist to remove.
    """
    raw = os.environ.get(env_var)
    if raw:
        try:
            value = float(raw)
        except ValueError:
            return default
        if math.isfinite(value) and value > 0:
            return value
    return default


def _fmt_deadline_s(value: float) -> str:
    return f"{value:g}s"


def _accepts_workspace_root(probe: Callable[..., dict[str, Any]]) -> bool:
    """Decide the call shape from ``probe``'s signature, never from a caught error.

    up0824-ppd-r1-f02: catching ``TypeError`` raised by the probe body itself
    and re-invoking without ``workspace_root`` cannot tell "this probe does
    not accept that keyword" apart from "this probe's own internals raised a
    TypeError" — either way it silently runs the probe a second time, masking
    the bug and double-paying any side effect (SSH round trip, subprocess).
    Inspecting the signature decides the call shape before the probe has run
    at all, so it never gets invoked twice.
    """
    try:
        parameters = inspect.signature(probe).parameters.values()
    except (TypeError, ValueError):
        # Not introspectable (e.g. some builtins/partials): keep the
        # historical default of passing workspace_root.
        return True
    return any(param.name == "workspace_root" or param.kind is inspect.Parameter.VAR_KEYWORD for param in parameters)


def _invoke_probe(
    name: str,
    *,
    probe: Callable[..., dict[str, Any]],
    workspace_root: Path | None,
) -> dict[str, Any]:
    if workspace_root is None or not _accepts_workspace_root(probe):
        return probe(name)
    return probe(name, workspace_root=workspace_root)


def _expired_result(
    name: str,
    *,
    deadline_s: float,
    elapsed_s: float,
    deadline_kind: str,
) -> dict[str, Any]:
    if deadline_kind == "aggregate":
        detail = (
            "availability probe unresolved when the aggregate probe deadline of "
            f"{_fmt_deadline_s(deadline_s)} expired; treating as unavailable for routing"
        )
    else:
        detail = (
            f"availability probe did not answer within {_fmt_deadline_s(deadline_s)}; "
            "treating as unavailable for routing"
        )
    _logger.warning(
        "availability_probe_deadline_expired",
        extra={
            "probe_name": name,
            "probe_deadline_kind": deadline_kind,
            "probe_deadline_s": deadline_s,
            "probe_elapsed_s": elapsed_s,
        },
    )
    return {
        "available": False,
        "is_available": False,
        "state": "unknown",
        "availability_state": "unknown",
        "detail": detail,
        "probe_deadline_s": deadline_s,
        "probe_elapsed_s": elapsed_s,
        "probe_expired": True,
    }


def _completed_result(
    name: str,
    outcome: dict[str, Any],
    *,
    deadline_s: float,
    elapsed_s: float,
) -> dict[str, Any]:
    if "error" not in outcome and not isinstance(outcome.get("result"), Mapping):
        # up0824-ppd-local-f01: a probe that returns a non-mapping (e.g. None)
        # used to reach `dict()` below with no guard, raising TypeError on the
        # CALLER's thread (not the probe's own try/except in _run_probe), so
        # it crashed key_info_admission_gate / worker_start / run_offload_pass
        # admission outright instead of degrading. Route it through the same
        # typed error branch as a raised exception.
        bad_result = outcome.get("result")
        outcome = {"error": TypeError(f"probe for {name!r} returned {type(bad_result).__name__}, expected a mapping")}
    if "error" in outcome:
        probe_exc = outcome["error"]
        # up0824-ppd-r2-f02: a dead/erroring probe must not fail open silently
        # — only _expired_result logged before this fix, so a raised-exception
        # (or now, a non-mapping) probe result was invisible to operators.
        _logger.warning(
            "availability_probe_failed",
            extra={
                "probe_name": name,
                "probe_deadline_s": deadline_s,
                "probe_elapsed_s": elapsed_s,
                "probe_error": f"{type(probe_exc).__name__}: {probe_exc}",
            },
        )
        return {
            "available": False,
            "is_available": False,
            "state": "error",
            "availability_state": "error",
            "detail": (
                f"availability probe raised {type(probe_exc).__name__}: {probe_exc}; "
                "treating as unavailable for routing"
            ),
            "probe_deadline_s": deadline_s,
            "probe_elapsed_s": elapsed_s,
            "probe_expired": False,
        }
    result = dict(outcome["result"])
    result.update(
        {
            "probe_deadline_s": deadline_s,
            "probe_elapsed_s": elapsed_s,
            "probe_expired": False,
        }
    )
    return result


def _start_or_join_probe(
    name: str,
    *,
    probe: Callable[..., dict[str, Any]],
    workspace_root: Path | None,
) -> tuple[threading.Event, dict[str, Any]]:
    """Start one probe thread for ``name``, or join an already-running one.

    up0824-ppd-r1-f04: without this fence, every caller against a hung
    backend started its own daemon thread; none of them were ever joined
    after expiry, so a hung backend accumulated one in-flight probe thread
    per caller, each of which could still mutate shared backend_registry
    caches whenever (if ever) it eventually resolved, long after its own
    caller had already timed out and moved on. Coalescing to one thread per
    name bounds that to a single thread and a single late mutation, no matter
    how many callers hit the same hung backend.

    Only entries whose probe has not yet finished (``done`` unset) are
    joined; an entry whose probe already completed is treated as free and
    replaced, so a finished probe can never be handed to a caller expecting a
    fresh run of a possibly-different probe callable.
    """
    key = (name, probe)
    with _inflight_lock:
        existing = _inflight.get(key)
        if existing is not None and not existing[0].is_set():
            return existing
        done = threading.Event()
        outcome: dict[str, Any] = {}

        def _run_probe() -> None:
            try:
                outcome["result"] = _invoke_probe(name, probe=probe, workspace_root=workspace_root)
            except BaseException as exc:  # degrade a failed probe; never raise from its daemon thread
                outcome["error"] = exc
            finally:
                done.set()
                with _inflight_lock:
                    if _inflight.get(key) is (done, outcome):
                        del _inflight[key]

        _inflight[key] = (done, outcome)
        threading.Thread(target=_run_probe, name=f"backend-avail-probe-{name}", daemon=True).start()
        return done, outcome


def bounded_probe(
    name: str,
    *,
    probe: Callable[..., dict[str, Any]],
    workspace_root: Path | None,
    deadline_s: float | None = None,
) -> dict[str, Any]:
    """Run one availability probe without blocking beyond its deadline."""
    resolved_deadline = (
        probe_deadline_from_env(PROBE_TIMEOUT_ENV, DEFAULT_PROBE_TIMEOUT_S) if deadline_s is None else deadline_s
    )
    started = time.monotonic()
    done, outcome = _start_or_join_probe(name, probe=probe, workspace_root=workspace_root)
    if not done.wait(resolved_deadline):
        elapsed = time.monotonic() - started
        return _expired_result(
            name,
            deadline_s=resolved_deadline,
            elapsed_s=elapsed,
            deadline_kind="per_probe",
        )
    return _completed_result(
        name,
        outcome,
        deadline_s=resolved_deadline,
        elapsed_s=time.monotonic() - started,
    )


def bounded_probe_many(
    names: Iterable[str],
    *,
    probe: Callable[..., dict[str, Any]],
    workspace_root: Path | None,
    per_probe_s: float | None = None,
    aggregate_s: float | None = None,
) -> dict[str, dict[str, Any]]:
    """Start probes concurrently and collect them within per-probe and aggregate bounds."""
    resolved_per_probe = (
        probe_deadline_from_env(PROBE_TIMEOUT_ENV, DEFAULT_PROBE_TIMEOUT_S) if per_probe_s is None else per_probe_s
    )
    resolved_aggregate = (
        probe_deadline_from_env(PROBE_AGGREGATE_TIMEOUT_ENV, DEFAULT_PROBE_AGGREGATE_TIMEOUT_S)
        if aggregate_s is None
        else aggregate_s
    )
    slots: dict[str, tuple[threading.Event, dict[str, Any]]] = {}
    started = time.monotonic()

    for name in names:
        slots[name] = _start_or_join_probe(name, probe=probe, workspace_root=workspace_root)

    results: dict[str, dict[str, Any]] = {}
    for name, (done, outcome) in slots.items():
        elapsed = time.monotonic() - started
        aggregate_remaining = resolved_aggregate - elapsed
        aggregate_binds = aggregate_remaining <= resolved_per_probe
        wait_s = max(0.0, min(resolved_per_probe, aggregate_remaining))
        if not done.wait(wait_s):
            elapsed = time.monotonic() - started
            deadline_kind = "aggregate" if aggregate_binds else "per_probe"
            deadline_s = resolved_aggregate if aggregate_binds else resolved_per_probe
            results[name] = _expired_result(
                name,
                deadline_s=deadline_s,
                elapsed_s=elapsed,
                deadline_kind=deadline_kind,
            )
            continue
        results[name] = _completed_result(
            name,
            outcome,
            deadline_s=resolved_per_probe,
            elapsed_s=time.monotonic() - started,
        )
    return results
