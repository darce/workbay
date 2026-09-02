"""Durable per-backend circuit breaker for worker dispatches."""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from collections.abc import Callable, MutableMapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .learned_wave_cap import bounded_file_lock

STATE_FILE = ".task-state/dispatch-breaker.json"
TRANSPORT_THRESHOLD = 3
CONFIGURATION_THRESHOLD = 1
COOLDOWN_S = 300.0
LOCK_DEADLINE_S = 2.0

_TRANSPORT_THRESHOLD_ENV = "WORKBAY_DISPATCH_BREAKER_TRANSPORT_THRESHOLD"
_COOLDOWN_ENV = "WORKBAY_DISPATCH_BREAKER_COOLDOWN_S"
_RESET_ENV = "WORKBAY_DISPATCH_BREAKER_RESET"
_STATE_PATH_ENV = "WORKBAY_DISPATCH_BREAKER_STATE_PATH"

logger = logging.getLogger(__name__)

_DETERMINISTIC_REFUSAL_RE = re.compile(
    r"refusing \S+ dispatch with (a build model|retired model|model)",
    re.IGNORECASE,
)
_DETERMINISTIC_REFUSAL_PHRASES = (
    "grok-build family refused",
    "allowed is the configured pin",
    "allowed is the curated allow-list",
)


def is_deterministic_dispatch_refusal(text: str) -> bool:
    """Return True when *text* is a deterministic model-pin dispatch refusal."""
    haystack = str(text or "")
    if not haystack.strip():
        return False
    if _DETERMINISTIC_REFUSAL_RE.search(haystack):
        return True
    lowered = haystack.lower()
    return any(phrase in lowered for phrase in _DETERMINISTIC_REFUSAL_PHRASES)


@dataclass(frozen=True)
class BreakerState:
    """Persisted state for one backend."""

    state: str = "closed"
    failure_count: int = 0
    failure_class: str | None = None
    opened_at: float | None = None
    trial_in_progress: bool = False
    trial_started_at: float | None = None


@dataclass(frozen=True)
class Admission:
    """The breaker decision returned to a dispatch caller."""

    allowed: bool
    state: str
    reason: str
    failure_class: str | None


def _state_path(root: str | os.PathLike[str]) -> Path:
    configured = str(os.environ.get(_STATE_PATH_ENV) or "").strip()
    return Path(configured) if configured else Path(root) / STATE_FILE


def _closed_state() -> BreakerState:
    return BreakerState()


def _configuration_key(backend_id: str, model: str | None) -> str:
    normalized_model = str(model or "").strip()
    return str(backend_id) if not normalized_model else f"{backend_id}::configuration::{normalized_model}"


def _coerce_state(value: object) -> BreakerState:
    if not isinstance(value, dict):
        return _closed_state()
    state = str(value.get("state") or "closed")
    if state not in {"closed", "open", "half_open"}:
        return _closed_state()
    try:
        failure_count = max(0, int(value.get("failure_count") or 0))
        opened_at_raw = value.get("opened_at")
        opened_at = float(opened_at_raw) if opened_at_raw is not None else None
        trial_started_at_raw = value.get("trial_started_at")
        trial_started_at = float(trial_started_at_raw) if trial_started_at_raw is not None else None
    except (TypeError, ValueError):
        return _closed_state()
    failure_class_raw = value.get("failure_class")
    failure_class = str(failure_class_raw) if failure_class_raw in {"configuration", "transport"} else None
    return BreakerState(
        state=state,
        failure_count=failure_count,
        failure_class=failure_class,
        opened_at=opened_at,
        trial_in_progress=bool(value.get("trial_in_progress", False)),
        trial_started_at=trial_started_at,
    )


def _read_data(root: str | os.PathLike[str]) -> dict[str, dict[str, Any]]:
    """Read durable state, degrading unreadable/torn data to an empty breaker.

    Transport failures must never become a permanent dispatch brick merely
    because the diagnostic state file was interrupted or became unreadable.
    Atomic writes keep normal readers from observing partial JSON; this fallback
    covers crashes and external edits that pre-date a replacement.
    """
    path = _state_path(root)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    if not isinstance(value, dict):
        return {}
    return {str(key): row for key, row in value.items() if isinstance(row, dict)}


def _write_data(root: str | os.PathLike[str], data: dict[str, dict[str, Any]]) -> None:
    path = _state_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a writer-specific temporary name and replace atomically so concurrent
    # readers never observe a half-written state file.
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _store_state(root: str | os.PathLike[str], backend_id: str, state: BreakerState) -> None:
    with bounded_file_lock(_state_path(root), deadline_s=LOCK_DEADLINE_S):
        data = _read_data(root)
        data[str(backend_id)] = asdict(state)
        _write_data(root, data)


def _log_transition(backend_id: str, old: BreakerState, new: BreakerState) -> None:
    if old.state == new.state:
        return
    failure_class = new.failure_class or old.failure_class or "none"
    logger.info(
        "dispatch breaker backend_id=%s failure_class=%s failure_count=%s transition %s -> %s",
        backend_id,
        failure_class,
        new.failure_count,
        old.state,
        new.state,
        extra={"transition_from": old.state, "transition_to": new.state},
    )


def _positive_env_number(name: str, default: int | float, cast: type[int] | type[float]) -> int | float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = cast(raw)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _settings() -> tuple[int, float, str | None]:
    try:
        threshold = int(_positive_env_number(_TRANSPORT_THRESHOLD_ENV, TRANSPORT_THRESHOLD, int))
        cooldown = float(_positive_env_number(_COOLDOWN_ENV, COOLDOWN_S, float))
    except (TypeError, ValueError) as exc:
        return TRANSPORT_THRESHOLD, COOLDOWN_S, f"invalid dispatch breaker environment override: {exc}"
    return threshold, cooldown, None


def classify_failure(result: dict[str, object]) -> str | None:
    """Classify a completed dispatch without treating exit 75 as sufficient."""
    if result.get("ok") is True:
        return None
    if result.get("outcome") == "disk_headroom_refused":
        return None
    raw = result.get("raw_payload") if isinstance(result.get("raw_payload"), dict) else {}
    defer_reason = str(raw.get("defer_reason") or "").lower()
    if defer_reason in {
        "vm_lane_cap",
        "vm_memory_pressure",
        "same_branch_lane_active",
        "sandbox_occupied",
        "host_memory_guard",
        "unknown",
    }:
        return "neutral"
    if defer_reason.startswith("residual_timeout_"):
        return "neutral"

    resumable_reason = str(raw.get("resumable_reason") or "").lower()
    if resumable_reason == "wall_clock_expiry":
        return "transport"

    blockers = result.get("blockers")
    blocker_text = " ".join(str(item) for item in blockers) if isinstance(blockers, list) else ""
    text = f"{result.get('error') or ''} {blocker_text} {result.get('outcome') or ''}".lower()
    configuration_markers = (
        "resolve_effective_model",
        "allowed_models",
        "workbay-fm-08",
        "writable_roots",
        "missing binary",
        "malformed backend spec",
    )
    if is_deterministic_dispatch_refusal(text) or any(marker in text for marker in configuration_markers):
        return "configuration"
    transport_markers = (
        "ssh connection failed",
        "vm unreachable",
        "wall-clock expiry",
        "wall clock expiry",
        "http 402",
        "out of usage",
        "rate_limited",
        "quota",
        "timed out after",
        "connection refused",
    )
    if any(marker in text for marker in transport_markers):
        return "transport"
    return None


def record_failure(
    root: str | os.PathLike[str],
    backend_id: str,
    failure_class: str,
    *,
    model: str | None = None,
    now: Callable[[], float] = time.time,
) -> BreakerState:
    """Record one classified outcome and return the resulting state."""
    with bounded_file_lock(_state_path(root), deadline_s=LOCK_DEADLINE_S):
        data = _read_data(root)
        state_key = _configuration_key(backend_id, model) if failure_class == "configuration" else str(backend_id)
        old = _coerce_state(data.get(state_key))
        if failure_class == "neutral":
            return old
        if failure_class not in {"configuration", "transport"}:
            raise ValueError(f"unknown dispatch failure class: {failure_class}")

        threshold, _, settings_error = _settings()
        if settings_error is not None:
            new = BreakerState("open", old.failure_count, failure_class, now(), False, None)
        elif failure_class == "configuration":
            count = old.failure_count + 1 if old.failure_class == failure_class else CONFIGURATION_THRESHOLD
            new = BreakerState("open", count, failure_class, now(), False, None)
        else:
            count = old.failure_count + 1 if old.failure_class == failure_class else 1
            if old.state in {"open", "half_open"} or count >= threshold:
                new = BreakerState("open", count, "transport", now(), False, None)
            else:
                new = BreakerState("closed", count, "transport", None, False, None)
        data[state_key] = asdict(new)
        _write_data(root, data)
    _log_transition(str(backend_id), old, new)
    return new


def record_success(
    root: str | os.PathLike[str],
    backend_id: str,
    *,
    now: Callable[[], float] = time.time,
) -> BreakerState:
    """Close the backend breaker and clear its consecutive-failure count."""
    del now  # Symmetric injectable seam; success does not need a timestamp.
    with bounded_file_lock(_state_path(root), deadline_s=LOCK_DEADLINE_S):
        data = _read_data(root)
        old = _coerce_state(data.get(str(backend_id)))
        new = _closed_state()
        data[str(backend_id)] = asdict(new)
        _write_data(root, data)
    _log_transition(str(backend_id), old, new)
    return new


def admit(
    root: str | os.PathLike[str],
    backend_id: str,
    *,
    model: str | None = None,
    now: Callable[[], float] = time.time,
) -> Admission:
    """Return whether this backend may start its next sequential execute."""
    _, cooldown, settings_error = _settings()
    if settings_error is not None:
        return Admission(False, "open", settings_error, None)

    with bounded_file_lock(_state_path(root), deadline_s=LOCK_DEADLINE_S):
        data = _read_data(root)
        configuration = _coerce_state(data.get(_configuration_key(backend_id, model)))
        if configuration.state == "open" and configuration.failure_class == "configuration":
            return Admission(False, "open", "configuration breaker requires operator reset", "configuration")
        old = _coerce_state(data.get(str(backend_id)))
        if old.state == "closed":
            return Admission(True, "closed", "breaker closed", old.failure_class)
        if old.failure_class == "configuration":
            return Admission(False, "open", "configuration breaker requires operator reset", "configuration")
        if old.state == "half_open":
            trial_started_at = old.trial_started_at if old.trial_started_at is not None else now()
            if now() - trial_started_at < cooldown:
                return Admission(False, "half_open", "half-open trial already in progress", old.failure_class)
            new = BreakerState("half_open", old.failure_count, "transport", old.opened_at, True, now())
            data[str(backend_id)] = asdict(new)
            _write_data(root, data)
            return Admission(True, "half_open", "transport breaker half-open trial re-armed", "transport")

        opened_at = old.opened_at if old.opened_at is not None else now()
        if now() - opened_at < cooldown:
            return Admission(False, "open", "transport breaker cooldown has not elapsed", old.failure_class)
        new = BreakerState("half_open", old.failure_count, "transport", old.opened_at, True, now())
        data[str(backend_id)] = asdict(new)
        _write_data(root, data)
    _log_transition(str(backend_id), old, new)
    return Admission(True, "half_open", "transport breaker half-open trial", "transport")


def _configuration_keys(data: dict[str, dict[str, Any]], backend_id: str | None) -> list[str]:
    """Return durable keys that may hold a configuration latch for *backend_id*.

    A missing backend is not a host-wide scan: other backends' bulkheads stay
    isolated. Callers that lack identity must inspect nothing rather than
    inherit an unrelated latch.
    """
    normalized_backend = str(backend_id or "").strip()
    if not normalized_backend:
        return []
    prefix = f"{normalized_backend}::configuration::"
    keys = [normalized_backend]
    keys.extend(sorted(key for key in data if key.startswith(prefix)))
    return keys


def _first_open_configuration(
    data: dict[str, dict[str, Any]],
    backend_id: str | None,
    model: str | None,
) -> tuple[str, BreakerState] | None:
    """Return the first open configuration latch that applies to this identity.

    Known backend+model matches ``admit``: the scoped model key plus an unscoped
    ``{backend}`` configuration row. A missing model keeps the same-backend
    scan so an unkeyed consult still fail-closes on that backend. A missing
    backend inspects nothing.
    """
    normalized_backend = str(backend_id or "").strip()
    normalized_model = str(model or "").strip()
    if not normalized_backend:
        return None
    if normalized_model:
        ordered = [
            _configuration_key(normalized_backend, normalized_model),
            normalized_backend,
        ]
    else:
        ordered = _configuration_keys(data, normalized_backend)
    seen: set[str] = set()
    for key in ordered:
        if key in seen:
            continue
        seen.add(key)
        state = _coerce_state(data.get(key))
        if state.state == "open" and state.failure_class == "configuration":
            return key, state
    return None


def inspect(
    root: str | os.PathLike[str],
    backend_id: str | None = None,
    *,
    model: str | None = None,
    now: Callable[[], float] = time.time,
) -> Admission:
    """Return the current breaker decision without arming a half-open trial.

    Guidance consults must call this instead of :func:`admit`. A known
    backend with a missing model fail-closes on any of that backend's
    configuration keys. A known backend and model match :func:`admit`
    (scoped model key plus an unscoped configuration row). A missing
    backend inspects nothing. Transport state is reported as-is and
    never written.
    """
    _, cooldown, settings_error = _settings()
    if settings_error is not None:
        return Admission(False, "open", settings_error, None)

    data = _read_data(root)
    open_config = _first_open_configuration(data, backend_id, model)
    if open_config is not None:
        key, _state = open_config
        return Admission(
            False,
            "open",
            f"configuration breaker {key} is open; operator reset required",
            "configuration",
        )

    normalized_backend = str(backend_id or "").strip()
    if not normalized_backend:
        return Admission(True, "closed", "breaker closed", None)

    old = _coerce_state(data.get(normalized_backend))
    if old.state == "closed":
        return Admission(True, "closed", "breaker closed", old.failure_class)
    if old.state == "half_open":
        return Admission(False, "half_open", "half-open trial already in progress", old.failure_class)
    opened_at = old.opened_at if old.opened_at is not None else now()
    if now() - opened_at < cooldown:
        return Admission(False, "open", "transport breaker cooldown has not elapsed", old.failure_class)
    return Admission(False, "open", "transport breaker cooldown elapsed; trial not armed", old.failure_class or "transport")


def breaker_state(root: str | os.PathLike[str], backend_id: str) -> dict[str, Any]:
    """Return the persisted state shape for one backend."""
    return asdict(_coerce_state(_read_data(root).get(str(backend_id))))


def reset(root: str | os.PathLike[str], backend_id: str) -> list[str]:
    """Reset one backend, or all persisted backends when ``backend_id == '*'``."""
    with bounded_file_lock(_state_path(root), deadline_s=LOCK_DEADLINE_S):
        data = _read_data(root)
        targets = (
            sorted(data)
            if backend_id == "*"
            else sorted(key for key in data if key == str(backend_id) or key.startswith(f"{backend_id}::configuration::"))
        )
        for target in targets:
            old = _coerce_state(data.pop(target, None))
            _log_transition(target, old, _closed_state())
        if targets:
            _write_data(root, data)
    return targets


def consume_reset_env(
    root: str | os.PathLike[str],
    environ: MutableMapping[str, str],
) -> list[str]:
    """Consume the one-shot operator reset variable and reset its target."""
    target = environ.pop(_RESET_ENV, None)
    if target is None or not str(target).strip():
        return []
    return reset(root, str(target).strip())
