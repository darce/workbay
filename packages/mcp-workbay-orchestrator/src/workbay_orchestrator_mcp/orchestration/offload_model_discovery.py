"""Off-box model catalogue discovery (implementation note M2/M3).

Catalogue parse, remote-gate listing, TTL cache, pin selection, and scoped
publish live here so ``offload_profiles`` stays the profile/bound table.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import subprocess
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any

from workbay_orchestrator_mcp.orchestration import backend_registry
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
    seed_cursor_effort_slugs_from_catalogue,
)

_LOGGER = logging.getLogger(__name__)

MODEL_DISCOVERY_FAILED_WARNING = "model_discovery_failed"
TRACKED_PIN_NOT_IN_CATALOGUE_WARNING = "tracked_pin_not_in_catalogue"
PIN_HOME_UNDECLARED = "pin_home_undeclared"
MODEL_DISCOVERY_TTL_S = 60.0

# SSH ConnectTimeout must sit under the process budget. An 8s subprocess
# kill classified a still-connecting gate as MODEL_DISCOVERY_FAILED even
# though the VM would have answered within the declared 10s connect window.
SSH_CONNECT_TIMEOUT_S = 10
LOCAL_LIST_MODELS_TIMEOUT_S = 8.0
REMOTE_LIST_MODELS_LISTING_BUDGET_S = 8.0
REMOTE_LIST_MODELS_PROCESS_TIMEOUT_S = SSH_CONNECT_TIMEOUT_S + REMOTE_LIST_MODELS_LISTING_BUDGET_S

_REMOTE_LIST_MODELS_SSH_OPTS: tuple[str, ...] = (
    "-o",
    "BatchMode=yes",
    "-o",
    f"ConnectTimeout={SSH_CONNECT_TIMEOUT_S}",
    "-o",
    "ServerAliveInterval=30",
    "-o",
    "ServerAliveCountMax=4",
)
_REMOTE_LIST_MODELS_PATH = "$HOME/.grok/bin:$HOME/.local/bin:$PATH"
_EFFORT_TOKENS: tuple[str, ...] = ("xhigh", "high", "medium", "low")
_VERSIONISH_RE = re.compile(r"^\d+(?:\.\d+)*$")
_TRAILING_QUALIFIER = frozenset({"preview", "latest", "sol", "fast", "rc"})
_MODEL_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+/-]*$")
_NOT_A_MODEL_SLUG = frozenset(
    {
        "you",
        "usage",
        "error",
        "warning",
        "model",
        "models",
        "id",
        "name",
        "available",
        "unavailable",
    }
)

_discovery_cache: dict[tuple[str, str, str], tuple[float, "ModelDiscovery"]] = {}
_cache_lock = threading.Lock()
_publish_lock = threading.Lock()


class PinHomeUndeclaredError(RuntimeError):
    """Dispatchable backend has no declared list-argv / tracked pin."""

    warning = PIN_HOME_UNDECLARED


@dataclass(frozen=True)
class PinHome:
    """Registry-declared catalogue home for one backend."""

    tracked_pin: str
    env_key: str
    list_argv: tuple[str, ...]


@dataclass(frozen=True)
class ModelDiscovery:
    """Resolved pin for one off-box backend after env / catalogue / tracked."""

    backend_id: str
    resolved_model: str
    tracked_pin: str
    catalogue: tuple[str, ...]
    source: str  # env | discovery | tracked
    warning: str | None = None
    gate_host: str | None = None


def _spec_declares_dispatchable_off_box(spec: Any) -> bool:
    caps = getattr(spec, "capabilities", None)
    return bool(getattr(caps, "dispatchable_off_box", False))


def _backend_probe_runs_off_box(backend_id: str) -> bool:
    spec = backend_registry.BACKENDS.get(backend_id)
    return spec is not None and _spec_declares_dispatchable_off_box(spec)


def pin_home_for(backend_id: str) -> PinHome:
    """Read list-argv / env / tracked-pin from the registry row.

    A dispatchable backend that omits the fields fails closed with
    :class:`PinHomeUndeclaredError` — never a swallowed ``KeyError``.
    """
    spec = backend_registry.BACKENDS.get(backend_id)
    if spec is None:
        raise KeyError(f"no model-pin home for backend {backend_id!r}")
    argv = getattr(spec, "list_models_argv", None)
    tracked = (getattr(spec, "tracked_model", None) or "").strip()
    env_key = (getattr(spec, "allowed_model_env", None) or "").strip()
    if argv and tracked and env_key:
        return PinHome(tracked_pin=tracked, env_key=env_key, list_argv=tuple(argv))
    if _spec_declares_dispatchable_off_box(spec):
        raise PinHomeUndeclaredError(
            f"{PIN_HOME_UNDECLARED}: backend {backend_id!r} is "
            "dispatchable_off_box but declares no list-argv/tracked pin"
        )
    raise KeyError(f"no model-pin home for backend {backend_id!r}")


def _looks_like_model_slug(slug: str) -> bool:
    """Refuse English/help-text tokens so a failed CLI cannot invent a pin."""
    if not slug or slug.lower() in _NOT_A_MODEL_SLUG:
        return False
    if any(ch.isspace() for ch in slug):
        return False
    if not _MODEL_SLUG_RE.fullmatch(slug):
        return False
    return ("-" in slug) or ("." in slug)


def _catalogue_item_slug(item: Any) -> str | None:
    if isinstance(item, str):
        slug = item.strip()
        return slug if _looks_like_model_slug(slug) else None
    if isinstance(item, dict):
        for key in ("id", "model", "name", "slug"):
            value = item.get(key)
            if isinstance(value, str) and _looks_like_model_slug(value.strip()):
                return value.strip()
    return None


def _slugs_from_json(data: Any) -> list[str]:
    slugs: list[str] = []
    items: Any = None
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("models")
        if items is None:
            items = data.get("data")
    if isinstance(items, list):
        for item in items:
            slug = _catalogue_item_slug(item)
            if slug:
                slugs.append(slug)
    return slugs


def parse_model_catalogue(stdout: str) -> tuple[str, ...]:
    """Parse a CLI listing into published slugs. Never invents a slug."""
    text = (stdout or "").strip()
    if not text:
        return ()
    slugs: list[str] = []
    if text[0] in "[{":
        try:
            slugs = _slugs_from_json(json.loads(text))
        except json.JSONDecodeError:
            slugs = []
    if not slugs:
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            candidate = _catalogue_item_slug(line.split()[0])
            if candidate:
                slugs.append(candidate)
    return tuple(dict.fromkeys(slugs))


def _family_stem(stem: str) -> str:
    kept: list[str] = []
    for part in stem.split("-"):
        lowered = part.lower()
        if _VERSIONISH_RE.fullmatch(part):
            break
        if lowered in _TRAILING_QUALIFIER or lowered.startswith("rc"):
            break
        kept.append(part)
    return "-".join(kept) if kept else stem


def slug_shape(slug: str) -> tuple[str, str | None, bool]:
    """Return ``(family, effort, fast)`` for same-shape catalogue matching."""
    text = (slug or "").strip()
    fast = text.endswith("-fast")
    stem = text[: -len("-fast")] if fast else text
    effort: str | None = None
    for token in _EFFORT_TOKENS:
        suffix = f"-{token}"
        if stem.endswith(suffix) and len(stem) > len(suffix):
            effort = token
            stem = stem[: -len(suffix)]
            break
    return _family_stem(stem), effort, fast


def _common_prefix_len(left: str, right: str) -> int:
    n = 0
    for a, b in zip(left, right, strict=False):
        if a != b:
            break
        n += 1
    return n


def _same_shape_match(tracked_pin: str, published: Sequence[str]) -> str | None:
    """Prefer a unique same-family / same-effort / same-fast published slug."""
    want = slug_shape(tracked_pin)
    if not want[0]:
        return None
    matches = [slug for slug in published if slug_shape(slug) == want]
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    ranked = sorted(
        matches,
        key=lambda slug: (_common_prefix_len(slug, tracked_pin), len(slug)),
        reverse=True,
    )
    best = ranked[0]
    tied = [slug for slug in ranked if _common_prefix_len(slug, tracked_pin) == _common_prefix_len(best, tracked_pin)]
    if len(tied) != 1:
        return None
    return best


def select_from_catalogue(
    catalogue: Sequence[str],
    *,
    tracked_pin: str,
    env_override: str | None,
) -> tuple[str, str, str | None]:
    """Return ``(resolved, source, warning)``. Never invents a slug."""
    published = tuple(slug.strip() for slug in catalogue if isinstance(slug, str) and slug.strip())
    if env_override:
        return env_override, "env", None
    if not published:
        return tracked_pin, "tracked", MODEL_DISCOVERY_FAILED_WARNING
    if tracked_pin in published:
        return tracked_pin, "discovery", None
    match = _same_shape_match(tracked_pin, published)
    if match is not None:
        return match, "discovery", None
    return tracked_pin, "tracked", TRACKED_PIN_NOT_IN_CATALOGUE_WARNING


def _resolve_offbox_probe_host() -> str:
    """Host a normal remote turn uses (env, then ``.workbay/remote-gate.env``)."""
    from workbay_protocol.remote_probe import resolve_remote_gate_host  # noqa: PLC0415

    host = (resolve_remote_gate_host(None) or "").strip()
    if not host:
        repo_root = backend_registry._resolve_remote_probe_repo_root()
        host = (resolve_remote_gate_host(repo_root) or "").strip()
    if not host:
        raise RuntimeError(
            "model catalogue probe: remote gate host is not configured "
            "(set WORKBAY_REMOTE_GATE_HOST); cannot list models on the VM"
        )
    if host.startswith("-") or any(ch.isspace() for ch in host):
        raise RuntimeError(f"model catalogue probe: remote gate host is malformed: {host!r}")
    return host


def resolve_probe_gate_host(backend_id: str) -> str | None:
    """Gate host identity for cache keys and receipts. Empty when not off-box."""
    if not _backend_probe_runs_off_box(backend_id):
        return None
    try:
        return _resolve_offbox_probe_host()
    except RuntimeError:
        return None


def build_remote_list_models_argv(argv: Sequence[str], *, host: str) -> list[str]:
    """SSH argv that runs the listing command on the remote gate VM."""
    remote_cmd = f"export PATH={_REMOTE_LIST_MODELS_PATH}; " + " ".join(shlex.quote(str(part)) for part in argv)
    return ["ssh", *_REMOTE_LIST_MODELS_SSH_OPTS, "--", host, remote_cmd]


def _run_completed(argv: Sequence[str], *, timeout_s: float, label: str) -> str:
    try:
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"{label}: {exc}") from exc
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(f"{label} {list(argv)!r} exited {completed.returncode}: {stderr}")
    return completed.stdout or ""


def _run_list_models_locally(argv: Sequence[str], *, timeout_s: float = LOCAL_LIST_MODELS_TIMEOUT_S) -> str:
    return _run_completed(argv, timeout_s=timeout_s, label="model catalogue probe failed")


def _run_list_models_on_remote_gate(
    argv: Sequence[str], *, timeout_s: float = REMOTE_LIST_MODELS_PROCESS_TIMEOUT_S
) -> str:
    host = _resolve_offbox_probe_host()
    transport = build_remote_list_models_argv(argv, host=host)
    return _run_completed(transport, timeout_s=timeout_s, label="model catalogue probe failed on remote gate")


def _run_list_models(
    argv: Sequence[str],
    *,
    timeout_s: float | None = None,
    backend_id: str | None = None,
) -> str:
    """Run a listing argv locally, or on the VM when *backend_id* is off-box."""
    if backend_id and _backend_probe_runs_off_box(backend_id):
        return _run_list_models_on_remote_gate(
            argv,
            timeout_s=REMOTE_LIST_MODELS_PROCESS_TIMEOUT_S if timeout_s is None else timeout_s,
        )
    return _run_list_models_locally(
        argv,
        timeout_s=LOCAL_LIST_MODELS_TIMEOUT_S if timeout_s is None else timeout_s,
    )


def _cache_key(backend_id: str, gate_host: str | None, env_override: str | None) -> tuple[str, str, str]:
    return (backend_id, gate_host or "", env_override or "")


def _lookup_cached(
    key: tuple[str, str, str],
    *,
    tracked_pin: str,
    env_override: str | None,
) -> ModelDiscovery | None:
    with _cache_lock:
        cached = _discovery_cache.get(key)
        if cached is None:
            return None
        expires_at, discovery = cached
        if time.monotonic() >= expires_at:
            _discovery_cache.pop(key, None)
            return None
    if env_override and discovery.resolved_model != env_override:
        return replace(
            discovery,
            resolved_model=env_override,
            tracked_pin=tracked_pin,
            source="env",
            warning=None,
        )
    return discovery


def _store_cached(key: tuple[str, str, str], discovery: ModelDiscovery) -> None:
    with _cache_lock:
        _discovery_cache[key] = (time.monotonic() + MODEL_DISCOVERY_TTL_S, discovery)


def clear_model_discovery_cache() -> None:
    """Drop the TTL cache (tests / host change)."""
    with _cache_lock:
        _discovery_cache.clear()


def _probe_catalogue(
    backend_id: str,
    argv: Sequence[str],
    *,
    runner: Callable[[Sequence[str]], str] | None,
) -> tuple[tuple[str, ...], str | None]:
    try:

        def _default_runner(listing_argv: Sequence[str]) -> str:
            return _run_list_models(listing_argv, backend_id=backend_id)

        raw = (runner or _default_runner)(argv)
        published = parse_model_catalogue(raw)
        if not published:
            return (), MODEL_DISCOVERY_FAILED_WARNING
        return published, None
    except (RuntimeError, OSError, TypeError, ValueError) as exc:
        _LOGGER.warning(
            "%s: %s; degrading to tracked pin",
            MODEL_DISCOVERY_FAILED_WARNING,
            exc,
        )
        return (), MODEL_DISCOVERY_FAILED_WARNING


def _finish_discovery(
    *,
    backend_id: str,
    tracked_pin: str,
    published: tuple[str, ...],
    env_override: str | None,
    warning: str | None,
    gate_host: str | None,
    cache: bool,
    cache_key: tuple[str, str, str],
) -> ModelDiscovery:
    resolved, source, select_warning = select_from_catalogue(
        published, tracked_pin=tracked_pin, env_override=env_override
    )
    warning = warning or select_warning
    if warning == MODEL_DISCOVERY_FAILED_WARNING and source != "env":
        resolved, source = tracked_pin, "tracked"
    discovery = ModelDiscovery(
        backend_id=backend_id,
        resolved_model=resolved,
        tracked_pin=tracked_pin,
        catalogue=published,
        source=source,
        warning=warning,
        gate_host=gate_host,
    )
    if cache:
        _store_cached(cache_key, discovery)
    return discovery


def _resolve_from_listing(
    backend_id: str,
    pin: PinHome,
    *,
    catalogue: Sequence[str] | None,
    probe: bool,
    runner: Callable[[Sequence[str]], str] | None,
    env_override: str | None,
    gate_host: str | None,
    cache: bool,
    cache_key: tuple[str, str, str],
) -> ModelDiscovery:
    if catalogue is not None:
        published = tuple(slug.strip() for slug in catalogue if isinstance(slug, str) and slug.strip())
        warning = MODEL_DISCOVERY_FAILED_WARNING if not published else None
    else:
        published, warning = _probe_catalogue(backend_id, pin.list_argv, runner=runner)
    return _finish_discovery(
        backend_id=backend_id,
        tracked_pin=pin.tracked_pin,
        published=published,
        env_override=env_override,
        warning=warning,
        gate_host=gate_host,
        cache=cache,
        cache_key=cache_key,
    )


def resolve_offbox_model(
    backend_id: str,
    *,
    catalogue: Sequence[str] | None = None,
    probe: bool = False,
    runner: Callable[[Sequence[str]], str] | None = None,
    cache: bool = True,
) -> ModelDiscovery:
    """Resolve one backend's pin: env > catalogue/probe > tracked."""
    pin = pin_home_for(backend_id)
    env_override = (os.environ.get(pin.env_key) or "").strip() or None
    gate_host = resolve_probe_gate_host(backend_id)
    key = _cache_key(backend_id, gate_host, env_override)
    if catalogue is not None:
        return _resolve_from_listing(
            backend_id,
            pin,
            catalogue=catalogue,
            probe=probe,
            runner=runner,
            env_override=env_override,
            gate_host=gate_host,
            cache=cache,
            cache_key=key,
        )
    if cache:
        cached = _lookup_cached(key, tracked_pin=pin.tracked_pin, env_override=env_override)
        if cached is not None:
            return cached
    if probe:
        return _resolve_from_listing(
            backend_id,
            pin,
            catalogue=None,
            probe=True,
            runner=runner,
            env_override=env_override,
            gate_host=gate_host,
            cache=cache,
            cache_key=key,
        )
    return ModelDiscovery(
        backend_id=backend_id,
        resolved_model=env_override or pin.tracked_pin,
        tracked_pin=pin.tracked_pin,
        catalogue=(),
        source="env" if env_override else "tracked",
        warning=None,
        gate_host=gate_host,
    )


def snapshot_published_pins() -> dict[str, str | None]:
    """Capture ``BACKENDS[].allowed_model`` for test restore."""
    return {
        name: spec.allowed_model for name, spec in backend_registry.BACKENDS.items() if spec.allowed_model is not None
    }


def restore_published_pins(snapshot: Mapping[str, str | None]) -> None:
    """Restore ``BACKENDS[].allowed_model`` from :func:`snapshot_published_pins`."""
    with _publish_lock:
        for name, model in snapshot.items():
            spec = backend_registry.BACKENDS.get(name)
            if spec is None:
                continue
            backend_registry.BACKENDS[name] = replace(spec, allowed_model=model)


def _publish_one(backend_id: str, discovery: ModelDiscovery) -> None:
    spec = backend_registry.BACKENDS.get(backend_id)
    if spec is not None and spec.allowed_model is not None:
        backend_registry.BACKENDS[backend_id] = replace(spec, allowed_model=discovery.resolved_model)
    if spec is not None and spec.model_family == "cursor" and discovery.catalogue:
        seed_cursor_effort_slugs_from_catalogue(discovery.catalogue)
    from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
        OFFLOAD_AGENT_PROFILES,
    )

    profile = OFFLOAD_AGENT_PROFILES.get(backend_id)
    if profile is None or profile.pinned_model is None:
        return
    OFFLOAD_AGENT_PROFILES[backend_id] = replace(
        profile,
        pinned_model=discovery.resolved_model,
        default_model=discovery.resolved_model,
    )


def publish_resolved_model_pins(discoveries: Mapping[str, ModelDiscovery]) -> None:
    """Rebind ``allowed_model`` on the probed backend only.

    Does not alias a remote catalogue onto a local CLI sibling and does not
    mutate process-global ``DEFAULT_*_MODEL`` names.
    """
    with _publish_lock:
        for backend_id, discovery in discoveries.items():
            _publish_one(backend_id, discovery)


@contextmanager
def published_model_pins(discoveries: Mapping[str, ModelDiscovery]) -> Iterator[None]:
    """Publish pins and restore ``BACKENDS.allowed_model`` on exit (tests)."""
    snapshot = snapshot_published_pins()
    try:
        publish_resolved_model_pins(discoveries)
        yield
    finally:
        restore_published_pins(snapshot)
        from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (  # noqa: PLC0415
            reset_cursor_effort_slugs,
        )

        reset_cursor_effort_slugs()
        clear_model_discovery_cache()


def build_offload_dispatch_receipt(
    backend_id: str,
    *,
    served_model: str | None = None,
    discovery: ModelDiscovery | None = None,
    catalogue: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Lane receipt naming resolved weights and the harness (pins 19/21)."""
    try:
        resolved = discovery or resolve_offbox_model(backend_id, catalogue=catalogue)
    except (KeyError, PinHomeUndeclaredError):
        return {
            "backend_id": backend_id,
            "resolved_model": served_model,
            "served_model": served_model,
            "tracked_pin": None,
            "pin_source": "unpinned",
        }
    payload: dict[str, Any] = {
        "backend_id": backend_id,
        "resolved_model": resolved.resolved_model,
        "served_model": served_model,
        "tracked_pin": resolved.tracked_pin,
        "pin_source": resolved.source,
    }
    if resolved.warning:
        payload["discovery_warning"] = resolved.warning
    if resolved.gate_host:
        payload["gate_host"] = resolved.gate_host
    return payload
