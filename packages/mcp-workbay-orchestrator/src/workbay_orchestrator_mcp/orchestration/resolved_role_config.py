"""Immutable versioned role-resolution record.

Timeout resolution remains the shared reader for the neutral
``WORKBAY_LANE_TIMEOUT_S`` and legacy ``WORKBAY_CODEX_LANE_TIMEOUT_S``
environment names. Role, backend, model, effort, and catalogue admission
extend this same record rather than introducing a second source of truth.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from workbay_orchestrator_mcp.orchestration.offload_timeout_ssot import CODEX_TIMEOUT_CAP

RESOLVED_ROLE_CONFIG_VERSION = 1
CATALOGUE_ADMISSION_TTL_S = 300.0

WORKBAY_LANE_TIMEOUT_ENV = "WORKBAY_LANE_TIMEOUT_S"
WORKBAY_CODEX_LANE_TIMEOUT_ENV = "WORKBAY_CODEX_LANE_TIMEOUT_S"

LANE_TIMEOUT_MIN_S = 300
LANE_TIMEOUT_MAX_S = CODEX_TIMEOUT_CAP
DEFAULT_LANE_TIMEOUT_S = max(LANE_TIMEOUT_MIN_S, min(30 * 60, LANE_TIMEOUT_MAX_S))

SOURCE_LAYER_REQUEST = "request"
SOURCE_LAYER_ROLE_MANIFEST = "role_manifest"
SOURCE_LAYER_PERSISTED_LANE = "persisted_lane"
SOURCE_LAYER_REGISTRY = "registry"
SOURCE_LAYER_NEUTRAL_ENVIRONMENT = "neutral_environment"
SOURCE_LAYER_LEGACY_ENVIRONMENT = "legacy_environment"
SOURCE_LAYER_EQUAL_DUAL_ENVIRONMENT = "equal_dual_environment"

ROLE_EXECUTION = "execution"
ROLE_REVIEW = "review"

_IDENTITY_FIELDS = ("role", "backend", "provider", "model", "effort", "transport")
_PROVIDER_BY_FAMILY = {
    "grok": "xai",
    "codex": "openai",
    "cursor": "cursor",
    "claude": "anthropic",
}
_TRACKED_PIN_UNVERIFIED = "tracked_pin_unverified"


class LaneTimeoutError(ValueError):
    """Operator-supplied lane wall-clock bound cannot be used."""


class CodexLaneTimeoutError(LaneTimeoutError):
    """Invalid integer or range for a shared or Codex lane timeout env."""


class LaneTimeoutConflictError(LaneTimeoutError):
    """Explicit neutral and legacy timeout values disagree."""


class RoleIdentityConflictError(ValueError):
    """Explicit role/backend/model identities disagree and cannot be relabelled."""


class IncompatibleTransportError(ValueError):
    """Resolved provider transport cannot be executed by this adapter."""


class CatalogueUnavailableError(RuntimeError):
    """Catalogue refresh was required and could not be completed before spawn."""


class UnsupportedModelError(ValueError):
    """Model is not catalogue-capable for the resolved transport."""


class UnsupportedEffortError(ValueError):
    """Effort is missing, unsupported, or out of range before spend."""


class EffortMismatchError(ValueError):
    """Adapter-applied effort does not equal the resolved effort."""


@dataclass(frozen=True)
class RoleIdentity:
    """One precedence layer's explicit identity. Absent fields are unset."""

    role: str | None = None
    backend: str | None = None
    provider: str | None = None
    model: str | None = None
    effort: str | None = None
    transport: str | None = None


@dataclass(frozen=True)
class CatalogueSnapshot:
    """Versioned catalogue admission snapshot with explicit capabilities."""

    version: str
    digest: str
    models: tuple[str, ...]
    capabilities: tuple[tuple[str, str], ...]
    captured_at: float
    advertised_efforts: tuple[tuple[tuple[str, str], tuple[str, ...]], ...] = ()


@dataclass(frozen=True)
class ResolvedRoleConfig:
    """Immutable versioned role-resolution record."""

    version: int
    timeout_s: int
    timeout_source_layer: str
    timeout_sources: tuple[str, ...]
    role: str | None = None
    backend: str | None = None
    provider: str | None = None
    model: str | None = None
    effort: str | None = None
    transport: str | None = None
    source_layer: str | None = None
    identity_sources: tuple[str, ...] = ()
    catalogue_version: str | None = None
    catalogue_digest: str | None = None
    catalogue_snapshot: tuple[str, ...] = ()
    catalogue_capabilities: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    capability_decision: str | None = None
    capability_reason: str | None = None

    def as_public_dict(self) -> dict[str, Any]:
        """Preflight-facing resolved identity. Never a served-model claim."""
        return {
            "version": self.version,
            "role": self.role,
            "backend": self.backend,
            "provider": self.provider,
            "model": self.model,
            "effort": self.effort,
            "transport": self.transport,
            "source_layer": self.source_layer,
            "identity_sources": list(self.identity_sources),
            "catalogue_version": self.catalogue_version,
            "catalogue_digest": self.catalogue_digest,
            "capability_decision": self.capability_decision,
            "capability_reason": self.capability_reason,
            "timeout_s": self.timeout_s,
            "timeout_source_layer": self.timeout_source_layer,
        }


def _invalid_timeout_message(env_name: str) -> str:
    return f"{env_name} must be an integer between {LANE_TIMEOUT_MIN_S} and {LANE_TIMEOUT_MAX_S} seconds"


def _parse_timeout(raw: str, env_name: str) -> int:
    message = _invalid_timeout_message(env_name)
    try:
        timeout = int(raw)
    except ValueError as exc:
        raise CodexLaneTimeoutError(message) from exc
    if not LANE_TIMEOUT_MIN_S <= timeout <= LANE_TIMEOUT_MAX_S:
        raise CodexLaneTimeoutError(message)
    return timeout


def resolve_lane_timeout() -> ResolvedRoleConfig:
    """Resolve the shared lane timeout from neutral and legacy env sources."""
    parsed: dict[str, int] = {}
    errors: list[CodexLaneTimeoutError] = []
    for env_name in (WORKBAY_LANE_TIMEOUT_ENV, WORKBAY_CODEX_LANE_TIMEOUT_ENV):
        raw = os.environ.get(env_name)
        if raw is None:
            continue
        try:
            parsed[env_name] = _parse_timeout(raw, env_name)
        except CodexLaneTimeoutError as exc:
            errors.append(exc)
    if errors:
        if len(errors) == 1:
            raise errors[0]
        raise CodexLaneTimeoutError(
            _invalid_timeout_message(f"{WORKBAY_LANE_TIMEOUT_ENV} and {WORKBAY_CODEX_LANE_TIMEOUT_ENV}")
        )

    if len(parsed) == 2:
        neutral = parsed[WORKBAY_LANE_TIMEOUT_ENV]
        legacy = parsed[WORKBAY_CODEX_LANE_TIMEOUT_ENV]
        if neutral != legacy:
            raise LaneTimeoutConflictError(
                "conflicting_lane_timeout: "
                f"{WORKBAY_LANE_TIMEOUT_ENV}={neutral} "
                f"{WORKBAY_CODEX_LANE_TIMEOUT_ENV}={legacy}"
            )
        return ResolvedRoleConfig(
            version=RESOLVED_ROLE_CONFIG_VERSION,
            timeout_s=neutral,
            timeout_source_layer=SOURCE_LAYER_EQUAL_DUAL_ENVIRONMENT,
            timeout_sources=(WORKBAY_LANE_TIMEOUT_ENV, WORKBAY_CODEX_LANE_TIMEOUT_ENV),
        )
    if WORKBAY_LANE_TIMEOUT_ENV in parsed:
        return ResolvedRoleConfig(
            version=RESOLVED_ROLE_CONFIG_VERSION,
            timeout_s=parsed[WORKBAY_LANE_TIMEOUT_ENV],
            timeout_source_layer=SOURCE_LAYER_NEUTRAL_ENVIRONMENT,
            timeout_sources=(WORKBAY_LANE_TIMEOUT_ENV,),
        )
    if WORKBAY_CODEX_LANE_TIMEOUT_ENV in parsed:
        return ResolvedRoleConfig(
            version=RESOLVED_ROLE_CONFIG_VERSION,
            timeout_s=parsed[WORKBAY_CODEX_LANE_TIMEOUT_ENV],
            timeout_source_layer=SOURCE_LAYER_LEGACY_ENVIRONMENT,
            timeout_sources=(WORKBAY_CODEX_LANE_TIMEOUT_ENV,),
        )
    return ResolvedRoleConfig(
        version=RESOLVED_ROLE_CONFIG_VERSION,
        timeout_s=DEFAULT_LANE_TIMEOUT_S,
        timeout_source_layer=SOURCE_LAYER_REGISTRY,
        timeout_sources=(),
    )


def _normalized_advertised_efforts(
    advertised_efforts: Sequence[tuple[tuple[str, str], Sequence[str]]] = (),
) -> tuple[tuple[tuple[str, str], tuple[str, ...]], ...]:
    items = [
        ((str(model), str(transport)), tuple(str(token) for token in tokens))
        for (model, transport), tokens in advertised_efforts
    ]
    items.sort(key=lambda item: (item[0][0], item[0][1]))
    return tuple(items)


def catalogue_digest(
    models: Sequence[str],
    capabilities: Sequence[tuple[str, str]],
    version: str,
    advertised_efforts: Sequence[tuple[tuple[str, str], Sequence[str]]] = (),
) -> str:
    """Stable digest of a catalogue snapshot. Names alone are not hashed as capability."""
    efforts = _normalized_advertised_efforts(advertised_efforts)
    payload = {
        "advertised_efforts": [[[model, transport], list(tokens)] for (model, transport), tokens in efforts],
        "capabilities": [list(pair) for pair in capabilities],
        "models": list(models),
        "version": version,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def make_catalogue_snapshot(
    *,
    version: str,
    models: Sequence[str],
    capabilities: Sequence[tuple[str, str]],
    captured_at: float,
    advertised_efforts: Sequence[tuple[tuple[str, str], Sequence[str]]] = (),
) -> CatalogueSnapshot:
    models_t = tuple(str(item) for item in models)
    caps_t = tuple((str(model), str(transport)) for model, transport in capabilities)
    efforts_t = _normalized_advertised_efforts(advertised_efforts)
    return CatalogueSnapshot(
        version=str(version),
        digest=catalogue_digest(models_t, caps_t, str(version), efforts_t),
        models=models_t,
        capabilities=caps_t,
        captured_at=float(captured_at),
        advertised_efforts=efforts_t,
    )


def require_applied_effort_matches_resolved(*, applied: str | None, resolved: ResolvedRoleConfig) -> None:
    """Refuse successful provenance when adapter-applied effort drifted."""
    if applied != resolved.effort:
        raise EffortMismatchError(f"effort_mismatch: applied={applied!r} resolved={resolved.effort!r}")


def _strip(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _role_from_lane_kind(kind: str | None) -> str | None:
    text = _strip(kind)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"review", "adjudicate"}:
        return ROLE_REVIEW
    if lowered in {"implement", "execution"}:
        return ROLE_EXECUTION
    return None


def _mapping_str(mapping: Mapping[str, Any], key: str) -> str | None:
    value = mapping.get(key)
    return _strip(value) if isinstance(value, str) else None


def identity_from_role_manifest(lane_config: Mapping[str, Any] | None) -> RoleIdentity | None:
    """Normalize a loaded lane_config / role-manifest mapping into RoleIdentity."""
    if not isinstance(lane_config, Mapping):
        return None
    role = _role_from_lane_kind(_mapping_str(lane_config, "lane_kind"))
    backend = _mapping_str(lane_config, "preferred_backend")
    model = _mapping_str(lane_config, "preferred_model")
    effort = _mapping_str(lane_config, "preferred_reasoning_effort")
    if role is None and backend is None and model is None and effort is None:
        return None
    return RoleIdentity(role=role, backend=backend, model=model, effort=effort)


def identity_from_persisted_lane(row: Mapping[str, Any] | None) -> RoleIdentity | None:
    """Normalize a persisted worktree-lane row into RoleIdentity."""
    if not isinstance(row, Mapping):
        return None
    role = _role_from_lane_kind(_mapping_str(row, "lane_kind"))
    backend = _mapping_str(row, "backend")
    model = _mapping_str(row, "model")
    effort = _mapping_str(row, "reasoning_effort")
    if role is None and backend is None and model is None and effort is None:
        return None
    return RoleIdentity(role=role, backend=backend, model=model, effort=effort)


def _registry_identity(role: str) -> RoleIdentity:
    if role == ROLE_REVIEW:
        return RoleIdentity(
            role=ROLE_REVIEW,
            backend="codex-remote",
            provider="openai",
            model="gpt-5.6-luna",
            effort="max",
            transport="remote_exec",
        )
    return RoleIdentity(
        role=ROLE_EXECUTION,
        backend="grok-cli",
        provider="xai",
        model="grok-4.6",
        effort="high",
        transport="grok-cli",
    )


def _tracked_model_for_backend(backend: str) -> str | None:
    from workbay_orchestrator_mcp.orchestration import backend_registry

    spec = backend_registry.BACKENDS.get(backend)
    if spec is None:
        return None
    return _strip(getattr(spec, "tracked_model", None))


def _backend_meta(backend: str) -> tuple[str, str, str]:
    from workbay_orchestrator_mcp.orchestration import backend_registry

    spec = backend_registry.BACKENDS.get(backend)
    if spec is None:
        raise RoleIdentityConflictError(f"conflicting_role_identity: unknown backend {backend!r}")
    family = str(spec.model_family)
    provider = _PROVIDER_BY_FAMILY.get(family, family)
    off_box = bool(getattr(getattr(spec, "capabilities", None), "dispatchable_off_box", False))
    transport = "remote_exec" if off_box else backend
    return provider, family, transport


def native_transport_for_backend(backend: str) -> str:
    """Return the adapter-native transport for a registry backend."""
    _, _, transport = _backend_meta(backend)
    return transport


def _model_family(model: str) -> str | None:
    slug = model.strip().lower()
    if slug.startswith("grok"):
        return "grok"
    if slug.startswith(("gpt-", "o1", "o3", "o4")) or "luna" in slug or slug.endswith("-sol") or "codex" in slug:
        return "codex"
    if slug.startswith("cursor") or slug.startswith("kimi"):
        return "cursor"
    return None


def _merge_explicit_layers(layers: Sequence[tuple[str, RoleIdentity | None]]) -> tuple[dict[str, str], dict[str, str]]:
    values: dict[str, str] = {}
    sources: dict[str, str] = {}
    for layer_name, identity in layers:
        if identity is None:
            continue
        for field_name in _IDENTITY_FIELDS:
            raw = _strip(getattr(identity, field_name))
            if raw is None:
                continue
            current = values.get(field_name)
            if current is None:
                values[field_name] = raw
                sources[field_name] = layer_name
            elif current != raw:
                raise RoleIdentityConflictError(
                    f"conflicting_role_identity: {field_name} {sources[field_name]}={current!r} {layer_name}={raw!r}"
                )
    return values, sources


def _legacy_model_from_env(backend_hint: str | None, environ: Mapping[str, str]) -> tuple[str | None, str | None]:
    if not backend_hint:
        return None, None
    from workbay_orchestrator_mcp.orchestration import backend_registry

    spec = backend_registry.BACKENDS.get(backend_hint)
    env_key = getattr(spec, "allowed_model_env", None) if spec is not None else None
    if not env_key:
        return None, None
    raw = _strip(environ.get(str(env_key)))
    return raw, str(env_key) if raw else None


def _environment_model_layer(
    backend_hint: str | None, environ: Mapping[str, str]
) -> tuple[RoleIdentity | None, str | None, tuple[str, ...]]:
    legacy, legacy_env = _legacy_model_from_env(backend_hint, environ)
    if legacy:
        return RoleIdentity(model=legacy), SOURCE_LAYER_LEGACY_ENVIRONMENT, (str(legacy_env),)
    return None, None, ()


def _source_layer_for(sources: Mapping[str, str], fallback: str) -> str:
    for field_name in ("model", "backend", "effort", "transport", "provider", "role"):
        if field_name in sources:
            return sources[field_name]
    return fallback


def _require_usable_catalogue(snapshot: CatalogueSnapshot, now: float) -> None:
    if not math.isfinite(now) or not math.isfinite(snapshot.captured_at):
        raise CatalogueUnavailableError("catalogue_unavailable: invalid catalogue timestamp")
    if snapshot.captured_at > now:
        raise CatalogueUnavailableError("catalogue_unavailable: future catalogue timestamp")
    expected = catalogue_digest(snapshot.models, snapshot.capabilities, snapshot.version, snapshot.advertised_efforts)
    if snapshot.digest != expected:
        raise CatalogueUnavailableError("catalogue_unavailable: catalogue digest mismatch")


def _catalogue_is_fresh(snapshot: CatalogueSnapshot, now: float) -> bool:
    return (now - snapshot.captured_at) <= CATALOGUE_ADMISSION_TTL_S


def _invoke_catalogue_refresh(refresh: Callable[[], CatalogueSnapshot]) -> CatalogueSnapshot:
    try:
        refreshed = refresh()
    except CatalogueUnavailableError:
        raise
    except Exception as exc:
        raise CatalogueUnavailableError("catalogue_unavailable: refresh failed") from exc
    if refreshed is None:
        raise CatalogueUnavailableError("catalogue_unavailable: refresh returned no snapshot")
    return refreshed


def _admit_refreshed_catalogue(
    refresh: Callable[[], CatalogueSnapshot] | None,
    now: float,
) -> CatalogueSnapshot:
    if refresh is None:
        raise CatalogueUnavailableError("catalogue_unavailable: refresh unavailable")
    refreshed = _invoke_catalogue_refresh(refresh)
    _require_usable_catalogue(refreshed, now)
    if not _catalogue_is_fresh(refreshed, now):
        raise CatalogueUnavailableError("catalogue_unavailable: refresh returned stale snapshot")
    return refreshed


def _refresh_catalogue(
    snapshot: CatalogueSnapshot | None,
    refresh: Callable[[], CatalogueSnapshot] | None,
    now: float,
    *,
    required: bool,
) -> CatalogueSnapshot | None:
    if snapshot is not None:
        _require_usable_catalogue(snapshot, now)
        if _catalogue_is_fresh(snapshot, now):
            return snapshot
        return _admit_refreshed_catalogue(refresh, now)
    if not required:
        return None
    return _admit_refreshed_catalogue(refresh, now)


def _validate_effort(backend: str, model: str, effort: str | None) -> str | None:
    from workbay_protocol.reasoning_effort import validate_reasoning_effort

    from workbay_orchestrator_mcp.orchestration.effort_policy import default_effort_for_model

    try:
        validated = validate_reasoning_effort(effort)
    except (TypeError, ValueError) as extra:
        raise UnsupportedEffortError(str(extra)) from extra
    if validated is None:
        return default_effort_for_model(backend, model)
    return validated


def resolve_role_config(
    *,
    role: str,
    request: RoleIdentity | None = None,
    role_manifest: RoleIdentity | None = None,
    persisted_lane: RoleIdentity | None = None,
    catalogue: CatalogueSnapshot | None = None,
    catalogue_refresh: Callable[[], CatalogueSnapshot] | None = None,
    now: float | None = None,
    environ: Mapping[str, str] | None = None,
    catalogue_required: bool = True,
) -> ResolvedRoleConfig:
    """Resolve one immutable record for an execution or review role."""
    import time

    if role not in {ROLE_EXECUTION, ROLE_REVIEW}:
        raise RoleIdentityConflictError(f"conflicting_role_identity: unknown role {role!r}")
    for layer_name, identity in (
        (SOURCE_LAYER_REQUEST, request),
        (SOURCE_LAYER_ROLE_MANIFEST, role_manifest),
        (SOURCE_LAYER_PERSISTED_LANE, persisted_lane),
    ):
        if identity is None:
            continue
        explicit_role = _strip(identity.role)
        if explicit_role is not None and explicit_role != role:
            raise RoleIdentityConflictError(
                f"conflicting_role_identity: role {layer_name}={explicit_role!r} resolved={role!r}"
            )

    timeout = resolve_lane_timeout()
    env_map = os.environ if environ is None else environ
    clock = time.monotonic() if now is None else now

    values, sources = _merge_explicit_layers(
        [
            (SOURCE_LAYER_REQUEST, request),
            (SOURCE_LAYER_ROLE_MANIFEST, role_manifest),
            (SOURCE_LAYER_PERSISTED_LANE, persisted_lane),
        ]
    )
    registry = _registry_identity(role)
    explicit_effort = values.get("effort")
    if "backend" not in values:
        raw = _strip(registry.backend)
        if raw is not None:
            values["backend"] = raw
            sources["backend"] = SOURCE_LAYER_REGISTRY
    if "role" not in values:
        values["role"] = role
        sources["role"] = SOURCE_LAYER_REQUEST if request is not None else SOURCE_LAYER_REGISTRY
    elif values["role"] != role:
        raise RoleIdentityConflictError(
            f"conflicting_role_identity: role {sources.get('role', SOURCE_LAYER_REGISTRY)}="
            f"{values['role']!r} resolved={role!r}"
        )

    backend = values.get("backend")
    if backend is None:
        raise RoleIdentityConflictError("conflicting_role_identity: backend is required")
    env_identity, env_layer, env_source_names = _environment_model_layer(backend, env_map)
    env_model = _strip(env_identity.model) if env_identity is not None else None
    if env_model is not None and env_layer is not None and "model" not in values:
        values["model"] = env_model
        sources["model"] = env_layer
    if "model" not in values:
        tracked_default = _tracked_model_for_backend(backend)
        if tracked_default is not None:
            values["model"] = tracked_default
            sources["model"] = SOURCE_LAYER_REGISTRY
    provider, family, native_transport = _backend_meta(backend)
    values.setdefault("provider", provider)
    sources.setdefault("provider", SOURCE_LAYER_REGISTRY)
    if values.get("provider") != provider:
        raise RoleIdentityConflictError(
            f"conflicting_role_identity: provider {values['provider']!r} is incompatible with backend {backend!r}"
        )

    requested_transport = values.get("transport")
    if requested_transport is None:
        values["transport"] = native_transport
        sources.setdefault("transport", SOURCE_LAYER_REGISTRY)
    elif requested_transport != native_transport:
        raise IncompatibleTransportError(
            f"incompatible_transport: backend {backend!r} transport is {native_transport!r}, "
            f"not {requested_transport!r}"
        )

    model = values.get("model")
    if model is not None:
        model_family = _model_family(model)
        if model_family is not None and model_family != family:
            raise RoleIdentityConflictError(
                f"conflicting_role_identity: model {model!r} family {model_family!r} "
                f"is incompatible with backend {backend!r}"
            )

    admitted = _refresh_catalogue(catalogue, catalogue_refresh, clock, required=catalogue_required)
    transport = values["transport"]
    if admitted is None and catalogue_required:
        raise CatalogueUnavailableError("catalogue_unavailable: no observed catalogue snapshot")
    capability_decision: str | None
    capability_reason: str | None
    if admitted is None:
        tracked = _tracked_model_for_backend(backend)
        if model is not None and tracked is not None and model == tracked:
            capability_decision = None
            capability_reason = _TRACKED_PIN_UNVERIFIED
        else:
            raise CatalogueUnavailableError(
                f"catalogue_unavailable: {model!r} is unproven without a catalogue snapshot"
            )
    else:
        capability_decision = "admitted"
        capability_reason = "catalogue_capability"
        if model is not None and model not in admitted.models:
            raise UnsupportedModelError(f"unsupported_model: {model!r} is not in the catalogue snapshot")
        if model is not None and (model, transport) not in admitted.capabilities:
            raise UnsupportedModelError(
                f"unsupported_model: {model!r} has no catalogue capability for transport {transport!r}"
            )

    try:
        effort = _validate_effort(backend, model or "", explicit_effort)
    except UnsupportedEffortError:
        raise
    if not effort:
        raise UnsupportedEffortError("unsupported_effort: no safe default exists")
    advertised_tokens: tuple[str, ...] | None = None
    if admitted is not None:
        for (model_name, transport_name), tokens in admitted.advertised_efforts:
            if model_name == model and transport_name == transport:
                advertised_tokens = tokens
                break
    if advertised_tokens is not None and effort not in advertised_tokens:
        raise UnsupportedEffortError(
            f"unsupported_effort: {effort!r} is not advertised for model {model!r} transport {transport!r}"
        )
    values["effort"] = effort
    if explicit_effort is None:
        sources["effort"] = SOURCE_LAYER_REGISTRY
    elif "effort" not in sources:
        sources["effort"] = SOURCE_LAYER_REGISTRY

    identity_parts: list[str] = []
    if env_source_names:
        identity_parts.extend(env_source_names)
        if env_layer and sources.get("model") != env_layer:
            identity_parts.append(env_layer)
    identity_parts.extend(sources.values())
    identity_sources = tuple(dict.fromkeys(identity_parts))
    source_layer = _source_layer_for(
        {key: value for key, value in sources.items() if value != SOURCE_LAYER_REGISTRY},
        SOURCE_LAYER_REGISTRY,
    )
    if sources.get("model") == SOURCE_LAYER_LEGACY_ENVIRONMENT:
        source_layer = sources["model"]

    return ResolvedRoleConfig(
        version=RESOLVED_ROLE_CONFIG_VERSION,
        timeout_s=timeout.timeout_s,
        timeout_source_layer=timeout.timeout_source_layer,
        timeout_sources=timeout.timeout_sources,
        role=role,
        backend=backend,
        provider=values["provider"],
        model=model,
        effort=values["effort"],
        transport=transport,
        source_layer=source_layer,
        identity_sources=identity_sources,
        catalogue_version=admitted.version if admitted is not None else None,
        catalogue_digest=admitted.digest if admitted is not None else None,
        catalogue_snapshot=admitted.models if admitted is not None else (),
        catalogue_capabilities=admitted.capabilities if admitted is not None else (),
        capability_decision=capability_decision,
        capability_reason=capability_reason,
    )


def admit_grok_cli_execution(
    resolved: ResolvedRoleConfig,
    *,
    model: str | None,
    effort: str | None,
) -> None:
    """Refuse Grok CLI spawn unless the resolved record admits this model/transport."""
    if resolved.backend != "grok-cli":
        raise IncompatibleTransportError(
            f"incompatible_transport: GrokCliAdapter cannot execute backend {resolved.backend!r}"
        )
    if resolved.transport != "grok-cli":
        raise IncompatibleTransportError(f"incompatible_transport: transport {resolved.transport!r} is not grok-cli")
    if resolved.capability_decision != "admitted":
        raise UnsupportedModelError(
            f"unsupported_model: capability decision {resolved.capability_decision!r} is not admitted"
        )
    effective = _strip(model) or resolved.model
    if effective != resolved.model:
        raise UnsupportedModelError(
            f"unsupported_model: execute model {effective!r} is not resolved {resolved.model!r}"
        )
    if (resolved.model, "grok-cli") not in resolved.catalogue_capabilities:
        raise UnsupportedModelError(
            "unsupported_model: no catalogue capability for grok-cli transport; a supported name is not proof"
        )
    if effort is not None and effort != resolved.effort:
        from workbay_protocol.reasoning_effort import CONCRETE_EFFORTS

        if effort not in CONCRETE_EFFORTS:
            raise UnsupportedEffortError(f"unsupported_effort: {effort!r} is out of range")
        raise EffortMismatchError(f"effort_mismatch: applied={effort!r} resolved={resolved.effort!r}")
