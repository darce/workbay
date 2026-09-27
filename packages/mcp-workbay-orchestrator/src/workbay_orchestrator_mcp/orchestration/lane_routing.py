"""Single-source lane routing resolution.

Routing is deliberately resolved as one value so backend, model, effort and
speed cannot be selected by independent precedence rules.  Database/manifest
drift is a routing refusal: no backend has been contacted at that point.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

_CORE_FIELDS = ("backend", "model", "effort", "speed")
_FIELDS = (*_CORE_FIELDS, "tier")
_ROW_KEYS = {
    "backend": "backend",
    "model": "model",
    "effort": "reasoning_effort",
    "speed": "speed",
    "tier": "tier",
}
_MANIFEST_KEYS = {
    "backend": "preferred_backend",
    "model": "preferred_model",
    "effort": "preferred_reasoning_effort",
    "speed": "preferred_speed",
    "tier": "preferred_tier",
}


@dataclass(frozen=True, slots=True)
class RoutingQuad:
    """The atomic routing selection (plus its tier classification)."""

    backend: str | None = None
    model: str | None = None
    effort: str | None = None
    speed: str | None = None
    tier: str | None = None
    # Keep provenance out of the five-value wire shape.  It is intentionally
    # excluded from equality so callers comparing route values do not become
    # sensitive to which equivalent source supplied them ([OBS-08]).
    _sources: tuple[tuple[str, str], ...] = field(default=(), init=False, repr=False, compare=False)

    @property
    def source_by_field(self) -> dict[str, str]:
        """Return the source that supplied each routing field.

        ``unset`` is explicit evidence that no source supplied a value; it is
        not a successful default.  Keeping this receipt beside the resolved
        values makes a NULL/omitted column distinguishable from an operator
        clear and satisfies the typed-outcome requirement ([OBS-08]).
        """

        sources = dict(self._sources)
        return {field: sources.get(field, "unset") for field in _FIELDS}

    @property
    def sources(self) -> dict[str, str]:
        """Compatibility alias for :attr:`source_by_field`."""

        return self.source_by_field

    @property
    def reasoning_effort(self) -> str | None:
        """Database/adapter spelling compatibility."""

        return self.effort

    def as_dict(self) -> dict[str, str | None]:
        return {field: getattr(self, field) for field in _FIELDS}


class RoutingDisagreement(RuntimeError):
    """Typed fail-closed routing refusal raised before backend contact."""

    failure_class = "routing"

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        missing_fields: tuple[str, ...] = (),
        disagreements: tuple[tuple[str, str | None, str | None], ...] = (),
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.missing_fields = missing_fields
        self.disagreements = disagreements


class InvalidSpeed(RoutingDisagreement):
    """A speed value that this backend cannot carry."""

    def __init__(self, backend: str, speed: str) -> None:
        self.backend = backend
        self.speed = speed
        if backend == "codex-remote":
            message = f"speed {speed!r} is not valid for {backend}; allowed is 'standard' or 'fast'"
        else:
            message = f"speed is not applicable to {backend}; got {speed!r}"
        super().__init__("invalid_speed", message)


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


_CODEX_REMOTE_SPEEDS = frozenset({"standard", "fast"})


def normalize_speed(backend: str | None, speed: object) -> str | None:
    """Canonical speed for capability digests and routing quads.

    Codex-remote treats a blank/omitted speed as ``standard`` (the tier
    table's default service tier; ``fast`` is the only other value). Speed is
    not applicable to any other backend and stays ``None`` there. Unknown
    values fail loudly ([OBS-08]).
    """

    cleaned = _clean(speed)
    backend_id = _clean(backend) or ""
    if backend_id == "codex-remote":
        if cleaned is None:
            return "standard"
        if cleaned in _CODEX_REMOTE_SPEEDS:
            return cleaned
        raise InvalidSpeed(backend_id, cleaned)
    if cleaned is None:
        return None
    raise InvalidSpeed(backend_id, cleaned)


def _source_values(source: object, keys: Mapping[str, str]) -> tuple[dict[str, str | None], set[str]]:
    values: dict[str, str | None] = {}
    supplied: set[str] = set()
    if source is None:
        return values, supplied
    if isinstance(source, RoutingQuad):
        for field in _FIELDS:
            raw = getattr(source, field)
            if raw is not None:
                supplied.add(field)
                values[field] = _clean(raw)
        return values, supplied
    if isinstance(source, Mapping):
        for field, key in keys.items():
            aliases = (key, field) if key != field else (key,)
            for alias in aliases:
                if alias in source and source[alias] is not None:
                    supplied.add(field)
                    values[field] = _clean(source[alias])
                    break
        return values, supplied
    for field, key in keys.items():
        raw = getattr(source, key, getattr(source, field, None))
        if raw is not None:
            supplied.add(field)
            values[field] = _clean(raw)
    return values, supplied


def _profile_quad(profile: object) -> RoutingQuad:
    values, _ = _source_values(profile, _ROW_KEYS)
    if not values and profile is not None:
        # OffloadProfile uses name/default_model/default_effort/default_speed.
        values = {
            "backend": _clean(getattr(profile, "backend", getattr(profile, "agent", getattr(profile, "name", None)))),
            "model": _clean(getattr(profile, "pinned_model", None) or getattr(profile, "default_model", None)),
            "effort": _clean(getattr(profile, "default_effort", None)),
            "speed": _clean(getattr(profile, "default_speed", None)),
            "tier": _clean(getattr(profile, "tier", None)),
        }
    return RoutingQuad(**{field: values.get(field) for field in _FIELDS})


def _with_sources(quad: RoutingQuad, sources: Mapping[str, str]) -> RoutingQuad:
    """Attach a field-level source receipt without changing the quad values."""

    object.__setattr__(
        quad,
        "_sources",
        tuple((field, str(sources[field])) for field in _FIELDS if field in sources),
    )
    return quad


def _merge_values(
    base: dict[str, str | None],
    sources: dict[str, str],
    values: Mapping[str, str | None],
    supplied: set[str],
    source: str,
    *,
    skip_none: bool = False,
) -> None:
    """Merge one source, optionally treating ``None`` as unspecified.

    Empty strings are already normalized to ``None`` but remain in
    ``supplied`` as explicit clears.  Only the tier-derived merge skips
    ``None`` values: an incomplete tier row must not erase a previously pinned
    coordinate, while a present caller/row/manifest clear remains meaningful.
    """

    for field in supplied:
        value = values.get(field)
        if skip_none and value is None:
            continue
        base[field] = value
        sources[field] = source


def _row_roles(row: object) -> frozenset[str]:
    raw = getattr(row, "roles", row.get("roles") if isinstance(row, Mapping) else None)
    if raw is None:
        return frozenset()
    return frozenset(raw)


def _tier_quad(tier: str, tier_table: Mapping[str, Any], *, role: str | None = None) -> RoutingQuad | None:
    candidates: list[tuple[str, Any]] = []
    for slug, row in tier_table.items():
        row_tier = _clean(getattr(row, "tier", row.get("tier") if isinstance(row, Mapping) else None))
        entitled = getattr(row, "entitled", row.get("entitled", True) if isinstance(row, Mapping) else True)
        if row_tier == tier and bool(entitled) and (role is None or role in _row_roles(row)):
            candidates.append((str(slug), row))
    if not candidates:
        return None
    slug, row = candidates[0]
    effort = _clean(getattr(row, "default_effort", row.get("default_effort") if isinstance(row, Mapping) else None))
    service_tiers = getattr(
        row,
        "allowed_service_tiers",
        row.get("allowed_service_tiers", ()) if isinstance(row, Mapping) else (),
    )
    raw_speed = "standard" if "default" in service_tiers else None
    speed = normalize_speed("codex-remote", raw_speed)
    return RoutingQuad("codex-remote", slug, effort, speed, tier)


def _infer_tier(model: str | None, tier_table: Mapping[str, Any]) -> str | None:
    if model is None or model not in tier_table:
        return None
    row = tier_table[model]
    return _clean(getattr(row, "tier", row.get("tier") if isinstance(row, Mapping) else None))


def resolve_routing_quad(
    *,
    caller: object = None,
    row: object = None,
    manifest_entry: object = None,
    tier_table: Mapping[str, Any] | None = None,
    profile: object = None,
    allow_partial_caller: bool = False,
    role: str | None = None,
) -> RoutingQuad:
    """Resolve caller > row > manifest > tier default > profile.

    A caller either supplies all four routing coordinates or none.  Empty
    strings count as supplied and explicitly clear that coordinate.  A lone
    tier is a selector, not a partial concrete quad.
    """

    tiers = tier_table or {}
    caller_values, caller_supplied = _source_values(caller, _ROW_KEYS)
    row_values, row_supplied = _source_values(row, _ROW_KEYS)
    manifest_values, manifest_supplied = _source_values(manifest_entry, _MANIFEST_KEYS)

    supplied_core = set(_CORE_FIELDS) & caller_supplied
    if supplied_core and supplied_core != set(_CORE_FIELDS) and not allow_partial_caller:
        missing = tuple(field for field in _CORE_FIELDS if field not in supplied_core)
        raise RoutingDisagreement(
            "partial_caller_quad",
            f"caller routing quad is partial; missing fields: {', '.join(missing)}",
            missing_fields=missing,
        )

    disagreements = tuple(
        (field, row_values.get(field), manifest_values.get(field))
        for field in _FIELDS
        if field in row_supplied and field in manifest_supplied and row_values.get(field) != manifest_values.get(field)
    )
    if disagreements:
        detail = "; ".join(
            f"row.{field}={row_value!r} disagrees with manifest.{field}={manifest_value!r}"
            for field, row_value, manifest_value in disagreements
        )
        raise RoutingDisagreement(
            "row_manifest_disagreement",
            detail,
            disagreements=disagreements,
        )

    profile_value = _profile_quad(profile)
    base = profile_value.as_dict()
    source_by_field = {
        field: (profile_value.source_by_field[field] if profile_value.source_by_field[field] != "unset" else "profile")
        for field in _FIELDS
        if getattr(profile_value, field) is not None
    }
    lone_caller_tier = "tier" in caller_supplied and not supplied_core
    selected_tier = caller_values.get("tier") if "tier" in caller_supplied else None
    if selected_tier is None:
        selected_tier = row_values.get("tier") if "tier" in row_supplied else None
    if selected_tier is None:
        selected_tier = manifest_values.get("tier") if "tier" in manifest_supplied else None
    tier_value = None
    if selected_tier:
        tier_value = _tier_quad(selected_tier, tiers, role=role)
        if tier_value is None:
            if role is not None:
                raise RoutingDisagreement(
                    "no_entitled_role",
                    f"routing tier {selected_tier!r} has no entitled model for role {role!r}",
                )
            raise RoutingDisagreement("unknown_tier", f"routing tier {selected_tier!r} has no entitled model")
        if not lone_caller_tier:
            _merge_values(
                base,
                source_by_field,
                tier_value.as_dict(),
                set(_FIELDS),
                "tier",
                skip_none=True,
            )

    for values, supplied, source in (
        (manifest_values, manifest_supplied, "manifest"),
        (row_values, row_supplied, "row"),
    ):
        _merge_values(base, source_by_field, values, supplied, source)
    if supplied_core:
        _merge_values(base, source_by_field, caller_values, caller_supplied, "caller")
        if "tier" not in caller_supplied:
            inferred_tier = _infer_tier(caller_values.get("model"), tiers)
            base["tier"] = inferred_tier
            if inferred_tier is None:
                source_by_field.pop("tier", None)
            else:
                source_by_field["tier"] = "inferred"
    elif lone_caller_tier:
        # A lone caller tier selects one atomic quad at caller precedence; no
        # lower-priority concrete field may split it into an incoherent route.
        assert tier_value is not None
        _merge_values(
            base,
            source_by_field,
            tier_value.as_dict(),
            set(_FIELDS),
            "tier",
            skip_none=True,
        )

    # A tier is a codex-remote selector. After row/manifest/caller determine
    # the backend, drop coordinates that only the tier table contributed so a
    # grok-remote (or other non-codex) row cannot inherit speed/model/effort.
    if base.get("backend") != "codex-remote":
        for name in tuple(source_by_field):
            if source_by_field[name] == "tier":
                base[name] = None
                del source_by_field[name]

    if base.get("tier") is None:
        inferred_tier = _infer_tier(base.get("model"), tiers)
        base["tier"] = inferred_tier
        if inferred_tier is None:
            source_by_field.pop("tier", None)
        else:
            source_by_field["tier"] = "inferred"
    model = base.get("model")
    if model in tiers:
        if base.get("backend") not in (None, "codex-remote"):
            raise RoutingDisagreement(
                "backend_model_disagreement",
                f"routing backend {base['backend']!r} cannot serve tier model {model!r}",
            )
        tier_row = tiers[model]
        entitled = getattr(
            tier_row,
            "entitled",
            tier_row.get("entitled", True) if isinstance(tier_row, Mapping) else True,
        )
        if not entitled:
            raise RoutingDisagreement("tier_not_entitled", f"routing model {model!r} is not entitled")
        model_tier = _infer_tier(model, tiers)
        if base.get("tier") is not None and base["tier"] != model_tier:
            raise RoutingDisagreement(
                "tier_model_disagreement",
                f"routing model {model!r} belongs to tier {model_tier!r}, not {base['tier']!r}",
            )
    return _with_sources(RoutingQuad(**base), source_by_field)
