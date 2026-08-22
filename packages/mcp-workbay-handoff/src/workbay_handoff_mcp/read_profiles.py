"""Named read profiles for ``get_handoff_state`` and ``load_session``.

internal Layer 1. A read profile expands to existing parameters
(``sections``, ``detail``, ``top_n_*``, plus ``load_session`` add-ons such
as ``open_findings_limit`` and ``top_n_touched_files``) so a caller can
express intent — identity probe, hot summary, review packet, open-items
gate, or full debug read — without hand-assembling every bounded-read
knob.

Profile expansion is deterministic:

* The caller's profile sets defaults first.
* Any explicitly supplied low-level argument overrides the profile.
* ``data.read_shape`` reports the applied shape when a profile is used.

Public callers and the MCP / CLI boundary express "argument not supplied"
by passing ``None``. We treat ``None`` as the sentinel so unrequested
public defaults never masquerade as explicit overrides. A caller that
truly wants to disable an additive section passes a zero-limit (e.g.
``top_n_touched_files=0``) — implementation note distinguishes "section disabled"
from "section omitted" before delegating to ``HandoffReadLimits``, which
otherwise clamps ``0 -> 1``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .shared_primitives import DEFAULT_HANDOFF_LIMITS
from .touched_files import DEFAULT_TOUCHED_FILES_LIMIT


@dataclass(frozen=True)
class StateReadShape:
    """Default ``get_handoff_state`` shape for a profile.

    ``sections`` is a comma-separated string with the same grammar as the
    public ``sections`` parameter, or ``None`` for "all sections". A
    profile's ``sections="identity"`` selects the existing identity-only
    response.
    """

    sections: str | None
    detail: str
    top_n_blockers: int
    top_n_actions: int
    top_n_decisions: int
    top_n_slices: int
    top_n_tests: int
    top_n_findings: int


@dataclass(frozen=True)
class SessionAddOnShape:
    """Default ``load_session`` add-on shape for a profile.

    ``open_findings_limit=0`` or ``top_n_touched_files=0`` is the zero-limit
    sentinel meaning "omit this add-on section entirely". The compound
    ``load_session`` handler short-circuits the corresponding fetch and
    records the omission in ``data.read_shape.session.omitted_sections``.
    """

    open_findings_limit: int
    open_findings_detail: str
    top_n_touched_files: int


@dataclass(frozen=True)
class ReadProfile:
    name: str
    state: StateReadShape
    session: SessionAddOnShape


# --- profile definitions ----------------------------------------------------

# "identity" mirrors the existing ``sections="identity"`` behavior on the
# state read and explicitly omits the additive ``open_findings`` /
# ``touched_files`` sections that ``load_session`` would otherwise fetch.
IDENTITY = ReadProfile(
    name="identity",
    state=StateReadShape(
        sections="identity",
        detail="summary",
        top_n_blockers=1,
        top_n_actions=1,
        top_n_decisions=1,
        top_n_slices=1,
        top_n_tests=1,
        top_n_findings=1,
    ),
    session=SessionAddOnShape(
        open_findings_limit=0,
        open_findings_detail="summary",
        top_n_touched_files=0,
    ),
)

HOT_SUMMARY = ReadProfile(
    name="hot_summary",
    state=StateReadShape(
        sections="blockers_open,actions_pending,findings_open,decisions_recent,tests_recent",
        detail="summary",
        top_n_blockers=3,
        top_n_actions=5,
        top_n_decisions=3,
        top_n_slices=DEFAULT_HANDOFF_LIMITS["slices"],
        top_n_tests=3,
        top_n_findings=5,
    ),
    session=SessionAddOnShape(
        open_findings_limit=5,
        open_findings_detail="summary",
        top_n_touched_files=10,
    ),
)

REVIEW_PACKET = ReadProfile(
    name="review_packet",
    state=StateReadShape(
        sections="blockers_open,actions_pending,findings_open,decisions_recent,tests_recent",
        detail="summary",
        top_n_blockers=20,
        top_n_actions=20,
        top_n_decisions=5,
        top_n_slices=DEFAULT_HANDOFF_LIMITS["slices"],
        top_n_tests=5,
        top_n_findings=20,
    ),
    session=SessionAddOnShape(
        open_findings_limit=20,
        open_findings_detail="summary",
        top_n_touched_files=20,
    ),
)

OPEN_ITEMS = ReadProfile(
    name="open_items",
    state=StateReadShape(
        sections="blockers_open,actions_pending,findings_open",
        detail="full",
        top_n_blockers=500,
        top_n_actions=500,
        top_n_decisions=DEFAULT_HANDOFF_LIMITS["decisions"],
        top_n_slices=DEFAULT_HANDOFF_LIMITS["slices"],
        top_n_tests=DEFAULT_HANDOFF_LIMITS["tests"],
        top_n_findings=500,
    ),
    session=SessionAddOnShape(
        open_findings_limit=500,
        open_findings_detail="full",
        top_n_touched_files=0,
    ),
)

FULL_DEBUG = ReadProfile(
    name="full_debug",
    state=StateReadShape(
        sections=None,
        detail="full",
        top_n_blockers=DEFAULT_HANDOFF_LIMITS["blockers"],
        top_n_actions=DEFAULT_HANDOFF_LIMITS["actions"],
        top_n_decisions=DEFAULT_HANDOFF_LIMITS["decisions"],
        top_n_slices=DEFAULT_HANDOFF_LIMITS["slices"],
        top_n_tests=DEFAULT_HANDOFF_LIMITS["tests"],
        top_n_findings=DEFAULT_HANDOFF_LIMITS["findings"],
    ),
    session=SessionAddOnShape(
        open_findings_limit=DEFAULT_HANDOFF_LIMITS["findings"],
        open_findings_detail="full",
        top_n_touched_files=DEFAULT_TOUCHED_FILES_LIMIT,
    ),
)

_PROFILES_BY_NAME: dict[str, ReadProfile] = {
    p.name: p for p in (IDENTITY, HOT_SUMMARY, REVIEW_PACKET, OPEN_ITEMS, FULL_DEBUG)
}

VALID_PROFILE_NAMES: tuple[str, ...] = tuple(_PROFILES_BY_NAME.keys())

# Sections that a profile considers required: ``auto_summary`` budgeting
# in implementation note must not silently omit these.
REQUIRED_SECTIONS_BY_PROFILE: dict[str, frozenset[str]] = {
    "open_items": frozenset({"blockers_open", "actions_pending", "findings_open"}),
}


class UnknownProfileError(ValueError):
    """Raised when an unknown ``read_profile`` name is requested."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.name = name


def get_profile(name: str | None) -> ReadProfile | None:
    """Return the profile registered under ``name`` or raise.

    ``None`` returns ``None`` (no profile requested). An unknown name
    raises :class:`UnknownProfileError`; callers translate that into an
    ``ok=False`` envelope with a ``valid_profiles`` listing.
    """

    if name is None:
        return None
    profile = _PROFILES_BY_NAME.get(name)
    if profile is None:
        raise UnknownProfileError(name)
    return profile


@dataclass(frozen=True)
class ResolvedStateShape:
    """Effective state-read parameters after profile + override merge.

    ``requested_profile`` records the caller's request (``None`` if
    omitted). ``applied_profile`` is the profile's canonical name when
    one was matched, or ``None`` when the caller did not request a
    profile. ``overrides`` lists the parameter names the caller supplied
    explicitly — they are what made the effective shape differ from the
    profile's baseline.
    """

    sections: str | None
    detail: str
    top_n_blockers: int
    top_n_actions: int
    top_n_decisions: int
    top_n_slices: int
    top_n_tests: int
    top_n_findings: int
    requested_profile: str | None
    applied_profile: str | None
    overrides: tuple[str, ...]


@dataclass(frozen=True)
class ResolvedSessionAddOnShape:
    """Effective ``load_session`` add-on parameters after profile merge."""

    open_findings_limit: int
    open_findings_detail: str
    top_n_touched_files: int
    overrides: tuple[str, ...]


def resolve_state_shape(
    *,
    read_profile: str | None,
    sections: str | None,
    detail: str | None,
    top_n_blockers: int | None,
    top_n_actions: int | None,
    top_n_decisions: int | None,
    top_n_slices: int | None,
    top_n_tests: int | None,
    top_n_findings: int | None,
) -> ResolvedStateShape:
    """Apply profile defaults, then layer explicit overrides on top.

    ``None`` means "caller did not supply"; the profile (or the
    ``full_debug`` baseline when no profile was requested) provides the
    default. Unknown profile names raise :class:`UnknownProfileError` —
    the public read handlers translate that into a structured envelope
    error.
    """

    profile = get_profile(read_profile)
    base = profile.state if profile is not None else FULL_DEBUG.state
    overrides: list[str] = []

    def _pick_str(name: str, supplied: str | None, default: str | None) -> str | None:
        if supplied is None:
            return default
        overrides.append(name)
        return supplied

    def _pick_int(name: str, supplied: int | None, default: int) -> int:
        if supplied is None:
            return default
        overrides.append(name)
        return supplied

    sections_eff = _pick_str("sections", sections, base.sections)
    detail_eff = _pick_str("detail", detail, base.detail) or "full"
    return ResolvedStateShape(
        sections=sections_eff,
        detail=detail_eff,
        top_n_blockers=_pick_int("top_n_blockers", top_n_blockers, base.top_n_blockers),
        top_n_actions=_pick_int("top_n_actions", top_n_actions, base.top_n_actions),
        top_n_decisions=_pick_int("top_n_decisions", top_n_decisions, base.top_n_decisions),
        top_n_slices=_pick_int("top_n_slices", top_n_slices, base.top_n_slices),
        top_n_tests=_pick_int("top_n_tests", top_n_tests, base.top_n_tests),
        top_n_findings=_pick_int("top_n_findings", top_n_findings, base.top_n_findings),
        requested_profile=read_profile,
        applied_profile=(profile.name if profile is not None else None),
        overrides=tuple(overrides),
    )


def resolve_session_add_on_shape(
    *,
    read_profile: str | None,
    open_findings_limit: int | None,
    open_findings_detail: str | None,
    top_n_touched_files: int | None,
) -> ResolvedSessionAddOnShape:
    """Resolve ``load_session`` add-on shape (open findings, touched files)."""

    profile = get_profile(read_profile)
    base = profile.session if profile is not None else FULL_DEBUG.session
    overrides: list[str] = []

    def _pick_int(name: str, supplied: int | None, default: int) -> int:
        if supplied is None:
            return default
        overrides.append(name)
        return supplied

    def _pick_str(name: str, supplied: str | None, default: str) -> str:
        if supplied is None:
            return default
        overrides.append(name)
        return supplied

    return ResolvedSessionAddOnShape(
        open_findings_limit=_pick_int("open_findings_limit", open_findings_limit, base.open_findings_limit),
        open_findings_detail=_pick_str("open_findings_detail", open_findings_detail, base.open_findings_detail),
        top_n_touched_files=_pick_int("top_n_touched_files", top_n_touched_files, base.top_n_touched_files),
        overrides=tuple(overrides),
    )


def state_shape_payload(
    shape: ResolvedStateShape,
    *,
    omitted_sections: list[str] | None = None,
) -> dict[str, object]:
    """Render a ``data.read_shape`` payload for the state read."""

    return {
        "requested_profile": shape.requested_profile,
        "applied_profile": shape.applied_profile,
        "sections": shape.sections,
        "detail": shape.detail,
        "limits": {
            "blockers": shape.top_n_blockers,
            "actions": shape.top_n_actions,
            "decisions": shape.top_n_decisions,
            "slices": shape.top_n_slices,
            "tests": shape.top_n_tests,
            "findings": shape.top_n_findings,
        },
        "overrides": list(shape.overrides),
        "omitted_sections": list(omitted_sections) if omitted_sections else [],
    }


def session_add_on_payload(
    shape: ResolvedSessionAddOnShape,
    *,
    omitted_sections: list[str] | None = None,
) -> dict[str, object]:
    """Render a ``data.read_shape.session`` payload for ``load_session``."""

    return {
        "open_findings_limit": shape.open_findings_limit,
        "open_findings_detail": shape.open_findings_detail,
        "top_n_touched_files": shape.top_n_touched_files,
        "overrides": list(shape.overrides),
        "omitted_sections": list(omitted_sections) if omitted_sections else [],
    }
