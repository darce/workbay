"""Cursor CLI lane configuration (model pin + cycle bound).

Mirrors :mod:`grok_lane_config`'s role as the single source for the Cursor
backend's model slug, so the registry description, the offload profile and the
adapter never re-derive it independently (implementation note S3 [REF-19]/[DATA-14] idiom).

Cursor differs from grok in one governance-relevant way: ``cursor-agent`` has no
``--max-turns`` flag, so a cursor lane cannot be bounded by turn COUNT. It is bounded by
wall-clock only, enforced by the adapter's process-group kill. That is why the
offload profile declares ``BOUND_ADAPTER_TIMEOUT`` rather than reusing grok's
turn+time pair — claiming a turn bound this backend cannot enforce would be a
silent cap ([AGT-10]).
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

# Cursor takes the model as a parameter, so the harness (cursor) and the model
# (grok) are separate axes — unlike grok-cli, where they coincide.
#
# Verified against a live `cursor-agent --list-models`: the grok family is
# published as `cursor-grok-4.5-{low,medium,high}` (each with a `-fast` twin).
# There is NO bare `grok-4.5` slug, and the CLI rejects an unknown id outright
# ("Cannot use this model: …"), so this default must name a real published slug.
#
# Operator direction: cursor grok lanes default to the FAST variant. Effort
# rewriting looks up the published (effort, fast) pair so a low/medium/high
# swap stays fast only when that twin was listed.
TRACKED_CURSOR_MODEL = "cursor-grok-4.5-high-fast"
WORKBAY_CURSOR_MODEL_ENV = "WORKBAY_CURSOR_MODEL"
# VM catalogue probe (implementation note M2). Fixture-driven in tests; never invent a slug.
CURSOR_LIST_MODELS_ARGV: tuple[str, ...] = ("cursor-agent", "--list-models")


def resolve_tracked_or_env_cursor_model() -> str:
    """Env override if set, else the tracked pin. Does not read settings.local.json."""
    raw = (os.environ.get(WORKBAY_CURSOR_MODEL_ENV) or "").strip()
    return raw or TRACKED_CURSOR_MODEL


# Import-time snapshot: env > tracked. Discovery publishes onto
# BACKENDS[].allowed_model / the offload profile only — DEFAULT_* stays
# the flock/CLI snapshot by design (a remote catalogue must not leak
# onto local CLI siblings).
DEFAULT_CURSOR_MODEL = resolve_tracked_or_env_cursor_model()

# Effort selection is a lookup in a table of slugs the vendor actually
# PUBLISHES — deliberately not string surgery on the slug.
#
# Measured: the bracket parameterization documented in `cursor-agent --help`
# ('model[context=1m,effort=high]') is REJECTED for these ids — a live turn with
# 'cursor-grok-4.5-high[effort=high]' fails while the plain slug succeeds. So
# effort has to be carried by picking a different slug.
#
# An earlier revision rewrote the trailing `-<effort>` segment generically. Two
# independent reviewers falsified that: it assumes every family publishes every
# effort, and it happily synthesized ids no family publishes —
# `cursor-grok-4.5-xhigh-fast` (grok publishes only low|medium|high),
# `claude-opus-4-8-thinking-low`, `gpt-5.5-low`. Worse, `xhigh` needs no
# operator action to occur: the shared effort resolver escalates high -> xhigh
# automatically when a previous run was exhausted, so the invalid slug would
# surface on RETRY. A table cannot invent an id that does not exist.
#
# The live table is keyed by (family, effort, fast) and stores only slugs that
# appeared in the catalogue (or the hand-transcribed tracked fallback).
# Concatenating a `-fast` tail is forbidden: a fast-only family plus a
# non-fast pin must not invent the missing twin.
#
# The literal is the degrade fallback when a catalogue probe fails or is empty.
# Discovery rewrites the live table via :func:`seed_cursor_effort_slugs_from_catalogue`
# so a new family (cursor-grok-4.6-*) is honored without a hand-transcribed bump.
_CURSOR_EFFORT_TOKENS: tuple[str, ...] = ("low", "medium", "high", "xhigh")

# Latency variant tail. A lane pinned to a fast slug stays fast only when
# the requested (effort, fast) pair was actually published.
CURSOR_FAST_SUFFIX = "-fast"

# family -> effort -> {False: non-fast slug, True: fast slug}. Only published ids.
CursorEffortVariants = dict[str, dict[str, dict[bool, str]]]

TRACKED_CURSOR_EFFORT_VARIANTS: CursorEffortVariants = {
    "cursor-grok-4.5": {
        "low": {
            False: "cursor-grok-4.5-low",
            True: "cursor-grok-4.5-low-fast",
        },
        "medium": {
            False: "cursor-grok-4.5-medium",
            True: "cursor-grok-4.5-medium-fast",
        },
        "high": {
            False: "cursor-grok-4.5-high",
            True: "cursor-grok-4.5-high-fast",
        },
    },
    # Ported from the implementation note S5 hand-authored table on main (landed 14aa9258b,
    # after this branch diverged). Non-fast entries reproduce that table exactly.
    # A ``True`` entry is present ONLY where the -fast slug appears in the curated
    # CURSOR_REMOTE_ALLOWED_MODELS; the missing twins are not invented.
    #
    # Vendor publishes low|high|max only for kimi - no medium, and no -fast tail.
    # Orchestrator xhigh maps onto max.
    "kimi-k3": {
        "low": {False: "kimi-k3-low"},
        "high": {False: "kimi-k3-high"},
        "xhigh": {False: "kimi-k3-max"},
    },
    "cursor-grok-4.6": {
        "low": {False: "cursor-grok-4.6-low"},
        "medium": {False: "cursor-grok-4.6-medium"},
        "high": {
            False: "cursor-grok-4.6-high",
            True: "cursor-grok-4.6-high-fast",
        },
        "xhigh": {False: "cursor-grok-4.6-xhigh"},
    },
}

# Backward-compat view: family -> effort -> non-fast slug. Sibling tests and
# adapters still read this; it never contains a synthesized id.
TRACKED_CURSOR_EFFORT_SLUGS: dict[str, dict[str, str]] = {
    family: {effort: variants[False] for effort, variants in efforts.items() if False in variants}
    for family, efforts in TRACKED_CURSOR_EFFORT_VARIANTS.items()
}

CURSOR_EFFORT_VARIANTS: CursorEffortVariants = {}
CURSOR_EFFORT_SLUGS: dict[str, dict[str, str]] = {}


def _copy_variant_table(table: Mapping[str, Mapping[str, Mapping[bool, str]]]) -> CursorEffortVariants:
    return {
        family: {effort: dict(variants) for effort, variants in efforts.items()} for family, efforts in table.items()
    }


def _sync_effort_slug_view() -> dict[str, dict[str, str]]:
    """Rebuild the non-fast ``CURSOR_EFFORT_SLUGS`` view from published variants."""
    CURSOR_EFFORT_SLUGS.clear()
    for family, efforts in CURSOR_EFFORT_VARIANTS.items():
        non_fast = {effort: variants[False] for effort, variants in efforts.items() if False in variants}
        if non_fast:
            CURSOR_EFFORT_SLUGS[family] = non_fast
    return CURSOR_EFFORT_SLUGS


def _parse_cursor_effort_slug(slug: str) -> tuple[str, str, bool] | None:
    """Return ``(family, effort, fast)`` for a published slug, or ``None``."""
    text = slug.strip()
    if not text:
        return None
    fast = text.endswith(CURSOR_FAST_SUFFIX)
    stem = text[: -len(CURSOR_FAST_SUFFIX)] if fast else text
    for token in _CURSOR_EFFORT_TOKENS:
        suffix = f"-{token}"
        if stem.endswith(suffix) and len(stem) > len(suffix):
            return stem[: -len(suffix)], token, fast
    return None


def reset_cursor_effort_slugs() -> dict[str, dict[str, str]]:
    """Restore the hand-transcribed degrade table (tests / failed probe)."""
    CURSOR_EFFORT_VARIANTS.clear()
    CURSOR_EFFORT_VARIANTS.update(_copy_variant_table(TRACKED_CURSOR_EFFORT_VARIANTS))
    return _sync_effort_slug_view()


def seed_cursor_effort_slugs_from_catalogue(
    catalogue: Sequence[str],
) -> dict[str, dict[str, str]]:
    """Populate effort tables from published slugs only.

    Keys are ``(family, effort, fast)``. An empty or unparseable catalogue
    degrades to :data:`TRACKED_CURSOR_EFFORT_VARIANTS` — never invents a slug
    and never concatenates a ``-fast`` tail that the catalogue omitted.
    """
    seeded: CursorEffortVariants = {}
    for raw in catalogue:
        if not isinstance(raw, str):
            continue
        parsed = _parse_cursor_effort_slug(raw)
        if parsed is None:
            continue
        family, effort, fast = parsed
        seeded.setdefault(family, {}).setdefault(effort, {})[fast] = raw.strip()
    if not seeded:
        return reset_cursor_effort_slugs()
    CURSOR_EFFORT_VARIANTS.clear()
    CURSOR_EFFORT_VARIANTS.update(seeded)
    return _sync_effort_slug_view()


def resolve_cursor_model(model: str, requested_effort: str | None) -> tuple[str, str | None, str | None]:
    """Map (slug, requested effort) onto a PUBLISHED slug and the effort it encodes.

    Returns ``(slug, effective_effort, downgrade_reason)``.

    ``effective_effort`` is the effort the returned slug actually encodes — not
    the caller's request. The adapter stamps this on ``BackendResult`` so the
    audit trail can never claim an effort the vendor slug did not carry (the
    fabricated-effort defect both reviewers flagged).

    ``downgrade_reason`` is non-None when the request could not be honored, so
    the caller can degrade LOUDLY instead of silently ([AGT-10]).

    Applies to explicitly-pinned models too. The offload profile pins a model on
    every dispatch, so skipping pinned models made the whole mechanism dead code
    on the primary path — and silently regressed effort handling that worked
    before.

    Never concatenates a ``-fast`` tail. If the requested ``(effort, fast)``
    pair is unpublished, the incoming pin is kept and a reason is set.
    """
    if not model:
        return model, None, None

    parsed = _parse_cursor_effort_slug(model)
    stem = model[: -len(CURSOR_FAST_SUFFIX)] if model.endswith(CURSOR_FAST_SUFFIX) else model
    want_fast = bool(parsed[2]) if parsed is not None else model.endswith(CURSOR_FAST_SUFFIX)

    family = None
    # Longest stem first so cursor-grok-4.6 wins over a shorter cursor-grok-4 prefix.
    for candidate in sorted(CURSOR_EFFORT_VARIANTS, key=len, reverse=True):
        if stem == candidate or stem.startswith(f"{candidate}-"):
            family = candidate
            break

    encoded = None
    if family is not None:
        for effort, variants in CURSOR_EFFORT_VARIANTS[family].items():
            if model in variants.values() or stem in variants.values():
                encoded = effort
                break

    # Sentinels are resolver bookkeeping, never vendor values.
    if not requested_effort or requested_effort in ("auto", "inherit"):
        return model, encoded, None

    if family is None:
        return (
            model,
            encoded,
            (
                f"model {model!r} is not in a family with published effort variants; "
                f"requested effort {requested_effort!r} not applied"
            ),
        )

    variants = CURSOR_EFFORT_VARIANTS[family].get(requested_effort)
    if not variants:
        return (
            model,
            encoded,
            (
                f"family {family!r} publishes no {requested_effort!r} variant "
                f"(available: {', '.join(sorted(CURSOR_EFFORT_VARIANTS[family]))}); "
                f"keeping {model!r}"
            ),
        )
    target = variants.get(want_fast)
    if target is None:
        kind = "fast" if want_fast else "non-fast"
        published = ", ".join(sorted(variants.values()))
        return (
            model,
            encoded,
            (
                f"family {family!r} publishes no {requested_effort!r} {kind} variant "
                f"(available: {published}); keeping {model!r}"
            ),
        )
    return target, requested_effort, None


# Seed the live tables from the tracked fallback at import.
reset_cursor_effort_slugs()


CURSOR_TIMEOUT_CAP_DEFAULT = 900


def _positive_int_env(name: str, default: int) -> int:
    """Read a positive-int env var, falling back to ``default`` on junk.

    Deliberately NOT a bare ``int(os.environ.get(...))``. This module is
    imported at top level by ``backend_registry``, so a ValueError here does not
    degrade one backend — it aborts the import and takes down the ENTIRE backend
    listing, including every backend unrelated to cursor. An empty string (the
    common ``export WORKBAY_CURSOR_TIMEOUT=`` spelling) is exactly such a value.
    A misconfigured knob must not be able to sink the registry ([RES-13]:
    contain the blast radius at the boundary).
    """
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# Wall-clock ceiling for one cursor cycle. Deliberately its own constant — not
# coupled to GROK_TIMEOUT_CAP (cursor 900 vs grok 1800 today); changing one must
# not silently move the other.
CURSOR_TIMEOUT_CAP = _positive_int_env("WORKBAY_CURSOR_TIMEOUT", CURSOR_TIMEOUT_CAP_DEFAULT)

# Flags that would hand Cursor its own worktree. Refused by the adapter: the lane
# already has exactly one worktree owned by the lifecycle, and letting
# cursor-agent create a second under ~/.cursor/worktrees/ would add another
# vendor-owned representation of a lane's checkout.
FORBIDDEN_CURSOR_FLAGS = ("-w", "--worktree", "--worktree-base")

# --- implementation note S7a: cursor-agent on the OCI lane VM (linux/arm64 tarball) -----
# Version / install-root / symlink layout / rollback / dated smoke probes are
# operator provisioning facts — not code. Full evidence lives in
# docs/runbooks/remote-gate-provisioning.md (Cursor section). Auth path:
# https://cursor.com/dashboard/api → provision_cursor_remote_auth.sh.
# cursor-cloud still out of scope for this plan (D12) — separate plan if needed.
#
# These pins record operator smoke evidence only — they do NOT flip
# BACKENDS["cursor-remote"].is_available (probe_availability/_probe_cursor_remote
# owns live reachability: host + VM binary + env file).
CURSOR_REMOTE_VERSION = "2026.07.23-e383d2b"
# AgentSpec.binary is the bare name "cursor-agent"; path_prepend stays
# home-relative (no embedded $HOME / absolute host path in the element).
# CURSOR_REMOTE_BIN is the same home-relative layout as a pin — resolved under
# the remote $HOME at use time (same convention as CURSOR_PATH_PREPEND).
CURSOR_REMOTE_BIN = ".local/bin/cursor-agent"
CURSOR_PATH_PREPEND = ".local/bin"
CURSOR_REMOTE_HEADLESS_AUTH_OK = True
CURSOR_REMOTE_SMOKE_OK = True
CURSOR_REMOTE_SLICE_STATUS = "feasible"
CURSOR_REMOTE_NOT_FEASIBLE_REASON = ""
DEFAULT_EST_TOKENS = 80_000
# Wall-clock pin for cursor-remote AgentSpec.lane_timeout_s (requires_timeout).
CURSOR_REMOTE_LANE_TIMEOUT_S = CURSOR_TIMEOUT_CAP

# Curated authorization set (implementation note S4, finding 18210). Hand-curated
# (SECD-05): deny-by-default membership, never catalogue- or probe-derived.
# The effort tables above ARE catalogue-refreshable; this allow-list is not.
CURSOR_REMOTE_ALLOWED_MODELS: frozenset[str] = frozenset(
    {
        "cursor-grok-4.5-high-fast",
        "cursor-grok-4.6-high",
        "cursor-grok-4.6-high-fast",
        "kimi-k3-low",
        "kimi-k3-high",
        "kimi-k3-max",
    }
)
