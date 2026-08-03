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

# Cursor takes the model as a parameter, so the harness (cursor) and the model
# (grok) are separate axes — unlike grok-cli, where they coincide.
#
# Verified against a live `cursor-agent --list-models`: the grok family is
# published as `cursor-grok-4.5-{low,medium,high}` (each with a `-fast` twin).
# There is NO bare `grok-4.5` slug, and the CLI rejects an unknown id outright
# ("Cannot use this model: …"), so this default must name a real published slug.
#
# Operator direction: cursor grok lanes default to the FAST variant. Effort
# rewriting below preserves the `-fast` tail, so a low/medium/high swap stays
# fast unless the operator pins a non-fast slug explicitly.
DEFAULT_CURSOR_MODEL = os.environ.get("WORKBAY_CURSOR_MODEL", "cursor-grok-4.5-high-fast")

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
# Keyed by family stem; each maps the orchestrator's effort vocabulary onto a
# published slug. Families absent from this table are never rewritten.
CURSOR_EFFORT_SLUGS: dict[str, dict[str, str]] = {
    "cursor-grok-4.5": {
        "low": "cursor-grok-4.5-low",
        "medium": "cursor-grok-4.5-medium",
        "high": "cursor-grok-4.5-high",
    },
}

# Latency variant tail. Preserved across an effort switch: a lane pinned to a
# fast slug stays fast.
CURSOR_FAST_SUFFIX = "-fast"


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
    """
    if not model:
        return model, None, None

    stem, fast = model, ""
    if stem.endswith(CURSOR_FAST_SUFFIX):
        stem, fast = stem[: -len(CURSOR_FAST_SUFFIX)], CURSOR_FAST_SUFFIX

    family = None
    for candidate in CURSOR_EFFORT_SLUGS:
        if stem == candidate or stem.startswith(f"{candidate}-"):
            family = candidate
            break

    encoded = None
    if family is not None:
        for effort, slug in CURSOR_EFFORT_SLUGS[family].items():
            if stem == slug:
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

    target = CURSOR_EFFORT_SLUGS[family].get(requested_effort)
    if target is None:
        return (
            model,
            encoded,
            (
                f"family {family!r} publishes no {requested_effort!r} variant "
                f"(available: {', '.join(sorted(CURSOR_EFFORT_SLUGS[family]))}); "
                f"keeping {model!r}"
            ),
        )
    return f"{target}{fast}", requested_effort, None


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


# Wall-clock ceiling for one cursor cycle. Matches GROK_TIMEOUT_CAP so a lane's
# single-cycle budget is comparable across CLI backends.
CURSOR_TIMEOUT_CAP = _positive_int_env("WORKBAY_CURSOR_TIMEOUT", CURSOR_TIMEOUT_CAP_DEFAULT)

# Flags that would hand Cursor its own worktree. Refused by the adapter: the lane
# already has exactly one worktree owned by the lifecycle, and letting
# cursor-agent create a second under ~/.cursor/worktrees/ would add another
# vendor-owned representation of a lane's checkout.
FORBIDDEN_CURSOR_FLAGS = ("-w", "--worktree", "--worktree-base")
