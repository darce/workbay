"""Typed offload-agent profiles for /offload.

Ports-and-adapters seam (Farley): each offload backend owns its model policy,
allowed reasoning efforts, single-cycle bound kind, and manifest defaults, instead
of ``offload_preflight`` switching on backend-id strings. The grok constants and
the derived single-cycle bound live here as their canonical home;
``offload_preflight`` re-exports them so existing grok callers/tests stay green.
"""

from __future__ import annotations

from dataclasses import dataclass

from workbay_orchestrator_mcp.orchestration import backend_registry
from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES
from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL

GROK_OFFLOAD_MODEL = DEFAULT_GROK_MODEL
GROK_OFFLOAD_BACKEND = "grok-cli"

# Single-cycle bounds (Release It! §5.1): sized from token_budget at the caller.
GROK_MAX_TURNS_CAP = 30
GROK_TIMEOUT_CAP = 900
ESTIMATED_TOKENS_PER_TURN = 4_000
MIN_TIMEOUT_SECONDS = 60
SECONDS_PER_TURN = 30

# Single-cycle bound kinds — how a profile guards one execution cycle.
BOUND_GROK_DERIVED = "grok_derived"  # max_turns/timeout derived from token_budget
BOUND_BRIDGE_TIMEOUT = "bridge_timeout"  # AppServerClient.timeout_seconds bridge path


class OffloadPreflightError(RuntimeError):
    """Distinct Fail-Fast error for offload preconditions."""


def derive_grok_single_cycle_bounds(token_budget: int) -> dict[str, int]:
    """Derive per-invocation grok max_turns/timeout from the lane token budget."""
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    max_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(GROK_TIMEOUT_CAP, max(MIN_TIMEOUT_SECONDS, max_turns * SECONDS_PER_TURN))
    return {"max_turns": max_turns, "timeout": timeout}


@dataclass(frozen=True)
class OffloadAgentProfile:
    """Backend-specific offload policy resolved from an explicit ``--agent`` id.

    ``allowed_efforts`` is drawn from the canonical
    :data:`_env.WORKER_REASONING_EFFORT_CHOICES` (reuse, not a re-derived list);
    ``auto|inherit`` members are resolved by ``_env.resolve_auto_reasoning_effort``
    at execution rather than pinned into the lane manifest. ``pinned_model`` forces
    an exact model (grok); ``None`` means the caller may pass an optional override.
    """

    agent: str
    allowed_efforts: tuple[str, ...]
    single_cycle_bound: str
    pinned_model: str | None = None
    default_model: str | None = None


OFFLOAD_AGENT_PROFILES: dict[str, OffloadAgentProfile] = {
    GROK_OFFLOAD_BACKEND: OffloadAgentProfile(
        agent=GROK_OFFLOAD_BACKEND,
        allowed_efforts=WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_GROK_DERIVED,
        pinned_model=GROK_OFFLOAD_MODEL,
        default_model=GROK_OFFLOAD_MODEL,
    ),
    "codex-subagent": OffloadAgentProfile(
        agent="codex-subagent",
        allowed_efforts=WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_BRIDGE_TIMEOUT,
        pinned_model=None,
        default_model=None,
    ),
}


def get_offload_profile(agent: str) -> OffloadAgentProfile:
    """Normalize an ``--agent`` id and require offload support (no fallback).

    Unknown backend ids raise ``RuntimeError`` via
    :func:`backend_registry.validate_backend`; a known-but-unsupported offload
    backend (e.g. ``codex-cli``, which has no single-cycle wall-clock bound yet)
    raises :class:`OffloadPreflightError`.
    """
    normalized = backend_registry.validate_backend(agent)
    profile = OFFLOAD_AGENT_PROFILES.get(normalized)
    if profile is None:
        raise OffloadPreflightError(
            f"backend {normalized!r} is not a supported /offload agent; "
            f"supported: {', '.join(OFFLOAD_AGENT_PROFILES)}. codex-cli is deferred "
            "until it carries a single-cycle wall-clock bound."
        )
    return profile
