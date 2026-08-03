"""Typed offload-agent profiles for /offload.

Ports-and-adapters seam (Farley): each offload backend owns its model policy,
allowed reasoning efforts, single-cycle bound kind, and manifest defaults, instead
of ``offload_preflight`` switching on backend-id strings. The grok constants and
the derived single-cycle bound live here as their canonical home;
``offload_preflight`` re-exports them so existing grok callers/tests stay green.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from workbay_orchestrator_mcp.orchestration import backend_registry
from workbay_orchestrator_mcp.orchestration._env import WORKER_REASONING_EFFORT_CHOICES
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
    CURSOR_TIMEOUT_CAP,
    DEFAULT_CURSOR_MODEL,
)
from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL

GROK_OFFLOAD_MODEL = DEFAULT_GROK_MODEL
CURSOR_OFFLOAD_MODEL = DEFAULT_CURSOR_MODEL
CURSOR_OFFLOAD_BACKEND = "cursor-cli"
GROK_OFFLOAD_BACKEND = "grok-cli"
# implementation note S2: under bootstrap execution_mode=remote_only the offload default
# flips to this remote backend; explicit local pins are refused (never silently
# substituted — [AGT-10]/[RLSE-05]).
REMOTE_ONLY_OFFLOAD_BACKEND = "grok-remote"
# Allowlist, not denylist (S2R-1): remote_only permits ONLY fully off-box
# backends. Any other explicit backend — including future registry additions —
# is refused, so a new local backend can never bypass the policy by omission.
REMOTE_OFFLOAD_BACKENDS = frozenset({REMOTE_ONLY_OFFLOAD_BACKEND})

# Single-cycle bounds (Release It! §5.1): sized from token_budget at the caller.
GROK_MAX_TURNS_CAP = 30
GROK_TIMEOUT_CAP = 900
ESTIMATED_TOKENS_PER_TURN = 4_000
MIN_TIMEOUT_SECONDS = 60
SECONDS_PER_TURN = 30

# Single-cycle bound kinds — how a profile guards one execution cycle.
BOUND_GROK_DERIVED = "grok_derived"  # max_turns/timeout derived from token_budget
BOUND_BRIDGE_TIMEOUT = "bridge_timeout"  # AppServerClient.timeout_seconds bridge path
# Wall-clock ONLY, enforced by the adapter's process-group kill. For a CLI that
# exposes no turn-count flag (cursor-agent has no --max-turns), where a derived
# max_turns would be a bound nothing enforces. Still a real [FM-08] bound — a
# cycle cannot run forever — so it satisfies the no-ungoverned-pass check in
# offload_preflight rather than being lumped in with "carries no bounds".
BOUND_ADAPTER_TIMEOUT = "adapter_timeout"


class OffloadPreflightError(RuntimeError):
    """Distinct Fail-Fast error for offload preconditions.

    Optional ``execution_mode`` / ``remote_probe_state`` carry the implementation note
    capability echo when the failure happens after those values are already
    computed (e.g. remote probe unavailable). Callers such as the MCP API map
    them onto the ``ok:false`` payload without a second probe.
    """

    def __init__(
        self,
        message: str,
        *,
        execution_mode: str | None = None,
        remote_probe_state: str | None = None,
    ) -> None:
        super().__init__(message)
        self.execution_mode = execution_mode
        self.remote_probe_state = remote_probe_state


def derive_grok_single_cycle_bounds(token_budget: int) -> dict[str, int]:
    """Derive per-invocation grok max_turns/timeout from the lane token budget."""
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    max_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(GROK_TIMEOUT_CAP, max(MIN_TIMEOUT_SECONDS, max_turns * SECONDS_PER_TURN))
    return {"max_turns": max_turns, "timeout": timeout}


def derive_adapter_timeout_bounds(token_budget: int) -> dict[str, int]:
    """Derive a wall-clock-ONLY single-cycle bound from the lane token budget.

    Same sizing arithmetic as :func:`derive_grok_single_cycle_bounds`, so a given
    budget buys comparable wall-clock across CLI backends, but deliberately omits
    ``max_turns``: the target CLI has no turn-count flag, and emitting a
    ``max_turns`` key that is never passed to the vendor would read as an
    enforced bound when nothing enforces it ([AGT-10] no silent caps).
    """
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    notional_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(CURSOR_TIMEOUT_CAP, max(MIN_TIMEOUT_SECONDS, notional_turns * SECONDS_PER_TURN))
    return {"timeout": timeout}


def derive_single_cycle_bounds(bound_kind: str, token_budget: int) -> dict[str, int] | None:
    """Resolve per-cycle bounds for a profile's declared bound kind.

    Returns ``None`` for kinds bounded somewhere else (the bridge path owns its
    own ``timeout_seconds``), which callers must read as "not governed HERE" —
    never as "ungoverned".
    """
    if bound_kind == BOUND_GROK_DERIVED:
        return derive_grok_single_cycle_bounds(token_budget)
    if bound_kind == BOUND_ADAPTER_TIMEOUT:
        return derive_adapter_timeout_bounds(token_budget)
    return None


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
    # Fully off-box grok execution (implementation note transport; registered as an
    # offload profile by implementation note/0152 coordination — exactly once, pulled
    # forward by whichever implements first). Same model pin and
    # token-budget-derived single-cycle bounds as grok-cli; the VM enforces
    # its own transport-side MAX_TURNS/admission floors in remote_agent.sh.
    "grok-remote": OffloadAgentProfile(
        agent="grok-remote",
        allowed_efforts=WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_GROK_DERIVED,
        pinned_model=GROK_OFFLOAD_MODEL,
        default_model=GROK_OFFLOAD_MODEL,
    ),
    # Cursor CLI running the grok model. Harness and model are separate axes
    # here, so the pin names the MODEL cursor should drive while the backend id
    # names the harness. Bounded by wall-clock only —
    # cursor-agent has no --max-turns — hence BOUND_ADAPTER_TIMEOUT.
    CURSOR_OFFLOAD_BACKEND: OffloadAgentProfile(
        agent=CURSOR_OFFLOAD_BACKEND,
        allowed_efforts=WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_ADAPTER_TIMEOUT,
        pinned_model=CURSOR_OFFLOAD_MODEL,
        default_model=CURSOR_OFFLOAD_MODEL,
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


def resolve_offload_backend_for_execution_mode(
    backend: str | None,
    *,
    repo_root: Path | str,
) -> tuple[str, str | None]:
    """Resolve an offload backend under bootstrap ``execution_mode`` (implementation note S2).

    Returns ``(resolved_backend, remote_required_error)``. When
    ``remote_required_error`` is not None the caller must refuse with outcome
    ``remote_required`` (or the equivalent validation error on non-pass surfaces)
    — never substitute a remote backend for an explicit local pin ([AGT-10],
    [RLSE-05]).

    Rules:
    - Explicit caller value wins for selection.
    - Missing/empty backend defaults to ``grok-remote`` under ``remote_only``,
      else ``grok-cli`` (today's default).
    - Any explicit backend outside :data:`REMOTE_OFFLOAD_BACKENDS` under
      ``remote_only`` is a policy refusal with a ledger-named remedy
      (``repair --no-remote``) — allowlist semantics so unlisted/new local
      backends cannot bypass by omission (S2R-1).
    """
    from workbay_protocol.bootstrap import load_execution_mode  # noqa: PLC0415

    mode = load_execution_mode(repo_root)
    explicit = backend is not None and str(backend).strip() != ""
    if explicit:
        resolved = str(backend).strip()
    elif mode == "remote_only":
        resolved = REMOTE_ONLY_OFFLOAD_BACKEND
    else:
        resolved = GROK_OFFLOAD_BACKEND

    if mode == "remote_only" and explicit and resolved not in REMOTE_OFFLOAD_BACKENDS:
        root = Path(repo_root).expanduser().resolve()
        error = (
            f"remote_required: bootstrap ledger execution_mode=remote_only at "
            f"{root}/.workbay-bootstrap.json refuses explicit local backend "
            f"{resolved!r}; use {REMOTE_ONLY_OFFLOAD_BACKEND!r} or run "
            f"`workbay repair --no-remote` to restore local_ok"
        )
        return resolved, error
    return resolved, None
