"""Typed offload-agent profiles for /offload.

Ports-and-adapters seam (Farley): each offload backend owns its model policy,
allowed reasoning efforts, single-cycle bound kind, and manifest defaults, instead
of ``offload_preflight`` switching on backend-id strings. The grok turn sizing,
wall-clock SSOT, and derived single-cycle bounds live at their canonical homes;
``offload_preflight`` re-exports them so existing grok callers/tests stay green.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration import backend_registry
from workbay_orchestrator_mcp.orchestration._env import (
    STANDARD_WORKER_REASONING_EFFORT_CHOICES,
)
from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
    CODEX_ALLOWED_SPEEDS,
    DEFAULT_CODEX_MODEL,
)
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
    DEFAULT_CURSOR_MODEL,
)
from workbay_orchestrator_mcp.orchestration.grok_lane_config import (
    DEFAULT_GROK_MODEL,
    GROK_MAX_TURNS_CAP,
    SECONDS_PER_TURN,
)
from workbay_orchestrator_mcp.orchestration.offload_model_discovery import (  # noqa: F401
    MODEL_DISCOVERY_FAILED_WARNING,
    MODEL_DISCOVERY_TTL_S,
    PIN_HOME_UNDECLARED,
    TRACKED_PIN_NOT_IN_CATALOGUE_WARNING,
    ModelDiscovery,
    PinHomeUndeclaredError,
    build_offload_dispatch_receipt,
    build_remote_list_models_argv,
    clear_model_discovery_cache,
    parse_model_catalogue,
    pin_home_for,
    publish_resolved_model_pins,
    published_model_pins,
    resolve_offbox_model,
    restore_published_pins,
    snapshot_published_pins,
)
from workbay_orchestrator_mcp.orchestration.offload_timeout_ssot import (  # noqa: F401
    CODEX_TIMEOUT_CAP,
    CURSOR_TIMEOUT_CAP,
    CURSOR_TIMEOUT_CAP_DEFAULT,
    GROK_TIMEOUT_CAP,
    resolve_cursor_timeout_cap,
)
from workbay_orchestrator_mcp.orchestration.openrouter_lane_config import (
    DEFAULT_OPENROUTER_MODEL,
    OPENROUTER_ALLOWED_EFFORTS,
    OPENROUTER_TIMEOUT_CAP,
)

GROK_OFFLOAD_MODEL = DEFAULT_GROK_MODEL
CURSOR_OFFLOAD_BACKEND = "cursor-cli"
GROK_OFFLOAD_BACKEND = "grok-cli"
# implementation note S2: under bootstrap execution_mode=remote_only the offload default
# flips to this remote backend; explicit local pins are refused (never silently
# substituted — [AGT-10]/[RLSE-05]).
REMOTE_ONLY_OFFLOAD_BACKEND = "grok-remote"


def _spec_declares_dispatchable_off_box(spec: Any) -> bool:
    """True only when the row declares ``capabilities.dispatchable_off_box``.

    Execution-location policy is this capability flag (REF-24), not
    ``cost_class``. ``work_graph_compiler._backend_is_off_box`` remains the
    reviewer-ranking OR (flag OR ``COST_REMOTE``); this allowlist uses the
    flag alone so reclassifying cost cannot silently change who may run
    under ``remote_only``.
    """
    caps = getattr(spec, "capabilities", None)
    return bool(getattr(caps, "dispatchable_off_box", False))


def derive_remote_offload_backends(
    registry: Mapping[str, Any] | None = None,
) -> frozenset[str]:
    """Allowlist by declared ``dispatchable_off_box`` (REF-24), never by name.

    A new backend that omits the flag is refused under ``remote_only``
    (S2R-1 allowlist polarity). ``cost_class=COST_REMOTE`` without the flag
    is also refused — cost/sizing is not an execution-location declaration.
    """
    specs = backend_registry.BACKENDS if registry is None else registry
    return frozenset(name for name, spec in specs.items() if _spec_declares_dispatchable_off_box(spec))


class _DerivedRemoteOffloadBackends:
    """Live view of :func:`derive_remote_offload_backends` (not an import-time literal)."""

    def _current(self) -> frozenset[str]:
        return derive_remote_offload_backends()

    def __contains__(self, item: object) -> bool:
        return item in self._current()

    def __iter__(self):
        return iter(self._current())

    def __len__(self) -> int:
        return len(self._current())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (set, frozenset, _DerivedRemoteOffloadBackends)):
            return self._current() == frozenset(other)
        return NotImplemented

    def __repr__(self) -> str:
        return repr(self._current())


# Allowlist, not denylist (S2R-1): remote_only permits ONLY fully off-box
# backends. Derived from declared capabilities (REF-24 / implementation note M1) so
# codex-remote and cursor-remote are admitted by construction.
REMOTE_OFFLOAD_BACKENDS = _DerivedRemoteOffloadBackends()

# Sizing inputs for derive_*_single_cycle_bounds below (Release It! §5.1).
# GROK_MAX_TURNS_CAP / SECONDS_PER_TURN live in grok_lane_config; its wall-clock
# ceiling is owned by offload_timeout_ssot. Both are re-exported above so
# existing importers keep working. These two locals only scale a token_budget
# into notional turns and a floor on wall-clock before those ceilings apply.
ESTIMATED_TOKENS_PER_TURN = 4_000
MIN_TIMEOUT_SECONDS = 60

# Single-cycle bound kinds — how a profile guards one execution cycle.
# ``derive_single_cycle_bounds`` dispatches on these; each kind maps to either
# a token-budget-derived pair (turns+timeout), a wall-clock-only adapter kill,
# or "bounded elsewhere" (bridge path owns its own timeout_seconds).
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
        outcome: str | None = None,
    ) -> None:
        super().__init__(message)
        self.execution_mode = execution_mode
        self.remote_probe_state = remote_probe_state
        self.outcome = outcome


def derive_grok_single_cycle_bounds(token_budget: int) -> dict[str, int]:
    """Derive per-invocation grok max_turns/timeout from the lane token budget."""
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    max_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(GROK_TIMEOUT_CAP, max(MIN_TIMEOUT_SECONDS, max_turns * SECONDS_PER_TURN))
    return {"max_turns": max_turns, "timeout": timeout}


def describe_budget_bounds(token_budget: int) -> dict[str, int]:
    """Return token_budget with the derived max_turns and timeout wall-clock."""
    bounds = derive_grok_single_cycle_bounds(token_budget)
    return {
        "token_budget": token_budget,
        "max_turns": bounds["max_turns"],
        "timeout": bounds["timeout"],
    }


def derive_adapter_timeout_bounds(
    token_budget: int,
    *,
    timeout_cap: int | None = None,
) -> dict[str, int]:
    """Derive a wall-clock-ONLY single-cycle bound from the lane token budget.

    Same sizing arithmetic as :func:`derive_grok_single_cycle_bounds`, so a given
    budget buys comparable wall-clock across CLI backends, but deliberately omits
    ``max_turns``: the target CLI has no turn-count flag, and emitting a
    ``max_turns`` key that is never passed to the vendor would read as an
    enforced bound when nothing enforces it ([AGT-10] no silent caps).

    implementation note S3: the ceiling is per-profile via ``timeout_cap`` (codex-remote
    lanes size against the codex lane cap, not the cursor one). ``None`` falls
    back to ``CURSOR_TIMEOUT_CAP`` so every pre-S3 caller is bit-for-bit
    unchanged. Note the turns term alone saturates at
    ``GROK_MAX_TURNS_CAP * SECONDS_PER_TURN`` — raising the ceiling past that
    buys wall-clock only up to that saturation point.
    """
    if token_budget <= 0:
        raise ValueError("token_budget must be a positive integer")
    cap = CURSOR_TIMEOUT_CAP if timeout_cap is None else timeout_cap
    notional_turns = min(GROK_MAX_TURNS_CAP, max(1, token_budget // ESTIMATED_TOKENS_PER_TURN))
    timeout = min(cap, max(MIN_TIMEOUT_SECONDS, notional_turns * SECONDS_PER_TURN))
    return {"timeout": timeout}


def resolve_adapter_timeout_cap(backend: str) -> int:
    """Return the wall-clock ceiling a backend's adapter-timeout bound sizes against.

    Normalizes via :func:`backend_registry.validate_backend` and reads the
    profile's ``timeout_cap``. An unknown or profile-less backend — and any
    profile whose ``timeout_cap`` is None — returns ``CURSOR_TIMEOUT_CAP``
    rather than raising: this helper SIZES a bound, it does not authorize
    anything (authorization stays with ``get_offload_profile``).
    """
    try:
        normalized = backend_registry.validate_backend(backend)
    except RuntimeError:
        return CURSOR_TIMEOUT_CAP
    profile = OFFLOAD_AGENT_PROFILES.get(normalized)
    if profile is None or profile.timeout_cap is None:
        return CURSOR_TIMEOUT_CAP
    return profile.timeout_cap


def derive_single_cycle_bounds(
    bound_kind: str,
    token_budget: int,
    *,
    timeout_cap: int | None = None,
) -> dict[str, int] | None:
    """Resolve per-cycle bounds for a profile's declared bound kind.

    Returns ``None`` for kinds bounded somewhere else (the bridge path owns its
    own ``timeout_seconds``), which callers must read as "not governed HERE" —
    never as "ungoverned". ``timeout_cap`` is a passthrough used only on the
    ``BOUND_ADAPTER_TIMEOUT`` branch (implementation note S3); the grok-derived kind keeps
    its own grok ceilings.
    """
    if bound_kind == BOUND_GROK_DERIVED:
        return derive_grok_single_cycle_bounds(token_budget)
    if bound_kind == BOUND_ADAPTER_TIMEOUT:
        return derive_adapter_timeout_bounds(token_budget, timeout_cap=timeout_cap)
    return None


@dataclass(frozen=True)
class OffloadAgentProfile:
    """Backend-specific offload policy resolved from an explicit ``--agent`` id.

    ``allowed_efforts`` is drawn from a canonical backend-appropriate effort
    set; OpenRouter uses its per-model advertised union while standard profiles
    retain :data:`_env.STANDARD_WORKER_REASONING_EFFORT_CHOICES`.
    ``auto|inherit`` members are resolved by ``_env.resolve_auto_reasoning_effort``
    at execution rather than pinned into the lane manifest. ``pinned_model`` forces
    an exact model (grok); ``None`` means the caller may pass an optional override.
    ``timeout_cap`` (implementation note S3) is the wall-clock ceiling a
    ``BOUND_ADAPTER_TIMEOUT`` profile sizes against; ``None`` means the kind
    carries its own ceiling (grok-derived, bridge) and the adapter-timeout
    fallback applies.
    """

    agent: str
    allowed_efforts: tuple[str, ...]
    single_cycle_bound: str
    pinned_model: str | None = None
    default_model: str | None = None
    timeout_cap: int | None = None
    # Empty means the profile does not advertise a speed override.  This is
    # intentionally separate from reasoning effort: speed translates to a
    # codex service tier, while effort remains a model reasoning control.
    allowed_speeds: tuple[str, ...] = ()


OFFLOAD_AGENT_PROFILES: dict[str, OffloadAgentProfile] = {
    GROK_OFFLOAD_BACKEND: OffloadAgentProfile(
        agent=GROK_OFFLOAD_BACKEND,
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_GROK_DERIVED,
        pinned_model=GROK_OFFLOAD_MODEL,
        default_model=GROK_OFFLOAD_MODEL,
    ),
    "codex-subagent": OffloadAgentProfile(
        agent="codex-subagent",
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
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
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
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
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_ADAPTER_TIMEOUT,
        pinned_model=DEFAULT_CURSOR_MODEL,
        default_model=DEFAULT_CURSOR_MODEL,
        timeout_cap=CURSOR_TIMEOUT_CAP,
    ),
    # implementation note S2: /offload reaches every off-box backend. codex has no
    # --max-turns (supports_adapter_timeout_bounds=True on the registry row),
    # so the wall-clock-only bound applies; its ceiling is the codex lane
    # cap, never the cursor cap.
    "codex-remote": OffloadAgentProfile(
        agent="codex-remote",
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_ADAPTER_TIMEOUT,
        pinned_model=DEFAULT_CODEX_MODEL,
        default_model=DEFAULT_CODEX_MODEL,
        timeout_cap=CODEX_TIMEOUT_CAP,
        allowed_speeds=tuple(sorted(CODEX_ALLOWED_SPEEDS)),
    ),
    # implementation note S4: same codex transport and lane cap, different provider +
    # credential port. Pin is the tracked stealth slug (env > tracked; no
    # catalogue probe).
    # Efforts: the advertised union only (S4-M-05). "auto" is excluded because
    # every ladder rung is unadvertised by at least one curated model; "inherit"
    # lets the adapter apply the selected model's default (possibly no override).
    "openrouter-remote": OffloadAgentProfile(
        agent="openrouter-remote",
        allowed_efforts=("inherit", *sorted(OPENROUTER_ALLOWED_EFFORTS)),
        single_cycle_bound=BOUND_ADAPTER_TIMEOUT,
        pinned_model=DEFAULT_OPENROUTER_MODEL,
        default_model=DEFAULT_OPENROUTER_MODEL,
        timeout_cap=OPENROUTER_TIMEOUT_CAP,
    ),
    # cursor-agent on the VM, same no---max-turns shape as cursor-cli.
    "cursor-remote": OffloadAgentProfile(
        agent="cursor-remote",
        allowed_efforts=STANDARD_WORKER_REASONING_EFFORT_CHOICES,
        single_cycle_bound=BOUND_ADAPTER_TIMEOUT,
        pinned_model=DEFAULT_CURSOR_MODEL,
        default_model=DEFAULT_CURSOR_MODEL,
        timeout_cap=CURSOR_TIMEOUT_CAP,
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
    if profile.pinned_model is None and profile.default_model is None:
        return profile
    try:
        discovery = resolve_offbox_model(normalized, probe=False)
    except PinHomeUndeclaredError as exc:
        raise OffloadPreflightError(str(exc)) from exc
    except KeyError:
        return profile
    return replace(
        profile,
        pinned_model=discovery.resolved_model if profile.pinned_model is not None else None,
        default_model=discovery.resolved_model,
    )


def _remote_only_refusal(resolved: str, admitted: frozenset[str], repo_root: Path | str) -> str:
    root = Path(repo_root).expanduser().resolve()
    admitted_list = ", ".join(sorted(admitted)) or "(none)"
    return (
        f"remote_required: bootstrap ledger execution_mode=remote_only at "
        f"{root}/.workbay-bootstrap.json refuses backend {resolved!r} "
        f"(not declared off-box / dispatchable_off_box=False); "
        f"admitted remotes: {admitted_list}. Use one of those, or as a last "
        f"resort run `workbay repair --no-remote` to restore local_ok"
    )


def resolve_offload_backend_for_execution_mode(
    backend: str | None,
    *,
    repo_root: Path | str,
) -> tuple[str, str | None]:
    """Resolve an offload backend under bootstrap ``execution_mode`` (implementation note S2).

    Returns ``(resolved_backend, remote_required_error)``. When
    ``remote_required_error`` is not None the caller must refuse with outcome
    ``remote_required`` — never substitute a remote backend for an explicit
    local pin ([AGT-10], [RLSE-05]).
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

    admitted = derive_remote_offload_backends()
    if mode == "remote_only" and explicit and resolved not in admitted:
        return resolved, _remote_only_refusal(resolved, admitted, repo_root)
    return resolved, None
