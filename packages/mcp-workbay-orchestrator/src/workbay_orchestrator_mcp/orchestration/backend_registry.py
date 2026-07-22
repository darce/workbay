import importlib
from pathlib import Path
import os
import subprocess
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable

from workbay_protocol.remote_probe import probe_remote_gate, resolve_remote_gate_host

from workbay_orchestrator_mcp.orchestration.backend_adapter import BackendAdapter
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import DEFAULT_CURSOR_MODEL
from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL
from workbay_orchestrator_mcp.orchestration.host_resources import COST_HEAVY, COST_REMOTE, COST_REMOTE_API


@dataclass(frozen=True)
class BackendCapabilities:
    is_available: bool = False
    supports_structured_output: bool = False
    supports_sandbox: bool = False
    supports_sync_turn: bool = False
    supports_reasoning_effort: bool = False
    preflight_tokenizer_family: str | None = None
    # Whether this backend reliably emits per-turn token usage that reaches
    # WorkerRunContext.cumulative_tokens. Only telemetry-capable backends are
    # subject to the offload token-budget hard-error on a zero-token turn; a
    # backend that cannot self-meter is governed by the turn-count + deadline
    # bounds instead (internal). Defaults True to
    # preserve strict governance; a backend known not to emit usage (grok-cli)
    # declares False so a working turn is not mislabeled a contract violation.
    supports_token_telemetry: bool = True
    # grok-family CLI worker: no per-turn API token telemetry, governed by
    # token_budget-derived cycle bounds (max_turns/timeout) + a per-turn kill
    # switch. Routes M6 cycle-governance without a hardcoded backend literal.
    supports_token_budget_cycle_bounds: bool = False
    # Backend whose adapter takes a bare ``timeout`` ctor kwarg and NO grok
    # kwargs — the wall-clock-only family (cursor-cli). Declared separately
    # because ``supports_token_budget_cycle_bounds`` is overloaded: it means
    # both "derive cycle bounds" AND "construct with grok kwargs
    # (grok_bin/grok_args/max_turns)". A wall-clock-only backend needs the first
    # without the second, and conflating them meant its derived timeout was
    # computed, advertised in preflight governance, and then never passed to the
    # adapter — a bound that reads as enforced while the ctor default silently
    # applied instead. This is the adapter-family routing the call sites' own
    # coupling notes said was missing.
    supports_adapter_timeout_bounds: bool = False
    # Whether this backend runs the lane's TEST_CMD (self-verify) OFF-BOX on the
    # same remote host as the agent and captures the outcome into
    # BackendResult.off_box_self_verify. When True the worker CONSUMES that
    # captured result rather than re-running the suite locally in a venv-less
    # linked worktree (which exits 127) — REF-20 / OBS-08. grok-remote today,
    # codex-remote next; a declared capability, never a backend-name check (REF-24).
    runs_self_verify_off_box: bool = False


@dataclass(frozen=True)
class BackendSpec:
    kind: str
    adapter_path: str
    description: str
    module: str | None = None
    capabilities: BackendCapabilities = field(default_factory=BackendCapabilities)
    # Host-memory admission cost class (internal D1). A
    # remote-API CLI driver (inference off-box, small local RSS) declares
    # COST_REMOTE_API so it is sized on a small footprint yet still thrash-gated;
    # genuinely in-process workers stay COST_HEAVY.
    cost_class: str = COST_HEAVY

    @property
    def adapter_class(self) -> type[BackendAdapter]:
        module_name, class_name = self.adapter_path.rsplit(".", 1)
        return getattr(importlib.import_module(module_name), class_name)


BACKENDS: dict[str, BackendSpec] = {
    "codex-cli": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.codex_cli.CodexCliAdapter",
        description="Shell out to codex exec.",
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=False,
            preflight_tokenizer_family="tiktoken",
        ),
    ),
    "codex-subagent": BackendSpec(
        kind="bridge",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.codex_subagent.CodexSubagentAdapter",
        module="workbay_codex_bridge",
        description="Codex app-server via bridge module.",
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=True,
            # Codex app-server bridge forwards `effort` to start_turn (internal
            # implementation note); declare it so offload effort selection is truthful.
            supports_reasoning_effort=True,
            preflight_tokenizer_family="tiktoken",
        ),
    ),
    "copilot-host": BackendSpec(
        kind="bridge",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.codex_subagent.CodexSubagentAdapter",
        module="vscode_copilot_bridge",
        description="VS Code Copilot runSubagent bridge (no worktree isolation).",
        capabilities=BackendCapabilities(
            supports_structured_output=False,
            supports_sandbox=False,
            supports_sync_turn=True,
        ),
    ),
    "claude-code": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.claude_code.ClaudeCodeAdapter",
        description="Anthropic Claude Code CLI.",
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=False,
            supports_reasoning_effort=True,
        ),
    ),
    "grok-cli": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.grok_cli.GrokCliAdapter",
        # Remote-API driver: grok inference runs off-box, so the lane worker's
        # local RSS is small — admit it under normal memory on a small host
        # instead of force-sizing it as a heavy worker (D1/PF-1).
        cost_class=COST_REMOTE_API,
        # implementation note S3 [REF-19]/DATA-14]: pin slug single-sourced from DEFAULT_GROK_MODEL.
        description=(
            f"Shell out to the grok CLI (xAI {DEFAULT_GROK_MODEL} junior worker; pin via WORKBAY_GROK_MODEL)."
        ),
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=False,
            # grok declares reasoning-effort directly (unlike codex-cli, which
            # probes `exec --help`); see implementation note D6 / REQUEST A1.
            supports_reasoning_effort=True,
            # grok-cli's envelope emits no per-turn API token usage; the
            # adapter self-meters only approximately via session context-fill
            # deltas (surfaced as usage_source="grok_context_delta" /
            # context_delta_total — a different unit, never an API token
            # count). Declare no telemetry: the offload token-budget governor
            # must fall back to turn/time bounds rather than hard-erroring a
            # working turn (internal / TB-001).
            supports_token_telemetry=False,
            supports_token_budget_cycle_bounds=True,
        ),
    ),
    "grok-remote": BackendSpec(
        kind="cli",  # remoteness is carried by cost_class + the adapter, not a novel kind
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.remote_exec.RemoteExecAdapter",
        # FULLY off-box: agent execution + tests run on the VM (which enforces its
        # OWN admission), so the local host-memory guard must not gate it. Distinct
        # from grok-cli's COST_REMOTE_API, whose tests run LOCALLY (internal-
        # OFFBOX-EXEMPT-01).
        cost_class=COST_REMOTE,
        description=(
            f"Ship each grok turn to the remote OCI VM (WORKBAY_REMOTE_GATE_HOST); agent "
            f"execution + tests run off-box, the commit lands locally (xAI {DEFAULT_GROK_MODEL} worker)."
        ),
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_reasoning_effort=True,
            supports_token_telemetry=False,  # grok emits no per-turn API token usage
            supports_sandbox=False,  # sandboxing is the VM's job, not a local shallow clone
            supports_token_budget_cycle_bounds=True,
            # Agent + tests run on the VM; the worker consumes the VM-captured
            # self-verify instead of a broken/redundant local re-run (item 26).
            runs_self_verify_off_box=True,
        ),
    ),
    "cursor-cli": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.cursor_cli.CursorCliAdapter",
        # Same shape as grok-cli: inference runs off-box so local RSS is small,
        # but the lane's TEST_CMD still runs LOCALLY — hence COST_REMOTE_API
        # (thrash-gated on a small footprint), not COST_REMOTE.
        cost_class=COST_REMOTE_API,
        description=(
            f"Shell out to the Cursor CLI (cursor-agent) running {DEFAULT_CURSOR_MODEL}; "
            f"pin via WORKBAY_CURSOR_MODEL. Harness and model are separate axes here — "
            f"cursor takes the model as a parameter."
        ),
        capabilities=BackendCapabilities(
            # cursor-agent has NO --json-schema/--output-schema equivalent; the
            # adapter recovers the result from prose via extract_result_payload
            #. Declared false so nothing downstream assumes
            # a vendor-enforced shape.
            supports_structured_output=False,
            supports_sandbox=True,  # --sandbox enabled|disabled
            supports_sync_turn=False,
            # Effort is carried by SELECTING A PUBLISHED SLUG
            # (cursor-grok-4.5-low|medium|high) — not by a flag, and NOT by the
            # bracket parameterization the CLI's own --help advertises, which a
            # live turn rejects ("Cannot use this model"). True because the
            # adapter really does switch slug, including for a pinned model.
            # When a family publishes no variant for the requested effort the
            # adapter keeps the pin, logs it, and reports the effort actually
            # encoded rather than the one requested.
            supports_reasoning_effort=True,
            # Unverified while the CLI is logged out: declaring telemetry we have
            # not observed would let the budget governor hard-error a working
            # turn as a contract violation. False routes governance to the
            # wall-clock bound instead (internal).
            supports_token_telemetry=False,
            # NOT grok's turn+time pair: cursor-agent has no --max-turns, so a
            # derived max_turns would be a bound nothing enforces. This backend
            # is bounded by wall-clock only; the offload profile says so via
            # BOUND_ADAPTER_TIMEOUT. False here also keeps cursor clear of the
            # grok ctor kwargs at the four get_adapter sites...
            supports_token_budget_cycle_bounds=False,
            # ...and this is how the derived wall-clock bound still REACHES the
            # adapter. Without it the timeout was computed and advertised but
            # never applied, so every cycle silently ran to the 900s ctor
            # default no matter how small the lane's budget.
            supports_adapter_timeout_bounds=True,
        ),
    ),
    "structured-turn": BackendSpec(
        kind="in-process",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.structured_turn.StructuredTurnAdapter",
        description="Always-available in-repo adapter that composes run_structured_turn; anchors cross-vendor equivalence coverage.",
        capabilities=BackendCapabilities(
            is_available=True,
            supports_structured_output=True,
            supports_sandbox=False,
            supports_sync_turn=True,
        ),
    ),
    "local-model-openai": BackendSpec(
        kind="api",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.local_model.LocalModelAdapter",
        description="Generic OpenAI-compatible local model API.",
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=False,
            preflight_tokenizer_family="tiktoken",
        ),
    ),
}


def get_backend_choices() -> tuple[str, ...]:
    return tuple(BACKENDS.keys())


def register_backend(name: str, spec: BackendSpec) -> None:
    BACKENDS[name] = spec


def validate_backend(name: str) -> str:
    normalized = name.strip()
    if normalized not in BACKENDS:
        raise RuntimeError(f"Unsupported execution backend '{name}'. Valid values: {', '.join(get_backend_choices())}")
    return normalized


def get_backend_spec(name: str) -> BackendSpec:
    return BACKENDS[validate_backend(name)]


def cost_class_for_backend(name: str | None) -> str:
    """Host-memory admission cost class for an offload backend (internal-
    COSTCLASS-01 D1). Single source: the backend profile. An unknown/None backend
    falls back to COST_HEAVY — the conservative (most-gated, largest-RSS) class, so
    a misconfiguration never *under*-reserves host memory."""
    normalized = (name or "").strip()
    spec = BACKENDS.get(normalized)
    return spec.cost_class if spec is not None else COST_HEAVY


def resolve_bridge(name: str) -> Callable[..., dict[str, Any] | str]:
    spec = get_backend_spec(name)
    if spec.kind != "bridge" or not spec.module:
        raise RuntimeError(f"Backend '{name}' does not expose a bridge runner.")
    try:
        bridge = importlib.import_module(spec.module)
    except ImportError as exc:
        raise RuntimeError(
            f"{name} backend is unavailable in this runtime. Provide a host bridge module named '{spec.module}'."
        ) from exc

    runner = getattr(bridge, "run_subagent", None)
    if not callable(runner):
        raise RuntimeError(f"{spec.module}.run_subagent is required for the {name} backend.")
    return runner


def backend_supports_token_telemetry(name: str | None) -> bool:
    """Whether ``name`` reliably emits per-turn token usage.

    Resolved from the declarative :class:`BackendSpec` (no adapter
    instantiation, no side effects). Unknown/None names default to ``True`` so
    the offload token-budget contract stays strict for anything not explicitly
    declared telemetry-free (internal).
    """
    spec = BACKENDS.get(name) if name else None
    if spec is None:
        return True
    return spec.capabilities.supports_token_telemetry


def backend_supports_token_budget_cycle_bounds(name: str | None) -> bool:
    """True if this backend derives per-cycle bounds (max_turns/timeout) from
    token_budget and enforces a per-turn timeout (grok-family CLI workers).
    Unknown / None → False."""
    if not name:
        return False
    spec = BACKENDS.get(name)
    return bool(spec and spec.capabilities.supports_token_budget_cycle_bounds)


def backend_supports_adapter_timeout_bounds(name: str | None) -> bool:
    """True if this backend's adapter takes a bare ``timeout`` ctor kwarg.

    The wall-clock-only family: derived single-cycle bounds must be threaded as
    ``timeout=`` WITHOUT the grok ctor kwargs. Unknown / None → False.
    """
    if not name:
        return False
    spec = BACKENDS.get(name)
    return bool(spec and spec.capabilities.supports_adapter_timeout_bounds)


def backend_derives_cycle_bounds(name: str | None) -> bool:
    """True if a token_budget-derived single-cycle bound applies at all.

    Union of the grok family (turns + time) and the wall-clock-only family.
    Call sites deciding *whether to derive* should use this; call sites deciding
    *which ctor kwargs to pass* must use the specific predicate.
    """
    return backend_supports_token_budget_cycle_bounds(name) or backend_supports_adapter_timeout_bounds(name)


def backend_runs_self_verify_off_box(name: str | None) -> bool:
    """True if this backend runs the lane self-verify (TEST_CMD) OFF-BOX and
    reports it via ``BackendResult.off_box_self_verify`` (grok-remote today,
    codex-remote next). Resolved from the declarative :class:`BackendSpec` — no
    adapter instantiation, no backend-name check. Unknown / None → False."""
    if not name:
        return False
    spec = BACKENDS.get(name)
    return bool(spec and spec.capabilities.runs_self_verify_off_box)


def get_adapter(name: str, **kwargs: Any) -> BackendAdapter:
    """Get an initialized adapter instance for the named backend."""
    spec = get_backend_spec(name)
    module_name, class_name = spec.adapter_path.rsplit(".", 1)
    cls = getattr(importlib.import_module(module_name), class_name)

    if spec.kind == "bridge":
        runner = resolve_bridge(name)
        return cls(runner, name=name)  # type: ignore[call-arg]

    if spec.kind == "in-process":
        # Thread the registry name so probe/dispatch identify the adapter by
        # construction, not by single-backend coincidence (internal review).
        return cls(name=name, **kwargs)  # type: ignore[call-arg]

    # For CLI, we might pass codex_bin/args
    return cls(**kwargs)  # type: ignore[call-arg]


def detect_runtime() -> str | None:
    # ... (existing detect_runtime)
    if os.environ.get("VSCODE_PID") or os.environ.get("VSCODE_IPC_HOOK_CLI"):
        if "copilot" in os.environ.get("VSCODE_AGENT_FOLDER", "").lower():
            return "copilot-host"
    return None


def _resolve_remote_probe_repo_root(workspace_root: Path | str | None = None) -> Path:
    """Resolve the consumer-repo root used for ``.workbay/remote-gate.env`` (bra3).

    Prefer an explicit workspace root (MCP server configured workspace), then
    the git common-dir parent of that workspace (same as doctor / remote_agent.sh),
    with ``Path.cwd()`` only as the last resort when no git root is available.

    Defensive: non-path / unresolvable ``workspace_root`` values fall back to
    cwd so probe seams that pass mocks or unexpected types stay fail-closed
    rather than raising on ``Path(...)``.
    """
    start = Path.cwd()
    if workspace_root is not None:
        try:
            start = Path(workspace_root).expanduser().resolve()
        except (TypeError, ValueError, OSError):
            start = Path.cwd()
    try:
        completed = subprocess.run(  # noqa: S603
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
            cwd=str(start),
        )
        common_dir = (completed.stdout or "").strip()
        if common_dir:
            try:
                return Path(common_dir).parent
            except (TypeError, ValueError, OSError):
                pass
    except (subprocess.SubprocessError, OSError, ValueError, TypeError):
        pass
    return start


def probe_capabilities(name: str, *, workspace_root: Path | str | None = None) -> BackendCapabilities:
    """Probe the environment to see if a backend is available and what it supports."""
    spec = get_backend_spec(name)
    base = spec.capabilities

    if name == "grok-remote":
        return _probe_grok_remote(workspace_root=workspace_root)["capabilities"]

    if name == "codex-cli":
        from workbay_orchestrator_mcp.orchestration.adapters.codex_cli import find_codex  # noqa: PLC0415

        try:
            bin_path = find_codex()
            # Probe for reasoning-effort
            help_res = subprocess.run(
                [bin_path, "exec", "--help"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            has_reasoning = "reasoning-effort" in help_res.stdout

            # replace preserves declared capability flags (telemetry, cycle-bounds, …).
            return replace(
                base,
                is_available=True,
                supports_reasoning_effort=has_reasoning,
            )
        except (RuntimeError, subprocess.TimeoutExpired):
            return BackendCapabilities(is_available=False)

    if name == "codex-subagent" or name == "copilot-host":
        try:
            resolve_bridge(name)
            # Carry the declared reasoning-effort flag so a direct
            # probe_capabilities("codex-subagent") is truthful; the bridge
            # path already supports effort (BACKENDS declares it True).
            return replace(base, is_available=True)
        except RuntimeError:
            return BackendCapabilities(is_available=False)

    if name == "claude-code":
        try:
            result = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            if result.returncode == 0:
                return replace(base, is_available=True)
        except (OSError, subprocess.TimeoutExpired):
            # Broadened from FileNotFoundError so *any* OSError subclass from
            # subprocess.run (PermissionError on a non-executable `grok`/`claude`
            # shim, NotADirectoryError on a broken PATH entry) reports the single
            # backend unavailable instead of propagating out of probe_capabilities
            # and turning list_available_backends' one broad except into a total
            # listing failure that hides every other backend. FileNotFoundError is
            # an OSError subclass, so the not-installed case stays covered.
            pass
        return BackendCapabilities(is_available=False)

    if name == "grok-cli":
        try:
            result = subprocess.run(
                ["grok", "--version"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            if result.returncode == 0:
                # grok declares reasoning-effort in its spec (no --help probe).
                return replace(base, is_available=True, supports_reasoning_effort=True)
        except (OSError, subprocess.TimeoutExpired):
            # Broadened from FileNotFoundError so *any* OSError subclass from
            # subprocess.run (PermissionError on a non-executable `grok`/`claude`
            # shim, NotADirectoryError on a broken PATH entry) reports the single
            # backend unavailable instead of propagating out of probe_capabilities
            # and turning list_available_backends' one broad except into a total
            # listing failure that hides every other backend. FileNotFoundError is
            # an OSError subclass, so the not-installed case stays covered.
            pass
        return BackendCapabilities(is_available=False)

    if name == "cursor-cli":
        return _probe_cursor_cli(base)[0]

    return base


# Cursor auth states, reported separately from install state because they need
# different operator actions (install the CLI vs. log it in).
CURSOR_OK = "ok"
CURSOR_NOT_INSTALLED = "not_installed"
CURSOR_NOT_AUTHENTICATED = "not_authenticated"


def _probe_cursor_cli(base: BackendCapabilities) -> tuple[BackendCapabilities, str]:
    """Probe cursor-agent install AND auth, returning (capabilities, state).

    Unlike grok/claude, presence of the binary is NOT sufficient: ``cursor-agent``
    ships logged-out and every headless turn then fails with "Authentication
    required". Reporting an unauthenticated CLI as available would be a false
    green that only surfaces as a failed lane cycle, so auth is part of
    availability here. The two failure modes stay distinguishable so
    :func:`probe_availability` can emit the right remedy.

    Resolved via ``cursor-agent`` explicitly, never the bare name ``agent``,
    which the grok CLI also installs.
    """
    from workbay_orchestrator_mcp.orchestration.adapters.cursor_cli import (  # noqa: PLC0415
        find_cursor_agent,
    )

    try:
        cursor_bin = find_cursor_agent()
    except RuntimeError:
        return BackendCapabilities(is_available=False), CURSOR_NOT_INSTALLED

    try:
        version = subprocess.run(
            [cursor_bin, "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if version.returncode != 0:
            return BackendCapabilities(is_available=False), CURSOR_NOT_INSTALLED

        status = subprocess.run(
            [cursor_bin, "status"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
        combined = f"{status.stdout or ''}{status.stderr or ''}"
        if status.returncode != 0 or "Not logged in" in combined or "Authentication required" in combined:
            return BackendCapabilities(is_available=False), CURSOR_NOT_AUTHENTICATED
    except (OSError, subprocess.TimeoutExpired):
        # Broad OSError for the same reason as the grok/claude branches: one
        # backend's probe failure must not sink the whole listing.
        return BackendCapabilities(is_available=False), CURSOR_NOT_INSTALLED

    return replace(base, is_available=True), CURSOR_OK


# Availability states surfaced by :func:`probe_availability` and, in turn, by the
# ``probe=True`` view of ``list_available_backends``. These deliberately separate
# the two failure modes the static declaration table conflates:
#   * AVAIL_NOT_INSTALLED — the backend is *declared* in :data:`BACKENDS` but its
#     optional host module is not importable in *this* runtime (e.g. the
#     orchestrator launched from a venv that lacks ``workbay-codex-bridge``).
#   * AVAIL_REACHABLE     — the bridge module imports and exposes a runner, so a
#     dispatch *can* reach it; liveness is NOT verified (a real
#     ``run_structured_turn`` may still time out or error at dispatch).
AVAIL_AVAILABLE = "available"
AVAIL_REACHABLE = "reachable"
AVAIL_NOT_INSTALLED = "declared_not_installed"
AVAIL_UNAVAILABLE = "unavailable"
AVAIL_UNKNOWN = "unknown"


def _availability_caps(base: BackendCapabilities, *, is_available: bool) -> BackendCapabilities:
    # Prefer replace so newly added capability fields (telemetry, cycle-bounds,
    # …) are never dropped when flipping only is_available.
    return replace(base, is_available=is_available)


_REMOTE_GATE_HOST_ENV = "WORKBAY_REMOTE_GATE_HOST"
_GROK_REMOTE_REMEDY = (
    "grok-remote requires WORKBAY_REMOTE_GATE_HOST to point at a provisioned OCI VM "
    "(grok CLI + auth present on the host). See docs/runbooks/remote-gate-provisioning.md."
)


def grok_remote_dispatch_block_reason(backend_name: str) -> str | None:
    """Dispatch gate for grok-remote (implementation note H4; [RES-13]/[RES-15]).

    S3 (cross-host admission fail-closed) and S5 (per-lane concurrency caps) have
    landed, so dispatch is permitted for grok-remote too. Bounds come from:

    - availability probe (unset ``WORKBAY_REMOTE_GATE_HOST`` → unavailable, typed skip)
    - VM admission floor + lane cap (``remote_agent.sh`` exit 75 → ``admission_deferred``)
    - per-scope ``MemoryMax`` / ``CPUQuota`` on each named ``grok-lane-*`` systemd scope

    Returns ``None`` when dispatch is permitted (always, for every backend today —
    including grok-remote). The function stays as a chokepoint at the pass-engine
    and daemon spawn edges so a future refusal can re-engage without rewiring
    callers. The direct ``RemoteExecAdapter.execute`` path is intentionally not
    gated here.
    """
    if backend_name != "grok-remote":
        return None
    return None


# [RES-08] Short TTL cache for the live SSH probe only. Keyed by host; expires via
# time.monotonic(). Early returns (unset host / malformed host) are never cached.
_PROBE_GROK_REMOTE_TTL_S = 30.0
_probe_grok_remote_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _probe_grok_remote(*, workspace_root: Path | str | None = None) -> dict[str, Any]:
    """Availability probe for grok-remote (implementation note S1). Never raises (fail-closed).

    Thin wrapper over the shared ``workbay_protocol.remote_probe.probe_remote_gate``
    (implementation note S1 extraction): the installer verifies ``install --with-remote``
    through the same probe, so install-time and dispatch-time reachability
    semantics cannot drift. This wrapper owns only the capability mapping and
    the TTL cache.

    Opt-in: no ``WORKBAY_REMOTE_GATE_HOST`` => ``declared_not_installed`` (a clean typed
    skip, mirroring copilot-host). With a host set, verify SSH reachability + that the VM
    grok CLI is present; any failure => ``unavailable``.

    SSH result branches (available / unavailable) are cached ~30s per host [RES-08] so
    repeated ``list_available_backends(probe=True)`` calls do not pay a network RTT each
    time. Unset-host and malformed-host early returns are never cached.

    ``workspace_root`` (bra3): explicit consumer-repo root for the
    ``.workbay/remote-gate.env`` fallback. MCP servers/daemons rarely run from
    that root; deriving it via git common-dir (doctor semantics) keeps the
    file-based config path working. Fail-closed when neither env nor file names
    a host.
    """
    base = BACKENDS["grok-remote"].capabilities
    # Env first (no git / no workspace root needed), then the repo config-file
    # fallback (.workbay/remote-gate.env under the consumer workspace / git
    # common-dir parent) — non-login-shell harnesses do not inherit the
    # operator's env, which false-reported a provisioned gate as absent.
    # Never rely on Path.cwd() alone (bra3). Resolving env before the git
    # common-dir lookup also keeps the CWE-88 pre-ssh host refusal free of
    # any subprocess when WORKBAY_REMOTE_GATE_HOST is set.
    host = (resolve_remote_gate_host(None) or "").strip()
    if not host:
        repo_root = _resolve_remote_probe_repo_root(workspace_root)
        host = (resolve_remote_gate_host(repo_root) or "").strip()
    if not host:
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_NOT_INSTALLED,
            "detail": _GROK_REMOTE_REMEDY,
        }
    # Fail-closed on a malformed host before any ssh (or other) subprocess
    # (SEC / RES-13 / CWE-88): leading '-' is parsed by ssh as an option
    # (e.g. -oProxyCommand=…), and whitespace can smuggle extra tokens.
    # probe_remote_gate repeats this guard; keep it here so the orchestrator
    # path refuses independently of shared-probe ordering and never shells out.
    if host.startswith("-") or any(ch.isspace() for ch in host):
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                "Remote gate host is malformed and was refused before probing "
                f"(leading '-' or whitespace): {host!r}."
            ),
        }
    now = time.monotonic()
    cached = _probe_grok_remote_cache.get(host)
    if cached is not None:
        expires_at, payload = cached
        if now < expires_at:
            return payload
        # Expired entry — drop so the dict cannot grow unbounded [RES-08].
        del _probe_grok_remote_cache[host]
    probe = probe_remote_gate(host)
    payload = {
        "capabilities": _availability_caps(base, is_available=probe.ok),
        "is_available": probe.ok,
        "state": AVAIL_AVAILABLE if probe.ok else AVAIL_UNAVAILABLE,
        "detail": probe.detail,
    }
    if probe.cacheable:
        _probe_grok_remote_cache[host] = (now + _PROBE_GROK_REMOTE_TTL_S, payload)
    return payload


def probe_availability(name: str, *, workspace_root: Path | str | None = None) -> dict[str, Any]:
    """Probe a backend and classify its availability for callers that surface it.

    Returns a dict with the probed :class:`BackendCapabilities` under
    ``capabilities``, the boolean ``is_available``, a coarse ``state`` (one of the
    ``AVAIL_*`` constants), and a human ``detail``. This is the single seam where
    the "declared but not installed" vs. "installed but not live" distinction is
    decided, so both the MCP tool and the CLI stay consistent.

    Contract: ``is_available``/``state`` reflect *reachability* — a CLI binary on
    PATH, a bridge module that imports and exposes ``run_subagent``, or an
    in-process adapter. They do NOT guarantee a successful turn: a ``reachable``
    bridge can still time out at dispatch. This is a cheap probe and may shell out
    to ``codex``/``claude`` or import an optional bridge module, so it is gated off
    the hot path by callers.

    ``workspace_root`` is threaded to the grok-remote probe (bra3) so the
    config-file host fallback resolves against the consumer repo.
    """
    spec = get_backend_spec(name)

    if name == "grok-remote":
        return _probe_grok_remote(workspace_root=workspace_root)

    if spec.kind == "in-process":
        caps = _availability_caps(spec.capabilities, is_available=True)
        probed: dict[str, Any] = {
            "capabilities": caps,
            "is_available": caps.is_available,
            "state": AVAIL_AVAILABLE,
            "detail": "In-process adapter; always available without a host prerequisite.",
        }
        # internal: in-process adapters compose a downstream backend; a real turn
        # needs that downstream to be reachable even though the adapter itself
        # is always importable. Annotate the probe (additive `downstream` key +
        # enriched detail) so probe-first routers see the true prerequisite.
        # Reachability contract for `is_available`/`state` is unchanged.
        downstream_name: str | None = None
        try:
            adapter = get_adapter(name)
            downstream_name = getattr(adapter, "downstream_backend", None)
        except Exception:  # pragma: no cover - probe must stay fail-open
            downstream_name = None
        if downstream_name:
            downstream_spec = get_backend_spec(downstream_name)
            if downstream_spec.kind == "in-process":
                # structural recursion guard mirror: never recurse the probe
                downstream_info = {
                    "backend": downstream_name,
                    "state": AVAIL_UNAVAILABLE,
                    "is_available": False,
                    "detail": f"Downstream backend '{downstream_name}' is in-process; recursive composition is refused at dispatch.",
                }
            else:
                downstream_probe = probe_availability(downstream_name)
                downstream_info = {
                    "backend": downstream_name,
                    "state": downstream_probe["state"],
                    "is_available": downstream_probe["is_available"],
                    "detail": downstream_probe["detail"],
                }
            probed["downstream"] = downstream_info
            probed["detail"] += (
                f" Composes downstream backend '{downstream_name}' ({downstream_info['state']});"
                " a successful turn requires that downstream to be reachable."
            )
        return probed

    if spec.kind == "bridge":
        if not spec.module:
            caps = _availability_caps(spec.capabilities, is_available=False)
            return {
                "capabilities": caps,
                "is_available": False,
                "state": AVAIL_UNAVAILABLE,
                "detail": f"Bridge backend '{name}' does not declare a host module.",
            }
        try:
            bridge = importlib.import_module(spec.module)
        except ImportError:
            caps = _availability_caps(spec.capabilities, is_available=False)
            return {
                "capabilities": caps,
                "is_available": False,
                "state": AVAIL_NOT_INSTALLED,
                "detail": (
                    f"Bridge module '{spec.module}' is not importable in this runtime. "
                    "Install the git-sourced bridge closure, for example: "
                    "REF=workbay-v0.3.8; R=git+https://github.com/darce/workbay.git@$REF; "
                    "uv tool install --no-sources "
                    '--with "$R#subdirectory=packages/workbay-protocol" '
                    '--with "$R#subdirectory=packages/mcp-workbay-handoff" '
                    '--with "$R#subdirectory=packages/workbay-codex-bridge" '
                    '--from "$R#subdirectory=packages/mcp-workbay-orchestrator" '
                    "mcp-workbay-orchestrator. Then reconnect/restart the MCP server."
                ),
            }

        runner = getattr(bridge, "run_subagent", None)
        if callable(runner):
            caps = _availability_caps(spec.capabilities, is_available=True)
            return {
                "capabilities": caps,
                "is_available": True,
                "state": AVAIL_REACHABLE,
                "detail": (
                    f"Bridge module '{spec.module}' is importable and exposes a runner; "
                    "liveness is not verified (a real turn may still time out at dispatch)."
                ),
            }

        caps = _availability_caps(spec.capabilities, is_available=False)
        return {
            "capabilities": caps,
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (f"Bridge module '{spec.module}' is importable but does not expose callable run_subagent."),
        }

    if name == "cursor-cli":
        # Distinct remedies: installing the CLI and logging it in are different
        # operator actions, and the generic "binary not found on PATH" detail
        # would send an operator hunting for a binary that is already there.
        caps, cursor_state = _probe_cursor_cli(spec.capabilities)
        if cursor_state == CURSOR_OK:
            return {
                "capabilities": caps,
                "is_available": True,
                "state": AVAIL_AVAILABLE,
                "detail": "cursor-agent found and authenticated.",
            }
        if cursor_state == CURSOR_NOT_AUTHENTICATED:
            return {
                "capabilities": caps,
                "is_available": False,
                "state": AVAIL_UNAVAILABLE,
                "detail": (
                    "cursor-agent is installed but not authenticated; every headless turn would "
                    "fail. Run 'cursor-agent login' (or set CURSOR_API_KEY) on this host."
                ),
            }
        return {
            "capabilities": caps,
            "is_available": False,
            "state": AVAIL_NOT_INSTALLED,
            "detail": (
                "cursor-agent CLI not found. Install the Cursor CLI, or set WORKBAY_CURSOR_BIN to "
                "its absolute path (never the bare name 'agent', which the grok CLI also provides)."
            ),
        }

    if spec.kind == "cli":
        caps = probe_capabilities(name)
        if caps.is_available:
            return {
                "capabilities": caps,
                "is_available": True,
                "state": AVAIL_AVAILABLE,
                "detail": "CLI binary found on PATH.",
            }
        return {
            "capabilities": caps,
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": "CLI binary not found on PATH.",
        }

    # Kinds with no probe implementation (e.g. ``api``): report declared caps only.
    caps = spec.capabilities
    return {
        "capabilities": caps,
        "is_available": caps.is_available,
        "state": AVAIL_UNKNOWN,
        "detail": "No probe implemented for this backend kind; declared capabilities only.",
    }
