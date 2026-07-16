import importlib
import os
import subprocess
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable

from workbay_orchestrator_mcp.orchestration.backend_adapter import BackendAdapter
from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL
from workbay_orchestrator_mcp.orchestration.host_resources import COST_HEAVY, COST_REMOTE_API


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
        cost_class=COST_REMOTE_API,  # near-zero LOCAL RSS: the VM does the work
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


def probe_capabilities(name: str) -> BackendCapabilities:
    """Probe the environment to see if a backend is available and what it supports."""
    spec = get_backend_spec(name)
    base = spec.capabilities

    if name == "grok-remote":
        return _probe_grok_remote()["capabilities"]

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

    return base


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


def _probe_grok_remote() -> dict[str, Any]:
    """Availability probe for grok-remote (implementation note S1). Never raises (fail-closed).

    Opt-in: no ``WORKBAY_REMOTE_GATE_HOST`` => ``declared_not_installed`` (a clean typed
    skip, mirroring copilot-host). With a host set, verify SSH reachability + that the VM
    grok CLI is present; any failure => ``unavailable``.

    SSH result branches (available / unavailable) are cached ~30s per host [RES-08] so
    repeated ``list_available_backends(probe=True)`` calls do not pay a network RTT each
    time. Unset-host and malformed-host early returns are never cached.
    """
    base = BACKENDS["grok-remote"].capabilities
    host = os.environ.get(_REMOTE_GATE_HOST_ENV, "").strip()
    if not host:
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_NOT_INSTALLED,
            "detail": _GROK_REMOTE_REMEDY,
        }
    # Fail-closed on a malformed host before it reaches ssh argv (SEC / RES-13):
    # a value that begins with '-' is parsed by ssh as an option (e.g.
    # ``-oProxyCommand=<cmd>`` => arbitrary LOCAL command execution, CWE-88), and
    # embedded whitespace/newlines can smuggle further tokens. The ``--`` separator
    # below is belt-and-suspenders for older ssh; this guard is the primary defense.
    if host.startswith("-") or any(ch.isspace() for ch in host):
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate host is malformed and was refused before probing (leading '-' or whitespace): {host!r}."
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
    try:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "--",
                host,
                'test -x "$HOME/.grok/bin/grok"',
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": f"Remote gate '{host}' unreachable: {exc}.",
        }
        _probe_grok_remote_cache[host] = (now + _PROBE_GROK_REMOTE_TTL_S, payload)
        return payload
    if result.returncode == 0:
        payload = {
            "capabilities": _availability_caps(base, is_available=True),
            "is_available": True,
            "state": AVAIL_AVAILABLE,
            "detail": f"Remote gate '{host}' reachable; VM grok CLI present.",
        }
        _probe_grok_remote_cache[host] = (now + _PROBE_GROK_REMOTE_TTL_S, payload)
        return payload
    payload = {
        "capabilities": _availability_caps(base, is_available=False),
        "is_available": False,
        "state": AVAIL_UNAVAILABLE,
        "detail": f"Remote gate '{host}' reachable check failed (VM grok CLI absent or SSH error).",
    }
    _probe_grok_remote_cache[host] = (now + _PROBE_GROK_REMOTE_TTL_S, payload)
    return payload


def probe_availability(name: str) -> dict[str, Any]:
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
    """
    spec = get_backend_spec(name)

    if name == "grok-remote":
        return _probe_grok_remote()

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
