import importlib
import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable

from workbay_orchestrator_mcp.orchestration.backend_adapter import BackendAdapter
from workbay_orchestrator_mcp.orchestration.grok_lane_config import DEFAULT_GROK_MODEL


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


@dataclass(frozen=True)
class BackendSpec:
    kind: str
    adapter_path: str
    description: str
    module: str | None = None
    capabilities: BackendCapabilities = field(default_factory=BackendCapabilities)

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

            return BackendCapabilities(
                is_available=True,
                supports_structured_output=base.supports_structured_output,
                supports_sandbox=base.supports_sandbox,
                supports_sync_turn=base.supports_sync_turn,
                supports_reasoning_effort=has_reasoning,
            )
        except (RuntimeError, subprocess.TimeoutExpired):
            return BackendCapabilities(is_available=False)

    if name == "codex-subagent" or name == "copilot-host":
        try:
            resolve_bridge(name)
            return BackendCapabilities(
                is_available=True,
                supports_structured_output=base.supports_structured_output,
                supports_sandbox=base.supports_sandbox,
                supports_sync_turn=base.supports_sync_turn,
                # Carry the declared reasoning-effort flag so a direct
                # probe_capabilities("codex-subagent") is truthful; the bridge
                # path already supports effort (BACKENDS declares it True).
                supports_reasoning_effort=base.supports_reasoning_effort,
            )
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
                return BackendCapabilities(
                    is_available=True,
                    supports_structured_output=base.supports_structured_output,
                    supports_sandbox=base.supports_sandbox,
                    supports_sync_turn=base.supports_sync_turn,
                )
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
                return BackendCapabilities(
                    is_available=True,
                    supports_structured_output=base.supports_structured_output,
                    supports_sandbox=base.supports_sandbox,
                    supports_sync_turn=base.supports_sync_turn,
                    # grok declares reasoning-effort in its spec (no --help probe).
                    supports_reasoning_effort=True,
                )
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
    return BackendCapabilities(
        is_available=is_available,
        supports_structured_output=base.supports_structured_output,
        supports_sandbox=base.supports_sandbox,
        supports_sync_turn=base.supports_sync_turn,
        supports_reasoning_effort=base.supports_reasoning_effort,
        preflight_tokenizer_family=base.preflight_tokenizer_family,
    )


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
