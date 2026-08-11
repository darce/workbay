import importlib
import os
import subprocess
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

from workbay_protocol.remote_probe import probe_remote_gate, resolve_remote_gate_host

from workbay_orchestrator_mcp.orchestration.backend_adapter import BackendAdapter
from workbay_orchestrator_mcp.orchestration.codex_lane_config import DEFAULT_CODEX_MODEL
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
    # Whether this backend may be dispatched to the remote VM (off-box agent
    # execution). Distinct from cost_class: a backend can be COST_REMOTE_API
    # (local tests, remote inference) without being dispatchable_off_box.
    # Claude-family backends must never declare this True (internal).
    dispatchable_off_box: bool = False


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
    # Declared model-family identity for reviewer complementarity (internal).
    # Never derived by string-splitting the backend name — that mis-families
    # copilot-host / structured-turn / local-model-openai and reintroduces
    # name-keyed routing (REF-24). Group siblings that share a model vendor
    # (grok-remote+grok-cli, codex-cli+codex-subagent); keep grok/codex/claude
    # mutually distinct.
    model_family: str | None = None
    # Declared review preference (internal WIDTH-21). Lower ranks first
    # among equally-off-box candidates; never fall back to alphabetical name
    # order. implementation note S2 names the local codex gate as the cross-model fallback
    # for a grok implementer, so codex-cli outranks other local CLIs.
    # Default 100 is intentional for non-reviewer backends; reviewer-eligible
    # entries (cli + COST_REMOTE_API) must set an explicit rank (WIDTH-47).
    review_rank: int = 100
    # implementation note S3: local shaping/effort delegate for remote transports
    # (RemoteExecAdapter). A field on the row — never a backend-name check.
    shaping_adapter_path: str | None = None
    # Declared availability-probe strategy (implementation note repair S6-H05). A dotted
    # callable path on this module (or another), same shape as adapter_path /
    # shaping_adapter_path — never a backend-name branch in the dispatcher.
    # When set, probe_availability / probe_capabilities route through it and
    # replace only live is_available (via _availability_caps).
    availability_probe_path: str | None = None
    # Whether this backend may be selected as a verify-twin / reviewer.
    # Default True preserves existing row behaviour; set False on implement-only
    # transports that must not capture a model-family reviewer slot.
    review_eligible: bool = True
    # Allowed model pin for remote transports (implementation note repair S4-H02). The
    # transport refuses any other slug when this is set. None means no pin —
    # local / unpinned backends leave it unset so the registry stays the single
    # source of pin truth rather than a second name-keyed table in remote_exec.
    allowed_model: str | None = None
    # Optional env var name that configures ``allowed_model`` (surfaced in the
    # mismatch message so operators know what to set). None when the pin is a
    # compile-time constant rather than an env override.
    allowed_model_env: str | None = None
    # Seconds reserved on the local wall-clock for post-turn artifact fetch
    # (result.json + debug.log scp) after the remote agent self-terminates.
    # RemoteExecAdapter subtracts this from the local bound when the budget is
    # large enough; measure per backend — the default matches the grok
    # measurement so grok behaviour stays byte-identical (HARM-H05).
    post_turn_fetch_headroom_s: int = 15

    @property
    def adapter_class(self) -> type[BackendAdapter]:
        module_name, class_name = self.adapter_path.rsplit(".", 1)
        return getattr(importlib.import_module(module_name), class_name)


BACKENDS: dict[str, BackendSpec] = {
    "codex-cli": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.codex_cli.CodexCliAdapter",
        # Same shape as grok-cli/cursor-cli: inference runs off-box so local RSS
        # is small, but the lane's TEST_CMD still runs LOCALLY — hence
        # COST_REMOTE_API (thrash-gated on a small footprint), not COST_REMOTE.
        cost_class=COST_REMOTE_API,
        model_family="codex",
        # implementation note S2: preferred local cross-model gate for a grok implementer.
        review_rank=10,
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
        model_family="codex",
        # Bridge / COST_HEAVY: not eligible as a local reviewer; rank after CLIs.
        review_rank=50,
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
        model_family="copilot",
        review_rank=60,
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
        # Same shape as codex-cli/grok-cli/cursor-cli: inference runs off-box so
        # local RSS is small, but the lane's TEST_CMD still runs LOCALLY — hence
        # COST_REMOTE_API (thrash-gated on a small footprint), not COST_REMOTE.
        cost_class=COST_REMOTE_API,
        model_family="claude",
        review_rank=20,
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
        model_family="grok",
        review_rank=40,
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
        model_family="grok",
        # Off-box reviewers still win via the outer off-box sort key; rank is the
        # tie-break among off-box peers when more exist.
        review_rank=5,
        description=(
            f"Ship each grok turn to the remote OCI VM (WORKBAY_REMOTE_GATE_HOST); agent "
            f"execution + tests run off-box, the commit lands locally (xAI {DEFAULT_GROK_MODEL} worker)."
        ),
        shaping_adapter_path=(
            "workbay_orchestrator_mcp.orchestration.adapters.grok_cli.GrokCliAdapter"
        ),
        availability_probe_path=(
            "workbay_orchestrator_mcp.orchestration.backend_registry._probe_grok_remote"
        ),
        allowed_model=DEFAULT_GROK_MODEL,
        allowed_model_env="WORKBAY_GROK_MODEL",
        # Grok-measured post-turn fetch cost; keep 15 so remote_timeout arithmetic
        # stays byte-identical with the pre-HARM-H05 literal (threshold = 3×).
        post_turn_fetch_headroom_s=15,
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_reasoning_effort=True,
            supports_token_telemetry=False,  # grok emits no per-turn API token usage
            supports_sandbox=False,  # sandboxing is the VM's job, not a local shallow clone
            supports_token_budget_cycle_bounds=True,
            # Agent + tests run on the VM; the worker consumes the VM-captured
            # self-verify instead of a broken/redundant local re-run (item 26).
            runs_self_verify_off_box=True,
            # Declared off-box dispatch capability (internal); never
            # inferred from the backend name.
            dispatchable_off_box=True,
        ),
    ),
    # implementation note S3: complete codex-remote row + RemoteExecAdapter(backend_id=...).
    "codex-remote": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.remote_exec.RemoteExecAdapter",
        cost_class=COST_REMOTE,
        model_family="codex",
        # Off-box peer rank among remotes: after grok-remote (5), before
        # cursor-remote (7). Within the codex family, off-box sort key already
        # beats local codex-cli (rank 10); 6 is the inter-remote tie-break only.
        review_rank=6,
        # ADMISSION (FXGATE-FX08 / deliberate reviewer-admission review):
        # review_eligible=True. Evidence: (1) select_review_backends admits any
        # row with review_eligible + a model_family different from the
        # implementer, one rep per family, preferring off-box then review_rank
        # — codex-remote is the natural codex-family rep for non-codex
        # implementers, same shape as grok-remote for the grok family.
        # (2) Transport and probe blockers that originally justified False are
        # cleared: RemoteExecAdapter is backend_id-generalized; availability_
        # probe_path routes to _probe_codex_remote; supports_structured_output
        # is True (unlike cursor-remote, which stays False for that reason).
        # (3) Cold / unreachable VM: select_review_backends and
        # _emit_verify_twins never consult is_available — same as grok-remote —
        # so a cold probe does not shrink the compile-time reviewer set and
        # does not fail allocation; the twin is still minted and runtime
        # dispatch fails closed via the probe. That is graceful at selection
        # (no hard fail) and consistent with existing off-box behaviour.
        # Consequence: codex-cli no longer wins the codex family slot while
        # this row is eligible; off-box codex twins scale unboundedly per
        # implement lane. Rank left at 6 (see above).
        review_eligible=True,
        description=(
            "Ship a codex exec turn to the remote OCI VM via remote_agent.sh "
            "--agent-spec (implementation note)."
        ),
        shaping_adapter_path=(
            "workbay_orchestrator_mcp.orchestration.adapters.codex_cli.CodexCliAdapter"
        ),
        availability_probe_path=(
            "workbay_orchestrator_mcp.orchestration.backend_registry._probe_codex_remote"
        ),
        allowed_model=DEFAULT_CODEX_MODEL,
        post_turn_fetch_headroom_s=15,
        capabilities=BackendCapabilities(
            supports_structured_output=True,  # --output-schema, verified 0.145.0
            supports_reasoning_effort=True,  # -c model_reasoning_effort
            # R2-M11: do not flip True until all turn kinds carry usage.
            supports_token_telemetry=False,
            supports_sandbox=True,
            # No --max-turns in the codex recipe — refuse max_turns at spec build.
            supports_token_budget_cycle_bounds=False,
            supports_adapter_timeout_bounds=True,
            runs_self_verify_off_box=True,
            dispatchable_off_box=True,
        ),
    ),
    # implementation note S7: cursor-remote on OCI VM (distinct from local cursor-cli).
    "cursor-remote": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.remote_exec.RemoteExecAdapter",
        cost_class=COST_REMOTE,
        model_family="cursor",
        review_rank=7,
        # supports_structured_output=False — a reviewer must return a parseable
        # findings block; this transport cannot be an authoritative reviewer.
        review_eligible=False,
        description=(
            "Ship a cursor-agent turn to the remote OCI VM via remote_agent.sh "
            "--agent-spec (implementation note S7)."
        ),
        shaping_adapter_path=(
            "workbay_orchestrator_mcp.orchestration.adapters.cursor_cli.CursorCliAdapter"
        ),
        availability_probe_path=(
            "workbay_orchestrator_mcp.orchestration.backend_registry._probe_cursor_remote"
        ),
        allowed_model=DEFAULT_CURSOR_MODEL,
        allowed_model_env="WORKBAY_CURSOR_MODEL",
        post_turn_fetch_headroom_s=15,
        capabilities=BackendCapabilities(
            # Declared False like other COST_REMOTE rows; live flip via
            # _probe_cursor_remote (host + VM cursor-agent + authenticated env).
            # Operator smoke evidence lives in cursor_lane_config pins/comments.
            is_available=False,
            supports_structured_output=False,  # no schema flag — D9 harvest
            supports_reasoning_effort=True,  # encoded in model slug
            supports_token_telemetry=False,
            supports_sandbox=True,
            supports_token_budget_cycle_bounds=False,  # no turn cap → requires_timeout
            supports_adapter_timeout_bounds=True,
            runs_self_verify_off_box=True,
            dispatchable_off_box=True,
        ),
    ),
    "cursor-cli": BackendSpec(
        kind="cli",
        adapter_path="workbay_orchestrator_mcp.orchestration.adapters.cursor_cli.CursorCliAdapter",
        # Same shape as grok-cli: inference runs off-box so local RSS is small,
        # but the lane's TEST_CMD still runs LOCALLY — hence COST_REMOTE_API
        # (thrash-gated on a small footprint), not COST_REMOTE.
        cost_class=COST_REMOTE_API,
        model_family="cursor",
        review_rank=30,
        description=(
            f"Shell out to the Cursor CLI (cursor-agent) running {DEFAULT_CURSOR_MODEL}; "
            f"pin via WORKBAY_CURSOR_MODEL. Harness and model are separate axes here — "
            f"cursor takes the model as a parameter."
        ),
        capabilities=BackendCapabilities(
            # cursor-agent has NO --json-schema/--output-schema equivalent; the
            # adapter recovers the result from prose via extract_result_payload
            # . Declared false so nothing downstream assumes
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
        model_family="structured-turn",
        review_rank=70,
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
        model_family="local-model",
        review_rank=80,
        description="Generic OpenAI-compatible local model API.",
        capabilities=BackendCapabilities(
            supports_structured_output=True,
            supports_sandbox=True,
            supports_sync_turn=False,
            preflight_tokenizer_family="tiktoken",
        ),
    ),
}


# Sentinel default for BackendSpec.review_rank — reviewer-eligible backends
# must not inherit it (WIDTH-47). Keep in sync with the dataclass field default.
_DEFAULT_REVIEW_RANK = 100


def _is_reviewer_eligible_local(spec: BackendSpec) -> bool:
    """Same predicate as the compiler's local-reviewer eligibility (REF-24)."""
    return spec.kind == "cli" and spec.cost_class == COST_REMOTE_API


def _require_explicit_review_rank(name: str, spec: BackendSpec) -> None:
    """Refuse silent default rank for backends that participate in selection."""
    if _is_reviewer_eligible_local(spec) and int(spec.review_rank) == _DEFAULT_REVIEW_RANK:
        raise ValueError(
            f"backend {name!r} is reviewer-eligible (cli + COST_REMOTE_API) but "
            f"inherits review_rank={_DEFAULT_REVIEW_RANK}; declare an explicit rank"
        )


def get_backend_choices() -> tuple[str, ...]:
    return tuple(BACKENDS.keys())


def register_backend(name: str, spec: BackendSpec) -> None:
    _require_explicit_review_rank(name, spec)
    BACKENDS[name] = spec


# Static registry: same guard as register_backend so a future table edit cannot
# silently land a default-ranked local reviewer (WIDTH-47).
for _name, _spec in BACKENDS.items():
    _require_explicit_review_rank(_name, _spec)


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

    # For CLI, we might pass codex_bin/args. RemoteExecAdapter (implementation note S3)
    # takes backend_id so get_adapter("codex-remote") constructs the right row.
    import inspect  # noqa: PLC0415

    try:
        params = inspect.signature(cls.__init__).parameters
    except (TypeError, ValueError):
        params = {}
    if "backend_id" in params and "backend_id" not in kwargs:
        return cls(backend_id=name, **kwargs)  # type: ignore[call-arg]
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


def _resolve_availability_probe(path: str) -> Callable[..., dict[str, Any]]:
    """Load a declared availability probe from a dotted callable path.

    Same shape as :attr:`BackendSpec.adapter_path` / ``shaping_adapter_path``.
    Prefer this module's globals when the path targets the registry module so
    file-loaded tests and package imports share one declaration without forking
    on a backend-name branch.
    """
    module_name, attr = path.rsplit(".", 1)
    if module_name in (
        __name__,
        "workbay_orchestrator_mcp.orchestration.backend_registry",
    ):
        fn = globals().get(attr)
        if callable(fn):
            return fn  # type: ignore[return-value]
    return getattr(importlib.import_module(module_name), attr)


def _run_declared_availability_probe(
    spec: BackendSpec, *, workspace_root: Path | str | None = None
) -> dict[str, Any] | None:
    """Invoke :attr:`BackendSpec.availability_probe_path` when declared."""
    path = spec.availability_probe_path
    if not path:
        return None
    probe_fn = _resolve_availability_probe(path)
    return probe_fn(workspace_root=workspace_root)


def probe_capabilities(name: str, *, workspace_root: Path | str | None = None) -> BackendCapabilities:
    """Probe the environment to see if a backend is available and what it supports."""
    spec = get_backend_spec(name)
    base = spec.capabilities

    # Declared probe strategy (S6-H05) — never a backend-name branch.
    declared = _run_declared_availability_probe(spec, workspace_root=workspace_root)
    if declared is not None:
        return declared["capabilities"]

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
_CURSOR_REMOTE_REMEDY = (
    "cursor-remote requires WORKBAY_REMOTE_GATE_HOST and a provisioned OCI VM with "
    "cursor-agent plus a readable ~/.config/cursor-agent/env carrying a non-empty "
    "CURSOR_API_KEY (authenticated readiness, not mere file presence). "
    "See docs/runbooks/remote-gate-provisioning.md (Cursor section) and "
    "scripts/provision_cursor_remote_auth.sh."
)
_CODEX_REMOTE_REMEDY = (
    "codex-remote requires WORKBAY_REMOTE_GATE_HOST and a provisioned OCI VM with "
    "the pinned codex binary (see codex_lane_config.CODEX_REMOTE_BIN / "
    "CODEX_CLI_VERSION). See docs/runbooks/remote-gate-provisioning.md."
)
# Remote cursor install vs auth are distinct machine-readable outcomes (S6-M01).
# Exit codes from the remote shell script:
#   0  — binary present + env readable + non-empty CURSOR_API_KEY + headless status ok
#   10 — binary missing / not executable
#   11 — env file missing or unreadable
#   12 — env present but no valid CURSOR_API_KEY assignment
#   13 — key present but headless status reports unauthenticated
#   14 — installed but auth could not be verified (status ambiguous)
# Never treat mere env-file presence as available.
_REMOTE_CURSOR_AUTH_PROBE = r"""
set +e
BIN="$HOME/.local/bin/cursor-agent"
ENVF="$HOME/.config/cursor-agent/env"
if ! test -x "$BIN"; then
  echo CURSOR_INSTALL_MISSING
  exit 10
fi
if ! test -f "$ENVF" || ! test -r "$ENVF"; then
  echo CURSOR_AUTH_MISSING
  exit 11
fi
if ! grep -Eq '^[[:space:]]*CURSOR_API_KEY=[^#[:space:]=][^[:space:]]*' "$ENVF"; then
  echo CURSOR_AUTH_INVALID
  exit 12
fi
set -a
# shellcheck disable=SC1090
. "$ENVF"
set +a
STATUS_OUT=$("$BIN" status 2>&1)
STATUS_RC=$?
COMBINED=$(printf '%s' "$STATUS_OUT")
case "$COMBINED" in
  *"Not logged in"*|*"Authentication required"*)
    echo CURSOR_AUTH_FAILED
    exit 13
    ;;
esac
if [ "$STATUS_RC" -ne 0 ]; then
  echo CURSOR_AUTH_UNVERIFIED
  exit 14
fi
echo CURSOR_AUTH_OK
exit 0
"""
# Machine-readable install/auth states for cursor-remote probe payloads.
CURSOR_REMOTE_INSTALL_INSTALLED = "installed"
CURSOR_REMOTE_INSTALL_MISSING = "not_installed"
CURSOR_REMOTE_AUTH_AUTHENTICATED = "authenticated"
CURSOR_REMOTE_AUTH_MISSING = "missing"
CURSOR_REMOTE_AUTH_INVALID = "invalid"
CURSOR_REMOTE_AUTH_FAILED = "unauthenticated"
CURSOR_REMOTE_AUTH_UNVERIFIED = "unverified"
CURSOR_REMOTE_AUTH_NA = "not_applicable"


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


_PROBE_CURSOR_REMOTE_TTL_S = 30.0
_probe_cursor_remote_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_PROBE_CODEX_REMOTE_TTL_S = 30.0
_probe_codex_remote_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _resolve_remote_gate_host_for_probe(
    *, workspace_root: Path | str | None = None
) -> str:
    """Shared host resolution for remote availability probes (env then repo file)."""
    host = (resolve_remote_gate_host(None) or "").strip()
    if not host:
        repo_root = _resolve_remote_probe_repo_root(workspace_root)
        host = (resolve_remote_gate_host(repo_root) or "").strip()
    return host


def _probe_cursor_remote(*, workspace_root: Path | str | None = None) -> dict[str, Any]:
    """Availability probe for cursor-remote (implementation note S7 / S6-M01). Never raises.

    Opt-in host like grok-remote. Installation and authenticated readiness are
    distinct machine-readable states; advertised availability depends only on
    authenticated readiness (bounded headless ``cursor-agent status`` after a
    readable env file with a non-empty CURSOR_API_KEY). Mere env-file presence
    never becomes ``is_available=True``. The authenticated result is TTL-cached.
    Declared BACKENDS.is_available stays False.
    """
    base = BACKENDS["cursor-remote"].capabilities
    host = _resolve_remote_gate_host_for_probe(workspace_root=workspace_root)
    if not host:
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_NOT_INSTALLED,
            "detail": _CURSOR_REMOTE_REMEDY,
            "install_state": CURSOR_REMOTE_INSTALL_MISSING,
            "auth_state": CURSOR_REMOTE_AUTH_NA,
        }
    if host.startswith("-") or any(ch.isspace() for ch in host):
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate host is malformed and was refused before probing "
                f"(leading '-' or whitespace): {host!r}."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_MISSING,
            "auth_state": CURSOR_REMOTE_AUTH_NA,
        }
    now = time.monotonic()
    cached = _probe_cursor_remote_cache.get(host)
    if cached is not None:
        expires_at, payload = cached
        if now < expires_at:
            return payload
        del _probe_cursor_remote_cache[host]
    try:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "--",
                host,
                _REMOTE_CURSOR_AUTH_PROBE,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": f"Remote gate '{host}' unreachable for cursor-remote: {exc}.",
            "install_state": CURSOR_REMOTE_INSTALL_MISSING,
            "auth_state": CURSOR_REMOTE_AUTH_NA,
        }
        _probe_cursor_remote_cache[host] = (now + _PROBE_CURSOR_REMOTE_TTL_S, payload)
        return payload

    rc = result.returncode
    if rc == 0:
        payload = {
            "capabilities": _availability_caps(base, is_available=True),
            "is_available": True,
            "state": AVAIL_AVAILABLE,
            "detail": (
                f"Remote gate '{host}' reachable; VM cursor-agent installed and "
                "authenticated (env key present; headless status ok)."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_INSTALLED,
            "auth_state": CURSOR_REMOTE_AUTH_AUTHENTICATED,
        }
    elif rc == 10:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' reachable but cursor-agent binary missing "
                "at ~/.local/bin/cursor-agent."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_MISSING,
            "auth_state": CURSOR_REMOTE_AUTH_NA,
        }
    elif rc == 11:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' has cursor-agent installed but "
                "~/.config/cursor-agent/env is missing or unreadable. "
                "See scripts/provision_cursor_remote_auth.sh."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_INSTALLED,
            "auth_state": CURSOR_REMOTE_AUTH_MISSING,
        }
    elif rc == 12:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' has cursor-agent installed but "
                "~/.config/cursor-agent/env has no valid non-empty CURSOR_API_KEY "
                "assignment (partial write or empty key)."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_INSTALLED,
            "auth_state": CURSOR_REMOTE_AUTH_INVALID,
        }
    elif rc == 13:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' has cursor-agent + env key present but "
                "headless status reports unauthenticated (revoked key or login "
                "required)."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_INSTALLED,
            "auth_state": CURSOR_REMOTE_AUTH_FAILED,
        }
    else:
        # rc == 14 or any unexpected: installed but auth not verified — refuse
        # to advertise availability (never convert file presence into available).
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' has cursor-agent installed but authenticated "
                "readiness could not be verified (installed-but-unverified; "
                "selection refuses availability)."
            ),
            "install_state": CURSOR_REMOTE_INSTALL_INSTALLED,
            "auth_state": CURSOR_REMOTE_AUTH_UNVERIFIED,
        }
    _probe_cursor_remote_cache[host] = (now + _PROBE_CURSOR_REMOTE_TTL_S, payload)
    return payload


def _parse_codex_cli_version(text: str) -> str | None:
    """Extract a dotted version (e.g. 0.145.0) from ``codex --version`` output."""
    import re  # noqa: PLC0415

    match = re.search(r"(\d+\.\d+\.\d+(?:[.-][0-9A-Za-z.]+)?)", text or "")
    return match.group(1) if match else None


def _codex_version_in_supported_range(remote_version: str, pinned: str) -> bool:
    """Fail-closed version gate: only the measured pin is supported until remeasured.

    The supported range is the single pinned snapshot from
    :data:`codex_lane_config.CODEX_CLI_VERSION`. Sandbox/auth fixture strings are
    measured against that binary; any drift reports unavailable.
    """
    return remote_version == pinned


def _probe_codex_remote(*, workspace_root: Path | str | None = None) -> dict[str, Any]:
    """Availability probe for codex-remote (implementation note S4-H01 / S1-M03). Never raises.

    Opt-in host like cursor-remote. SSH checks the pinned absolute binary
    (:data:`codex_lane_config.CODEX_REMOTE_BIN`), runs ``--version``, and
    fail-closes when the remote version drifts from the pin. Preserves declared
    capabilities via :func:`_availability_caps`.
    """
    # Import inside the probe so codex_lane_config stays the single owner of the
    # version literal (lane 07) — never duplicate the pin into this module.
    from workbay_orchestrator_mcp.orchestration.codex_lane_config import (  # noqa: PLC0415
        CODEX_CLI_VERSION,
        CODEX_REMOTE_BIN,
    )

    base = BACKENDS["codex-remote"].capabilities
    host = _resolve_remote_gate_host_for_probe(workspace_root=workspace_root)
    if not host:
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_NOT_INSTALLED,
            "detail": _CODEX_REMOTE_REMEDY,
        }
    if host.startswith("-") or any(ch.isspace() for ch in host):
        return {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate host is malformed and was refused before probing "
                f"(leading '-' or whitespace): {host!r}."
            ),
        }
    now = time.monotonic()
    cached = _probe_codex_remote_cache.get(host)
    if cached is not None:
        expires_at, payload = cached
        if now < expires_at:
            return payload
        del _probe_codex_remote_cache[host]

    # Bounded remote check: binary exists + print --version. Fail-closed.
    remote_cmd = (
        f'bin={CODEX_REMOTE_BIN}; '
        f'if ! test -x "$bin"; then echo CODEX_MISSING; exit 10; fi; '
        f'ver=$("$bin" --version 2>&1); echo "CODEX_VERSION:$ver"; exit 0'
    )
    try:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "--",
                host,
                remote_cmd,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": f"Remote gate '{host}' unreachable for codex-remote: {exc}.",
        }
        _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
        return payload

    combined = f"{result.stdout or ''}{result.stderr or ''}"
    if result.returncode == 10 or "CODEX_MISSING" in combined:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' reachable but pinned codex binary "
                f"missing at {CODEX_REMOTE_BIN}."
            ),
        }
        _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
        return payload
    if result.returncode != 0:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' reachable but codex-remote probe failed "
                f"(exit {result.returncode})."
            ),
        }
        _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
        return payload

    remote_version = _parse_codex_cli_version(combined)
    if remote_version is None:
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"Remote gate '{host}' codex --version output unparseable "
                f"({combined.strip()!r}); fail-closed until remeasured."
            ),
        }
        _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
        return payload
    if not _codex_version_in_supported_range(remote_version, CODEX_CLI_VERSION):
        payload = {
            "capabilities": _availability_caps(base, is_available=False),
            "is_available": False,
            "state": AVAIL_UNAVAILABLE,
            "detail": (
                f"codex-remote version drift: remote reports {remote_version}, "
                f"supported pin is {CODEX_CLI_VERSION}; fail-closed until "
                "sandbox/auth fixtures are remeasured."
            ),
        }
        _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
        return payload

    payload = {
        "capabilities": _availability_caps(base, is_available=True),
        "is_available": True,
        "state": AVAIL_AVAILABLE,
        "detail": (
            f"Remote gate '{host}' reachable; VM codex at {CODEX_REMOTE_BIN} "
            f"reports version {remote_version} (matches pin)."
        ),
    }
    _probe_codex_remote_cache[host] = (now + _PROBE_CODEX_REMOTE_TTL_S, payload)
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

    ``workspace_root`` is threaded to declared remote probes (bra3) so the
    config-file host fallback resolves against the consumer repo. Remote probe
    selection is driven by :attr:`BackendSpec.availability_probe_path` (S6-H05),
    never by a backend-name branch.
    """
    spec = get_backend_spec(name)

    # Declared probe strategy (S6-H05) — never a backend-name branch.
    declared = _run_declared_availability_probe(spec, workspace_root=workspace_root)
    if declared is not None:
        return declared

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
