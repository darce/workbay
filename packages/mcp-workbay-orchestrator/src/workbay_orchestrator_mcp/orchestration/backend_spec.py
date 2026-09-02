"""Invocation-spec builder for remote backends (implementation note S2).

``build_agent_spec`` / ``write_agent_spec`` turn a backend id + operator knobs
into a v2 AgentSpec (JSON + NUL-delimited argv). ``remote_agent.sh --agent-spec``
executes the argv blindly ([REF-26], [SEC-04]).

``binary`` is the executable; ``argv`` is the argument vector only (no argv[0]
duplicate of binary).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration import (
    codex_lane_config,
    cursor_lane_config,
    openrouter_lane_config,
)
from workbay_orchestrator_mcp.orchestration.backend_registry import (
    BACKENDS,
    auth_port,
    raise_if_retired_backend,
)
from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
    AUTH_MATCH_PATTERNS,
    AUTH_MATCH_STREAMS,
    CODEX_MODEL_ALLOWED_SERVICE_TIERS,
    CODEX_PATH_PREPEND,
    CODEX_SPEED_TO_SERVICE_TIER,
    DEFAULT_CODEX_MODEL,
    LANE_SANDBOX,
    LANE_WRITABLE_ROOTS,
    SERVICE_TIER_WARNING_PATTERNS,
    WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT,
    WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT,
)
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
    CURSOR_PATH_PREPEND,
    resolve_cursor_model,
)
from workbay_orchestrator_mcp.orchestration.env_file_path import (  # noqa: F401 — re-exported
    _ENV_FILE_RE,
    _validate_env_file,
)
from workbay_orchestrator_mcp.orchestration.grok_lane_config import (
    GROK_MAX_TURNS_CAP,
    retired_model_warning,
)
from workbay_orchestrator_mcp.orchestration.openrouter_lane_config import (
    DEFAULT_OPENROUTER_MODEL,
    OPENROUTER_ALLOWED_MODELS,
    OPENROUTER_BASE_URL,
    OPENROUTER_MODEL_ALLOWED_EFFORTS,
    OPENROUTER_PROVIDER_ID,
    OPENROUTER_PROVIDER_NAME,
    OPENROUTER_WIRE_API,
    WORKBAY_OPENROUTER_API_KEY_ENV,
)

_PLACEHOLDER_TOKEN = re.compile(r"\{[^{}]+\}")

# Single source for remote effort allow-list (was adapters/remote_exec.py).
REMOTE_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})


def default_effort_for_model(backend_id: str, model: str) -> str | None:
    """Return the remote default for an unset reasoning effort.

    OpenRouter models get ``high`` only when they advertise it. Otherwise argv
    omits the override rather than silently selecting another advertised tier.
    """
    if backend_id == "openrouter-remote":
        model = model or _default_model_for(backend_id)
        advertised_efforts = OPENROUTER_MODEL_ALLOWED_EFFORTS.get(model)
        if advertised_efforts and "high" in advertised_efforts:
            return "high"
        return None
    return "high"


# Cursor headless credential file (implementation note D6). Spec data only — bash never
# hardcodes this path; remote_agent sources AgentSpec.env_file when present.
# implementation note S1: the registry's AuthPort is the single literal; this name is a
# re-export kept for existing callers, not a second copy of the path.
CURSOR_ENV_FILE: str = auth_port("cursor-remote").require_env_file()

# _ENV_FILE_RE / _validate_env_file live in env_file_path (implementation note S1,
# DATA-14) so backend_registry.AuthPort validates with the identical regex.

# Backends with an invocation recipe in this module (S2–S7; 0208 S4 adds openrouter).
_INVOCATION_BACKENDS = frozenset({"grok-remote", "codex-remote", "cursor-remote", "openrouter-remote"})

# implementation note S4: auth-failure patterns for openrouter-remote. remote_agent.sh
# matches these as BARE SUBSTRINGS over stdout+stderr with no rc gate, so every
# pattern must be a full diagnostic line, never a bare number or identifier
# (S4-H-01: "401" matched `"input_tokens":4401`; the bare env-var name matched
# any lane that grepped this repo). The codex "Model metadata for model `…`
# not found" line is deliberately NOT here — it is printed on every non-OpenAI
# slug (VERIFY-1) and is not a credential condition.
OPENROUTER_AUTH_MATCH_PATTERNS: tuple[str, ...] = (
    "Missing environment variable: `WORKBAY_OPENROUTER_API_KEY`",
    "No auth credentials",
)
# OpenRouter model-not-served signal: classified separately from auth so a
# retired/renamed stealth slug never reads as a credential problem. Same
# substring discipline: no bare "404", no metadata-noise line (S4-M-01).
#
# Live-observed NOISE (2026-08-23, authenticated, successful turns) that S5
# must NOT reclassify as auth or model-unavailable:
#   (a) JSONL item of type "error" on EVERY successful turn:
#       {"type":"item.completed","item":{"type":"error","message":"Model
#        metadata for `stealth/ox-alpha` not found. Defaulting to fallback
#        metadata…"}}
#       (quoted slug is the retired stealth pin, kept verbatim as a dated
#       2026-08-23 observation)
#   (b) stderr: `ERROR codex_models_manager::manager: failed to refresh
#       available models: … missing field `models`` — codex probing
#       OpenRouter's /models endpoint; harmless.
# OPENROUTER_LIVE_NOISE_LINES keeps them as negative fixtures for the pattern tests.
OPENROUTER_MODEL_UNAVAILABLE_PATTERNS: tuple[str, ...] = ("No endpoints found",)
# Same S4-H-01 substring discipline as OPENROUTER_AUTH_MATCH_PATTERNS above: every
# pattern here must be a full diagnostic line, never a bare identifier or bare
# phrase, because remote_agent.sh's rate-limit arm is also a bare-substring
# scan. The identifier "rate_limit_exceeded" and the phrase "Rate limit
# exceeded" were removed (canon AGT-10) after a lane that merely mentioned
# either string self-triggered a false rate_limited classification.
RATE_LIMIT_MATCH_PATTERNS: tuple[str, ...] = (
    "429 Too Many Requests",
    "Too Many Requests",
    "last status: 429",
)
OPENROUTER_LIVE_NOISE_LINES: tuple[str, ...] = (
    # Quoted slug is the retired stealth pin, kept verbatim as a dated 2026-08-23 observation.
    '{"type":"item.completed","item":{"type":"error","message":"Model metadata for '
    '`stealth/ox-alpha` not found. Defaulting to fallback metadata…"}}',
    "ERROR codex_models_manager::manager: failed to refresh available models: "
    "error decoding response body: missing field `models`",
)

# Canonical flock column defaults when backend/effort cells are empty.
# Sole registry for offload_flock.sh (via the `defaults` CLI); bash never embeds these.
FLOCK_DEFAULTS: dict[str, str] = {
    "backend": "grok-remote",
    "effort": "high",
}


def invocation_backend_ids() -> frozenset[str]:
    """Ids with a ``build_agent_spec`` recipe (D3 assertion 4 bijection)."""
    return _INVOCATION_BACKENDS


class UnknownBackendError(KeyError):
    """Raised when ``backend_id`` is missing from BACKENDS or has no recipe."""


class UnenforceableBoundError(ValueError):
    """Caller asked for a bound the backend cannot enforce."""


class PlaceholderInOperatorValueError(ValueError):
    """Manifest-sourced value contains a ``{...}`` token ([R2-H09])."""


class NulInSpecValueError(ValueError):
    """Value contains NUL, which collides with the ``.argv`` sidecar delimiter."""


class RetiredModelError(ValueError):
    """Caller asked to dispatch a vendor-retired model slug."""


class EmptyRecipePlaceholderSetError(ValueError):
    """Host recipe argv has no brace-bearing elements.

    An empty required set is not compliance: it is indistinguishable from a
    detector that failed to extract what the recipe needs.
    """

    kind = "recipe_placeholder_set_empty"


# Tokens this module emits into argv/stdio fields (executor whole-element sub).
_KNOWN_PLACEHOLDERS: frozenset[str] = frozenset(
    {
        "{brief_file}",
        "{brief_inline}",
        "{schema_file}",
        "{schema_inline}",
        "{result_file}",
        "{stream_file}",
        "{run_log}",
        "{debug_file}",
    }
)

_RESOLVER_FN_NAME = "_agent_spec_resolve_argv"
# Whole-element '{token_name}' — used to distinguish a known arm from an
# embedded or stray brace. The detector itself is broader (any brace).
_WHOLE_ELEMENT_TOKEN = re.compile(r"^\{[A-Za-z_][A-Za-z0-9_]*\}$")
_RESOLVER_CASE_ARM = re.compile(r"'(\{[A-Za-z_][A-Za-z0-9_]*\})'")


def argv_placeholder_tokens(argv: Sequence[str]) -> frozenset[str]:
    """Brace-bearing argv elements — same fire condition as ``remote_agent.sh``.

    The runtime residual arm (``*'{'*|*'}'*``) refuses any element that
    contains a brace character anywhere. A whole-element ``{token}`` is
    returned as-is and can match a resolver arm; an embedded or stray brace
    is returned as the full element, which no arm matches, so preflight
    refuses the same input the VM would exit 7 on.
    """
    return frozenset(el for el in argv if "{" in el or "}" in el)


def require_recipe_placeholder_set(argv: Sequence[str]) -> frozenset[str]:
    """Return brace-bearing argv elements, or refuse an empty probe.

    An empty result must not read as "this recipe needs nothing".
    """
    required = argv_placeholder_tokens(argv)
    if not required:
        raise EmptyRecipePlaceholderSetError(
            "host recipe required-placeholder set is empty; "
            "refusing to treat an empty probe as compliance "
            "(parse failure and 'needs nothing' are indistinguishable)"
        )
    return required


def collect_resolver_placeholder_arms(script_text: str) -> frozenset[str]:
    """Parse ``'{token}'`` case arms from ``_agent_spec_resolve_argv``.

    Empty when the function is absent. Callers distinguish a missing file from
    a readable script that simply has no matching arms.
    """
    body = _extract_bash_function(script_text, _RESOLVER_FN_NAME)
    if not body:
        return frozenset()
    return frozenset(_RESOLVER_CASE_ARM.findall(body))


def format_placeholder_tokens(tokens: Iterable[str]) -> str:
    ordered = ",".join(sorted(tokens))
    return ordered if ordered else "(none)"


def _extract_bash_function(script_text: str, name: str) -> str:
    lines = script_text.splitlines()
    start: int | None = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(rf"^{re.escape(name)}\s*\(\)\s*\{{", line):
            start = i
            break
        if stripped.startswith(f"{name}()") and "{" in stripped:
            start = i
            break
    if start is None:
        return ""
    depth = 0
    started = False
    block: list[str] = []
    for line in lines[start:]:
        block.append(line)
        for ch in line:
            if ch == "{":
                depth += 1
                started = True
            elif ch == "}":
                depth -= 1
        if started and depth == 0:
            break
    return "\n".join(block)


@dataclass(frozen=True)
class AgentSpec:
    spec_version: int
    backend_id: str
    binary: str
    path_prepend: list[str]
    env: dict[str, str]
    argv: list[str]
    operator_values: list[str]
    stdin: str
    stdout: str
    stderr: str
    close_fds: list[int]
    requires_timeout: bool
    lane_timeout_s: int
    result_source: str
    agent_exit_to_status: dict[str, str]
    auth_match: dict[str, Any] = field(default_factory=dict)
    rate_limit_match: dict[str, Any] = field(default_factory=dict)
    service_tier_warning_match: dict[str, Any] = field(default_factory=dict)
    # When True, dispatch must re-probe sandbox/userns before trusting LANE_SANDBOX.
    requires_live_sandbox_preflight: bool = False
    # Optional remote-side env file (e.g. CURSOR_ENV_FILE). Sourced only inside
    # the agent launch subshell so credentials never leak into the D9 classifier.
    env_file: str | None = None
    # implementation note S4: model-not-served signals (distinct from auth_match). Data
    # only; the VM classifier ignores unknown keys and reports agent_failed.
    model_unavailable_match: dict[str, Any] = field(default_factory=dict)
    # Host-side only (implementation note S5): reason a requested effort was not applied.
    # Stripped from to_json_dict — not part of the remote AgentSpec wire format.
    effort_downgrade_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_env_file(self.env_file)

    def to_json_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("effort_downgrade_reason", None)
        return d


def _refuse_placeholder_in_operator(value: str, *, field_name: str) -> None:
    if _PLACEHOLDER_TOKEN.search(value):
        raise PlaceholderInOperatorValueError(f"operator value {field_name}={value!r} contains a placeholder token")


def _refuse_nul(value: str, *, field_name: str) -> None:
    if "\0" in value:
        raise NulInSpecValueError(
            f"operator value {field_name}={value!r} contains a NUL character (collides with .argv sidecar delimiter)"
        )


def _refuse_placeholder_element(value: str, *, field_name: str) -> None:
    """Refuse when value is exactly a known recipe placeholder (whole-element)."""
    if value.strip() in _KNOWN_PLACEHOLDERS:
        raise PlaceholderInOperatorValueError(f"operator value {field_name}={value!r} is exactly a recipe placeholder")


def _grok_remote_argv(*, model: str, effort: str, max_turns: int) -> list[str]:
    # Argv only — binary is AgentSpec.binary. Mirrors remote_agent.sh L1127–1130.
    return [
        "--prompt-file",
        "{brief_file}",
        "--cwd",
        ".",
        "-m",
        model,
        "--json-schema",
        "{schema_inline}",
        "--max-turns",
        str(max_turns),
        "--always-approve",
        "--no-plan",
        "--no-subagents",
        "--reasoning-effort",
        effort,
        "--debug-file",
        "{debug_file}",
    ]


def _openrouter_remote_argv(*, model: str, effort: str | None) -> list[str]:
    """codex argv plus the five ``-c`` provider overrides (VERIFY-1 shape).

    Nested ``model_providers.<id>.<key>=`` overrides are accepted by codex
    0.145 and the key is read from the VM environment via ``env_key`` — the
    secret is never on argv.
    """
    return _codex_remote_argv(model=model, effort=effort) + [
        "-c",
        f"model_provider={OPENROUTER_PROVIDER_ID}",
        "-c",
        f"model_providers.{OPENROUTER_PROVIDER_ID}.name={OPENROUTER_PROVIDER_NAME}",
        "-c",
        f"model_providers.{OPENROUTER_PROVIDER_ID}.base_url={OPENROUTER_BASE_URL}",
        "-c",
        f"model_providers.{OPENROUTER_PROVIDER_ID}.env_key={WORKBAY_OPENROUTER_API_KEY_ENV}",
        "-c",
        f"model_providers.{OPENROUTER_PROVIDER_ID}.wire_api={OPENROUTER_WIRE_API}",
    ]


def _codex_remote_argv(
    *,
    model: str,
    effort: str | None,
    service_tier: str | None = None,
) -> list[str]:
    # Argv only — binary is AgentSpec.binary ('codex'). implementation note D2 row.
    #
    # writable_roots re-allows .git under the SAME workspace-write policy so the
    # lane can commit; without it the commit dies read-only and turn.patch comes
    # back empty. Entries are relative to -C, and the `-c` value parses as TOML,
    # which json.dumps emits correctly for a list of plain strings. This is a
    # narrow re-allow, never danger-full-access ([SEC-04]).
    argv = [
        "exec",
        "--json",
        "--ignore-user-config",
        "-C",
        ".",
        "-m",
        model,
    ]
    if effort is not None:
        argv.extend(("-c", f"model_reasoning_effort={effort}"))
    if service_tier is not None:
        advertised_tiers = CODEX_MODEL_ALLOWED_SERVICE_TIERS.get(model, frozenset())
        if service_tier not in advertised_tiers:
            raise ValueError(
                f"Refusing codex-remote dispatch with service tier {service_tier!r}: "
                f"{model!r} advertises {sorted(advertised_tiers)} "
                "(codex_lane_config.CODEX_MODEL_ALLOWED_SERVICE_TIERS)"
            )
        argv.extend(("-c", f"service_tier={service_tier}"))
    argv.extend(
        (
            "-c",
            f"sandbox_workspace_write.writable_roots={json.dumps(list(LANE_WRITABLE_ROOTS))}",
            "-s",
            LANE_SANDBOX,
            "--output-schema",
            "{schema_file}",
            "-o",
            "{result_file}",
            "-",
        )
    )
    return argv


def _cursor_remote_argv(*, model: str) -> list[str]:
    # Argv only — binary is AgentSpec.binary ('cursor-agent'). The brief is
    # staged out-of-band (--brief → .brief.md); recipe carries {brief_inline}
    # so the remote resolver substitutes file contents after the known-
    # placeholder match. Operator free text never transits the brace scan.
    # cursor-agent has no --prompt-file; path-as-positional delivers the path
    # string as the user message (measured), not the file body.
    return [
        "--print",
        "--output-format",
        "json",
        "--workspace",
        ".",
        "--force",
        "--trust",
        "--model",
        model,
        "--",
        "{brief_inline}",
    ]


def _seal_agent_spec(spec: AgentSpec) -> AgentSpec:
    """Argv-construction chokepoint: refuse an empty or unprobeable recipe set."""
    require_recipe_placeholder_set(spec.argv)
    return spec


def _cursor_remote_default_timeout_s() -> int:
    return int(cursor_lane_config.CURSOR_REMOTE_LANE_TIMEOUT_S)


def _cursor_remote_timeout_ceiling_s() -> int:
    return int(cursor_lane_config.offload_timeout_ssot.resolve_cursor_timeout_cap())


def _codex_remote_default_timeout_s() -> int:
    return int(codex_lane_config.LANE_TIMEOUT_S)


def _codex_remote_timeout_ceiling_s() -> int:
    return int(codex_lane_config.CODEX_TIMEOUT_CAP)


def _openrouter_remote_default_timeout_s() -> int:
    return int(openrouter_lane_config.OPENROUTER_REMOTE_LANE_TIMEOUT_S)


def _openrouter_remote_timeout_ceiling_s() -> int:
    return int(openrouter_lane_config.OPENROUTER_TIMEOUT_CAP)


# Named owners only. A new adapter-timeout remote must be added here; it must
# not inherit another backend's bound by falling through.
_ADAPTER_TIMEOUT_BACKEND_BOUNDS = {
    "cursor-cli": (None, _cursor_remote_timeout_ceiling_s),
    "cursor-remote": (_cursor_remote_default_timeout_s, _cursor_remote_timeout_ceiling_s),
    "codex-remote": (_codex_remote_default_timeout_s, _codex_remote_timeout_ceiling_s),
    "openrouter-remote": (_openrouter_remote_default_timeout_s, _openrouter_remote_timeout_ceiling_s),
}


def default_remote_lane_timeout_s(backend_id: str) -> int:
    """This backend's own declared default wall-clock bound, in seconds.

    One owner for how many wall-clock seconds a timeout-requiring remote
    lane gets when no caller timeout is supplied (REF-26, REF-09). This is
    a default bound, not a floor, and it is capped by the same backend ceiling
    used for budget-derived adapter timeouts.

    Known adapter-timeout remotes are named in
    ``_ADAPTER_TIMEOUT_BACKEND_BOUNDS``. Unknown ids, turn-bounded
    backends, and local siblings raise rather than inherit another
    backend's pin.
    """
    return resolve_remote_lane_timeout_s(backend_id)


def remote_lane_timeout_ceiling_s(backend_id: str) -> int:
    """Return the backend's current adapter-timeout ceiling."""
    owners = _ADAPTER_TIMEOUT_BACKEND_BOUNDS.get(backend_id)
    if owners is None:
        raise ValueError(f"remote_lane_timeout_ceiling_s has no declared bound for {backend_id!r}")
    return int(owners[1]())


def resolve_remote_lane_timeout_s(backend_id: str, requested: int | None = None) -> int:
    """Resolve default and derived adapter timeouts through one chokepoint."""
    if requested is None:
        owners = _ADAPTER_TIMEOUT_BACKEND_BOUNDS.get(backend_id)
        if owners is None or owners[0] is None:
            raise ValueError(f"default_remote_lane_timeout_s has no declared bound for {backend_id!r}")
        return min(int(owners[0]()), int(owners[1]()))
    return min(int(requested), remote_lane_timeout_ceiling_s(backend_id))


def normalize_operator_speed(speed: str | None) -> str | None:
    """Return a canonical speed or refuse spelling dispatch would not accept.

    Empty means unset for both preflight and dispatch. Non-empty values are
    intentionally case- and whitespace-sensitive so operator input is never
    silently rewritten between admission and execution.
    """
    if speed is None or speed == "":
        return None
    raw = str(speed)
    if raw != raw.strip().lower():
        raise ValueError(f"speed {raw!r} is not canonical; use lowercase with no surrounding whitespace")
    return raw


def require_canonical_operator_value(value: str, *, field_name: str, lowercase: bool = False) -> str:
    """Refuse operator spellings that another dispatch stage would rewrite.

    Backend ids and efforts are lowercase protocol values; models are
    case-sensitive but still may not carry surrounding whitespace.  Returning
    the original value (rather than a rewritten one) keeps admission and argv
    construction on the same contract.
    """
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    raw = value
    canonical = raw.strip().lower() if lowercase else raw.strip()
    if raw != canonical:
        qualifier = "lowercase with no surrounding whitespace" if lowercase else "no surrounding whitespace"
        raise ValueError(f"{field_name} {raw!r} is not canonical; use {qualifier}")
    return raw


def build_agent_spec(
    backend_id: str,
    *,
    model: str,
    effort: str | None,
    speed: str | None = None,
    max_turns: int | None = None,
    agent_turn_timeout_s: int | None = None,
    prompt: str | None = None,
    test_cmd: str | None = None,
    out_dir: str | None = None,
) -> AgentSpec:
    """Build a v2 AgentSpec for a BACKENDS-registered invocation backend."""
    del test_cmd, out_dir  # reserved for later wiring

    backend_id = require_canonical_operator_value(backend_id, field_name="backend", lowercase=True)
    model = require_canonical_operator_value(model, field_name="model")
    if effort is not None:
        effort = require_canonical_operator_value(effort, field_name="effort", lowercase=True)
    speed = normalize_operator_speed(speed)
    raise_if_retired_backend(backend_id)
    if backend_id not in BACKENDS:
        raise UnknownBackendError(backend_id)
    if backend_id not in _INVOCATION_BACKENDS:
        raise UnknownBackendError(backend_id)

    _refuse_placeholder_in_operator(model, field_name="model")
    if effort is not None:
        _refuse_placeholder_in_operator(effort, field_name="effort")
    if speed is not None:
        _refuse_placeholder_in_operator(speed, field_name="speed")
    _refuse_nul(model, field_name="model")
    if effort is not None:
        _refuse_nul(effort, field_name="effort")
    if speed is not None:
        _refuse_nul(speed, field_name="speed")
    # Single owner for flock/CLI/adapter callers: a stale WORKBAY_GROK_MODEL
    # (or an explicit retired --model) must not reach argv -m.
    retired = retired_model_warning(model)
    if retired is not None:
        raise RetiredModelError(retired)
    if effort is not None and effort not in REMOTE_EFFORTS:
        raise ValueError(f"effort {effort!r} not in {sorted(REMOTE_EFFORTS)}; refusing to ship a substituted effort")
    if speed is not None and backend_id != "codex-remote":
        raise ValueError(f"{backend_id} does not advertise an orchestration speed")

    caps = BACKENDS[backend_id].capabilities
    if max_turns is not None and not caps.supports_token_budget_cycle_bounds:
        raise UnenforceableBoundError(
            f"{backend_id} cannot enforce max_turns (supports_token_budget_cycle_bounds is False)"
        )

    # D3/D5: one derivation — turn-bounded backends do not require wall timeout.
    requires_timeout = not caps.supports_token_budget_cycle_bounds
    if agent_turn_timeout_s is not None and not requires_timeout:
        raise UnenforceableBoundError(
            f"{backend_id} cannot enforce agent_turn_timeout_s (supports_token_budget_cycle_bounds is True)"
        )
    if agent_turn_timeout_s is not None:
        max_s = remote_lane_timeout_ceiling_s(backend_id)
        if (
            isinstance(agent_turn_timeout_s, bool)
            or not isinstance(agent_turn_timeout_s, int)
            or not 1 <= agent_turn_timeout_s <= max_s
        ):
            raise ValueError(f"{backend_id} agent_turn_timeout_s {agent_turn_timeout_s!r} is outside [1, {max_s}]")
        lane_timeout_s = agent_turn_timeout_s
    elif requires_timeout:
        lane_timeout_s = resolve_remote_lane_timeout_s(backend_id)
    else:
        lane_timeout_s = 0
    operator_effort = effort if effort is not None else ""

    if backend_id == "grok-remote":
        if max_turns is None:
            raise UnenforceableBoundError("grok-remote requires max_turns (supports_token_budget_cycle_bounds)")
        return _seal_agent_spec(
            AgentSpec(
                spec_version=2,
                backend_id="grok-remote",
                binary="grok",
                # Home-relative segment; executor prepends under the remote $HOME
                # (no embedded '$HOME' literal in the element — [AGT-02]).
                path_prepend=[".grok/bin"],
                env={},
                argv=_grok_remote_argv(model=model, effort=operator_effort, max_turns=max_turns),
                operator_values=[model, operator_effort],
                stdin="/dev/null",
                stdout="{result_file}",
                stderr="{run_log}",
                close_fds=[9],
                requires_timeout=requires_timeout,
                lane_timeout_s=lane_timeout_s,
                result_source="stdout",
                agent_exit_to_status={"0": "ok", "*": "agent_failed"},
                auth_match={},
                requires_live_sandbox_preflight=False,
            )
        )

    if backend_id == "codex-remote":
        service_tier: str | None = None
        if speed is not None:
            service_tier = CODEX_SPEED_TO_SERVICE_TIER.get(speed)
            if service_tier is None:
                raise ValueError(
                    f"Refusing codex-remote dispatch with speed {speed!r}: "
                    f"allowed is {sorted(CODEX_SPEED_TO_SERVICE_TIER)}"
                )
        return _seal_agent_spec(
            AgentSpec(
                spec_version=2,
                backend_id="codex-remote",
                binary="codex",
                path_prepend=[CODEX_PATH_PREPEND],
                env={},
                argv=_codex_remote_argv(model=model, effort=effort, service_tier=service_tier),
                operator_values=[model, operator_effort] + ([service_tier] if service_tier is not None else []),
                stdin="{brief_file}",
                stdout="{stream_file}",
                stderr="{run_log}",
                close_fds=[9],
                requires_timeout=requires_timeout,
                lane_timeout_s=lane_timeout_s,
                result_source="output_last_message",
                agent_exit_to_status={"0": "ok", "*": "agent_failed"},
                auth_match={
                    "streams": list(AUTH_MATCH_STREAMS),
                    "exit_codes": [],
                    "patterns": list(AUTH_MATCH_PATTERNS),
                    "precedence": "exit_codes_then_patterns",
                    "lane_timeout_s": lane_timeout_s,
                    "default_model": DEFAULT_CODEX_MODEL,
                },
                rate_limit_match={
                    "streams": list(AUTH_MATCH_STREAMS),
                    "exit_codes": [],
                    "patterns": list(RATE_LIMIT_MATCH_PATTERNS),
                    "precedence": "exit_codes_then_patterns",
                },
                service_tier_warning_match={
                    "streams": ["stderr"],
                    "patterns": list(SERVICE_TIER_WARNING_PATTERNS) if service_tier is not None else [],
                },
                requires_live_sandbox_preflight=(
                    WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT or WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT
                ),
            )
        )

    if backend_id == "openrouter-remote":
        if model not in OPENROUTER_ALLOWED_MODELS:
            raise ValueError(
                f"Refusing openrouter-remote dispatch with model {model!r}: "
                f"allowed is {sorted(OPENROUTER_ALLOWED_MODELS)} (openrouter_lane_config; "
                "OpenRouter's catalogue never feeds this allow-list)"
            )
        allowed_efforts = OPENROUTER_MODEL_ALLOWED_EFFORTS[model]
        if effort is not None and effort not in allowed_efforts:
            # S4-M-05: the transport accepts any effort silently; only the
            # model-specific advertised ones may ship.
            raise ValueError(
                f"Refusing openrouter-remote dispatch with effort {effort!r}: "
                f"{model!r} advertises {sorted(allowed_efforts)} "
                "(openrouter_lane_config.OPENROUTER_MODEL_ALLOWED_EFFORTS)"
            )
        # The spend bound (OpenRouter data.limit) is recorded ONCE, on the
        # lane-manifest row by offload_preflight.record_lane_spend_bound; it is
        # not part of the wire spec (S4-M-04, option b).
        return _seal_agent_spec(
            AgentSpec(
                spec_version=2,
                backend_id="openrouter-remote",
                binary="codex",
                path_prepend=[CODEX_PATH_PREPEND],
                env={},
                argv=_openrouter_remote_argv(model=model, effort=effort),
                operator_values=[model, operator_effort],
                stdin="{brief_file}",
                stdout="{stream_file}",
                stderr="{run_log}",
                close_fds=[9],
                requires_timeout=requires_timeout,
                lane_timeout_s=lane_timeout_s,
                result_source="output_last_message",
                agent_exit_to_status={"0": "ok", "*": "agent_failed"},
                auth_match={
                    "streams": list(AUTH_MATCH_STREAMS),
                    "exit_codes": [],
                    "patterns": list(OPENROUTER_AUTH_MATCH_PATTERNS),
                    "precedence": "exit_codes_then_patterns",
                    "lane_timeout_s": lane_timeout_s,
                    "default_model": DEFAULT_OPENROUTER_MODEL,
                },
                rate_limit_match={
                    "streams": list(AUTH_MATCH_STREAMS),
                    "exit_codes": [],
                    "patterns": list(RATE_LIMIT_MATCH_PATTERNS),
                    "precedence": "exit_codes_then_patterns",
                },
                requires_live_sandbox_preflight=(
                    WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT or WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT
                ),
                env_file=BACKENDS["openrouter-remote"].auth.env_file,
                model_unavailable_match={
                    "streams": list(AUTH_MATCH_STREAMS),
                    "patterns": list(OPENROUTER_MODEL_UNAVAILABLE_PATTERNS),
                },
            )
        )

    if backend_id == "cursor-remote":
        slug, encoded_effort, downgrade = resolve_cursor_model(model, effort)
        prompt_text = prompt if prompt is not None else ""
        if not prompt_text.strip():
            raise ValueError("cursor-remote requires a non-empty prompt (staged brief; argv uses {brief_inline})")
        # NUL truncates bash $(cat …) / command-substitution inlining on the VM.
        _refuse_nul(prompt_text, field_name="prompt")
        return _seal_agent_spec(
            AgentSpec(
                spec_version=2,
                backend_id="cursor-remote",
                binary="cursor-agent",
                path_prepend=[CURSOR_PATH_PREPEND],
                env={},
                argv=_cursor_remote_argv(model=slug),
                # Model + effort only (flock reads ops[0]/ops[1]); brief is not argv.
                operator_values=[slug, encoded_effort or operator_effort],
                stdin="/dev/null",
                stdout="{result_file}",
                stderr="{run_log}",
                close_fds=[9],
                requires_timeout=requires_timeout,
                lane_timeout_s=lane_timeout_s,
                result_source="stdout",
                agent_exit_to_status={"0": "ok", "*": "agent_failed"},
                auth_match={
                    "streams": ["stderr", "stdout"],
                    "exit_codes": [],
                    "patterns": [
                        "Authentication required",
                        "CURSOR_API_KEY",
                        "agent login",
                    ],
                    "precedence": "exit_codes_then_patterns",
                    "lane_timeout_s": lane_timeout_s,
                    "default_model": _default_model_for("cursor-remote"),
                },
                requires_live_sandbox_preflight=False,
                env_file=auth_port("cursor-remote").require_env_file(),
                effort_downgrade_reason=downgrade,
            )
        )

    raise UnknownBackendError(backend_id)


def write_agent_spec(spec: AgentSpec, path: Path | str) -> tuple[Path, Path]:
    """Write ``<path>.json`` and sibling ``<path>.argv`` (NUL-delimited).

    Builds both payloads in memory, writes each to a sibling temp file with
    owner-only mode, then ``os.replace``s ``.argv`` first and ``.json`` second
    so the existence of the JSON implies the argv sidecar already exists.
    """
    for i, el in enumerate(spec.argv):
        if "\0" in el:
            raise NulInSpecValueError(f"argv[{i}] contains a NUL character (collides with .argv sidecar delimiter)")

    base = Path(path)
    if base.suffix == ".json":
        json_path = base
        argv_path = base.with_suffix(".argv")
    else:
        json_path = Path(str(base) + ".json")
        argv_path = Path(str(base) + ".argv")

    json_path.parent.mkdir(parents=True, exist_ok=True)

    json_text = json.dumps(spec.to_json_dict(), indent=2, sort_keys=True) + "\n"
    argv_payload = b"\0".join(el.encode("utf-8") for el in spec.argv) + b"\0"

    argv_tmp = argv_path.with_name(argv_path.name + ".tmp")
    json_tmp = json_path.with_name(json_path.name + ".tmp")
    try:
        argv_tmp.write_bytes(argv_payload)
        os.chmod(argv_tmp, 0o600)
        json_tmp.write_text(json_text)
        os.chmod(json_tmp, 0o600)
        os.replace(argv_tmp, argv_path)
        os.replace(json_tmp, json_path)
    except BaseException:
        for tmp in (argv_tmp, json_tmp):
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    return json_path, argv_path


def _default_model_for(backend_id: str) -> str:
    """Flock/CLI snapshot from lane-config ``DEFAULT_*``.

    ``publish_resolved_model_pins`` rebinds ``BACKENDS[].allowed_model`` and
    the offload profile pin only. Flock defaults stay on this import-time
    snapshot so a remote catalogue cannot leak onto local CLI siblings.
    """
    raise_if_retired_backend(backend_id)
    if backend_id == "grok-remote":
        from workbay_orchestrator_mcp.orchestration.grok_lane_config import (  # noqa: PLC0415
            DEFAULT_GROK_MODEL,
        )

        return DEFAULT_GROK_MODEL
    if backend_id == "codex-remote":
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import (  # noqa: PLC0415
            DEFAULT_CODEX_MODEL,
        )

        return DEFAULT_CODEX_MODEL
    if backend_id == "cursor-remote":
        from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (  # noqa: PLC0415
            DEFAULT_CURSOR_MODEL,
        )

        return DEFAULT_CURSOR_MODEL
    if backend_id == "openrouter-remote":
        return DEFAULT_OPENROUTER_MODEL
    raise UnknownBackendError(backend_id)


def default_est_tokens(backend_id: str) -> int:
    """Backend DEFAULT_EST_TOKENS for flock pre-charge (implementation note S6)."""
    raise_if_retired_backend(backend_id)
    from workbay_orchestrator_mcp.orchestration.grok_lane_config import (
        DEFAULT_EST_TOKENS as GROK_EST,
    )

    if backend_id == "grok-remote":
        return int(GROK_EST)
    if backend_id == "codex-remote":
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
            DEFAULT_EST_TOKENS as CODEX_EST,
        )

        return int(CODEX_EST)
    if backend_id == "cursor-remote":
        from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
            DEFAULT_EST_TOKENS as CURSOR_EST,
        )

        return int(CURSOR_EST)
    if backend_id == "openrouter-remote":
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
            DEFAULT_EST_TOKENS as OPENROUTER_EST,
        )

        return int(OPENROUTER_EST)
    raise UnknownBackendError(backend_id)


def flock_column_defaults() -> dict[str, str]:
    """Canonical backend/effort for empty flock manifest columns.

    Values live only in ``FLOCK_DEFAULTS``; the ``defaults`` CLI emits them for
    bash so offload_flock.sh never carries a second registry.
    """
    backend = FLOCK_DEFAULTS.get("backend") or ""
    effort = FLOCK_DEFAULTS.get("effort") or ""
    if not backend or not effort:
        raise ValueError(
            "flock defaults unresolved: backend and effort must be non-empty "
            f"(got backend={backend!r}, effort={effort!r})"
        )
    return {"backend": backend, "effort": effort}


def flock_receipt_schema() -> dict[str, Any]:
    """JSON Schema for ``<out-dir>/flock-receipt.json`` (implementation note D8)."""
    lane_states = [
        "ok",
        "failed",
        "degraded",
        "refused_budget",
        "refused_bound",
        "not_dispatched",
    ]
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": [
            "receipt_version",
            "manifest_path",
            "out_dir",
            "started_at",
            "finished_at",
            "flock_exit",
            "aborted",
            "token_budget",
            "charged_total",
            "lanes",
        ],
        "properties": {
            "receipt_version": {"const": 1},
            "manifest_path": {"type": "string"},
            "out_dir": {"type": "string"},
            "started_at": {"type": "string"},
            "finished_at": {"type": "string"},
            "flock_exit": {"type": "integer"},
            "aborted": {
                "type": ["string", "null"],
                "enum": [None, "auth_failure", "budget_breach", "validation"],
            },
            "token_budget": {"type": ["integer", "null"]},
            "charged_total": {"type": "integer"},
            "lanes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "lane_id",
                        "kind",
                        "backend_id",
                        "state",
                        "est_tokens",
                        "reason",
                    ],
                    "properties": {
                        "lane_id": {"type": "string"},
                        "kind": {"type": "string"},
                        "backend_id": {"type": "string"},
                        "model": {"type": ["string", "null"]},
                        "effort": {"type": ["string", "null"]},
                        "service_tier": {"type": ["string", "null"]},
                        "requested_service_tier": {"type": ["string", "null"]},
                        "service_tier_confirmation": {
                            "type": ["string", "null"],
                            "enum": [None, "unconfirmed"],
                        },
                        "lane_timeout_s": {"type": ["integer", "null"]},
                        "state": {"type": "string", "enum": lane_states},
                        "dispatched_at": {"type": ["string", "null"]},
                        "finished_at": {"type": ["string", "null"]},
                        "agent_exit_code": {"type": ["integer", "null"]},
                        "reason": {"type": "string"},
                        "est_tokens": {"type": "integer"},
                        "observed_tokens": {"type": ["integer", "null"]},
                        "token_provenance": {
                            "type": ["string", "null"],
                            "enum": [None, "measured", "estimated"],
                        },
                        "charged_tokens": {"type": ["integer", "null"]},
                        "estimate_overshoot": {"type": "boolean"},
                        "result_parse": {
                            "type": ["string", "null"],
                            "enum": [None, "ok", "degraded"],
                        },
                        "operator_action": {"type": ["string", "null"]},
                    },
                    "additionalProperties": True,
                },
            },
        },
    }


def write_flock_receipt(path: Path | str, receipt: dict[str, Any]) -> Path:
    """Atomically write a flock receipt (``.tmp`` + rename)."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    tmp.replace(out)
    return out


def _cli_main(argv: list[str] | None = None) -> int:
    """File-path CLI for offload_flock.sh (implementation note S4–S6)."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Build a remote AgentSpec (implementation note).")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="Write agent.spec.json + .argv for a backend.")
    build.add_argument("--backend", required=True)
    build.add_argument("--model", default=None, help="Default: registry pin for backend.")
    build.add_argument("--effort", default=FLOCK_DEFAULTS["effort"])
    build.add_argument("--speed", default=None, help="Codex-remote only: standard or fast.")
    build.add_argument("--out", required=True, help="Base path (writes .json + .argv).")
    build.add_argument(
        "--max-turns",
        type=int,
        default=None,
        help=(
            f"Required for turn-bounded backends; grok-remote defaults to GROK_MAX_TURNS_CAP ({GROK_MAX_TURNS_CAP})."
        ),
    )
    build.add_argument(
        "--prompt-file",
        default=None,
        help=(
            "Required for cursor-remote: non-empty brief (staged out-of-band; "
            "argv references {brief_inline}, not free text)."
        ),
    )
    sub.add_parser("schema-receipt", help="Print flock-receipt.json schema (D8).")
    est = sub.add_parser("default-est", help="Print DEFAULT_EST_TOKENS for a backend.")
    est.add_argument("--backend", required=True)
    sub.add_parser(
        "defaults",
        help="Print shell-safe KEY=value flock column defaults (backend, effort).",
    )
    write_rc = sub.add_parser("write-receipt", help="Atomically write flock-receipt.json.")
    write_rc.add_argument("--out", required=True)
    write_rc.add_argument(
        "--payload",
        required=True,
        help="Path to a JSON receipt document to write atomically.",
    )
    args = parser.parse_args(argv)

    if args.command == "schema-receipt":
        print(json.dumps(flock_receipt_schema(), indent=2))
        return 0
    if args.command == "default-est":
        try:
            print(default_est_tokens(args.backend))
        except UnknownBackendError as exc:
            print(f"UnknownBackendError: {exc}", file=sys.stderr)
            return 2
        return 0
    if args.command == "defaults":
        import shlex

        try:
            resolved = flock_column_defaults()
        except ValueError as exc:
            print(f"defaults: {exc}", file=sys.stderr)
            return 2
        print(f"backend={shlex.quote(resolved['backend'])}")
        print(f"effort={shlex.quote(resolved['effort'])}")
        return 0
    if args.command == "write-receipt":
        payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
        write_flock_receipt(args.out, payload)
        print(args.out)
        return 0
    if args.command != "build":
        parser.error(f"unknown command: {args.command}")
    try:
        model = args.model if args.model else _default_model_for(args.backend)
        max_turns = args.max_turns
        # grok-remote CLI default must track the canonical cap — never a bare
        # literal (a second source of truth that silently disagrees with
        # grok_lane_config / GrokCliAdapter).
        if max_turns is None and args.backend == "grok-remote":
            max_turns = GROK_MAX_TURNS_CAP
        prompt = None
        if args.prompt_file:
            prompt = Path(args.prompt_file).read_text(encoding="utf-8")
        spec = build_agent_spec(
            args.backend,
            model=model,
            effort=args.effort,
            speed=args.speed,
            max_turns=max_turns,
            prompt=prompt,
        )
    except UnknownBackendError as exc:
        print(f"UnknownBackendError: {exc}", file=sys.stderr)
        return 2
    except (
        UnenforceableBoundError,
        PlaceholderInOperatorValueError,
        NulInSpecValueError,
        RetiredModelError,
        ValueError,
    ) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    json_path, argv_path = write_agent_spec(spec, args.out)
    print(json_path)
    print(argv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli_main())
