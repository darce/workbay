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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS, auth_port
from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
    AUTH_MATCH_PATTERNS,
    AUTH_MATCH_STREAMS,
    CODEX_PATH_PREPEND,
    DEFAULT_CODEX_MODEL,
    LANE_SANDBOX,
    LANE_TIMEOUT_MAX_S,
    LANE_TIMEOUT_S,
    LANE_WRITABLE_ROOTS,
    WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT,
    WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT,
)
from workbay_orchestrator_mcp.orchestration.cursor_lane_config import (
    CURSOR_PATH_PREPEND,
    CURSOR_REMOTE_LANE_TIMEOUT_S,
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
from workbay_orchestrator_mcp.orchestration.oxalpha_lane_config import (
    DEFAULT_0XALPHA_MODEL,
    OXALPHA_ALLOWED_EFFORTS,
    OXALPHA_ALLOWED_MODELS,
    OXALPHA_BASE_URL,
    OXALPHA_PROVIDER_ID,
    OXALPHA_PROVIDER_NAME,
    OXALPHA_WIRE_API,
    WORKBAY_0XALPHA_API_KEY_ENV,
)

_PLACEHOLDER_TOKEN = re.compile(r"\{[^{}]+\}")

# Single source for remote effort allow-list (was adapters/remote_exec.py).
REMOTE_EFFORTS = frozenset({"low", "medium", "high", "xhigh"})

# Cursor headless credential file (implementation note D6). Spec data only — bash never
# hardcodes this path; remote_agent sources AgentSpec.env_file when present.
# implementation note S1: the registry's AuthPort is the single literal; this name is a
# re-export kept for existing callers, not a second copy of the path.
CURSOR_ENV_FILE: str = auth_port("cursor-remote").require_env_file()

# _ENV_FILE_RE / _validate_env_file live in env_file_path (implementation note S1,
# DATA-14) so backend_registry.AuthPort validates with the identical regex.

# Backends with an invocation recipe in this module (S2–S7; 0208 S4 adds 0xalpha).
_INVOCATION_BACKENDS = frozenset({"grok-remote", "codex-remote", "cursor-remote", "0xalpha-remote"})

# implementation note S4: auth-failure patterns for 0xalpha-remote. remote_agent.sh
# matches these as BARE SUBSTRINGS over stdout+stderr with no rc gate, so every
# pattern must be a full diagnostic line, never a bare number or identifier
# (S4-H-01: "401" matched `"input_tokens":4401`; the bare env-var name matched
# any lane that grepped this repo). The codex "Model metadata for model `…`
# not found" line is deliberately NOT here — it is printed on every non-OpenAI
# slug (VERIFY-1) and is not a credential condition.
OXALPHA_AUTH_MATCH_PATTERNS: tuple[str, ...] = (
    "Missing environment variable: `WORKBAY_0XALPHA_API_KEY`",
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
#   (b) stderr: `ERROR codex_models_manager::manager: failed to refresh
#       available models: … missing field `models`` — codex probing
#       OpenRouter's /models endpoint; harmless.
# OXALPHA_LIVE_NOISE_LINES keeps them as negative fixtures for the pattern tests.
OXALPHA_MODEL_UNAVAILABLE_PATTERNS: tuple[str, ...] = ("No endpoints found",)
OXALPHA_LIVE_NOISE_LINES: tuple[str, ...] = (
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


class BraceInArgvPromptError(ValueError):
    """cursor-remote prompt contains a brace and would fail remote argv resolution."""


class NulInSpecValueError(ValueError):
    """Value contains NUL, which collides with the ``.argv`` sidecar delimiter."""


class RetiredModelError(ValueError):
    """Caller asked to dispatch a vendor-retired model slug."""


# Tokens this module emits into argv/stdio fields (executor whole-element sub).
_KNOWN_PLACEHOLDERS: frozenset[str] = frozenset(
    {
        "{brief_file}",
        "{schema_file}",
        "{schema_inline}",
        "{result_file}",
        "{stream_file}",
        "{run_log}",
        "{debug_file}",
    }
)


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


def _brace_excerpt(text: str, offset: int, *, width: int = 80) -> str:
    """Return a short excerpt of *text* centred on *offset* (at most *width* chars)."""
    half = width // 2
    start = max(0, offset - half)
    end = min(len(text), start + width)
    start = max(0, end - width)
    return text[start:end]


def _refuse_brace_in_argv_prompt(prompt_text: str, *, backend_id: str) -> None:
    """Refuse braces in a free-text argv prompt (cursor-remote positional only)."""
    for i, ch in enumerate(prompt_text):
        if ch in "{}":
            excerpt = _brace_excerpt(prompt_text, i)
            raise BraceInArgvPromptError(
                f"{backend_id} prompt contains brace at offset {i} "
                f"(prompt_len={len(prompt_text)}): excerpt={excerpt!r}. "
                f"{backend_id} carries the prompt as a positional argv element "
                "and the remote argv resolver refuses braces, so this would "
                "otherwise fail remotely with exit 7."
            )


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


def _0xalpha_remote_argv(*, model: str, effort: str) -> list[str]:
    """codex argv plus the five ``-c`` provider overrides (VERIFY-1 shape).

    Nested ``model_providers.<id>.<key>=`` overrides are accepted by codex
    0.145 and the key is read from the VM environment via ``env_key`` — the
    secret is never on argv.
    """
    return _codex_remote_argv(model=model, effort=effort) + [
        "-c",
        f"model_provider={OXALPHA_PROVIDER_ID}",
        "-c",
        f"model_providers.{OXALPHA_PROVIDER_ID}.name={OXALPHA_PROVIDER_NAME}",
        "-c",
        f"model_providers.{OXALPHA_PROVIDER_ID}.base_url={OXALPHA_BASE_URL}",
        "-c",
        f"model_providers.{OXALPHA_PROVIDER_ID}.env_key={WORKBAY_0XALPHA_API_KEY_ENV}",
        "-c",
        f"model_providers.{OXALPHA_PROVIDER_ID}.wire_api={OXALPHA_WIRE_API}",
    ]


def _codex_remote_argv(*, model: str, effort: str) -> list[str]:
    # Argv only — binary is AgentSpec.binary ('codex'). implementation note D2 row.
    #
    # writable_roots re-allows .git under the SAME workspace-write policy so the
    # lane can commit; without it the commit dies read-only and turn.patch comes
    # back empty. Entries are relative to -C, and the `-c` value parses as TOML,
    # which json.dumps emits correctly for a list of plain strings. This is a
    # narrow re-allow, never danger-full-access ([SEC-04]).
    return [
        "exec",
        "--json",
        "--ignore-user-config",
        "-C",
        ".",
        "-m",
        model,
        "-c",
        f"model_reasoning_effort={effort}",
        "-c",
        f"sandbox_workspace_write.writable_roots={json.dumps(list(LANE_WRITABLE_ROOTS))}",
        "-s",
        LANE_SANDBOX,
        "--output-schema",
        "{schema_file}",
        "-o",
        "{result_file}",
        "-",
    ]


def _cursor_remote_argv(*, model: str, prompt: str) -> list[str]:
    # Argv only — binary is AgentSpec.binary ('cursor-agent'). Prompt is a
    # positional (no --prompt-file); NUL argv carries arbitrary brief text.
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
        prompt,
    ]


def build_agent_spec(
    backend_id: str,
    *,
    model: str,
    effort: str,
    max_turns: int | None = None,
    agent_turn_timeout_s: int | None = None,
    prompt: str | None = None,
    test_cmd: str | None = None,
    out_dir: str | None = None,
) -> AgentSpec:
    """Build a v2 AgentSpec for a BACKENDS-registered invocation backend."""
    del test_cmd, out_dir  # reserved for later wiring

    if backend_id not in BACKENDS:
        raise UnknownBackendError(backend_id)
    if backend_id not in _INVOCATION_BACKENDS:
        raise UnknownBackendError(backend_id)

    _refuse_placeholder_in_operator(model, field_name="model")
    _refuse_placeholder_in_operator(effort, field_name="effort")
    _refuse_nul(model, field_name="model")
    _refuse_nul(effort, field_name="effort")
    # Single owner for flock/CLI/adapter callers: a stale WORKBAY_GROK_MODEL
    # (or an explicit retired --model) must not reach argv -m.
    retired = retired_model_warning(model)
    if retired is not None:
        raise RetiredModelError(retired)
    if effort not in REMOTE_EFFORTS:
        raise ValueError(f"effort {effort!r} not in {sorted(REMOTE_EFFORTS)}; refusing to ship a substituted effort")

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
        if (
            isinstance(agent_turn_timeout_s, bool)
            or not isinstance(agent_turn_timeout_s, int)
            or not 1 <= agent_turn_timeout_s <= LANE_TIMEOUT_MAX_S
        ):
            raise ValueError(
                f"agent_turn_timeout_s must be an integer in [1, {LANE_TIMEOUT_MAX_S}], got {agent_turn_timeout_s!r}"
            )
        lane_timeout_s = agent_turn_timeout_s
    elif backend_id == "cursor-remote":
        lane_timeout_s = int(CURSOR_REMOTE_LANE_TIMEOUT_S)
    elif requires_timeout:
        lane_timeout_s = int(LANE_TIMEOUT_S)
    else:
        lane_timeout_s = 0

    if backend_id == "grok-remote":
        if max_turns is None:
            raise UnenforceableBoundError("grok-remote requires max_turns (supports_token_budget_cycle_bounds)")
        return AgentSpec(
            spec_version=2,
            backend_id="grok-remote",
            binary="grok",
            # Home-relative segment; executor prepends under the remote $HOME
            # (no embedded '$HOME' literal in the element — [AGT-02]).
            path_prepend=[".grok/bin"],
            env={},
            argv=_grok_remote_argv(model=model, effort=effort, max_turns=max_turns),
            operator_values=[model, effort],
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

    if backend_id == "codex-remote":
        return AgentSpec(
            spec_version=2,
            backend_id="codex-remote",
            binary="codex",
            path_prepend=[CODEX_PATH_PREPEND],
            env={},
            argv=_codex_remote_argv(model=model, effort=effort),
            operator_values=[model, effort],
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
            requires_live_sandbox_preflight=(
                WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT or WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT
            ),
        )

    if backend_id == "0xalpha-remote":
        if model not in OXALPHA_ALLOWED_MODELS:
            raise ValueError(
                f"Refusing 0xalpha-remote dispatch with model {model!r}: "
                f"allowed is {sorted(OXALPHA_ALLOWED_MODELS)} (oxalpha_lane_config; "
                "OpenRouter's catalogue never feeds this allow-list)"
            )
        if effort not in OXALPHA_ALLOWED_EFFORTS:
            # S4-M-05: the transport accepts any effort silently; only the
            # advertised ones may ship (oxalpha_lane_config.OXALPHA_ALLOWED_EFFORTS).
            raise ValueError(
                f"Refusing 0xalpha-remote dispatch with effort {effort!r}: "
                f"allowed is {sorted(OXALPHA_ALLOWED_EFFORTS)} (oxalpha_lane_config; the "
                "model advertises max/high/low and the transport accepts unadvertised "
                "efforts silently)"
            )
        # The spend bound (OpenRouter data.limit) is recorded ONCE, on the
        # lane-manifest row by offload_preflight.record_lane_spend_bound; it is
        # not part of the wire spec (S4-M-04, option b).
        return AgentSpec(
            spec_version=2,
            backend_id="0xalpha-remote",
            binary="codex",
            path_prepend=[CODEX_PATH_PREPEND],
            env={},
            argv=_0xalpha_remote_argv(model=model, effort=effort),
            operator_values=[model, effort],
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
                "patterns": list(OXALPHA_AUTH_MATCH_PATTERNS),
                "precedence": "exit_codes_then_patterns",
                "lane_timeout_s": lane_timeout_s,
                "default_model": DEFAULT_0XALPHA_MODEL,
            },
            requires_live_sandbox_preflight=(
                WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT or WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT
            ),
            env_file=BACKENDS["0xalpha-remote"].auth.env_file,
            model_unavailable_match={
                "streams": list(AUTH_MATCH_STREAMS),
                "patterns": list(OXALPHA_MODEL_UNAVAILABLE_PATTERNS),
            },
        )

    if backend_id == "cursor-remote":
        slug, encoded_effort, downgrade = resolve_cursor_model(model, effort)
        prompt_text = prompt if prompt is not None else ""
        if not prompt_text.strip():
            raise ValueError("cursor-remote requires a non-empty prompt (positional argv)")
        _refuse_nul(prompt_text, field_name="prompt")
        _refuse_placeholder_element(prompt_text, field_name="prompt")
        _refuse_brace_in_argv_prompt(prompt_text, backend_id=backend_id)
        return AgentSpec(
            spec_version=2,
            backend_id="cursor-remote",
            binary="cursor-agent",
            path_prepend=[CURSOR_PATH_PREPEND],
            env={},
            argv=_cursor_remote_argv(model=slug, prompt=prompt_text),
            operator_values=[slug, encoded_effort or effort, prompt_text],
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
                "lane_timeout_s": CURSOR_REMOTE_LANE_TIMEOUT_S,
                "default_model": _default_model_for("cursor-remote"),
            },
            requires_live_sandbox_preflight=False,
            env_file=auth_port("cursor-remote").require_env_file(),
            effort_downgrade_reason=downgrade,
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
    if backend_id == "0xalpha-remote":
        return DEFAULT_0XALPHA_MODEL
    raise UnknownBackendError(backend_id)


def default_est_tokens(backend_id: str) -> int:
    """Backend DEFAULT_EST_TOKENS for flock pre-charge (implementation note S6)."""
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
    if backend_id == "0xalpha-remote":
        from workbay_orchestrator_mcp.orchestration.codex_lane_config import (
            DEFAULT_EST_TOKENS as OXALPHA_EST,
        )

        return int(OXALPHA_EST)
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
        help="Required for cursor-remote: brief text baked into positional argv.",
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
            max_turns=max_turns,
            prompt=prompt,
        )
    except UnknownBackendError as exc:
        print(f"UnknownBackendError: {exc}", file=sys.stderr)
        return 2
    except (
        UnenforceableBoundError,
        PlaceholderInOperatorValueError,
        BraceInArgvPromptError,
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
