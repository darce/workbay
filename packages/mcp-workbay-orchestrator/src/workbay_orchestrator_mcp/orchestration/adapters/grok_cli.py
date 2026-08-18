"""Execution adapter for the ``grok`` CLI (xAI junior worker lane).

implementation note D1+D2. The adapter is a port at the integration seam (Farley): every
grok-specific concern — flag mapping, bounded subprocess, the narrated-JSON
parse quirk (Evidence #7) — lives here, not in the host-agnostic lane exec.

Bounded per Nygard (Integration Points / Timeouts / Fail Fast): a hard
wall-clock ``subprocess`` timeout plus ``--max-turns`` (the codex heartbeat
loop is deliberately NOT copied — it is unbounded). Because grok runs with
``--always-approve`` and spawns tool/shell grandchildren, the timeout kills the
whole process GROUP, not just the direct child. A model outside the configured
``WORKBAY_GROK_MODEL`` / ``DEFAULT_GROK_MODEL`` pin (never grok-build) is refused
pre-exec (fail fast; implementation note S2 retires the legacy pin-attestation arm); an
unparseable turn fails closed to ``needs_guidance`` rather than returning a
silent-empty result.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Callable

_LOGGER = logging.getLogger(__name__)

from ..backend_adapter import BackendAdapter, BackendResult
from ..grok_lane_config import (
    DEFAULT_GROK_MODEL,
    GROK_MAX_TURNS_CAP,
    GROK_TIMEOUT_CAP,
    RETIRED_GROK_MODELS,
    retired_model_warning,
)
from ..secure_sandbox import (
    ShallowSandbox,
    sandbox_provision_enabled,
    secure_sandbox_enabled,
)
from ..token_estimate import build_token_estimates
from ._result_text import (
    KNOWN_HANDOFF_ACTIONS,
    _iter_balanced_json_objects,
    find_embedded_json_object,
    handoff_action_needs_clamp,
    is_shaped_result_payload,
    normalize_cli_usage,
    recover_unshaped_payload,
    stamp_recovery_tier,
    RECOVERY_TIER_BALANCED,
    RECOVERY_TIER_EMBEDDED,
)
from .grok_session_tokens import (
    read_cumulative_total,
    read_session_token_deltas,
    resolve_session_dir,
)

#: Pinned-model guarantee: grok-build must never author task work. Any resolved
#: ``-m`` model carrying a build spelling is refused pre-exec (decision #2799).
_GROK_BUILD_TOKEN = "grok-build"

#: Tolerant build-token matcher: catches ``grok-build``, ``grok_build``,
#: ``grok4-build``, dotted versioned builds (``grok-4.5-build``), and build
#: variants regardless of separator/casing so the pre-exec guard does not fail
#: OPEN on an alternate spelling (s3-a-009). ``[\w.\-]*`` is required: ``\w``
#: alone excludes ``.`` and cannot see versioned build spellings the CLI writes.
_GROK_BUILD_RE = re.compile(r"grok[\w.\-]*build", re.IGNORECASE)

#: A ``grok_args`` value that re-pins the model at the CLI level would bypass the
#: pre-exec guard (last-wins parsing), so any model-override flag in the caller's
#: extra args is refused (s3-a-004).
_MODEL_OVERRIDE_RE = re.compile(r"(^|\s)(-m|--model)(\s|=|$)|(^|\s)-c\s*[\"']?[\w.]*model", re.IGNORECASE)

#: Reasoning-effort tiers grok declares (REQUEST A1). Anything else is dropped
#: from argv rather than passed through to a fail-fast at exec.
_VALID_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})

#: macOS malloc-debug ambient vars — must never reach the grok child (stderr noise).
_MALLOC_DEBUG_ENV_KEYS = ("MallocStackLogging", "MallocStackLoggingNoCompact")

#: The schema-shaped result object always carries this key; the parse chain uses
#: it to validate shape so a narrated ``{...}`` fragment is not mistaken for the
#: result (s3-a-002).
_RESULT_KEY = "handoff_action"

#: Keys that mark a dict as grok's ``--output-format json`` envelope (vs a bare
#: result object), used to pick the real envelope out of noisy stdout (s3-a-003).
_ENVELOPE_MARKERS = (
    "structuredOutput",
    "structuredOutputError",
    "usage",
    "model",
    "text",
    "output_text",
    "sessionId",  # implementation note: only reliable token-telemetry key on grok envelope
)

#: Extract model_id values from debug-log lines for pin-match verification
#: (REV-S4-02). Tolerant of separator / quotes / casing around the key.
_MODEL_ID_VALUE_RE = re.compile(r"model_id[\s:=\"']+([^\s\"',;]+)", re.IGNORECASE)

#: Positive confirmation that the scan input is a recognizable grok debug log at
#: all; a non-empty log with none of these markers cannot attest model-pin
#: authorship and must fail closed (s5-a-004).
_DEBUG_MARKER_RE = re.compile(r"assistantitem|model_id", re.IGNORECASE)

#: Fenced code blocks (```json ... ```). Iterated in full (not just the first)
#: so a leading non-JSON fence — e.g. a bash block with ``awk '{print $1}'`` —
#: does not hide a later JSON result fence (s3-a-001).
_FENCE_RE = re.compile(r"```(?:[A-Za-z0-9_-]+)?\s*\n?(.*?)```", re.DOTALL)

#: Authoritative served-model record. The debug log is tracing-style text:
#: ``<ts> <LEVEL> <module>: <message> key=value``. Only this module's metadata
#: line is an authored served-model claim; every other ``model_id`` is noise.
_CONVERSATION_MODULE = "xai_grok_sampling_types::conversation"
_METADATA_MARKER = "setting model metadata on AssistantItem"
_TRACE_LINE_RE = re.compile(
    r"^\S+\s+\S+\s+(?P<module>[A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+):\s+(?P<message>.*)$"
)
_BUILD_SUFFIX = "-build"
_OPTION_NONE = "None"
_OPTION_SOME_PREFIX = "Some("
_WIDE_ENCODING_HEAD = 4096
_WIDE_NUL_FRACTION = 0.25
_WIDE_BOM_PREFIXES = (
    b"\xff\xfe\x00\x00",  # UTF-32-LE
    b"\x00\x00\xfe\xff",  # UTF-32-BE
    b"\xff\xfe",  # UTF-16-LE
    b"\xfe\xff",  # UTF-16-BE
)


def _served_base_model(model_id: str) -> str:
    """Strip a trailing ``-build`` suffix; ``grok-4.6-build`` → ``grok-4.6``."""
    mid = str(model_id or "").strip()
    if mid.lower().endswith(_BUILD_SUFFIX):
        return mid[: -len(_BUILD_SUFFIX)]
    return mid


def _parse_option_token(raw: str) -> tuple[str, str] | None:
    """Parse ``None`` or ``Some(inner)``. Malformed tokens return None."""
    token = str(raw or "").strip()
    if token == _OPTION_NONE:
        return ("none", "")
    if token.startswith(_OPTION_SOME_PREFIX) and token.endswith(")") and len(token) > len(_OPTION_SOME_PREFIX) + 1:
        inner = token[len(_OPTION_SOME_PREFIX) : -1]
        if not inner:
            return None
        return ("some", inner)
    return None


def _consume_quoted_span(message: str, start: int) -> int:
    """Return the index after the quoted span that begins at *start*."""
    i = start + 1
    n = len(message)
    while i < n:
        ch = message[i]
        if ch == "\\" and i + 1 < n:
            i += 2
            continue
        if ch == '"':
            return i + 1
        i += 1
    return n


def _consume_paren_span(message: str, start: int) -> int:
    """Return the index after the parenthesised span that begins at *start*."""
    depth = 0
    i = start
    n = len(message)
    while i < n:
        ch = message[i]
        if ch == '"':
            i = _consume_quoted_span(message, i)
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth -= 1
            i += 1
            if depth == 0:
                return i
            continue
        i += 1
    return n


def _consume_balanced_value(message: str, start: int) -> int:
    """Consume a quoted or parenthesised value through its matching close."""
    rest = message[start:]
    if rest.startswith('Some("'):
        end = _consume_quoted_span(message, start + len("Some("))
        if end < len(message) and message[end] == ")":
            return end + 1
        return end
    if rest.startswith('"'):
        return _consume_quoted_span(message, start)
    if rest.startswith("("):
        return _consume_paren_span(message, start)
    return start


def _take_field_value(message: str, start: int) -> tuple[str, int]:
    """Extract one field value starting at *start*. Never raises."""
    n = len(message)
    if start >= n or message[start].isspace():
        return "", start
    rest = message[start:]
    if rest.startswith('Some("') or rest.startswith('"') or rest.startswith("("):
        end = _consume_balanced_value(message, start)
        return message[start:end], end
    end = start
    while end < n and not message[end].isspace():
        end += 1
    return message[start:end], end


def _next_top_level_field(message: str, start: int) -> tuple[str | None, str, int]:
    """Parse one top-level token as ``key=value``, or skip a non-field token."""
    n = len(message)
    j = start
    while j < n:
        ch = message[j]
        if ch.isspace() or ch in '="()':
            break
        j += 1
    if j > start and j < n and message[j] == "=":
        value, end = _take_field_value(message, j + 1)
        return message[start:j], value, end
    _, end = _take_field_value(message, start)
    return None, "", end


def _field_value(message: str, key: str) -> str | None:
    """Return the unique top-level ``key=value`` token, or None.

    A field is a top-level whitespace-bounded token. Occurrences of
    ``key=`` inside a quoted ``\"...\"`` span or parenthesised ``Some(...)``
    nesting are not fields. When the value begins with ``Some(\"``, ``\"``,
    or ``(``, consume through the matching close so internal whitespace is
    not a delimiter. Empty or whitespace-only values, absent keys, and
    duplicate top-level keys all return None (fail closed). Never raises.
    """
    values: list[str] = []
    i = 0
    n = len(message)
    while i < n:
        if message[i].isspace():
            i += 1
            continue
        field_key, value, i = _next_top_level_field(message, i)
        if field_key == key:
            values.append(value)
    if len(values) != 1:
        return None
    value = values[0]
    if not value.strip():
        return None
    return value


def _parse_conversation_metadata_line(
    line: str,
) -> tuple[str, str, str, str] | None | bool:
    """Parse one conversation-module metadata line into a served-model tuple.

    Returns ``(model_id, fingerprint_inner, effort_kind, base_model)`` when
    the line is an authoritative metadata record. Returns ``False`` when the
    line is that record but cannot be typed (fail closed). Returns ``None``
    when the line is not a conversation-module metadata record.
    """
    match = _TRACE_LINE_RE.match(line.rstrip("\n"))
    if match is None:
        return None
    if match.group("module") != _CONVERSATION_MODULE:
        return None
    message = match.group("message")
    if _METADATA_MARKER not in message:
        return None
    model_id = _field_value(message, "model_id")
    fingerprint_raw = _field_value(message, "model_fingerprint")
    effort_raw = _field_value(message, "reasoning_effort")
    if not model_id or fingerprint_raw is None or effort_raw is None:
        return False
    fingerprint = _parse_option_token(fingerprint_raw)
    effort = _parse_option_token(effort_raw)
    if fingerprint is None or fingerprint[0] != "some" or effort is None:
        return False
    return (model_id, fingerprint[1], effort[0], _served_base_model(model_id))


def _is_bootstrap_base(base_model: str) -> bool:
    # Bare ({"", "grok"}) authored bases are contamination. A versioned
    # -build base is checked against RETIRED_GROK_MODELS and the requested
    # pin by :func:`_scan_conversation_served_models`.
    return base_model.strip().lower() in {"", "grok"}


def _scan_conversation_served_models(
    debug_text: str,
    requested_model: str | None = None,
) -> tuple[str, list[str]] | None:
    """Typed reader: contamination from conversation-module served-model tuples.

    An authored turn (``reasoning_effort=Some(...)``) is contamination iff
    any of:

    * its served base is empty or bare ``grok`` (see :func:`_is_bootstrap_base`)
    * its served base is in :data:`RETIRED_GROK_MODELS` (e.g. ``grok-4.5``)
    * a requested pin is provided and the served base does not match it

    So an authored ``grok-4.5-build`` line under a ``grok-4.6`` pin is
    quarantine, not a clean lane. None-effort bootstrap records are ignored.
    A metadata line that cannot be typed is contamination. Every other
    ``model_id`` token is ignored.

    ``requested_model`` is the dispatch pin; the scan compares the served
    *base* (``-build`` stripped) to that pin so a vendor-resolved
    ``grok-4.6-build`` under a ``grok-4.6`` request stays clean.
    """
    requested_base = (
        _served_base_model(requested_model).strip().lower() if requested_model else ""
    )
    retired = {slug.strip().lower() for slug in RETIRED_GROK_MODELS}
    evidence: list[str] = []
    saw_bootstrap = False
    saw_retired_or_off_pin = False
    for raw_record in debug_text.split("\n"):
        raw_line = raw_record.rstrip("\r")
        parsed = _parse_conversation_metadata_line(raw_line)
        if parsed is None:
            continue
        if parsed is False:
            evidence.append(raw_line.strip())
            continue
        _model_id, _fingerprint, effort_kind, base_model = parsed
        if effort_kind == "none":
            continue
        if effort_kind != "some":
            continue
        base = (base_model or "").strip().lower()
        if _is_bootstrap_base(base):
            evidence.append(raw_line.strip())
            saw_bootstrap = True
            continue
        if base in retired:
            evidence.append(raw_line.strip())
            saw_retired_or_off_pin = True
            continue
        if requested_base and base != requested_base:
            evidence.append(raw_line.strip())
            saw_retired_or_off_pin = True
    if not evidence:
        return None
    if saw_retired_or_off_pin and not saw_bootstrap:
        return (
            "authored served-model is retired or does not match the requested "
            "pin — contamination quarantine",
            evidence[:20],
        )
    return (
        "authored served-model is bare grok-build — contamination quarantine",
        evidence[:20],
    )


def count_grok_build_items(debug_text: str, requested_model: str | None = None) -> int:
    """Count typed contamination records in a debug log (pure)."""
    text = debug_text if isinstance(debug_text, str) else ""
    result = _scan_conversation_served_models(text, requested_model)
    if result is None:
        return 0
    return len(result[1])


def _looks_like_wide_encoding(raw: bytes) -> bool:
    """True iff *raw* carries a UTF-16/UTF-32 BOM or a high-NUL file head."""
    if raw.startswith(_WIDE_BOM_PREFIXES):
        return True
    head = raw[:_WIDE_ENCODING_HEAD]
    if not head:
        return False
    return (head.count(0) / len(head)) > _WIDE_NUL_FRACTION


def _detect_grok_build_contamination(
    debug_file: Path,
    requested_model: str | None = None,
) -> tuple[str, list[str]] | None:
    """Post-turn served-model contamination quarantine (implementation note S2).

    Reads only the conversation-module metadata tuple. Missing/empty logs
    are not contamination. Non-UTF-8 bytes, a genuine wide encoding
    (UTF-16/UTF-32 BOM or a high NUL-byte fraction in the file head), and
    untyped metadata lines fail closed. An incidental NUL in otherwise
    valid UTF-8 is not wide encoding. Returns ``(blocker, evidence_lines)``
    or ``None``.
    """
    if not debug_file.is_file():
        return None
    try:
        raw = debug_file.read_bytes()
    except OSError:
        return (
            "debug log is not valid UTF-8 — grok-build contamination quarantine",
            [],
        )
    if _looks_like_wide_encoding(raw):
        return (
            "debug log is a wide encoding — grok-build contamination quarantine",
            [],
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return (
            "debug log is not valid UTF-8 — grok-build contamination quarantine",
            [],
        )
    if not text.strip():
        return None
    return _scan_conversation_served_models(text, requested_model)


def _verify_model_pin(debug_file: Path, expected_model: str) -> tuple[str, list[str]] | None:
    """Full post-turn model-pin check (utility / unit-test backstop; REV-S4-02).

    Execute() no longer invokes this for pin attestation (implementation note S2);
    production uses :func:`_detect_grok_build_contamination` only. Kept for
    config-layer regression tests that still assert the strict pin scanner.
    """
    pin = str(expected_model or "").strip()
    if not pin:
        return ("model pin empty — pin guarantee unverifiable (failing closed)", [])
    if not debug_file.is_file():
        return (
            f"grok debug log absent — model pin '{pin}' unverifiable (failing closed)",
            [],
        )
    text = debug_file.read_text(errors="replace")
    if not text.strip():
        return (
            f"grok debug log empty — model pin '{pin}' unverifiable (failing closed)",
            [],
        )
    if not _DEBUG_MARKER_RE.search(text):
        return (
            "grok debug log has no recognizable AssistantItem/model_id markers — "
            f"model pin '{pin}' unverifiable (failing closed)",
            [],
        )
    contamination = _detect_grok_build_contamination(debug_file, requested_model=pin)
    if contamination is not None:
        blocker, evidence = contamination
        return (blocker.replace("contamination quarantine", f"model pin '{pin}' violated"), evidence)
    observed = [m.group(1) for m in _MODEL_ID_VALUE_RE.finditer(text)]
    if not observed:
        return (
            f"grok debug log has no extractable model_id values — model pin '{pin}' unverifiable (failing closed)",
            [],
        )
    pin_lower = pin.lower()
    foreign = [m for m in observed if m.lower() != pin_lower]
    if foreign:
        evidence = [
            line.strip()
            for line in text.splitlines()
            if _MODEL_ID_VALUE_RE.search(line) and any(f.lower() in line.lower() for f in foreign)
        ][:20]
        return (
            f"non-pinned model marker(s) {sorted(set(foreign))!r} — expected pin '{pin}' (failing closed)",
            evidence,
        )
    return None


def _worktree_branch(worktree_path: Path | str) -> str:
    """Current branch of the lane worktree (for the secure-sandbox clone).

    Raises RuntimeError on a detached HEAD or git failure — the secure sandbox is
    fail-closed, so an unresolvable branch must abort rather than run insecurely.
    """
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    branch = (res.stdout or "").strip()
    if res.returncode != 0 or not branch or branch == "HEAD":
        raise RuntimeError(
            f"cannot resolve lane branch for secure sandbox at '{worktree_path}' "
            f"(detached HEAD or git error): {(res.stderr or '').strip()[-200:]}"
        )
    return branch


def find_grok(explicit_path: str | None = None) -> str:
    """Find the grok CLI executable (explicit override > PATH)."""
    if explicit_path:
        return explicit_path
    res = subprocess.run(["which", "grok"], capture_output=True, text=True)
    if res.returncode == 0 and res.stdout.strip():
        return res.stdout.strip()
    raise RuntimeError("grok CLI not found in PATH. Install it or provide --grok-bin.")


def _validate_timeout(timeout: object) -> int:
    # timeout must be a positive int: remote_exec does int(self._grok.timeout)
    # and emits --timeout <n>; 0 is unbounded in remote_agent.sh, and any
    # 0 < x < 1 truncates to 0. bool is an int subclass — reject it first.
    # [AGT-10]: name the value so construction failures are diagnosable.
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError(
            f"timeout must be a positive integer (got {timeout!r}); "
            "a non-integral or non-positive timeout is truncated to "
            "--timeout 0, which runs the agent unbounded"
        )
    return timeout


class GrokCliAdapter(BackendAdapter):
    supports_jail = True

    """Execution adapter for the ``grok`` CLI headless turn."""

    def __init__(
        self,
        grok_bin: str | None = None,
        grok_args: list[str] | None = None,
        *,
        timeout: int = GROK_TIMEOUT_CAP,
        max_turns: int = GROK_MAX_TURNS_CAP,
    ):
        # Resolve the binary LAZILY (in execute), not in the ctor: an eager
        # find_grok here raises RuntimeError when grok is absent, and the daemon
        # constructs the adapter OUTSIDE its EXEC_FAILED try/except, so an
        # unresolved binary would crash the whole worker process instead of
        # logging a failed cycle (s4-a-001). Contrast claude-code, which also
        # defers binary resolution to execute().
        # Assign through the property so construction and post-construction
        # assignment share one validation path [RES-02].
        self.grok_bin = grok_bin
        self.grok_args = grok_args or []
        self.timeout = timeout
        self.max_turns = max_turns

    @property
    def timeout(self) -> int:
        return self._timeout

    @timeout.setter
    def timeout(self, value: object) -> None:
        self._timeout = _validate_timeout(value)

    def resolve_reasoning_effort(
        self,
        *,
        orchestrator_root: Path,
        task_ref: str,
        lane_id: str,
        requested: str,
        cycle: int,
        prompt_override: str | None,
        previous_run_exhausted: bool = False,
    ) -> tuple[str | None, list[str]]:
        """Resolve reasoning effort via the shared auto-resolver (as codex-cli)."""
        from .._env import resolve_auto_reasoning_effort  # noqa: PLC0415

        return resolve_auto_reasoning_effort(
            orchestrator_root=orchestrator_root,
            task_ref=task_ref,
            lane_id=lane_id,
            requested=requested,
            cycle=cycle,
            prompt_override=prompt_override,
            previous_run_exhausted=previous_run_exhausted,
        )

    def execute(
        self,
        prompt: str,
        schema: dict[str, Any],
        worktree_path: Path,
        model: str | None = None,
        reasoning_effort: str | None = None,
        session_mode: str | None = None,
        env: dict[str, str] | None = None,
        progress_callback: Callable[..., None] | None = None,
        **kwargs: Any,
    ) -> BackendResult:
        """Execute one bounded grok turn and parse its result."""
        from workbay_handoff_mcp.enums import (  # noqa: PLC0415
            WorkerEventName,
            normalize_model_identity,
            normalize_model_label,
        )

        del session_mode  # accepted-and-ignored (no session resume; YAGNI)
        extra_args = kwargs.get("grok_args") or self.grok_args

        # Allowed-model pre-exec hard-fail (fail fast, no retry — decision #2799).
        # implementation note S2 [REF-19]: legacy pin-attestation allow-list arm retired. Allowed: the
        # configured pin DEFAULT_GROK_MODEL only (env WORKBAY_GROK_MODEL >
        # shipped default). grok-build (any spelling) and unknown slugs refused —
        # allow-list polarity preserved, cannot fail OPEN (s3-a-009).
        effective_model = model or DEFAULT_GROK_MODEL
        retired_warning = retired_model_warning(effective_model)
        if (
            _GROK_BUILD_RE.search(effective_model)
            or effective_model != DEFAULT_GROK_MODEL
            or retired_warning is not None
        ):
            message = (
                f"Refusing to dispatch grok with model '{effective_model}': allowed is "
                f"the configured pin '{DEFAULT_GROK_MODEL}' (WORKBAY_GROK_MODEL); "
                "grok-build family is refused (decision #2799)."
            )
            if retired_warning is not None:
                message = f"{message} {retired_warning}"
            raise RuntimeError(message)
        # A model re-pin smuggled through grok_args would bypass the guard above
        # (grok appends extra_args AFTER '-m', last-wins), so refuse any
        # model-override flag or build token in the caller's extra args (s3-a-004).
        joined_extra = " ".join(str(a) for a in extra_args)
        if _GROK_BUILD_RE.search(joined_extra) or _MODEL_OVERRIDE_RE.search(joined_extra):
            raise RuntimeError(
                "Refusing to dispatch grok: grok_args must not re-pin the model "
                f"(pin guard bypass, decision #2799): {joined_extra!r}"
            )

        # Tier-less identity so the prompt-suffix actor matches the config-env
        # WORKBAY_HANDOFF_DEFAULT_AGENT that bootstrap_lane derives from the SAME
        # effective model (both slices normalize the effective model, so the two
        # identities stay harmonized under a model override, not only for the
        # default — s6-a-003).
        pinned_model_identity = (
            normalize_model_identity(normalize_model_label(effective_model), None) or effective_model
        )

        # Attributed telemetry must reflect what actually ran: an out-of-range
        # reasoning effort is dropped from argv, so it must NOT be stamped back
        # onto the result verbatim (s3-a-006).
        applied_effort = reasoning_effort if reasoning_effort in _VALID_REASONING_EFFORTS else None

        grok_bin = find_grok(self.grok_bin)

        # implementation note S1: WorkBay-turn delta = post cumulative − pre cumulative
        # (PR-0094-04). Fresh CLI calls have no prior session → pre_total=0 (the
        # whole session total legitimately IS this turn). If a prior session id
        # is supplied (future resume / explicit kwarg), snapshot its cumulative
        # now; a FAILED snapshot stays None so the reader marks the baseline
        # unavailable instead of attributing the whole resumed-session cumulative
        # to one turn (REV-S1-02). The snapshot's session id travels with it so a
        # post-call session restart is detected, not silently clamped (REV-S1-01).
        pre_total: int | None = 0
        pre_session_id: str | None = None
        prior_session_id = kwargs.get("grok_session_id")
        if isinstance(prior_session_id, str) and prior_session_id.strip():
            pre_session_id = prior_session_id.strip()
            pre_total = read_cumulative_total(pre_session_id, worktree_path)

        with ExitStack() as _stack:
            # Secure offload (internal): confine grok to a
            # shallow, history-stripped clone of the lane branch so it cannot
            # bundle the full .git object DB to gs://grok-code-session-traces
            # (feedback_grok_cli_repo_exfiltration). A worktree shares the
            # primary .git; the sandbox does not. FAIL-CLOSED: a sandbox failure
            # raises rather than silently running grok against full history. Grok
            # commits inside the sandbox; port_commits_back replays them onto the
            # real lane branch after a green exec so the pass engine's
            # commit-landed detection + close_slice are unchanged.
            exec_root: Path = Path(worktree_path)
            _sandbox = None
            # Provision outcome for pass-result telemetry (implementation note). Distinguishes
            # sanctioned no_python_project skips from admission refusals and real
            # uv-sync failures (those still raise SecureSandboxError).
            sandbox_provision: str | None = None
            if secure_sandbox_enabled():
                _sandbox = _stack.enter_context(ShallowSandbox(Path(worktree_path), _worktree_branch(worktree_path)))
                # Provision the sandbox env (uv sync when a root pyproject.toml is
                # present) so the worker's self-verify runs against sandbox src.
                # Fail-closed on real provision failure; detect-and-skip when the
                # clone has no root Python project.
                if sandbox_provision_enabled():
                    sandbox_provision = _sandbox.provision_env(timeout=self.timeout)
                exec_root = _sandbox.path
                # Surface the sandbox secret-scan advisories (incl. HIGH-severity
                # KEY-MATERIAL private-key hits) so they reach the operator instead
                # of vanishing on tempdir teardown (review F-MED: dead signal).
                for _adv in _sandbox.advisory_findings:
                    _LOGGER.warning("grok secure sandbox advisory: %s", _adv)
            tmpdir = _stack.enter_context(tempfile.TemporaryDirectory(prefix="grok-cli-"))
            tmp = Path(tmpdir)
            prompt_file = tmp / "prompt.md"
            debug_file = tmp / "debug.log"

            # Prompt append precedent: claude_code.py. Schema-emission instruction
            # so grok knows the expected final shape (the explicit-actor suffix is
            # layered in S5 alongside the D4 attribution work).
            full_prompt = _build_grok_prompt(prompt, schema, pinned_model_identity)
            prompt_file.write_text(full_prompt)

            # Lane write-jail prefix (implementation note / adoption C). Empty unless gated in.
            # sandbox-exec becomes the process-group leader; the timeout killpg
            # path still terminates the whole tree.
            jail_prefix = list(kwargs.get("jail_argv_prefix") or [])
            cmd = [
                *jail_prefix,
                grok_bin,
                "--prompt-file",
                str(prompt_file),
                "--cwd",
                str(exec_root),
                "-m",
                effective_model,
                # grok --json-schema takes the schema DOCUMENT inline (the consumer
                # oracle grok-backend-probe.sh cats any file first: `--json-schema
                # "$SCHEMA"`), NOT a path — contrast --prompt-file which IS a path.
                "--json-schema",
                json.dumps(schema),
                "--max-turns",
                str(self.max_turns),
                "--always-approve",
                "--no-plan",
                "--no-subagents",
                "--debug-file",
                str(debug_file),
                *extra_args,
            ]
            if applied_effort:
                cmd.extend(["--reasoning-effort", applied_effort])

            if progress_callback:
                progress_callback(WorkerEventName.EXEC_SPAWNED, backend="grok-cli")

            # Force grok's ZDR trace-upload gate ON via its own env var (found in
            # the grok binary: GROK_ZDR_ENABLED). Defense-in-depth alongside the
            # shallow clone + config opt-out; harmless where the account already
            # gates uploads server-side (upload_reason="zdr_team").
            # Drop macOS malloc-debug ambient vars: they are inherited from
            # operator shells and flood child stderr (~56 lines per run).
            grok_env = dict(env) if env else os.environ.copy()
            for _malloc_key in _MALLOC_DEBUG_ENV_KEYS:
                grok_env.pop(_malloc_key, None)
            grok_env.setdefault("GROK_ZDR_ENABLED", "1")
            try:
                completed = _run_bounded(
                    cmd,
                    env=grok_env,
                    # Run FROM exec_root (the secure sandbox when enabled, else the
                    # worktree) so grok's project-scope config discovery
                    # (./.grok/config.toml) resolves the pinned-model config —
                    # cloned into the sandbox and augmented with the telemetry
                    # opt-out — regardless of whether it keys off --cwd or cwd.
                    cwd=str(exec_root),
                    timeout=self.timeout,
                )
            except subprocess.TimeoutExpired as exc:
                tail = _tail_text(exc.stdout) or _tail_text(exc.stderr)
                raise RuntimeError(f"grok exec timed out after {self.timeout}s.\n{tail}")
            except FileNotFoundError:
                # Disambiguate the three FileNotFoundError causes so the operator
                # is not misdirected (s3-a-007 / s5-a-005): a torn-down worktree
                # cwd, an explicit override path, or a genuinely-absent PATH grok.
                if not Path(exec_root).exists():
                    raise RuntimeError(
                        f"grok exec cwd '{exec_root}' is missing "
                        "(concurrent teardown / sandbox failure?) — not a grok install problem."
                    )
                if self.grok_bin:
                    raise RuntimeError(f"grok binary '{grok_bin}' not found or not executable.")
                raise RuntimeError(f"grok CLI '{grok_bin}' not found in PATH.")

            if completed.returncode != 0:
                stderr_tail = _tail_text(completed.stderr)
                raise RuntimeError(f"grok exec failed (exit {completed.returncode}):\n{stderr_tail}")

            # Replay grok's sandbox commits onto the real lane branch so the pass
            # engine (which reads the worktree branch after execute) sees them.
            # No-op when disabled or when grok made no commit.
            if _sandbox is not None:
                _sandbox.port_commits_back()

            stdout = completed.stdout or ""
            envelope = _parse_envelope(stdout)
            token_usage = normalize_cli_usage(envelope) if envelope else None
            response_model = (envelope.get("model") if envelope else None) or effective_model
            # implementation note S1: extract sessionId from the json envelope (no usage
            # block on grok) so the session-token reader can resolve artifacts.
            session_id = _session_id_from_envelope(envelope)
            session_tokens = (
                read_session_token_deltas(
                    session_id,
                    worktree_path,
                    pre_total=pre_total,
                    pre_session_id=pre_session_id,
                )
                if session_id
                else None
            )
            # implementation note S2: deterministic prompt/output estimates for usage-less
            # backends. total_tokens stays grok_context_delta; input_tokens is
            # never invented. Session artifacts key off the lane worktree cwd.
            token_estimates = _estimate_usage_less_tokens(
                prompt_text=full_prompt,
                session_id=session_id,
                lane_cwd=worktree_path,
            )

            if progress_callback:
                progress_callback(WorkerEventName.EXEC_COMPLETE, backend="grok-cli")

            # Post-turn served-model contamination quarantine only (implementation note S2).
            # Model-pin attestation retired: missing/empty debug logs no longer
            # rewrite a green result to needs_guidance. Authored conversation-
            # module tuples whose base does not match the pin still quarantine.
            contamination = _detect_grok_build_contamination(debug_file, requested_model=effective_model)
            if contamination is not None:
                blocker, evidence = contamination
                return BackendResult(
                    handoff_action="needs_guidance",
                    summary="grok-build contamination detected in debug log",
                    details=_tail_text(stdout),
                    merge_ready=False,
                    blockers=[blocker],
                    token_usage=token_usage,
                    response_model=response_model,
                    reasoning_effort=applied_effort,
                    raw_payload=_with_session_meta(
                        {
                            "stdout_tail": _tail_text(stdout),
                            # Lift the offending debug-log records out of the tempdir
                            # so the violation stays auditable (s5-a-009).
                            "composer_violation_evidence": evidence,
                            "attestation": {
                                "status": "failed",
                                "reason": "grok_build_contamination",
                                "pin": effective_model,
                            },
                        },
                        session_id=session_id,
                        session_tokens=session_tokens,
                        token_estimates=token_estimates,
                    ),
                    sandbox_provision=sandbox_provision,
                )

            payload = _extract_grok_payload(stdout, envelope)
            if payload is None:
                # Fail closed (never silent-empty): the turn produced no parseable
                # result across fenced block, balanced object, and structuredOutput.
                return BackendResult(
                    handoff_action="needs_guidance",
                    summary="grok produced no parseable JSON result",
                    details=_tail_text(stdout),
                    merge_ready=False,
                    blockers=[
                        "grok output unparseable (checked fenced blocks, balanced "
                        "objects, and structuredOutput for a handoff_action result)"
                    ],
                    token_usage=token_usage,
                    response_model=response_model,
                    reasoning_effort=applied_effort,
                    raw_payload=_with_session_meta(
                        {"stdout_tail": _tail_text(stdout)},
                        session_id=session_id,
                        session_tokens=session_tokens,
                        token_estimates=token_estimates,
                    ),
                    sandbox_provision=sandbox_provision,
                )

            result = BackendResult.from_dict(payload)
            # Decouple action validation from selection: preserve summary/tests_run
            # when the payload was parsed-but-unshaped or carries an off-enum/
            # null action; clamp fail-closed so invalid actions never pass as green.
            if handoff_action_needs_clamp(payload):
                blockers = list(result.blockers)
                if "invalid_handoff_action" not in blockers:
                    blockers.append("invalid_handoff_action")
                result = BackendResult(
                    handoff_action="needs_guidance",
                    summary=result.summary,
                    details=result.details,
                    tests_run=list(result.tests_run),
                    blockers=blockers,
                    changed_files=list(result.changed_files),
                    merge_ready=False,
                    token_usage=result.token_usage,
                    response_model=result.response_model,
                    reasoning_effort=result.reasoning_effort,
                    raw_payload=result.raw_payload if isinstance(result.raw_payload, dict) else dict(payload),
                    sandbox_provision=result.sandbox_provision,
                )
            raw_payload = _with_session_meta(
                dict(result.raw_payload) if isinstance(result.raw_payload, dict) else {},
                session_id=session_id,
                session_tokens=session_tokens,
                token_estimates=token_estimates,
            )
            if (
                token_usage
                or response_model is not None
                or applied_effort is not None
                or session_id
                or session_tokens
                or sandbox_provision is not None
            ):
                result = BackendResult(
                    handoff_action=result.handoff_action,
                    summary=result.summary,
                    details=result.details,
                    tests_run=result.tests_run,
                    blockers=result.blockers,
                    changed_files=result.changed_files,
                    merge_ready=result.merge_ready,
                    token_usage=token_usage or result.token_usage,
                    response_model=response_model,
                    reasoning_effort=applied_effort,
                    raw_payload=raw_payload,
                    sandbox_provision=sandbox_provision,
                )
            return result


def _terminate_process_group(proc: "subprocess.Popen[str]") -> None:
    """SIGKILL the child's whole process group (best effort)."""
    try:
        pgid = os.getpgid(proc.pid)
    except (ProcessLookupError, OSError):
        proc.kill()
        return
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
        proc.kill()


def _run_bounded(cmd: list[str], *, env: dict[str, str], cwd: str, timeout: int) -> "subprocess.CompletedProcess[str]":
    """Run ``cmd`` with a wall-clock bound that kills the whole process GROUP.

    ``subprocess.run(timeout=...)`` kills only the direct child on TimeoutExpired;
    grok runs with ``--always-approve`` and spawns tool/shell grandchildren which
    would be re-parented and keep MUTATING the lane worktree after the adapter
    already raised (s3-a-005). Running in a new session (``start_new_session``)
    and ``os.killpg``-ing the group on timeout stops the whole tree.
    """
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd,
        env=env,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _terminate_process_group(proc)
        try:
            stdout, stderr = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        raise subprocess.TimeoutExpired(cmd, timeout, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)


def _tail_text(text: str | bytes | None, limit: int = 500) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text.strip()[-limit:]


def _loads_dict(block: str | None) -> dict[str, Any] | None:
    if not block:
        return None
    try:
        obj = json.loads(block)
    except (json.JSONDecodeError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _decode_json_object_stream(text: str) -> list[dict[str, Any]]:
    """Decode a buffer that may hold one or more concatenated JSON objects.

    grok-remote emits ONE JSON object per turn. When those are appended into a
    single result.json (``{...}{...}{...}``), ``json.loads`` raises Extra data
    and the pass previously treated a complete turn as nonexistent
    (OFFLOAD-RESULT-UNPARSEABLE-HIDES-A-COMPLETE-TURN-PATCH-01). Use
    ``JSONDecoder.raw_decode`` in a loop rather than a single-document loads.

    Semantic (deliberate): callers that need a single payload MUST take the
    **last complete object carrying a non-empty result payload**
    (``handoff_action`` present). Intermediate turn objects are progress or
    envelope noise; the final schema-shaped object is the authoritative worker
    report (matches the narrated-text tier's "LAST schema-shaped wins"). We do
    **not** merge fields across objects — a merge would invent a hybrid no turn
    actually emitted (OBS-04 / CLM-04). When no object carries
    ``handoff_action``, callers fall back to last-complete / envelope scoring.

    Tolerates narration noise around the JSON by skipping non-``{`` prefixes and
    advancing past decode failures to the next ``{``.
    """
    if not text:
        return []
    decoder = json.JSONDecoder()
    objects: list[dict[str, Any]] = []
    i = 0
    n = len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        if text[i] != "{":
            nxt = text.find("{", i)
            if nxt < 0:
                break
            i = nxt
        try:
            obj, end = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        if isinstance(obj, dict):
            objects.append(obj)
        i = end if end > i else i + 1
    return objects


def _last_result_payload(dicts: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Select the authoritative object from a concatenated stream (three tiers).

    Ordered preference (parity with ``remote_agent.sh`` post-classify salvage):

    1. The **last** handoff-shaped object (``handoff_action`` value in
       :data:`KNOWN_HANDOFF_ACTIONS` — key presence alone is not shape).
    2. Else the **last** findings-shaped object (list-valued ``findings``).
    3. Else the **last** object in the stream.

    Returns None only when ``dicts`` is empty. Payload extraction callers that
    need a shaped result still gate on :func:`is_shaped_result_payload`; this
    helper only picks which object is authoritative under multi-object streams.
    """
    if not dicts:
        return None
    for d in reversed(dicts):
        action = d.get(_RESULT_KEY)
        if isinstance(action, str) and action in KNOWN_HANDOFF_ACTIONS:
            return d
    for d in reversed(dicts):
        if isinstance(d.get("findings"), list):
            return d
    return dicts[-1]


def _text_result_dicts(text: str) -> list[dict[str, Any]]:
    """All parseable JSON dict objects reachable in ``text``, best-effort, in order.

    Prefers a raw_decode stream of concatenated top-level objects (multi-turn
    result.json), then scans every fenced code block (grok fences its result
    JSON), then every top-level balanced ``{...}`` in the raw text, then the
    greedy embedded fallback. Critically, a non-JSON fence (e.g. a bash block
    with ``awk '{print $1}'``) no longer short-circuits later candidates
    (s3-a-001).

    Scanner is the shared :func:`_iter_balanced_json_objects` (contract §1): an
    unbalanced ``{`` advances past that brace rather than abandoning the tail
    [REF-26] / [NAME-05] — the private duplicate that abandoned was deleted.
    """
    dicts: list[dict[str, Any]] = []
    # Concatenated multi-object streams first (raw_decode; not brace-balance).
    stream = _decode_json_object_stream(text)
    if stream:
        dicts.extend(stream)
    for body in _FENCE_RE.findall(text):
        for block in _iter_balanced_json_objects(body):
            d = _loads_dict(block)
            if d is not None:
                dicts.append(d)
    for block in _iter_balanced_json_objects(text):
        d = _loads_dict(block)
        if d is not None:
            dicts.append(d)
    embedded = _loads_dict(find_embedded_json_object(text))
    if embedded is not None:
        dicts.append(embedded)
    return dicts


def _build_grok_prompt(prompt: str, schema: dict[str, Any], pinned_model_identity: str) -> str:
    """Compose the grok turn prompt: task prompt + schema-emission + actor-pin suffix.

    Extracted from ``GrokCliAdapter.execute`` (seam separability, implementation note S1) so the
    remote-exec adapter can reuse the exact prompt shaping instead of duplicating it.
    """
    return (
        f"{prompt}\n\n"
        f"IMPORTANT: Your final output must be a single JSON object matching this schema:\n"
        f"{json.dumps(schema, indent=2)}\n\n"
        f"IMPORTANT: When recording WorkBay handoff state, set the write actor to "
        f"'{pinned_model_identity}' (your pinned model identity), not the orchestrator.\n"
    )


def _parse_envelope(stdout: str) -> dict[str, Any] | None:
    """Return grok's ``--output-format json`` envelope dict, tolerating noise.

    Returns the **agent envelope**, never a bare worker payload. Payload
    selection lives in :func:`_extract_grok_payload`, which re-reads the stream
    independently — this parser must not collapse the two selections.

    A clean whole-stdout parse wins. Otherwise decode a concatenated multi-object
    stream via ``raw_decode`` (and balanced-object fallback), then pick the
    HIGHEST-SCORING object by **envelope marker keys only**
    (``sessionId`` / ``usage`` / ``model`` / ``text`` / …), breaking ties toward
    the LAST occurrence. The real envelope carries several markers at once, so a
    stray CLI banner, an earlier narrated fragment with one marker (REV-S1-03), a
    trailing bare ``handoff_action`` result object, or noise after the envelope
    with one generic key (REV2-B-03) all lose to it.

    Bare worker payloads (handoff/findings only) score zero on markers and are
    never preferred over a true envelope when both appear in the stream.
    """
    try:
        parsed = json.loads(stdout)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    candidates: list[dict[str, Any]] = list(_decode_json_object_stream(stdout))
    if not candidates:
        for block in _iter_balanced_json_objects(stdout):
            d = _loads_dict(block)
            if d is not None:
                candidates.append(d)
    # Envelope selection only — never promote a bare worker payload via
    # handoff_action / findings. Payload extraction re-reads the stream.
    first: dict[str, Any] | None = None
    best: dict[str, Any] | None = None
    best_score = 0
    for d in candidates:
        if first is None:
            first = d
        score = sum(1 for marker in _ENVELOPE_MARKERS if marker in d)
        if score and score >= best_score:
            best = d
            best_score = score
    return best if best is not None else first


def _session_id_from_envelope(envelope: dict[str, Any] | None) -> str | None:
    """Extract grok envelope ``sessionId`` (implementation note S1). Never raises."""
    if not isinstance(envelope, dict):
        return None
    for key in ("sessionId", "session_id"):
        raw = envelope.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def _estimate_usage_less_tokens(
    *,
    prompt_text: str,
    session_id: str | None,
    lane_cwd: str | Path,
) -> dict[str, Any]:
    """Deterministic prompt/output estimates for a usage-less backend turn.

    Best-effort: never raises. Output estimate requires a resolvable session
    dir with ``updates.jsonl`` model-output kinds (implementation note S2).
    """
    session_dir = None
    if session_id:
        try:
            session_dir = resolve_session_dir(session_id, lane_cwd)
        except Exception:  # noqa: BLE001 — best-effort; estimates are optional
            session_dir = None
    try:
        return build_token_estimates(prompt_text=prompt_text, session_dir=session_dir)
    except Exception:  # noqa: BLE001 — never break the execute path for telemetry
        return {
            "prompt_tokens": None,
            "prompt_chars": None,
            "prompt_token_source": None,
            "output_tokens": None,
            "output_token_source": None,
        }


def _with_session_meta(
    payload: dict[str, Any],
    *,
    session_id: str | None,
    session_tokens: dict[str, Any] | None,
    token_estimates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach sessionId + session-token reader result onto a raw_payload dict."""
    out = dict(payload)
    if session_id:
        out["session_id"] = session_id
        out["sessionId"] = session_id
    if session_tokens is not None:
        out["grok_session_tokens"] = session_tokens
    if token_estimates is not None:
        out["token_estimates"] = token_estimates
    return out


def _stamp_payload_channel(payload: dict[str, Any], channel: str) -> dict[str, Any]:
    """Return a shallow copy of ``payload`` with the winning extraction channel named.

    Surfaces in ``BackendResult.raw_payload`` via ``from_dict`` so operators can
    see which parse tier produced the outcome (structured vs stream vs text).
    """
    out = dict(payload)
    out["payload_channel"] = channel
    return out


def _shaped_structured_channel(
    envelope: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Prefer ``structuredOutput`` over generic result keys on the envelope.

    Returns ``(payload, channel_name)`` when a shaped structured channel exists;
    ``(None, None)`` otherwise. Generic keys (result/content/output/message) are
    consulted only when structuredOutput is null or unshaped.
    """
    if envelope is None:
        return None, None
    structured = envelope.get("structuredOutput")
    if isinstance(structured, dict) and is_shaped_result_payload(structured):
        return structured, "structuredOutput"
    if isinstance(structured, str):
        # Dedupe concatenated JSON blocks in the structured string channel
        # (last handoff-shaped object wins) before falling through.
        stream_from_structured = _last_result_payload(_decode_json_object_stream(structured))
        if stream_from_structured is not None and is_shaped_result_payload(stream_from_structured):
            return stamp_recovery_tier(stream_from_structured, RECOVERY_TIER_EMBEDDED), "structuredOutput"
        candidate = _loads_dict(structured)
        if candidate is not None and is_shaped_result_payload(candidate):
            return stamp_recovery_tier(candidate, RECOVERY_TIER_EMBEDDED), "structuredOutput"
    for key in ("result", "content", "output", "message"):
        value = envelope.get(key)
        if isinstance(value, dict) and is_shaped_result_payload(value):
            return value, key
    return None, None


def _extract_grok_payload(stdout: str, envelope: dict[str, Any] | None) -> dict[str, Any] | None:
    """Extract the ``BackendResult`` payload from grok output.

    Priority (REAPCONV-ENGINE-MERGEREADY-MISREAD-AS-NEEDS-GUIDANCE-01):

    0. **Shaped structured channels first** on the highest-scoring envelope —
       ``structuredOutput`` beats generic result/content/output/message keys,
       and any shaped structured channel beats a bare top-level stream object.
       Bare stream objects used to short-circuit before structuredOutput was
       consulted, misreading green ``merge_ready`` as ``needs_guidance``.
    1. Concatenated multi-object top-level stream (last handoff-shaped wins)
       only when no envelope carries a shaped structured channel.
    2. Envelope root is itself a shaped bare BackendResult.
    3. Narrated ``text`` channel (Evidence #7 when structured channels are null):
       fenced/balanced scan with last schema-shaped object wins (contract §3).
    4. Parsed-but-unshaped fallthrough (clamp fail-closed at the call site).

    Every tier is shape-validated per SHAPED-PAYLOAD RECOVERY CONTRACT v1 §2
    (``handoff_action`` in known enum OR list-valued ``findings``). Non-strict
    recovery stamps ``shaped_payload_recovery`` and logs a warning (contract §4).
    Returns None only when nothing parseable is found.
    """
    # Stream candidate (may be a bare trailing object). Used only when no
    # shaped structured channel exists on the envelope — never overrides it.
    stream_payload = _last_result_payload(_decode_json_object_stream(stdout))
    stream_shaped = stream_payload is not None and is_shaped_result_payload(stream_payload)

    # 0. Structured channels on the already-parsed (highest-scoring) envelope.
    #    structuredOutput first; generic keys only when structuredOutput is
    #    null/unshaped. Prefer over bare stream objects.
    structured_payload, structured_channel = _shaped_structured_channel(envelope)
    if structured_payload is not None and structured_channel is not None:
        if stream_shaped:
            # Stamp/log: legacy stream short-circuit would have overridden the
            # structured channel (envelope+trailing-bare / bare-then-envelope).
            _LOGGER.warning(
                "stream-shaped payload suppressed in favor of envelope %s channel "
                "(stream handoff_action=%r, structured handoff_action=%r)",
                structured_channel,
                stream_payload.get(_RESULT_KEY) if isinstance(stream_payload, dict) else None,
                structured_payload.get(_RESULT_KEY),
            )
            stamped = _stamp_payload_channel(structured_payload, structured_channel)
            stamped["stream_override_suppressed"] = True
            return stamped
        return _stamp_payload_channel(structured_payload, structured_channel)

    # 1. Bare stream object — only when no envelope carries a shaped structured
    #    channel (single envelope objects that are not themselves shaped still
    #    fall through to envelope-root / text).
    if stream_shaped and stream_payload is not None:
        return _stamp_payload_channel(stream_payload, "stream")

    # 2. The envelope root IS the payload — a bare BackendResult object, exactly
    #    what the S3 prompt suffix demands ("a single JSON object") (harm-001).
    #    Strict path: no recovery stamp beyond the channel name.
    if envelope is not None and is_shaped_result_payload(envelope):
        return _stamp_payload_channel(envelope, "envelope_root")

    # 3. Narrated text channel — only when structured channels are null/unshaped
    #    (Evidence #7). Last schema-shaped object wins among concatenated blocks.
    texts: list[str] = []
    if envelope is not None:
        for key in ("text", "output_text", "content", "message", "result"):
            value = envelope.get(key)
            if isinstance(value, str):
                texts.append(value)
    else:
        texts.append(stdout)
    for text in texts:
        shaped = [d for d in _text_result_dicts(text) if is_shaped_result_payload(d)]
        if shaped:
            # Prefer balanced-tier stamp; embedded greedy is the last entry in
            # _text_result_dicts when it is the only match path for an object.
            # Last-wins is deliberate: earlier conflicting objects lose.
            return _stamp_payload_channel(
                stamp_recovery_tier(shaped[-1], RECOVERY_TIER_BALANCED),
                "text",
            )

    # 4. Parsed-but-unshaped fallthrough (off-enum / null / absent action).
    return recover_unshaped_payload(
        texts,
        text_dicts_fn=_text_result_dicts,
        envelope=envelope,
        loads_dict_fn=_loads_dict,
    )
