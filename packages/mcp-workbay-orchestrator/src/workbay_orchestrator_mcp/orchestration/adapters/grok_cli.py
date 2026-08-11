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

#: A grok-build-authored debug-log item — the pinned-model contamination marker
#: (spike Evidence #5: `--debug-file` line `AssistantItem model_id=grok-build`).
#: Tolerant of the separator (``=``/``:``/space), surrounding quotes, casing, and
#: build spelling (incl. dotted versioned builds) so format drift cannot silently
#: pass a contaminated log (s5-a-004).
_GROK_BUILD_ITEM_RE = re.compile(r"model_id[\s:=\"']*grok[\w.\-]*build", re.IGNORECASE)

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


#: Remote VM bootstrap emits exactly one AssistantItem with bare
#: ``model_id=grok-build`` and this fixed ``model_fingerprint`` before any
#: worker turn. Byte-identical across independent lanes; not task authorship.
_REMOTE_BOOTSTRAP_MODEL_ID = "grok-build"
_REMOTE_BOOTSTRAP_MODEL_FINGERPRINT = "fp_36bb860c5ab2a013"
_REMOTE_BOOTSTRAP_FINGERPRINT_RE = re.compile(
    rf"model_fingerprint[\s:=\"']*{re.escape(_REMOTE_BOOTSTRAP_MODEL_FINGERPRINT)}",
    re.IGNORECASE,
)


def _is_benign_cli_build_resolution(requested_model: str, model_id: str) -> bool:
    """True when ``model_id`` is the CLI's build-spelling resolution of ``requested_model``.

    The grok CLI resolves a pin like ``grok-4.5`` to a build spelling such as
    ``grok-4.5-build`` in debug-log ``model_id`` fields. That is not contamination.
    A bare ``grok-build`` (base ``grok``) is never a resolution of a versioned pin.
    """
    req = str(requested_model or "").strip().lower()
    mid = str(model_id or "").strip().lower()
    if not req or not mid:
        return False
    if not _GROK_BUILD_RE.search(mid):
        return False
    # Strip the build token and any trailing build-family suffix; compare bases.
    matched = re.match(r"^(.*?)(?:[-_]?build)(?:[-_.].*)?$", mid, re.IGNORECASE)
    if matched is None:
        return False
    base = matched.group(1).rstrip("-_")
    return bool(base) and base == req


#: Worker-authored debug-log markers. The remote pre-first-turn bootstrap
#: AssistantItem (fixed fingerprint ``fp_36bb860c5ab2a013``) is a bare
#: ``model_id=grok-build`` record with none of these fields; real contaminated
#: work items carry at least one. Used only to exempt the bootstrap shape —
#: never to raise the global contamination threshold ([OBS-08]).
_WORKER_BUILD_ITEM_MARKERS_RE = re.compile(
    r"reasoning[_-]?effort|tool_calls|stopReason|stop_reason|"
    r"\"content\"|output_text|parent_tool|message_id",
    re.IGNORECASE,
)

#: Bootstrap emission shape: a bare AssistantItem carrying model_id=grok-build.
#: Non-AssistantItem bare lines (e.g. other record kinds) are NOT exempt —
#: they count as contamination (REV-ADAPTERS-EXEMPTION-WIDER-THAN-BOOTSTRAP-SILENT-01).
_BOOTSTRAP_ASSISTANT_ITEM_RE = re.compile(r"\bAssistantItem\b", re.IGNORECASE)


def _is_remote_bootstrap_grok_build_item(line: str, model_id: str) -> bool:
    """True for the known remote pre-first-turn bootstrap AssistantItem shape.

    Narrow exemption only: bare ``grok-build`` (not versioned / not
    ``grok-build-fast``) on an ``AssistantItem`` record that carries the pinned
    remote bootstrap ``model_fingerprint`` and no worker-payload markers.
    Non-AssistantItem bare lines, bare items without the pinned fingerprint
    (fail closed on fingerprint drift), and real contaminated items with worker
    markers all still count. Does not raise on malformed input ([RES-07]
    fail-closed: return False so the item is counted).
    """
    mid = str(model_id or "").strip().lower()
    if mid != _REMOTE_BOOTSTRAP_MODEL_ID:
        return False
    text = line if isinstance(line, str) else ""
    if not text.strip():
        # Empty/unresolved line cannot prove bootstrap shape ([RES-07] safe side).
        return False
    if _BOOTSTRAP_ASSISTANT_ITEM_RE.search(text) is None:
        return False
    if _REMOTE_BOOTSTRAP_FINGERPRINT_RE.search(text) is None:
        # A bare grok-build item without the pinned bootstrap fingerprint is
        # contamination, not bootstrap (fail closed on fingerprint drift).
        return False
    try:
        bare = _WORKER_BUILD_ITEM_MARKERS_RE.search(text) is None
    except Exception:  # noqa: BLE001 — fail closed: item still counts ([RES-07])
        return False
    if bare:
        _LOGGER.debug(
            "exempting remote bootstrap grok-build AssistantItem line: %s",
            text.strip()[:200],
        )
    return bare


def _line_for_match(text: str, match: re.Match[str]) -> str:
    """Return the single line of ``text`` that contains ``match`` (never raises)."""
    try:
        start = text.rfind("\n", 0, match.start()) + 1
        end = text.find("\n", match.end())
        return text[start:] if end < 0 else text[start:end]
    except (TypeError, ValueError, AttributeError):
        return ""


def count_grok_build_items(debug_text: str, requested_model: str | None = None) -> int:
    """Count grok-build-authored AssistantItems in a grok debug log (pure).

    When ``requested_model`` is provided, CLI resolution of that model to its
    corresponding build spelling is not counted (not contamination). The known
    remote bootstrap bare-``grok-build`` AssistantItem (pinned
    ``model_fingerprint``, no worker-payload markers) is also not counted —
    that single pre-turn item is not authored work; a bare grok-build without
    the pinned fingerprint still counts (fail closed on fingerprint drift).
    Line-resolution failure fails closed: the item is counted ([RES-07]).
    """
    count = 0
    text = debug_text if isinstance(debug_text, str) else ""
    for match in _MODEL_ID_VALUE_RE.finditer(text):
        mid = match.group(1)
        if not _GROK_BUILD_RE.search(mid):
            continue
        if requested_model and _is_benign_cli_build_resolution(requested_model, mid):
            continue
        line = _line_for_match(text, match)
        # RES-07: empty line-resolution must never exempt a real item via the
        # bootstrap check running on "" (REV-ADAPTERS-LINEFAIL-EXEMPTS-REAL-ITEM-01).
        if not line:
            count += 1
            continue
        if _is_remote_bootstrap_grok_build_item(line, mid):
            continue
        count += 1
    return count


def _grok_build_evidence(debug_text: str, requested_model: str | None = None, limit: int = 20) -> list[str]:
    """Return the contaminated log lines so the violation stays auditable.

    The debug log lives in a TemporaryDirectory that is torn down as soon as
    ``execute`` returns, so the offending records are lifted into the result
    (s5-a-009) rather than being destroyed with the tempdir.
    """
    hits = [line.strip() for line in debug_text.splitlines() if count_grok_build_items(line, requested_model) > 0]
    return hits[:limit]


def _detect_grok_build_contamination(
    debug_file: Path,
    requested_model: str | None = None,
) -> tuple[str, list[str]] | None:
    """Post-turn grok-build contamination quarantine only (implementation note S2).

    Model-pin attestation (missing/empty debug log, format drift, foreign
    model markers) is retired. This backstop only fails when the debug log
    shows grok-build authored AssistantItems — the cheaper auto-routed model
    threat that still warrants quarantine ([OBS-08]). CLI resolution of
    ``requested_model`` to its build spelling is not contamination.
    Missing/empty logs are not contamination. Returns
    ``(blocker, evidence_lines)`` or ``None``.
    """
    if not debug_file.is_file():
        return None
    text = debug_file.read_text(errors="replace")
    if not text.strip():
        return None
    count = count_grok_build_items(text, requested_model)
    if count <= 0:
        return None
    return (
        f"grok-build authored {count} AssistantItem(s) — contamination quarantine",
        _grok_build_evidence(text, requested_model),
    )


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
        if _GROK_BUILD_RE.search(effective_model) or effective_model != DEFAULT_GROK_MODEL:
            raise RuntimeError(
                f"Refusing to dispatch grok with model '{effective_model}': allowed is "
                f"the configured pin '{DEFAULT_GROK_MODEL}' (WORKBAY_GROK_MODEL); "
                "grok-build family is refused (decision #2799)."
            )
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

            # Post-turn grok-build contamination quarantine only (implementation note S2).
            # Model-pin attestation retired: missing/empty/format-drift debug
            # logs no longer rewrite a green result to needs_guidance. Real
            # grok-build AssistantItems still quarantine ([OBS-08]). CLI
            # resolution of the requested pin to its build spelling is not
            # contamination.
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
