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
import os
import re
import signal
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ..grok_lane_config import DEFAULT_GROK_MODEL
from ._result_text import (
    find_embedded_json_object,
    find_first_balanced_json,
    normalize_cli_usage,
)
from .grok_session_tokens import read_cumulative_total, read_session_token_deltas

#: Pinned-model guarantee: grok-build must never author task work. Any resolved
#: ``-m`` model carrying a build spelling is refused pre-exec (decision #2799).
_GROK_BUILD_TOKEN = "grok-build"

#: Tolerant build-token matcher: catches ``grok-build``, ``grok_build``,
#: ``grok4-build`` and build variants regardless of separator/casing so the
#: pre-exec guard does not fail OPEN on an alternate spelling (s3-a-009).
_GROK_BUILD_RE = re.compile(r"grok[\w]*[-_]?build", re.IGNORECASE)

#: A ``grok_args`` value that re-pins the model at the CLI level would bypass the
#: pre-exec guard (last-wins parsing), so any model-override flag in the caller's
#: extra args is refused (s3-a-004).
_MODEL_OVERRIDE_RE = re.compile(r"(^|\s)(-m|--model)(\s|=|$)|(^|\s)-c\s*[\"']?[\w.]*model", re.IGNORECASE)

#: Reasoning-effort tiers grok declares (REQUEST A1). Anything else is dropped
#: from argv rather than passed through to a fail-fast at exec.
_VALID_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})

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
#: build spelling so format drift cannot silently pass a contaminated log
#: (s5-a-004).
_GROK_BUILD_ITEM_RE = re.compile(r"model_id[\s:=\"']*grok[\w]*[-_]?build", re.IGNORECASE)

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


def count_grok_build_items(debug_text: str) -> int:
    """Count grok-build-authored AssistantItems in a grok debug log (pure)."""
    return len(_GROK_BUILD_ITEM_RE.findall(debug_text))


def _grok_build_evidence(debug_text: str, limit: int = 20) -> list[str]:
    """Return the contaminated log lines so the violation stays auditable.

    The debug log lives in a TemporaryDirectory that is torn down as soon as
    ``execute`` returns, so the offending records are lifted into the result
    (s5-a-009) rather than being destroyed with the tempdir.
    """
    hits = [line.strip() for line in debug_text.splitlines() if _GROK_BUILD_ITEM_RE.search(line)]
    return hits[:limit]


def _detect_grok_build_contamination(debug_file: Path) -> tuple[str, list[str]] | None:
    """Post-turn grok-build contamination quarantine only (implementation note S2).

    Model-pin attestation (missing/empty debug log, format drift, foreign
    model markers) is retired. This backstop only fails when the debug log
    shows grok-build authored AssistantItems — the cheaper auto-routed model
    threat that still warrants quarantine ([OBS-08]). Missing/empty logs are
    not contamination. Returns ``(blocker, evidence_lines)`` or ``None``.
    """
    if not debug_file.is_file():
        return None
    text = debug_file.read_text(errors="replace")
    if not text.strip():
        return None
    count = count_grok_build_items(text)
    if count <= 0:
        return None
    return (
        f"grok-build authored {count} AssistantItem(s) — contamination quarantine",
        _grok_build_evidence(text),
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
    contamination = _detect_grok_build_contamination(debug_file)
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


def find_grok(explicit_path: str | None = None) -> str:
    """Find the grok CLI executable (explicit override > PATH)."""
    if explicit_path:
        return explicit_path
    res = subprocess.run(["which", "grok"], capture_output=True, text=True)
    if res.returncode == 0 and res.stdout.strip():
        return res.stdout.strip()
    raise RuntimeError("grok CLI not found in PATH. Install it or provide --grok-bin.")


class GrokCliAdapter(BackendAdapter):
    supports_jail = True

    """Execution adapter for the ``grok`` CLI headless turn."""

    def __init__(
        self,
        grok_bin: str | None = None,
        grok_args: list[str] | None = None,
        *,
        timeout: int = 900,
        max_turns: int = 30,
    ):
        # Resolve the binary LAZILY (in execute), not in the ctor: an eager
        # find_grok here raises RuntimeError when grok is absent, and the daemon
        # constructs the adapter OUTSIDE its EXEC_FAILED try/except, so an
        # unresolved binary would crash the whole worker process instead of
        # logging a failed cycle (s4-a-001). Contrast claude-code, which also
        # defers binary resolution to execute().
        self.grok_bin = grok_bin
        self.grok_args = grok_args or []
        self.timeout = timeout
        self.max_turns = max_turns

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

        with tempfile.TemporaryDirectory(prefix="grok-cli-") as tmpdir:
            tmp = Path(tmpdir)
            prompt_file = tmp / "prompt.md"
            debug_file = tmp / "debug.log"

            # Prompt append precedent: claude_code.py. Schema-emission instruction
            # so grok knows the expected final shape (the explicit-actor suffix is
            # layered in S5 alongside the D4 attribution work).
            full_prompt = (
                f"{prompt}\n\n"
                f"IMPORTANT: Your final output must be a single JSON object matching this schema:\n"
                f"{json.dumps(schema, indent=2)}\n\n"
                f"IMPORTANT: When recording WorkBay handoff state, set the write actor to "
                f"'{pinned_model_identity}' (your pinned model identity), not the orchestrator.\n"
            )
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
                str(worktree_path),
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

            try:
                completed = _run_bounded(
                    cmd,
                    env=env or os.environ.copy(),
                    # Run FROM the worktree so grok's project-scope config
                    # discovery (./.grok/config.toml) resolves the materialized
                    # pinned-model config regardless of whether it keys off the
                    # --cwd flag or the process cwd.
                    cwd=str(worktree_path),
                    timeout=self.timeout,
                )
            except subprocess.TimeoutExpired as exc:
                tail = _tail_text(exc.stdout) or _tail_text(exc.stderr)
                raise RuntimeError(f"grok exec timed out after {self.timeout}s.\n{tail}")
            except FileNotFoundError:
                # Disambiguate the three FileNotFoundError causes so the operator
                # is not misdirected (s3-a-007 / s5-a-005): a torn-down worktree
                # cwd, an explicit override path, or a genuinely-absent PATH grok.
                if not Path(worktree_path).exists():
                    raise RuntimeError(
                        f"grok lane worktree '{worktree_path}' is missing "
                        "(concurrent teardown?) — not a grok install problem."
                    )
                if self.grok_bin:
                    raise RuntimeError(f"grok binary '{grok_bin}' not found or not executable.")
                raise RuntimeError(f"grok CLI '{grok_bin}' not found in PATH.")

            if completed.returncode != 0:
                stderr_tail = _tail_text(completed.stderr)
                raise RuntimeError(f"grok exec failed (exit {completed.returncode}):\n{stderr_tail}")

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

            if progress_callback:
                progress_callback(WorkerEventName.EXEC_COMPLETE, backend="grok-cli")

            # Post-turn grok-build contamination quarantine only (implementation note S2).
            # Model-pin attestation retired: missing/empty/format-drift debug
            # logs no longer rewrite a green result to needs_guidance. Real
            # grok-build AssistantItems still quarantine ([OBS-08]).
            contamination = _detect_grok_build_contamination(debug_file)
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
                    ),
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
                    ),
                )

            result = BackendResult.from_dict(payload)
            raw_payload = _with_session_meta(
                dict(result.raw_payload) if isinstance(result.raw_payload, dict) else {},
                session_id=session_id,
                session_tokens=session_tokens,
            )
            if token_usage or response_model is not None or applied_effort is not None or session_id or session_tokens:
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


def _iter_balanced_objects(text: str) -> "list[str]":
    """Yield each top-level brace-balanced ``{...}`` substring of ``text`` in order."""
    blocks: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        start = text.find("{", i)
        if start == -1:
            break
        block = find_first_balanced_json(text[start:])
        if block is None:
            break
        blocks.append(block)
        i = start + len(block)
    return blocks


def _text_result_dicts(text: str) -> list[dict[str, Any]]:
    """All parseable JSON dict objects reachable in ``text``, best-effort, in order.

    Scans every fenced code block first (grok fences its result JSON), then every
    top-level balanced ``{...}`` in the raw text, then the greedy embedded
    fallback. Critically, a non-JSON fence (e.g. a bash block with
    ``awk '{print $1}'``) no longer short-circuits later candidates (s3-a-001).
    """
    dicts: list[dict[str, Any]] = []
    for body in _FENCE_RE.findall(text):
        for block in _iter_balanced_objects(body):
            d = _loads_dict(block)
            if d is not None:
                dicts.append(d)
    for block in _iter_balanced_objects(text):
        d = _loads_dict(block)
        if d is not None:
            dicts.append(d)
    embedded = _loads_dict(find_embedded_json_object(text))
    if embedded is not None:
        dicts.append(embedded)
    return dicts


def _parse_envelope(stdout: str) -> dict[str, Any] | None:
    """Return grok's ``--output-format json`` envelope dict, tolerating noise.

    A clean whole-stdout parse wins. Otherwise scan every balanced object and
    prefer the HIGHEST-SCORING one — score = marker-key hits, with the result
    key weighted — breaking ties toward the LAST occurrence. The real envelope
    carries several markers at once (``text`` + ``sessionId`` + ...), so a
    stray CLI banner around the JSON, an earlier narrated fragment with one
    marker key (REV-S1-03), or trailing noise after the envelope with one
    generic key (REV2-B-03) all lose to it.
    """
    try:
        parsed = json.loads(stdout)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    first: dict[str, Any] | None = None
    best: dict[str, Any] | None = None
    best_score = 0
    for block in _iter_balanced_objects(stdout):
        d = _loads_dict(block)
        if d is None:
            continue
        if first is None:
            first = d
        score = (2 if _RESULT_KEY in d else 0) + sum(1 for marker in _ENVELOPE_MARKERS if marker in d)
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


def _with_session_meta(
    payload: dict[str, Any],
    *,
    session_id: str | None,
    session_tokens: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach sessionId + session-token reader result onto a raw_payload dict."""
    out = dict(payload)
    if session_id:
        out["session_id"] = session_id
        out["sessionId"] = session_id
    if session_tokens is not None:
        out["grok_session_tokens"] = session_tokens
    return out


def _extract_grok_payload(stdout: str, envelope: dict[str, Any] | None) -> dict[str, Any] | None:
    """Extract the ``BackendResult`` payload from grok output.

    Grok priority (implementation note D2, Evidence #7 inverts the usual order): a bare
    result object, then the narrated ``text`` channel (fenced/balanced), then
    dict-valued envelope fields, then the ``structuredOutput`` channel (often
    null). Every tier is shape-validated on ``handoff_action`` so a narrated
    ``{...}`` fragment is never mistaken for the result (s3-a-002); within the
    narrated text the LAST schema-shaped object wins because the prompt demands
    the FINAL output be the result. Returns None when nothing parses — the
    caller fails closed.
    """
    # 1. The envelope root IS the payload — a bare BackendResult object, exactly
    #    what the S3 prompt suffix demands ("a single JSON object") (harm-001).
    if envelope is not None and _RESULT_KEY in envelope:
        return envelope

    # 2. Narrated text channel (grok primary, Evidence #7).
    texts: list[str] = []
    if envelope is not None:
        for key in ("text", "output_text", "content", "message", "result"):
            value = envelope.get(key)
            if isinstance(value, str):
                texts.append(value)
    else:
        texts.append(stdout)
    for text in texts:
        shaped = [d for d in _text_result_dicts(text) if _RESULT_KEY in d]
        if shaped:
            return shaped[-1]

    # 3. Dict-valued envelope fields (harm-001), then structuredOutput fallback.
    if envelope is not None:
        for key in ("result", "content", "output", "message"):
            value = envelope.get(key)
            if isinstance(value, dict) and _RESULT_KEY in value:
                return value
        structured = envelope.get("structuredOutput")
        if isinstance(structured, dict) and _RESULT_KEY in structured:
            return structured
        if isinstance(structured, str):
            candidate = _loads_dict(structured)
            if candidate is not None and _RESULT_KEY in candidate:
                return candidate
    return None
