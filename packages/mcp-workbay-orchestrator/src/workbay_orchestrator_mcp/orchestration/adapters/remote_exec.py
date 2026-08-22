"""Remote-exec backend adapter (implementation note).

Ships each grok worker turn to the remote OCI VM (``WORKBAY_REMOTE_GATE_HOST``)
via ``scripts/remote_agent.sh`` and lands the resulting commit on the LOCAL lane.
Turn shaping + reasoning-effort resolution are delegated to :class:`GrokCliAdapter`
(the local grok port); only the transport is overridden.

Fetch-back is **patch-based, not ``git fetch``**: the VM sandbox is remote-severed
(0 remotes) by design, so ``remote_agent.sh`` returns a ``git format-patch`` on
``--out`` and grok's structured result JSON on ``--result-out``. This adapter
``git apply --index``-es the patch and commits it locally with the offload-engine
identity, so ``offload_pass._commits_since_start`` sees a real local commit and
``_worktree_dirty`` reports clean afterward (no spurious ``_checkpoint_if_dirty``).
No grok/LLM authorship lands in git history — the local commit is engine-authored.

MVP session mode is fresh_turn only; ``shared_lane`` continuity across remote turns
is a documented non-goal (harden-later).
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

from ..backend_adapter import BackendAdapter, BackendResult
from ..backend_registry import BackendSpec
from ..backend_spec import REMOTE_EFFORTS as _REMOTE_EFFORTS
from ..backend_spec import build_agent_spec, write_agent_spec
from ..codex_lane_config import (
    LANE_SANDBOX,
    LANE_WRITABLE_ROOTS,
    WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT,
    WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT,
)
from ..commit_subject import (
    VENDOR_CREDIT_TOKENS as VENDOR_CREDIT_TOKENS,
)
from ..commit_subject import (
    build_remote_turn_commit_message as build_remote_turn_commit_message,
)
from ..commit_subject import (
    read_patch_text as read_patch_text,
)
from ..commit_subject import (
    sanitize_lane_id_for_commit_message as sanitize_lane_id_for_commit_message,
)
from ..grok_lane_config import ENGINE_GIT_IDENTITY as _ENGINE_GIT_IDENTITY
from ..grok_lane_config import retired_model_warning
from ._result_text import (
    RECOVERY_TIER_BALANCED,
    RECOVERY_TIER_UNSHAPED,
    SHAPED_PAYLOAD_RECOVERY_KEY,
    handoff_action_needs_clamp,
    is_shaped_result_payload,
    recover_unshaped_payload,
    select_last_shaped_payload,
    stamp_recovery_tier,
)
from .git_control_paths import (
    patch_touches_git_control_paths as patch_touches_git_control_paths,
)
from .grok_cli import (
    _GROK_BUILD_RE,
    _build_grok_prompt,
    _detect_grok_build_contamination,
    _extract_grok_payload,
    _loads_dict,
    _parse_envelope,
    _tail_text,
    _text_result_dicts,
    _worktree_branch,
)

#: Paths touched by a ``git format-patch`` / ``git apply``-able unified diff.
#: Used by ``_changed_files_from_patch`` (b-side, unquoted ``a/`` / ``b/`` only).
_DIFF_GIT_PATH_RE = re.compile(r"^diff --git a/(.+?) b/(.+)$", re.MULTILINE)

#: Cap on remote-controlled --result-out file size before parse [RES-05].
_RESULT_FILE_MAX_BYTES = 5 * 1024 * 1024

logger = logging.getLogger(__name__)

#: Debug logs are remote-controlled too, but they are the only host-side carrier
#: of per-lane token usage, so an oversized log is tail-read instead of skipped.
_DEBUG_LOG_MAX_BYTES = 64 * 1024 * 1024
_DEBUG_LOG_TAIL_BYTES = 8 * 1024 * 1024

#: Upper plausibility bound for any count scraped from the debug log.
#:
#: The lower bound (no negatives, no bools) was never the whole job: the remote
#: writes this file, Python ints are unbounded, and an integral float reaches
#: ~1.8e308, so magnitude was entirely remote-controlled. That is not merely a
#: cosmetic bad number. The scraped total reaches ``run_ctx.cumulative_tokens``
#: and both offload-pass budget checks are ``cumulative_tokens >= token_budget``
#: with no capability guard, so a single forged large count halts the lane with
#: ``token_budget_exceeded`` -- the measured party choosing when its own
#: governor fires [CARD-11]. A trillion is far above any real session rollup and
#: far below the int64 the handoff ``total_tokens`` column can store.
_MAX_PLAUSIBLE_COUNT = 1_000_000_000_000

#: Miss reasons that are ordinary rather than suspicious. An absent or empty log
#: is the normal shape for a backend that writes none; the rest mean the log was
#: there and the harvest still came away with nothing, which is worth a warning.
_QUIET_MISS_REASONS = frozenset({"log_absent", "log_empty"})

#: The xai gateway logs each response body as raw JSON after this marker.
_GATEWAY_RESPONSE_MARKER = 'received "session/prompt" response: '

#: Token fields shared by the per-call and cumulative usage records.
_USAGE_TOKEN_FIELDS = (
    "inputTokens",
    "outputTokens",
    "totalTokens",
    "cachedReadTokens",
    "reasoningTokens",
)

#: Gateway field -> the snake_case breakdown key used by ``normalize_cli_usage``.
#: The downstream promoter (``worker_daemon._maybe_record_result_token_telemetry``)
#: only recognizes usage through ``{last,total}.total_tokens``, so a record that
#: carries the gateway's own camelCase names and nothing else is invisible to it
#: and the harvest would stamp a field nothing reads.
_USAGE_BREAKDOWN_KEYS = {
    "inputTokens": "input_tokens",
    "outputTokens": "output_tokens",
    "totalTokens": "total_tokens",
    "cachedReadTokens": "cached_input_tokens",
    "reasoningTokens": "reasoning_output_tokens",
}

#: remote_agent.sh exit codes (see the script header).
# Effort allow-list: ``_REMOTE_EFFORTS`` imported from backend_spec (implementation note S2).
_RC_PATCH = 0
_RC_HARD_FAIL = 1  # security posture tripwire or fatal in-sandbox setup (not transport)
_RC_USAGE = 2
_RC_GROK_FAILED = 3
_RC_NO_CHANGES = 4
_RC_RESULT_DEGRADED = 5  # implementation note D7 — structured result unusable; non-retryable
_RC_AUTH_FAILED = 6  # implementation note D7 — auth_match; non-retryable flock abort
_RC_POLICY_REFUSED = 7  # implementation note D7 — policy/placeholder refuse; non-retryable
# Wall-clock bound expired on a *live*, reachable VM (remote_agent.sh exit 8).
# Retryable/resumable — not transport; salvage patch is applied without commit
# so the offload checkpoint arm can preserve work (REMOTEEXEC-EXIT8…).
_RC_BOUND_EXPIRED = 8
_RC_ADMISSION_DEFERRED = (
    75  # VM admission (mem floor, lane cap, residual timeout, or same-branch lane lock); retryable (S3b/S5)
)
_RC_HOST_UNCONFIGURED = 78

# ---------------------------------------------------------------------------
# implementation note S1.4 — phase-timing merge (adapter half)
# ---------------------------------------------------------------------------
#
# Ownership: the script fetches ``.grok-phases.json``; this module merges.
# No ssh/scp/fetch logic here. Fail-open on every malformed/absent input.
# Totals are measured, never derived from their parts (S1.1).

_MARKER_LINE = "remote_agent: remote_body_start"
_DEFAULT_MARKER_TIMEOUT_SEC = 120
_UNACCOUNTED_SKEW_TOLERANCE_S = 2
_SAME_CLOCK_BOUND = 0.05

_HOST_OWNED_PHASE_NAMES = frozenset({"pre_spawn", "transport", "host_stage", "ssh_connect"})
_VM_PASSTHROUGH_KEYS = frozenset({"schema_version", "vm_setup", "vm_span", "phases", "warm_skip", "partial"})
# Defence-in-depth parity with scripts/remote_agent.sh:_emit_phases_record, which
# pops the same five host-owned keys before writing the VM record (REVSEAM-0192-S1-03).
_BANNED_VM_TOP_LEVEL = frozenset({"setup", "wall_seconds", "completeness_class", "host_span", "unaccounted"})
_EXPECTED_VM_SCHEMA_VERSION = 1
_PHASE_LINE_RE = re.compile(r"^remote_agent:\s+phase\s+(\S+)\s+(.*)$")
_KV_RE = re.compile(r"(\w+)=(\S+)")
# Bound unfiltered unknown host phase names before they land on the envelope
# (OBS-02). Dedup is exact-message on the reader; the envelope keeps a capped,
# distinct-name list only.
_UNKNOWN_HOST_PHASE_NAME_CAP = 16
_UNKNOWN_HOST_PHASE_NAME_MAX_LEN = 64
_UNKNOWN_HOST_PHASE_NAME_RE = re.compile(r"phase-instrument degrade unknown_host_phase name=(\S+)")


def _int_unix_now() -> int:
    """Integer Unix seconds. Host stamps are quantized before any subtraction."""
    return int(time.time())


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass
class ConcurrentStderrState:
    """Filled by the concurrent stderr reader while the remote-agent child runs."""

    lines: list[str] = field(default_factory=list)
    marker_seen_ts: int | None = None
    ssh_call_ts: int | None = None
    ssh_return_ts: int | None = None
    structured_phases: dict[str, dict[str, Any]] = field(default_factory=dict)
    marker_timeout_announced: bool = False
    degrade_lines: list[str] = field(default_factory=list)
    # C-08: reader failure must not collapse into a silent marker_absent.
    reader_error: str | None = None


@dataclass
class PhaseHostObservations:
    """Host-side stamps and structured host phases for one dispatch merge."""

    spawn_ts: int
    exit_ts: int
    decision_ts: int | None = None
    marker_seen_ts: int | None = None
    marker_absent: bool = False
    ssh_call_ts: int | None = None
    ssh_return_ts: int | None = None
    host_phases: dict[str, dict[str, Any]] = field(default_factory=dict)
    stderr_text: str = ""
    degrade_lines: list[str] = field(default_factory=list)
    concurrent_reader_used: bool = False


def _parse_structured_phase_line(line: str) -> tuple[str, dict[str, Any]] | None:
    """Parse script-emitted ``remote_agent: phase …`` lines. Fail-open to None."""
    m = _PHASE_LINE_RE.match(line.strip())
    if not m:
        return None
    name = m.group(1)
    rest = m.group(2)
    kvs = {k: v for k, v in _KV_RE.findall(rest)}
    if name in ("ssh_call_ts", "ssh_return_ts"):
        ts = _safe_int(kvs.get("ts"))
        if ts is None:
            return None
        return name, {"ts": ts}
    start = _safe_int(kvs.get("start_ts"))
    end = _safe_int(kvs.get("end_ts"))
    if start is None or end is None:
        return None
    dur = _safe_int(kvs.get("duration_s"))
    if dur is None:
        dur = end - start
    return name, {
        "side": "host",
        "start_ts": start,
        "end_ts": end,
        "duration_s": dur,
    }


def _marker_timeout_sec(env: dict[str, str] | None) -> int:
    raw = None
    if env is not None:
        raw = env.get("WORKBAY_REMOTE_AGENT_MARKER_TIMEOUT_SEC")
    if raw is None:
        raw = os.environ.get("WORKBAY_REMOTE_AGENT_MARKER_TIMEOUT_SEC")
    n = _safe_int(raw)
    if n is None or n <= 0:
        return _DEFAULT_MARKER_TIMEOUT_SEC
    return n


def _handle_stderr_line(
    line: str,
    state: ConcurrentStderrState,
    *,
    marker_timeout_sec: int,
) -> None:
    """Consume one stderr line into *state* (tee semantics — never filters)."""
    state.lines.append(line)
    # Whole-line match only (C-09): Lane A emits the token as an exact line.
    # Substring matches (set -x traces, quoted errors) must not stamp the marker.
    stripped = line.rstrip("\n\r")
    if state.marker_seen_ts is None and stripped.rstrip() == _MARKER_LINE:
        # Stamp at the moment the matching line is read — never at process exit.
        state.marker_seen_ts = _int_unix_now()
    parsed = _parse_structured_phase_line(stripped)
    if parsed is None:
        return
    name, payload = parsed
    if name == "ssh_call_ts":
        state.ssh_call_ts = int(payload["ts"])
        return
    if name == "ssh_return_ts":
        state.ssh_return_ts = int(payload["ts"])
        return
    if name in _HOST_OWNED_PHASE_NAMES:
        state.structured_phases[name] = payload
        return
    # REVSEAM-0192-S1-01: a clean phase line with a name outside the host-owned
    # set must not vanish silently — operator sees which phase was dropped, and
    # a future producer (e.g. venv_seed) no longer lands only as host_unattributed.
    msg = f"remote_agent: phase-instrument degrade unknown_host_phase name={name}"
    if msg not in state.degrade_lines:
        state.degrade_lines.append(msg)
        try:
            print(msg, file=sys.stderr)
        except Exception:  # noqa: BLE001
            pass


def _maybe_announce_marker_timeout(
    state: ConcurrentStderrState,
    *,
    marker_timeout_sec: int,
) -> None:
    """AGT-10: one stderr degrade line on marker timeout; never kills the child."""
    if state.marker_timeout_announced or state.marker_seen_ts is not None:
        return
    if state.ssh_call_ts is None:
        return
    now = _int_unix_now()
    if now - state.ssh_call_ts < marker_timeout_sec:
        return
    state.marker_timeout_announced = True
    msg = (
        f"remote_agent: phase-instrument degrade marker_absent "
        f"reason=marker_timeout_sec={marker_timeout_sec} ssh_call_ts={state.ssh_call_ts}"
    )
    state.degrade_lines.append(msg)
    try:
        print(msg, file=sys.stderr)
    except Exception:  # noqa: BLE001 — fail-open
        pass


def _record_reader_failure(state: ConcurrentStderrState, exc: BaseException) -> None:
    """Surface reader death as a named degrade (C-08), never kill the dispatch."""
    name = type(exc).__name__
    detail = f"{name}: {exc}"
    state.reader_error = detail
    msg = f"remote_agent: phase-instrument degrade reader_failed reason={name}"
    if msg not in state.degrade_lines:
        state.degrade_lines.append(msg)
    try:
        print(msg, file=sys.stderr)
    except Exception:  # noqa: BLE001 — fail-open
        pass


def _stderr_reader_thread(
    stream: Any,
    state: ConcurrentStderrState,
    *,
    marker_timeout_sec: int,
    done: threading.Event,
) -> None:
    """Line-oriented concurrent reader: drains while the child runs (tee)."""
    try:
        if stream is None:
            return

        # Blocking readline is fine: the child closing stderr unblocks us.
        # A side timer watches the marker timeout without killing the child.
        def _timeout_watch() -> None:
            while not done.wait(0.25):
                _maybe_announce_marker_timeout(state, marker_timeout_sec=marker_timeout_sec)
            _maybe_announce_marker_timeout(state, marker_timeout_sec=marker_timeout_sec)

        watcher = threading.Thread(
            target=_timeout_watch,
            name="remote-exec-marker-timeout",
            daemon=True,
        )
        watcher.start()
        while True:
            line = stream.readline()
            if line == "" or line is None:
                break
            _handle_stderr_line(line, state, marker_timeout_sec=marker_timeout_sec)
    except Exception as exc:  # noqa: BLE001 — fail-open: never kill the dispatch
        _record_reader_failure(state, exc)
    finally:
        _maybe_announce_marker_timeout(state, marker_timeout_sec=marker_timeout_sec)


def _finalize_stream_reader(
    *,
    stream: Any,
    reader: threading.Thread,
    state: ConcurrentStderrState,
    join_timeout: float = 2.0,
) -> None:
    """Join the stderr owner; close the stream if the reader outlives the window (C-08)."""
    reader.join(timeout=join_timeout)
    if reader.is_alive():
        try:
            if stream is not None:
                stream.close()
        except Exception:  # noqa: BLE001
            pass
        reader.join(timeout=1.0)
        if reader.is_alive() and state.reader_error is None:
            # Secondary-drain miss: name it so it cannot look like clean EOF.
            state.reader_error = "reader_join_timeout"
            msg = "remote_agent: phase-instrument degrade reader_failed reason=reader_join_timeout"
            if msg not in state.degrade_lines:
                state.degrade_lines.append(msg)
            try:
                print(msg, file=sys.stderr)
            except Exception:  # noqa: BLE001
                pass


_APPLY_STDERR_BOUND = 500
_APPLY_STDERR_TRUNCATION_MARKER = "...[truncated]..."


def _bounded_stderr_tail(text: str, *, limit: int = _APPLY_STDERR_BOUND) -> str:
    """Return a bounded stderr tail; mark only when the head was actually dropped."""
    cleaned = (text or "").strip()
    if not cleaned:
        return "no stderr"
    if len(cleaned) > limit:
        return f"{_APPLY_STDERR_TRUNCATION_MARKER}{cleaned[-limit:]}"
    return cleaned


def _default_remote_runner(
    cmd: list[str],
    *,
    cwd: str,
    env: dict[str, str] | None,
    timeout: float,
) -> "subprocess.CompletedProcess[str]":
    """Run ``remote_agent.sh`` bounded with a concurrent stderr tee.

    Injectable seam: tests pass a fake runner (via the ``remote_runner`` execute
    kwarg) that writes canned --out/--result-out files and returns an exit code,
    so no SSH/VM is touched.

    ``stdin=DEVNULL`` is load-bearing (implementation note): the orchestrator MCP server's
    own stdin is the JSON-RPC stdio pipe — a non-tty, never-EOF fd. Without this,
    ``remote_agent.sh`` inherits it and its step-1 ``git push`` (git's default ssh)
    blocks reading it forever, consuming the whole timeout budget with no VM
    sandbox and 0 grok output (root cause, decision 4134). A ``/dev/null`` (EOF)
    stdin makes the identical dispatch complete normally.

    implementation note S1.1/S1.4: stderr is drained line-oriented *while* the process
    runs so ``marker_seen_ts`` is the host observation instant of
    ``remote_agent: remote_body_start``, not process exit. Structured host-phase
    lines (``transport``, ``host_stage``, ``ssh_call_ts``, ``ssh_return_ts``)
    are parsed on the same reader. The accumulated text is identical to a
    post-hoc capture (tee, not filter). Host observations are attached as
    ``completed.phase_host_state`` for the merge path.

    C-01 (security): stderr has exactly one consumer — the reader thread. The
    main thread waits on the child and drains stdout on a separate thread so
    ``Popen.communicate`` never races the tee (dual-consumer drop of the
    sandbox-not-remote-severed tripwire).
    """
    marker_timeout_sec = _marker_timeout_sec(env)
    state = ConcurrentStderrState()
    done = threading.Event()
    try:
        proc = subprocess.Popen(  # noqa: S603
            cmd,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception:
        # Fail-open to the historical buffered path if Popen itself fails oddly.
        completed = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        completed.phase_host_state = state  # type: ignore[attr-defined]
        completed.concurrent_reader_used = False  # type: ignore[attr-defined]
        return completed

    reader = threading.Thread(
        target=_stderr_reader_thread,
        args=(proc.stderr, state),
        kwargs={"marker_timeout_sec": marker_timeout_sec, "done": done},
        name="remote-exec-stderr-reader",
        daemon=True,
    )
    reader.start()

    # Sole stdout owner (main-side thread). communicate() is intentionally not
    # used: it would also register stderr=PIPE and dual-drain with the reader.
    stdout_parts: list[str] = []

    def _read_stdout() -> None:
        try:
            if proc.stdout is not None:
                data = proc.stdout.read()
                if data:
                    stdout_parts.append(data)
        except Exception:  # noqa: BLE001 — partial stdout is still returned
            pass

    stdout_reader = threading.Thread(
        target=_read_stdout,
        name="remote-exec-stdout-reader",
        daemon=True,
    )
    stdout_reader.start()

    timed_out = False
    try:
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
            try:
                proc.wait(timeout=5)
            except Exception:  # noqa: BLE001
                pass
    finally:
        # C-08: always signal done and join readers, including non-timeout errors.
        done.set()
        stdout_reader.join(timeout=2)
        _finalize_stream_reader(stream=proc.stderr, reader=reader, state=state)
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception:  # noqa: BLE001
            pass

    stdout_text = "".join(stdout_parts)
    stderr_text = "".join(state.lines)
    if timed_out:
        # C-10: re-raise with partial stdout collected by the stdout owner thread
        # (mirrors subprocess.run TimeoutExpired.output shape).
        raise subprocess.TimeoutExpired(
            cmd,
            timeout,
            output=stdout_text,
            stderr=stderr_text,
        ) from None

    # Final timeout check after child exit (marker may never have arrived).
    _maybe_announce_marker_timeout(state, marker_timeout_sec=marker_timeout_sec)
    completed = subprocess.CompletedProcess(
        cmd,
        proc.returncode if proc.returncode is not None else -1,
        stdout_text or "",
        stderr_text,
    )
    completed.phase_host_state = state  # type: ignore[attr-defined]
    completed.concurrent_reader_used = True  # type: ignore[attr-defined]
    return completed


def _phase_dict(
    *,
    side: str,
    start_ts: int,
    end_ts: int,
    duration_s: int | None = None,
) -> dict[str, Any]:
    if duration_s is None:
        duration_s = end_ts - start_ts
    return {
        "side": side,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "duration_s": int(duration_s),
    }


def _sum_side_phase_durations(phases: dict[str, Any], *, side: str) -> int | None:
    """Sum ``duration_s`` for phases with matching ``side``. None on bad input."""
    if not isinstance(phases, dict):
        return None
    total = 0
    for _name, body in phases.items():
        if not isinstance(body, dict):
            return None
        body_side = body.get("side")
        if body_side is not None and body_side != side:
            continue
        # When side is absent, infer: host-owned names are host; else treat as VM
        # only when the caller asked for that side and the name matches ownership.
        if body_side is None:
            if side == "host" and _name not in _HOST_OWNED_PHASE_NAMES:
                continue
            if side == "vm" and _name in _HOST_OWNED_PHASE_NAMES:
                continue
        dur = _safe_int(body.get("duration_s"))
        if dur is None:
            start = _safe_int(body.get("start_ts"))
            end = _safe_int(body.get("end_ts"))
            if start is None or end is None:
                return None
            dur = end - start
        total += dur
    return total


def _phase_nonmonotonic(phases: dict[str, Any]) -> bool:
    if not isinstance(phases, dict):
        return False
    for body in phases.values():
        if not isinstance(body, dict):
            continue
        start = _safe_int(body.get("start_ts"))
        end = _safe_int(body.get("end_ts"))
        dur = _safe_int(body.get("duration_s"))
        if start is not None and end is not None and end < start:
            return True
        if dur is not None and dur < 0:
            return True
    return False


def _validate_vm_record(record: Any) -> str | None:
    """Return a mode-4 degrade reason, or None if the VM half is usable.

    Reason tokens name the refusal cause (OBS-08). Structural corruption stays
    ``truncated_or_invalid_json``; policy/schema refusals use distinct tokens so
    operators do not debug scp/JSON when the record is well-formed but rejected.
    """
    if not isinstance(record, dict):
        return "truncated_or_invalid_json"
    for banned in _BANNED_VM_TOP_LEVEL:
        if banned in record:
            return "banned_host_owned_key"
    for key in _VM_PASSTHROUGH_KEYS:
        if key not in record:
            return "missing_required_key"
    # REVSEAM-0192-S1-02: schema_version is not just required-present — refuse
    # any version the consumer does not understand (unit / meaning drift).
    if _safe_int(record.get("schema_version")) != _EXPECTED_VM_SCHEMA_VERSION:
        return "schema_version_unsupported"
    phases = record.get("phases")
    if not isinstance(phases, dict):
        return "truncated_or_invalid_json"
    # Collision: VM must not author host-owned phase names (S1.4 phases merge rule).
    for name in phases:
        if name in _HOST_OWNED_PHASE_NAMES:
            return "host_owned_phase_name"
    for name, body in phases.items():
        if not isinstance(body, dict):
            return "truncated_or_invalid_json"
        for field_name in ("start_ts", "end_ts", "duration_s"):
            if field_name in body and _safe_int(body.get(field_name)) is None:
                return "truncated_or_invalid_json"
    if _safe_int(record.get("vm_span")) is None:
        return "truncated_or_invalid_json"
    if _safe_int(record.get("vm_setup")) is None:
        return "truncated_or_invalid_json"
    return None


def _load_vm_phases_file(path: Path | None) -> tuple[dict[str, Any] | None, str | None, bool]:
    """Load local fetched phases file.

    Returns ``(record, degrade_reason, present)``.
    ``present`` is True when the path exists and is non-empty (scp "succeeded"
    from the adapter's local view). Fail-open: never raises.

    When ``present`` is False, ``degrade_reason`` names the concrete absence
    (C-05): ``phases_path_none``, ``phases_file_missing``, ``phases_file_empty``,
    rather than collapsing every miss into scp-nonzero.
    """
    if path is None:
        return None, "phases_path_none", False
    try:
        p = Path(path)
        if not p.is_file():
            return None, "phases_file_missing", False
        size = p.stat().st_size
        if size <= 0:
            return None, "phases_file_empty", False
        if size > _RESULT_FILE_MAX_BYTES:
            return None, "truncated_or_invalid_json", True
        text = p.read_text(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return None, "unparseable_json", False
    if not text.strip():
        return None, "phases_file_empty", False
    try:
        data = json.loads(text)
    except Exception:  # noqa: BLE001
        return None, "unparseable_json", True
    reason = _validate_vm_record(data)
    if reason is not None:
        return None, reason, True
    assert isinstance(data, dict)
    return data, None, True


def _exit_class_is_pre_materialize(*, rc: int, stderr_text: str) -> bool:
    """True for admission / pre-materialize residual / lock / occupancy defers."""
    stderr_l = (stderr_text or "").lower()
    if rc == _RC_HOST_UNCONFIGURED:
        return True
    if rc != _RC_ADMISSION_DEFERRED:
        return False
    # Post-materialize setup-overrun is the exception on exit 75.
    if "in-sandbox setup" in stderr_l or "setup overrun" in stderr_l:
        return False
    return True


def classify_vm_half_absence(
    *,
    rc: int,
    stderr_text: str,
    phases_present: bool,
    parse_reason: str | None,
    phases_flag_state: str | None = None,
) -> tuple[bool, str | None]:
    """Normative if/elif partition (S1.4). Owner is the adapter alone.

    Detection order (C-04/C-05 — named causes, not a single scp collapse):
      0. script_unreadable / phases_flag_omitted (flag never shipped)
      1. pre_materialize_miss
      2. phases_file_empty / phases_file_missing / timeout_phases_absent /
         fetch_scp_failure
      3. unparseable_json
      4. truncated_or_invalid_json

    ``phases_flag_state`` is ``"shipped"``, ``"omitted"``, ``"script_unreadable"``,
    or None (legacy callers — fall back to the coarser scp limb).
    """
    scp_nonzero = not phases_present
    pre = _exit_class_is_pre_materialize(rc=rc, stderr_text=stderr_text)

    # Capability / probe limbs first — never mislabel as scp failure (C-04).
    if scp_nonzero and phases_flag_state == "script_unreadable":
        return True, "script_unreadable"
    if scp_nonzero and phases_flag_state == "omitted":
        return True, "phases_flag_omitted"

    if pre and scp_nonzero:
        return True, "pre_materialize_miss"

    if scp_nonzero and not pre:
        # Distinct no-VM-half causes (C-05). Do not re-derive every miss as scp.
        # Timeout-shaped rc wins over file-detail so (rc=-1, missing) is not
        # collapsed into the same bucket as (rc=0, missing).
        if rc == -1:
            return True, "timeout_phases_absent"
        if parse_reason == "phases_file_empty":
            return True, "phases_file_empty"
        if parse_reason == "phases_file_missing":
            return True, "phases_file_missing"
        if parse_reason == "phases_path_none":
            # Path never bound (flag path) — still not scp when flag state unknown.
            return True, "phases_path_none"
        return True, "fetch_scp_failure"

    # Present limb: any non-None parse_reason means the VM half is unusable.
    # Total over R3 policy tokens (schema_version_unsupported, banned_host_owned_key,
    # missing_required_key, host_owned_phase_name) as well as the JSON-shape tokens.
    # Call sites that hard-code phases_present=False never reach this limb today;
    # keep it total so a future present-and-invalid path cannot invert to usable.
    if parse_reason is not None:
        return True, parse_reason
    return False, None


def _instrument_catchall_envelope(
    *,
    spawn_ts: int,
    exit_ts: int,
    exc: BaseException | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Fail-open degrade envelope for instrument exceptions (C-06, C-07).

    Omits ``wall_seconds`` rather than writing a falsified 0 beside unequal
    stamps. Surfaces a human-readable reason on the envelope and stderr.
    """
    if reason is None:
        if exc is not None:
            reason = f"instrument_exception:{type(exc).__name__}"
        else:
            reason = "instrument_exception"
    msg = f"remote_agent: phase-instrument degrade vm_half_absent reason={reason}"
    try:
        print(msg, file=sys.stderr)
    except Exception:  # noqa: BLE001
        pass
    return {
        "marker_absent": True,
        "vm_half_absent": True,
        "spawn_ts": int(spawn_ts),
        "exit_ts": int(exit_ts),
        # wall_seconds intentionally omitted (C-06): 0 next to unequal stamps
        # is a synthesized measurement, not a degrade omission.
        "completeness_class": "bounds_not_evaluated",
        "vm_half_absent_reason": reason,
    }


def _evaluate_completeness_class(
    *,
    marker_absent: bool,
    vm_half_absent: bool,
    host_unattributed: int | None,
    host_span: int | None,
    vm_unattributed: int | None,
    vm_span: int | None,
    unaccounted: int | None,
    nonmonotonic: bool,
) -> str:
    """Total assignment map (S1.1). Every input yields exactly one enum value."""
    if marker_absent or vm_half_absent:
        return "bounds_not_evaluated"
    if nonmonotonic:
        return "hard_reject"
    # Ordinary path: evaluate same-clock ratios + skew band.
    if host_span is None or vm_span is None:
        return "hard_reject"
    if host_span == 0 or vm_span == 0:
        return "hard_reject"
    if host_unattributed is None or vm_unattributed is None or unaccounted is None:
        return "hard_reject"
    if host_unattributed < 0 or vm_unattributed < 0:
        return "hard_reject"
    if host_unattributed / host_span > _SAME_CLOCK_BOUND:
        return "hard_reject"
    if vm_unattributed / vm_span > _SAME_CLOCK_BOUND:
        return "hard_reject"
    if abs(unaccounted) > _UNACCOUNTED_SKEW_TOLERANCE_S:
        return "hard_reject"
    return "ordinary"


def merge_phase_envelope(
    *,
    vm_record: dict[str, Any] | None,
    host: PhaseHostObservations,
    vm_half_absent: bool,
    vm_half_reason: str | None = None,
) -> dict[str, Any]:
    """Merge VM record + host observations into one envelope (OBS-02).

    Always writes ``completeness_class``. Never raises. Derives host-side residual
    keys only when ``(marker_absent, vm_half_absent) == (false, false)``. Derives
    ``vm_unattributed`` whenever ``vm_half_absent`` is false (including the
    marker-absent path — REV0192R12-A-1).
    """
    try:
        return _merge_phase_envelope_impl(
            vm_record=vm_record,
            host=host,
            vm_half_absent=vm_half_absent,
            vm_half_reason=vm_half_reason,
        )
    except Exception as exc:  # noqa: BLE001 — fail-open hard degrade
        return _instrument_catchall_envelope(
            spawn_ts=int(getattr(host, "spawn_ts", 0) or 0),
            exit_ts=int(getattr(host, "exit_ts", 0) or 0),
            exc=exc,
        )


def _merge_phase_envelope_impl(
    *,
    vm_record: dict[str, Any] | None,
    host: PhaseHostObservations,
    vm_half_absent: bool,
    vm_half_reason: str | None = None,
) -> dict[str, Any]:
    spawn_ts = int(host.spawn_ts)
    exit_ts = int(host.exit_ts)
    decision_ts = host.decision_ts
    marker_absent = bool(host.marker_absent) or host.marker_seen_ts is None
    # C-02: wall_seconds is exit_ts − decision_ts. When decision_ts is absent do
    # NOT silently open the wall at spawn — that yields an ordinary-looking
    # under-count (OBS-08). Omit wall_seconds and name the gap.
    decision_ts_absent = decision_ts is None
    wall_seconds: int | None
    if decision_ts is not None:
        wall_seconds = exit_ts - int(decision_ts)
    else:
        wall_seconds = None

    host_phases: dict[str, dict[str, Any]] = {}
    # pre_spawn from this cycle's decision_ts (never adapter entry / pass-level).
    if decision_ts is not None:
        host_phases["pre_spawn"] = _phase_dict(
            side="host",
            start_ts=int(decision_ts),
            end_ts=spawn_ts,
        )
    for name, body in (host.host_phases or {}).items():
        if name in _HOST_OWNED_PHASE_NAMES and name != "pre_spawn" and isinstance(body, dict):
            host_phases[name] = {
                "side": "host",
                "start_ts": _safe_int(body.get("start_ts")),
                "end_ts": _safe_int(body.get("end_ts")),
                "duration_s": _safe_int(body.get("duration_s")),
            }
            # Drop incomplete host phase entries rather than emit nulls.
            if any(host_phases[name][k] is None for k in ("start_ts", "end_ts", "duration_s")):
                host_phases.pop(name, None)

    # ssh_connect from script ssh_call_ts → adapter marker_seen_ts (literal only).
    if not marker_absent and host.ssh_call_ts is not None and host.marker_seen_ts is not None:
        host_phases["ssh_connect"] = _phase_dict(
            side="host",
            start_ts=int(host.ssh_call_ts),
            end_ts=int(host.marker_seen_ts),
        )

    envelope: dict[str, Any] = {
        "marker_absent": marker_absent,
        "vm_half_absent": bool(vm_half_absent),
        "spawn_ts": spawn_ts,
        "exit_ts": exit_ts,
    }
    if wall_seconds is not None:
        envelope["wall_seconds"] = wall_seconds
    if decision_ts_absent:
        envelope["decision_ts_absent"] = True

    # Host-stamped spans that depend on the marker split.
    # When the marker arrived, s14 (false,true) and s11 (false,false) both require
    # remote_body_wall + host_tail_span. Prefer the script-emitted ssh_return_ts;
    # if the return stamp was missed by the reader (C-01 residual), fall back to
    # exit_ts so host_tail_span is 0 and the required keys are still present
    # rather than silently omitted (REV0192S1-C-11, REV0192S1-C-12). Name the
    # assumption on the envelope (OBS-08 / REV0192R2G-LOCAL-01): never report an
    # assumed host tail as a measured zero that can reach completeness ordinary.
    remote_body_wall: int | None = None
    host_tail_span: int | None = None
    ssh_return_ts_absent = False
    if not marker_absent and host.marker_seen_ts is not None:
        if host.ssh_return_ts is not None:
            return_ts = int(host.ssh_return_ts)
        else:
            return_ts = exit_ts
            ssh_return_ts_absent = True
            envelope["ssh_return_ts_absent"] = True
        remote_body_wall = return_ts - int(host.marker_seen_ts)
        host_tail_span = exit_ts - return_ts
        envelope["remote_body_wall"] = remote_body_wall
        envelope["host_tail_span"] = host_tail_span

    if vm_half_absent:
        # Host-only degrade envelope (s14-vm-half-absent-required). No VM
        # pass-through, no banned residual synthesis.
        if host_phases:
            envelope["phases"] = dict(host_phases)
        envelope["completeness_class"] = "bounds_not_evaluated"
        if vm_half_reason:
            envelope["vm_half_absent_reason"] = vm_half_reason
        return envelope

    # VM half claimed present but record missing — degrade rather than raise.
    if vm_record is None:
        envelope["vm_half_absent"] = True
        envelope["completeness_class"] = "bounds_not_evaluated"
        if host_phases:
            envelope["phases"] = dict(host_phases)
        return envelope

    # VM half present — pass through VM keys (phases is a merge object).
    for key in ("schema_version", "vm_setup", "vm_span", "warm_skip", "partial"):
        envelope[key] = vm_record[key]
    vm_phases = dict(vm_record.get("phases") or {})
    # Merge order: start from VM phases, then host.update wins on name collision.
    # That is intentional — host is authoritative for host-owned names. Safety does
    # NOT come from merge order; it comes from _validate_vm_record rejecting any
    # VM record whose phases already contain a host-owned name (REVSEAM-0192-S1-04).
    # Do not relax that load-time check while relying on this merge.
    merged_phases = dict(vm_phases)
    merged_phases.update(host_phases)
    envelope["phases"] = merged_phases

    vm_span = _safe_int(vm_record.get("vm_span"))
    vm_setup = _safe_int(vm_record.get("vm_setup"))
    nonmonotonic = _phase_nonmonotonic(merged_phases)

    # vm_unattributed whenever vm_half_absent is false (incl. marker_absent).
    vm_phase_sum = _sum_side_phase_durations(merged_phases, side="vm")
    vm_unattributed: int | None = None
    if vm_span is not None and vm_phase_sum is not None:
        vm_unattributed = vm_span - vm_phase_sum
        envelope["vm_unattributed"] = vm_unattributed

    host_span: int | None = None
    setup: int | None = None
    unaccounted: int | None = None
    host_unattributed: int | None = None

    if decision_ts_absent:
        # C-02: without decision_ts the wall is undefined — never claim ordinary and
        # never synthesize host residual keys that depend on wall_seconds. But a
        # wall-independent rejection still outranks "not evaluated" (REV0192R34-LOCAL-02).
        # Do NOT call _evaluate_completeness_class here: all-None spans force hard_reject
        # even when phases are clean (over-reject). nonmonotonic is the only wall-independent
        # signal safe to consult on this path.
        envelope["completeness_class"] = "hard_reject" if nonmonotonic else "bounds_not_evaluated"
        return envelope

    if not marker_absent:
        # Host residual arithmetic only on ordinary joint row (false, false).
        assert wall_seconds is not None
        if remote_body_wall is not None:
            host_span = wall_seconds - remote_body_wall
            envelope["host_span"] = host_span
        if host_span is not None and host_tail_span is not None and vm_setup is not None:
            pre_spawn = 0
            if "pre_spawn" in host_phases:
                pre_spawn = int(host_phases["pre_spawn"]["duration_s"])
            # setup = (host_span − host_tail_span − pre_spawn) + vm_setup
            setup = (host_span - host_tail_span - pre_spawn) + vm_setup
            envelope["setup"] = setup
        if host_span is not None and vm_span is not None:
            unaccounted = wall_seconds - (host_span + vm_span)
            envelope["unaccounted"] = unaccounted  # raw; never clamp
        if host_span is not None and host_tail_span is not None:
            host_sum = _sum_side_phase_durations(merged_phases, side="host")
            if host_sum is not None:
                host_unattributed = host_span - host_tail_span - host_sum
                envelope["host_unattributed"] = host_unattributed

        # Evaluate first so hard_reject keeps precedence over an assumed tail
        # (REV0192R3-LOCAL-01). Downgrade only the optimistic ordinary verdict.
        cls = _evaluate_completeness_class(
            marker_absent=False,
            vm_half_absent=False,
            host_unattributed=host_unattributed,
            host_span=host_span,
            vm_unattributed=vm_unattributed,
            vm_span=vm_span,
            unaccounted=unaccounted,
            nonmonotonic=nonmonotonic,
        )
        if ssh_return_ts_absent and cls == "ordinary":
            # Assumed host tail must not evaluate as ordinary (same class as
            # decision_ts_absent: residual keys may still be present).
            cls = "bounds_not_evaluated"
        envelope["completeness_class"] = cls
    else:
        # (true, false): marker-absent required set; no host residual keys.
        envelope["completeness_class"] = "bounds_not_evaluated"

    return envelope


def build_host_observations_from_run(
    *,
    spawn_ts: int,
    exit_ts: int,
    decision_ts: int | None,
    completed: "subprocess.CompletedProcess[str]",
    marker_timeout_sec: int | None = None,
) -> PhaseHostObservations:
    """Fold concurrent-reader state (or post-hoc stderr parse) into observations."""
    state: ConcurrentStderrState | None = getattr(completed, "phase_host_state", None)
    concurrent = bool(getattr(completed, "concurrent_reader_used", False))
    stderr_text = completed.stderr or ""
    if state is None:
        # Fake runner / buffered path: parse text only (marker_seen_ts unknown).
        state = ConcurrentStderrState()
        for line in stderr_text.splitlines(keepends=True):
            _handle_stderr_line(line, state, marker_timeout_sec=marker_timeout_sec or _DEFAULT_MARKER_TIMEOUT_SEC)
        # Without a concurrent reader we cannot honestly stamp marker_seen_ts
        # at arrival — leave it None so marker_absent degrades (OBS-08) unless
        # tests inject observations directly.
        if not concurrent:
            state.marker_seen_ts = None

    marker_absent = state.marker_seen_ts is None
    # If the reader announced timeout, marker is absent even if a late line arrives
    # after the reporting decision (timeout is reporting-only).
    if state.marker_timeout_announced:
        marker_absent = True
        state.marker_seen_ts = None

    host_phases = {
        k: v for k, v in state.structured_phases.items() if k in _HOST_OWNED_PHASE_NAMES and k != "pre_spawn"
    }
    return PhaseHostObservations(
        spawn_ts=spawn_ts,
        exit_ts=exit_ts,
        decision_ts=decision_ts,
        marker_seen_ts=None if marker_absent else state.marker_seen_ts,
        marker_absent=marker_absent,
        ssh_call_ts=state.ssh_call_ts,
        ssh_return_ts=state.ssh_return_ts,
        host_phases=host_phases,
        stderr_text=stderr_text,
        degrade_lines=list(state.degrade_lines),
        concurrent_reader_used=concurrent,
    )


def build_phase_timing_for_dispatch(
    *,
    phases_path: Path | None,
    spawn_ts: int,
    exit_ts: int,
    decision_ts: int | None,
    completed: "subprocess.CompletedProcess[str]",
    marker_timeout_sec: int | None = None,
    phases_flag_state: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """End-to-end fail-open merge for one dispatch. Returns (envelope, degrade_lines).

    ``phases_flag_state`` is ``"shipped"`` / ``"omitted"`` / ``"script_unreadable"``
    so absence classification can name probe outcomes (C-04) instead of scp.
    """
    degrade_lines: list[str] = []
    try:
        host = build_host_observations_from_run(
            spawn_ts=spawn_ts,
            exit_ts=exit_ts,
            decision_ts=decision_ts,
            completed=completed,
            marker_timeout_sec=marker_timeout_sec,
        )
        degrade_lines.extend(host.degrade_lines)
        vm_record, parse_reason, present = _load_vm_phases_file(phases_path)
        stderr_text = host.stderr_text or (completed.stderr or "")
        rc = int(completed.returncode if completed.returncode is not None else 0)

        if not present:
            vm_half_absent, reason = classify_vm_half_absence(
                rc=rc,
                stderr_text=stderr_text,
                phases_present=False,
                parse_reason=parse_reason,
                phases_flag_state=phases_flag_state,
            )
            vm_record = None
        elif parse_reason is not None:
            vm_half_absent, reason = True, parse_reason
            vm_record = None
        else:
            vm_half_absent, reason = False, None

        if vm_half_absent and reason:
            line = f"remote_agent: phase-instrument degrade vm_half_absent reason={reason}"
            if line not in degrade_lines:
                degrade_lines.append(line)
                try:
                    print(line, file=sys.stderr)
                except Exception:  # noqa: BLE001
                    pass
        if host.marker_absent and not any("marker_absent" in d for d in degrade_lines):
            # Marker absent without an earlier timeout announce (e.g. process
            # exited before timeout with no marker line).
            timeout = marker_timeout_sec or _DEFAULT_MARKER_TIMEOUT_SEC
            ts = host.ssh_call_ts if host.ssh_call_ts is not None else 0
            line = (
                f"remote_agent: phase-instrument degrade marker_absent "
                f"reason=marker_timeout_sec={timeout} ssh_call_ts={ts}"
            )
            degrade_lines.append(line)
            try:
                print(line, file=sys.stderr)
            except Exception:  # noqa: BLE001
                pass

        envelope = merge_phase_envelope(
            vm_record=vm_record if not vm_half_absent else None,
            host=host,
            vm_half_absent=vm_half_absent,
            vm_half_reason=reason,
        )
        return envelope, degrade_lines
    except Exception as exc:  # noqa: BLE001
        env = _instrument_catchall_envelope(
            spawn_ts=spawn_ts,
            exit_ts=exit_ts,
            exc=exc,
        )
        reason = env.get("vm_half_absent_reason") or "instrument_exception"
        line = f"remote_agent: phase-instrument degrade vm_half_absent reason={reason}"
        if line not in degrade_lines:
            degrade_lines.append(line)
        return env, degrade_lines


def _stamp_phase_degrades_on_envelope(
    envelope: dict[str, Any],
    degrade_lines: list[str] | None,
) -> None:
    """Copy host-phase degrade tokens onto the envelope (OBS-02).

    ``build_phase_timing_for_dispatch`` returns degrade lines as its second
    element; execute-path callers must stamp them before attach so a consumer of
    ``raw_payload["phase_timing"]`` sees the same named drops the process log
    already carries. Bounds the distinct-name set and truncates each name so an
    unbounded flood of unique ``\\S+`` phase tokens cannot grow without limit on
    the structured record.
    """
    if not degrade_lines:
        return
    names: list[str] = []
    seen: set[str] = set()
    for line in degrade_lines:
        m = _UNKNOWN_HOST_PHASE_NAME_RE.search(line)
        if m is None:
            continue
        name = m.group(1)[:_UNKNOWN_HOST_PHASE_NAME_MAX_LEN]
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
        if len(names) >= _UNKNOWN_HOST_PHASE_NAME_CAP:
            break
    if not names:
        return
    envelope["unknown_host_phase"] = True
    envelope["unknown_host_phase_names"] = names


def _attach_phase_timing(result: BackendResult, envelope: dict[str, Any] | None) -> BackendResult:
    """Stamp the merged phase envelope onto raw_payload (OBS-02 / OBS-08)."""
    if envelope is None:
        return result
    payload = dict(result.raw_payload or {})
    payload["phase_timing"] = envelope
    return BackendResult(
        handoff_action=result.handoff_action,
        summary=result.summary,
        details=result.details,
        tests_run=list(result.tests_run),
        blockers=list(result.blockers),
        changed_files=list(result.changed_files),
        merge_ready=result.merge_ready,
        token_usage=result.token_usage,
        response_model=result.response_model,
        reasoning_effort=result.reasoning_effort,
        raw_payload=payload,
        sandbox_provision=result.sandbox_provision,
        off_box_self_verify=result.off_box_self_verify,
    )


def _resolve_script(worktree_path: Path) -> Path:
    script = Path(worktree_path) / "scripts" / "remote_agent.sh"
    if not script.is_file():
        raise RuntimeError(
            f"grok-remote transport missing: {script} not found (needs scripts/remote_agent.sh in the lane worktree)."
        )
    return script


def _local_dirty(worktree_path: Path) -> bool:
    """True when the lane worktree has uncommitted changes. Fail-closed: a non-zero
    git status raises (never read as clean)."""
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"git status failed in {worktree_path} (rc={res.returncode}): {res.stderr.strip()[-200:] or 'no stderr'}"
        )
    return bool(res.stdout.strip())


def _committed_files(worktree_path: Path) -> list[str]:
    """Paths touched by HEAD (the just-applied engine commit) — the authoritative
    changed-file list, preferred over grok's self-report."""
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "show", "--name-only", "--format=", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        return []
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]


def _changed_files_from_patch(patch_file: Path) -> list[str]:
    """Derive changed paths from a turn.patch when structured result is unusable.

    Load-bearing for OFFLOAD-RESULT-UNPARSEABLE-HIDES-A-COMPLETE-TURN-PATCH-01:
    exit 5 / unparseable result must not report ``changed_files=[]`` while an
    applyable patch sits on disk — that asserts "no work" and discards green
    lanes. Paths come from ``diff --git a/… b/…`` headers (b-side), preserving
    order and de-duplicating. Empty/missing/unreadable → [].
    """
    if not patch_file.is_file():
        return []
    try:
        text = patch_file.read_text(errors="replace")
    except OSError:
        return []
    if not text.strip():
        return []
    files: list[str] = []
    seen: set[str] = set()
    for match in _DIFF_GIT_PATH_RE.finditer(text):
        path = match.group(2).strip()
        if not path or path == "/dev/null" or path in seen:
            continue
        seen.add(path)
        files.append(path)
    return files


# Re-exported from commit_subject so existing tests keep importing from here.
# VENDOR_CREDIT_TOKENS / build_remote_turn_commit_message / sanitize_lane_id_for_commit_message
# are the public surface; the screen itself lives in the shared leaf module.


class PatchUnreadableError(RuntimeError):
    """Host I/O failed while reading the turn patch — not a control-path plant."""


def _apply_and_commit(worktree_path: Path, patch_file: Path, *, lane_id: str, summary: str) -> None:
    """``git apply --index`` the returned patch, then ONE engine-identity commit.

    Collapses the remote sandbox commits into a single local commit authored by
    the offload engine (no remote sandbox authorship in git history).

    Fail-closed (RES-13 crumple zone / RES-04 cleanup-on-throw): ``git apply`` is
    atomic — a failed apply applies nothing, leaving the lane untouched. But if the
    apply succeeds and the *commit* then fails (e.g. empty index or identity
    missing), the tree would be left staged/dirty, breaking the offload pass's
    clean-HEAD invariant (``_worktree_dirty`` / ``_commits_since_start``). So on
    commit failure we reverse the just-applied patch (``git apply -R --index`` — the
    exact inverse of a clean apply) before raising, restoring the pre-turn HEAD.
    Either way the caller sees a raise with no partial commit; recovery is a re-run.

    Engine commit is hook-neutralized (``-c core.hooksPath=/dev/null`` and
    ``--no-verify``): a lane-poisoned ``scripts/hooks/git/pre-commit`` must not
    execute on the operator host. Control-path patches are quarantined *before*
    apply (fail-closed; nothing is written).

    Patch text is read *before* apply so a decode/read failure cannot leave a
    staged tree (the clean-HEAD invariant). Decode is defensive (UTF-8 replace).
    """
    # Evidence is the turn's own patch text, not HEAD (which predates this patch).
    # Read above apply so a failure cannot leave the index staged. Host I/O
    # failure is *not* a control-path plant: map it to patch_unreadable so
    # retry/recovery is not steered as if the lane poisoned .git.
    try:
        grounding: tuple[str, ...] = (read_patch_text(patch_file),)
    except OSError as exc:
        raise PatchUnreadableError(f"patch_unreadable: {exc}") from exc
    patch_text = grounding[0]
    if patch_touches_git_control_paths(patch_text):
        raise RuntimeError("quarantined: patch touches git control paths")
    message = build_remote_turn_commit_message(lane_id=lane_id, summary=summary, grounding=grounding)
    apply = subprocess.run(
        ["git", "-C", str(worktree_path), "apply", "--index", str(patch_file)],
        capture_output=True,
        text=True,
        check=False,
    )
    if apply.returncode != 0:
        raise RuntimeError(
            f"git apply --index failed for the remote patch: {apply.stderr.strip()[-500:] or 'no stderr'}"
        )
    commit = subprocess.run(
        [
            "git",
            "-C",
            str(worktree_path),
            *_ENGINE_GIT_IDENTITY,
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "--no-verify",
            "-m",
            message,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if commit.returncode != 0:
        rollback = subprocess.run(
            ["git", "-C", str(worktree_path), "apply", "-R", "--index", str(patch_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        rollback_note = (
            "reversed the applied patch (lane restored to pre-turn HEAD)"
            if rollback.returncode == 0
            else f"WARNING: rollback failed (rc={rollback.returncode}); lane may be dirty: {rollback.stderr.strip()[-200:] or 'no stderr'}"
        )
        raise RuntimeError(
            f"local commit of the remote patch failed: {commit.stderr.strip()[-500:] or 'no stderr'} — {rollback_note}"
        )


def _backend_result_from_worker_payload(payload: dict[str, Any]) -> BackendResult:
    """Build a BackendResult from a parsed worker payload, clamping invalid action.

    Action validation is decoupled from payload selection: an off-enum / null /
    unshaped recovery still preserves summary and tests_run. Fail-closed clamp
    to ``needs_guidance`` + ``merge_ready=False`` + typed blocker so an invalid
    action never flows downstream as a green pass (precedent: from_dict defaults
    an *absent* action rather than discarding the payload).
    """
    result = BackendResult.from_dict(payload)
    if not handoff_action_needs_clamp(payload):
        return result
    blockers = list(result.blockers)
    if "invalid_handoff_action" not in blockers:
        blockers.append("invalid_handoff_action")
    return BackendResult(
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
        off_box_self_verify=result.off_box_self_verify,
    )


def _result_from_json(result_file: Path) -> BackendResult | None:
    """Parse grok's structured stdout JSON (fetched to --result-out) into a
    BackendResult, tolerating narration noise (same parse chain as GrokCliAdapter).
    Returns None when absent/empty/unparseable/oversized (caller fails closed).

    Size-capped before read [RES-05]: the remote controls --result-out content;
    an unbounded read of a multi-GB file would OOM the local adapter process.
    """
    if not result_file.is_file():
        return None
    try:
        size = result_file.stat().st_size
    except OSError:
        return None
    if size > _RESULT_FILE_MAX_BYTES:
        return None
    stdout = result_file.read_text(errors="replace")
    if not stdout.strip():
        return None
    envelope = _parse_envelope(stdout)
    payload = _extract_grok_payload(stdout, envelope)
    if payload is not None:
        return _backend_result_from_worker_payload(payload)
    # Review turns emit REVIEW_OUTPUT_SCHEMA payloads (findings/summary,
    # additionalProperties:false) which by design carry NO handoff_action —
    # _extract_grok_payload's shape key — so they extract to None and were
    # hard-failed as "transport failures", destroying real findings
    # (r07163433 HIGH-1). Fall back to a review-shaped extraction and wrap the
    # payload; rc-code handling clamps handoff_action anyway.
    review = _extract_review_payload(stdout, envelope)
    if review is None:
        return None
    # Unshaped recovery (off-enum/null/absent action without findings): preserve
    # the full report via the same clamp path as the grok extractor. Findings-
    # only / shaped review keeps the historical summary-only wrap.
    if review.get(SHAPED_PAYLOAD_RECOVERY_KEY) == RECOVERY_TIER_UNSHAPED:
        return _backend_result_from_worker_payload(review)
    return BackendResult(
        handoff_action="needs_guidance",
        summary=str(review.get("summary") or "review output (no handoff envelope)")[:200],
        details="",
        raw_payload=review,
    )


#: Frozen self-verify outcome enum literals emitted by remote_agent.sh.
#: Anything else is dropped — never pass through arbitrary remote-controlled strings.
_SELF_VERIFY_OUTCOMES = frozenset({"passed", "failed", "harness_error"})


def _is_selfverify_shaped(d: dict[str, Any]) -> bool:
    """Return True when ``d`` is a shaped off-box self-verify capture.

    Real remote captures carry an int-coercible ``exit_code`` plus either a
    ``passed`` flag or a ``self_verify_outcome`` enum member. Bare
    ``{"exit_code": ...}`` trailer/log objects are NOT shaped so they cannot
    steal last-wins from a real green capture (DURREV-RP-F2 / D4). Key presence
    of ``exit_code`` alone is insufficient (mirrors contract §2 discipline).
    """
    try:
        int(d["exit_code"])
    except (KeyError, TypeError, ValueError):
        return False
    if "passed" in d:
        return True
    return d.get("self_verify_outcome") in _SELF_VERIFY_OUTCOMES


def _load_selfverify_dict(text: str) -> dict[str, Any] | None:
    """Tolerant reader for ``--selfverify-out`` capture text (HG0804-27).

    Parse once; both :func:`_off_box_self_verify_from_json` and
    :func:`_self_verify_outcome_from_json` MUST derive from this same selected
    object (DURREV-RP-F1 / D3).

    Tier order (SHAPED-PAYLOAD RECOVERY CONTRACT v1):

    1. Strict ``json.loads`` of the whole text:
       - top-level dict → return it (silent; pre-fix success path).
       - top-level array → select the last selfverify-shaped member (REVA-04
         uniform array handling; stamps recovery so pure-array and
         noise-wrapped-array agree). Empty/unshaped arrays → None.
       - other scalars → None (no fallthrough).
    2. On strict failure only: scan brace-balanced objects via the shared
       non-abandoning scanner and select the **last** selfverify-shaped dict
       (contract §1 + §3). Stamps ``shaped_payload_recovery`` and logs a
       warning (contract §4 / DURREV-RP-F5).
    3. Give up → None.

    The historical greedy ``find_embedded_json_object`` tier was deleted as
    dead code (DURREV-RP-F4 / D5): after the non-abandoning balanced scan,
    every input that previously needed greedy recovery is recovered at tier 2
    (or is unrecoverable either way).
    """
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        payload = None
    else:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list):
            # REVA-04: pure top-level array must agree with the noise-wrapped
            # balanced scan (last shaped member wins). Stamp recovery so this
            # is never silent.
            last: dict[str, Any] | None = None
            for item in payload:
                if isinstance(item, dict) and _is_selfverify_shaped(item):
                    last = item
            if last is None:
                return None
            return stamp_recovery_tier(last, RECOVERY_TIER_BALANCED)
        return None

    return select_last_shaped_payload(text, shaped=_is_selfverify_shaped)


def _read_selfverify_file(selfverify_file: Path) -> dict[str, Any] | None:
    """Size-capped read + single parse of a ``--selfverify-out`` file.

    Shared by both consumers so the gate and the outcome enum cannot disagree
    about one file (DURREV-RP-F1). Returns None when absent/empty/oversized/
    unreadable/unparseable.
    """
    if not selfverify_file.is_file():
        return None
    try:
        if selfverify_file.stat().st_size > _RESULT_FILE_MAX_BYTES:
            return None
    except OSError:
        return None
    text = selfverify_file.read_text(errors="replace")
    if not text.strip():
        return None
    return _load_selfverify_dict(text)


def _off_box_self_verify_from_json(selfverify_file: Path) -> dict[str, Any] | None:
    """Parse the off-box self-verify JSON (fetched to --selfverify-out) into a
    normalized ``{command, exit_code, passed, output_tail}`` dict, or None when
    absent/empty/unparseable/oversized/malformed. Fail-open to None: a missing
    capture is handled by the worker's OBS-08 enforcement (a typed failure),
    never a silent local re-run.

    Size-capped before read [RES-05]: the remote controls the file content.
    Shares :func:`_read_selfverify_file` with the outcome consumer (D3).
    When recovery was non-strict, also surfaces
    ``shaped_payload_recovery`` so operators can see the tier (D6); the
    four-key self-verify gate fields are unchanged.
    """
    payload = _read_selfverify_file(selfverify_file)
    if not isinstance(payload, dict) or "exit_code" not in payload:
        return None
    try:
        exit_code = int(payload["exit_code"])
    except (TypeError, ValueError):
        return None
    out: dict[str, Any] = {
        "command": str(payload.get("command") or ""),
        "exit_code": exit_code,
        "passed": bool(payload.get("passed", exit_code == 0)),
        "output_tail": _tail_text(str(payload.get("output_tail") or "")),
    }
    recovery = payload.get(SHAPED_PAYLOAD_RECOVERY_KEY)
    if recovery is not None:
        out[SHAPED_PAYLOAD_RECOVERY_KEY] = recovery
    return out


def _self_verify_outcome_from_json(selfverify_file: Path) -> str | None:
    """Read the ``self_verify_outcome`` enum from the off-box self-verify JSON.

    Returns one of ``"passed" | "failed" | "harness_error"``, or None when
    the file is missing/oversized/empty/unparseable/non-dict or the value is not
    one of the three frozen literals. Sibling of
    :func:`_off_box_self_verify_from_json` — deliberately separate so the
    four-key whitelist is never widened (F5); the enum travels on
    ``raw_payload``, never inside ``off_box_self_verify``.

    Uses the same :func:`_read_selfverify_file` selection as the gate consumer
    so both agree on one object (DURREV-RP-F1 / D3).
    """
    payload = _read_selfverify_file(selfverify_file)
    if not isinstance(payload, dict):
        return None
    outcome = payload.get("self_verify_outcome")
    if outcome in _SELF_VERIFY_OUTCOMES:
        return str(outcome)
    return None


def _extract_review_payload(stdout: str, envelope: dict[str, Any] | None) -> dict[str, Any] | None:
    """Mirror ``_extract_grok_payload``'s tiers keyed on the shaped-payload contract.

    A dict qualifies under SHAPED-PAYLOAD RECOVERY CONTRACT v1 §2:
    ``handoff_action`` in the known enum OR list-valued ``findings``. Key
    presence alone is not enough (null/empty/unknown handoff_action rejected).
    Aligns with the VM gate so a findings-only review is not reported as
    ``no_findings_block`` (DURREV-VM-F7).

    Tier order (schema-forced channel first — OFFLOAD-RESULT-TEXT-ACTION-
    SPLITBRAIN-02 / REAPCONV-ENGINE-MERGEREADY-MISREAD):

    0. Envelope root is itself shaped.
    1. ``structuredOutput`` (dict or JSON string), then generic dict-valued
       envelope keys (result/content/output/message).
    2. Narrated text channels only when structured channels are null/unshaped
       (LAST shaped object wins; stamps recovery tier).
    3. Parsed-but-unshaped fallthrough (preserves summary/tests_run; callers
       clamp the action).

    Action validation is decoupled from payload selection: when no candidate
    passes the shape gate but a well-formed dict was still parsed, return it
    stamped :data:`RECOVERY_TIER_UNSHAPED` instead of None so a committed
    turn's summary/tests_run are not destroyed. Callers clamp the action.
    """
    if envelope is not None and is_shaped_result_payload(envelope):
        return envelope

    texts: list[str] = []
    if envelope is not None:
        for key in ("text", "output_text", "content", "message", "result"):
            value = envelope.get(key)
            if isinstance(value, str):
                texts.append(value)
    else:
        texts.append(stdout)

    # 1. Schema-forced structured channels beat narrated progress text.
    if envelope is not None:
        structured = envelope.get("structuredOutput")
        if isinstance(structured, dict) and is_shaped_result_payload(structured):
            return structured
        if isinstance(structured, str):
            candidate = _loads_dict(structured)
            if candidate is not None and is_shaped_result_payload(candidate):
                return stamp_recovery_tier(candidate, RECOVERY_TIER_BALANCED)
        for key in ("result", "content", "output", "message"):
            value = envelope.get(key)
            if isinstance(value, dict) and is_shaped_result_payload(value):
                return value

    # 2. Narrated text — only when structured channels are null/unshaped.
    for text in texts:
        shaped = [d for d in _text_result_dicts(text) if is_shaped_result_payload(d)]
        if shaped:
            return stamp_recovery_tier(shaped[-1], RECOVERY_TIER_BALANCED)

    # 3. Shaped selection failed: preserve a parsed-but-unshaped report rather
    # than conflating it with true unparseable (None).
    return recover_unshaped_payload(
        texts,
        text_dicts_fn=_text_result_dicts,
        envelope=envelope,
        loads_dict_fn=_loads_dict,
    )


def _remote_unavailable_result(
    *,
    summary: str,
    blocker: str,
    details: str,
    model: str,
    effort: str | None,
    raw_payload: dict | None = None,
) -> BackendResult:
    """Typed fail-closed result for a transport failure (implementation note S3).

    An ungated grok-remote dispatch whose VM is unconfigured, unreachable, or
    times out must degrade to a recorded ``needs_guidance`` blocker (announced +
    recorded by the pass) rather than crashing it with a ``RuntimeError`` — the
    RES-13 fail-closed crumple zone at the cross-host boundary. No commit landed,
    so ``merge_ready`` is always False.

    ``raw_payload`` defaults to empty; the exit-75 VM admission path (memory
    floor or lane cap) may attach an ``admission_deferred`` marker so the
    orchestrator can recover via a fresh re-dispatch (admission is re-checked)
    while keeping ``handoff_action="needs_guidance"`` as the fail-safe default.
    """
    return BackendResult(
        handoff_action="needs_guidance",
        summary=summary,
        details=details,
        merge_ready=False,
        blockers=[blocker],
        response_model=model,
        reasoning_effort=effort,
        raw_payload=raw_payload if raw_payload is not None else {},
    )


def _lane_state_dir_from_env(env: dict[str, str] | None) -> Path | None:
    """Locate the orchestrator ``.task-state`` dir from managed TMPDIR hints.

    ``pythonpath_env`` sets TMPDIR to ``<orchestrator>/.task-state/tmp/<lane>``;
    walk parents to find ``.task-state`` so remote result-out can share the
    same durable root as lane status JSON files.
    """
    if not env:
        return None
    tmp = env.get("TMPDIR") or env.get("TMP") or env.get("TEMP")
    if not tmp:
        return None
    current = Path(tmp)
    for candidate in (current, *current.parents):
        if candidate.name == ".task-state":
            return candidate
    return None


def _spool_prefix(kind: str, lane_id: str) -> str:
    """Build a filesystem-safe tempfile prefix for spool directories/files."""
    safe_lane = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in lane_id) or "lane"
    return f"{kind}-{safe_lane}-"


def _open_remote_exec_spool(
    *,
    lane_id: str,
    env: dict[str, str] | None,
) -> tuple[Path, bool, Callable[[], None]]:
    """Open a spool directory for remote intermediate files.

    Prefer a durable directory under the orchestrator lane-state root (never
    auto-deleted). Fall back to a process TemporaryDirectory under TMPDIR when
    the durable root is unavailable, and flag the fallback for the envelope.
    """
    prefix = _spool_prefix("remote-exec", lane_id)
    state_dir = _lane_state_dir_from_env(env)
    if state_dir is not None:
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
            durable = Path(tempfile.mkdtemp(prefix=prefix, dir=str(state_dir)))
            return durable, False, lambda: None
        except Exception:  # noqa: BLE001 - durable failure falls back to TMPDIR
            pass
    tmp = tempfile.TemporaryDirectory(prefix=prefix)
    return Path(tmp.name), True, tmp.cleanup


def _worktree_head_sha(worktree_path: Path) -> str | None:
    """HEAD SHA at spool open (before the turn commit). None on git failure."""
    res = subprocess.run(
        ["git", "-C", str(worktree_path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    sha = (res.stdout or "").strip()
    if res.returncode != 0 or not sha:
        return None
    return sha


def _write_owner_pid(spool: Path) -> None:
    """Record the adapter process pid so the staging reaper can skip live owners."""
    (spool / "owner.pid").write_text(f"{os.getpid()}\n", encoding="ascii")


def _release_owner_pid(spool: Path) -> None:
    """Drop the live-owner lock after the turn so completed spools can be culled.

    Unlink first; if unlink fails, zero the file so a leftover lock is not a
    live owner. Missing file is already released.
    """
    lock = spool / "owner.pid"
    try:
        lock.unlink()
    except FileNotFoundError:
        return
    except OSError:
        try:
            lock.write_text("", encoding="ascii")
        except OSError:
            pass


def _stamp_spool_identity(spec_path: Path, *, branch: str, head_sha: str | None) -> None:
    """ADD ``branch`` / ``head_sha`` to AgentSpec ``spec.json`` without replacing it."""
    if not head_sha:
        return
    try:
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict):
        return
    payload["branch"] = branch
    payload["head_sha"] = head_sha
    spec_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _harvest_miss(debug_file: Path, reason: str, **fields: Any) -> dict[str, Any] | None:
    """Log one structured line for a harvest that yielded nothing, and return None.

    Every miss used to return a bare ``None`` with no log line and no reason
    code, so a defeated parser and a genuinely usage-free log were the same
    observation [OBS-08][AGT-10]. That is not just an operability gap: it is
    what makes a shaped log *safe* to write. Padding the real record out of the
    tail window, or appending one ``_meta`` line that displaces the final call,
    both land as "this lane reported no token usage" -- indistinguishable from
    the honest case, and identical to what the pre-harvest system always said.

    ``reason`` is the discriminator; ``fields`` carry the counters that separate
    "no log" from "1,204 marker lines scanned, none usable".
    """
    detail = "".join(f" {key}={value}" for key, value in sorted(fields.items()))
    emit = logger.debug if reason in _QUIET_MISS_REASONS else logger.warning
    emit("token_usage harvest miss: reason=%s path=%s%s", reason, debug_file, detail)
    return None


def _token_usage_from_debug_log(debug_file: Path) -> dict[str, Any] | None:
    """Recover per-lane token usage from the remote agent's debug log.

    The remote transport builds its ``BackendResult`` from ``result.json``, whose
    schema carries no token fields, so ``token_usage`` was never populated on this
    backend while every on-box adapter populated it. The counts do reach the host:
    the gateway client logs the ``session/prompt`` response body as raw JSON, and
    that body carries two *different* records under ``_meta``:

    * ``_meta``        -- the final model call only
    * ``_meta.usage``  -- cumulative session totals, identified by ``modelCalls``

    Taking the former for the latter undercounts a multi-call turn severely
    (43,569 vs 234,777 tokens on the lane this was derived from), so the cumulative
    record is preferred and the fallback is labelled degraded rather than passed
    off as a session total [OBS-08].

    Units are tokens. ``costUsdTicks`` is present in the record but deliberately not
    read: the tick-to-currency scale is undocumented, so deriving a cost here would
    be inventing a number.

    Returns None when the log is absent or carries no usage record, so the caller
    leaves ``token_usage`` unset rather than reporting a zero-cost turn.
    """
    try:
        if not debug_file.is_file():
            return _harvest_miss(debug_file, "log_absent")
        size = debug_file.stat().st_size
    except OSError as exc:
        return _harvest_miss(debug_file, "stat_failed", error=type(exc).__name__)
    if size <= 0:
        return _harvest_miss(debug_file, "log_empty", size=size)

    # Size-capped like the result file [RES-05]: the remote controls this file. The
    # cumulative record rides the final response, so an oversized log is read from
    # the tail rather than skipped outright.
    try:
        with debug_file.open("rb") as fh:
            # max(0, ...) matters when the tail window is wider than the file: a
            # negative seek raises and would silently cost the whole record.
            oversized = size > _DEBUG_LOG_MAX_BYTES
            offset = max(0, size - _DEBUG_LOG_TAIL_BYTES) if oversized else 0
            if offset:
                fh.seek(offset)
            lines = fh.read().decode("utf-8", errors="replace").split("\n")
            # The partial line at the seek point is NOT discarded. A seek only
            # truncates a line's *front*, and the marker precedes the JSON, so
            # there are exactly two cases: the cut reached the marker, in which
            # case ``find`` returns -1 below and the line is skipped anyway; or
            # it did not, in which case the marker and the whole body survived
            # and the line is a complete, usable record missing nothing but its
            # timestamp prefix. An unconditional ``del lines[0]`` can therefore
            # only ever destroy the second case. (A surviving marker over a
            # damaged body is a truncated *write*, not a seek, and the
            # ``json.loads`` guard in the loop already covers that.)
    except Exception as exc:  # noqa: BLE001 - the remote sizes this file [I6]
        # Not just OSError: reading up to the 8MiB tail of a remote-controlled
        # file can raise MemoryError, which is not an OSError. That escaped the
        # harvest entirely and was caught only by ``_envelope`` -- so the turn
        # survived, but I6's claim that the harvest itself degrades to None was
        # not true of this path.
        return _harvest_miss(debug_file, "read_failed", error=type(exc).__name__, size=size)

    cumulative: dict[str, Any] | None = None
    cumulative_key: tuple[int, int] = (0, 0)
    final_call: dict[str, Any] | None = None
    marker_hits = 0
    parse_failures = 0
    for line in lines:
        idx = line.find(_GATEWAY_RESPONSE_MARKER)
        if idx == -1:
            continue
        marker_hits += 1
        try:
            obj = json.loads(line[idx + len(_GATEWAY_RESPONSE_MARKER) :].strip())
        except Exception:  # noqa: BLE001 - the remote writes this file [I6]
            # Not just JSONDecodeError/ValueError: a deeply nested payload raises
            # RecursionError and an enormous one MemoryError, neither of which is a
            # ValueError. Uncaught, they escape the stamp, escape ``_envelope``, and
            # destroy the turn's real result -- a telemetry scrape losing the work it
            # was only supposed to measure.
            parse_failures += 1
            continue  # interleaved or truncated line; not a usable record
        if not isinstance(obj, dict):
            continue
        meta = obj.get("_meta")
        if not isinstance(meta, dict):
            continue
        if any(f in meta for f in _USAGE_TOKEN_FIELDS):
            final_call = meta
        usage = meta.get("usage")
        # ``modelCalls`` must be a real call count -- not merely a present key,
        # and not merely an integer. The arm it selects stamps
        # ``cumulative_session_usage``, and a session reporting zero (or fewer)
        # model calls is not a rollup [I1].
        calls = _coerce_count(usage.get("modelCalls")) if isinstance(usage, dict) else None
        if calls:
            # Keep the *largest* rollup, not the last one seen. These records are
            # cumulative, so the largest is the most complete. Taking the last
            # let a trailing low rollup -- a retried or re-initialised session
            # writing to the same file, or a hostile remote appending one line --
            # displace a real total and still wear the session label [I1]. The
            # previous code claimed this property in a comment and enforced none
            # of it.
            key = (_coerce_count(usage.get("totalTokens")) or 0, calls)
            if cumulative is None or key >= cumulative_key:
                cumulative, cumulative_key = usage, key

    if cumulative is not None and not _has_positive_count(cumulative):
        # Selected on its call count, but it carries no usable tokens. Choosing
        # the cumulative arm used to be irreversible, so this discarded a
        # perfectly good final-call record and returned None -- an absence that
        # reads downstream as a free turn. I2 already specifies how to label the
        # fallback, so fall through to it rather than losing the counts.
        cumulative = None

    if cumulative is None and final_call is None:
        # ``oversized``/``offset`` are the tail-window frame. Without them a log
        # padded until the real record fell outside the window is
        # indistinguishable from a log that never carried one.
        return _harvest_miss(
            debug_file,
            "no_usage_record",
            lines_scanned=len(lines),
            marker_hits=marker_hits,
            parse_failures=parse_failures,
            oversized=oversized,
            offset=offset,
            size=size,
        )

    rec: dict[str, Any] = {
        "available": True,
        "source": "remote_debug_log",
        "unit": "tokens",
    }
    if cumulative is not None:
        for name in (*_USAGE_TOKEN_FIELDS, "modelCalls", "numTurns", "apiDurationMs"):
            count = _coerce_count(cumulative.get(name))
            if count is not None:
                rec[name] = count
        model_usage = cumulative.get("modelUsage")
        if isinstance(model_usage, dict):
            rec["models"] = sorted(str(k) for k in model_usage)
        if isinstance(final_call, dict) and final_call.get("modelId") is not None:
            # ``models`` comes from ``modelUsage`` keys, which are deployment build
            # tags ("grok-4.5-build") and differ from ``modelId`` ("grok-4.5") in
            # every observed log; carry both so a join on either resolves.
            rec["modelId"] = str(final_call["modelId"])
        rec["basis"] = "cumulative_session_usage"
    else:
        for name in _USAGE_TOKEN_FIELDS:
            count = _coerce_count((final_call or {}).get(name))
            if count is not None:
                rec[name] = count
        rec["basis"] = "final_model_call_only"
        rec["degraded"] = "no cumulative _meta.usage in log; undercounts multi-call turns"

    if not _has_positive_count(rec):
        # A label with no count answers no question while reading as available --
        # the same failure as reporting a zero, one indirection out [I4]. This
        # tests for a *positive* count, not a present key: an all-zero payload
        # satisfied the old presence test and stamped ``usage_source=observed``
        # over totals of nothing.
        return _harvest_miss(
            debug_file,
            "no_positive_count",
            basis=rec.get("basis"),
            marker_hits=marker_hits,
            parse_failures=parse_failures,
        )

    # The downstream promoter reads only ``{last,total}.total_tokens`` in the
    # snake_case breakdown shape ``normalize_cli_usage`` produces; without these
    # the record is stamped on the result and never recorded anywhere.
    # ``_meta.usage`` is the session rollup and ``_meta`` the final call, which is
    # exactly the total/last split that shape means.
    total_src = cumulative if cumulative is not None else final_call
    last_src = final_call if final_call is not None else cumulative
    rec["total"] = _usage_breakdown(total_src)
    rec["last"] = _usage_breakdown(last_src)
    # A derived total is not a reported one. On the reference lane the gateway's
    # own ``totalTokens`` (43,569) exceeds ``input + output`` (43,568), so the
    # derivation is close but wrong, and an unmarked derived figure is
    # indistinguishable from an observed one.
    rec["total_tokens_derived"] = not _coerce_count((total_src or {}).get("totalTokens"))
    rec["model_context_window"] = None
    # KNOWN COARSE LABEL, deliberately not "fixed" here. These counts are
    # scraped from a file the *measured party* writes, so they are not
    # host-metered the way an on-box adapter's are, and bucketing them under
    # "observed" puts them in the same aggregate as counts that were [CARD-11].
    # The honest label would be a distinct provenance value, but ``usage_source``
    # is pinned by a CHECK constraint to four values (shared_schema.py) and
    # validated again in ``lanes.py``; emitting anything else makes the insert
    # reject and loses the telemetry outright, which is strictly worse. Adding a
    # value is a v27 migration across the handoff package, tracked separately.
    # Meanwhile the row is not lying to a careful reader: ``source``, ``basis``,
    # ``degraded`` and ``total_tokens_derived`` above are persisted verbatim into
    # ``raw_usage_json`` (worker_daemon ``raw_usage = dict(token_usage)``), so
    # per-row provenance survives -- it is the bucket that is coarse.
    rec["usage_source"] = "observed"
    return rec


def _coerce_count(value: Any) -> int | None:
    """The value as a usable token count, or ``None`` when it is not one.

    Type alone is not enough -- four rejections, each for a defect found in
    review rather than an imagined one:

    * ``bool`` is an ``int`` subclass and is not a count.
    * A negative is not a count. The remote writes this file [I6], and the
      promoter's admission test is ``bool(observed_total)``, so a negative is
      truthy: it promotes as observed spend and then subtracts from any
      aggregate built by summing turns.
    * A non-integral float is not a count.
    * A count above ``_MAX_PLAUSIBLE_COUNT`` is not a count. Bounding below
      while leaving the top open left magnitude fully remote-controlled, and
      magnitude is what decides: the scraped total drives the lane's token
      budget, so one forged line could halt the lane [CARD-11].

    An *integral* float is accepted. JSON has no integer type, so a conforming
    producer may emit ``234777.0``; rejecting it discarded the entire record
    over a notation difference.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= _MAX_PLAUSIBLE_COUNT else None
    if isinstance(value, float) and value.is_integer() and 0 <= value <= _MAX_PLAUSIBLE_COUNT:
        return int(value)
    return None


def _has_positive_count(src: dict[str, Any] | None) -> bool:
    """True when at least one token field carries a count that is not zero.

    Key presence is not the question and neither is type: a record of all zeros
    answers no spend question while reading as available, which is the same
    failure as reporting a zero [I4].
    """
    return any(_coerce_count((src or {}).get(f)) for f in _USAGE_TOKEN_FIELDS)


def _usage_breakdown(src: dict[str, Any] | None) -> dict[str, int]:
    """Project a gateway usage record onto the snake_case breakdown keys."""
    out: dict[str, int] = {}
    for gateway_name, key in _USAGE_BREAKDOWN_KEYS.items():
        out[key] = _coerce_count((src or {}).get(gateway_name)) or 0
    if not _coerce_count((src or {}).get("totalTokens")):
        # Derive when the gateway reported no total *or* reported zero. The
        # earlier guard was ``is not a count``, which let an explicit ``0``
        # through untouched: the promoter reads ``bool(total_tokens)``, so that
        # zero suppressed the whole record while positive input/output counts
        # sat right beside it.
        out["total_tokens"] = out["input_tokens"] + out["output_tokens"]
    return out


def _stamp_backend_result_token_usage(
    result: BackendResult,
    *,
    debug_file: Path,
) -> BackendResult:
    """Populate ``token_usage`` from the debug log when the payload carried none.

    ``BackendResult`` is frozen; return a copy (same idiom as
    ``_stamp_backend_result_path``). An existing ``token_usage`` always wins so a
    future transport that reports usage inline is never overwritten by a scrape.
    """
    if result.token_usage:
        return result
    usage = _token_usage_from_debug_log(debug_file)
    if usage is None:
        return result
    return BackendResult(
        handoff_action=result.handoff_action,
        summary=result.summary,
        details=result.details,
        tests_run=list(result.tests_run),
        blockers=list(result.blockers),
        changed_files=list(result.changed_files),
        merge_ready=result.merge_ready,
        token_usage=usage,
        response_model=result.response_model,
        reasoning_effort=result.reasoning_effort,
        raw_payload=dict(result.raw_payload or {}),
        sandbox_provision=result.sandbox_provision,
        off_box_self_verify=result.off_box_self_verify,
    )


def _stamp_backend_result_path(
    result: BackendResult,
    *,
    result_path: Path,
    tmpdir_fallback: bool,
) -> BackendResult:
    """Surface result location on the structured envelope (raw_payload + discoverable fields).

    ``BackendResult`` is frozen; return a copy with path metadata on
    ``raw_payload`` so both success and stalled/blocked outcomes keep a
    machine-readable pointer without changing the on-disk payload format of
    the remote result-out file itself.
    """
    payload = dict(result.raw_payload or {})
    payload["result_path"] = str(result_path)
    payload["result_path_tmpdir_fallback"] = bool(tmpdir_fallback)
    return BackendResult(
        handoff_action=result.handoff_action,
        summary=result.summary,
        details=result.details,
        tests_run=list(result.tests_run),
        blockers=list(result.blockers),
        changed_files=list(result.changed_files),
        merge_ready=result.merge_ready,
        token_usage=result.token_usage,
        response_model=result.response_model,
        reasoning_effort=result.reasoning_effort,
        raw_payload=payload,
        sandbox_provision=result.sandbox_provision,
        off_box_self_verify=result.off_box_self_verify,
    )


#: Operator VM attestation for writable_roots. Distinct from
#: ``WORKBAY_REMOTE_SANDBOX_PREFLIGHT_OK`` (userns / workspace-write only).
#: The host ``which(codex)`` exec is a host-side check, not evidence the VM
#: lane under ``--ignore-user-config`` can write ``.git`` [CARD-12].
WRITABLE_ROOTS_PREFLIGHT_ENV = "WORKBAY_REMOTE_WRITABLE_ROOTS_PREFLIGHT_OK"
SANDBOX_PREFLIGHT_ENV = "WORKBAY_REMOTE_SANDBOX_PREFLIGHT_OK"

_PROBE_GIT = (
    "git",
    "-c",
    "user.name=writable-roots-probe",
    "-c",
    "user.email=writable-roots-probe@workbay.local",
    "-c",
    "commit.gpgsign=false",
)


def writable_roots_preflight_argv() -> list[str]:
    """Sandbox flags the host-side writable_roots check shares with the lane."""
    return [
        "-s",
        LANE_SANDBOX,
        "-c",
        f"sandbox_workspace_write.writable_roots={json.dumps(list(LANE_WRITABLE_ROOTS))}",
    ]


def _default_writable_roots_sandbox_exec(
    argv: list[str],
    repo: Path,
    *,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    """Run host ``codex exec`` with the same ``-s``/``-c`` pair as the lane.

    This is a host-side version check, **not** VM attestation [CARD-12]. A
    passing host commit does not prove the remote lane under
    ``--ignore-user-config`` can write ``.git``. Fail-closed: missing flags,
    missing binary, timeout, or spawn error → rc=1 and no commit. Tests inject
    an honoring/ignoring ``sandbox_exec`` instead of requiring a live Codex.
    """
    joined = " ".join(argv)
    if (
        "-s" not in argv
        or LANE_SANDBOX not in argv
        or "sandbox_workspace_write.writable_roots=" not in joined
        or ".git" not in joined
    ):
        return subprocess.CompletedProcess(argv, 1, "", "writable_roots probe argv missing sandbox flags")
    codex = shutil.which("codex")
    if not codex:
        return subprocess.CompletedProcess(argv, 1, "", "codex binary not found for writable_roots probe")
    # Match build_agent_spec / _codex_remote_argv for the host-config hole:
    # --ignore-user-config and -C . ; do not add --skip-git-repo-check (the
    # lane recipe does not). Host which(codex) is still not VM evidence.
    cmd = [
        codex,
        "exec",
        "--ignore-user-config",
        "-C",
        ".",
        *argv,
        "git add -A && git -c commit.gpgsign=false commit -m writable-roots-probe",
    ]
    try:
        return subprocess.run(
            cmd,
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return subprocess.CompletedProcess(cmd, 1, "", str(exc))


def probe_writable_roots_git_commit(
    worktree: Path,
    *,
    sandbox_exec: Callable[[list[str], Path], subprocess.CompletedProcess[str]] | None = None,
    timeout: int = 30,
) -> bool:
    """True iff a throwaway git commit lands under the lane writable_roots flags.

    Creates a nested temp repo so the lane worktree stays clean. The commit
    itself is performed by *sandbox_exec* under
    ``-s workspace-write -c sandbox_workspace_write.writable_roots=[".git"]``.
    The default exec is a host-side check (``--ignore-user-config -C .`` plus
    the same ``-s``/``-c`` pair as the lane). It is **not** evidence the VM
    Codex can write ``.git`` [CARD-12]. Fail-closed: any exception, nonzero
    exec, or missing HEAD is False.
    """
    argv = writable_roots_preflight_argv()
    probe_root = Path(worktree) / ".workbay-writable-roots-probe"
    try:
        if probe_root.exists():
            shutil.rmtree(probe_root)
        probe_root.mkdir(parents=True)
        init = subprocess.run(
            [*_PROBE_GIT, "init", "-b", "probe", "."],
            cwd=probe_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if init.returncode != 0:
            return False
        (probe_root / "probe.txt").write_text("writable-roots probe\n", encoding="utf-8")
        exec_fn = sandbox_exec or (
            lambda flags, repo: _default_writable_roots_sandbox_exec(flags, repo, timeout=timeout)
        )
        completed = exec_fn(list(argv), probe_root)
        if getattr(completed, "returncode", 1) != 0:
            return False
        head = subprocess.run(
            [*_PROBE_GIT, "rev-parse", "--verify", "HEAD"],
            cwd=probe_root,
            capture_output=True,
            text=True,
            check=False,
        )
        return head.returncode == 0 and bool(head.stdout.strip())
    except Exception:  # noqa: BLE001 - live probe is fail-closed
        return False
    finally:
        shutil.rmtree(probe_root, ignore_errors=True)


def resolve_effective_model(row: BackendSpec, backend_id: str, model: str | None) -> str:
    """Resolve the model a remote dispatch is authorized to use, or refuse.

    ONE shared authorization path for every remote dispatch ([WEB-33]
    deny-by-default, [SECD-03] complete mediation on each use): the pin
    default, the grok-build family refusal, the retired-model refusal and the
    curated allow-list membership test all run here, on every call, regardless
    of whether the registry row carries a pin. A row that sets
    ``allowed_models`` without ``allowed_model`` can no longer skip
    authorization — model-less dispatch has no default and refuses, and any
    caller slug must be a member of the curated list.

    The allow-list itself stays hand-curated on the BackendSpec registry row
    ([SECD-05]) — never derived from ``cursor-agent --list-models`` or any
    remote probe. This function moves where the check RUNS, never where the
    list comes from.

    Returns the resolved model slug; raises RuntimeError to refuse.
    """
    pin = row.allowed_model
    if pin is None and row.allowed_models is None:
        # No authorization set configured at all: preserve the historical
        # pass-through arm — the caller's slug goes through unchanged and a
        # missing model refuses.
        if not model:
            raise RuntimeError(
                f"Refusing {backend_id} dispatch: no model provided and BackendSpec.allowed_model is unset"
            )
        return model
    if pin is None:
        # Curated allow-list without a pin: deny-by-default. There is no
        # default, so model-less dispatch refuses, and every caller slug is
        # screened below.
        effective_model = model
        if not effective_model:
            raise RuntimeError(
                f"Refusing {backend_id} dispatch: no model provided and BackendSpec.allowed_model is unset"
            )
    else:
        effective_model = model or pin
        if not effective_model:
            raise RuntimeError(f"Refusing {backend_id} dispatch: allowed_model unset on BackendSpec (pin required)")
    # Preserve the build-specific message substring "grok-build" so existing
    # tests keep matching; refuse the family for every row. A retired slug
    # must also refuse when it *is* the registry pin (effective_model == pin);
    # otherwise a stale WORKBAY_GROK_MODEL dispatches a retired model. Parity
    # with GrokCliAdapter. Both refusals stay AHEAD of the curated membership
    # test on every path.
    retired_warning = retired_model_warning(effective_model)
    if _GROK_BUILD_RE.search(effective_model):
        raise RuntimeError(
            f"Refusing {backend_id} dispatch with a build model '{effective_model}' (grok-build family refused)."
        )
    if retired_warning is not None:
        raise RuntimeError(f"Refusing {backend_id} dispatch with retired model '{effective_model}'. {retired_warning}")
    allowed_models = row.allowed_models if row.allowed_models is not None else frozenset([pin])
    if effective_model not in allowed_models:
        env_hint = f" ({row.allowed_model_env})" if row.allowed_model_env else ""
        if allowed_models == frozenset({pin}):
            allowed_clause = f"allowed is the configured pin '{pin}'{env_hint}."
        else:
            allowed_clause = f"allowed is the curated allow-list including configured pin '{pin}'{env_hint}."
        raise RuntimeError(f"Refusing {backend_id} dispatch with model '{effective_model}': {allowed_clause}")
    return effective_model


class RemoteExecAdapter(BackendAdapter):
    """Backend-neutral remote transport (implementation note S3).

    Ships a turn to the OCI VM via ``remote_agent.sh --agent-spec``. Turn shaping
    + effort resolution are delegated to the registry row's
    ``shaping_adapter_path`` (grok-remote → GrokCliAdapter, codex-remote →
    CodexCliAdapter). ``backend_id`` defaults to ``grok-remote`` so existing
    call sites stay unchanged.
    """

    # Sandboxing is the VM's job (the remote sandbox is history-stripped +
    # remote-severed), not a local shallow clone — unlike GrokCliAdapter.
    supports_jail = False

    # implementation note S3 / D7 — aliases of module-level RC_* (single source).
    _RC_RESULT_DEGRADED = _RC_RESULT_DEGRADED
    _RC_AUTH_FAILED = _RC_AUTH_FAILED
    _RC_POLICY_REFUSED = _RC_POLICY_REFUSED

    def __init__(self, *args: Any, backend_id: str = "grok-remote", **kwargs: Any) -> None:
        from ..backend_registry import BACKENDS  # noqa: PLC0415
        from ..codex_lane_config import LANE_TIMEOUT_S  # noqa: PLC0415

        if backend_id not in BACKENDS:
            raise KeyError(f"unknown remote backend_id: {backend_id}")
        row = BACKENDS[backend_id]
        shaping_path = row.shaping_adapter_path
        if not shaping_path:
            raise RuntimeError(
                f"{backend_id} missing shaping_adapter_path on BackendSpec "
                "(implementation note S3 requires a shaping delegate field)"
            )
        module_name, class_name = shaping_path.rsplit(".", 1)
        shaping_cls = getattr(importlib.import_module(module_name), class_name)
        self.backend_id = backend_id
        # CodexCliAdapter has no timeout= ctor; wall-clock bound lives on the
        # transport (supports_adapter_timeout_bounds). Grok shaping keeps timeout.
        transport_timeout = kwargs.pop("timeout", None)
        if not row.capabilities.supports_token_budget_cycle_bounds:
            # Drop grok-only ctor kwargs before constructing the codex/cursor port.
            for k in ("grok_bin", "grok_args", "max_turns"):
                kwargs.pop(k, None)
        else:
            if transport_timeout is not None:
                kwargs["timeout"] = transport_timeout
        self._shaping = shaping_cls(*args, **kwargs)
        # Backward-compat alias: existing tests / call sites use ``_grok``.
        self._grok = self._shaping
        if transport_timeout is not None:
            self._transport_timeout = int(transport_timeout)
        elif row.capabilities.supports_adapter_timeout_bounds and LANE_TIMEOUT_S:
            self._transport_timeout = int(LANE_TIMEOUT_S)
        else:
            self._transport_timeout = int(getattr(self._shaping, "timeout", 300))

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
        return self._shaping.resolve_reasoning_effort(
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
        """Ship one remote turn to the VM, land its commit locally, return a typed result."""
        from workbay_handoff_mcp.enums import (  # noqa: PLC0415
            WorkerEventName,
            normalize_model_identity,
            normalize_model_label,
        )

        del session_mode  # fresh_turn only (MVP); shared_lane continuity is a non-goal.
        worktree_path = Path(worktree_path)
        applied_effort = reasoning_effort if reasoning_effort in _REMOTE_EFFORTS else None

        from ..backend_registry import BACKENDS  # noqa: PLC0415

        row = BACKENDS[self.backend_id]
        row_caps = row.capabilities

        # Per-backend model pin AND curated allow-list both live on the
        # BackendSpec registry row (implementation note S4-H02 / implementation note S4 Option A /
        # implementation note finding P0199-S4-adapter-backend-id-name-branch).
        # ALL model authorization — pin default, grok-build refusal,
        # retired-model refusal, curated membership — is delegated to the
        # shared module-level resolver so every dispatch path is mediated
        # identically (finding P0199-S3-allowed-models-gated-behind-pin-presence;
        # [WEB-33] deny-by-default, [SECD-03] complete mediation on each use).
        effective_model = resolve_effective_model(row, self.backend_id, model)

        # Fail-closed on a dirty local tree: the offload pass checkpoints before each
        # turn, so HEAD is always the turn's committed input; a dirty tree is an
        # unexpected state, not something to ship to the VM.
        if _local_dirty(worktree_path):
            return BackendResult(
                handoff_action="needs_guidance",
                summary="grok-remote refused: local worktree is dirty before the turn",
                details="",
                merge_ready=False,
                blockers=["dirty local worktree before remote dispatch (expected a clean, committed HEAD)"],
                response_model=effective_model,
                reasoning_effort=applied_effort,
            )

        branch = _worktree_branch(worktree_path)
        head_sha = _worktree_head_sha(worktree_path)
        script = _resolve_script(worktree_path)
        pinned_identity = normalize_model_identity(normalize_model_label(effective_model), None) or effective_model
        # Grok keeps the structured wrapper; other remotes ship the brief as-is
        # (codex reads stdin / --prompt-file from the agent-spec recipe).
        if self.backend_id == "grok-remote":
            full_prompt = _build_grok_prompt(prompt, schema, pinned_identity)
        else:
            full_prompt = prompt
        runner = kwargs.get("remote_runner") or _default_remote_runner
        lane_id = str(kwargs.get("lane_id") or branch)
        # Off-box self-verify command (item 26): when set, the VM runs it in the
        # sandbox venv after grok commits and reports the outcome, which the
        # worker consumes instead of re-running locally. Empty/None → not shipped
        # (review lanes and on-box paths are unaffected).
        test_cmd = str(kwargs.get("test_cmd") or "").strip() or None

        run_env = dict(env) if env else os.environ.copy()
        # Ensure the gate host reaches the script even under a restricted lane env
        # (the script also falls back to .workbay/remote-gate.env when unset).
        if "WORKBAY_REMOTE_GATE_HOST" not in run_env and os.environ.get("WORKBAY_REMOTE_GATE_HOST"):
            run_env["WORKBAY_REMOTE_GATE_HOST"] = os.environ["WORKBAY_REMOTE_GATE_HOST"]

        if progress_callback:
            progress_callback(WorkerEventName.EXEC_SPAWNED, backend=self.backend_id)

        # Wall-clock bound (implementation note S4-H03): rows that declare
        # supports_adapter_timeout_bounds own the bound on the transport
        # (caller timeout= kwarg / LANE_TIMEOUT_S). Their shaping adapter may
        # carry a different default (e.g. CursorCliAdapter.timeout=900) that
        # must NOT silently win over an explicit caller-supplied bound. Grok
        # receives timeout= into the shaping adapter, so read-through-shaping
        # is the authoritative path for that family.
        if row_caps.supports_adapter_timeout_bounds:
            local_timeout = int(self._transport_timeout)
        else:
            local_timeout = int(getattr(self._shaping, "timeout", self._transport_timeout))
        # Defense against pickle/copy __dict__ restore, which bypasses the
        # property setter on GrokCliAdapter.timeout [RES-02] [AGT-10]. Fail
        # closed: never clamp or substitute — a silently corrected bound is
        # how an unbounded turn survived prior review rounds.
        if local_timeout <= 0:
            raise ValueError(
                f"timeout must be a positive integer (got {local_timeout}); "
                "a non-positive timeout would emit --timeout 0, which runs "
                "the agent unbounded [AGT-10]"
            )
        # Remote hard-timeout sits just under the local SSH bound (RES-02) so
        # the remote agent self-terminates on the VM — and its result/debug
        # logs still fetch — before the local runner gives up. Headroom is the
        # declared post-turn artifact fetch cost on BackendSpec (HARM-H05);
        # grok keeps 15s so its numbers stay byte-identical. Threshold is 3×
        # headroom (historical 15/45 pair): when local_timeout is at or below
        # that floor, remote==local is accepted explicitly — there is no room
        # to reserve post-turn fetch without collapsing the remote window to
        # zero, and unit-test / short-probe regimes prefer equal bounds over a
        # hollowed-out turn.
        headroom = int(row.post_turn_fetch_headroom_s)
        headroom_threshold = headroom * 3
        if headroom > 0 and local_timeout > headroom_threshold:
            remote_timeout = local_timeout - headroom
        else:
            remote_timeout = local_timeout

        tmp, spool_tmpdir_fallback, spool_cleanup = _open_remote_exec_spool(
            lane_id=lane_id,
            env=run_env,
        )
        try:
            _write_owner_pid(tmp)
            brief_file = tmp / "brief.md"
            brief_file.write_text(full_prompt)
            schema_file = tmp / "schema.json"
            schema_file.write_text(json.dumps(schema))
            patch_file = tmp / "turn.patch"
            result_file = tmp / "result.json"
            debug_file = tmp / "debug.log"
            selfverify_file = tmp / "selfverify.json"
            # implementation note S1.4: local destination for script-owned fetch_phases.
            # Adapter only reads this path after the runner returns — no scp here.
            phases_file = tmp / "phases.json"
            # decision_ts is optional (Lane B threads it). None degrades pre_spawn.
            decision_ts_raw = kwargs.get("decision_ts")
            decision_ts = _safe_int(decision_ts_raw)
            phase_timing_envelope: dict[str, Any] | None = None
            effort_downgrade_reason: str | None = None

            def _envelope(result: BackendResult) -> BackendResult:
                """Stamp phase timing, token usage, and result location on every return.

                Token usage is stamped here rather than at the parse site so that
                blocked/stalled/timeout returns carry counts too: a turn that failed
                still spent tokens, and attributing zero to it is the observability
                failure this closes [OBS-08].
                """
                stamped = _attach_phase_timing(result, phase_timing_envelope)
                try:
                    stamped = _stamp_backend_result_token_usage(stamped, debug_file=debug_file)
                except Exception:  # noqa: BLE001 - telemetry never costs the turn [I6]
                    # The harvest already degrades to None internally; this is the
                    # outer guarantee, so no future change to it can drop a real
                    # result on the floor.
                    logger.warning(
                        "token-usage harvest failed for %s; result kept unstamped",
                        debug_file,
                        exc_info=True,
                    )
                stamped = _stamp_backend_result_path(
                    stamped,
                    result_path=result_file,
                    tmpdir_fallback=spool_tmpdir_fallback,
                )
                # implementation note S5: surface a dropped effort onto the audit trail
                # rather than letting the lane believe it was applied [RLSE-05].
                if effort_downgrade_reason:
                    raw = dict(stamped.raw_payload or {})
                    raw["downgrade_reason"] = effort_downgrade_reason
                    stamped = replace(
                        stamped,
                        downgrade_reason=effort_downgrade_reason,
                        raw_payload=raw,
                    )
                return stamped

            # implementation note S2/S4: ship argv via AgentSpec (--agent-spec required).
            # Default effort is high when unset; invalid non-None effort is
            # refused (not silently upgraded) [CARD-12].
            if reasoning_effort is not None and applied_effort is None:
                raise ValueError(
                    f"reasoning_effort {reasoning_effort!r} not in {_REMOTE_EFFORTS}; "
                    "refusing to ship a substituted effort [AGT-10]"
                )
            spec_effort = applied_effort or "high"
            max_turns: int | None = None
            if row_caps.supports_token_budget_cycle_bounds:
                max_turns = int(getattr(self._shaping, "max_turns", self._grok.max_turns))
            # Always pass the brief; only cursor-remote bakes it into argv.
            # Other recipes ignore prompt (stdin / brief-file are separate).
            agent_spec = build_agent_spec(
                self.backend_id,
                model=effective_model,
                effort=spec_effort,
                max_turns=max_turns,
                agent_turn_timeout_s=(None if row_caps.supports_token_budget_cycle_bounds else int(local_timeout)),
                prompt=full_prompt,
            )
            effort_downgrade_reason = agent_spec.effort_downgrade_reason
            if agent_spec.requires_live_sandbox_preflight:
                # workspace-write / userns attestation — does NOT satisfy the
                # writable_roots grant. A Codex release that ignores
                # writable_roots must not dispatch just because this bit is set.
                workspace_ok = bool(kwargs.get("sandbox_preflight_ok")) or (run_env.get(SANDBOX_PREFLIGHT_ENV) == "1")
                # Distinct operator override for the .git write probe.
                roots_override = bool(kwargs.get("writable_roots_preflight_ok")) or (
                    run_env.get(WRITABLE_ROOTS_PREFLIGHT_ENV) == "1"
                )
                roots_ok = True
                if WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT:
                    if roots_override:
                        roots_ok = True
                    else:
                        probe_fn = kwargs.get("writable_roots_probe")
                        try:
                            if probe_fn is not None:
                                roots_ok = bool(probe_fn(worktree_path))
                            else:
                                roots_ok = bool(probe_writable_roots_git_commit(worktree_path))
                        except Exception:  # noqa: BLE001 - probe is fail-closed
                            roots_ok = False
                workspace_needed = WORKSPACE_WRITE_REQUIRES_LIVE_PREFLIGHT
                blockers: list[str] = []
                payload: dict[str, Any] = {
                    "preflight_required": True,
                    "retryable": True,
                }
                if workspace_needed and not workspace_ok:
                    blockers.append(
                        "requires_live_sandbox_preflight: pass sandbox_preflight_ok=True "
                        f"or {SANDBOX_PREFLIGHT_ENV}=1 after a fresh userns/"
                        "workspace-write probe [FM-08]"
                    )
                    payload["workspace_write_preflight_required"] = True
                if WRITABLE_ROOTS_REQUIRES_LIVE_PREFLIGHT and not roots_ok:
                    blockers.append(
                        "writable_roots live probe failed: a git commit under "
                        "-s workspace-write -c sandbox_workspace_write.writable_roots=['.git'] "
                        "did not land on the operator host. Host which(codex) is not "
                        "VM attestation — pass writable_roots_preflight_ok=True / "
                        f"{WRITABLE_ROOTS_PREFLIGHT_ENV}=1 after a VM-side "
                        "writable_roots commit probe [FM-08][CARD-12]"
                    )
                    payload["writable_roots_preflight_required"] = True
                if blockers:
                    return _envelope(
                        BackendResult(
                            handoff_action="needs_guidance",
                            summary=(f"{self.backend_id} refused: live sandbox preflight required"),
                            details="",
                            merge_ready=False,
                            blockers=blockers,
                            response_model=effective_model,
                            reasoning_effort=applied_effort,
                            raw_payload=payload,
                        )
                    )
            spec_json_path, _spec_argv_path = write_agent_spec(agent_spec, tmp / "spec")
            _stamp_spool_identity(spec_json_path, branch=branch, head_sha=head_sha)

            cmd = [
                str(script),
                "build",
                "--branch",
                branch,
                "--brief",
                str(brief_file),
                "--schema",
                str(schema_file),
                "--out",
                str(patch_file),
                "--result-out",
                str(result_file),
                "--debug-out",
                str(debug_file),
                "--timeout",
                str(remote_timeout),
                "--agent-spec",
                str(spec_json_path),
            ]
            if test_cmd:
                # Off-box self-verify (item 26): the VM runs this TEST_CMD in the
                # sandbox venv after grok commits, writing the outcome JSON to
                # --selfverify-out (fetched below into off_box_self_verify).
                cmd += ["--test-cmd", test_cmd, "--selfverify-out", str(selfverify_file)]
            # implementation note S1.4: ask the script to land the fetched phases artifact
            # locally. FETCH remains script-owned (Lane A). Only ship the flag
            # when this worktree's remote_agent.sh already advertises it —
            # older scripts `die "unknown arg"` on unrecognized flags, which
            # must not turn instrumentation into a dispatch failure (fail-open).
            # C-04: distinguish unreadable script from flag-absent; never
            # collapse either into fetch_scp_failure. Probe stays a pure
            # substring presence check (no dispatch-behavior change).
            phases_flag_state = "omitted"
            try:
                _script_text = Path(script).read_text(encoding="utf-8", errors="replace")
            except Exception as _script_exc:  # noqa: BLE001
                _script_text = ""
                phases_flag_state = "script_unreadable"
                try:
                    print(
                        "remote_agent: phase-instrument degrade vm_half_absent "
                        f"reason=script_unreadable detail={type(_script_exc).__name__}",
                        file=sys.stderr,
                    )
                except Exception:  # noqa: BLE001
                    pass
            if phases_flag_state != "script_unreadable" and "--phases-out" in _script_text:
                cmd += ["--phases-out", str(phases_file)]
                phases_flag_state = "shipped"

            spawn_ts = _int_unix_now()
            try:
                completed = runner(cmd, cwd=str(worktree_path), env=run_env, timeout=local_timeout)
            except subprocess.TimeoutExpired as exc:
                # Fail closed (S3): a slow/hung VM is announced+recorded, not a crash.
                exit_ts = _int_unix_now()
                # Best-effort phase envelope on timeout (fail-open).
                # C-03: never leave phase_timing missing — always attach at least
                # a catch-all envelope so consumers can read the degrade flags.
                try:
                    # Prefer partial stdout/stderr from the original TimeoutExpired
                    # (C-10); fall back to empty strings.
                    partial_out = getattr(exc, "output", None)
                    if partial_out is None:
                        partial_out = getattr(exc, "stdout", None) or ""
                    partial_err = getattr(exc, "stderr", None) or ""
                    fake = subprocess.CompletedProcess(
                        cmd,
                        -1,
                        partial_out if isinstance(partial_out, str) else "",
                        partial_err if isinstance(partial_err, str) else "",
                    )
                    # Carry concurrent reader state if the runner attached it
                    # before timing out (default runner does via the re-raise).
                    phase_host = getattr(exc, "phase_host_state", None)
                    if phase_host is not None:
                        fake.phase_host_state = phase_host  # type: ignore[attr-defined]
                        fake.concurrent_reader_used = True  # type: ignore[attr-defined]
                    phase_timing_envelope, phase_degrades = build_phase_timing_for_dispatch(
                        phases_path=phases_file if phases_flag_state == "shipped" else None,
                        spawn_ts=spawn_ts,
                        exit_ts=exit_ts,
                        decision_ts=decision_ts,
                        completed=fake,
                        marker_timeout_sec=_marker_timeout_sec(run_env),
                        phases_flag_state=phases_flag_state,
                    )
                    _stamp_phase_degrades_on_envelope(phase_timing_envelope, phase_degrades)
                except Exception as merge_exc:  # noqa: BLE001
                    phase_timing_envelope = _instrument_catchall_envelope(
                        spawn_ts=spawn_ts,
                        exit_ts=exit_ts,
                        exc=merge_exc,
                        reason="timeout_merge_failure",
                    )
                return _envelope(
                    _remote_unavailable_result(
                        summary=f"{self.backend_id} turn timed out after {local_timeout}s",
                        blocker=(
                            f"remote turn exceeded the local transport bound ({local_timeout}s) — "
                            "VM slow or unreachable; failing closed."
                        ),
                        details=_tail_text(exc.stderr),
                        model=effective_model,
                        effort=applied_effort,
                    )
                )

            exit_ts = _int_unix_now()
            # Merge VM phase record + host stamps into one envelope (OBS-02).
            # Never fatal: instrumentation failure must not change lane outcome.
            try:
                phase_timing_envelope, phase_degrades = build_phase_timing_for_dispatch(
                    phases_path=phases_file if phases_flag_state == "shipped" else None,
                    spawn_ts=spawn_ts,
                    exit_ts=exit_ts,
                    decision_ts=decision_ts,
                    completed=completed,
                    marker_timeout_sec=_marker_timeout_sec(run_env),
                    phases_flag_state=phases_flag_state,
                )
                _stamp_phase_degrades_on_envelope(phase_timing_envelope, phase_degrades)
            except Exception as merge_exc:  # noqa: BLE001
                phase_timing_envelope = _instrument_catchall_envelope(
                    spawn_ts=spawn_ts,
                    exit_ts=exit_ts,
                    exc=merge_exc,
                )

            rc = completed.returncode
            stderr_tail = _tail_text(completed.stderr)
            grok_result = _result_from_json(result_file)

            if rc == _RC_HOST_UNCONFIGURED:
                # Exit 78 is shared: genuine host-unconfigured OR flock missing on the
                # VM (same-branch collision guard). Discriminate from stderr so the
                # operator is not sent to set WORKBAY_REMOTE_GATE_HOST when the host
                # is configured and the VM image simply lacks util-linux.
                stderr_l_78 = (stderr_tail or "").lower()
                if "flock unavailable" in stderr_l_78:
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote unavailable: flock missing on the VM",
                            blocker=(
                                "flock unavailable on the remote VM (remote_agent.sh exit 78): "
                                "the VM image lacks util-linux/flock required for the same-branch "
                                "collision guard — install util-linux on the VM image."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                        )
                    )
                # Opt-in graceful degradation (S3): unconfigured host is a typed skip,
                # never a hard error — normally gated by the availability probe.
                return _envelope(
                    _remote_unavailable_result(
                        summary="grok-remote unavailable: remote gate host not configured",
                        blocker=(
                            "remote gate host not configured (remote_agent.sh exit 78); set "
                            "WORKBAY_REMOTE_GATE_HOST. Normally gated by the availability probe."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                    )
                )
            if rc == _RC_RESULT_DEGRADED:
                # implementation note D9 (edit kind): handoff_action=needs_guidance and
                # blockers contain the literal result_unparseable.
                #
                # Load-bearing leg (OFFLOAD-RESULT-UNPARSEABLE-...-01): remote_agent
                # still emits format-patch on exit 5 when the agent committed above
                # $BASE. Reporting changed_files=[] here asserts "no work" and
                # discards green lanes. Prefer: apply+commit the patch (work lands),
                # else at least surface paths observed in patch headers. work_present
                # is true only when the tree actually carries the content (commit
                # succeeded or an apply left the tree dirty) — header parse alone
                # is not evidence of landed work (OBS-08 / AGT-10 / CLM-04 / REF-37).
                # Off-box self-verify is attached the same way as the exit-3 arm when
                # a test command was shipped (capture still lives in this tmp scope).
                salvage_contamination = _detect_grok_build_contamination(debug_file, requested_model=effective_model)
                if salvage_contamination is not None:
                    blocker, evidence = salvage_contamination
                    salvage_payload: dict[str, Any] = {
                        "result_degraded": True,
                        "result_parse": "degraded",
                        "retryable": False,
                        "composer_violation_evidence": evidence,
                        "attestation": {
                            "status": "failed",
                            "reason": "grok_build_contamination",
                            "pin": effective_model,
                        },
                    }
                    off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
                    sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None
                    if sv_outcome is not None:
                        salvage_payload["self_verify_outcome"] = sv_outcome
                    return _envelope(
                        BackendResult(
                            handoff_action="needs_guidance",
                            summary="grok-remote: grok-build contamination detected in the remote debug log",
                            details=stderr_tail,
                            merge_ready=False,
                            blockers=[blocker],
                            changed_files=[],
                            response_model=effective_model,
                            reasoning_effort=applied_effort,
                            raw_payload=salvage_payload,
                            off_box_self_verify=off_box_sv,
                        )
                    )
                patch_files = _changed_files_from_patch(patch_file)
                committed: list[str] = []
                commit_succeeded = False
                apply_commit_error: str | None = None
                if patch_files:
                    try:
                        patch_text = patch_file.read_text(errors="replace") if patch_file.is_file() else ""
                    except OSError:
                        patch_text = ""
                    if patch_text.strip():
                        try:
                            summary_for_commit = (
                                grok_result.summary
                                if grok_result is not None and grok_result.summary
                                else "structured result unusable; patch salvaged"
                            )
                            _apply_and_commit(
                                worktree_path,
                                patch_file,
                                lane_id=lane_id,
                                summary=summary_for_commit,
                            )
                            # Explicit success flag: do not overload committed-list
                            # emptiness as the commit signal (list is best-effort).
                            commit_succeeded = True
                            committed = _committed_files(worktree_path)
                        except RuntimeError as exc:
                            # Apply/commit/rollback failed: announce loudly
                            # (OBS-08 / AGT-10 / REF-37 / RLSE-05). Header paths
                            # still surface separately; work_present stays false
                            # unless the tree is actually dirty.
                            apply_commit_error = str(exc).strip()[-500:] or type(exc).__name__
                            committed = []
                # Prefer committed paths when the list is non-empty; otherwise fall
                # back to header paths so changed_files never asserts "no work"
                # while a complete patch sat on disk. Commit success is tracked
                # separately via commit_succeeded (not list emptiness).
                if commit_succeeded:
                    changed = committed or patch_files
                else:
                    changed = patch_files
                tree_has_work = commit_succeeded
                if not tree_has_work and patch_files:
                    try:
                        tree_has_work = _local_dirty(worktree_path)
                    except RuntimeError:
                        tree_has_work = False
                degraded_payload: dict[str, Any] = {
                    "result_degraded": True,
                    "result_parse": "degraded",
                    # D7: exit 5 is non-retryable (operator/schema shape).
                    "retryable": False,
                }
                if patch_files:
                    # Paths observed in patch headers — not proof the tree holds them.
                    degraded_payload["turn_patch_header_paths"] = list(patch_files)
                if tree_has_work:
                    degraded_payload["work_present"] = True
                    degraded_payload["work_present_source"] = (
                        "turn_patch_committed" if commit_succeeded else "turn_patch_dirty"
                    )
                # self_verify_outcome rides raw_payload only (F5: never widen
                # the four-key off_box_self_verify whitelist).
                off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
                sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None
                if sv_outcome is not None:
                    degraded_payload["self_verify_outcome"] = sv_outcome
                blockers = ["result_unparseable"]
                details = stderr_tail
                if apply_commit_error is not None:
                    blockers.append("turn_patch_apply_or_commit_failed")
                    details = f"{details}\n{apply_commit_error}".strip() if details else apply_commit_error
                summary = (
                    f"{self.backend_id} degraded: structured result unusable"
                    + ("; work present in turn.patch" if tree_has_work else "")
                    + ("; turn patch apply/commit failed" if apply_commit_error is not None else "")
                )
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary=summary,
                        details=details,
                        merge_ready=False,
                        blockers=blockers,
                        changed_files=changed,
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=degraded_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )
            if rc == _RC_AUTH_FAILED:
                return _envelope(
                    _remote_unavailable_result(
                        summary=f"{self.backend_id} auth failed on the remote VM",
                        blocker=(
                            "remote turn exited 6 (auth_failed): credential/auth_match "
                            "failure — non-retryable; re-auth before redispatch "
                            "(flock abort per implementation note D7)"
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={"auth_failed": True, "retryable": False},
                    )
                )
            if rc == _RC_POLICY_REFUSED:
                return _envelope(
                    _remote_unavailable_result(
                        summary=f"{self.backend_id} refused by policy/placeholder contract",
                        blocker=(
                            "remote turn exited 7 (policy_refused): argv placeholder "
                            "contract refusal — non-retryable; offending value is either "
                            "the agent-spec recipe or an operator-supplied value carried "
                            "in argv (see stderr for element index and excerpt)"
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={"policy_refused": True, "retryable": False},
                    )
                )
            if rc == _RC_NO_CHANGES:
                # Exit 4: grok ran, made no commit. Keep raw_payload intact so
                # review_runner can still read findings from --result-out
                # ([REF-10] review path is payload-only). Mark the operator-
                # facing summary UNVERIFIED and drop tests_run — a commitless
                # self-report is not bankable evidence (WIDTH-28). Clamp
                # handoff_action to needs_guidance and force merge_ready=False:
                # the execute chain keys exclusively on handoff_action
                # (0144 R3 / HIGH-1), so a commitless payload that claims
                # finished must never report merge-ready.
                no_commit_blocker = "remote turn made no commit (remote_agent.sh exit 4)"
                if grok_result is not None:
                    blockers = list(grok_result.blockers)
                    if no_commit_blocker not in blockers:
                        blockers.append(no_commit_blocker)
                    agent_summary = grok_result.summary or ""
                    return _envelope(
                        BackendResult(
                            handoff_action="needs_guidance",
                            summary=f"UNVERIFIED: {agent_summary}" if agent_summary else "UNVERIFIED",
                            details=grok_result.details or stderr_tail,
                            tests_run=[],
                            blockers=blockers,
                            changed_files=list(grok_result.changed_files),
                            merge_ready=False,
                            token_usage=grok_result.token_usage,
                            response_model=grok_result.response_model or effective_model,
                            reasoning_effort=applied_effort or grok_result.reasoning_effort,
                            raw_payload=grok_result.raw_payload,
                        )
                    )
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary="grok-remote produced no committed changes",
                        details=stderr_tail,
                        merge_ready=False,
                        blockers=[no_commit_blocker],
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                    )
                )
            if rc == _RC_GROK_FAILED:
                # implementation note S3: when the agent committed above $BASE then exited
                # nonzero, remote_agent.sh emits format-patch on stdout before
                # exit 3. Apply that patch WITHOUT committing so the lane tree
                # stays dirty for the offload checkpoint arm (a committed tree
                # would miss the dirty gate and look "confident-wrong").
                #
                # REMOTEEXEC-EXIT3-STDERR-TAIL-DIES-IN-DETAILS: exit 3 is multi-
                # cause (unknown status, mktemp, format-patch, empty format-patch
                # while HEAD≠BASE). The stderr tail is the only discriminator.
                # worker_report persists blockers (not details); the execute-
                # blocked probe reads only blockers[0]. Fold the tail into
                # position 0 on both sub-arms so the diagnosis survives the
                # persistence boundary (same shape as L1-03 / E8FIX-R2-A-02).
                exit3_base = "remote grok run failed (remote_agent.sh exit 3)"
                if stderr_tail:
                    exit3_blocker = f"{stderr_tail} — {exit3_base}"
                else:
                    exit3_blocker = exit3_base
                try:
                    patch_text = patch_file.read_text(errors="replace") if patch_file.is_file() else ""
                except OSError:
                    patch_text = ""
                if patch_text.strip():
                    salvage_contamination = _detect_grok_build_contamination(
                        debug_file, requested_model=effective_model
                    )
                    if salvage_contamination is not None:
                        blocker, evidence = salvage_contamination
                        salvage_payload: dict[str, Any] = {
                            "composer_violation_evidence": evidence,
                            "attestation": {
                                "status": "failed",
                                "reason": "grok_build_contamination",
                                "pin": effective_model,
                            },
                        }
                        off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
                        sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None
                        if sv_outcome is not None:
                            salvage_payload["self_verify_outcome"] = sv_outcome
                        return _envelope(
                            BackendResult(
                                handoff_action="needs_guidance",
                                summary="grok-remote: grok-build contamination detected in the remote debug log",
                                details=stderr_tail,
                                merge_ready=False,
                                blockers=[blocker],
                                changed_files=[],
                                response_model=effective_model,
                                reasoning_effort=applied_effort,
                                raw_payload=salvage_payload,
                                off_box_self_verify=off_box_sv,
                            )
                        )
                    if patch_touches_git_control_paths(patch_text):
                        return _envelope(
                            BackendResult(
                                handoff_action="needs_guidance",
                                summary="grok run failed on the remote VM",
                                details=stderr_tail,
                                merge_ready=False,
                                blockers=[exit3_blocker, "control_path_quarantine"],
                                response_model=effective_model,
                                reasoning_effort=applied_effort,
                            )
                        )
                    apply = subprocess.run(
                        [
                            "git",
                            "-C",
                            str(worktree_path),
                            "apply",
                            "--index",
                            str(patch_file),
                        ],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if apply.returncode != 0:
                        raise RuntimeError(
                            "git apply --index failed for the remote salvage patch: "
                            f"{apply.stderr.strip()[-500:] or 'no stderr'}"
                        )
                    # Marker PAIR required (resumable_work + resumable_reason).
                    # self_verify_outcome rides raw_payload only (F5: never widen
                    # the four-key off_box_self_verify whitelist).
                    salvage_payload: dict[str, Any] = {
                        "resumable_work": True,
                        "resumable_reason": "agent_exit_with_work",
                    }
                    off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
                    sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None
                    if sv_outcome is not None:
                        salvage_payload["self_verify_outcome"] = sv_outcome
                    return _envelope(
                        BackendResult(
                            handoff_action="needs_guidance",
                            summary="grok run failed on the remote VM",
                            details=stderr_tail,
                            merge_ready=False,
                            blockers=[exit3_blocker],
                            response_model=effective_model,
                            reasoning_effort=applied_effort,
                            raw_payload=salvage_payload,
                            off_box_self_verify=off_box_sv,
                        )
                    )
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary="grok run failed on the remote VM",
                        details=stderr_tail,
                        merge_ready=False,
                        blockers=[exit3_blocker],
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                    )
                )
            if rc == _RC_BOUND_EXPIRED:
                # remote_agent.sh exit 8: wall-clock bound expired on a live,
                # reachable VM. The script still salvages format-patch on
                # stdout (same as exit 3). Apply WITHOUT committing so the
                # dirty tree trips the offload checkpoint arm; do NOT route
                # through _remote_unavailable_result (that mis-steers recovery
                # toward "VM broken" instead of re-dispatch).
                #
                # Distinguish empty-patch causes so a host read failure is not
                # reported as "agent produced nothing" (patch_absent /
                # salvage_read_error / patch_empty).
                salvage_patch_cause: str | None = None
                patch_text = ""
                if not patch_file.is_file():
                    salvage_patch_cause = "patch_absent"
                else:
                    try:
                        patch_text = patch_file.read_text(errors="replace")
                    except OSError:
                        salvage_patch_cause = "salvage_read_error"
                        patch_text = ""
                    else:
                        if not patch_text.strip():
                            salvage_patch_cause = "patch_empty"
                # self_verify_outcome rides raw_payload only (F5: never widen
                # the four-key off_box_self_verify whitelist). Capture for both
                # non-empty and empty branches so a real on-disk capture is not
                # reported as "no verify evidence".
                off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
                sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None
                if patch_text.strip():
                    salvage_contamination = _detect_grok_build_contamination(
                        debug_file, requested_model=effective_model
                    )
                    if salvage_contamination is not None:
                        blocker, evidence = salvage_contamination
                        salvage_payload: dict[str, Any] = {
                            "composer_violation_evidence": evidence,
                            "attestation": {
                                "status": "failed",
                                "reason": "grok_build_contamination",
                                "pin": effective_model,
                            },
                        }
                        if sv_outcome is not None:
                            salvage_payload["self_verify_outcome"] = sv_outcome
                        return _envelope(
                            BackendResult(
                                handoff_action="needs_guidance",
                                summary="grok-remote: grok-build contamination detected in the remote debug log",
                                details=stderr_tail,
                                merge_ready=False,
                                blockers=[blocker],
                                changed_files=[],
                                response_model=effective_model,
                                reasoning_effort=applied_effort,
                                raw_payload=salvage_payload,
                                off_box_self_verify=off_box_sv,
                            )
                        )
                    if patch_touches_git_control_paths(patch_text):
                        typed_failure_reason = "control_path_quarantine"
                        apply_fail_payload: dict[str, Any] = {
                            "salvage_patch_cause": "apply_failed",
                            "failure_reason": typed_failure_reason,
                        }
                        if sv_outcome is not None:
                            apply_fail_payload["self_verify_outcome"] = sv_outcome
                        apply_err = "quarantined: patch touches git control paths"
                        details = f"{stderr_tail}\n{apply_err}".strip() if stderr_tail else apply_err
                        if spool_tmpdir_fallback:
                            apply_wall_clock_blocker = (
                                "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                                "reachable VM; salvage patch apply failed and salvage bytes were "
                                "discarded with the volatile TMPDIR spool — result_path will not "
                                "exist after return; re-dispatch starts from the remote's last "
                                "commit, not from the salvage patch"
                            )
                        else:
                            apply_wall_clock_blocker = (
                                "remote wall-clock bound expired (remote_agent.sh exit 8) on a reachable VM"
                            )
                        consumer_blocker = f"{apply_err} — {apply_wall_clock_blocker}; {typed_failure_reason}"
                        return _envelope(
                            BackendResult(
                                handoff_action="needs_guidance",
                                summary=(
                                    "grok-remote wall-clock bound expired on a reachable VM "
                                    "(salvage patch apply failed)"
                                ),
                                details=details,
                                merge_ready=False,
                                blockers=[
                                    consumer_blocker,
                                    typed_failure_reason,
                                ],
                                response_model=effective_model,
                                reasoning_effort=applied_effort,
                                raw_payload=apply_fail_payload,
                                off_box_self_verify=off_box_sv,
                            )
                        )
                    apply = subprocess.run(
                        [
                            "git",
                            "-C",
                            str(worktree_path),
                            "apply",
                            "--index",
                            str(patch_file),
                        ],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    if apply.returncode != 0:
                        # Do NOT raise: a structured needs_guidance envelope is the
                        # right shape for an apply failure (mirrors the exit-5
                        # degraded arm). Note: spool_cleanup is a no-op on the
                        # durable lane-state spool path (lambda: None); it only
                        # deletes on the TMPDIR-fallback path, and that cleanup
                        # runs on return as well as raise — so returning does not
                        # preserve turn.patch either way. Do NOT set resumable
                        # markers here: git apply --index failed, the host tree is
                        # clean, and wall_clock_expiry is checkpoint-eligible, so
                        # markers would steer recovery into a checkpoint arm for
                        # work that does not exist on the host (E8FIX-A-01/A-04).
                        # Bound stderr with an explicit truncation marker when the
                        # head was dropped (E8ENV-R1-A-06); short stderr stays unmarked.
                        apply_stderr_tail = _bounded_stderr_tail(apply.stderr)
                        apply_err = (
                            f"git apply --index failed for the remote bound-expiry salvage patch: {apply_stderr_tail}"
                        )
                        # Typed failure reason on a NON-marker channel (E8FIX-R2-C-01).
                        # Do NOT set resumable_work/resumable_reason here: the host
                        # tree is clean and wall_clock_expiry is checkpoint-eligible,
                        # so markers would steer recovery into a checkpoint arm for
                        # work that does not exist on the host (E8FIX-A-01/A-04).
                        typed_failure_reason = "turn_patch_apply_or_commit_failed"
                        apply_fail_payload: dict[str, Any] = {
                            "salvage_patch_cause": "apply_failed",
                            "failure_reason": typed_failure_reason,
                        }
                        if sv_outcome is not None:
                            apply_fail_payload["self_verify_outcome"] = sv_outcome
                        details = f"{stderr_tail}\n{apply_err}".strip() if stderr_tail else apply_err
                        # Durable spool: turn.patch survives for operator inspect.
                        # TMPDIR fallback: finally deletes the only host copy —
                        # say so; do not imply the salvage bytes remain (L1-03).
                        if spool_tmpdir_fallback:
                            apply_wall_clock_blocker = (
                                "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                                "reachable VM; salvage patch apply failed and salvage bytes were "
                                "discarded with the volatile TMPDIR spool — result_path will not "
                                "exist after return; re-dispatch starts from the remote's last "
                                "commit, not from the salvage patch"
                            )
                        else:
                            apply_wall_clock_blocker = (
                                "remote wall-clock bound expired (remote_agent.sh exit 8) on a reachable VM"
                            )
                        # E8FIX-R2-A-02 / C-01: the execute-blocked probe reads only
                        # blockers[0] (then summary). details is NOT unread —
                        # lane_result maps it to handoff --message — but the probe
                        # never consults details or blockers[1+] (E8ENV-R1-A-01/A-02).
                        # Fold apply stderr, wall-clock prose, AND the typed token
                        # into blockers[0] so the complete diagnosis reaches the
                        # consumer without resumable markers. Keep the opaque token
                        # as a separate list entry for membership pins.
                        consumer_blocker = f"{apply_err} — {apply_wall_clock_blocker}; {typed_failure_reason}"
                        return _envelope(
                            BackendResult(
                                handoff_action="needs_guidance",
                                summary=(
                                    "grok-remote wall-clock bound expired on a reachable VM "
                                    "(salvage patch apply failed)"
                                ),
                                details=details,
                                merge_ready=False,
                                blockers=[
                                    consumer_blocker,
                                    typed_failure_reason,
                                ],
                                response_model=effective_model,
                                reasoning_effort=applied_effort,
                                raw_payload=apply_fail_payload,
                                off_box_self_verify=off_box_sv,
                            )
                        )
                    # Marker PAIR required (resumable_work + resumable_reason).
                    # Distinct reason from exit-3's agent_exit_with_work so the
                    # operator can tell "agent gave up" from "we ran out of time".
                    # Checkpoint outcome: worker_daemon propagates
                    # resumable_reason into execute_stop_reason; offload_pass
                    # checkpoints dirty work for wall_clock_expiry (like
                    # max_turns) but does NOT salvage-commit a red self-verify
                    # (unlike agent_exit_with_work).
                    salvage_payload: dict[str, Any] = {
                        "resumable_work": True,
                        "resumable_reason": "wall_clock_expiry",
                    }
                    if sv_outcome is not None:
                        salvage_payload["self_verify_outcome"] = sv_outcome
                    return _envelope(
                        BackendResult(
                            handoff_action="needs_guidance",
                            summary=(
                                "grok-remote wall-clock bound expired on a reachable VM "
                                "(work is resumable via re-dispatch)"
                            ),
                            details=stderr_tail,
                            merge_ready=False,
                            blockers=[
                                "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                                "reachable VM; salvage applied dirty for checkpoint — re-dispatch "
                                "to continue"
                            ],
                            response_model=effective_model,
                            reasoning_effort=applied_effort,
                            raw_payload=salvage_payload,
                            off_box_self_verify=off_box_sv,
                        )
                    )
                # Empty / unreadable patch exit 8: still not transport. Stamp
                # machine-readable cause; only "patch_empty" claims nothing to
                # recover — missing-file / read-error mean the VM may have work
                # the host failed to load.
                empty_payload: dict[str, Any] = {}
                if salvage_patch_cause is not None:
                    empty_payload["salvage_patch_cause"] = salvage_patch_cause
                if sv_outcome is not None:
                    empty_payload["self_verify_outcome"] = sv_outcome
                if salvage_patch_cause == "patch_empty":
                    summary = "grok-remote wall-clock bound expired on a reachable VM (no salvageable patch)"
                    blockers = [
                        "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                        "reachable VM with no salvageable work — re-dispatch if the "
                        "objective still needs work"
                    ]
                elif salvage_patch_cause == "salvage_read_error":
                    summary = (
                        "grok-remote wall-clock bound expired on a reachable VM (host failed to read salvage patch)"
                    )
                    # Durable: inspect spool is honest (cleanup is a no-op).
                    # TMPDIR fallback: finally deletes the spool this call named
                    # in result_path — do not tell the operator to inspect a
                    # path that will not exist (L1-02 / CLM-04 / RLSE-05).
                    if spool_tmpdir_fallback:
                        blockers = [
                            "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                            "reachable VM; salvage patch present but unreadable on host "
                            "(salvage_read_error) — salvage bytes were discarded with the "
                            "volatile TMPDIR spool; result_path will not exist after return; "
                            "re-dispatch starts from the remote's last commit, not from the "
                            "salvage patch"
                        ]
                    else:
                        blockers = [
                            "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                            "reachable VM; salvage patch present but unreadable on host "
                            "(salvage_read_error) — inspect spool / re-dispatch"
                        ]
                else:
                    # patch_absent (or unexpected None treated as absent)
                    summary = "grok-remote wall-clock bound expired on a reachable VM (salvage patch absent on host)"
                    blockers = [
                        "remote wall-clock bound expired (remote_agent.sh exit 8) on a "
                        "reachable VM; salvage patch absent on host (patch_absent) — "
                        "re-dispatch if the objective still needs work"
                    ]
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary=summary,
                        details=stderr_tail,
                        merge_ready=False,
                        blockers=blockers,
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=empty_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )
            if rc == _RC_ADMISSION_DEFERRED:
                # VM admission fired (remote_agent.sh exit 75): memory floor,
                # concurrent lane cap, residual timeout, or same-branch lane lock.
                # Retryable backpressure defer ([RES-14]), announced+recorded, not
                # a fault. Discriminate reason from the script's stderr so the pass
                # can surface the right defer text; carry admission_deferred on
                # raw_payload so the orchestrator can recover via a fresh re-dispatch
                # when the VM has headroom / the peer lane finishes.
                stderr_l = (stderr_tail or "").lower()
                if "lane cap" in stderr_l:
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote deferred: VM lane cap reached",
                            blocker=(
                                "remote turn deferred by the VM lane cap (remote_agent.sh exit 75): "
                                "lane cap reached — deferring; retry when a concurrent lane frees."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "admission_deferred": True,
                                "defer_reason": "vm_lane_cap",
                            },
                        )
                    )
                if "residual timeout exhausted" in stderr_l:
                    # Two producers share the phrase with OPPOSITE causes:
                    # pre-dispatch transport vs in-sandbox setup/uv-sync.
                    # Discriminate on the unique producer token so a slow sync
                    # is not reported as "the VM itself is fine" [TEST-15].
                    if "in-sandbox setup" in stderr_l:
                        return _envelope(
                            _remote_unavailable_result(
                                summary=("grok-remote deferred: turn budget exhausted by in-sandbox setup"),
                                blocker=(
                                    "remote turn deferred (remote_agent.sh exit 75): residual "
                                    "timeout exhausted after in-sandbox setup (archive/uv-sync) "
                                    "— retry with more budget or a warm venv; not a transport "
                                    "or free-RAM diagnosis."
                                ),
                                details=stderr_tail,
                                model=effective_model,
                                effort=applied_effort,
                                raw_payload={
                                    "admission_deferred": True,
                                    "defer_reason": "residual_timeout_in_sandbox_setup",
                                },
                            )
                        )
                    # pre-dispatch (or unknown residual) — transport ate budget.
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote deferred: turn budget exhausted before grok started",
                            blocker=(
                                "remote turn deferred (remote_agent.sh exit 75): residual timeout "
                                "exhausted by pre-dispatch probe/push/scp — retry with more budget "
                                "or when transport is faster; the VM itself is fine."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "admission_deferred": True,
                                "defer_reason": "residual_timeout_pre_dispatch",
                            },
                        )
                    )
                if "same-branch lane already active" in stderr_l:
                    # Fourth exit-75 cause: another dispatch of this branch holds
                    # the same-branch lane lock. NOT a memory diagnosis — a false
                    # vm_memory_pressure here sends the operator hunting VM RAM
                    # when the remedy is to wait for the peer lane (serialization).
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote deferred: same-branch lane already active",
                            blocker=(
                                "remote turn deferred by same-branch lane lock (remote_agent.sh "
                                "exit 75): another dispatch of this branch is already active — "
                                "deferring; retry when that lane finishes (branch serialization)."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "admission_deferred": True,
                                "defer_reason": "same_branch_lane_active",
                            },
                        )
                    )
                if "occupying sandbox" in stderr_l:
                    # Fifth exit-75 cause: occupancy re-check after lock — a prior
                    # same-key dispatch lost its shell (and the lock) while its
                    # agent still occupies $SBX under the named scope. NOT memory
                    # pressure: the peer lane owns the sandbox; free RAM will not
                    # help. Becomes common once the scope-named occupancy probe
                    # actually fires.
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote deferred: same-branch lane still occupying sandbox",
                            blocker=(
                                "remote turn deferred by sandbox occupancy (remote_agent.sh "
                                "exit 75): a same-branch lane still occupies the sandbox under "
                                "its systemd scope — deferring; retry when that occupant exits "
                                "(peer owns the sandbox; free VM headroom will not help)."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "admission_deferred": True,
                                "defer_reason": "sandbox_occupied",
                            },
                        )
                    )
                return _envelope(
                    _remote_unavailable_result(
                        summary="grok-remote deferred: VM under memory pressure",
                        blocker=(
                            "remote turn deferred by the VM memory guard (remote_agent.sh exit 75): "
                            "VM MemAvailable below the reserved floor; retry when the VM has headroom "
                            "(dispatched lanes yield to all non-lane work)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "admission_deferred": True,
                            "defer_reason": "vm_memory_pressure",
                        },
                    )
                )
            if rc == _RC_HARD_FAIL:
                # Exit 1 has two producers: the SANDBOX-NOT-REMOTE-SEVERED security
                # assertion, and a fatal uv-sync failure. Neither is transport —
                # labeling them "VM unreachable" invites a retry that re-runs the
                # same posture violation [RES-01][AGT-10].
                stderr_l_1 = (stderr_tail or "").lower()
                if "sandbox not remote-severed" in stderr_l_1 or "not remote-severed" in stderr_l_1:
                    return _envelope(
                        _remote_unavailable_result(
                            summary=("grok-remote security tripwire: sandbox not remote-severed"),
                            blocker=(
                                "remote_agent.sh failed (exit 1): SANDBOX NOT REMOTE-SEVERED — "
                                "security posture assertion; do not retry until the sandbox "
                                "init path is fixed (not a transport/VM-reachability flake)."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "hard_fail_reason": "sandbox_not_remote_severed",
                            },
                        )
                    )
                if "uv sync failed" in stderr_l_1:
                    return _envelope(
                        _remote_unavailable_result(
                            summary="grok-remote failed: in-sandbox uv sync failed",
                            blocker=(
                                "remote_agent.sh failed (exit 1): uv sync failed after fresh "
                                "venv rebuild — dependency sync on the VM; not a transport "
                                "error (retry may help only after lock/index is healthy)."
                            ),
                            details=stderr_tail,
                            model=effective_model,
                            effort=applied_effort,
                            raw_payload={
                                "hard_fail_reason": "uv_sync_failed",
                            },
                        )
                    )
                return _envelope(
                    _remote_unavailable_result(
                        summary="grok-remote hard failure (remote_agent.sh exit 1)",
                        blocker=(
                            "remote_agent.sh failed (exit 1): hard failure on the remote "
                            "path (not classified as transport); failing closed (announced "
                            "+ recorded)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                        raw_payload={
                            "hard_fail_reason": "unclassified_exit_1",
                        },
                    )
                )
            if rc == _RC_USAGE:
                return _envelope(
                    _remote_unavailable_result(
                        summary="grok-remote usage/validation error (remote_agent.sh exit 2)",
                        blocker=(
                            "remote_agent.sh failed (exit 2): usage or validation error — "
                            "fix the dispatch arguments/config (not a transport flake)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                    )
                )
            if rc != _RC_PATCH:
                # VM unreachable / SSH failure / unexpected transport error (S3):
                # fail closed with an announced+recorded reason, do not crash the pass.
                return _envelope(
                    _remote_unavailable_result(
                        summary=f"grok-remote transport failed (remote_agent.sh exit {rc})",
                        blocker=(
                            f"remote_agent.sh failed (exit {rc}) — VM unreachable or transport error; "
                            "failing closed (announced + recorded)."
                        ),
                        details=stderr_tail,
                        model=effective_model,
                        effort=applied_effort,
                    )
                )

            # Post-turn served-model gate BEFORE commit [P13]: a contaminated
            # turn must not produce a local commit. Absent/empty log => None.
            contamination = _detect_grok_build_contamination(debug_file, requested_model=effective_model)
            # Off-box self-verify capture (item 26): parse inside the tempdir (the
            # file lives under tmp). None when no test_cmd shipped or the VM
            # emitted nothing — the worker's OBS-08 enforcement then blocks a
            # commit-landed lane with no captured verify rather than silently pass.
            off_box_sv = _off_box_self_verify_from_json(selfverify_file) if test_cmd else None
            # Enum travels on raw_payload (F5: never widen the four-key whitelist).
            sv_outcome = _self_verify_outcome_from_json(selfverify_file) if test_cmd else None

            if progress_callback:
                progress_callback(WorkerEventName.EXEC_COMPLETE, backend=self.backend_id)

            if contamination is not None:
                blocker, evidence = contamination
                contam_payload: dict[str, Any] = {
                    "composer_violation_evidence": evidence,
                    "attestation": {"status": "failed", "reason": "grok_build_contamination", "pin": effective_model},
                }
                if sv_outcome is not None:
                    contam_payload["self_verify_outcome"] = sv_outcome
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary="grok-remote: grok-build contamination detected in the remote debug log",
                        details=stderr_tail,
                        merge_ready=False,
                        blockers=[blocker],
                        changed_files=[],
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=contam_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )

            # rc == 0 and clean: apply the returned patch locally + commit.
            # Quarantine is a typed blocker (same as exit 3/8 salvage), not an
            # untyped apply/commit crash. Host read I/O is salvage_read_error,
            # not control_path_quarantine. Genuine apply/commit failures re-raise.
            summary = grok_result.summary if grok_result is not None else ""
            try:
                _apply_and_commit(worktree_path, patch_file, lane_id=lane_id, summary=summary)
            except PatchUnreadableError as exc:
                read_payload: dict[str, Any] = {
                    "failure_reason": "salvage_read_error",
                    "salvage_patch_cause": "salvage_read_error",
                }
                if sv_outcome is not None:
                    read_payload["self_verify_outcome"] = sv_outcome
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary=(f"{self.backend_id} refused: host failed to read the turn patch (patch_unreadable)"),
                        details=str(exc).strip()[-500:] or stderr_tail,
                        merge_ready=False,
                        blockers=["salvage_read_error", "patch_unreadable"],
                        changed_files=[],
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=read_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )
            except RuntimeError as exc:
                if "quarantined:" not in str(exc):
                    raise
                quarantine_payload: dict[str, Any] = {
                    "failure_reason": "control_path_quarantine",
                }
                if sv_outcome is not None:
                    quarantine_payload["self_verify_outcome"] = sv_outcome
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary=(f"{self.backend_id} refused: remote patch touches git control paths"),
                        details=str(exc).strip()[-500:] or stderr_tail,
                        merge_ready=False,
                        blockers=["control_path_quarantine"],
                        changed_files=[],
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=quarantine_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )
            committed = _committed_files(worktree_path)
            if grok_result is None:
                # Patch applied + committed, but structured result was unparseable.
                # Fail closed to needs_guidance (never silent-empty); the commit stands.
                # implementation note D9 (edit kind): blockers must contain literal result_unparseable.
                unparseable_payload: dict[str, Any] = {}
                if sv_outcome is not None:
                    unparseable_payload["self_verify_outcome"] = sv_outcome
                return _envelope(
                    BackendResult(
                        handoff_action="needs_guidance",
                        summary=(f"{self.backend_id} turn committed but its structured result was unparseable"),
                        details=stderr_tail,
                        merge_ready=False,
                        blockers=["result_unparseable"],
                        changed_files=committed,
                        response_model=effective_model,
                        reasoning_effort=applied_effort,
                        raw_payload=unparseable_payload,
                        off_box_self_verify=off_box_sv,
                    )
                )

            # Trust the committed file list over grok's self-report; carry the rest through.
            # Merge outcome into a copy of raw_payload — never mutate a dict we do not own.
            success_payload = dict(grok_result.raw_payload or {})
            if sv_outcome is not None:
                success_payload["self_verify_outcome"] = sv_outcome
            return _envelope(
                BackendResult(
                    handoff_action=grok_result.handoff_action,
                    summary=grok_result.summary,
                    details=grok_result.details,
                    tests_run=grok_result.tests_run,
                    blockers=grok_result.blockers,
                    changed_files=committed or grok_result.changed_files,
                    merge_ready=grok_result.merge_ready,
                    token_usage=grok_result.token_usage,
                    response_model=grok_result.response_model or effective_model,
                    reasoning_effort=applied_effort or grok_result.reasoning_effort,
                    raw_payload=success_payload,
                    off_box_self_verify=off_box_sv,
                )
            )
        finally:
            _release_owner_pid(tmp)
            spool_cleanup()
