"""Synchronous offload pass engine (internal S2).

Carves the worker daemon's single-pass internals into a synchronous,
outcome-typed engine: one call validates the lane is actionable, runs the
bounded execute→review→fix loop, enforces a commit gate between execute and
review (review never sees a dirty tree), and returns a typed outcome enum —
never a bare ok/exit-0 the caller has to guess about.

Contract highlights (task plan `internal`):
- Mandatory positive ``token_budget`` and ``timeout_seconds``; the MCP layer
  refuses un-bounded calls before any spend and the engine re-asserts.
- Budget enforcement is fail-closed and three-point: pre-turn admission,
  backend turn bound where supported, post-turn reconciliation. A budgeted
  turn that reports no token usage is a typed ``error``, never
  warn-and-continue.
- ``timeout`` / ``error`` outcomes never re-execute inside the engine;
  recovery is a new explicit dispatch (idempotent on ``dispatch_id``).
- Dirty implementation-lane execute output is checkpointed
  (``wip(offload): <lane_id> checkpoint <n>``); review-lane output stays
  uncommitted so review findings cannot land as implementation work.
- Pass state persists in ``<state_dir>/offload-pass-<pass_id>.json`` so a
  disconnected client can recover the outcome via ``await_offload_pass``.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import html
import json
import logging
import math
import os
import random
import re
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Sequence
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace
from dataclasses import field as dataclass_field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import StrEnum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

_ACTIVE_OFFLOAD_PASS_SWEEP_STATE: ContextVar[dict[str, Any] | None] = ContextVar(
    "active_offload_pass_sweep_state",
    default=None,
)
_ACTIVE_DETACHED_JOB_ID: ContextVar[str | None] = ContextVar("active_detached_job_id", default=None)
_ACTIVE_DETACHED_CHECKPOINT_PASS: ContextVar[tuple[Path, str] | None] = ContextVar(
    "active_detached_checkpoint_pass", default=None
)
_ACTIVE_COMPLETION_DEADLINE: ContextVar[float | None] = ContextVar("completion_deadline", default=None)
_COMPLETION_MARGIN_SECONDS = 30.0


class CompletionDeadlineExceeded(TimeoutError):
    """RES-13 lexicons/engineering.md:124: overdue completion stays retryable."""

    reason = "completion_deadline_exceeded"

    def __init__(self) -> None:
        super().__init__(self.reason)


def completion_timeout_seconds(pass_state: dict[str, Any]) -> float:
    """Bound status (30s), collection (300s), engine, and cleanup margin."""
    config = pass_state.get("completion_config")
    value = config.get("timeout_seconds", 60.0) if isinstance(config, dict) else 60.0
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("completion timeout_seconds must be finite and positive")
    return 30.0 + 300.0 + float(value) + _COMPLETION_MARGIN_SECONDS


def _completion_remaining() -> float:
    deadline = _ACTIVE_COMPLETION_DEADLINE.get()
    if deadline is None:
        return float("inf")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CompletionDeadlineExceeded()
    return remaining


from workbay_handoff_mcp.bounded_call import run_with_daemon_timeout
from workbay_handoff_mcp.runtime import get_runtime_config
from workbay_handoff_mcp.shared_schema import connect_handoff_db
from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held
from workbay_protocol.lifecycle_coordination import LifecycleLease, LifecycleLeaseTimeout
from workbay_protocol.review_impact import is_blocking

from workbay_orchestrator_mcp.lanes import BRIEF_ACK_SUCCESS_OUTCOMES
from workbay_orchestrator_mcp.orchestration.backend_registry import (
    BACKENDS,
    COST_REMOTE,
    backend_supports_adapter_timeout_bounds,
    backend_supports_token_budget_cycle_bounds,
    backend_supports_token_telemetry,
    cost_class_for_backend,
    token_telemetry_presentation_for_backend,
    token_telemetry_total_key_for_backend,
)
from workbay_orchestrator_mcp.orchestration.grok_lane_config import (
    ENGINE_GIT_IDENTITY as _ENGINE_GIT_IDENTITY,
)
from workbay_orchestrator_mcp.orchestration.lane_context_packet import lookup_related_prior
from workbay_orchestrator_mcp.orchestration.lane_lifecycle_contracts import RemoteJobIdentity
from workbay_orchestrator_mcp.orchestration.lane_publication_fence import (
    LanePassPublicationRefused,
    lane_publication_lease,
)
from workbay_orchestrator_mcp.orchestration.offload_pass_index import lane_pass_records, record_pass_id
from workbay_orchestrator_mcp.orchestration.offload_preflight import (
    TOKEN_BUDGET_BELOW_FLOOR_PREFIX,
    OffloadPreflightError,
    check_disk_headroom,
    ensure_lane_manifest_for_offload,
)
from workbay_orchestrator_mcp.orchestration.offload_profiles import OFFLOAD_AGENT_PROFILES
from workbay_orchestrator_mcp.orchestration.remote_agent_root import resolve_remote_agent_root
from workbay_orchestrator_mcp.orchestration.remote_job_ledger import RemoteJobLedger, RemoteJobLedgerError
from workbay_orchestrator_mcp.orchestration.remote_phase_budget import RemotePhaseBudget
from workbay_orchestrator_mcp.orchestration.stop_reasons import (
    SALVAGE_STOP_REASONS as _SALVAGE_STOP_REASONS,
)
from workbay_orchestrator_mcp.orchestration.stop_reasons import (
    STOP_REASONS_CHECKPOINT as _STOP_REASONS_CHECKPOINT,
)
from workbay_orchestrator_mcp.orchestration.stop_reasons import admission_defer_summary


class OffloadOutcome(StrEnum):
    """Canonical terminal and progress outcomes produced by one offload pass."""

    HANDOFF_READY = "handoff_ready"
    # implementation note R3: clean review lane with a parseable findings block harvested.
    REVIEW_COMPLETE = "review_complete"
    # internal: routing persisted for a junior-to-senior switch.
    ESCALATED = "escalated"
    NEEDS_GUIDANCE = "needs_guidance"
    RATE_LIMITED = "rate_limited"
    TRANSPORT_FAILURE = "transport_failure"
    # Green self-verified committed work without a genuine worker question.
    COMPLETED_UNREVIEWED = "completed_unreviewed"
    NO_ACTIONABLE_WORK = "no_actionable_work"
    UNCOMMITTED_WORK = "uncommitted_work"
    TOKEN_BUDGET_EXCEEDED = "token_budget_exceeded"
    TIMEOUT = "timeout"
    ERROR = "error"
    STILL_RUNNING = "still_running"
    LANE_NOT_FOUND = "lane_not_found"
    SELF_VERIFY_FAILED = "self_verify_failed"
    SELF_VERIFY_INCONCLUSIVE = "self_verify_inconclusive"
    COMPOSER_VIOLATION_QUARANTINED = "composer_violation_quarantined"
    CHECKPOINT = "checkpoint"
    SERVER_STALE_RESTART_REQUIRED = "server_stale_restart_required"
    REMOTE_JOB_PENDING = "remote_job_pending"
    ADMISSION_DEFERRED = "admission_deferred"
    ADMISSION_REFUSED = "admission_refused"
    DISPATCH_REFUSED = "dispatch_refused"
    REMOTE_REQUIRED = "remote_required"
    WORKTREE_UNRECOVERABLE = "worktree_unrecoverable"
    WORKTREE_CLAIM_HELD = "worktree_claim_held"
    CEREMONY_FAILED = "ceremony_failed"
    REVIEW_INCOMPLETE = "review_incomplete"
    VERIFICATION_FAILED = "verification_failed"
    VERIFICATION_SKIPPED = "verification_skipped"
    VERIFICATION_HARNESS_ERROR = "verification_harness_error"
    GUIDANCE_CONTRACT_INVALID = "guidance_contract_invalid"


PASS_OUTCOMES = frozenset(outcome.value for outcome in OffloadOutcome)

# Canonical completed-pass products are shared with the lane brief-ack gate.
# Keep this compatibility name as an alias rather than a second hand-maintained
# outcome set; new success outcomes must update the canonical set in ``lanes``.
PASS_SUCCESS_OUTCOMES = BRIEF_ACK_SUCCESS_OUTCOMES

_SELF_VERIFY_PASS_GATE_STATUSES = frozenset({"passed", "skipped"})


def __getattr__(name: str) -> Any:
    """Build the legacy presentation view from registry-owned capabilities."""
    if name != "_TOKEN_TELEMETRY_BACKEND_PRESENTATION":
        raise AttributeError(name)
    return {
        backend_name: (
            {
                "usage_source": presentation.usage_source,
                "total_key": presentation.total_key,
                "pending_reason": presentation.pending_reason,
            }
            if (presentation := spec.capabilities.token_telemetry_presentation) is not None
            and presentation.usage_source is not None
            else None
        )
        for backend_name, spec in BACKENDS.items()
    }


def _build_token_telemetry_payload(
    run_ctx: Any,
    *,
    backend: str,
    lane_id: str,
    token_budget: int,
    token_telemetry_supported: bool,
) -> dict[str, Any]:
    """Build pass-end token telemetry using the backend capability contract."""
    cumulative = int(getattr(run_ctx, "cumulative_tokens", 0) or 0)
    main_tokens: dict[str, Any] | None = None
    try:
        from workbay_orchestrator_mcp.orchestration.main_agent_tokens import (  # noqa: PLC0415
            read_main_agent_turn_tokens,
        )

        main_tokens = read_main_agent_turn_tokens()
    except Exception:  # noqa: BLE001 — degrade loudly via unavailable line
        main_tokens = None

    resolved_backend = str(getattr(run_ctx, "backend", None) or backend)
    resolved_telemetry_supported = (
        token_telemetry_supported
        if resolved_backend == str(backend)
        else backend_supports_token_telemetry(resolved_backend)
    )
    presentation = None if resolved_telemetry_supported else token_telemetry_presentation_for_backend(resolved_backend)
    subagents: list[dict[str, Any]] = []
    if resolved_telemetry_supported:
        if cumulative > 0:
            subagents.append(
                {
                    "lane_id": lane_id,
                    "usage_source": "observed",
                    "total_tokens": cumulative,
                }
            )
        else:
            subagents.append(
                {
                    "lane_id": lane_id,
                    "usage_source": None,
                    "total_tokens": None,
                    "reason": "unavailable",
                }
            )
        usage_source_label = "observed" if cumulative > 0 else "unavailable"
    elif presentation is not None and presentation.usage_source is not None:
        if cumulative > 0:
            subagents.append(
                {
                    "lane_id": lane_id,
                    "usage_source": presentation.usage_source,
                    "total_tokens": cumulative,
                }
            )
            usage_source_label = presentation.usage_source
        else:
            subagents.append(
                {
                    "lane_id": lane_id,
                    "usage_source": presentation.usage_source,
                    "total_tokens": None,
                    "reason": presentation.pending_reason,
                }
            )
            usage_source_label = "unavailable"
    else:
        subagents.append(
            {
                "lane_id": lane_id,
                "usage_source": None,
                "total_tokens": None,
                "reason": "unavailable",
            }
        )
        usage_source_label = "unavailable"

    try:
        from workbay_orchestrator_mcp.orchestration.turn_summary import (  # noqa: PLC0415
            render_turn_token_summary,
        )

        summary = render_turn_token_summary(main_tokens, subagents)
    except Exception:  # noqa: BLE001 — summary is additive; never fail the pass
        lane_ids = [str(entry.get("lane_id") or "unknown") for entry in subagents]
        fallback_lines = ["main-agent: unavailable"] + [f"subagent {lid}: unavailable" for lid in lane_ids]
        summary = {
            "text": "\n".join(fallback_lines),
            "lines": fallback_lines,
            "main_agent_available": False,
            "observed_total": 0,
            "grok_context_approx_total": 0,
            "total_tokens_by_usage_source": {},
            "unavailable_lanes": lane_ids,
        }

    tokens: dict[str, Any] = {
        "token_budget": token_budget,
        "token_telemetry": resolved_telemetry_supported,
        "usage_source": usage_source_label,
        "summary": summary,
        "summary_text": summary.get("text") if isinstance(summary, dict) else str(summary),
    }
    if resolved_telemetry_supported:
        tokens["cumulative_total"] = cumulative
    else:
        tokens["cumulative_total"] = None
        if presentation is not None and presentation.total_key is not None and cumulative > 0:
            tokens[presentation.total_key] = cumulative
    return tokens


def _pass_ok_from_outcome_and_self_verify_gate(outcome: str, self_verify_gate: str) -> bool:
    """Return the lane ``ok`` flag only for a successful, verified pass."""
    return outcome in PASS_SUCCESS_OUTCOMES and self_verify_gate in _SELF_VERIFY_PASS_GATE_STATUSES


# Candidate names intersected with PASS_OUTCOMES so a retired member cannot
# linger as a dead attach site (`no_tests_executed` is not live; the live
# equivalent is `self_verify_inconclusive`).
_FAILURE_PRIOR_ART_OUTCOME_CANDIDATES = frozenset(
    {
        "needs_guidance",
        "error",
        "transport_failure",
        "timeout",
        "self_verify_failed",
        "self_verify_inconclusive",
        "uncommitted_work",
        "rate_limited",
        "token_budget_exceeded",
    }
)
FAILURE_PRIOR_ART_OUTCOMES = frozenset(_FAILURE_PRIOR_ART_OUTCOME_CANDIDATES & PASS_OUTCOMES)

# Advisory lookup is optional context. A short bound keeps a hung embedding
# provider from stalling pass publish; the handoff package owns the shared
# abandonable daemon-call helper used by both paths.
FAILURE_PRIOR_ART_LOOKUP_TIMEOUT_SECONDS = 5.0
FAILURE_PRIOR_ART_QUERY_FRAGMENT_CHARS = 300
_FAILURE_PRIOR_ART_FRAGMENT_KEYS = ("description", "summary", "title", "text")
_FAILURE_PRIOR_ART_ERROR_PAYLOAD = {
    "status": "error",
    "results": [],
    "note": "related_prior:error",
}
_FAILURE_PRIOR_ART_TIMEOUT_PAYLOAD = {
    "status": "error",
    "results": [],
    "note": "failure_prior_art:timeout",
}

# Dual-axis terminal discriminators (internal / engoutcome).
# The outcome enum used to collapse "did work land?" and "did the handoff
# ceremony complete?" into one value; ceremony failure then overwrote a
# work-landed success into error/needs_guidance. Callers branch on these
# fields without parsing prose [OBS-08].
WORK_STATUS_LANDED = "landed"
WORK_STATUS_NOT_LANDED = "not_landed"
WORK_STATUS_UNCOMMITTED_SALVAGED = "uncommitted_salvaged"
WORK_STATUSES = frozenset(
    {
        WORK_STATUS_LANDED,
        WORK_STATUS_NOT_LANDED,
        WORK_STATUS_UNCOMMITTED_SALVAGED,
    }
)

CEREMONY_STATUS_CLEAN = "clean"
CEREMONY_STATUS_FAILED = "failed"
CEREMONY_STATUS_NOT_ATTEMPTED = "not_attempted"
CEREMONY_STATUSES = frozenset({CEREMONY_STATUS_CLEAN, CEREMONY_STATUS_FAILED, CEREMONY_STATUS_NOT_ATTEMPTED})

# Exit codes that mean the gate did not meaningfully execute tests.
# 4/5 = pytest usage / no tests collected; 70 = off-box not_run; 75 = suite-lock
# deferral; 126/127 = harness not-executable / not-found. Exit 2 (bash misuse /
# syntax) is handled via output_tail patterns so a real suite interrupted with
# exit 2 is not mislabeled unrun.
# Bare pytest after a red suite exits 1 (uses 0–5 only). Codes 70/75/126/127
# are reachable only via a harness wrapper and MUST be corroborated by the
# absence of red-suite evidence in the tail (see ``_self_verify_is_inconclusive``).
# These must not share the self_verify_failed bucket with a red suite.
_SELF_VERIFY_INCONCLUSIVE_EXIT_CODES = frozenset({4, 5, 70, 75, 126, 127})

# Red-suite evidence in output_tail. Presence vetoes promotion to unrun
# regardless of exit code, stamped outcome, or unrun-shaped substrings that
# merely appear inside assertion text (OFFP-A-R1 / MC1–MC7).
# Non-zero count required: "0 failed" is the wrong polarity for a suite-failed
# detector (OFFP2A-R1-01). Error-count / ERROR nodeid alternations cover
# collection/fixture/setup failures that carry no "failed" token (OFFP2A-R1-02).
_SELF_VERIFY_RED_TAIL_RE = re.compile(
    r"^\s*FAILED\s+\S+"
    r"|^\s*ERROR\s+\S+"
    r"|\b[1-9]\d*\s+failed\b"
    r"|\b[1-9]\d*\s+errors?\b"
    r"|^={3,}\s*FAILURES\s*={3,}",
    re.IGNORECASE | re.MULTILINE,
)

# Tail / stamped-outcome evidence that the TEST_CMD never reached a real suite.
# Syntax-error alternative is anchored to a shell diagnostic line so a red
# pytest run whose assertion text *quotes* the phrase is not typed unrun.
# Includes residual double-entity ``&&`` markers (``&amp;amp;``) that appear
# only when the suite never ran (red evidence above vetoes quote contamination).
_SELF_VERIFY_UNRUN_TAIL_RE = re.compile(
    r"(?m)^(?:ba)?sh:[^\n]*syntax error near unexpected token"
    r"|&amp;amp;"
    r"|self_verify_outcome=(?:harness_error|not_run)"
    r"|(?:^|[\s:`])(?:ba)?sh: .*(?:command not found|No such file or directory|Permission denied)",
    re.IGNORECASE | re.MULTILINE,
)

# Off-box / consumer-side stamped outcomes that mean the gate did not run.
_SELF_VERIFY_UNRUN_OUTCOMES = frozenset({"harness_error", "not_run"})

# Double-or-deeper HTML-entity encoding of ``&&`` (two amp-chains, each with
# at least two ``amp;`` units). Single-escaped ``&amp;&amp;`` and unrelated
# entities (``&amp;``, ``&lt;``, numeric ``&#38;``) are intentionally NOT
# matched — identity on those inputs (OFFP-B-R1 narrow repair).
# Case policy (OFFP2B-R1-01 / OFFP2B-R1-04): matcher is IGNORECASE; the decode
# step folds named-entity names to lowercase before html.unescape so mixed-case
# spans (``&Amp;``) are admitted *and* decodable. stdlib html.unescape handles
# ``&amp;``/``&AMP;`` but not mixed case — without the fold, IGNORECASE admits
# spans that never converge and raise ValueError.
_DOUBLE_ENTITY_AND_AND_RE = re.compile(r"&(?:amp;){2,}&(?:amp;){2,}", re.IGNORECASE)

# Named HTML entity token used to case-fold before stdlib unescape (decoder
# half of the case policy above).
_NAMED_ENTITY_TOKEN_RE = re.compile(r"&([A-Za-z][A-Za-z0-9]*);")

# Max html.unescape iterations when repairing one matched span. Exhaustion is
# a hard failure (never ship a partial residual entity string).
_NORMALIZE_AND_AND_MAX_UNESCAPE = 8


def _html_unescape_named_ci(text: str) -> str:
    """``html.unescape`` with case-insensitive named-entity recognition.

    Folds ``&Amp;`` / ``&aMp;`` → ``&amp;`` before decoding so the normalizer's
    IGNORECASE matcher and its decoder share one case policy (OFFP2B-R1-01).
    """
    folded = _NAMED_ENTITY_TOKEN_RE.sub(lambda m: f"&{m.group(1).lower()};", text)
    return html.unescape(folded)


# implementation note S3 / exit-8: checkpoint / salvage gates — derived from the single
# owner registry (stop_reasons.STOP_REASON_REGISTRY). Names kept for existing
# callers/tests; membership is never re-typed here.


def normalize_lane_test_cmd(test_cmd: str | None) -> str | None:
    """Return ``test_cmd`` with double-entity ``&&`` corruption repaired, or identity.

    Finding OFFLOAD-LANE-TESTCMD-AMPERSAND-DOUBLE-HTML-ESCAPED-AT-SHELL: a
    literal ``&&`` that reaches bash as ``&amp;amp;&amp;amp;`` is rejected with
    ``syntax error near unexpected token ;&`` and the pass reports a red-like
    failure even though no suite ran.

    Producer search (OFFP-B-R1 / this repair lane): zero production
    ``html.escape`` sites on the ``test_cmd`` path in this monorepo (control
    probe: ``def normalize_lane_test_cmd`` and ``def run_offload_pass_engine``
    are found; ``html.escape`` hits are this docstring and synthetic test
    fixtures only). Visibility ends at the sandbox boundary — an external
    MCP client / HTML round-trip / paste may still introduce the form. With
    no in-tree producer, this function is a narrow, logged peel of the
    specific double-or-deeper entity-encoded ``&&`` shape, and the engine
    converges the stored ``worktree_lanes.test_cmd`` row when it fires so
    operator-visible storage matches the executed value.

    Contract:
    - No double-entity ``&&`` match → byte-identical identity (other entities
      untouched).
    - Match → replace each span with literal ``&&``; log both forms.
    - Bounded unescape cannot reach ``&&`` → raise (never ship partial).
    Emptiness is not this function's job. ``classify_test_cmd`` is the owner:
    not-supplied stays ``None``; a cleared command stays ``""`` (run no test
    command, as distinct from leaving the stored row in place).
    """
    from workbay_orchestrator_mcp.orchestration.lane_test_cmd import (  # noqa: PLC0415
        classify_test_cmd,
    )

    state, text = classify_test_cmd(test_cmd)
    if state == "not_supplied":
        return None
    if state == "cleared":
        return ""
    cmd = text if text is not None else ""
    if not _DOUBLE_ENTITY_AND_AND_RE.search(cmd):
        return cmd

    def _repair_span(match: re.Match[str]) -> str:
        span = match.group(0)
        cur = span
        for _ in range(_NORMALIZE_AND_AND_MAX_UNESCAPE):
            # Case-insensitive named-entity decode (matches IGNORECASE matcher).
            nxt = _html_unescape_named_ci(cur)
            if nxt == cur:
                break
            cur = nxt
            if cur == "&&":
                return "&&"
        if cur != "&&":
            raise ValueError(
                "normalize_lane_test_cmd: could not fully repair entity-encoded "
                f"&& (span={span!r} residual={cur!r}); refusing partial result"
            )
        return "&&"

    fixed = _DOUBLE_ENTITY_AND_AND_RE.sub(_repair_span, cmd)
    if fixed != cmd:
        logger.warning(
            "normalize_lane_test_cmd repaired double-entity &&: before=%r after=%r",
            cmd,
            fixed,
        )
    return fixed


def _converge_stored_lane_test_cmd(*, task_ref: str, lane_id: str, test_cmd: str) -> bool:
    """Write the repaired ``test_cmd`` back to ``worktree_lanes`` (source converge).

    Returns True when at least one row was updated. Best-effort: never raises
    into the pass outcome path — the runtime value is already repaired on the
    consumer; storage convergence is the durable fix for other readers.
    Returns False (not a false success) when the row is absent or the DB is
    unopenable (OFFP2B-R1-03). The UPDATE is scoped by task_ref AND lane_id so
    a peel cannot rewrite sibling lanes under the same task.

    Connection lifecycle (OFFP2B-R1-02): ``sqlite3.connect`` as a context
    manager manages the *transaction* only (commit/rollback); the connection
    stays open. Wrap with ``contextlib.closing`` so each peel closes its handle.
    """
    try:
        # closing() owns the connection; the inner with-conn still commits.
        with contextlib.closing(connect_handoff_db(_handoff_db_path())) as conn:
            cur = conn.execute(
                """
                UPDATE worktree_lanes
                SET test_cmd = ?, updated_at = datetime('now')
                WHERE task_ref = ? AND lane_id = ?
                """,
                (test_cmd, task_ref, lane_id),
            )
            conn.commit()
            updated = int(cur.rowcount or 0) > 0
    except Exception as exc:  # noqa: BLE001 — storage converge must not abort pass
        logger.warning(
            "normalize_lane_test_cmd: failed to converge stored test_cmd for task_ref=%s lane_id=%s: %s",
            task_ref,
            lane_id,
            exc,
        )
        return False
    if updated:
        logger.warning(
            "normalize_lane_test_cmd: converged worktree_lanes.test_cmd for task_ref=%s lane_id=%s to %r",
            task_ref,
            lane_id,
            test_cmd,
        )
    return updated


def _host_admission_should_park(orchestrator_root: Path, backend: str | None = None) -> str | None:
    """internal pre-turn re-check: reason to park a long pass, or None.

    Only a hard *refuse* dimension (rising memory pressure / swap floor / width
    0) parks mid-pass — slot capacity does not apply because the worker already
    holds its slot. ``WORKBAY_HOSTGOV_DISABLE`` and non-``enforce`` modes never
    park. Never raises (a probe failure returns None — the pass proceeds).

    The re-check runs under the pass's OWN cost class (resolved from ``backend``):
    a fully off-box COST_REMOTE lane is never gated by local memory, so it never
    parks on local pressure mid-pass — otherwise a multi-cycle grok-remote pass
    would defer on exactly the local condition the exemption ignores at dispatch
    (internal). Unknown/None backend => conservative heavy.
    """
    if os.environ.get("WORKBAY_HOSTGOV_DISABLE") == "1":
        return None
    try:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (
            cost_class_for_backend,
        )
        from workbay_orchestrator_mcp.orchestration.host_resources import (
            evaluate_admission,
            load_host_memory_policy,
            probe_host,
            record_admission_telemetry,
        )

        policy = load_host_memory_policy(orchestrator_root)
        if policy.enforcement != "enforce":
            return None
        # Forward backend identity + live per-backend held ledger so the
        # per-backend local cap can evaluate (internal). Mid-pass only
        # parks on *refuse*; slots_full_outcome defaults to defer, so a saturated
        # peer does not abort a worker that already holds its own slot.
        held_slots_by_backend = None
        backend_key = str(backend) if backend else None
        if backend_key and policy.per_backend_local_cap > 0:
            try:
                from workbay_orchestrator_mcp.orchestration.host_resources import (  # noqa: PLC0415
                    count_held_backend_slots,
                    locks_root,
                )

                held_slots_by_backend = {
                    backend_key: count_held_backend_slots(
                        locks_root(orchestrator_root),
                        backend_key,
                        policy.per_backend_local_cap,
                    )
                }
            except Exception:  # noqa: BLE001 — ledger failure degrades to empty count
                held_slots_by_backend = {backend_key: 0}
        elif backend_key:
            held_slots_by_backend = {backend_key: 0}
        decision = evaluate_admission(
            probe_host(),
            cost_class_for_backend(backend),
            policy,
            held_slots=0,
            backend=backend_key,
            held_slots_by_backend=held_slots_by_backend,
        )
    except Exception:  # noqa: BLE001 — a probe failure must not abort a live pass
        return None
    if decision.decision != "refuse":
        return None
    record_admission_telemetry(orchestrator_root, decision, surface="pre_turn_recheck")
    return decision.reason


#: Stage markers carried on every outcome payload so the gate can branch without
#: git archaeology (internal). ``None`` means no failure stage (success
#: / pre-pass refusal / non-stage terminal like timeout with no phase fault).
FAILED_STAGES = frozenset(
    {
        "execute",
        "self_verify",
        "review",
        "handoff",
        "attestation",
        "history",
    }
)


def _payload_failed_stage(failed_stage: str | None) -> str | None:
    """Keep declared stage markers and clear unrecognized payload values."""
    if failed_stage is not None and failed_stage not in FAILED_STAGES:
        return None
    return failed_stage


# Machine-readable prefixes on engine refusals for remote implement dispatch.
# TOKEN_BUDGET_BELOW_FLOOR_PREFIX is owned by offload_preflight; this module
# owns the test-command prefix so a cycle is not required to name the input.
TEST_CMD_UNUSABLE_PREFIX = "test_cmd_unusable: "


# The reset value's unit is provider/header-defined, not derivable from
# magnitude alone (up0824-rlo-local-f03, API-05). RFC 9110's Retry-After is
# numeric delay-SECONDS (never milliseconds) or an HTTP-date; the
# OpenRouter-family headers this diff targets (x-ratelimit-reset /
# rate_limit_reset) are documented MILLISECONDS, epoch or duration. Convert
# per header; magnitude only breaks the epoch-vs-duration tie within a unit —
# a 13+ digit value is unambiguously a millisecond epoch (a millisecond
# DURATION that large would be 400+ years).
_MS_UNIT_RESET_HEADERS = frozenset({"x-ratelimit-reset", "rate_limit_reset"})
_RETRY_AFTER_NUMERIC_RE = re.compile(r"(\d+)(?:\s*(ms|milliseconds|s|sec|secs|second|seconds))?")
_MS_EPOCH_FLOOR = 1_000_000_000_000  # 13-digit floor


def _retry_after_seconds(reset: str | None, *, header: str | None = None, now: float | None = None) -> int | None:
    if reset is None:
        return None
    value = reset.strip()
    current = time.time() if now is None else now

    # RFC 9110 HTTP-date form — the only non-numeric Retry-After shape.
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        now_dt = datetime.fromtimestamp(current, tz=timezone.utc)
        return max(0, int((parsed - now_dt).total_seconds()))

    match = _RETRY_AFTER_NUMERIC_RE.fullmatch(value.lower())
    if match is None:
        return None
    n = int(match.group(1))
    unit_suffix = match.group(2)
    header_key = header.strip().lower() if header else None

    if unit_suffix in ("ms", "milliseconds"):
        return max(0, round(n / 1000))
    if n >= _MS_EPOCH_FLOOR:
        return max(0, int(n / 1000 - current))
    if n >= 1_000_000_000:
        return max(0, int(n - current))
    if header_key in _MS_UNIT_RESET_HEADERS:
        return max(0, round(n / 1000))
    return n


def _worker_daemon_module() -> Any:
    """Resolve the worker_daemon module with bare-name-first semantics.

    Mirrors ``api._import_orchestration_module`` so tests that patch the
    module the API layer resolves patch the same object this engine calls.
    """
    module = sys.modules.get("worker_daemon")
    if module is not None:
        return module
    from workbay_orchestrator_mcp.orchestration import worker_daemon  # noqa: PLC0415

    return worker_daemon


REVIEW_DEFER_RETRY_DELAYS_SECONDS = (30.0, 60.0, 120.0)


def _review_defer_wait(seconds: float) -> None:
    time.sleep(seconds)


def _review_admission_defer_reason(stop: StopIteration) -> str | None:
    """Read a typed review deferral from either module identity used at runtime."""
    cause = stop.__cause__
    if cause is None:
        return None
    for module_name in ("review_runner", "workbay_orchestrator_mcp.orchestration.review_runner"):
        review_runner = sys.modules.get(module_name)
        defer_type = getattr(review_runner, "ReviewAdmissionDeferred", None) if review_runner is not None else None
        if isinstance(defer_type, type) and isinstance(cause, defer_type):
            return str(getattr(cause, "defer_reason", None) or "vm_admission")
    return None


# ---------------------------------------------------------------------------
# Pass state persistence (disconnect recovery)
# ---------------------------------------------------------------------------


def _pass_state_path(state_dir: Path, pass_id: str) -> Path:
    return Path(state_dir) / f"offload-pass-{pass_id}.json"


def _terminal_pass_terminal_at(payload: dict[str, Any]) -> float | None:
    """Return ``terminal_at`` as epoch seconds for a terminal pass, else ``None``.

    ``None`` means the pass is not provably terminal: an unrecognized or
    non-terminal status, a ``done`` pass without an explicit known terminal
    outcome, or a missing/unparseable ``terminal_at``.
    """

    from .pass_terminality import pass_outcome_is_terminal  # noqa: PLC0415

    status = payload.get("status")
    if not isinstance(status, str) or (status != "done" and status not in _TERMINAL_NON_DONE_PASS_STATUSES):
        return None
    result = payload.get("result")
    if status == "done" and not (
        isinstance(result, dict)
        and isinstance(result.get("outcome"), str)
        and result["outcome"] in PASS_OUTCOMES
        and pass_outcome_is_terminal(result["outcome"])
    ):
        return None
    return _history_timestamp(payload.get("terminal_at"))


def _latest_pass_payload(records: Sequence[Any]) -> dict[str, Any] | None:
    """Choose the latest pass using its recorded start/sequence and identity.

    A pass can be rewritten as checkpoints or when it completes, so file mtime
    and terminal time do not describe when the pass started. Legacy single-pass
    lanes remain readable; ambiguous multi-pass history fails closed, except
    that legacy records which are all terminal are ordered by ``terminal_at``.
    """

    candidates: list[tuple[int | None, float | None, str, dict[str, Any]]] = []
    for record in records:
        payload = getattr(record, "payload", None)
        if not isinstance(payload, dict):
            continue
        pass_id_value = payload.get("pass_id") or getattr(record, "pass_id", None)
        if not isinstance(pass_id_value, str) or not pass_id_value:
            continue
        sequence_value = payload.get("pass_sequence", payload.get("sequence"))
        sequence = sequence_value if type(sequence_value) is int and sequence_value >= 0 else None
        raw_started_at = payload.get("started_at") or payload.get("created_at")
        started_at: float | None = None
        if isinstance(raw_started_at, str) and raw_started_at.strip():
            try:
                parsed = datetime.fromisoformat(raw_started_at.strip().replace("Z", "+00:00"))
            except ValueError:
                parsed = None
            if parsed is not None:
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                started_at = parsed.astimezone(timezone.utc).timestamp()
        candidates.append((sequence, started_at, pass_id_value, {**payload, "pass_id": pass_id_value}))

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0][3]
    if all(sequence is not None for sequence, _started_at, _pass_id, _payload in candidates):
        candidates.sort(
            key=lambda item: (
                item[0],
                item[1] if item[1] is not None else float("-inf"),
                item[2],
            )
        )
    elif all(started_at is not None for _sequence, started_at, _pass_id, _payload in candidates):
        candidates.sort(
            key=lambda item: (
                item[1],
                item[0] if item[0] is not None else -1,
                item[2],
            )
        )
    else:
        # DATA-15: wall-clock ``terminal_at`` is NOT a safe order for passes in
        # general (clock skew, rewrites). It is used here only because every
        # candidate is terminal: no running pass can be hidden by the
        # ordering, so the caller's retire verdict cannot flip between
        # "terminal" and "not terminal" whichever record is picked. Any
        # non-terminal candidate or unparseable terminal_at keeps the
        # SECD-05 fail-closed ``None``.
        by_terminal_at: list[tuple[float, str, dict[str, Any]]] = []
        for _sequence, _started_at, pass_id, payload in candidates:
            terminal_at = _terminal_pass_terminal_at(payload)
            if terminal_at is None:
                return None
            by_terminal_at.append((terminal_at, pass_id, payload))
        by_terminal_at.sort(key=lambda item: (item[0], item[1]))
        return by_terminal_at[-1][2]
    return candidates[-1][3]


_REHARVEST_KEY = "reharvest"


@contextlib.contextmanager
def _pass_publication_lock(path: Path, *, deadline: float | None = None):
    """Bounded per-pass EX; callers take lane SH first, release before index."""
    assert_no_write_lock_held("pass publication lock")
    resource = path.with_name(f".{path.name}.publication")
    try:
        with LifecycleLease(resource, exclusive=True, timeout=1.0, deadline=deadline):
            yield
    except LifecycleLeaseTimeout as exc:
        raise LanePassPublicationRefused("lane_pass_publication_busy") from exc


def _first_pass_lane_cycle_binding(task_ref: str, lane_id: str) -> dict[str, Any]:
    """One RO snapshot; close it before resolving the authoritative source base."""
    binding = {
        "v": 1,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "row_id": None,
        "cycle_epoch": None,
        "worktree_path": None,
        "branch": None,
        "source_base_sha": None,
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    try:
        config = get_runtime_config()
        deadline = time.monotonic() + min(0.5, _completion_remaining())
        with contextlib.closing(connect_handoff_db(config.db_path, read_only=True, deadline=deadline)) as conn:
            rows = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=? LIMIT 2",
                (task_ref, lane_id),
            ).fetchall()
            row = dict(rows[0]) if len(rows) == 1 else None
        if row is None:
            return binding
        binding.update(
            row_id=row["id"],
            cycle_epoch=row.get("cycle_epoch"),
            worktree_path=row.get("worktree_path"),
            branch=row.get("branch"),
        )
        from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
            _try_authoritative_lane_base_sha,
        )

        source, detail = _try_authoritative_lane_base_sha(
            row=row,
            task_ref=task_ref,
            lane_id=lane_id,
            repo=config.workspace_root,
        )
        binding["source_base_sha"] = source
        if detail is not None:
            logger.warning("lane_cycle_binding_source_unknown: %s", detail)
    except Exception:  # noqa: BLE001 — unknown authority blocks release, never publication
        logger.warning("lane_cycle_binding_unknown for %s/%s", task_ref, lane_id, exc_info=True)
    return binding


_COMPLETION_IDENTITY_HOLES = ("start_head_ref", "capability_receipt_id", "capability_config_digest", "dispatch_id")


def _completion_requires_live_probe(backend: Any) -> bool:
    """Apply the selected backend's AgentSpec live-sandbox-preflight contract.

    The adapter authorizes spawn with complete probe evidence only when the
    AgentSpec runs a live sandbox probe. Unregistered backends stay probe-complete.
    """
    from workbay_orchestrator_mcp.orchestration import backend_spec, wave_dispatch  # noqa: PLC0415

    if not isinstance(backend, str) or backend not in backend_spec.invocation_backend_ids():
        return True
    try:
        # The admission mirror reads only the backend of the selected spec.
        return bool(wave_dispatch._requires_live_sandbox_preflight(SimpleNamespace(backend=backend)))
    except Exception:  # noqa: BLE001 — an unresolvable contract keeps the strict check
        return True


def _merge_completion_identity(
    pass_id: str,
    previous: dict[str, Any],
    payload: dict[str, Any],
    *,
    validate_receipt: bool = True,
) -> dict[str, Any]:
    """D-contract: keep first; authenticate hole-fill; log conflicts without raising."""
    frozen = dict(previous["completion_config"])
    candidate = payload.get("completion_config")
    if not isinstance(candidate, dict):
        return frozen
    conflicts = [
        key for key, value in frozen.items() if value is not None and key in candidate and candidate[key] != value
    ]
    if conflicts:
        logger.warning("completion_identity_conflict pass_id=%s fields=%s", pass_id, sorted(conflicts))
    holes = [key for key in _COMPLETION_IDENTITY_HOLES if frozen.get(key) is None and candidate.get(key) is not None]
    if not holes or not validate_receipt:
        return frozen
    try:
        from workbay_orchestrator_mcp.orchestration.preflight_attestation import (
            validate_capability_receipt,  # noqa: PLC0415
        )

        # Validate the persisted invocation, not identity asserted by a rewrite.
        identity = {**payload, **previous}
        config = {**candidate, **{key: value for key, value in frozen.items() if value is not None}}
        root = identity.get("orchestrator_root")
        if root is None or candidate.get("pass_id", pass_id) != pass_id:
            return frozen
        # The attestation store has its own longer lock budget. Validate before
        # taking publication permission, with a bounded wait and no DB/lease
        # carried into the worker. A timeout only declines optional hole-fill.
        valid = run_with_daemon_timeout(
            lambda: validate_capability_receipt(
                Path(root),
                candidate.get("capability_receipt_id") or frozen.get("capability_receipt_id"),
                task_ref=identity.get("task_ref"),
                lane_id=identity.get("lane_id"),
                pass_id=pass_id,
                dispatch_id=config.get("dispatch_id"),
                worktree_path=identity.get("worktree_path"),
                config_digest=config.get("capability_config_digest"),
                require_complete_identity=False,
                require_complete_capability=_completion_requires_live_probe(config.get("backend")),
            ),
            timeout_seconds=min(0.5, _completion_remaining()),
            timeout_message="completion identity validation timed out",
        )
        if valid.get("ok") is True:
            for key in holes:
                frozen[key] = candidate[key]
    except Exception:  # noqa: BLE001 — identity enrichment must never strand a running pass
        logger.warning("completion_identity_validation_failed pass_id=%s", pass_id, exc_info=True)
    return frozen


def _pass_lane_identity(payload: dict[str, Any], previous: dict[str, Any]) -> tuple[str | None, str | None]:
    values = []
    for key in ("task_ref", "lane_id"):
        value = previous.get(key) or payload.get(key)
        values.append(value if isinstance(value, str) and value else None)
    return values[0], values[1]


def _merge_pass_state(
    pass_id: str,
    payload: dict[str, Any],
    previous_record: dict[str, Any] | None,
    invocation_token: str | None,
    validated_previous: dict[str, Any],
    validated_completion: dict[str, Any] | None,
    first_publication: bool,
) -> dict[str, Any]:
    previous = previous_record or {}
    payload = dict(payload)
    # These fields are generated only here, never accepted from a callback.
    payload.pop("lane_cycle_binding", None)
    payload.pop("invocation_token", None)
    payload.pop("superseded_invocations", None)
    task_ref, lane_id = _pass_lane_identity(payload, previous)
    if task_ref is not None:
        payload["task_ref"] = task_ref
    if lane_id is not None:
        payload["lane_id"] = lane_id
    if first_publication and task_ref is not None and lane_id is not None:
        payload["lane_cycle_binding"] = _first_pass_lane_cycle_binding(task_ref, lane_id)
    elif "lane_cycle_binding" in previous:
        payload["lane_cycle_binding"] = previous["lane_cycle_binding"]
    history = list(previous.get("superseded_invocations") or [])
    new_invocation = invocation_token is not None and invocation_token != previous.get("invocation_token")
    if new_invocation and previous_record is not None:
        history.append(
            {
                "invocation_token": previous.get("invocation_token"),
                "completion_config": previous.get("completion_config"),
            }
        )
        # All rescue/progress identity is invocation-scoped; the native binding
        # remains first-publication-only, including for legacy records.
        previous = {}
        previous_record = None
    token = invocation_token or previous.get("invocation_token")
    if token is not None:
        payload["invocation_token"] = token
    if history:
        payload["superseded_invocations"] = history
    for key in (
        "started_at",
        "dispatch_id",
        "lane_spec",
        "branch",
        "worktree_path",
        "agent_stream_path",
        "job_id",
        "attempt",
        "detached_checkpoint",
        "session",
        "orchestrator_root",
        # Audit of a reopened terminal receipt; must survive the engine's own rewrites.
        _REHARVEST_KEY,
    ):
        if previous.get(key) is not None and (key != "agent_stream_path" or not payload.get(key)):
            if key == "detached_checkpoint":
                old = previous[key]
                new = payload.get(key)
                if (
                    isinstance(old, dict)
                    and old.get("prepared") is True
                    and isinstance(new, dict)
                    and new.get("job_id") == old.get("job_id")
                    and new.get("head") == old.get("head")
                    and "prepared" not in new
                ):
                    continue
            if key == _REHARVEST_KEY:
                old = previous[key]
                new = payload.get(key)
                # Each locked reopen appends the receipt it supersedes. Accept only that
                # append; any other rewrite keeps the recorded history unchanged.
                if (
                    isinstance(old, dict)
                    and isinstance(new, dict)
                    and isinstance(old.get("superseded_results"), list)
                    and isinstance(new.get("superseded_results"), list)
                    and new["superseded_results"][: len(old["superseded_results"])] == old["superseded_results"]
                    and {k: v for k, v in new.items() if k != "superseded_results"}
                    == {k: v for k, v in old.items() if k != "superseded_results"}
                ):
                    continue
            payload[key] = previous[key]
    if previous_record is not None and previous.get("started_at") is None:
        # A legacy record's rewrite time cannot establish when its pass started.
        payload.pop("started_at", None)
    elif payload.get("started_at") is None:
        payload["started_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(previous.get("completion_config"), dict):
        payload["completion_config"] = (
            validated_completion
            if previous == validated_previous and validated_completion is not None
            else _merge_completion_identity(pass_id, previous, payload, validate_receipt=False)
        )
    previous_progress = previous.get("progress")
    next_progress = payload.get("progress")
    if isinstance(previous_progress, dict):
        payload["progress"] = {
            **previous_progress,
            **(next_progress if isinstance(next_progress, dict) else {}),
        }
    return payload


def write_pass_state(
    state_dir: Path,
    pass_id: str,
    payload: dict[str, Any],
    *,
    invocation_token: str | None = None,
    expected_invocation_token: str | None = None,
) -> bool:
    _completion_remaining()
    assert_no_write_lock_held("pass publication")
    if invocation_token is not None and re.fullmatch(r"[0-9a-f]{32}", invocation_token) is None:
        raise ValueError("invocation_token must be 32 lowercase hex characters")
    path = _pass_state_path(state_dir, pass_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline: float | None = None
    while True:
        hint = read_pass_state(state_dir, pass_id) or {}
        validated_completion = None
        if isinstance(hint.get("completion_config"), dict) and invocation_token in (None, hint.get("invocation_token")):
            validated_completion = _merge_completion_identity(pass_id, hint, payload)
        # A routing retry retains the original lock-acquisition deadline.
        if deadline is None:
            deadline = time.monotonic() + min(1.0, _completion_remaining())
        task_ref, lane_id = _pass_lane_identity(payload, hint)
        with contextlib.ExitStack() as leases:
            if task_ref is not None and lane_id is not None:
                try:
                    lease = leases.enter_context(
                        lane_publication_lease(state_dir, task_ref, lane_id, deadline=deadline)
                    )
                except LifecycleLeaseTimeout as exc:
                    raise LanePassPublicationRefused("lane_pass_publication_busy") from exc
                if lease.borrowed:
                    raise LanePassPublicationRefused("lane_pass_publication_destructive_owner")
            with _pass_publication_lock(path, deadline=deadline):
                previous_record = read_pass_state(state_dir, pass_id)
                # A competing first writer may establish a different lane.
                # Retry in total lock order, never acquire a second lane SH here.
                if _pass_lane_identity(payload, previous_record or {}) != (task_ref, lane_id):
                    continue
                if (
                    expected_invocation_token is not None
                    and (previous_record or {}).get("invocation_token") != expected_invocation_token
                ):
                    return False
                published = _merge_pass_state(
                    pass_id, payload, previous_record, invocation_token, hint, validated_completion, not path.exists()
                )
                tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
                try:
                    with tmp.open("w", encoding="utf-8") as stream:
                        stream.write(json.dumps(published, sort_keys=False))
                        stream.flush()
                        os.fsync(stream.fileno())
                    _completion_remaining()
                    os.replace(tmp, path)
                    directory_fd = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    tmp.unlink(missing_ok=True)
            # The derived index is short and bounded; lane SH lasts through
            # visibility, while the per-pass lock is already released.
            if task_ref is not None and lane_id is not None:
                try:
                    record_pass_id(
                        state_dir,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        pass_id=pass_id,
                        index_lock_timeout_seconds=max(0.0, deadline - time.monotonic()),
                    )
                except Exception:  # noqa: BLE001 — readers heal the derived index
                    logger.exception("could not record offload pass %s in the lane index", pass_id)
            return True


def read_pass_state(state_dir: Path, pass_id: str) -> dict[str, Any] | None:
    path = _pass_state_path(state_dir, pass_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _bounded_finding_fragment(finding: Any) -> str:
    """Return a capped scalar snippet from a finding; never ``str(finding)``.

    A raising ``__str__`` / ``__repr__`` on the finding object must not escape
    the typed-degrade path. Prefer a known text field and slice the string
    before any concatenation.
    """
    if isinstance(finding, dict):
        for key in _FAILURE_PRIOR_ART_FRAGMENT_KEYS:
            value = finding.get(key)
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped[:FAILURE_PRIOR_ART_QUERY_FRAGMENT_CHARS]
        return ""
    if isinstance(finding, str):
        return finding.strip()[:FAILURE_PRIOR_ART_QUERY_FRAGMENT_CHARS]
    return ""


def _failure_prior_art_query_text(
    *,
    outcome: str,
    failed_stage: str | None,
    lane_id: Any,
    backend: Any,
    model: Any,
    findings: list[Any] | None,
) -> str:
    query_text = (
        f"failed pass outcome={outcome} failed_stage={failed_stage or 'none'} "
        f"lane={lane_id} backend={backend} model={model}"
    )
    if isinstance(findings, list) and findings:
        fragment = _bounded_finding_fragment(findings[0])
        if fragment:
            query_text = f"{query_text} {fragment}"
    return query_text


def _call_related_prior_with_timeout(fn: Any, *, timeout_seconds: float) -> Any:
    """Run *fn* with a wall-clock bound in an abandonable daemon worker.

    A stuck embedding provider raises ``TimeoutError`` instead of blocking
    forever, and the worker cannot delay interpreter shutdown.
    """
    return run_with_daemon_timeout(
        fn,
        timeout_seconds=timeout_seconds,
        timeout_message=f"failure_prior_art lookup timed out after {timeout_seconds}s",
    )


def _attach_failure_prior_art(result: dict[str, Any]) -> None:
    """Annotate a failed, unpublished pass with bounded related-prior lookup.

    Must never raise. A ``timeout`` pass outcome skips the provider (budget
    already exhausted) and attaches the typed timeout degrade. Other attach
    outcomes run ``lookup_related_prior`` inside a wall-clock bound.
    """
    outcome = result.get("outcome")
    if outcome not in FAILURE_PRIOR_ART_OUTCOMES or result.get("commit_landed"):
        return
    if outcome == "timeout":
        result["failure_prior_art"] = dict(_FAILURE_PRIOR_ART_TIMEOUT_PAYLOAD)
        return
    try:
        query_text = _failure_prior_art_query_text(
            outcome=str(outcome),
            failed_stage=result.get("failed_stage"),
            lane_id=result.get("lane_id"),
            backend=result.get("backend"),
            model=result.get("model"),
            findings=result.get("findings") if isinstance(result.get("findings"), list) else None,
        )
        task_ref = str(result.get("task_ref") or "")

        def _call() -> dict[str, Any]:
            return lookup_related_prior(query_text=query_text, task_ref=task_ref)

        result["failure_prior_art"] = _call_related_prior_with_timeout(
            _call,
            timeout_seconds=FAILURE_PRIOR_ART_LOOKUP_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        logger.warning("failure-prior-art lookup timed out: %s", exc)
        result["failure_prior_art"] = dict(_FAILURE_PRIOR_ART_TIMEOUT_PAYLOAD)
    except Exception as exc:  # noqa: BLE001 — never raise from the result builder
        logger.warning("failure-prior-art lookup failed: %s", exc)
        result["failure_prior_art"] = dict(_FAILURE_PRIOR_ART_ERROR_PAYLOAD)


# ---------------------------------------------------------------------------
# Commit gate
# ---------------------------------------------------------------------------


class WorktreeUnrecoverableError(RuntimeError):
    """Git can no longer resolve a path that was admitted as a worktree."""


class _PassAuthorityChanged(RuntimeError):
    """Durable invocation/runtime authority changed after the first source publication."""

    def __init__(self, fields: Sequence[str]) -> None:
        self.fields = tuple(fields)
        super().__init__(f"pass publication authority changed before external execution: {', '.join(self.fields)}")


def _worktree_dirty(worktree_path: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    # Fail closed: a non-zero git status (corrupt repo, missing binary, permission
    # error) must NOT be read as "clean" — that would silently skip the commit gate
    # and let review run against an unknown tree state. Raise so the caller maps it
    # to a typed error / uncommitted_work rather than proceeding as if clean.
    if result.returncode != 0:
        error_type = WorktreeUnrecoverableError if result.returncode == 128 else RuntimeError
        raise error_type(
            f"git status failed in {worktree_path} (rc={result.returncode}): {result.stderr.strip() or 'no stderr'}"
        )
    return bool(result.stdout.strip())


def build_checkpoint_commit_message(*, lane_id: str, checkpoint_number: int, job_id: str | None = None) -> str:
    """Pure builder for the checkpoint commit subject line.

    Neutral template only: lane id and checkpoint number. No vendor, backend,
    model, or trailer. ``lane_id`` is an *identifier*, not prose: sanitize for
    line-structure safety and length, but never run the commit-subject *prose*
    screen on it. That screen collapses credit-looking identifiers into the
    single fallback ``remote turn``, erasing lane identity from history
    (REVBYL-B-02). Identifier credit is handled by the lane-id sanitizer's
    discriminating digest path instead.
    """
    from workbay_orchestrator_mcp.orchestration.commit_subject import (  # noqa: PLC0415
        sanitize_lane_id_for_commit_message,
    )

    prefix = "wip(offload): "
    suffix = f" checkpoint {checkpoint_number}"
    # Git's hard wrap is on the whole subject, not on the lane_id component alone.
    max_lane_len = max(0, 72 - len(prefix) - len(suffix))
    safe_lane = sanitize_lane_id_for_commit_message(lane_id, max_len=max_lane_len)
    message = f"{prefix}{safe_lane}{suffix}"
    if job_id:
        message = f"{message}\n\nRemote-Job-Id: {job_id}"
    return message


def _checkpoint_commit(
    worktree_path: Path,
    lane_id: str,
    checkpoint_number: int,
    *,
    job_id: str | None = None,
) -> str | None:
    """Create the engine-identity checkpoint commit; return its sha or None."""
    add = subprocess.run(
        ["git", "-C", str(worktree_path), "add", "-A"],
        capture_output=True,
        text=True,
        check=False,
    )
    if add.returncode != 0:
        return None
    message = build_checkpoint_commit_message(lane_id=lane_id, checkpoint_number=checkpoint_number, job_id=job_id)
    checkpoint_pass = _ACTIVE_DETACHED_CHECKPOINT_PASS.get()
    if job_id is not None and checkpoint_pass is not None:
        return _commit_bound_detached_checkpoint(worktree_path, job_id, message, checkpoint_pass)
    commit = subprocess.run(
        ["git", "-C", str(worktree_path), *_ENGINE_GIT_IDENTITY, "commit", "-m", message],
        capture_output=True,
        text=True,
        check=False,
    )
    if commit.returncode != 0:
        logger.warning(
            "checkpoint commit failed lane_id=%s returncode=%s stderr=%s",
            lane_id,
            commit.returncode,
            (commit.stderr or "")[:500],
        )
        return None
    committed_ref = "HEAD"
    if job_id is not None:
        # Resolve the commit Git just created, rather than a mutable HEAD an
        # operator could move between commit and the checkpoint receipt write.
        created = re.search(r"^\[[^\]\n]* ([0-9a-f]{4,64})\]", commit.stdout or "", re.MULTILINE)
        if created is None:
            return None
        committed_ref = f"{created.group(1)}^{{commit}}"
    sha = subprocess.run(
        ["git", "-C", str(worktree_path), "rev-parse", "--verify", committed_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    return sha.stdout.strip() or None


def _commit_bound_detached_checkpoint(
    worktree: Path, job_id: str, message: str, checkpoint_pass: tuple[Path, str]
) -> str | None:
    """Durably prepare an exact commit object before atomically advancing HEAD.

    A kill before or after update-ref leaves the same prepared object available
    to replay. A later commit with a copied job trailer cannot replace its SHA.
    """
    state_dir, pass_id = checkpoint_pass
    state = read_pass_state(state_dir, pass_id) or {}
    deadline = _bounded_git_probe_deadline(_ACTIVE_COMPLETION_DEADLINE.get())

    def probe(*args: str) -> str:
        result = _run_git_evidence_probe(worktree, *args, deadline=deadline)
        if result is None or result.returncode != 0:
            raise RuntimeError("detached checkpoint commit failed")
        return result.stdout.strip()

    try:
        source = probe("rev-parse", "HEAD")
        tree = probe("write-tree")
        receipt = state.get("detached_checkpoint")
        if receipt is not None:
            sha = _recorded_detached_checkpoint(state, job_id)
            if not sha or not isinstance(receipt, dict) or receipt.get("prepared") is not True:
                return None
            if probe("show", "-s", "--format=%P", sha) != source or probe("rev-parse", f"{sha}^{{tree}}") != tree:
                return None
        else:
            sha = probe(
                *_ENGINE_GIT_IDENTITY, "-c", "commit.gpgsign=false", "commit-tree", tree, "-p", source, "-m", message
            )
            write_pass_state(
                state_dir,
                pass_id,
                {**state, "detached_checkpoint": {"job_id": job_id, "head": sha, "prepared": True}},
            )
        probe("update-ref", "-m", f"commit: {message.splitlines()[0]}", "HEAD", sha, source)
        return sha
    except (OSError, RuntimeError, _GitEvidenceProbeTimeout):
        return None


def _checkpoint_if_dirty(
    worktree_path: Path,
    lane_id: str,
    checkpoints: list[str],
    *,
    dry_run: bool = False,
) -> bool:
    """Checkpoint any dirty tree. Returns False only when the checkpoint failed.

    ``dry_run=True`` never mutates HEAD: dirty or clean, the probe reports success
    without writing so inspection-mode passes cannot land real commits
    (OFFLOAD-DRY-RUN-COMMITS-TO-THE-BRANCH-UNDER-REVIEW-01).
    """
    if dry_run:
        return True
    if not _worktree_dirty(worktree_path):
        return True
    _completion_remaining()
    job_id = _ACTIVE_DETACHED_JOB_ID.get()
    sha = _checkpoint_commit(worktree_path, lane_id, len(checkpoints) + 1, job_id=job_id)
    if sha is None:
        return False
    checkpoints.append(sha)
    checkpoint_pass = _ACTIVE_DETACHED_CHECKPOINT_PASS.get()
    if job_id is not None and checkpoint_pass is not None:
        state_dir, pass_id = checkpoint_pass
        state = read_pass_state(state_dir, pass_id) or {}
        write_pass_state(
            state_dir,
            pass_id,
            {**state, "detached_checkpoint": {"job_id": job_id, "head": sha}},
        )
    return True


def _remote_lane_key(branch: str) -> str:
    """Mirror ``remote_agent.sh`` LANE_KEY derivation (branch sanitise + 8-hex)."""
    import hashlib  # noqa: PLC0415
    import re  # noqa: PLC0415

    branch_hash = hashlib.sha256(branch.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
    key = re.sub(r"[^A-Za-z0-9-]", "-", branch)[:40]
    while key.startswith("-"):
        key = key[1:]
    key = key or "lane"
    return f"{key}-{branch_hash}"


def _detached_artifact_paths(argv: Any, worktree_path: Path) -> dict[str, Path]:
    """Read the output paths from a persisted remote-agent build command."""
    if not isinstance(argv, list) or len(argv) < 2 or argv[1] != "build":
        return {}
    output_flags = {
        "--out": "patch",
        "--result-out": "result",
        "--debug-out": "debug",
        "--stream-out": "stream",
        "--selfverify-out": "selfverify",
        "--uncommitted-out": "uncommitted",
        "--phases-out": "phases",
        "--provenance-out": "provenance",
    }
    paths: dict[str, Path] = {}
    for index, value in enumerate(argv[:-1]):
        name = output_flags.get(value) if isinstance(value, str) else None
        target = argv[index + 1]
        if name is None or not isinstance(target, str) or not target.strip():
            continue
        path = Path(target).expanduser()
        if not path.is_absolute():
            path = worktree_path / path
        paths[name] = path
    return paths


_DETACHED_PATCH_SOURCE_MOVED = "detached_patch_source_moved"


def _recorded_detached_checkpoint(pass_state: dict[str, Any], job_id: str) -> str | None:
    receipt = pass_state.get("detached_checkpoint")
    if receipt is None:
        return None
    if (
        isinstance(receipt, dict)
        and receipt.get("job_id") == job_id
        and isinstance(receipt.get("head"), str)
        and re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", receipt["head"])
    ):
        return receipt["head"]
    return ""  # A malformed or foreign host receipt never falls back to ancestry.


def _detached_checkpoint_refusal(
    *,
    worktree_path: Path,
    patch_file: Path | None,
    job_id: str,
    start_head: str | None,
    checkpoint_head: str | None = None,
    expected_tip: str | None = None,
    checkpoint_prepared: bool = False,
) -> str | None:
    """Bind replay to the source or the exact job checkpoint with its exact patch tree.

    A job trailer on an ancestor cannot certify today's HEAD. Even on HEAD the
    trailer is insufficient: the checkpoint must have the authenticated source
    as its sole parent and contain exactly that source plus the retained patch.
    The private index never touches the lane index or working files.
    """
    if not isinstance(start_head, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", start_head):
        return _DETACHED_PATCH_SOURCE_MOVED
    if checkpoint_head is not None and (
        not isinstance(checkpoint_head, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", checkpoint_head)
    ):
        return _DETACHED_PATCH_SOURCE_MOVED
    probe_deadline = _bounded_git_probe_deadline(_ACTIVE_COMPLETION_DEADLINE.get())

    def probe(*args: str, env: dict[str, str] | None = None) -> str:
        result = _run_git_evidence_probe(worktree_path, *args, deadline=probe_deadline, env=env)
        if result is None or result.returncode != 0:
            raise RuntimeError("detached checkpoint probe unavailable")
        return result.stdout.strip()

    try:
        head = probe("rev-parse", "HEAD")
        if expected_tip is not None and head != expected_tip:
            return _DETACHED_PATCH_SOURCE_MOVED
        prepared_at_source = checkpoint_prepared and head == start_head
        if checkpoint_head is not None and head != checkpoint_head and not prepared_at_source:
            return _DETACHED_PATCH_SOURCE_MOVED
        checkpoint = probe("log", "-1", "--format=%H", "--grep", f"^Remote-Job-Id: {job_id}$")
        if prepared_at_source:
            checkpoint = checkpoint_head or ""
        if not checkpoint:
            return None if head == start_head else _DETACHED_PATCH_SOURCE_MOVED
        if (head != checkpoint and not prepared_at_source) or probe(
            "show", "-s", "--format=%P", checkpoint
        ) != start_head:
            return _DETACHED_PATCH_SOURCE_MOVED
        if checkpoint_head is None:
            return _DETACHED_PATCH_SOURCE_MOVED
        if patch_file is None or not patch_file.is_file() or not patch_file.stat().st_size:
            return _DETACHED_PATCH_SOURCE_MOVED
        with tempfile.TemporaryDirectory(prefix="detached-checkpoint-") as scratch:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(scratch) / "index"))
            probe("read-tree", start_head, env=env)
            probe("apply", "--cached", str(patch_file), env=env)
            expected_tree = probe("write-tree", env=env)
        if probe("rev-parse", f"{checkpoint}^{{tree}}") != expected_tree:
            return _DETACHED_PATCH_SOURCE_MOVED
    except (OSError, RuntimeError, _GitEvidenceProbeTimeout):
        return _DETACHED_PATCH_SOURCE_MOVED
    return None


def _apply_detached_patch_once(
    *,
    worktree_path: Path,
    patch_file: Path | None,
    lane_id: str,
    summary: str,
    job_id: str,
    start_head: str | None = None,
    checkpoint_head: str | None = None,
    checkpoint_prepared: bool = False,
) -> str | None:
    """Use the attached adapter's apply helper and recognize an interrupted replay.

    Returns a typed refusal reason, or None when the patch is applied or this
    job already applied it. ``start_head`` is the authenticated source the VM
    patch was generated against; only it or this job's exact patch checkpoint
    may consume the retained capture, including jobs with no patch.
    """
    _completion_remaining()
    if start_head is None:
        # Legacy direct callers lack a provenance carrier. Product installation
        # supplies the authenticated source explicitly, never this inference.
        deadline = _bounded_git_probe_deadline(_ACTIVE_COMPLETION_DEADLINE.get())
        checkpoint = _run_git_evidence_probe(
            worktree_path, "log", "-1", "--format=%P", "--grep", f"^Remote-Job-Id: {job_id}$", deadline=deadline
        )
        if checkpoint is not None and checkpoint.returncode == 0 and checkpoint.stdout.strip():
            start_head = checkpoint.stdout.strip()
            legacy_head = _run_git_evidence_probe(worktree_path, "rev-parse", "HEAD", deadline=deadline)
            checkpoint_head = (
                legacy_head.stdout.strip() if legacy_head is not None and legacy_head.returncode == 0 else ""
            )
        else:
            source = _run_git_evidence_probe(worktree_path, "rev-parse", "HEAD", deadline=deadline)
            start_head = source.stdout.strip() if source is not None and source.returncode == 0 else ""
    refusal = _detached_checkpoint_refusal(
        worktree_path=worktree_path,
        patch_file=patch_file,
        job_id=job_id,
        start_head=start_head,
        checkpoint_head=checkpoint_head,
        checkpoint_prepared=checkpoint_prepared,
    )
    if refusal is not None:
        return refusal
    if patch_file is None or not patch_file.is_file() or not patch_file.stat().st_size:
        return None
    if _git_stdout(worktree_path, "rev-parse", "HEAD") != start_head:
        # The guard already authenticated the installed checkpoint and its tree.
        return None
    reverse_check = ["git", "-C", str(worktree_path), "apply", "--reverse", "--cached", "--check", str(patch_file)]
    if _ACTIVE_COMPLETION_DEADLINE.get() is None:
        already_staged = subprocess.run(reverse_check, capture_output=True, text=True, check=False).returncode == 0
    else:
        # RES-02/RES-13: the replay check gets only the remaining lifecycle
        # deadline. Overrunning it kills the process group and raises the typed,
        # retryable deadline; a late answer is never an "already applied" None.
        timeout = _completion_remaining()
        proc = subprocess.Popen(
            reverse_check,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=(os.name == "posix"),
        )
        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            _terminate_git_probe_group(proc)
            raise CompletionDeadlineExceeded() from exc
        already_staged = proc.returncode == 0
    if already_staged:
        return None
    from workbay_orchestrator_mcp.orchestration.adapters.remote_exec import (  # noqa: PLC0415
        _apply_and_commit,
    )

    _completion_remaining()
    _apply_and_commit(worktree_path, patch_file, lane_id=lane_id, summary=summary, commit=False)
    return None


def _stamp_detached_off_box_capture(
    result_payload: dict[str, Any], raw_payload: dict[str, Any], selfverify_file: Path | None
) -> None:
    """Stamp the off-box self-verify capture where the attached adapter puts it.

    The collected ``--selfverify-out`` file is the only authority. The four-key
    capture lives on the top-level result (read by
    ``worker_daemon._consume_off_box_self_verify`` and the verified-gate writer).
    The enum rides ``raw_payload.self_verify_outcome`` only alongside an accepted
    capture, so a malformed file never leaves a "passed" claim behind. A typed
    absence reason otherwise explains the missing capture. Any capture or enum a
    remote result.json carried is discarded.
    """
    from workbay_orchestrator_mcp.orchestration.adapters import remote_exec  # noqa: PLC0415

    result_payload.pop("off_box_self_verify", None)
    raw_payload.pop("off_box_self_verify", None)
    raw_payload.pop("self_verify_outcome", None)
    raw_payload.pop(remote_exec._OFF_BOX_SELF_VERIFY_ABSENCE_KEY, None)
    if selfverify_file is None:
        return
    read = remote_exec._off_box_self_verify_read(selfverify_file)
    if read.capture is not None:
        result_payload["off_box_self_verify"] = read.capture
        if read.outcome is not None:
            raw_payload["self_verify_outcome"] = read.outcome
    if read.absence_reason is not None:
        raw_payload[remote_exec._OFF_BOX_SELF_VERIFY_ABSENCE_KEY] = read.absence_reason


def _stamp_detached_remainder_evidence(raw_payload: dict[str, Any], uncommitted_file: Path | None) -> None:
    """Stamp dirty-remainder evidence from the observed local artifact, as the adapter does.

    A remote result.json cannot claim a remainder: the four remainder keys and
    ``work_status`` are removed, then restated from ``--uncommitted-out`` alone.
    """
    from workbay_orchestrator_mcp.orchestration.adapters import remote_exec  # noqa: PLC0415

    for key in (
        "uncommitted_remainder_present",
        "uncommitted_remainder_path",
        "uncommitted_remainder_bytes",
        "uncommitted_remainder_read_error",
        "work_status",
    ):
        raw_payload.pop(key, None)
    if uncommitted_file is None:
        return
    raw_payload.update(remote_exec._uncommitted_remainder_payload(uncommitted_file))


def _stamp_detached_source_provenance(
    raw_payload: dict[str, Any],
    provenance_file: Path | None,
    *,
    worktree_path: Path,
    start_head: str | None,
) -> str | None:
    """Stamp the gate-authored source receipt, bound to the recorded source head.

    Returns the adapter's failure reason (``missing``/``malformed``/...) when the
    receipt does not validate against the host branch and the recorded head's tree,
    else None. A worker-authored ``source_provenance`` is never trusted.
    """
    from workbay_orchestrator_mcp.orchestration.adapters import remote_exec  # noqa: PLC0415

    raw_payload.pop("source_provenance", None)
    if provenance_file is None:
        return "missing"
    # Each probe gets only the remaining completion deadline and raises the typed
    # CompletionDeadlineExceeded when it elapses (RES-13).
    bound_head = start_head or _git_stdout(worktree_path, "rev-parse", "HEAD")
    head_tree = _git_stdout(worktree_path, "rev-parse", f"{bound_head}^{{tree}}") if bound_head else ""
    provenance, failure = remote_exec._source_provenance_from_json(
        provenance_file,
        branch=_git_stdout(worktree_path, "rev-parse", "--abbrev-ref", "HEAD"),
        head_sha=bound_head or None,
        head_tree=head_tree or None,
    )
    # A validation that finished after the deadline is never accepted.
    _completion_remaining()
    if provenance is None:
        return failure or "malformed"
    raw_payload["source_provenance"] = provenance
    if provenance.get("model_process_started") is not True:
        raw_payload["execution_failure"] = "model_process_not_started"
    return None


def _refuse_detached_result(result_payload: dict[str, Any], raw_payload: dict[str, Any], reason: str) -> None:
    """Turn a harvested result into a typed, non-green refusal. Nothing is applied."""
    raw_payload["failure_reason"] = reason
    result_payload["handoff_action"] = "needs_guidance"
    result_payload["merge_ready"] = False
    original_summary = str(result_payload.get("summary") or "").strip()
    result_payload["summary"] = f"Detached result refused ({reason}). {original_summary}".rstrip()
    blockers = list(result_payload.get("blockers") or [])
    if reason not in blockers:
        blockers.insert(0, reason)
    result_payload["blockers"] = blockers


def _install_detached_result(
    *,
    run_ctx: Any,
    job_record: dict[str, Any],
    worktree_path: Path,
    state_dir: Path,
    job_id: str,
    lane_kind: str | None = None,
    start_head: str | None = None,
) -> bool:
    """Load collected result evidence and apply its patch before the shared pass gates.

    ``lane_kind`` is the engine's confirmed authority. This path skips
    ``worker_daemon._execute_phase``, the only other writer of ``run_ctx.lane_kind``,
    so the caller's value is stamped here before the patch gate reads it.
    ``start_head`` is the recorded source the VM patch was generated against.
    """
    spec = job_record.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    paths = _detached_artifact_paths(spec.get("argv"), worktree_path)
    result_file = paths.get("result")
    from workbay_orchestrator_mcp.orchestration.adapters import remote_exec  # noqa: PLC0415

    backend_result = remote_exec._result_from_json(result_file) if result_file is not None else None
    if backend_result is None:
        result_payload: dict[str, Any] = {
            "handoff_action": "needs_guidance",
            "summary": "Detached remote job finished without a readable result artifact.",
            "details": "The collected result.json was absent, oversized, or unparseable.",
            "blockers": ["detached_result_unavailable"],
            "merge_ready": False,
            "raw_payload": {"outcome": "transport_failure", "failure_reason": "detached_result_unavailable"},
        }
    else:
        backend_result = remote_exec._stamp_backend_result_token_usage(
            backend_result,
            debug_file=paths.get("debug"),
            stream_file=paths.get("stream"),
            backend_id=spec.get("backend"),
        )
        result_payload = backend_result.to_dict()
    raw_payload = result_payload.get("raw_payload")
    raw_payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    raw_payload.update(
        {
            "job_id": job_id,
            "attempt": spec.get("attempt", 0),
            "remote_exec_mode": "detached",
        }
    )
    _stamp_detached_off_box_capture(result_payload, raw_payload, paths.get("selfverify"))
    _stamp_detached_remainder_evidence(raw_payload, paths.get("uncommitted"))
    provenance_failure = _stamp_detached_source_provenance(
        raw_payload,
        paths.get("provenance"),
        worktree_path=worktree_path,
        start_head=start_head,
    )
    effective_lane_kind = lane_kind if lane_kind is not None else getattr(run_ctx, "lane_kind", None)
    run_ctx.lane_kind = effective_lane_kind
    claims_change = result_payload.get("merge_ready") is True or bool(result_payload.get("changed_files"))
    refusal: str | None = None
    if provenance_failure is not None and (claims_change or effective_lane_kind == "implement"):
        # The attached adapter refuses a claimed change without its receipt; a patch
        # is never applied without one.
        refusal = f"source_provenance_{provenance_failure}"
    elif effective_lane_kind == "implement":
        source_provenance = raw_payload.get("source_provenance") or {}
        authenticated_head = start_head or source_provenance.get("remote_source_commit")
        pass_state = read_pass_state(state_dir, str(spec.get("pass_id") or "unknown")) or {}
        checkpoint_receipt = pass_state.get("detached_checkpoint")
        refusal = _apply_detached_patch_once(
            worktree_path=worktree_path,
            patch_file=paths.get("patch"),
            lane_id=str(spec.get("lane_id") or run_ctx.config.lane_id),
            summary=str(result_payload.get("summary") or ""),
            job_id=job_id,
            start_head=authenticated_head,
            checkpoint_head=_recorded_detached_checkpoint(pass_state, job_id),
            checkpoint_prepared=isinstance(checkpoint_receipt, dict) and checkpoint_receipt.get("prepared") is True,
        )
    if refusal is not None:
        _refuse_detached_result(result_payload, raw_payload, refusal)
    result_payload["raw_payload"] = raw_payload

    result_path = state_dir / f"detached-result-{str(spec.get('pass_id') or 'unknown')}.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = result_path.with_name(f"{result_path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(result_payload, sort_keys=True), encoding="utf-8")
    os.replace(temporary, result_path)
    run_ctx.final_result_path = result_path
    run_ctx.final_result_snapshot = dict(result_payload)
    run_ctx.current_process_ran = True
    token_entry = result_payload.get("token_usage_totals")
    if isinstance(token_entry, dict):
        _worker_daemon_module()._accumulate_run_ctx_tokens(run_ctx, result_payload)
    return True


def _co_located_remote_sandbox(worktree_path: Path) -> Path | None:
    """Best-effort path to a co-located remote_agent sandbox for this branch.

    When the orchestrator host is the same machine as ``WORKBAY_REMOTE_AGENT_ROOT``
    (gate layout), uncommitted remote work lives under
    ``$HOME/<agent_root>/<LANE_KEY>`` — not under the linked host worktree. Returns
    None when the sandbox is absent or unreadable.
    """
    branch = _git_stdout(worktree_path, "rev-parse", "--abbrev-ref", "HEAD")
    if not branch or branch == "HEAD":
        return None
    try:
        agent_root = resolve_remote_agent_root()
    except ValueError:
        return None
    root = Path(agent_root) if agent_root.startswith("/") else Path.home() / agent_root
    sbx = root / _remote_lane_key(branch)
    if not sbx.is_dir():
        return None
    if (sbx / ".git").exists() or (sbx / ".workbay-lane-sandbox").is_file():
        return sbx
    return None


def _backend_is_remote(backend: str | None) -> bool:
    """True for off-box remote backends whose agent writes outside the host worktree."""
    if not backend:
        return False
    try:
        from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
            backend_runs_self_verify_off_box,
        )

        if backend_runs_self_verify_off_box(backend):
            return True
    except Exception:  # noqa: BLE001 — registry must not break salvage
        pass
    return str(backend).endswith("-remote")


def _salvage_probe_paths(backend: str | None, worktree_path: Path) -> list[Path]:
    """Trees the end-of-pass salvage arm must probe.

    Remote backends: prefer the co-located remote sandbox (where the agent
    actually wrote), then the host worktree (exit-3 may have applied a patch
    there). Local backends: host worktree only.
    """
    host = Path(worktree_path)
    if not _backend_is_remote(backend):
        return [host]
    paths: list[Path] = []
    sbx = _co_located_remote_sandbox(host)
    if sbx is not None:
        paths.append(sbx)
    paths.append(host)
    # De-dupe while preserving order.
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        try:
            key = str(path.resolve())
        except OSError:
            pass
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _salvage_checkpoint_if_dirty(
    backend: str | None,
    worktree_path: Path,
    lane_id: str,
    checkpoints: list[str],
    *,
    dry_run: bool = False,
) -> bool:
    """Checkpoint dirty trees across salvage probe paths. Fail only if a dirty tree could not commit."""
    if dry_run:
        return True
    any_failed = False
    for path in _salvage_probe_paths(backend, worktree_path):
        try:
            if not _checkpoint_if_dirty(path, lane_id, checkpoints, dry_run=False):
                any_failed = True
        except RuntimeError:
            any_failed = True
    return not any_failed


def _self_verify_is_inconclusive(self_verify_result: dict[str, Any] | None) -> bool:
    """True when the gate did not meaningfully run tests (unrun), not a red suite.

    Covers pytest zero-collection (exit 4/5), off-box ``not_run`` / harness
    errors, suite-lock deferral, and shell-level syntax rejection (including a
    double-HTML-escaped ``&&``). A genuine red suite must return False so the
    red-gate salvage arm stays separate from the unrun arm.

    Corroboration rule (OFFP-A-R1): ``unrun`` is a claim that no suite ran. Red
    evidence in the tail (pytest failure count / FAILED nodeid / FAILURES
    banner) **vetoes** every unrun signal — stamped outcome, tail regex, and
    inconclusive exit codes alike. Mixed evidence degrades to red (fail-safe),
    never to unrun.
    """
    if not isinstance(self_verify_result, dict):
        return False
    if self_verify_result.get("passed") or self_verify_result.get("skipped"):
        return False
    tail = str(self_verify_result.get("output_tail") or "")
    # Mixed evidence → red. Veto before any unrun promotion path.
    if _SELF_VERIFY_RED_TAIL_RE.search(tail):
        return False
    stamped = self_verify_result.get("self_verify_outcome")
    if stamped in _SELF_VERIFY_UNRUN_OUTCOMES:
        return True
    if _SELF_VERIFY_UNRUN_TAIL_RE.search(tail):
        return True
    try:
        exit_code = int(self_verify_result.get("exit_code"))
    except (TypeError, ValueError):
        return False
    return exit_code in _SELF_VERIFY_INCONCLUSIVE_EXIT_CODES


def _self_verify_gate_status(self_verify_result: dict[str, Any] | None) -> str:
    """Typed gate status for the pass surface: ``passed`` / ``skipped`` / ``unrun`` /
    ``red`` / ``not_reached``.

    Promotes the unrun-vs-red distinction so orchestrators can branch without
    parsing ``output_tail`` prose (findings 3 and 4).

    Total by construction [PIPE-SV-01]. A pass that failed before the
    self-verify phase has no result to classify, and the previous ``None``
    answer made the stamp site skip the key entirely -- so an absent
    ``self_verify_gate`` meant "never reached", "field predates this record",
    and "read the wrong path" at once, and only inference over adjacent fields
    could tell them apart. ``not_reached`` names the state instead. [OBS-08]
    """
    if not isinstance(self_verify_result, dict):
        return "not_reached"
    if self_verify_result.get("skipped"):
        return "skipped"
    if self_verify_result.get("passed"):
        return "passed"
    if _self_verify_is_inconclusive(self_verify_result):
        return "unrun"
    return "red"


# Single source of truth for the max-turns execute contract string (E8FIX-R2-B-01 /
# REF-26). Both the early execute-stop arm and the review→execute reclassification
# arm must use this helper so the bare phrasing and the optional named-cause
# suffix cannot diverge.
_MAX_TURNS_EXECUTE_ERROR_BASE = "execute stopped: max turns reached"


def _max_turns_execute_error_reason(named_exec: str = "") -> str:
    """Contract string for a max_turns execute stop.

    ``named_exec`` is the stripped ``run_ctx.execute_error`` text when the
    adapter-exception path wrote it (worker_daemon run_lane_exec except). On the
    marker path ``execute_error`` stays unset and the bare base string is used.
    """
    named = (named_exec or "").strip()
    if named:
        return f"{_MAX_TURNS_EXECUTE_ERROR_BASE} ({named})"
    return _MAX_TURNS_EXECUTE_ERROR_BASE


def _result_handoff_was_defaulted(probe: dict[str, Any]) -> bool:
    """True when the persisted envelope recorded an omitted/blank handoff_action.

    ``from_dict`` keeps the {merge_ready, needs_guidance} vocabulary and stamps
    provenance onto the serialized result. Classify from those stamps, not from
    whether ``handoff_action`` is present — after defaulting the key is always
    ``needs_guidance``.
    """
    if probe.get("handoff_action_explicit") is False:
        return True
    if probe.get("handoff_action_source") == "defaulted":
        return True
    nested = probe.get("raw_payload")
    return isinstance(nested, dict) and nested.get("handoff_action_source") == "defaulted"


def _guidance_contract_is_invalid(probe: dict[str, Any]) -> bool:
    """Return True for guidance whose only blockers are parse-time advisories."""
    if probe.get("handoff_action") != OffloadOutcome.NEEDS_GUIDANCE.value:
        return False
    if _result_handoff_was_defaulted(probe):
        return False
    raw_payload = probe.get("raw_payload")
    if isinstance(raw_payload, dict) and raw_payload.get("admission_deferred"):
        return False
    return not _guidance_contract_has_concrete_blocker(probe)


def _guidance_contract_has_concrete_blocker(probe: dict[str, Any]) -> bool:
    """Return True when an explicit guidance result includes a worker blocker."""
    if probe.get("handoff_action") != OffloadOutcome.NEEDS_GUIDANCE.value:
        return False
    if _result_handoff_was_defaulted(probe):
        return False
    raw_payload = probe.get("raw_payload")
    if isinstance(raw_payload, dict) and raw_payload.get("admission_deferred"):
        return False
    blockers = probe.get("blockers")
    advisory_notes = _advisory_floor_notes()
    return isinstance(blockers, list) and any(
        isinstance(item, str) and item.strip() and item.strip() not in advisory_notes for item in blockers
    )


_GENUINE_ERROR_MARKERS = (
    "lane verification failed",
    "no parseable findings",
    "record_failed",
    "scope_violation",
    "claim_contradicted_by_evidence",
)
_GENERIC_BLOCKED_HANDOFF_PREFIX = "worker handed a blocked result back for guidance"
_RESULT_LOAD_ERRORS = (
    OSError,
    json.JSONDecodeError,
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
)


def _advisory_floor_notes() -> frozenset[str]:
    """Legacy floor notes that are not worker asks.

    Strip the guidance and merge-ready advisories from saved envelopes, so a
    parser note cannot be mistaken for a substantive worker ask.
    """
    # Function-local to match backend_adapter: worker_daemon also loads
    # lane_result as a top-level module.
    from workbay_orchestrator_mcp.orchestration import lane_result as _lane_result

    return frozenset(
        {
            _lane_result._ADVISORY_EMPTY_BLOCKERS,
            _lane_result._ADVISORY_EMPTY_TESTS_RUN,
        }
    )


def _load_result_blockers(wd: Any, path: Any) -> tuple[list[str], str | None, Any]:
    """Load structured blockers, provenance, and adapter markers from one result read.

    Advisory floor notes are stripped: they record a waived schema floor, not
    an orchestrator question.
    """
    if path is None:
        return [], None, None
    try:
        probe = wd._load_result(Path(path))
    except _RESULT_LOAD_ERRORS:
        return [], None, None
    if not isinstance(probe, dict):
        return [], None, None
    raw_blockers = probe.get("blockers")
    blockers: list[str] = []
    if isinstance(raw_blockers, list):
        floor_notes = _advisory_floor_notes()
        for raw in raw_blockers:
            text = str(raw).strip()
            if text and text not in floor_notes:
                blockers.append(text)
    provenance = "defaulted" if _result_handoff_was_defaulted(probe) else None
    return blockers, provenance, probe.get("raw_payload")


def _question_from(
    provenance: str | None,
    blockers: list[str],
    error_reason: str | None,
    raw_payload: Any = None,
    *,
    served: bool = False,
) -> bool:
    """Apply genuine-question precedence, first match wins.

    1. Adapter-owned admission/transport markers, exit 75, or VM defer reason
       → not a question, even for a served turn.
    1b. An unserved turn (including unknown served evidence) whose blockers
        or error_reason name ``vm_<gauge> saturation`` → not a question.
    2. Non-empty blockers → genuine question.
    3. A typed genuine error marker → genuine question.
    4. Defaulted provenance → not a question.
    5. Empty error_reason → not a question.
    6. Generic blocked-handoff template → not a question.
    7. Any more specific error_reason → genuine question.
    """
    if isinstance(raw_payload, dict) and (
        raw_payload.get("admission_deferred") is True
        or raw_payload.get("transport_failure") is True
        or raw_payload.get("outcome") in ("admission_deferred", "transport_failure")
    ):
        return False
    if isinstance(raw_payload, dict):
        if any(raw_payload.get(key) == 75 for key in ("exit_code", "agent_exit_code", "remote_exit_code")):
            return False
        defer_reason = raw_payload.get("defer_reason")
        if isinstance(defer_reason, str) and defer_reason.casefold().startswith("vm_"):
            return False
    if not served and any(
        re.search(r"\bvm_[a-z0-9_]+\s+saturation\b", text.casefold()) for text in [*blockers, error_reason or ""]
    ):
        return False
    if blockers:
        return True
    text = (error_reason or "").strip()
    lowered = text.casefold()
    if any(marker in lowered for marker in _GENUINE_ERROR_MARKERS):
        return True
    if provenance == "defaulted":
        return False
    if not text:
        return False
    if lowered.startswith(_GENERIC_BLOCKED_HANDOFF_PREFIX):
        return False
    return True


def _handoff_carries_genuine_question(
    *,
    error_reason: str | None,
    run_ctx: Any,
    wd: Any,
) -> bool:
    """Fail-closed detector: only reclassify completed_unreviewed when no real ask.

    Precedence, first match wins:

    1. Adapter-owned admission-deferred or transport-failure markers → not a question.
    1b. An unserved turn (including unknown served evidence) whose blockers
        or error_reason name ``vm_<gauge> saturation`` → not a question.
    2. Non-empty structured blockers on the result file (excluding
       per-action floor advisories) → genuine question.
    3. error_reason carrying a typed genuine marker (lane verification,
       parseable-findings, record_failed, scope_violation,
       claim_contradicted_by_evidence) → genuine question.
    4. Defaulted provenance with no structured blocker → not a question,
       whatever the error_reason text.
    5. Empty error_reason → not a question.
    6. Generic blocked-handoff template → not a question.
    7. Any more specific error_reason → genuine question.
    """
    result_path = getattr(run_ctx, "final_result_path", None)
    blockers, provenance, raw_payload = _load_result_blockers(wd, result_path)
    return _question_from(provenance, blockers, error_reason, raw_payload)


def _ceremony_status_for(run_ctx: Any) -> str:
    """Classify handoff/reporting ceremony from run_ctx (always one of CEREMONY_STATUSES).

    ``handoff_exit`` defaults to 1 (fail-closed) even when no submit was attempted,
    so ``handoff_attempted`` is the required precondition for failed/clean.
    Attempted + exit ``None`` is failed (unknown submit), not clean.
    """
    if run_ctx is None or not getattr(run_ctx, "handoff_attempted", False):
        return CEREMONY_STATUS_NOT_ATTEMPTED
    exit_code = getattr(run_ctx, "handoff_exit", None)
    if exit_code == 0:
        return CEREMONY_STATUS_CLEAN
    return CEREMONY_STATUS_FAILED


def _worker_guidance_blockers(run_ctx: Any) -> list[str]:
    """Return substantive blockers from an explicit worker guidance result.

    Parser advisory floor notes record a waived schema floor, not a worker
    ask. An empty guidance request that only carries those notes is not
    substantive and must not keep a failed handoff on the guidance path.
    """
    blockers = getattr(run_ctx, "worker_blockers", None)
    if not isinstance(blockers, list):
        return []
    floor_notes = _advisory_floor_notes()
    substantive: list[str] = []
    for item in blockers:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if text and text not in floor_notes:
            substantive.append(text)
    return substantive


def _worker_requested_guidance(run_ctx: Any) -> bool:
    """True when the worker's submitted result explicitly requested guidance."""
    return getattr(run_ctx, "worker_handoff_action", None) == "needs_guidance" and bool(
        _worker_guidance_blockers(run_ctx)
    )


# ``timeout(1)`` exits 124 when it kills the command it was supervising. 106
# passes in the ``.task-state`` corpus ended on this status and every one was
# filed under the generic handoff message, so a deadline problem was never once
# legible as a deadline problem.
HANDOFF_EXIT_TIMEOUT = 124


def _handoff_failure_reason(run_ctx: Any) -> str | None:
    """Name the handoff failure by its ``(attempted, exit)`` cell; ``None`` when clean.

    The terminal used to branch on ``handoff_exit != 0`` alone and emit one
    fixed string. That reads one dimension of a two-dimensional state, and
    ``handoff_exit`` defaults to 1 fail-closed when nothing was submitted (see
    ``_ceremony_status_for``), so "the submit failed" and "no submit ever ran"
    arrived at the operator as the same sentence. Across the 3772 recorded
    passes that sentence covers 618 of them -- 16% -- with at least three
    distinct causes underneath and 294 of them holding a landed commit.

    Every cell is enumerated here rather than left to fall through to a
    neighbour, and the residual cell (any other non-zero status) is total by
    construction: it formats the code it was given, so a status this function
    has never seen still reaches the operator intact. [GRPH-27] [OBS-08]
    """
    status = _ceremony_status_for(run_ctx)
    if status == CEREMONY_STATUS_CLEAN:
        return None
    if status == CEREMONY_STATUS_NOT_ATTEMPTED:
        # Deliberately quotes no exit code: the stored 1 is this module's
        # fail-closed default, not something the submit reported.
        return (
            "review phase ended without a handoff: submit was never attempted "
            "(recorded status is the fail-closed default, not an observed result)"
        )
    exit_code = getattr(run_ctx, "handoff_exit", None)
    if exit_code is None:
        return "review handoff submit ended with an unknown exit status"
    if exit_code == HANDOFF_EXIT_TIMEOUT:
        return (
            f"review handoff submit timed out (exit {HANDOFF_EXIT_TIMEOUT}: "
            "timeout(1) killed the submit at its deadline)"
        )
    return f"review handoff submit failed (exit {exit_code})"


# Post-review lane-check statuses that are not an explicit pass. Unknown is
# fail-closed: a verification failure must stay a verification class.
_POST_REVIEW_GATE_NON_PASS_STATUSES = frozenset({"skipped", "failed", "system_error", "timed_out", "unknown"})
_POST_REVIEW_GATE_NOT_RUN_REASONS = frozenset({"make_abort", "missing_target", "no_rail_token", "unresolvable_command"})


def _exhaustion_review_residual(review_output: Any, *, wd: Any, run_ctx: Any) -> dict[str, Any] | None:
    """Typed residual when review cycles end on recorded findings.

    HIGH skips the lane-check. MEDIUM and LOW run ``wd._verify_phase`` once.
    Severity comes from ``blocking_findings``, the same rule as the review loop.
    A lane-check exception is ``lane_check: error`` with its reason (RES-13).
    """
    if not isinstance(review_output, dict):
        return None
    raw_findings = review_output.get("findings")
    if not isinstance(raw_findings, (list, tuple)):
        return None
    typed = [item for item in raw_findings if isinstance(item, dict)]
    if not typed:
        return None
    from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
        blocking_findings,
        residual_max_severity,
    )

    recorded = review_output.get("recorded_finding_ids")
    if not isinstance(recorded, list):
        recorded = review_output.get("recorded_ids")
    if not isinstance(recorded, list):
        recorded = [item.get("finding_id") for item in typed]
    recorded_ids = [str(item) for item in recorded if isinstance(item, str) and item]
    raw_unrecorded = review_output.get("unrecorded_ids")
    unrecorded_ids = (
        [str(item) for item in raw_unrecorded if isinstance(item, str) and item]
        if isinstance(raw_unrecorded, list)
        else []
    )
    block: dict[str, Any] = {
        "max_severity": residual_max_severity(typed),
        "recorded_ids": recorded_ids,
        "unrecorded_ids": unrecorded_ids,
    }
    result_parse = review_output.get("result_parse")
    if isinstance(result_parse, str) and result_parse:
        block["result_parse"] = result_parse
    high_residual = any(str(item.get("severity") or "").strip().upper() == "HIGH" for item in blocking_findings(typed))
    if high_residual:
        block["lane_check"] = "not_run_high_residual"
        return block
    try:
        gate = wd._verify_phase(run_ctx, review_output)
    except Exception as exc:  # noqa: BLE001 — typed residual, never a silent pass
        block["lane_check"] = "error"
        block["reason"] = str(exc)
        return block
    block["lane_check"] = "passed" if _post_review_gate_passed(gate) else "failed"
    return block


def _post_review_gate_passed(gate_result: Any) -> bool:
    """True only when the post-review lane-check explicitly passed.

    Fail-closed: skip / system-error / timeout / unknown / missing are not a pass.
    A boolean True is accepted for tests and historical bool-only callers.
    """
    if gate_result is True:
        return True
    if isinstance(gate_result, dict):
        status = str(gate_result.get("status") or "").strip().casefold()
        if gate_result.get("skipped") or status in _POST_REVIEW_GATE_NON_PASS_STATUSES:
            return False
        return gate_result.get("passed") is True and status in {"", "passed"}
    return False


def _post_review_gate_outcome(gate_result: Any, *, pass_process_ran: bool) -> tuple[str, str]:
    """Classify a non-passing post-review gate without losing its reason."""
    reason = ""
    status = ""
    if isinstance(gate_result, dict):
        status = str(gate_result.get("status") or "").strip().casefold()
        reason = str(gate_result.get("reason") or "").strip()
    if status == "system_error" and reason in _POST_REVIEW_GATE_NOT_RUN_REASONS:
        return OffloadOutcome.VERIFICATION_SKIPPED.value, f"verification_skipped:{reason}"
    if status == "system_error":
        return (
            OffloadOutcome.VERIFICATION_HARNESS_ERROR.value,
            f"verification_harness_error:{reason or 'unknown'}",
        )
    if not pass_process_ran:
        suffix = f": {reason}" if status == "system_error" and reason else ""
        return (
            OffloadOutcome.NEEDS_GUIDANCE.value,
            f"lane verification failed after review convergence{suffix}",
        )
    if status == "system_error" and reason:
        return (
            OffloadOutcome.VERIFICATION_FAILED.value,
            f"lane verification failed after review convergence: {reason}",
        )
    return OffloadOutcome.VERIFICATION_FAILED.value, "lane verification failed after review convergence"


def _post_review_gate_blocks_ceremony_reclass(gate_result: Any) -> bool:
    """True when a post-review gate ran and did not pass.

    ``None`` means the gate never ran (review did not converge); that is not a
    verification failure to preserve, so it does not block ceremony reclass.
    An explicit non-pass — False, skip, system-error, timeout, unknown — does.
    """
    if gate_result is None:
        return False
    return not _post_review_gate_passed(gate_result)


def _work_status_for(commit_landed: bool) -> str:
    return WORK_STATUS_LANDED if commit_landed else WORK_STATUS_NOT_LANDED


def _self_verify_not_failed(self_verify_result: dict[str, Any] | None) -> bool:
    """True when self_verify is absent, skipped, or passed (not a red suite)."""
    if not isinstance(self_verify_result, dict):
        return True
    if self_verify_result.get("skipped"):
        return True
    return bool(self_verify_result.get("passed"))


def _budget_trip_work_landed(
    *,
    failed_stage: str | None,
    commit_landed: bool,
    self_verify_result: dict[str, Any] | None,
) -> bool:
    """True when a token-budget trip stopped a lane whose work already landed green.

    The budget governor is an admission bound on further agent turns, not a
    verdict on the work: the post-commit trip sits between the commit gate and
    review, so the product is already committed and self-verified. Typing that
    as token_budget_exceeded (plus a task-wide blocker) reads as a failure and
    misleads orchestrators into treating delivered work as lost.
    """
    if failed_stage in ("execute", "self_verify"):
        return False
    if not commit_landed:
        return False
    return _self_verify_not_failed(self_verify_result)


def _final_result_merge_ready_clean(run_ctx: Any, wd: Any) -> bool:
    """True when the lane's final result is merge_ready with no structured blockers.

    Used to stop a green, merge-ready worker reply from being stuck as
    needs_guidance solely because a findings block was absent or the generic
    blocked-handoff template was applied (ENGINE-NEEDS-GUIDANCE-MISREPORTS…).
    """
    try:
        result_path = getattr(run_ctx, "final_result_path", None)
        if result_path is None:
            return False
        probe = wd._load_result(Path(result_path))
        if not isinstance(probe, dict):
            return False
        if str(probe.get("handoff_action") or "").strip() != "merge_ready":
            return False
        raw_blockers = probe.get("blockers")
        if isinstance(raw_blockers, list) and any(str(b).strip() for b in raw_blockers):
            return False
        return True
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError, AttributeError):
        return False


def _maybe_reclassify_completed_unreviewed(
    *,
    outcome: str,
    failed_stage: str | None,
    commit_landed: bool,
    self_verify_result: dict[str, Any] | None,
    error_reason: str | None,
    run_ctx: Any,
    wd: Any,
    task_ref: str | None = None,
    lane_id: str | None = None,
    baseline_report_id: int = 0,
) -> tuple[str, str | None, str | None]:
    """OL-PF2 fail-closed guard for ``completed_unreviewed``.

    May reclassify only when:
    - outcome is needs_guidance
    - self_verify passed (or was a no-op skip / absent)
    - commit_landed is true
    - failed_stage is NOT execute or self_verify
    - the handoff ceremony did not fail (attempted + exit != 0 stays needs_guidance)
    - handoff carries no genuine question
      OR the final result is merge_ready with empty blockers (engoutcome finding 2)
    - latest worker report is not a composer-violation handoff

    Returns (outcome, failed_stage, error_reason).
    """
    if outcome != "needs_guidance":
        return outcome, failed_stage, error_reason
    if failed_stage in ("execute", "self_verify"):
        return outcome, failed_stage, error_reason
    if not commit_landed:
        return outcome, failed_stage, error_reason
    if not _self_verify_not_failed(self_verify_result):
        return outcome, failed_stage, error_reason
    # A failed submit is not a no-question green lane. The ceremony helper
    # rewrites needs_guidance when the gate passed; when it did not, this
    # fallback must still refuse so ceremony_failed never becomes
    # completed_unreviewed. Consult a real bool only — mock run_ctx objects
    # in older pins leave MagicMock attributes here.
    if getattr(run_ctx, "handoff_attempted", False) is True and getattr(run_ctx, "handoff_exit", None) != 0:
        return outcome, failed_stage, error_reason
    # Fail closed without identity: report-backed guards cannot run safely
    # (SECD-05). Skipping them would reclassify without consulting durable evidence.
    if not task_ref or not lane_id:
        return outcome, failed_stage, error_reason
    # Load the fresh report once for composer + durable-blocker checks. Do not
    # depend on run_ctx.final_result_path here: a successful blocked handoff
    # deletes that file after persisting blockers into worker_reports (OBS-08).
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    # Composer violation must never look like green completed_unreviewed (and
    # must stay needs_guidance so salvage stays gated off that report shape).
    if report is not None and _is_composer_violation_handoff_report(report):
        return outcome, failed_stage, error_reason
    # Real structured blockers survive only on the durable report after handoff
    # success cleans the result file — refuse reclass when any are present (AGT-10).
    if report is not None and _report_carries_blockers(report):
        return outcome, failed_stage, error_reason
    # Green merge_ready with empty blockers is never a genuine "blocked for
    # guidance" result — even when the engine error string mentions a missing
    # findings block or the generic blocked template (engoutcome finding 2).
    if _final_result_merge_ready_clean(run_ctx, wd):
        return "completed_unreviewed", None, None
    if _handoff_carries_genuine_question(error_reason=error_reason, run_ctx=run_ctx, wd=wd):
        return outcome, failed_stage, error_reason
    return "completed_unreviewed", None, None


def _maybe_reclassify_ceremony_failed(
    *,
    outcome: str,
    failed_stage: str | None,
    commit_landed: bool,
    self_verify_result: dict[str, Any] | None,
    run_ctx: Any,
    post_review_gate: Any = None,
) -> str:
    """Reclassify a failed outcome when work landed but its ceremony did not.

    Fail-closed: never widens success enums or failed execute/self-verify work.
    Rewrites ``error`` / engine-generated ``needs_guidance`` when commit_landed
    is true and the only terminal failure is a failed handoff attempt downstream
    of product execution and verification. An explicit worker guidance result
    with structured blockers retains ``needs_guidance``
    (ENGINE-HANDOFF-SUBMIT-FAILURE-REPORTS-ERROR-FOR-COMPLETED-LANE).
    A post-review gate that did not pass (including skip / system-error /
    timeout / unknown) is a verification failure and is never rewritten to
    ceremony_failed, even when submit also failed. Rewriting ``needs_guidance``
    here is what keeps a failed ceremony from falling through to
    ``completed_unreviewed``.
    """
    if outcome not in ("error", "needs_guidance"):
        return outcome
    # An explicit worker question remains a guidance outcome even when its
    # submit could not reach the receiver. The failed submit is surfaced on the
    # independent ceremony axis; it does not erase the worker's product intent.
    if outcome == "needs_guidance" and _worker_requested_guidance(run_ctx):
        return outcome
    if not commit_landed:
        return outcome
    # A landed commit is durable product evidence even when review fails before
    # handoff submit; keep execute/self-verify failures fail-closed [OBS-08][OBS-04].
    if failed_stage in ("execute", "self_verify"):
        return outcome
    if not _self_verify_not_failed(self_verify_result):
        return outcome
    # Post-review lane-check is product verification. Refuse the rewrite when
    # that gate did not pass so a crossed submit failure cannot erase it.
    if _post_review_gate_blocks_ceremony_reclass(post_review_gate):
        return outcome
    # Keep this classifier aligned with the status exposed to callers: ceremony
    # that never ran is not_attempted, and an exit-0 blocked handoff is clean.
    if _ceremony_status_for(run_ctx) != CEREMONY_STATUS_FAILED:
        return outcome
    return "ceremony_failed"


# ---------------------------------------------------------------------------
# Worker end-state contract (PR-09/PR-10): evidence + engine-recorded closure
# ---------------------------------------------------------------------------


_GIT_EVIDENCE_PROBE_MAX_SECONDS = 20.0


class _GitEvidenceProbeTimeout(TimeoutError):
    """A product-identity or lineage Git probe exceeded its deadline."""


def _bounded_git_probe_deadline(deadline: float | None) -> float:
    bounded = time.monotonic() + _GIT_EVIDENCE_PROBE_MAX_SECONDS
    if deadline is not None:
        bounded = min(bounded, deadline)
    return bounded


def _terminate_git_probe_group(proc: subprocess.Popen[str]) -> None:
    """Terminate Git and any filter/helper children started by its probe."""
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.communicate(timeout=1)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except OSError:
            pass
        try:
            proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass


def _run_git_evidence_probe(
    worktree_path: Path,
    *args: str,
    deadline: float | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str] | None:
    """Run an evidence Git probe with a process-group timeout and cleanup."""
    bounded_deadline = _bounded_git_probe_deadline(deadline)
    timeout = bounded_deadline - time.monotonic()
    if timeout <= 0:
        raise _GitEvidenceProbeTimeout("Git evidence probe deadline elapsed")
    command = ["git", "-C", str(worktree_path), *args]
    try:
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=(os.name == "posix"),
        )
    except OSError:
        return None
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_git_probe_group(proc)
        raise _GitEvidenceProbeTimeout("Git evidence probe timed out") from exc
    return subprocess.CompletedProcess(command, proc.returncode, stdout, stderr)


def _git_stdout(worktree_path: Path, *args: str, deadline: float | None = None) -> str:
    """Return a Git read's stripped stdout, or "" when Git reports failure.

    RES-13: inside a completion the read gets only the remaining lifecycle
    deadline, never a fresh budget of its own. Overrunning it kills the probe's
    process group and raises the typed, retryable CompletionDeadlineExceeded; a
    timeout is never an empty answer. An explicit pass deadline also bounds
    source capture before completion; other reads retain their existing behavior.
    """
    command = ["git", "-C", str(worktree_path), *args]
    if deadline is not None:
        result = _run_git_evidence_probe(worktree_path, *args, deadline=deadline)
        return result.stdout.strip() if result is not None and result.returncode == 0 else ""
    if _ACTIVE_COMPLETION_DEADLINE.get() is None:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else ""
    timeout = _completion_remaining()
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=(os.name == "posix"),
    )
    try:
        stdout, _stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_git_probe_group(proc)
        raise CompletionDeadlineExceeded() from exc
    return stdout.strip() if proc.returncode == 0 else ""


_OFFLOAD_TEST_TREE_PREFIX = "offload-test-tree-"
# Bound leftover removal so one probe cannot scan-delete without a limit (RES-07).
_OFFLOAD_TEST_TREE_RECLAIM_LIMIT = 8
_OFFLOAD_TEST_TREE_LEASE = "owner.lease"
_OFFLOAD_TEST_TREE_CUSTODY = ".custody-"


def _offload_test_tree_custody(path: Path, directory: os.stat_result) -> Path:
    # Bind the alias to the directory inode, so pathname reuse cannot confer custody.
    return path.with_name(f"{path.name}{_OFFLOAD_TEST_TREE_CUSTODY}{directory.st_dev}-{directory.st_ino}")


def _reclaim_offload_test_tree_custody(path: Path) -> bool:
    """Reap an alias left by interruption after the directory was removed."""
    name, separator, identity = path.name.rpartition(_OFFLOAD_TEST_TREE_CUSTODY)
    if not separator or re.fullmatch(r"[0-9]+-[0-9]+", identity) is None:
        return False
    lease = None
    try:
        lease = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(lease).st_mode):
            return False
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if path.with_name(name).exists():
            return False
        current, opened = path.stat(follow_symlinks=False), os.fstat(lease)
        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            return False
        path.unlink()
        return True
    except OSError:
        return False
    finally:
        if lease is not None:
            os.close(lease)


def _allocate_offload_test_tree(temp_root: Path) -> tuple[Path, int]:
    """Lock before publication: expose the marker only after locking its inode,
    then publish the directory. Each open file description has its own flock, including
    same-process threads. Kernel close on process death releases custody.
    """
    staging = Path(tempfile.mkdtemp(prefix="." + _OFFLOAD_TEST_TREE_PREFIX, dir=temp_root))
    lease = None
    try:
        pending_marker = staging / ("." + _OFFLOAD_TEST_TREE_LEASE)
        lease = os.open(pending_marker, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.rename(pending_marker, staging / _OFFLOAD_TEST_TREE_LEASE)
        published = staging.with_name(staging.name[1:])
        staging.rename(published)
        return published, lease
    except BaseException:
        try:
            _release_offload_test_tree(staging, event="offload_test_tree_cleanup_failed")
        finally:
            if lease is not None:
                os.close(lease)
        raise


def _reclaim_offload_test_tree(path: Path) -> bool:
    """Delete only after nonblocking proof of abandonment; unknown legacy
    directories and symlink markers do not supply that proof. Never create a
    replacement marker: all owners/reclaimers must lock the same inode.
    """
    directory = lease = None
    try:
        directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            lease = os.open(_OFFLOAD_TEST_TREE_LEASE, os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory)
        except FileNotFoundError:
            lease = os.open(_offload_test_tree_custody(path, os.fstat(directory)), os.O_RDWR | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(lease).st_mode):
            return False
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A competing reclaimer may already have removed this directory. Keep
        # the lock through removal and check the pathname still names our inode.
        current = path.stat(follow_symlinks=False)
        opened = os.fstat(directory)
        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            return False
        _release_offload_test_tree(path, event="offload_test_tree_leftover_remains")
        return True
    except OSError:
        return False
    finally:
        if lease is not None:
            os.close(lease)
        if directory is not None:
            os.close(directory)


def _release_offload_test_tree(path: Path, *, event: str) -> None:
    """Remove one probe temp directory. A removal OSError is logged, not raised.

    The caller both allocates and releases the directory (RES-20). Contain the
    removal failure here (RES-13) and emit one WARNING with errno, path, and why
    the directory remains (OBS-08). Equivalent to TemporaryDirectory
    ignore_cleanup_errors, plus the warning that flag would swallow.
    """
    try:
        if path.is_symlink():
            logger.warning("%s errno=%s path=%s reason=%s", event, None, path, "refusing to remove a symlink")
            return
        custody = _offload_test_tree_custody(path, path.stat(follow_symlinks=False))
        marker = path / _OFFLOAD_TEST_TREE_LEASE
        if marker.exists():
            # Hard-link before destructive progress, retaining the locked inode
            # outside rmtree's reach even on BaseException or process death.
            try:
                os.link(marker, custody, follow_symlinks=False)
            except FileExistsError:
                original = marker.stat(follow_symlinks=False)
                retained = custody.stat(follow_symlinks=False)
                if (original.st_dev, original.st_ino) != (retained.st_dev, retained.st_ino):
                    raise FileExistsError("custody inode mismatch")
        shutil.rmtree(path)
        custody.unlink(missing_ok=True)
    except FileNotFoundError:
        return
    except OSError as exc:
        logger.warning(
            "%s errno=%s path=%s reason=%s",
            event,
            exc.errno,
            path,
            exc.strerror or type(exc).__name__,
        )


def _product_tree_seed_is_file(path: Path) -> bool | None:
    """Unknown seed inputs require read-tree, rather than failing the probe."""
    try:
        return path.is_file()
    except OSError:
        return None


def _product_tree_seed_external_attributes_reason(
    worktree_path: Path, *, deadline: float, env: dict[str, str]
) -> str | None:
    """Return a fallback reason when external attributes make cached blobs unsafe."""
    try:
        info_attributes = _run_git_evidence_probe(
            worktree_path,
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "info/attributes",
            deadline=deadline,
            env=env,
        )
        if info_attributes is None or info_attributes.returncode != 0:
            return "repository attributes could not be checked"
        info_attributes_present = _product_tree_seed_is_file(Path((info_attributes.stdout or "").strip()))
        if info_attributes_present is None:
            return "repository attributes could not be checked"
        if info_attributes_present:
            return "repository info attributes are present"

        xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
        try:
            default_attributes_root = Path(xdg_config_home) if xdg_config_home else Path.home() / ".config"
        except (OSError, RuntimeError):
            return "default global attributes path could not be resolved"
        default_attributes_present = _product_tree_seed_is_file(default_attributes_root / "git" / "attributes")
        if default_attributes_present is None:
            return "default global attributes could not be checked"
        if default_attributes_present:
            return "default global attributes are present"

        configured_attributes = _run_git_evidence_probe(
            worktree_path,
            "config",
            "--path",
            "--get",
            "core.attributesFile",
            deadline=deadline,
            env=env,
        )
        if configured_attributes is None or configured_attributes.returncode not in {0, 1}:
            return "configured attributes could not be checked"
        if configured_attributes.returncode == 0:
            return "a core attributes file is configured"
        return None
    except UnicodeError:
        # Attribute paths are printed by Git and may be raw bytes. Discard the seed.
        return "external attributes metadata could not be decoded"


def _product_tree_seed_conversion_reason(worktree_path: Path, *, deadline: float, env: dict[str, str]) -> str | None:
    """Cached blobs equal a full rehash only when no conversion can apply.

    Attribute contents cannot prove safety: a selected filter's configuration
    may have changed since staging. Unselected filter configuration is harmless.
    """
    try:
        tracked_attributes = _run_git_evidence_probe(
            worktree_path,
            "ls-files",
            "-z",
            "--",
            ":(glob)**/.gitattributes",
            deadline=deadline,
            env=env,
        )
        if tracked_attributes is None or tracked_attributes.returncode != 0:
            return "tracked attributes could not be checked"
        if tracked_attributes.stdout:
            return "tracked attributes may select a conversion"

        autocrlf = _run_git_evidence_probe(
            worktree_path,
            "config",
            "--type=bool-or-str",
            "--get",
            "core.autocrlf",
            deadline=deadline,
            env=env,
        )
        if autocrlf is None or autocrlf.returncode not in {0, 1}:
            return "core.autocrlf could not be checked"
        if autocrlf.returncode == 0:
            value = (autocrlf.stdout or "").strip().lower()
            if value in {"true", "input"}:
                return "core.autocrlf converts line endings"
            if value != "false":
                return "core.autocrlf could not be checked"
    except _GitEvidenceProbeTimeout:
        # Never seed after an inconclusive check. The fallback still uses the
        # same deadline, so an exhausted budget retains the existing timeout.
        return "conversion safety probe timed out"
    except UnicodeError:
        # Tracked attributes paths are raw bytes under -z, which the text-mode
        # runner cannot always decode. Discard the seed; the read-tree path runs.
        return "conversion safety metadata could not be decoded"
    return None


def _product_tree_seed_cached_size_reason(worktree_path: Path, *, deadline: float, env: dict[str, str]) -> str | None:
    """Reject cached blobs whose sizes show a retired conversion.

    The caller has already proved that the private index equals HEAD, so one
    ls-tree --long supplies object-database sizes for exactly those entries.
    ls-files --debug supplies cached worktree sizes without opening the files;
    unavailable fields on older Git versions require the read-tree fallback.
    Checking all entries conservatively covers every stat-clean entry.

    Known Round 3 limit: a retired clean filter whose output has exactly the
    input's size is undetectable by this metadata-only proof. Closing that gap
    would require rehashing the worktree, defeating this seed optimization.
    """
    try:
        cached = _run_git_evidence_probe(
            worktree_path, "ls-files", "--stage", "--debug", "-z", deadline=deadline, env=env
        )
        if cached is None or cached.returncode != 0:
            return "cached worktree sizes could not be checked"
        objects = _run_git_evidence_probe(
            worktree_path, "ls-tree", "--long", "-r", "-z", "HEAD", deadline=deadline, env=env
        )
        if objects is None or objects.returncode != 0:
            return "cached blob sizes could not be checked"
    except _GitEvidenceProbeTimeout:
        return "cached size safety probe timed out"
    except UnicodeError:
        # The shared text-mode runner cannot decode every valid POSIX path.
        # Its process has exited; discard the seed and use the existing scan.
        return "cached size metadata could not be decoded"

    object_entries: dict[str, tuple[str, str, int | None]] = {}
    object_output = objects.stdout or ""
    if object_output and not object_output.endswith("\0"):
        return "cached blob size metadata could not be parsed"
    for entry in object_output.split("\0"):
        if not entry:
            continue
        header, separator, path = entry.partition("\t")
        fields = header.split()
        if not separator or len(fields) != 4 or path in object_entries:
            return "cached blob size metadata could not be parsed"
        mode, kind, oid, size = fields
        if mode == "160000" and kind == "commit" and size == "-":
            # Gitlinks are commit references, not cached worktree file blobs.
            blob_size = None
        elif kind == "blob" and size.isascii() and size.isdigit():
            blob_size = int(size)
        else:
            return "cached blob size differs from worktree size"
        object_entries[path] = (mode, oid, blob_size)

    cached_output = cached.stdout or ""
    offset = 0
    while offset < len(cached_output):
        entry_end = cached_output.find("\0", offset)
        if entry_end < 0:
            return "cached worktree size metadata could not be parsed"
        entry = cached_output[offset:entry_end]
        header, path_separator, path = entry.partition("\t")
        fields = header.split()
        stat_start = entry_end + 1
        stat_end = stat_start
        for _ in range(5):
            newline = cached_output.find("\n", stat_end)
            if newline < 0:
                return "cached worktree size metadata could not be parsed"
            stat_end = newline + 1
        stat_lines = cached_output[stat_start : stat_end - 1].split("\n")
        if not path_separator or len(fields) != 3:
            return "cached worktree size metadata could not be parsed"
        mode, oid, stage = fields
        if stage != "0" or not all(
            line.startswith(prefix)
            for line, prefix in zip(stat_lines[:4], ("  ctime:", "  mtime:", "  dev:", "  uid:"), strict=True)
        ):
            return "cached worktree size metadata could not be parsed"
        size_field = re.fullmatch(r"  size: ([0-9]+)\tflags: [0-9a-fA-F]+", stat_lines[4])
        if size_field is None:
            return "cached worktree size metadata could not be parsed"
        object_entry = object_entries.pop(path, None)
        if object_entry is None or object_entry[:2] != (mode, oid):
            return "cached blob size metadata differs from private index"
        blob_size = object_entry[2]
        if blob_size is not None and blob_size != int(size_field.group(1)):
            return "cached blob size differs from worktree size"
        offset = stat_end
    if object_entries:
        return "cached blob size metadata differs from private index"
    return None


def _worktree_tree_sha(worktree_path: Path, *, deadline: float | None = None) -> str | None:
    """Return the Git tree for tracked and non-ignored untracked worktree files.

    Use a private temporary index so capturing the product does not stage files
    in the worker's real index before self-verify. The complete scan shares one
    bounded deadline and kills Git's whole process group if a clean filter stalls.
    Cleanup of that private directory cannot change the hash this function
    already computed. A later call reclaims leftover offload-test-tree
    directories from a previous failed cleanup, up to a fixed bound. Only an
    unlocked owner lease proves abandonment; legacy directories are unknown.
    """
    probe_deadline = _bounded_git_probe_deadline(deadline)
    temp_root = Path(tempfile.gettempdir())
    temp_dir, owner_lease = _allocate_offload_test_tree(temp_root)
    try:
        reclaimed = 0
        try:
            candidates = sorted(temp_root.iterdir(), key=lambda entry: entry.name)
        except OSError as exc:
            logger.warning(
                "offload_test_tree_leftover_remains errno=%s path=%s reason=%s",
                exc.errno,
                temp_root,
                exc.strerror or "temp root unreadable",
            )
            candidates = []
        root_resolved = temp_root.resolve()
        excluded = temp_dir.resolve()
        for entry in candidates:
            if reclaimed >= _OFFLOAD_TEST_TREE_RECLAIM_LIMIT:
                break
            if not entry.name.startswith((_OFFLOAD_TEST_TREE_PREFIX, "." + _OFFLOAD_TEST_TREE_PREFIX)):
                continue
            try:
                if entry.is_symlink():
                    continue
                if not entry.is_dir():
                    if _reclaim_offload_test_tree_custody(entry):
                        reclaimed += 1
                    continue
                resolved = entry.resolve()
            except OSError as exc:
                logger.warning(
                    "offload_test_tree_leftover_remains errno=%s path=%s reason=%s",
                    exc.errno,
                    entry,
                    exc.strerror or type(exc).__name__,
                )
                reclaimed += 1
                continue
            if resolved == excluded or (resolved != root_resolved and root_resolved not in resolved.parents):
                continue
            if _reclaim_offload_test_tree(entry):
                reclaimed += 1
        env = dict(os.environ)
        private_index = temp_dir / "index"
        env["GIT_INDEX_FILE"] = str(private_index)
        tree_sha = ""
        try:
            # Resolve the worktree's index without inheriting a caller's private
            # index override, then preserve its stat cache and racy-git timestamp.
            source_index_env = dict(os.environ)
            source_index_env.pop("GIT_INDEX_FILE", None)
            seeded_private_index = False
            fallback_reason = "worktree index path lookup failed"
            try:
                source_index = _run_git_evidence_probe(
                    worktree_path,
                    "rev-parse",
                    "--path-format=absolute",
                    "--git-path",
                    "index",
                    deadline=probe_deadline,
                    env=source_index_env,
                )
                if source_index is not None and source_index.returncode == 0:
                    source_index_path = Path((source_index.stdout or "").strip())
                    source_index_present = _product_tree_seed_is_file(source_index_path)
                    if source_index_present is None:
                        fallback_reason = "worktree index file could not be checked"
                    elif not source_index_present:
                        fallback_reason = "worktree index file is missing"
                    else:
                        try:
                            shutil.copy2(source_index_path, private_index)
                        except OSError as exc:
                            fallback_reason = (
                                f"worktree index copy failed (errno={exc.errno}, "
                                f"reason={exc.strerror or type(exc).__name__})"
                            )
                        else:
                            listing = _run_git_evidence_probe(
                                worktree_path,
                                "ls-files",
                                "-v",
                                deadline=probe_deadline,
                                env=env,
                            )
                            if listing is None or listing.returncode != 0:
                                fallback_reason = "private index flags could not be checked"
                            else:
                                unrefreshable_tags = {
                                    line[:1]
                                    for line in (listing.stdout or "").splitlines()
                                    if line and (line[:1].islower() or line[:1] in {"S", "M"})
                                }
                                if unrefreshable_tags:
                                    fallback_reason = "private index contains entries add --all may not refresh"
                                else:
                                    staged_changes = _run_git_evidence_probe(
                                        worktree_path,
                                        "diff-index",
                                        "--cached",
                                        "--quiet",
                                        "HEAD",
                                        "--",
                                        deadline=probe_deadline,
                                        env=env,
                                    )
                                    if staged_changes is None or staged_changes.returncode != 0:
                                        fallback_reason = "private index differs from HEAD"
                                    else:
                                        changed_attributes = _run_git_evidence_probe(
                                            worktree_path,
                                            "status",
                                            "--porcelain",
                                            "--untracked-files=all",
                                            "--ignored=traditional",
                                            "--",
                                            ":(glob)**/.gitattributes",
                                            deadline=probe_deadline,
                                            env=env,
                                        )
                                        if changed_attributes is None or changed_attributes.returncode != 0:
                                            fallback_reason = "worktree attributes could not be checked"
                                        elif (changed_attributes.stdout or "").strip():
                                            fallback_reason = "worktree attributes differ from the index"
                                        else:
                                            external_attributes_reason = _product_tree_seed_external_attributes_reason(
                                                worktree_path, deadline=probe_deadline, env=env
                                            )
                                            if external_attributes_reason is not None:
                                                fallback_reason = external_attributes_reason
                                            else:
                                                conversion_reason = _product_tree_seed_conversion_reason(
                                                    worktree_path, deadline=probe_deadline, env=env
                                                )
                                                if conversion_reason is not None:
                                                    fallback_reason = conversion_reason
                                                else:
                                                    size_reason = _product_tree_seed_cached_size_reason(
                                                        worktree_path, deadline=probe_deadline, env=env
                                                    )
                                                    if size_reason is not None:
                                                        fallback_reason = size_reason
                                                    else:
                                                        seeded_private_index = True
            except UnicodeError:
                # The shared text-mode runner cannot decode every valid POSIX path.
                # Discard any partial seed; the read-tree sequence below then runs.
                fallback_reason = "worktree index metadata could not be decoded"

            if not seeded_private_index:
                logger.warning("Using read-tree for product-tree probe: %s", fallback_reason)
                try:
                    private_index.unlink()
                except FileNotFoundError:
                    pass
                commands = (("read-tree", "HEAD"), ("add", "--all"), ("write-tree",))
            else:
                commands = (("add", "--all"), ("write-tree",))

            for args in commands:
                result = _run_git_evidence_probe(
                    worktree_path,
                    *args,
                    deadline=probe_deadline,
                    env=env,
                )
                if result is None or result.returncode != 0:
                    return None
                tree_sha = (result.stdout or "").strip()
        except _GitEvidenceProbeTimeout:
            raise
        except OSError:
            # Probe I/O failure is a missing tree (typed by the caller), not a pass crash.
            return None
        return tree_sha or None
    finally:
        try:
            _release_offload_test_tree(temp_dir, event="offload_test_tree_cleanup_failed")
        finally:
            os.close(owner_lease)


_PRODUCT_TREE_PROBE_RETRY_MIN_REMAINING_SECONDS = 0.25


def _worktree_tree_sha_with_retry(worktree_path: Path, *, deadline: float) -> str | None:
    """Retry a timed-out product-tree probe once, within the pass deadline."""
    try:
        return _worktree_tree_sha(worktree_path, deadline=deadline)
    except _GitEvidenceProbeTimeout:
        remaining = deadline - time.monotonic()
        if remaining <= _PRODUCT_TREE_PROBE_RETRY_MIN_REMAINING_SECONDS:
            raise
        backoff = random.uniform(0.02, 0.10)
        if backoff >= remaining:
            raise
        time.sleep(backoff)
        if time.monotonic() >= deadline:
            raise _GitEvidenceProbeTimeout("Git evidence probe deadline elapsed before retry")
        return _worktree_tree_sha(worktree_path, deadline=deadline)
    except OSError:
        return None


def _worktree_state_after_probe_timeout(worktree_path: Path, *, deadline: float) -> str:
    """Return a bounded porcelain status classification after a tree timeout."""
    try:
        result = _run_git_evidence_probe(worktree_path, "status", "--porcelain", deadline=deadline)
    except _GitEvidenceProbeTimeout:
        return "unknown"
    if result is None or result.returncode != 0:
        return "unknown"
    return "dirty" if (result.stdout or "").strip() else "clean"


def _handoff_db_path() -> Path:
    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

    return Path(get_runtime_config().db_path)


def _open_dispatch_id(task_ref: str, lane_id: str) -> str | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            """
            SELECT dispatch_id FROM lane_messages
            WHERE task_ref = ? AND lane_id = ? AND direction = 'orchestrator_to_worker'
              AND status = 'open' AND dispatch_id IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id),
        ).fetchone()
    if row is None:
        return None
    dispatch_id = str(row[0] or "").strip()
    return dispatch_id or None


def _max_worker_report_id(task_ref: str, lane_id: str) -> int:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM worker_reports WHERE task_ref = ? AND lane_id = ?",
            (task_ref, lane_id),
        ).fetchone()
    return int(row[0])


def _prior_producer_question_count(task_ref: str, lane_id: str) -> int:
    """Count durable earlier producer handoffs carrying structured questions."""
    with contextlib.closing(connect_handoff_db(_handoff_db_path(), read_only=True)) as conn:
        rows = conn.execute(
            """
            SELECT blockers_json FROM worker_reports
            WHERE task_ref = ? AND lane_id = ? AND outcome = 'stopped'
            """,
            (task_ref, lane_id),
        ).fetchall()
    count = 0
    for (raw_blockers,) in rows:
        try:
            blockers = json.loads(raw_blockers or "[]")
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(blockers, list) and any(str(item).strip() for item in blockers):
            count += 1
    return count


def _fresh_worker_report(task_ref: str, lane_id: str, baseline_report_id: int) -> dict[str, Any] | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM worker_reports
            WHERE task_ref = ? AND lane_id = ? AND id > ?
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id, baseline_report_id),
        ).fetchone()
    return dict(row) if row is not None else None


_UNPARSEABLE_SUMMARY = "grok produced no parseable JSON result"
_UNPARSEABLE_BLOCKER_PREFIX = "grok output unparseable"
_COMPOSER_VIOLATION_SUMMARY = "grok Composer-only guarantee not confirmed"
_COMPOSER_VIOLATION_BLOCKER_MARKERS = (
    "grok-build authored",
    "Composer-only guarantee violated",
    "Composer-only guarantee not confirmed",
)


def _max_verified_test_id(task_ref: str) -> int:
    with sqlite3.connect(_handoff_db_path()) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM verified_tests WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    return int(row[0])


class _BlockerEvidence(NamedTuple):
    """Decoded ``blockers_json`` with unreadable as a first-class state.

    ``blockers`` is empty when the payload is unreadable or genuinely empty —
    callers that need "do blockers exist?" must consult ``readable`` (and
    ``present``) rather than treating empty as absence of evidence (OBS-08).

    ``blockers`` keeps only strings for marker scanning (composer / unparseable
    prefixes). ``any_non_empty_item`` is the fail-closed safety answer over
    *all* decoded list items with ``any(str(i).strip() for i in items)`` so a
    structured blocker such as ``[{"msg": "disk full"}]`` still carries.
    """

    blockers: tuple[str, ...]  # string blockers only — marker scanning
    readable: bool
    present: bool
    any_non_empty_item: bool  # any(str(i).strip() for i in <all items>)


def _decode_blockers_json(report: dict[str, Any]) -> _BlockerEvidence:
    """Total decode of durable ``blockers_json``; never raises.

    Handles null / 42 / true / corrupt text / missing key / pre-decoded lists.
    ``readable=False`` means the payload did not decode to a JSON list — that
    is distinct from an empty list of blockers.
    """
    if "blockers_json" not in report:
        # Key absent: not present. Readable empty so the missing-key fail-open
        # in _report_carries_blockers stays intentional (see that helper).
        return _BlockerEvidence(blockers=(), readable=True, present=False, any_non_empty_item=False)

    raw = report.get("blockers_json")
    if raw is None:
        # SQL NULL / Python None stored in the column: present, empty, readable.
        return _BlockerEvidence(blockers=(), readable=True, present=True, any_non_empty_item=False)

    if isinstance(raw, list):
        # String filter is correct for marker scanning; safety uses all items.
        blockers = tuple(str(item) for item in raw if isinstance(item, str))
        any_item = any(str(item).strip() for item in raw)
        return _BlockerEvidence(
            blockers=blockers,
            readable=True,
            present=True,
            any_non_empty_item=any_item,
        )

    if not isinstance(raw, str):
        # Non-string durable cell (int/bool/dict): unreadable, not a list.
        return _BlockerEvidence(blockers=(), readable=False, present=True, any_non_empty_item=False)

    text = raw.strip()
    if not text or text == "null":
        return _BlockerEvidence(blockers=(), readable=True, present=True, any_non_empty_item=False)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return _BlockerEvidence(blockers=(), readable=False, present=True, any_non_empty_item=False)

    if parsed is None:
        return _BlockerEvidence(blockers=(), readable=True, present=True, any_non_empty_item=False)

    if not isinstance(parsed, list):
        # Valid JSON that is not a list (42, true, {}, ...): unreadable as blockers.
        return _BlockerEvidence(blockers=(), readable=False, present=True, any_non_empty_item=False)

    # String filter is correct for marker scanning; safety uses all items.
    blockers = tuple(str(item) for item in parsed if isinstance(item, str))
    any_item = any(str(item).strip() for item in parsed)
    return _BlockerEvidence(
        blockers=blockers,
        readable=True,
        present=True,
        any_non_empty_item=any_item,
    )


def _parse_worker_report_blockers(report: dict[str, Any]) -> list[str]:
    """Return decoded string blockers; total over any ``blockers_json`` shape.

    Never raises. Unreadable payloads yield ``[]`` — callers that need to
    distinguish unreadable from empty must use ``_decode_blockers_json``.
    """
    return list(_decode_blockers_json(report).blockers)


def _report_carries_blockers(report: dict[str, Any]) -> bool:
    """True when the durable worker report carries any non-empty blocker.

    Used by the completed_unreviewed reclassifier so genuine asks survive after
    a successful blocked handoff deletes the result file. Fail-closed on
    unparseable or non-list blocker payloads: malformed evidence is not evidence
    of absence (OBS-08). Empty / missing / whitespace-only / JSON null / ``[]``
    are treated as no blockers.

    List items are judged with the original total rule
    ``any(str(item).strip() for item in parsed)`` via
    ``any_non_empty_item`` — not the string-only ``blockers`` tuple used for
    marker scanning. Structured items such as ``[{"msg": "..."}]`` still carry.

    Missing-key fail-open: production ``_fresh_worker_report`` does
    ``SELECT *`` then ``dict(sqlite3.Row)``, so the ``blockers_json`` column is
    always present on real loader rows. The ``not present`` branch is only
    reachable from hand-built / mocked dicts; flipping it would churn tests
    for no production gain. The SELECT * invariant is pinned in tests.
    """
    evidence = _decode_blockers_json(report)
    if not evidence.present:
        return False  # missing-key fail-open, unchanged, still commented
    if not evidence.readable:
        return True  # OBS-08, unchanged
    return evidence.any_non_empty_item


def _first_nonempty_blocker(value: Any) -> str:
    """Return the first operator-visible blocker, ignoring blank list entries."""
    if not isinstance(value, list):
        return ""
    return next((str(item).strip() for item in value if str(item).strip()), "")


def _transport_failure_reason(exec_probe: dict[str, Any], raw_payload: Any) -> str:
    """Fold typed transport evidence into the single operator-facing reason."""
    blocker_reason = _first_nonempty_blocker(exec_probe.get("blockers"))
    if not isinstance(raw_payload, dict):
        return " ".join((blocker_reason or "remote provider transport failure").split())[:1_024]
    code_value = " ".join(str(raw_payload.get("provider_error_code") or blocker_reason).split())[:128]
    detail_value = " ".join(str(raw_payload.get("provider_error_detail") or "").split())
    if len(detail_value) > 1_024:
        detail_value = detail_value[:1_010].rstrip() + "… [truncated]"
    if code_value and detail_value:
        return f"{code_value}: {detail_value}"
    return code_value or detail_value or "remote provider transport failure"


def _is_composer_violation_handoff_report(report: dict[str, Any]) -> bool:
    summary = str(report.get("summary") or "")
    if _COMPOSER_VIOLATION_SUMMARY in summary:
        return True
    for blocker in _parse_worker_report_blockers(report):
        if any(marker in blocker for marker in _COMPOSER_VIOLATION_BLOCKER_MARKERS):
            return True
    return False


def _is_unparseable_handoff_report(report: dict[str, Any]) -> bool:
    if _is_composer_violation_handoff_report(report):
        return False
    summary = str(report.get("summary") or "")
    if summary != _UNPARSEABLE_SUMMARY:
        return False
    evidence = _decode_blockers_json(report)
    if not evidence.readable:
        # Unreadable blockers_json is not absence of the unparseable marker.
        # With a matching summary, keep the salvage route open rather than
        # fail-opening to an empty list and silently dropping the candidate.
        return True
    return any(_UNPARSEABLE_BLOCKER_PREFIX in blocker for blocker in evidence.blockers)


def _latest_worker_report(task_ref: str, lane_id: str) -> dict[str, Any] | None:
    from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

    payload = worker_reports(
        operation="list",
        task_ref=task_ref,
        lane_id=lane_id,
        limit=1,
        fields="id,session,summary,blockers_json,created_at",
    )
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return None
    reports = payload.get("reports")
    if not isinstance(reports, list) or not reports:
        return None
    report = reports[0]
    return report if isinstance(report, dict) else None


def _commits_since_start(worktree_path: Path, start_head: str) -> list[str]:
    if not start_head:
        return []
    output = _git_stdout(worktree_path, "rev-list", f"{start_head}..HEAD")
    if not output:
        return []
    return [sha.strip() for sha in output.splitlines() if sha.strip()]


def _passing_test_since_baseline(task_ref: str, lane_id: str, baseline_test_id: int) -> dict[str, Any] | None:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM verified_tests
            WHERE task_ref = ? AND (lane_id = ? OR lane_id IS NULL) AND id > ? AND passed = 1
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id, baseline_test_id),
        ).fetchone()
    return dict(row) if row is not None else None


def _tail_text(text: str, *, limit: int = 500) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[-limit:]


def _malformed_raw_output_tail(task_ref: str, lane_id: str) -> str:
    with sqlite3.connect(_handoff_db_path()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT message FROM lane_messages
            WHERE task_ref = ? AND lane_id = ? AND direction = 'worker_to_orchestrator'
            ORDER BY id DESC LIMIT 1
            """,
            (task_ref, lane_id),
        ).fetchone()
    if row is None:
        return ""
    return _tail_text(str(row["message"] or ""))


def _evaluate_malformed_handoff_salvage(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
    start_head: str,
    baseline_test_id: int,
    baseline_report_id: int,
) -> dict[str, Any] | None:
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    if report is None or not _is_unparseable_handoff_report(report):
        return None
    commits = _commits_since_start(worktree_path, start_head)
    if not commits:
        return None
    passing_test = _passing_test_since_baseline(task_ref, lane_id, baseline_test_id)
    if passing_test is None:
        return None
    return {
        "commit_shas": commits,
        "passing_test": {
            "id": passing_test.get("id"),
            "command": passing_test.get("command"),
            "verified_at": passing_test.get("verified_at"),
        },
        "worker_report_id": report.get("id"),
        "raw_output_tail": _malformed_raw_output_tail(task_ref, lane_id),
    }


def _record_salvage_audit_decision(
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    evidence: dict[str, Any],
) -> None:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    passing_test_raw = evidence.get("passing_test")
    passing_test = passing_test_raw if isinstance(passing_test_raw, dict) else {}
    test_id = passing_test.get("id", "unknown")
    commits_raw = evidence.get("commit_shas")
    commits = commits_raw if isinstance(commits_raw, list) else []
    raw_tail = str(evidence.get("raw_output_tail") or "")
    decision_id = f"offload_salvage_candidate_{task_ref}_{lane_id}_{test_id}"
    rationale = (
        "## Salvage candidate (malformed handoff)\n"
        "Worker produced committed, test-green work but the final grok turn was unparseable.\n\n"
        "## Evidence\n"
        f"- Commits: {', '.join(str(commit) for commit in commits)}\n"
        f"- Passing test: #{passing_test.get('id')} `{passing_test.get('command')}` "
        f"at {passing_test.get('verified_at')}\n"
        f"- Worker report: #{evidence.get('worker_report_id')}\n\n"
        "## Malformed raw output tail\n"
        f"```\n{raw_tail}\n```\n"
    )
    record_decision(
        session=session,
        decision=decision_id,
        rationale=rationale,
        task_ref=task_ref,
    )


def _collect_pass_findings(*, task_ref: str, limit: int = 50) -> list[dict[str, Any]]:
    """Return worker-recorded in-lane findings for the pass payload (T4 / [OBS-04]).

    Surfaces BR-* rows the worker wrote via MCP so the orchestrator does not need
    close-check archaeology after a degraded smoke review.
    """
    try:
        from workbay_handoff_mcp.review_findings_queries import list_review_findings  # noqa: PLC0415
    except Exception:  # pragma: no cover - optional import degrade
        return []
    try:
        envelope = list_review_findings(task_ref=task_ref, status="open", limit=limit, detail="summary")
    except Exception:  # pragma: no cover - I/O degrade; never fail the pass on listing
        return []
    data = envelope.get("data") if isinstance(envelope.get("data"), dict) else envelope
    if not isinstance(data, dict):
        return []
    raw = data.get("findings")
    if not isinstance(raw, list):
        return []
    findings: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        findings.append(
            {
                "finding_id": item.get("finding_id"),
                "severity": item.get("severity"),
                "file_path": item.get("file_path"),
                "description": item.get("description"),
                "status": item.get("status"),
            }
        )
    return findings


_FINDINGS_SCOPE_LIMIT = 500
# Independent cap on the read: 20 pages of 500 is 10,000 open rows.
_FINDINGS_SCOPE_MAX_PAGES = 20
_FINDINGS_SCOPE_LOOKUP_FAILED = "findings_scope_lookup_failed"
_FINDINGS_SCOPE_INCOMPLETE = "findings_scope_incomplete"
# Server severity order for open findings. Unknown severities sort first, as NULL does in SQLite.
_FINDINGS_SCOPE_SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
_LANE_VERIFICATION_FAILED_REASON = "lane verification failed after review convergence"


@dataclass(frozen=True)
class _LaneFindingsScope:
    """Open findings split between this lane and the rest of the task."""

    ok: bool
    lane_findings: list[dict[str, Any]]
    task_open_blockers: list[dict[str, Any]]
    code: str | None = None
    lane_high: list[dict[str, Any]] = dataclass_field(default_factory=list)
    downgraded_from: str | None = None


def _findings_scope_document(scope: _LaneFindingsScope) -> dict[str, Any]:
    document: dict[str, Any] = {"ok": scope.ok}
    if not scope.ok and scope.code:
        document["code"] = scope.code
    if scope.downgraded_from == OffloadOutcome.HANDOFF_READY.value and scope.lane_high:
        document["downgraded_from"] = scope.downgraded_from
        document["lane_high"] = scope.lane_high
    return document


def _scope_refusal(code: str) -> _LaneFindingsScope:
    return _LaneFindingsScope(ok=False, lane_findings=[], task_open_blockers=[], code=code)


def _normalize_repo_relative(path: str) -> str | None:
    value = path.strip().replace("\\", "/")
    while value.startswith("./"):
        value = value[2:]
    if not value or value in {".", "/"}:
        return None
    if value.startswith("/") or re.match(r"^[A-Za-z]:(?:/|$)", value):
        return None
    parts = [part for part in value.split("/") if part not in {"", "."}]
    if not parts or any(part == ".." for part in parts):
        return None
    return "/".join(parts)


def _owned_root(raw: str) -> str | None:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import _normalize_owned_path  # noqa: PLC0415

    try:
        return _normalize_owned_path(raw)
    except ValueError:
        return None


def _file_in_owned_paths(file_path: object, owned_paths: list[str]) -> bool:
    if not isinstance(file_path, str):
        return False
    path = _normalize_repo_relative(file_path)
    if path is None:
        return False
    for raw in owned_paths:
        root = _owned_root(raw)
        if root is None:
            continue
        if root == "" or path == root or path.startswith(root + "/"):
            return True
    return False


def _blank_lane_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _project_scoped_finding(item: dict[str, Any]) -> dict[str, Any]:
    projected: dict[str, Any] = {
        "finding_id": item.get("finding_id"),
        "severity": item.get("severity"),
        "file_path": item.get("file_path"),
        "description": item.get("description"),
        "status": item.get("status"),
        "lane_id": _blank_lane_id(item.get("lane_id")),
    }
    # Review scope v2: carry impact/origin only when stored, so legacy rows keep their shape.
    for key in ("impact", "origin"):
        if key in item:
            projected[key] = item[key]
    return projected


def _finding_is_high(item: dict[str, Any]) -> bool:
    """HIGH and blocking. A ``follow_up`` HIGH never holds the lane (review scope v2)."""
    return str(item.get("severity") or "").strip().casefold() == "high" and is_blocking(item)


def _finding_belongs_to_lane(item: dict[str, Any], *, lane_id: str, owned_paths: list[str]) -> bool:
    attributed = _blank_lane_id(item.get("lane_id"))
    if attributed is not None:
        return attributed == lane_id
    return _file_in_owned_paths(item.get("file_path"), owned_paths)


def _lane_owned_paths_for_findings_scope(
    *,
    task_ref: str,
    lane_id: str,
    orchestrator_root: Path,
) -> tuple[list[str] | None, str | None]:
    """Return owned paths, or ``(None, code)`` when the scope lookup itself failed.

    A missing manifest is an empty ownership set. A corrupt manifest is a
    lookup failure: treating it as "no owned paths" would hide a null-lane
    finding that sits on a path this lane owns.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config  # noqa: PLC0415
    except Exception:
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    try:
        lane = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except FileNotFoundError:
        return [], None
    except Exception:
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    if not isinstance(lane, dict):
        return [], None
    raw = lane.get("owned_paths", [])
    if raw is None:
        return [], None
    if not isinstance(raw, list):
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    owned: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            return None, _FINDINGS_SCOPE_LOOKUP_FAILED
        owned.append(item.strip())
    return owned, None


def _validate_findings_page(
    envelope: object,
    *,
    page_index: int,
    last_id: int | None,
) -> tuple[tuple[list[dict[str, Any]], bool, int | None] | None, str | None]:
    """Check one keyset page of open findings.

    Returns ``((rows, has_more, next_after_id), None)`` for a page the read can use, or ``(None, code)``
    for a page it must refuse. ``last_id`` is the cursor this page followed, so across the whole read
    the ids must strictly descend from it.
    """
    if not isinstance(envelope, dict) or envelope.get("ok") is not True:
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    data = envelope["data"] if "data" in envelope else envelope
    if not isinstance(data, dict):
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    has_more = data.get("has_more")
    if type(has_more) is not bool:
        return None, _FINDINGS_SCOPE_INCOMPLETE
    page = data.get("findings")
    if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
        return None, _FINDINGS_SCOPE_LOOKUP_FAILED
    # A terminal first page is the entire result; only its integer total must agree.
    # Multi-page totals can drift while the immutable-id cursor walk remains valid.
    total_matching = data.get("total_matching")
    if page_index == 0 and has_more is False and type(total_matching) is int and total_matching != len(page):
        return None, _FINDINGS_SCOPE_INCOMPLETE
    # A page that claims more rows must be full; a short one is not a page the server produced.
    if has_more is True and len(page) != _FINDINGS_SCOPE_LIMIT:
        return None, _FINDINGS_SCOPE_INCOMPLETE
    next_after_id = data.get("next_after_id")
    if page_index > 0 or has_more is True:
        # Multi-page read: each id is a plain int below the one before it, so a repeat, a reorder or a row
        # that moved between reads is refused. A cursor that does not name the last row is refused too.
        previous = last_id
        for item in page:
            row_id = item.get("id")
            if type(row_id) is not int or (previous is not None and row_id >= previous):
                return None, _FINDINGS_SCOPE_INCOMPLETE
            previous = row_id
        if has_more is True and (type(next_after_id) is not int or next_after_id != previous):
            return None, _FINDINGS_SCOPE_INCOMPLETE
    return (page, has_more, next_after_id), None


def _read_open_findings_keyset(
    list_review_findings: Any,
    *,
    task_ref: str,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Read every open finding by keyset cursor, or return ``(None, code)`` when completeness is not proven.

    The cursor is the table's immutable integer id, so a row closed or opened between two reads cannot move
    a still-open row across a page boundary. The walk is capped and never retries.
    """
    rows: list[dict[str, Any]] = []
    cursor: int | None = None
    for page_index in range(_FINDINGS_SCOPE_MAX_PAGES):
        try:
            envelope = list_review_findings(
                task_ref=task_ref,
                status="open",
                limit=_FINDINGS_SCOPE_LIMIT,
                after_id=cursor,
                detail="full",
            )
        except Exception:
            return None, _FINDINGS_SCOPE_LOOKUP_FAILED
        page, code = _validate_findings_page(envelope, page_index=page_index, last_id=cursor)
        if page is None:
            return None, code
        page_rows, has_more, next_after_id = page
        rows.extend(page_rows)
        if has_more is False:
            break
        cursor = next_after_id
    else:
        # Every page claimed more rows: the cap, not the data, ended the read.
        return None, _FINDINGS_SCOPE_INCOMPLETE
    # The keyset walk serves id DESC. Restore the order the server used (severity, then recency, then id).
    # Each sort is stable, so the least significant key is applied first.
    rows.sort(key=lambda item: item.get("id") if type(item.get("id")) is int else -1, reverse=True)
    rows.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
    rows.sort(key=lambda item: _FINDINGS_SCOPE_SEVERITY_RANK.get(str(item.get("severity")), -1))
    return rows, None


def _scope_lane_open_findings(
    *,
    task_ref: str,
    lane_id: str,
    orchestrator_root: Path,
) -> _LaneFindingsScope:
    """Split open task findings into this lane's rows and other lanes' HIGHs.

    A finding belongs to this lane when its ``lane_id`` matches, or when
    ``lane_id`` is blank and ``file_path`` is inside this lane's owned paths.
    Other open HIGHs are task blockers. They do not decide this lane's outcome.
    A failed or truncated lookup is a typed refusal, never an empty success.
    """
    owned_paths, owned_error = _lane_owned_paths_for_findings_scope(
        task_ref=task_ref,
        lane_id=lane_id,
        orchestrator_root=orchestrator_root,
    )
    if owned_error is not None or owned_paths is None:
        return _scope_refusal(_FINDINGS_SCOPE_LOOKUP_FAILED)
    try:
        from workbay_handoff_mcp.review_findings_queries import list_review_findings  # noqa: PLC0415
    except Exception:
        return _scope_refusal(_FINDINGS_SCOPE_LOOKUP_FAILED)
    raw, code = _read_open_findings_keyset(list_review_findings, task_ref=task_ref)
    if raw is None:
        return _scope_refusal(code or _FINDINGS_SCOPE_LOOKUP_FAILED)
    lane_findings: list[dict[str, Any]] = []
    task_open_blockers: list[dict[str, Any]] = []
    lane_high: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            return _scope_refusal(_FINDINGS_SCOPE_LOOKUP_FAILED)
        projected = _project_scoped_finding(item)
        if _finding_belongs_to_lane(item, lane_id=lane_id, owned_paths=owned_paths):
            lane_findings.append(projected)
            if _finding_is_high(item):
                lane_id_value = _blank_lane_id(item.get("lane_id"))
                evidence = {
                    "finding_id": item.get("finding_id"),
                    "file_path": item.get("file_path"),
                    "attribution": "lane_id" if lane_id_value is not None else "owned_path",
                }
                created_at = item.get("created_at")
                if created_at is not None:
                    evidence["created_at"] = created_at
                lane_high.append(evidence)
        elif _finding_is_high(item):
            task_open_blockers.append(projected)
    lane_high.sort(key=lambda item: str(item.get("finding_id") or ""))
    return _LaneFindingsScope(
        ok=True,
        lane_findings=lane_findings,
        task_open_blockers=task_open_blockers,
        lane_high=lane_high,
    )


def _apply_lane_findings_scope(
    scope: _LaneFindingsScope,
    *,
    outcome: str | None,
    failed_stage: str | None,
    error_reason: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Downgrade a successful lane pass when its own scope is unsafe.

    Only ``handoff_ready`` fails closed on this lane's own open HIGH. Task-level
    merge readiness stays with the landing gate. A lookup or truncated page
    refuses every success outcome instead of reading silence as a clean pass.
    An already-failing outcome keeps its reason.
    """
    if outcome not in PASS_SUCCESS_OUTCOMES:
        return outcome, failed_stage, error_reason
    if not scope.ok:
        return (
            OffloadOutcome.VERIFICATION_HARNESS_ERROR.value,
            "review",
            scope.code or _FINDINGS_SCOPE_LOOKUP_FAILED,
        )
    if outcome == OffloadOutcome.HANDOFF_READY.value and any(_finding_is_high(item) for item in scope.lane_findings):
        return (
            OffloadOutcome.VERIFICATION_FAILED.value,
            "review",
            _LANE_VERIFICATION_FAILED_REASON,
        )
    return outcome, failed_stage, error_reason


# ---------------------------------------------------------------------------
# implementation note R2: harvest GROK_REVIEW_FINDINGS_JSON from review-lane terminal results
# ---------------------------------------------------------------------------

_GROK_REVIEW_FINDINGS_MARKER = "GROK_REVIEW_FINDINGS_JSON"
_MARKER_LINE_RE = re.compile(rf"(?m)^{re.escape(_GROK_REVIEW_FINDINGS_MARKER)}(?=[ \t\r\n:=\[{{]|$)[ \t]*[:=]?[ \t]*")
# Harness-agnostic completeness marker. Last occurrence in details, then
# summary, wins. Tokens are exact and case-sensitive.
_REVIEW_VERDICT_LINE_RE = re.compile(r"(?m)^REVIEW_VERDICT:[ \t]*(\S+)[ \t]*\r?$")
_REVIEW_COMPLETE_TOKENS = frozenset({"pass", "pass_with_findings", "fail"})
_REVIEW_INCOMPLETE_TOKEN = "incomplete"
_CARRIER_INCOMPLETE_STATUSES = frozenset({"skipped_unparseable", "refused"})
REVIEW_COMPLETION_DECLARED = "review_verdict_declared"
REVIEW_COMPLETION_INCOMPLETE = "review_incomplete"
REVIEW_COMPLETION_UNAVAILABLE = "review_verdict_unavailable"
REVIEW_COMPLETION_INVALID = "review_verdict_invalid"
# Opening fence only — do NOT match a closing fence with a non-greedy span:
# real finding descriptions can embed ``` (code-fence examples) which would
# truncate a ``.*?````` capture mid-JSON (R1 fixture, attribution-parsing item).
_OPEN_FENCE_RE = re.compile(r"^```(?:json)?[ \t]*\r?\n", re.IGNORECASE)
_VALID_HARVEST_SEVERITIES = frozenset({"high", "medium", "low"})
_UNSTRUCTURED_SALVAGE_MAX_CHARS = 4_000
_UNSTRUCTURED_SALVAGE_PREFIX = "UNSTRUCTURED SALVAGE: "
_UNSTRUCTURED_SALVAGE_FILE_PATH = "<review-output>"
_SALVAGE_SEVERITY_MARKER = re.compile(
    r"\[(CRITICAL|HIGH|MEDIUM|LOW)\]|\*\*(CRITICAL|HIGH|MEDIUM|LOW)\*\*|"
    r"(?<![\w-])severity\b\s*:\s*(CRITICAL|HIGH|MEDIUM|LOW)(?![\w-])",
    re.IGNORECASE,
)
_DISPATCH_REFUSAL_CARRIER = re.compile(
    r"^\s*(?:"
    r"[\w.-]+\s+refused(?::|\s+before\s+(?:dispatch|ssh|model\s+spawn|probing)\b)|"
    r"(?:backend|dispatch)\s+(?:refusal|refused|failed|failure)\b|"
    r"(?:transport|ssh)\s+(?:error|failure|failed|refused)\b|"
    r"ssh:|"
    r"connection\s+(?:timed\s+out|refused|reset|closed)\b|"
    r"(?:timeout(?:error)?|connectionerror):"
    r")",
    re.IGNORECASE,
)


def _salvage_severity(text: str) -> tuple[str, dict[str, int]]:
    """Return the most severe explicit review marker and counts by severity."""
    counts = {"high": 0, "medium": 0, "low": 0}
    for match in _SALVAGE_SEVERITY_MARKER.finditer(text):
        marker = next(group for group in match.groups() if group is not None).lower()
        severity = "high" if marker == "critical" else marker
        counts[severity] += 1

    if counts["high"]:
        severity = "high"
    elif counts["medium"]:
        severity = "medium"
    else:
        severity = "low"
    return severity, counts


def _is_dispatch_refusal_carrier(text: str) -> bool:
    """Recognize adapter failures that mean the reviewer never got a turn."""
    return _DISPATCH_REFUSAL_CARRIER.search(text) is not None


def _is_typed_dispatch_refusal(raw_payload: Any) -> bool:
    """Recognize explicit adapter provenance that proves no review turn ran."""
    if not isinstance(raw_payload, dict):
        return False
    pre_dispatch = raw_payload.get("pre_dispatch_refusal")
    if isinstance(pre_dispatch, dict) and pre_dispatch.get("outcome") == "dispatch_refused":
        return True
    return raw_payload.get("adapter_refusal") is True


def _extract_findings_json(text: str) -> tuple[Any | None, str]:
    """Locate ``GROK_REVIEW_FINDINGS_JSON`` and parse the following JSON value.

    Returns ``(parsed, status)`` where status is one of:
    ``no_marker``, ``unparseable_block``, ``ok``.

    After the marker (optionally followed by : or =), accept inline JSON or
    a following line with an optional opening ```` ```json ```` /
    ```` ``` ```` fence then ``json.JSONDecoder.raw_decode`` the payload. Using
    raw_decode (not closing-fence scan) tolerates nested ``` inside description
    strings (implementation note R1 fixture).
    """
    if not isinstance(text, str) or not text:
        return None, "no_marker"
    matches = list(_MARKER_LINE_RE.finditer(text))
    if not matches:
        return None, "no_marker"
    # The marker is a machine contract, not an append-only log. More than one
    # block is ambiguous (and may contain contradictory verdicts), so never
    # select the first block and silently ignore the rest.
    if len(matches) != 1:
        return None, "unparseable_block"
    match = matches[0]
    rest = text[match.end() :]
    # Drop a single leading newline after the marker line.
    if rest.startswith("\r\n"):
        rest = rest[2:]
    elif rest.startswith("\n"):
        rest = rest[1:]
    if not rest.strip():
        return None, "unparseable_block"
    open_fence = _OPEN_FENCE_RE.match(rest)
    if open_fence is not None:
        rest = rest[open_fence.end() :]
    try:
        parsed, _end = json.JSONDecoder().raw_decode(rest.lstrip())
    except (json.JSONDecodeError, ValueError, TypeError):
        return None, "unparseable_block"
    return parsed, "ok"


def _tolerant_harvest_items(raw_items: Any) -> tuple[list[dict[str, Any]], int]:
    """Keep items with non-empty file_path + description; free-form categories.

    Intentionally does **not** use ``review_runner._validate_review_result``:
    real review-lane categories are free-form (``contract-drift``, …) and the
    self-review enum would drop every finding ([DATA-03] / implementation note R2).
    Severity is stripped and case-normalized to {high,medium,low}. Unknown
    values become high with severity_raw and severity_coerced audit metadata;
    category defaults to ``GAP`` when absent.
    """
    if not isinstance(raw_items, list):
        return [], 0
    kept: list[dict[str, Any]] = []
    dropped = 0
    for item in raw_items:
        if not isinstance(item, dict):
            dropped += 1
            continue
        file_path = item.get("file_path")
        description = item.get("description")
        if not isinstance(file_path, str) or not file_path.strip():
            dropped += 1
            continue
        if not isinstance(description, str) or not description.strip():
            dropped += 1
            continue
        severity_raw = item.get("severity")
        severity = severity_raw.strip().lower() if isinstance(severity_raw, str) else None
        severity_coerced = severity not in _VALID_HARVEST_SEVERITIES
        if severity_coerced:
            severity = "high"
        category_raw = item.get("category")
        category = category_raw.strip() if isinstance(category_raw, str) and category_raw.strip() else "GAP"
        normalized: dict[str, Any] = {
            "severity": severity,
            "category": category,
            "file_path": file_path.strip(),
            "description": description.strip(),
        }
        if severity_coerced:
            normalized["severity_raw"] = severity_raw
            normalized["severity_coerced"] = True
        from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
            _review_protocol_fields,
        )

        # The runner owns v2 tags and D6 format classification on every carrier.
        normalized.update(_review_protocol_fields({**item, "category": category}))
        if isinstance(item.get("evidence"), dict):
            normalized["evidence"] = dict(item["evidence"])
        line_start = item.get("line_start")
        if isinstance(line_start, int):
            normalized["line_start"] = line_start
        line_end = item.get("line_end")
        if isinstance(line_end, int):
            normalized["line_end"] = line_end
        fix = item.get("fix")
        if isinstance(fix, str):
            normalized["fix"] = fix
        kept.append(normalized)
    return kept, dropped


def _rematerialize_details_artifact(source_id: int, orchestrator_root: Path) -> str | None:
    """Reassemble dematerialized details from the artifact sidecar. Fail-open → None."""
    try:
        from workbay_handoff_mcp import (
            RuntimeConfig,  # noqa: PLC0415
            artifact_index,  # noqa: PLC0415
        )

        artifact_db_path = RuntimeConfig.for_repo(orchestrator_root).artifact_db_path
        source = artifact_index.get_artifact_source(
            source_id=int(source_id),
            artifact_db_path=artifact_db_path,
        )
        if not isinstance(source, dict):
            return None
        chunks = source.get("chunks") or []
        if not chunks:
            return None
        parts: list[str] = []
        for chunk in chunks:
            if isinstance(chunk, dict) and isinstance(chunk.get("body"), str):
                parts.append(chunk["body"])
        if not parts:
            return None
        return "\n".join(parts)
    except Exception:  # noqa: BLE001 — artifact_unavailable: never raise from harvest
        return None


def _report_scan_text(report: dict[str, Any] | str | None) -> str | None:
    """Scannable text from a worker report carrier (expected miss for real reviews)."""
    if report is None:
        return None
    if isinstance(report, str):
        return report if report.strip() else None
    if not isinstance(report, dict):
        return None
    details = report.get("details")
    if isinstance(details, str) and details.strip():
        return details
    return None


def _extract_style_telemetry(*results: Any) -> tuple[int | None, list[str]]:
    """Extract style telemetry from the final lane gate, then self-verify fallback."""
    style_debt: int | None = None
    advisories: list[str] = []
    for candidate in results:
        if not isinstance(candidate, dict):
            continue
        if not advisories:
            raw_advisories = candidate.get("advisories")
            if isinstance(raw_advisories, list):
                advisories = [str(item) for item in raw_advisories]
        raw_style_debt = candidate.get("style_debt")
        if (
            style_debt is None
            and candidate.get("style_debt_reported") is True
            and isinstance(raw_style_debt, int)
            and not isinstance(raw_style_debt, bool)
        ):
            style_debt = raw_style_debt
    return style_debt, advisories


def _parse_findings_from_text(text: str) -> tuple[str, list[dict[str, Any]] | None, int]:
    """Scan one carrier text.

    Returns ``(status, findings_or_none, dropped)`` where status is one of:
    ``no_marker``, ``unparseable_block``, ``no_valid_findings``, ``ok``.
    """
    parsed, extract_status = _extract_findings_json(text)
    if extract_status == "no_marker":
        return "no_marker", None, 0
    if extract_status == "unparseable_block" or parsed is None:
        return "unparseable_block", None, 0
    if isinstance(parsed, dict):
        parsed = parsed.get("findings")
    # Fail closed on roots without an explicit findings list; they must never
    # become a "reviewed clean" empty array (implementation note review, grok/B2).
    if not isinstance(parsed, list):
        return "unparseable_block", None, 0
    kept, dropped = _tolerant_harvest_items(parsed)
    if not kept:
        return "no_valid_findings", None, dropped
    return "ok", kept, dropped


def _harvest_resolution_metadata(text: str) -> dict[str, Any]:
    """Pass through resolution IDs for the coordinator, without resolving rows."""
    parsed, _ = _extract_findings_json(text)
    if isinstance(parsed, dict) and isinstance(parsed.get("findings"), list):
        resolved = parsed.get("resolved_findings")
        if isinstance(resolved, list) and all(isinstance(item, str) for item in resolved):
            return {"resolved_findings": resolved}
    return {}


def _lane_kind(orchestrator_root: Path, task_ref: str, lane_id: str) -> tuple[str | None, bool]:
    """Return (kind, confirmed). confirmed only when MCP params carry an explicit kind.

    Fail-closed: unresolved lookup (exception, missing params, or invalid kind)
    returns ``(None, False)`` — never fabricates the domain default ``"implement"``
    (cs0166-r07-10 / DOM-06). Callers must refuse implement-only and review-only
    routing until kind is confirmed; treating a blipped review lane as implement
    would run TEST_CMD and misclassify clean-tree needs_guidance as transport failure.
    """
    try:
        from workbay_orchestrator_mcp.orchestration import worker_daemon as _wd  # noqa: PLC0415

        params = _wd._fetch_mcp_lane_params(Path(orchestrator_root), task_ref, lane_id)
        if not isinstance(params, dict):
            return None, False
        kind = params.get("lane_kind")
        if kind in ("implement", "review"):
            return str(kind), True
        return None, False
    except Exception:  # noqa: BLE001 — unresolved is typed unavailable, not implement
        return None, False


def _claim_contradicted_by_evidence(exec_probe: dict[str, Any], raw_payload: Any) -> bool:
    """True when structured payload claims finished work under a clean, unmoved tree.

    Structured signals only — never NLP-match the free-form summary alone (implementation note S4).
    """
    if bool(exec_probe.get("tests_run")):
        return True
    if isinstance(raw_payload, dict):
        if raw_payload.get("handoff_action") == "merge_ready":
            return True
        if raw_payload.get("merge_ready") is True:
            return True
    return False


# Machine-readable harvest verdicts threaded onto the pass payload (fail-closed
# review contract). Only ``recorded`` and ``reviewed_clean`` may produce a
# passing review outcome; ``harvest_failed`` must never look like either.
# ``not_attempted`` is emitted only for non-review terminals (no review product
# expected) and is intentionally excluded from _PASSING_HARVEST_VERDICTS so it
# can never promote a lane to review_complete.
HARVEST_VERDICT_RECORDED = "recorded"
HARVEST_VERDICT_REVIEWED_CLEAN = "reviewed_clean"
HARVEST_VERDICT_HARVEST_FAILED = "harvest_failed"
HARVEST_VERDICT_NOT_ATTEMPTED = "not_attempted"
_PASSING_HARVEST_VERDICTS = frozenset({HARVEST_VERDICT_RECORDED, HARVEST_VERDICT_REVIEWED_CLEAN})


def _harvest_verdict(harvest: dict[str, Any] | None) -> str:
    """Stable typed harvest verdict from harvest *data* [AGT-21][OBS-08][SECD-05].

    - ``recorded``: ≥1 finding parsed and persisted with no malformed items
      dropped (status == recorded and dropped == 0).
    - ``reviewed_clean``: well-formed EMPTY findings array (reason
      ``no_valid_findings`` with ``dropped == 0``) — genuine reviewed-and-found-nothing.
    - ``harvest_failed``: every other case — including ``salvaged_unstructured``,
      missing/duplicate marker, unparseable JSON, any malformed array item
      (``dropped > 0``), persist failure, or non-dict harvest. Never eligible for ``review_complete``;
      must not be collapsed into the empty-findings shape of ``reviewed_clean``.

    Callers that emit onto the pass payload must further gate with
    ``review_product_harvest``: non-review terminals emit
    ``HARVEST_VERDICT_NOT_ATTEMPTED`` instead of ``harvest_failed`` so
    implement lanes never look like broken review capture.
    """
    if not isinstance(harvest, dict):
        return HARVEST_VERDICT_HARVEST_FAILED
    dropped = harvest.get("dropped", 0)
    if harvest.get("status") == "recorded" and type(dropped) is int and dropped == 0:
        return HARVEST_VERDICT_RECORDED
    if harvest.get("status") == "salvaged_unstructured":
        return HARVEST_VERDICT_HARVEST_FAILED
    if (harvest.get("reason") == "no_valid_findings" and type(dropped) is int and dropped == 0
            and not harvest.get("unverified_listed_finding_ids")):
        return HARVEST_VERDICT_REVIEWED_CLEAN
    return HARVEST_VERDICT_HARVEST_FAILED


def _pass_result_summary(
    final_result: dict[str, Any] | None,
    *,
    outcome: str,
    error: str | None = None,
    reason: str | None = None,
) -> str:
    """Return the required non-empty summary for a terminal pass envelope."""
    candidates = (error, final_result.get("summary") if isinstance(final_result, dict) else None, reason)
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return f"Offload pass ended with outcome '{outcome}'."


def _harvest_block_parsed(harvest: dict[str, Any] | None) -> bool:
    """True when harvest found a *clean well-formed* GROK_REVIEW_FINDINGS_JSON block (implementation note R3).

    ``recorded`` (>=1 finding) or an EMPTY array (``no_valid_findings`` with
    ``dropped == 0`` — "reviewed clean") both mean a reviewer emitted a well-formed
    block. Completeness is a separate ``_review_completion`` predicate; a parsed
    block is necessary for ``review_complete`` but not sufficient. A missing
    marker (``no_findings_block``), invalid / non-list JSON (``unparseable_block``),
    an ALL-MALFORMED array (``no_valid_findings`` with ``dropped > 0`` — a broken
    reviewer), or a persist failure (``record_failed``) is NOT a clean block and
    must not type review_complete (grok/B2 false-success guard).
    """
    return _harvest_verdict(harvest) in _PASSING_HARVEST_VERDICTS


def _last_review_verdict_token(text: str) -> str | None:
    """Return the last ``REVIEW_VERDICT: <token>`` on its own line, or None."""
    token = None
    for match in _REVIEW_VERDICT_LINE_RE.finditer(text):
        token = match.group(1)
    return token


def _review_completion(final_result: dict[str, Any] | None) -> dict[str, Any]:
    """Typed review completeness. Fail closed [SECD-05][AGT-21][OBS-08].

    Returns ``{"complete": bool, "verdict": str | None, "reason": str}``.
    Never infers completeness from harvest, handoff_action, blockers, or summary prose.
    """
    unavailable = {
        "complete": False,
        "verdict": None,
        "reason": REVIEW_COMPLETION_UNAVAILABLE,
    }
    if not isinstance(final_result, dict):
        return unavailable
    if final_result.get("review_result_carrier") is True:
        status = final_result.get("review")
        if not isinstance(status, str) or not status:
            status = "ok"
        complete = status not in _CARRIER_INCOMPLETE_STATUSES
        return {
            "complete": complete,
            "verdict": f"carrier:{status}",
            "reason": REVIEW_COMPLETION_DECLARED if complete else REVIEW_COMPLETION_INCOMPLETE,
        }
    token: str | None = None
    for key in ("details", "summary"):
        text = final_result.get(key)
        if not isinstance(text, str) or not text:
            continue
        found = _last_review_verdict_token(text)
        if found is not None:
            token = found
    if token is None:
        return unavailable
    if token in _REVIEW_COMPLETE_TOKENS:
        return {"complete": True, "verdict": token, "reason": REVIEW_COMPLETION_DECLARED}
    if token == _REVIEW_INCOMPLETE_TOKEN:
        return {"complete": False, "verdict": token, "reason": REVIEW_COMPLETION_INCOMPLETE}
    return {"complete": False, "verdict": token, "reason": REVIEW_COMPLETION_INVALID}


def _safe_final_result(run_ctx: Any) -> dict[str, Any]:
    """Read the current spool, pre-cleanup snapshot, or exact remote result."""
    from workbay_orchestrator_mcp.orchestration import worker_daemon as _wd  # noqa: PLC0415

    try:
        if getattr(run_ctx, "final_result_path", None):
            loaded = _wd._load_result(Path(run_ctx.final_result_path))
            if isinstance(loaded, dict) and loaded:
                return loaded
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
        pass
    snapshot = getattr(run_ctx, "final_result_snapshot", None)
    if isinstance(snapshot, dict) and snapshot:
        return snapshot
    # The adapter stamps this execution's raw_payload.result_path. Never glob
    # for the newest lane spool: that could silently import an older verdict.
    try:
        remote_path = getattr(run_ctx, "remote_result_path", None)
        if remote_path:
            loaded = _wd._load_result(Path(remote_path))
            if isinstance(loaded, dict):
                return loaded
    except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
        pass
    return {}


def _safe_latest_worker_report(task_ref: str, lane_id: str) -> dict[str, Any] | str | None:
    """Best-effort latest worker report for harvest; ``None`` on any failure (implementation note)."""
    try:
        return _latest_worker_report(task_ref, lane_id)
    except Exception:  # noqa: BLE001 — report load is best-effort for harvest
        return None


def _is_transport_only_carrier(carrier_name: str, text: str, raw_payload: Any) -> bool:
    """Exclude adapter-stamped stderr copies, never infer provenance from prose.

    Reports are independent reviewer evidence even when they quote the same
    diagnostic. The adapter must stamp the exact stderr tail it forwards into
    result fields; absent that provenance, those fields remain reviewer text.
    """
    if carrier_name == "report" or not isinstance(raw_payload, dict):
        return False
    return any(
        isinstance(tail, str) and bool(tail.strip()) and text.strip() == tail.strip()
        for tail in (raw_payload.get("transport_stderr_tail"), raw_payload.get("stderr_tail"))
    )


_REVIEW_REQUIRED_IDENTITY_FIELDS = (
    "pass_id",
    "task_ref",
    "lane_id",
    "base_sha",
    "tip_sha",
    "backend",
    "model",
    "reasoning_effort",
)
_REVIEW_DECLARABLE_IDENTITY_FIELDS = ("dispatch_id", "agent_stream_path")
_REVIEW_IDENTITY_FIELDS = _REVIEW_REQUIRED_IDENTITY_FIELDS + _REVIEW_DECLARABLE_IDENTITY_FIELDS


def _review_identity_complete(identity: dict[str, Any]) -> bool:
    return all(
        isinstance(identity.get(key), str) and bool(identity[key].strip()) for key in _REVIEW_REQUIRED_IDENTITY_FIELDS
    ) and all(
        key in identity and (identity[key] is None or (isinstance(identity[key], str) and bool(identity[key].strip())))
        for key in _REVIEW_DECLARABLE_IDENTITY_FIELDS
    )


def _review_identity_matches(saved: dict[str, Any], current: dict[str, Any]) -> bool:
    return (
        _review_identity_complete(saved)
        and _review_identity_complete(current)
        and all(saved.get(key) == current.get(key) for key in _REVIEW_IDENTITY_FIELDS)
    )


class _IncompleteReviewStream(ValueError):
    pass


def _review_stream_text(identity: dict[str, Any]) -> str | None:
    """Read only the bounded terminal tail of this pass's attributed spool."""
    path = identity.get("agent_stream_path")
    if not identity.get("pass_id") or not isinstance(path, str) or not path:
        return None
    try:
        if not Path(path).is_file():
            return None
        with Path(path).open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            stream.seek(max(0, size - 2 * 1024 * 1024))
            tail = stream.read(2 * 1024 * 1024)
            if size > 2 * 1024 * 1024:
                boundary = tail.find(b"\n")
                if boundary < 0:
                    raise _IncompleteReviewStream()
                tail = tail[boundary + 1 :]
            lines = tail.decode("utf-8", errors="replace").splitlines()
    except OSError:
        return None

    def texts(value: Any, depth: int = 0) -> list[str]:
        if depth > 12:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [text for item in value for text in texts(item, depth + 1)]
        if isinstance(value, dict):
            if value.get("role") in {"user", "system", "tool"} or value.get("type") in {
                "user",
                "tool_use",
                "tool_result",
            }:
                return []
            return [
                text
                for key in ("text", "details", "content", "message", "delta", "result", "item", "output")
                if key in value
                for text in texts(value[key], depth + 1)
            ]
        return []

    chunks = []
    incomplete = False
    for line in lines:
        if len(line) > 1024 * 1024:
            incomplete = True
            continue
        try:
            candidate = "\n".join(texts(json.loads(line)))
        except (ValueError, RecursionError):
            incomplete = True
            continue
        chunks.append(candidate)
    text = "\n".join(chunks)
    marker = text.rfind("GROK_REVIEW_FINDINGS_JSON")
    if incomplete:
        raise _IncompleteReviewStream()
    return text[marker:] if marker >= 0 else None


def _harvest_structured_degraded_findings(
    findings: list[dict[str, Any]],
    *,
    task_ref: str,
    lane_id: str | None,
    session: str,
    orchestrator_root: Path,
    reconcile_committed: bool = False,
    operation_token: str | None = None,
) -> dict[str, Any]:
    """Record schema-valid degraded findings without a marker or parse rewrite.

    The self-reported ``result_parse`` stays ``degraded`` (OBS-08). A failed
    write names ``unrecorded_ids`` and never looks recorded.
    """
    from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
        FindingsPersistError,
        _persist_shortfall_block,
        _record_findings,
        finding_stable_id,
    )

    findings = _link_prior_findings(findings, task_ref=task_ref, orchestrator_root=Path(orchestrator_root))
    finding_links = _harvest_finding_links(findings, lane_id)
    record_kwargs: dict[str, Any] = {}
    if reconcile_committed:
        record_kwargs = {"reconcile_committed": True, "operation_token": operation_token}

    def _unrecorded_fallback() -> list[str]:
        ids = [finding_stable_id(item) for item in findings if isinstance(item, dict)]
        return ids or ["unrecorded"]

    try:
        persist = _record_findings(
            findings,
            task_ref=task_ref,
            session=session,
            lane_id=lane_id,
            orchestrator_root=Path(orchestrator_root),
            **record_kwargs,
        )
    except FindingsPersistError as exc:
        shortfall = _persist_shortfall_block(exc)
        if not shortfall["unrecorded_ids"]:
            shortfall["unrecorded_ids"] = _unrecorded_fallback()
        return {
            "status": "skipped",
            "reason": "record_failed",
            "result_parse": "degraded",
            **shortfall,
        }
    except Exception as exc:  # noqa: BLE001 — harvest must never raise
        return {
            "status": "skipped",
            "reason": "record_failed",
            "result_parse": "degraded",
            "recorded_ids": [],
            "unrecorded_ids": _unrecorded_fallback(),
            "unrecorded_findings": [dict(item) for item in findings if isinstance(item, dict)],
            "causes": {"record": f"{type(exc).__name__}: {exc}"},
        }
    findings = _link_prior_findings(findings, task_ref=task_ref, orchestrator_root=Path(orchestrator_root))
    finding_links = _harvest_finding_links(findings, lane_id)
    shortfall = _persist_shortfall_block(persist)
    recorded_ids = list(shortfall["recorded_ids"])
    if shortfall["unrecorded_ids"] or not recorded_ids:
        if not shortfall["unrecorded_ids"]:
            shortfall["unrecorded_ids"] = _unrecorded_fallback()
        return {
            "status": "skipped",
            "reason": "record_failed",
            "result_parse": "degraded",
            "legacy_reconciliation": persist.get("legacy_reconciliation", "none"),
            "legacy_cause": persist.get("legacy_cause"),
            **shortfall,
        }
    return {
        "status": "recorded",
        "result_parse": "degraded",
        "legacy_reconciliation": persist.get("legacy_reconciliation", "none"),
        "legacy_cause": persist.get("legacy_cause", shortfall["causes"].get("legacy_reconciliation")),
        "count": len(recorded_ids),
        "finding_ids": [
            next((link["finding_id"] for link in finding_links if link["reported_finding_id"] == fid), fid)
            for fid in recorded_ids
        ],
        "finding_links": finding_links,
        "carrier": "schema",
        "dropped": 0,
        **shortfall,
    }


def _stamp_schema_degraded_review(review_output: Any, **kwargs: Any) -> Any:
    """Persist a schema-valid degraded review onto a copy of the review output."""
    if not isinstance(review_output, dict):
        return review_output
    from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
        finding_stable_id,
        schema_degraded_findings,
    )

    findings = schema_degraded_findings(review_output)
    if not findings:
        return review_output
    harvest = _harvest_structured_degraded_findings(findings, **kwargs)
    stamped = dict(review_output)
    stamped["result_parse"] = "degraded"
    recorded = harvest.get("recorded_ids")
    if not isinstance(recorded, list):
        recorded = harvest.get("finding_ids")
    stamped["recorded_finding_ids"] = (
        [str(item) for item in recorded if isinstance(item, str) and item] if isinstance(recorded, list) else []
    )
    raw_unrecorded = harvest.get("unrecorded_ids")
    unrecorded_ids = (
        [str(item) for item in raw_unrecorded if isinstance(item, str) and item]
        if isinstance(raw_unrecorded, list)
        else []
    )
    if harvest.get("status") != "recorded" and not unrecorded_ids:
        unrecorded_ids = [finding_stable_id(item) for item in findings]
    stamped["unrecorded_ids"] = unrecorded_ids
    return stamped


def _link_prior_findings(
    findings: list[dict[str, Any]],
    *,
    task_ref: str,
    orchestrator_root: Path,
) -> list[dict[str, Any]]:
    """Apply durable dispositions before recording or deciding on another fix round."""
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.review_findings_recording import link_prior_review_findings

    runtime = RuntimeConfig.for_repo(orchestrator_root)
    if not isinstance(runtime.db_path, Path) or not runtime.db_path.exists():
        return findings
    configure_runtime(runtime)
    return link_prior_review_findings(task_ref, findings)


def _harvest_finding_links(findings: list[dict[str, Any]], lane_id: str | None) -> list[dict[str, Any]]:
    from workbay_orchestrator_mcp.orchestration.review_runner import _generate_finding_id  # noqa: PLC0415

    occurrences: dict[str, int] = {}
    links = []
    for finding in sorted(findings, key=lambda f: (_generate_finding_id(lane_id, 0, f), json.dumps(f, sort_keys=True))):
        base_id = _generate_finding_id(lane_id, 0, finding)
        occurrences[base_id] = occurrences.get(base_id, 0) + 1
        ordinal = occurrences[base_id]
        reported_id = base_id if ordinal == 1 else f"{base_id}-{ordinal}"
        if finding.get("linked_finding_id"):
            links.append(
                {
                    "reported_finding_id": reported_id,
                    "finding_id": finding["linked_finding_id"],
                    "prior_disposition": finding["prior_disposition"],
                }
            )
    return links


def _harvest_review_findings(
    result: dict[str, Any],
    report: dict[str, Any] | str | None,
    *,
    task_ref: str,
    lane_id: str | None,
    session: str,
    orchestrator_root: Path,
    salvage_unstructured: bool = True,
    receipt_path: Path | None = None,
    receipt_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist review intent before MCP writes; retain source and attribution."""
    from workbay_orchestrator_mcp.orchestration.pass_rescue import _atomic_write

    kwargs = dict(
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        orchestrator_root=str(orchestrator_root),
        salvage_unstructured=salvage_unstructured,
    )
    import uuid

    identity = {**(receipt_identity or {}), "task_ref": task_ref, "lane_id": lane_id}
    receipt = dict(
        identity=identity,
        result=result,
        report=report,
        kwargs=kwargs,
        status="record_pending",
        operation_token=result.get("review_operation_token") or uuid.uuid4().hex,
    )
    if receipt_path is not None:
        if not _review_identity_complete(identity):
            return {"status": "refused", "reason": "identity_incomplete"}
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            saved_identity = {
                **receipt["identity"],
                "task_ref": receipt["kwargs"]["task_ref"],
                "lane_id": receipt["kwargs"].get("lane_id"),
            }
            if not _review_identity_complete(saved_identity) or not _review_identity_complete(identity):
                return {"status": "refused", "reason": "identity_incomplete"}
            if not _review_identity_matches(saved_identity, identity) or any(
                saved_identity.get(key) != identity.get(key) for key in ("branch", "worktree_path")
                if saved_identity.get(key) is not None or identity.get(key) is not None
            ):
                return {"status": "refused", "reason": "review_receipt_identity_mismatch"}
            if receipt.get("status") == "recorded":
                return receipt["harvest"]
            result, report, kwargs = receipt["result"], receipt["report"], receipt["kwargs"]
        else:
            _atomic_write(receipt_path, json.dumps(receipt))
    from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
        schema_degraded_findings,
    )

    # A schema-valid degraded document is harvested from its findings. Do not
    # promote result_parse to ok or replace the document with a stream marker.
    structured_degraded = schema_degraded_findings(result)
    try:
        stream_text = None if structured_degraded is not None else _review_stream_text(identity)
    except _IncompleteReviewStream:
        return {"status": "skipped", "reason": "incomplete_stream"}
    if stream_text:
        result = {**result, "details": stream_text, "result_parse": "ok"}
    review_context = None
    if receipt_path is not None and identity.get("branch"):
        from workbay_orchestrator_mcp.orchestration.review_ratchet import RatchetRefused, load_context

        try:
            review_context = load_context(Path(identity.get("worktree_path") or orchestrator_root), receipt_path.parent,
                                          task_ref=task_ref, branch=identity["branch"],
                                          current_pass_id=identity.get("pass_id"), before=identity.get("started_at"),
                                          orchestrator_root=Path(orchestrator_root))
        except RatchetRefused as exc:
            return {"status": "refused", "reason": str(exc)}
    harvest = _harvest_review_findings_once(
        result,
        report,
        reconcile_committed=receipt_path is not None,
        operation_token=receipt.get("operation_token") if receipt_path is not None else None,
        review_context=review_context,
        review_repo=Path(identity.get("worktree_path") or orchestrator_root),
        **kwargs,
    )
    if receipt_path is not None and harvest.get("reason") == "record_failed":
        harvest = {**harvest, "status": "record_pending"}
    if review_context is not None:
        from workbay_orchestrator_mcp.orchestration.review_ratchet import unresolved_listed

        harvest["unverified_listed_finding_ids"] = unresolved_listed(Path(orchestrator_root), review_context,
                                                                   task_ref=task_ref)
    carried = (harvest.get("unverified_listed_finding_ids") or []
               if receipt_path is not None and review_context is not None else [])
    if receipt_path is not None and (harvest.get("status") == "recorded" or carried):
        from workbay_handoff_mcp.shared_schema import _get_db_connection
        from workbay_protocol.review_impact import is_blocking

        with _get_db_connection() as conn:
            ids = set(harvest.get("finding_ids") or [])
            harvest["blocking_findings"] = [dict(row) for row in conn.execute(
                "SELECT * FROM review_findings WHERE task_ref = ?", (task_ref,))
                if row["finding_id"] in ids and row["status"] == "open" and is_blocking(dict(row))]
        # The next load_context names only this receipt's blocking snapshots. Carry
        # each still-unverified named blocker forward with its original snapshot
        # until an explicit verified disposition releases it.
        if carried:
            published = {row["finding_id"] for row in harvest["blocking_findings"]}
            snapshots = {str(item["finding_id"]): item for item in review_context.listed_findings}
            harvest["blocking_findings"] += [dict(snapshots[fid]) for fid in carried if fid not in published]
            harvest["finding_ids"] = [*(harvest.get("finding_ids") or []),
                                      *(fid for fid in carried if fid not in ids)]
        # A listed-only final gate classifies only its trusted reserved scope.
        from workbay_orchestrator_mcp.orchestration.review_cycle_dispatch import scope_harvest

        harvest = scope_harvest(Path(identity.get("worktree_path") or orchestrator_root), identity, harvest)
    receipt["harvest"] = harvest
    receipt["status"] = (
        "recorded"
        if (_harvest_block_parsed(harvest) or harvest.get("status") == "salvaged_unstructured")
        else "record_pending"
    )
    if receipt_path is not None:
        _atomic_write(receipt_path, json.dumps(receipt))
    return harvest


def _harvest_review_findings_once(
    result: dict[str, Any],
    report: dict[str, Any] | str | None,
    *,
    task_ref: str,
    lane_id: str | None,
    session: str,
    orchestrator_root: Path,
    salvage_unstructured: bool = True,
    reconcile_committed: bool = False,
    operation_token: str | None = None,
    review_context: Any = None,
    review_repo: Path | None = None,
) -> dict[str, Any]:
    """Scan terminal pass result for ``GROK_REVIEW_FINDINGS_JSON`` and record findings.

    Fail-open: never raises, never mutates pass outcome. Carrier order (first
    that yields ≥1 parseable finding wins): details → summary →
    raw_payload.details → details_artifact_ref rematerialization → report body
    (implementation note R1/R2). If every non-empty carrier is marker-free, preserve the
    first one's prose as one typed, bounded unstructured finding [OBS-08].
    """
    try:
        if not isinstance(result, dict):
            result = {}
        raw_payload = result.get("raw_payload")
        typed_dispatch_refusal = _is_typed_dispatch_refusal(raw_payload)

        # implementation note D9: harvest skips any result whose result_parse is not "ok",
        # except a schema-valid self-reported degraded document with findings.
        from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
            _persist_shortfall_block,
            review_result_is_harvestable,
            review_schema_document,
            schema_degraded_findings,
        )

        structured = schema_degraded_findings(result)
        if structured is not None:
            if review_context is not None:
                from workbay_orchestrator_mcp.orchestration.review_ratchet import apply, reopen_new_damage

                structured = apply(review_repo or Path(orchestrator_root), structured, review_context)
                reopen_new_damage(Path(orchestrator_root), structured, review_context, task_ref=task_ref)
            return _harvest_structured_degraded_findings(
                structured,
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                orchestrator_root=Path(orchestrator_root),
                reconcile_committed=reconcile_committed,
                operation_token=operation_token,
            )

        if not review_result_is_harvestable(result):
            if (
                isinstance(result, dict)
                and "findings" in result
                and result.get("result_parse") == "degraded"
                and review_schema_document(result) is None
            ):
                return {"status": "skipped", "reason": "schema_invalid"}
            return {"status": "skipped", "reason": "result_parse_degraded"}

        carriers: list[tuple[str, str | None]] = []
        details = result.get("details")
        carriers.append(("details", details if isinstance(details, str) else None))
        summary = result.get("summary")
        carriers.append(("summary", summary if isinstance(summary, str) else None))

        if isinstance(raw_payload, dict):
            raw_details = raw_payload.get("details")
            carriers.append(("raw_payload", raw_details if isinstance(raw_details, str) else None))
        else:
            carriers.append(("raw_payload", None))

        artifact_text: str | None = None
        artifact_ref = result.get("details_artifact_ref")
        if artifact_ref is not None:
            try:
                artifact_text = _rematerialize_details_artifact(int(artifact_ref), Path(orchestrator_root))
            except (TypeError, ValueError):
                artifact_text = None
        carriers.append(("artifact", artifact_text))
        carriers.append(("report", _report_scan_text(report)))

        findings_block_present = any(
            isinstance(text, str) and _MARKER_LINE_RE.search(text) is not None for _, text in carriers
        )
        text_refusal_present = any(
            carrier_name != "report" and text and text.strip() and _is_dispatch_refusal_carrier(text)
            for carrier_name, text in carriers
        )
        refusal_product_metadata = {}
        if findings_block_present and (typed_dispatch_refusal or text_refusal_present):
            refusal_product_metadata = {
                "salvage_suppressed": "none",
                "salvage_suppressed_by": "none",
                "salvage_note": "structured_findings_block_present; refusal did not suppress review product",
            }

        # A loaded result with blank review fields is an empty product, not
        # missing evidence. Only the absent-result sentinel implies source loss.
        if not result and not any(text and text.strip() for _, text in carriers):
            return {"status": "skipped", "reason": "harvest_source_missing"}

        saw_unparseable = False
        saw_no_valid = False
        last_dropped = 0
        last_resolution_metadata: dict[str, Any] = {}
        salvage_carrier: tuple[str, str] | None = None
        transport_carrier: str | None = None
        dispatch_refusal_carrier: str | None = None
        dispatch_refusal_by: str | None = None

        for carrier_name, text in carriers:
            if not text or not text.strip():
                continue
            if carrier_name != "report" and typed_dispatch_refusal and not findings_block_present:
                if dispatch_refusal_carrier is None:
                    dispatch_refusal_carrier = carrier_name
                    dispatch_refusal_by = "typed"
                continue
            if _is_transport_only_carrier(carrier_name, text, raw_payload):
                if transport_carrier is None:
                    transport_carrier = carrier_name
                continue
            if carrier_name != "report" and not findings_block_present and _is_dispatch_refusal_carrier(text):
                if dispatch_refusal_carrier is None:
                    dispatch_refusal_carrier = carrier_name
                    dispatch_refusal_by = "text"
                continue
            status, findings, dropped = _parse_findings_from_text(text)
            if status == "no_marker":
                if salvage_carrier is None:
                    salvage_carrier = (carrier_name, text.strip())
                continue
            if status == "unparseable_block":
                # Marker present but unparseable here — remember it and keep trying
                # later, more-authoritative carriers. A truncated top-level
                # ``details`` can hold a partial marker while ``raw_payload.details``
                # / the rematerialized artifact hold the full block; short-circuiting
                # here would drop harvestable findings (reported post-loop only if
                # no carrier parses).
                saw_unparseable = True
                continue
            if status == "no_valid_findings":
                saw_no_valid = True
                last_dropped = dropped
                last_resolution_metadata = _harvest_resolution_metadata(text)
                continue
            # status == "ok"
            assert findings is not None
            attribution_evidence = []
            if review_context is not None:
                from workbay_orchestrator_mcp.orchestration.review_ratchet import apply, reopen_new_damage

                findings = apply(review_repo or Path(orchestrator_root), findings, review_context,
                                 evidence_log=attribution_evidence)
                reopen_new_damage(Path(orchestrator_root), findings, review_context, task_ref=task_ref)
            findings = _link_prior_findings(findings, task_ref=task_ref, orchestrator_root=Path(orchestrator_root))
            finding_links = _harvest_finding_links(findings, lane_id)
            try:
                from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                    _record_findings,
                )

                persist = _record_findings(
                    findings,
                    task_ref=task_ref,
                    session=session,
                    lane_id=lane_id,
                    orchestrator_root=Path(orchestrator_root),
                    **({"review_context": review_context} if review_context is not None else {}),
                    **(
                        {"reconcile_committed": True, "operation_token": operation_token} if reconcile_committed else {}
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — harvest must never raise
                logger.warning(
                    "review findings persist failed with zero rows recorded (task_ref=%s session=%s lane_id=%s): %s",
                    task_ref,
                    session,
                    lane_id,
                    exc,
                )
                evidence = getattr(exc, "persist", {})
                return {
                    "status": "skipped",
                    "reason": "record_failed",
                    **{key: evidence[key] for key in ("legacy_reconciliation", "legacy_cause") if key in evidence},
                    **_persist_shortfall_block(evidence),
                }
            findings = _link_prior_findings(findings, task_ref=task_ref, orchestrator_root=Path(orchestrator_root))
            finding_links = _harvest_finding_links(findings, lane_id)
            shortfall = _persist_shortfall_block(persist)
            recorded_ids = shortfall["recorded_ids"]
            unrecorded_ids = shortfall["unrecorded_ids"]
            causes = shortfall["causes"]
            reconciliation = {
                "legacy_reconciliation": persist.get("legacy_reconciliation", "none"),
                "legacy_cause": persist.get("legacy_cause", causes.get("legacy_reconciliation")),
            }
            if unrecorded_ids:
                logger.warning(
                    "review findings persist shortfall "
                    "(task_ref=%s session=%s lane_id=%s recorded=%s unrecorded=%s): %s",
                    task_ref,
                    session,
                    lane_id,
                    recorded_ids,
                    unrecorded_ids,
                    causes,
                )
                return {
                    "status": "skipped",
                    "reason": "record_failed",
                    **reconciliation,
                    **shortfall,
                    "carrier": carrier_name,
                    "dropped": dropped,
                }
            return {
                "status": "recorded",
                "unrecorded_ids": [],
                **reconciliation,
                "causes": causes,
                "count": len(recorded_ids),
                "finding_ids": [
                    next((link["finding_id"] for link in finding_links if link["reported_finding_id"] == fid), fid)
                    for fid in recorded_ids
                ],
                "finding_links": finding_links,
                **({"attribution_evidence": attribution_evidence} if attribution_evidence else {}),
                "carrier": carrier_name,
                "dropped": dropped,
                **refusal_product_metadata,
                **_harvest_resolution_metadata(text),
            }

        if saw_unparseable:
            return {"status": "skipped", "reason": "unparseable_block"}
        if saw_no_valid:
            return {
                "status": "skipped",
                "reason": "no_valid_findings",
                "unrecorded_ids": [],
                "dropped": last_dropped,
                **last_resolution_metadata,
            }
        if salvage_unstructured and salvage_carrier is not None:
            carrier_name, salvage_text = salvage_carrier
            description_chars = _UNSTRUCTURED_SALVAGE_MAX_CHARS - len(_UNSTRUCTURED_SALVAGE_PREFIX)
            severity, marker_counts = _salvage_severity(salvage_text)
            severity_source = "explicit_marker" if any(marker_counts.values()) else "default_low"
            salvage_finding = {
                "severity": severity,
                "category": "harvest-salvage",
                "file_path": _UNSTRUCTURED_SALVAGE_FILE_PATH,
                "description": _UNSTRUCTURED_SALVAGE_PREFIX + salvage_text[:description_chars],
            }
            try:
                from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                    _record_findings,
                )

                persist = _record_findings(
                    [salvage_finding],
                    task_ref=task_ref,
                    session=session,
                    lane_id=lane_id,
                    orchestrator_root=Path(orchestrator_root),
                    **(
                        {"reconcile_committed": True, "operation_token": operation_token} if reconcile_committed else {}
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — harvest must never raise
                evidence = getattr(exc, "persist", {})
                return {
                    "status": "skipped",
                    "reason": "record_failed",
                    **{key: evidence[key] for key in ("legacy_reconciliation", "legacy_cause") if key in evidence},
                    **_persist_shortfall_block(evidence),
                }
            shortfall = _persist_shortfall_block(persist)
            recorded_ids = shortfall["recorded_ids"]
            unrecorded_ids = shortfall["unrecorded_ids"]
            causes = shortfall["causes"]
            reconciliation = {
                "legacy_reconciliation": persist.get("legacy_reconciliation", "none"),
                "legacy_cause": persist.get("legacy_cause", causes.get("legacy_reconciliation")),
            }
            if unrecorded_ids:
                logger.warning(
                    "review findings persist shortfall "
                    "(task_ref=%s session=%s lane_id=%s recorded=%s unrecorded=%s): %s",
                    task_ref,
                    session,
                    lane_id,
                    recorded_ids,
                    unrecorded_ids,
                    causes,
                )
                return {
                    "status": "skipped",
                    "reason": "record_failed",
                    **reconciliation,
                    **shortfall,
                    "carrier": carrier_name,
                }
            return {
                "status": "salvaged_unstructured",
                **reconciliation,
                "causes": causes,
                "reason": "no_findings_block",
                "salvaged_chars": len(salvage_text),
                "carrier": carrier_name,
                "count": len(recorded_ids),
                "finding_ids": recorded_ids,
                "severity_source": severity_source,
                "severity_marker_counts": marker_counts,
            }
        if salvage_unstructured and transport_carrier is not None:
            return {
                "status": "skipped",
                "reason": "no_findings_block",
                "carrier": transport_carrier,
                "salvage_suppressed": "transport_stderr",
            }
        if salvage_unstructured and dispatch_refusal_carrier is not None:
            return {
                "status": "skipped",
                "reason": "no_review_turn",
                "carrier": dispatch_refusal_carrier,
                "salvage_suppressed": "dispatch_refusal",
                "salvage_suppressed_by": dispatch_refusal_by,
            }
        return {"status": "skipped", "reason": "no_findings_block"}
    except Exception:  # noqa: BLE001 — ultimate fail-open for the pass
        return {"status": "skipped", "reason": "record_failed"}


# implementation note R9: a grok pass can die pre-work on a backend-INTERNAL fault (e.g.
# ``max_tokens_truncation``, a provider rate-limit / overload / 5xx). The engine's
# no-auto-retry policy is correct, but a bare ``error`` outcome forces the
# coordinator into log forensics before it can re-dispatch. Classify these as a
# ``backend_transient`` discriminator on the error payload so re-dispatch is a
# mechanical decision (the outcome stays ``error`` — no new enum value).
_BACKEND_TRANSIENT_PATTERNS = re.compile(
    r"(?i)(max_tokens_truncation|max[-_ ]?tokens|truncat|rate[-_ ]?limit|\b429\b|overloaded"
    r"|temporarily unavailable|\b50[023]\b|internal server error|connection reset|connection error"
    r"|service unavailable|upstream (?:error|timeout))"
)


def _is_backend_transient_error(text: str | None) -> bool:
    """implementation note R9: True when an ``error`` outcome's text names a backend-internal
    transient fault safe to mechanically re-dispatch (not a code/worker fault)."""
    if not text:
        return False
    return _BACKEND_TRANSIENT_PATTERNS.search(str(text)) is not None


# implementation note R7: the engine spawns these scripts as subprocesses from the imported
# module's own dir (``SCRIPT_DIR = Path(__file__).parent`` in worker_daemon /
# lane_exec). If a concurrent env flip deletes that source after import, the spawn
# fails as a bare ``lane_prompt.py --check failed (exit 2)`` crash mid-pass.
_ENGINE_CRITICAL_SCRIPTS: tuple[str, ...] = ("offload_pass.py", "lane_prompt.py", "worker_daemon.py")


def _engine_source_integrity_note(engine_dir: Path | None = None) -> str | None:
    """implementation note R7: verify the engine's own on-disk source still exists.

    Returns a remedy string naming ``server_stale_restart_required`` when a
    critical engine script has vanished from the module dir since import (the
    concurrent-env-flip incident), else ``None``. ``engine_dir`` defaults to this
    module's own directory; it is a parameter only so the check is unit-testable.
    Existence is a distinct signal from version/commit skew (already surfaced at
    startup by handoff ``package_skew.emit_src_installed_skew_startup_log``), so
    this is not a forked fingerprint mechanism ([REF-19]).
    """
    resolved = engine_dir if engine_dir is not None else Path(__file__).resolve().parent
    missing = [name for name in _ENGINE_CRITICAL_SCRIPTS if not (resolved / name).exists()]
    if not missing:
        return None
    return (
        "server_stale_restart_required: the orchestrator engine's on-disk source no "
        f"longer exists ({', '.join(missing)} missing under {resolved}); a concurrent "
        "environment flip (e.g. a dev-redirect .pth removing the installed package) "
        "invalidated the running server. Restart the MCP orchestrator server before "
        "dispatching further offload passes."
    )


# implementation note R5: close-time package-smoke wall-clock cap (per touched package).
# A slice whose full-package suite fits under the cap is smoked at closure; a
# suite that overruns degrades to a typed skip (never an unbounded run).
_PACKAGE_SMOKE_WALL_CLOCK_CAP_SECONDS = 300


def _touched_packages(changed_files: list[str] | None, worktree_path: Path) -> dict[str, Path]:
    """Map a slice's ``changed_files`` to the ``packages/<name>/tests`` dirs it
    touched (only packages that actually ship a ``tests`` dir). Sorted by name
    for determinism. implementation note R5."""
    packages: dict[str, Path] = {}
    for entry in changed_files or []:
        parts = str(entry).split("/")
        if len(parts) >= 2 and parts[0] == "packages":
            name = parts[1]
            tests_dir = worktree_path / "packages" / name / "tests"
            if tests_dir.is_dir():
                packages[name] = tests_dir
    return dict(sorted(packages.items()))


def _package_smoke_pythonpath(worktree_path: Path) -> str:
    """Prefer source trees from the checkout being smoked over editable installs."""
    root = Path(worktree_path).resolve()
    source_roots = sorted(path for path in (root / "packages").glob("*/src") if path.is_dir())
    source_roots.append(root / "packages" / "workbay-system")
    inherited = os.environ.get("PYTHONPATH")
    entries = [str(path) for path in source_roots]
    if inherited:
        entries.append(inherited)
    return os.pathsep.join(entries)


def _package_smoke_evidence(proc: subprocess.CompletedProcess[str]) -> str:
    """Keep both output streams and the exit status in a compact smoke note."""
    stdout = "\n".join((proc.stdout or "").strip().splitlines()[-15:])
    stderr = "\n".join((proc.stderr or "").strip().splitlines()[-15:])
    prefix = f"rc={proc.returncode}\nstdout tail:\n"
    stderr_label = "\nstderr tail:\n"
    budget = max(0, 1900 - len(prefix) - len(stderr_label))
    stdout_budget = budget // 2
    stderr_budget = budget - stdout_budget
    if len(stdout) > stdout_budget:
        stdout = stdout[-stdout_budget:] if stdout_budget else ""
    else:
        stderr_budget += stdout_budget - len(stdout)
    if len(stderr) > stderr_budget:
        stderr = stderr[-stderr_budget:] if stderr_budget else ""
    return f"{prefix}{stdout}{stderr_label}{stderr}"


def _package_smoke_failed_node_ids(*streams: str | None) -> list[str]:
    """Parse pytest's ``-rfE`` summary node ids, preserving first-seen order."""
    failed: list[str] = []
    for stream in streams:
        for line in (stream or "").splitlines():
            match = re.match(r"^(?:FAILED|ERROR)\s+(.+)$", line.strip())
            if not match:
                continue
            summary = match.group(1)
            node_id = summary.rsplit(" - ", maxsplit=1)[0] if " - " in summary else summary
            node_id = node_id.strip()
            if node_id and node_id not in failed:
                failed.append(node_id)
    return failed


def _package_smoke_path_guard_fault(*streams: str | None) -> bool:
    marker = re.compile(
        r"ERROR: [A-Za-z_][A-Za-z0-9_.]* loaded from .+, but cwd is .+\. "
        r"Run uv sync --extra dev in this worktree's package directory and retry\."
    )
    return any(marker.fullmatch(line.strip()) for stream in streams for line in (stream or "").splitlines())


def _package_smoke_pytest_unavailable(*streams: str | None) -> bool:
    """Recognize Python's missing-pytest startup error as an environment fault."""
    pytest_started = re.compile(
        r"^(?:={3,}\s*test session starts\s*={3,}|platform\s+\S+|rootdir:|collected\s+\d+\s+items?\b)"
    )
    if any(pytest_started.match(line.strip()) for stream in streams for line in (stream or "").splitlines()):
        return False
    if _package_smoke_failed_node_ids(*streams):
        return False

    # Python prints this diagnostic before pytest can emit a session header.
    # Only interpreter-like prefixes count; arbitrary words can be exception
    # class names emitted by a plugin or wrapper. The bare form keeps
    # compatibility with existing runners that omit the interpreter prefix.
    missing_pytest = re.compile(r"^(?P<program>(?:[A-Za-z]:)?[^:\s]+): No module named pytest$")
    python_basename = re.compile(r"^python(?:[0-9]+(?:\.[0-9]+)*)?$")
    current_interpreter_basename = sys.executable.replace("\\", "/").rsplit("/", maxsplit=1)[-1]
    if current_interpreter_basename.lower().endswith(".exe"):
        current_interpreter_basename = current_interpreter_basename[:-4]
    bare_missing_pytest = re.compile(r"^No module named pytest$")

    def is_interpreter_prefix(program: str) -> bool:
        if program.endswith(("Error", "Exception")):
            return False

        basename = program.rsplit("/", maxsplit=1)[-1].rsplit("\\", maxsplit=1)[-1]
        unqualified_program = program == basename
        if basename.lower().endswith(".exe"):
            basename = basename[:-4]
        python_name = python_basename.fullmatch(basename) is not None
        if not python_name and basename != current_interpreter_basename:
            return False

        # An unqualified non-Python token is not an interpreter path.
        return "/" in program or "\\" in program or (unqualified_program and python_name)

    return any(
        (
            (match := missing_pytest.fullmatch(line.strip())) is not None
            and is_interpreter_prefix(match.group("program"))
        )
        or bare_missing_pytest.fullmatch(line.strip())
        for stream in streams
        for line in (stream or "").splitlines()
    )


class _PackageSmokeReason(StrEnum):
    """Typed outcome of comparing a failed lane smoke with its base run."""

    PASS = "pass"
    ENV_FAULT = "env_fault"
    UNRUNNABLE_REGRESSION = "unrunnable_regression"
    UNRECOGNIZED_UNRUNNABLE = "unrecognized_unrunnable"
    NEW_FAILURES = "new_failures"
    PREEXISTING_FAILURES = "preexisting_failures"
    SUITE_FAILURE = "suite_failure"
    BASELINE_UNAVAILABLE = "baseline_unavailable"


def _classify_package_smoke(
    lane_returncode: int,
    lane_path_guard: bool,
    baseline_returncode: int | None,
    baseline_path_guard: bool,
    has_new_failures: bool | None,
    lane_pytest_unavailable: bool = False,
    baseline_pytest_unavailable: bool = False,
) -> _PackageSmokeReason:
    """Classify package smoke with one ordered, pure decision table.

    A normal pytest failure (rc=1) is always judged by its failing node ids,
    regardless of marker-like output. A path-guard or missing-pytest startup
    fault is shared only when the base has the same recognized diagnostic.
    """
    if lane_returncode == 0:
        return _PackageSmokeReason.PASS
    if lane_returncode == 1:
        if has_new_failures is True:
            return _PackageSmokeReason.NEW_FAILURES
        if has_new_failures is False:
            return _PackageSmokeReason.PREEXISTING_FAILURES
        return _PackageSmokeReason.SUITE_FAILURE
    if lane_path_guard:
        if baseline_returncode is None:
            return _PackageSmokeReason.BASELINE_UNAVAILABLE
        if baseline_path_guard and baseline_returncode != 0:
            return _PackageSmokeReason.ENV_FAULT
        if baseline_returncode in (0, 1, 5):
            return _PackageSmokeReason.UNRUNNABLE_REGRESSION
        if baseline_returncode in (3, 4):
            return _PackageSmokeReason.UNRECOGNIZED_UNRUNNABLE
        return _PackageSmokeReason.BASELINE_UNAVAILABLE
    if lane_returncode in (3, 4):
        if baseline_returncode is None:
            return _PackageSmokeReason.BASELINE_UNAVAILABLE
        if lane_pytest_unavailable and baseline_pytest_unavailable:
            return _PackageSmokeReason.ENV_FAULT
        if baseline_returncode in (0, 1, 5):
            return _PackageSmokeReason.UNRUNNABLE_REGRESSION
        if baseline_returncode in (3, 4):
            return _PackageSmokeReason.UNRECOGNIZED_UNRUNNABLE
        return _PackageSmokeReason.BASELINE_UNAVAILABLE
    return _PackageSmokeReason.SUITE_FAILURE


class _PackageSmokeBaseline(NamedTuple):
    base_sha: str
    rootdir_relative: Path
    tests_relative: Path | None


def _package_smoke_resolve_baseline(
    worktree_path: Path,
    lane_result: subprocess.CompletedProcess[str],
    *,
    tests_dir: Path,
    base_sha: str | None,
    cap_seconds: int,
    infer_rootdir: bool,
    map_tests_dir: bool,
) -> tuple[_PackageSmokeBaseline | None, str | None]:
    """Resolve rootdir and merge base once for either baseline comparison."""
    rootdir_relative, rootdir_reason = _package_smoke_rootdir_relative(
        worktree_path,
        lane_result.stdout,
        lane_result.stderr,
        tests_dir=tests_dir if infer_rootdir else None,
    )
    if rootdir_reason:
        return None, rootdir_reason
    assert rootdir_relative is not None
    tests_relative = None
    if map_tests_dir:
        try:
            tests_relative = tests_dir.resolve().relative_to(Path(worktree_path).resolve())
        except (OSError, RuntimeError, ValueError) as exc:
            return None, f"package test path could not be mapped to the base worktree ({exc})"

    resolved_base = base_sha
    if not resolved_base:
        try:
            base_proc = subprocess.run(
                ["git", "merge-base", "HEAD", "main"],
                cwd=str(worktree_path),
                capture_output=True,
                text=True,
                timeout=cap_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return None, "git merge-base timed out"
        except OSError as exc:
            return None, f"git merge-base could not run ({exc})"
        if base_proc.returncode == 0 and base_proc.stdout.strip():
            resolved_base = base_proc.stdout.strip().splitlines()[0]
        else:
            return None, f"git merge-base failed; {_package_smoke_evidence(base_proc)}"
    return _PackageSmokeBaseline(resolved_base, rootdir_relative, tests_relative), None


def _run_package_smoke_suite(
    worktree_path: Path,
    tests_dir: Path,
    *,
    python_bin: str,
    cap_seconds: int,
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    """Run one package's full test directory, reporting bounded runner faults."""
    try:
        result = subprocess.run(
            [
                python_bin,
                "-m",
                "pytest",
                str(tests_dir),
                "-q",
                "-v",
                "-p",
                "no:cacheprovider",
                "-rfE",
            ],
            cwd=str(worktree_path),
            env={**os.environ, "PYTHONPATH": _package_smoke_pythonpath(worktree_path)},
            capture_output=True,
            text=True,
            timeout=cap_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError:
        return None, "unavailable"
    return result, None


def _package_smoke_classification_note(
    reason: _PackageSmokeReason,
    name: str,
    lane_result: subprocess.CompletedProcess[str],
    baseline: subprocess.CompletedProcess[str],
    *,
    evidence: str,
    resolved_base: str,
    failed_node_ids: list[str] | None = None,
    new_failures: list[str] | None = None,
    unrunnable_reason: str | None = None,
) -> str:
    """Build user-facing evidence for a completed lane/base comparison."""
    lane_error = _package_smoke_first_error_line(lane_result)
    baseline_error = _package_smoke_first_error_line(baseline)
    unrunnable_detail = f"unrunnable_reason: {unrunnable_reason}; " if unrunnable_reason else ""
    if reason is _PackageSmokeReason.ENV_FAULT:
        return (
            f"smoke_env_fault: {unrunnable_detail}package {name!r} suite was unrunnable at both lane and base "
            f"(lane_rc={lane_result.returncode}; lane_error={lane_error!r}; "
            f"baseline_rc={baseline.returncode}; baseline_error={baseline_error!r}); {evidence}"
        )
    if reason is _PackageSmokeReason.UNRUNNABLE_REGRESSION:
        return (
            f"smoke_unrunnable_regression: {unrunnable_detail}package {name!r} suite cannot run on lane but did on base "
            f"(lane_rc={lane_result.returncode}; lane_error={lane_error!r}; "
            f"baseline_rc={baseline.returncode}; baseline_error={baseline_error!r}); {evidence}"
        )
    if reason is _PackageSmokeReason.UNRECOGNIZED_UNRUNNABLE:
        return (
            f"smoke_unrecognized_unrunnable: package {name!r} suite returned startup failures without a matching "
            f"recognized environment diagnostic (lane_rc={lane_result.returncode}; lane_error={lane_error!r}; "
            f"baseline_rc={baseline.returncode}; baseline_error={baseline_error!r}); "
            f"lane evidence:\n{evidence}\nbaseline evidence:\n{_package_smoke_evidence(baseline)}"
        )
    if reason is _PackageSmokeReason.NEW_FAILURES:
        assert new_failures
        return f"package_smoke_failed: package {name!r} new failing ids: {', '.join(new_failures)}\n{evidence}"
    if reason is _PackageSmokeReason.PREEXISTING_FAILURES:
        assert failed_node_ids is not None
        return (
            f"smoke_preexisting_failures: {len(failed_node_ids)} ids also red at "
            f"{resolved_base[:12]}: {', '.join(failed_node_ids)}\n{evidence}"
        )
    return (
        f"package_smoke_failed: package {name!r} full test suite failed at slice close "
        f"(a slice must not break its own package; green scoped-suite is not enough); {evidence}"
    )


def _package_smoke_first_error_line(proc: subprocess.CompletedProcess[str]) -> str:
    """Return a compact, useful error line from pytest output."""
    lines = [
        line.strip() for stream in (proc.stderr, proc.stdout) for line in (stream or "").splitlines() if line.strip()
    ]
    for line in lines:
        if re.search(r"error|exception|traceback|failed|usage", line, re.IGNORECASE):
            return line[:240]
    return "<no error line>"


def _package_smoke_rootdir_relative(
    worktree_path: Path,
    *streams: str | None,
    tests_dir: Path | None = None,
) -> tuple[Path | None, str | None]:
    """Resolve or infer pytest's rootdir relative to the lane worktree."""
    output = "\n".join(stream or "" for stream in streams)
    match = re.search(r"(?m)^\s*rootdir:\s*(.+?)\s*$", output)
    if not match:
        if tests_dir is None:
            return None, "baseline_unavailable: pytest rootdir was missing from lane output"
        try:
            resolved_worktree = Path(worktree_path).resolve()
            resolved_tests = Path(tests_dir).resolve()
            resolved_tests.relative_to(resolved_worktree)
        except (OSError, RuntimeError, ValueError) as exc:
            return None, f"baseline_unavailable: pytest rootdir could not be inferred ({exc})"
        config_sections = {
            "tox.ini": r"(?m)^\s*\[pytest\]\s*$",
            "setup.cfg": r"(?m)^\s*\[tool:pytest\]\s*$",
            "pyproject.toml": r"(?m)^\s*\[tool\.pytest(?:\.ini_options)?\]\s*$",
        }
        for directory in (resolved_tests, *resolved_tests.parents):
            try:
                relative = directory.relative_to(resolved_worktree)
            except ValueError:
                break
            for filename, section_pattern in config_sections.items():
                config_path = directory / filename
                try:
                    content = config_path.read_text(encoding="utf-8")
                except OSError:
                    continue
                if re.search(section_pattern, content):
                    return relative, None
            if any((directory / filename).is_file() for filename in ("pytest.ini", ".pytest.ini")):
                return relative, None
        # With no discovered config, the lane invocation's cwd is the
        # worktree root, which is pytest's common-ancestor rootdir.
        return Path("."), None

    reported_rootdir = Path(match.group(1))
    if not reported_rootdir.is_absolute():
        reported_rootdir = Path(worktree_path) / reported_rootdir
    try:
        resolved_rootdir = reported_rootdir.resolve()
        resolved_worktree = Path(worktree_path).resolve()
    except (OSError, RuntimeError) as exc:
        return None, f"baseline_unavailable: pytest rootdir could not be resolved ({exc})"
    try:
        return resolved_rootdir.relative_to(resolved_worktree), None
    except ValueError:
        return (
            None,
            f"baseline_unavailable: pytest rootdir is outside the lane worktree ({resolved_rootdir})",
        )


def _package_smoke_unrunnable_baseline(
    worktree_path: Path,
    *,
    base_sha: str,
    rootdir_relative: Path,
    tests_relative: Path,
    python_bin: str,
    cap_seconds: int,
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    """Run the same full package smoke at base to classify lane rc 3 or 4."""
    base_path: Path | None = None
    baseline: subprocess.CompletedProcess[str] | None = None
    failure_reason: str | None = None
    cleanup_reason: str | None = None
    try:
        base_path = Path(tempfile.mkdtemp(prefix="workbay-package-smoke-unrunnable-base-"))
        try:
            add = subprocess.run(
                ["git", "worktree", "add", "--detach", str(base_path), base_sha],
                cwd=str(worktree_path),
                capture_output=True,
                text=True,
                timeout=cap_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            failure_reason = "git worktree add timed out"
        except OSError as exc:
            failure_reason = f"git worktree add could not run ({exc})"
        else:
            if add.returncode != 0:
                failure_reason = f"git worktree add failed; {_package_smoke_evidence(add)}"
            else:
                baseline_cwd = base_path / rootdir_relative
                baseline_tests = base_path / tests_relative
                if not baseline_cwd.is_dir():
                    failure_reason = (
                        "baseline_unavailable: pytest rootdir "
                        f"{rootdir_relative.as_posix() or '.'!r} is absent from base worktree"
                    )
                elif not baseline_tests.is_dir():
                    failure_reason = (
                        "baseline_unavailable: package tests directory "
                        f"{tests_relative.as_posix()!r} is absent from base worktree"
                    )
                else:
                    try:
                        baseline = subprocess.run(
                            [
                                python_bin,
                                "-m",
                                "pytest",
                                str(baseline_tests),
                                "-q",
                                "-v",
                                "-p",
                                "no:cacheprovider",
                                "-rfE",
                            ],
                            cwd=str(baseline_cwd),
                            env={**os.environ, "PYTHONPATH": _package_smoke_pythonpath(base_path)},
                            capture_output=True,
                            text=True,
                            timeout=cap_seconds,
                            check=False,
                        )
                    except subprocess.TimeoutExpired:
                        failure_reason = f"base pytest smoke exceeded the {cap_seconds}s cap"
                    except OSError as exc:
                        failure_reason = f"base pytest smoke could not run ({exc})"
    except OSError as exc:
        failure_reason = f"temporary base worktree could not be created ({exc})"
    finally:
        if base_path is not None:
            try:
                removed = subprocess.run(
                    ["git", "worktree", "remove", "--force", str(base_path)],
                    cwd=str(worktree_path),
                    capture_output=True,
                    text=True,
                    timeout=cap_seconds,
                    check=False,
                )
                if removed.returncode != 0:
                    cleanup_reason = f"git worktree remove failed; {_package_smoke_evidence(removed)}"
            except (OSError, subprocess.TimeoutExpired) as exc:
                cleanup_reason = f"git worktree remove could not complete ({exc})"
            finally:
                shutil.rmtree(base_path, ignore_errors=True)
    if cleanup_reason:
        failure_reason = f"{failure_reason}; {cleanup_reason}" if failure_reason else cleanup_reason
    return baseline, failure_reason


def _package_smoke_base_failures(
    worktree_path: Path,
    failed_node_ids: list[str],
    *,
    base_sha: str,
    rootdir_relative: Path,
    python_bin: str,
    cap_seconds: int,
) -> tuple[list[str] | None, str | None]:
    """Run only slice-failing node ids at base; return red ids or a fail-closed reason."""
    base_path: Path | None = None
    base_failures: list[str] | None = None
    failure_reason: str | None = None
    cleanup_reason: str | None = None
    try:
        base_path = Path(tempfile.mkdtemp(prefix="workbay-package-smoke-base-"))
        try:
            add = subprocess.run(
                ["git", "worktree", "add", "--detach", str(base_path), base_sha],
                cwd=str(worktree_path),
                capture_output=True,
                text=True,
                timeout=cap_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            failure_reason = "git worktree add timed out"
        except OSError as exc:
            failure_reason = f"git worktree add could not run ({exc})"
        else:
            if add.returncode != 0:
                add_evidence = _package_smoke_evidence(add)
                failure_reason = f"git worktree add failed; {add_evidence}"
            else:
                baseline_cwd = base_path / rootdir_relative
                if not baseline_cwd.is_dir():
                    rootdir_label = rootdir_relative.as_posix() or "."
                    failure_reason = (
                        f"baseline_unavailable: pytest rootdir {rootdir_label!r} is absent from base worktree"
                    )
                else:
                    try:
                        baseline = subprocess.run(
                            [
                                python_bin,
                                "-m",
                                "pytest",
                                *failed_node_ids,
                                "-q",
                                "-p",
                                "no:cacheprovider",
                                "-rfE",
                            ],
                            cwd=str(baseline_cwd),
                            env={**os.environ, "PYTHONPATH": _package_smoke_pythonpath(base_path)},
                            capture_output=True,
                            text=True,
                            timeout=cap_seconds,
                            check=False,
                        )
                    except subprocess.TimeoutExpired:
                        failure_reason = f"base pytest rerun exceeded the {cap_seconds}s cap"
                    except OSError as exc:
                        failure_reason = f"base pytest rerun could not run ({exc})"
                    else:
                        if baseline.returncode == 1:
                            identified = _package_smoke_failed_node_ids(baseline.stdout, baseline.stderr)
                            if identified:
                                failed_set = set(failed_node_ids)
                                base_failures = [node_id for node_id in identified if node_id in failed_set]
                            else:
                                failure_reason = (
                                    "base pytest rerun failed without identifiable node ids; "
                                    f"{_package_smoke_evidence(baseline)}"
                                )
                        elif baseline.returncode in (3, 4) or _package_smoke_path_guard_fault(
                            baseline.stdout, baseline.stderr
                        ):
                            fault = (
                                "pytest usage error (rc=4)"
                                if baseline.returncode == 4
                                else "pytest internal error (rc=3)"
                                if baseline.returncode == 3
                                else "pytest path-guard refusal"
                            )
                            failure_reason = (
                                f"baseline_env_fault: base pytest rerun had a {fault}; "
                                f"{_package_smoke_evidence(baseline)}"
                            )
                        elif baseline.returncode == 0:
                            base_failures = []
                        else:
                            failure_reason = (
                                "baseline_unavailable: base pytest rerun returned "
                                f"rc={baseline.returncode}; {_package_smoke_evidence(baseline)}"
                            )
    except OSError as exc:
        failure_reason = f"temporary base worktree could not be created ({exc})"
    finally:
        if base_path is not None:
            try:
                removed = subprocess.run(
                    ["git", "worktree", "remove", "--force", str(base_path)],
                    cwd=str(worktree_path),
                    capture_output=True,
                    text=True,
                    timeout=cap_seconds,
                    check=False,
                )
                if removed.returncode != 0:
                    cleanup_reason = f"git worktree remove failed; {_package_smoke_evidence(removed)}"
            except (OSError, subprocess.TimeoutExpired) as exc:
                cleanup_reason = f"git worktree remove could not complete ({exc})"
            finally:
                shutil.rmtree(base_path, ignore_errors=True)
    if cleanup_reason:
        failure_reason = f"{failure_reason}; {cleanup_reason}" if failure_reason else cleanup_reason
    if failure_reason:
        return None, failure_reason
    return base_failures, None


def _package_smoke(
    worktree_path: Path,
    changed_files: list[str] | None,
    *,
    cap_seconds: int = _PACKAGE_SMOKE_WALL_CLOCK_CAP_SECONDS,
    python_bin: str | None = None,
    base_sha: str | None = None,
) -> tuple[bool, str | None]:
    """Close-time package smoke; compare failures with the merge-base suite.

    A path-guard startup fault is non-blocking only if the same full smoke at
    base also emits the exact path-guard marker. Normal rc=1 failures always
    use the failed-node comparison, regardless of marker-like output.
    """
    packages = _touched_packages(changed_files, Path(worktree_path))
    if not packages:
        return True, None
    if python_bin is None:
        py = os.path.abspath(Path(worktree_path) / ".venv" / "bin" / "python")
    else:
        py = python_bin
    if python_bin is not None and not os.path.isabs(py) and (os.sep in py or (os.altsep and os.altsep in py)):
        py = os.path.abspath(Path(worktree_path) / py)
    # Only confirmed metadata drift licenses skipping smoke. An unavailable
    # probe leaves the existing suite behavior intact.
    try:
        drift_probe = subprocess.run(
            [py, "scripts/version_of_drift.py", "--print-drifted-names"],
            cwd=str(worktree_path),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    else:
        if drift_probe.returncode == 0:
            drifted_names = (drift_probe.stdout or "").split()
            if drifted_names:
                return True, (
                    f"smoke_env_drift: {', '.join(drifted_names)}; smoke was skipped because "
                    "the interpreter environment, not the slice, is stale; "
                    "run make sync-version-drift-venv"
                )
    # internal D4: serialize the close-time smoke behind the global suite
    # lock — the worktree's git-common-dir resolves to the shared lock, so a
    # smoke run and a concurrent lane's self-verify never spike memory together.
    # A lock timeout degrades non-blocking (the scoped self-verify already
    # stands); the bulkhead is a no-op when disabled.
    from workbay_orchestrator_mcp.orchestration.host_resources import (
        SuiteLockTimeout,
        acquire_suite_bulkhead,
    )

    try:
        suite_fd = acquire_suite_bulkhead(Path(worktree_path))
    except SuiteLockTimeout as exc:
        return True, f"smoke_skipped_suite_lock_timeout: {exc}; the scoped self-verify stands"
    notes: list[str] = []
    try:
        for name, tests_dir in packages.items():
            proc, runner_fault = _run_package_smoke_suite(
                Path(worktree_path),
                tests_dir,
                python_bin=py,
                cap_seconds=cap_seconds,
            )
            if runner_fault == "timeout":
                return True, (
                    f"smoke_skipped_too_slow: package {name!r} suite exceeded the "
                    f"{cap_seconds}s close-time cap; the scoped self-verify stands"
                )
            if runner_fault == "unavailable":
                return True, f"smoke_skipped: package {name!r} suite could not run ({py} unavailable)"
            assert proc is not None
            evidence = _package_smoke_evidence(proc)
            if proc.returncode == 0:
                continue

            lane_path_guard = _package_smoke_path_guard_fault(proc.stdout, proc.stderr)
            lane_pytest_unavailable = _package_smoke_pytest_unavailable(proc.stdout, proc.stderr)
            if proc.returncode == 1 and not lane_pytest_unavailable:
                failed_node_ids = _package_smoke_failed_node_ids(proc.stdout, proc.stderr)
                if not failed_node_ids:
                    return False, (
                        f"package_smoke_failed: package {name!r} suite failed with rc=1 but pytest "
                        f"reported no failing node ids; {evidence}"
                    )
                baseline_info, baseline_reason = _package_smoke_resolve_baseline(
                    Path(worktree_path),
                    proc,
                    tests_dir=tests_dir,
                    base_sha=base_sha,
                    cap_seconds=cap_seconds,
                    infer_rootdir=False,
                    map_tests_dir=False,
                )
                if baseline_reason or baseline_info is None:
                    return False, (
                        f"package_smoke_failed: package {name!r} has failing ids but the "
                        f"baseline check could not run ({baseline_reason or 'baseline unavailable'}); {evidence}"
                    )
                base_failures, base_reason = _package_smoke_base_failures(
                    Path(worktree_path),
                    failed_node_ids,
                    base_sha=baseline_info.base_sha,
                    rootdir_relative=baseline_info.rootdir_relative,
                    python_bin=py,
                    cap_seconds=cap_seconds,
                )
                if base_reason:
                    return False, (
                        f"package_smoke_failed: package {name!r} has failing ids but the "
                        f"baseline check could not run ({base_reason}); {evidence}"
                    )
                assert base_failures is not None
                new_failures = [node_id for node_id in failed_node_ids if node_id not in base_failures]
                reason = _classify_package_smoke(
                    proc.returncode,
                    lane_path_guard,
                    1 if base_failures else 0,
                    False,
                    bool(new_failures),
                )
                note = _package_smoke_classification_note(
                    reason,
                    name,
                    proc,
                    subprocess.CompletedProcess([], 1 if base_failures else 0, stdout="", stderr=""),
                    evidence=evidence,
                    resolved_base=baseline_info.base_sha,
                    failed_node_ids=failed_node_ids,
                    new_failures=new_failures,
                )
                if reason is _PackageSmokeReason.NEW_FAILURES:
                    return False, note
                if reason is _PackageSmokeReason.PREEXISTING_FAILURES:
                    notes.append(note)
                    continue
                return False, note

            if lane_path_guard or proc.returncode in (3, 4) or lane_pytest_unavailable:
                lane_error = _package_smoke_first_error_line(proc)
                lane_unrunnable_reason = "interpreter_missing_pytest" if lane_pytest_unavailable else None
                unrunnable_detail = f"unrunnable_reason: {lane_unrunnable_reason}; " if lane_unrunnable_reason else ""
                baseline_info, baseline_reason = _package_smoke_resolve_baseline(
                    Path(worktree_path),
                    proc,
                    tests_dir=tests_dir,
                    base_sha=base_sha,
                    cap_seconds=cap_seconds,
                    infer_rootdir=True,
                    map_tests_dir=True,
                )
                if baseline_reason or baseline_info is None:
                    return False, (
                        f"smoke_baseline_unavailable: {unrunnable_detail}package {name!r} suite was unrunnable at lane "
                        f"(lane_rc={proc.returncode}; lane_error={lane_error!r}); "
                        f"{baseline_reason or 'baseline unavailable'}; {evidence}"
                    )
                assert baseline_info.tests_relative is not None
                baseline, baseline_reason = _package_smoke_unrunnable_baseline(
                    Path(worktree_path),
                    base_sha=baseline_info.base_sha,
                    rootdir_relative=baseline_info.rootdir_relative,
                    tests_relative=baseline_info.tests_relative,
                    python_bin=py,
                    cap_seconds=cap_seconds,
                )
                if baseline_reason or baseline is None:
                    return False, (
                        f"smoke_baseline_unavailable: {unrunnable_detail}package {name!r} suite was unrunnable at lane "
                        f"(lane_rc={proc.returncode}; lane_error={lane_error!r}); "
                        f"baseline_rc=unavailable; baseline_error={baseline_reason or 'no baseline result'}; "
                        f"{evidence}"
                    )
                baseline_path_guard = _package_smoke_path_guard_fault(baseline.stdout, baseline.stderr)
                baseline_pytest_unavailable = _package_smoke_pytest_unavailable(
                    baseline.stdout,
                    baseline.stderr,
                )
                reason = _classify_package_smoke(
                    3 if lane_pytest_unavailable else proc.returncode,
                    lane_path_guard,
                    3 if baseline_pytest_unavailable else baseline.returncode,
                    baseline_path_guard,
                    None,
                    lane_pytest_unavailable=lane_pytest_unavailable,
                    baseline_pytest_unavailable=baseline_pytest_unavailable,
                )
                if reason is _PackageSmokeReason.ENV_FAULT:
                    notes.append(
                        _package_smoke_classification_note(
                            reason,
                            name,
                            proc,
                            baseline,
                            evidence=evidence,
                            resolved_base=baseline_info.base_sha,
                            unrunnable_reason=lane_unrunnable_reason,
                        )
                    )
                    continue
                if reason in (
                    _PackageSmokeReason.UNRUNNABLE_REGRESSION,
                    _PackageSmokeReason.UNRECOGNIZED_UNRUNNABLE,
                ):
                    return False, _package_smoke_classification_note(
                        reason,
                        name,
                        proc,
                        baseline,
                        evidence=evidence,
                        resolved_base=baseline_info.base_sha,
                        unrunnable_reason=lane_unrunnable_reason,
                    )
                baseline_error = _package_smoke_first_error_line(baseline)
                return False, (
                    f"smoke_baseline_unavailable: package {name!r} base smoke did not complete "
                    f"(lane_rc={proc.returncode}; lane_error={lane_error!r}; "
                    f"baseline_rc={baseline.returncode}; baseline_error={baseline_error!r}); {evidence}"
                )

            return False, (
                f"package_smoke_failed: package {name!r} full test suite failed at slice close "
                f"(a slice must not break its own package; green scoped-suite is not enough); {evidence}"
            )
        return True, "\n".join(notes) if notes else None
    finally:
        if suite_fd is not None:
            os.close(suite_fd)


def _verified_lane_gate_event_id(
    *,
    task_ref: str,
    lane_id: str,
    pass_id: str,
    tip: str,
    command: str,
) -> str:
    """Stable store-level identity for replaying one pass/tip/command gate."""
    identity = json.dumps(
        [task_ref, lane_id, pass_id, tip, command],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"offload-gate-v1-{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


class _WriterProbeRefused(RuntimeError):
    """A native-writer Git query expired or could not run; ``reason`` is the typed refusal."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _writer_git_probe(
    worktree: Path,
    probe: str,
    *args: str,
    deadline: float,
) -> subprocess.CompletedProcess[str]:
    """Run one native-writer Git query under the shared probe deadline.

    Expiry or a git process that cannot start is a typed refusal, never a clean
    or empty answer. A non-zero exit is returned for the caller to classify.
    """
    try:
        result = _run_git_evidence_probe(worktree, *args, deadline=deadline)
    except _GitEvidenceProbeTimeout as exc:
        raise _WriterProbeRefused(f"{probe}_probe_timeout") from exc
    if result is None:
        raise _WriterProbeRefused(f"{probe}_probe_unavailable")
    return result


# Static git check-ref-format rules that need no ref lookup, so the check stays in-process.
_BRANCH_NAME_FORBIDDEN_CHARS = frozenset(" ~^:?*[\\")


def _is_well_formed_branch_name(name: str) -> bool:
    """Whether a non-empty branch read is one plain git branch name.

    Mirrors the static ``git check-ref-format --branch`` rules, with the component rules
    applied to each ``/``-separated part. A zero-exit read can still carry malformed text
    (an embedded newline, NUL, control character, space, leading ``-``, an empty or
    dot-led component, or a ``.lock`` component); recording that would store a name no branch has.
    """
    if name.startswith("-"):
        return False
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        return False
    if any(char in _BRANCH_NAME_FORBIDDEN_CHARS for char in name):
        return False
    if ".." in name or "@{" in name or name.endswith("."):
        return False
    return all(bool(part) and not part.startswith(".") and not part.endswith(".lock") for part in name.split("/"))


def persist_verified_lane_gate(
    task_ref: str,
    lane_id: str,
    session: str,
    pass_id: str,
    worktree_path: Path,
    expected_tip: str,
    test_command: str,
    verification: dict[str, Any],
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Persist one green self-verify result against the exact checkpoint tip.

    This writer only records a verified_tests row. Slice closure and the main
    HIGH review gate remain separate decisions. Off-box captures keep their
    established four-key gate object; adapter-stamped source lineage and clean
    remainder evidence are required alongside it before recording a positive
    result. Every Git query here shares one bounded deadline; expiry or an
    unavailable git is a typed refusal that writes nothing.
    """

    def refused(reason: str) -> dict[str, Any]:
        return {
            "ok": False,
            "persisted": False,
            "reason": reason,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "pass_id": pass_id,
            "tip": expected_tip,
            "command": test_command,
        }

    if any(not isinstance(value, str) or not value.strip() for value in (task_ref, lane_id, session, pass_id)):
        return refused("gate_identity_missing")
    if not isinstance(test_command, str) or not test_command.strip():
        return refused("test_command_missing")
    if not isinstance(verification, dict):
        return refused("verification_missing")
    if verification.get("execution_ran") is not True:
        return refused("execution_not_observed")
    if verification.get("skipped") is True:
        return refused("verification_skipped")
    if verification.get("passed") is not True:
        return refused("verification_not_green")
    if verification.get("command") != test_command:
        return refused("test_command_mismatch")
    exit_code = verification.get("exit_code")
    if isinstance(exit_code, bool) or not isinstance(exit_code, int) or exit_code != 0:
        return refused("verification_exit_not_zero")
    outcome = verification.get("self_verify_outcome")
    if outcome is not None and outcome != "passed":
        return refused("verification_outcome_not_passed")

    worktree = Path(worktree_path)
    if not isinstance(expected_tip, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", expected_tip):
        return refused("expected_tip_invalid")
    # One shared budget for every native Git query in this writer: a caller's
    # earlier deadline wins, and no query gets a fresh budget of its own.
    probe_deadline = _bounded_git_probe_deadline(deadline)
    try:
        head = _writer_git_probe(worktree, "checkpoint_head", "rev-parse", "HEAD", deadline=probe_deadline)
        current_tip = head.stdout.strip() if head.returncode == 0 else ""
        if current_tip != expected_tip:
            return refused("worktree_tip_moved")
        status = _writer_git_probe(worktree, "checkpoint_status", "status", "--porcelain", deadline=probe_deadline)
        # A failed status is unknown, so it is refused rather than read as clean.
        if status.returncode != 0:
            return refused("checkpoint_status_unavailable")
        if status.stdout.strip():
            return refused("checkpoint_worktree_dirty")
    except _WriterProbeRefused as exc:
        return refused(exc.reason)
    product_tree = verification.get("product_tree")
    if not isinstance(product_tree, str) or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", product_tree):
        return refused("tested_product_identity_missing")
    try:
        tree = _writer_git_probe(
            worktree, "checkpoint_tree", "rev-parse", f"{expected_tip}^{{tree}}", deadline=probe_deadline
        )
    except _WriterProbeRefused as exc:
        return refused(exc.reason)
    committed_tree = tree.stdout.strip() if tree.returncode == 0 else ""
    if committed_tree != product_tree:
        return refused("checkpoint_product_differs_from_tested_product")

    provenance_note = "worker_self_verify"
    if verification.get("off_box_delegated") is True:
        # [GRPH-14] Keep the source receipt as a walkable lineage from the
        # producer's verified source identity to the host checkpoint product.
        # The off-box producer's capture whitelist is command/exit_code/passed/output_tail.
        # Outcome and source lineage are separate raw_payload fields by contract.
        capture = verification.get("producer_capture")
        if not isinstance(capture, dict):
            return refused("off_box_capture_missing")
        if capture.get("command") != test_command or capture.get("passed") is not True or capture.get("exit_code") != 0:
            return refused("off_box_capture_mismatch")
        remainder = verification.get("producer_remainder_evidence")
        if not isinstance(remainder, dict) or "uncommitted_remainder_present" not in remainder:
            return refused("off_box_remainder_evidence_missing")
        if remainder.get("uncommitted_remainder_read_error") is True:
            return refused("off_box_remainder_read_error")
        remainder_present = remainder.get("uncommitted_remainder_present")
        remainder_bytes = remainder.get("uncommitted_remainder_bytes")
        remainder_path = remainder.get("uncommitted_remainder_path")
        if (
            remainder_present is True
            or remainder.get("work_status") == WORK_STATUS_UNCOMMITTED_SALVAGED
            or (isinstance(remainder_bytes, int) and not isinstance(remainder_bytes, bool) and remainder_bytes > 0)
            or (isinstance(remainder_path, str) and bool(remainder_path.strip()))
        ):
            return refused("off_box_remainder_present")
        if remainder_present is not False:
            return refused("off_box_remainder_evidence_uncertain")
        remainder_read_error = remainder.get("uncommitted_remainder_read_error")
        if remainder_read_error is not None and remainder_read_error is not False:
            return refused("off_box_remainder_evidence_uncertain")
        provenance = verification.get("source_provenance")
        if not isinstance(provenance, dict):
            return refused("off_box_source_provenance_missing")
        nonce = provenance.get("dispatch_nonce")
        source_commit = provenance.get("remote_source_commit")
        source_tree = provenance.get("remote_source_tree")
        source_head = provenance.get("agent_spec_head_sha")
        base_tree = provenance.get("sandbox_base_tree")
        base_commit = provenance.get("sandbox_base_commit")
        object_id = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
        try:
            branch_probe = _writer_git_probe(
                worktree, "off_box_branch", "rev-parse", "--abbrev-ref", "HEAD", deadline=probe_deadline
            )
        except _WriterProbeRefused as exc:
            return refused(exc.reason)
        branch = branch_probe.stdout.strip() if branch_probe.returncode == 0 else ""
        if (
            provenance.get("schema_version") != 1
            or provenance.get("history_stripped") is not True
            or provenance.get("model_process_started") is not True
            or not isinstance(nonce, str)
            or not nonce
            or not isinstance(branch, str)
            or not branch
            or provenance.get("requested_branch") != branch
            or provenance.get("requested_ref") != f"refs/heads/{branch}"
            or not isinstance(provenance.get("dispatch_ref"), str)
            or not provenance["dispatch_ref"].startswith("refs/heads/")
            or not provenance["dispatch_ref"].endswith(f"-{nonce}")
            or not all(
                isinstance(value, str) and object_id.fullmatch(value)
                for value in (source_commit, source_tree, source_head, base_tree, base_commit)
            )
            or source_commit != source_head
            or source_tree != base_tree
        ):
            return refused("off_box_source_provenance_invalid")
        try:
            ancestry = _run_git_evidence_probe(
                worktree,
                "merge-base",
                "--is-ancestor",
                source_commit,
                expected_tip,
                deadline=probe_deadline,
            )
        except _GitEvidenceProbeTimeout:
            return refused("off_box_source_ancestry_probe_timeout")
        if ancestry is None:
            return refused("off_box_source_ancestry_probe_unavailable")
        if ancestry.returncode != 0:
            return refused("off_box_source_not_ancestor_of_tip")
        provenance_note = (
            f"off_box dispatch={nonce} source_commit={source_commit} source_tree={source_tree} "
            f"sandbox_base_commit={base_commit}"
        )

    event_id = _verified_lane_gate_event_id(
        task_ref=task_ref,
        lane_id=lane_id,
        pass_id=pass_id,
        tip=expected_tip,
        command=test_command,
    )
    try:
        record_probe = _writer_git_probe(
            worktree, "checkpoint_branch", "rev-parse", "--abbrev-ref", "HEAD", deadline=probe_deadline
        )
    except _WriterProbeRefused as exc:
        return refused(exc.reason)
    # A failed branch read is unknown metadata, not a detached HEAD: refuse before any write.
    if record_probe.returncode != 0:
        return refused("checkpoint_branch_unavailable")
    branch = record_probe.stdout.strip()
    # A zero exit with malformed text is unknown metadata too: refuse before the passing write.
    if branch and not _is_well_formed_branch_name(branch):
        return refused("checkpoint_branch_unavailable")
    # Detached HEAD is the one legitimate empty branch; it is recorded without a branch.
    if branch == "HEAD":
        branch = ""
    evidence_note = (
        f"Verified lane gate pass_id={pass_id}; tip={expected_tip}; command={test_command}; "
        f"provenance={provenance_note}."
    )

    try:
        from workbay_handoff_mcp.decisions import record_test_result  # noqa: PLC0415
        from workbay_handoff_mcp.shared_write_context import build_write_actor  # noqa: PLC0415

        actor = build_write_actor(
            agent="offload-engine",
            branch=branch or None,
            commit_sha=expected_tip,
            lane_id=lane_id,
        )
        # [DATA-13][GRPH-33] The same explicit pass/tip/command identity and
        # structured writer result make a replay a store-level no-op.
        recorded = record_test_result(
            session=session,
            command=test_command,
            passed=True,
            result=evidence_note,
            exit_code=0,
            actor=actor,
            task_ref=task_ref,
            event_id=event_id,
        )
    except Exception as exc:  # noqa: BLE001 - an uncertain ledger write is not green evidence
        # [RES-13] A failed or uncertain store write is contained as a negative
        # evidence result; it never degrades into a successful review input.
        return refused(f"verified_test_write_failed:{type(exc).__name__}:{exc}")

    if not isinstance(recorded, dict) or recorded.get("ok") is not True:
        data = recorded.get("data") if isinstance(recorded, dict) else None
        error = data.get("error") if isinstance(data, dict) else None
        return refused(f"verified_test_write_rejected:{error or 'record_test_result rejected'}")
    data = recorded.get("data")
    test_row = data.get("test") if isinstance(data, dict) else None
    if not isinstance(test_row, dict):
        return refused("verified_test_write_missing_row")
    if (
        test_row.get("task_ref") != task_ref
        or test_row.get("lane_id") != lane_id
        or test_row.get("session") != session
        or test_row.get("command") != test_command
        or test_row.get("commit_sha") != expected_tip
        or test_row.get("passed") not in (1, True)
    ):
        return refused("verified_test_write_identity_mismatch")
    test_id = test_row.get("id")
    if isinstance(test_id, bool) or not isinstance(test_id, int):
        return refused("verified_test_write_missing_id")
    return {
        "ok": True,
        "persisted": True,
        "test_id": test_id,
        "idempotent": bool(data.get("idempotent")) if isinstance(data, dict) else False,
        "event_id": event_id,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "session": session,
        "pass_id": pass_id,
        "tip": expected_tip,
        "command": test_command,
        "provenance": provenance_note,
    }


def _record_worker_closure(
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    backend: str,
    model: str | None,
    worktree_path: Path,
    start_head: str,
    baseline_report_id: int,
    pass_id: str | None = None,
    verified_gate_command: str | None = None,
) -> "tuple[dict[str, Any] | None, str | None]":
    """Verify commit + fresh report + test evidence, then record test_result rows
    and the slice-complete decision with the backend's actor identity.

    Returns ``(closure_info, None)`` in three shapes:
    - ``recorded=True``: evidence + slice-complete decision were written.
    - ``recorded=False`` with a ``reason``: the worker did real work but did NOT
      mark it mergeable (merge_ready=false / blocked / has blockers), so the slice
      is deliberately left open for the review gate — NOT an error.
    Returns ``(None, error_reason)`` when the worker end-state contract is violated
    (no commit, no fresh report, no evidence, write failure) — no closure recorded.
    """
    if not start_head:
        return None, ("worker end-state violated: could not resolve the lane branch HEAD before the pass (fail-closed)")
    head = _git_stdout(worktree_path, "rev-parse", "HEAD")
    if not head or head == start_head:
        return None, ("worker end-state violated: no commit landed on the lane branch during this pass")
    # The landed HEAD must descend from the pre-pass HEAD; a rewound or unrelated
    # HEAD (force-reset, wrong worktree) is not evidence the worker advanced the lane.
    ancestry = subprocess.run(
        ["git", "-C", str(worktree_path), "merge-base", "--is-ancestor", start_head, head],
        capture_output=True,
        text=True,
        check=False,
    )
    if ancestry.returncode != 0:
        return None, ("worker end-state violated: lane HEAD does not descend from the pre-pass HEAD")
    report = _fresh_worker_report(task_ref, lane_id, baseline_report_id)
    if report is None:
        return None, (
            "worker end-state violated: missing/stale worker report — no report was "
            "recorded during this pass; no slice closure recorded"
        )
    try:
        test_commands = json.loads(report.get("test_commands_json") or "[]")
    except json.JSONDecodeError:
        test_commands = []
    if isinstance(test_commands, list):
        test_commands = [command for command in test_commands if isinstance(command, str) and command.strip()]
    else:
        test_commands = []
    if not test_commands:
        return None, ("worker end-state violated: worker report carries no test evidence; no slice closure recorded")

    # Merge-readiness gate: a slice-complete decision asserts the work is ready for
    # the review gate. A worker that finished but reported merge_ready=false (or a
    # blocked/failed outcome, or open blockers) must NOT auto-close the slice.
    merge_ready = bool(report.get("merge_ready"))
    report_outcome = str(report.get("outcome") or "").strip().lower()
    try:
        blockers = json.loads(report.get("blockers_json") or "[]")
    except json.JSONDecodeError:
        blockers = []
    if not merge_ready or report_outcome in {"failed", "exhausted", "stopped"} or blockers:
        return {
            "recorded": False,
            "reason": (
                f"worker report not mergeable (merge_ready={merge_ready}, "
                f"outcome={report_outcome or 'unset'}, blockers={len(blockers) if isinstance(blockers, list) else 0}); "
                "slice left open for the review gate"
            ),
            "merge_ready": merge_ready,
            "commit_sha": head,
            "worker_report_id": report.get("id"),
        }, None

    from workbay_handoff_mcp.core import close_slice as handoff_close_slice  # noqa: PLC0415
    from workbay_handoff_mcp.decisions import record_test_result  # noqa: PLC0415
    from workbay_handoff_mcp.shared_write_context import build_write_actor  # noqa: PLC0415

    author_tag = re.sub(r"[^a-z]", "", str(backend).split("-")[0].lower()) or "worker"
    branch = _git_stdout(worktree_path, "rev-parse", "--abbrev-ref", "HEAD") or None
    if branch == "HEAD":  # detached HEAD has no branch name
        branch = None
    backend_model = f"{backend}/{model or 'default'}"
    # Attribute the write to the offload backend engine (not the model identity):
    # build_write_actor would otherwise derive agent from model and drop the
    # backend marker. lane_id is carried for provenance; branch 'HEAD' -> None.
    actor = build_write_actor(
        agent=f"{author_tag}-offload-engine",
        branch=branch,
        commit_sha=head,
        lane_id=lane_id,
    )
    for command in test_commands:
        evidence = record_test_result(
            session=session,
            command=str(command),
            passed=merge_ready,
            result=f"Recorded by the offload engine ({backend_model}) from worker report #{report.get('id')} at {head}.",
            actor=actor,
            task_ref=task_ref,
            event_id=(
                _verified_lane_gate_event_id(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    pass_id=pass_id,
                    tip=head,
                    command=str(command),
                )
                if pass_id is not None and str(command) == verified_gate_command
                else None
            ),
        )
        if isinstance(evidence, dict) and evidence.get("ok") is False:
            evidence_err = evidence.get("error") or (evidence.get("data") or {}).get("error")
            return None, (
                f"worker evidence write failed for '{command}': {evidence_err or 'record_test_result rejected'}"
            )

    try:
        changed_files_raw = json.loads(report.get("changed_files_json") or "[]")
    except json.JSONDecodeError:
        changed_files_raw = []
    changed_files = [str(path) for path in changed_files_raw if isinstance(path, str)] or None

    # implementation note R5: close-time package smoke. A slice that broke its own package
    # (out-of-scope of the worker's scoped TEST_CMD) must fail at closure, not
    # slip through to the merge gate. Blocking on a real red (no closure, commit
    # preserved); the cap, environment-fault, and pre-existing-red notes are
    # the non-blocking paths.
    smoke_ok, smoke_note = _package_smoke(worktree_path, changed_files, base_sha=start_head)
    if not smoke_ok:
        return None, smoke_note

    with sqlite3.connect(_handoff_db_path()) as conn:
        revision_row = conn.execute(
            "SELECT revision FROM handoff_state WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    expected_revision = int(revision_row[0]) if revision_row is not None else None

    # Bind the decision id to the landed commit so a second pass on the same lane
    # (new commit) writes a distinct decision instead of hitting close_slice's
    # idempotent envelope and silently reporting a false success.
    slug = re.sub(r"\W", "_", f"offload_{lane_id}_{head[:12]}")
    decision_id = f"{author_tag}_slice_complete_{task_ref}_{slug}"
    summary = str(report.get("summary") or "Offloaded slice completed by the backend worker.")
    rationale = (
        f"## Changes\n{summary}\n\n"
        f"## Verification\nWorker test commands recorded as fresh test_result rows at {head} "
        f"by {backend_model}: {', '.join(str(command) for command in test_commands)}. merge_ready={merge_ready}.\n\n"
        "## Schema / Contract Changes\nNone recorded by the offload engine; see the lane diff at the review gate.\n\n"
        "## Open Threads\nLane handoff diff awaits the orchestrator review gate (no auto-merge)."
    )
    closure = handoff_close_slice(
        session=session,
        decision=decision_id,
        rationale=rationale,
        actor=actor,
        expected_revision=expected_revision,
        task_ref=task_ref,
        changed_files=changed_files,
    )
    closure_data = closure.get("data", {}) if isinstance(closure.get("data"), dict) else {}
    if not closure.get("ok"):
        return None, (
            "worker evidence verified but the engine slice closure write failed: "
            f"{closure_data.get('error') or closure_data.get('state_error') or 'unknown close_slice failure'}"
        )
    # close_slice's idempotent envelope returns ok=true but decision_recorded=false
    # when the same decision id already exists. Report that as NOT recorded rather
    # than a false success, so a repeated pass on one commit cannot masquerade as a
    # fresh closure.
    decision_recorded = closure.get("decision_recorded")
    if decision_recorded is None:
        decision_recorded = closure_data.get("decision_recorded")
    idempotent = bool(closure.get("idempotent") or closure_data.get("idempotent"))
    if idempotent or decision_recorded is False:
        return {
            "recorded": False,
            "reason": "idempotent close_slice: a slice-complete decision already exists for this commit; no new decision written",
            "decision": decision_id,
            "commit_sha": head,
            "worker_report_id": report.get("id"),
            "merge_ready": merge_ready,
        }, None
    closure_result: dict[str, Any] = {
        "recorded": True,
        "decision": decision_id,
        "commit_sha": head,
        "worker_report_id": report.get("id"),
        "test_commands": [str(command) for command in test_commands],
        "merge_ready": merge_ready,
        "changed_files": changed_files or [],
    }
    # Propagate close_slice partial/degrade so a degraded close is not reported
    # as clean success ([RES-06], [OBS-08]). partial/side_effect_failures live on
    # the envelope data block; warnings are top-level on the envelope.
    partial = bool(closure.get("partial") or closure_data.get("partial"))
    if partial:
        closure_result["partial"] = True
        failures = closure.get("side_effect_failures") or closure_data.get("side_effect_failures")
        if failures:
            closure_result["side_effect_failures"] = failures
        close_warnings = closure.get("warnings") or closure_data.get("warnings")
        if close_warnings:
            closure_result["warnings"] = close_warnings
    # implementation note R5: surface a non-blocking package-smoke degrade (cap tripped /
    # runner unavailable) so a skipped smoke reads as a named gap, not silence.
    if smoke_note is not None:
        closure_result["smoke_note"] = smoke_note
    return closure_result, None


# ---------------------------------------------------------------------------
# Lane lifecycle (implementation note S13 / T26)
# ---------------------------------------------------------------------------
# Sync /offload creates the worktree lane but historically never transitioned
# its status; only the daemon auto-closed post-intake. Own the lifecycle here:
# handoff_ready → status "review" (closeable state for the gate); gate then
# one-call closes status "merged" via close_offload_lane_merged / next_lane_action.
# MERGECLOSE-01: a plain git merge into main/master also closes via
# close_lanes_merged_into_integration (post-merge hook → close_offload_lane_merged)
# when the lane branch is fully contained in the tip and actually diverged
# before landing; never-started cuts and unmerged branches stay open.
# task-finish remains the disk teardown path, not the row-close trigger.
# Heuristics: [OBS-08] terminal lifecycle must not be silent; [CON-04] avoid
# orphan open lanes after a completed pass.

# Page size matches the binder's historical single-page ask. Cap pages so a
# stuck has_more cannot hang the post-merge hook; past the cap the result
# reports truncation rather than looking complete.
_MERGECLOSE_LIST_PAGE_SIZE = 500
_MERGECLOSE_LIST_MAX_PAGES = 20  # 10_000 lanes per task; hang bound


def _lane_payload_dict(payload: Any) -> dict[str, Any]:
    """Normalize manage_worktree_lane / list responses to a plain dict."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("schema_version") == 2 and isinstance(payload.get("data"), dict):
        flat = dict(payload)
        flat.update(payload["data"])
        return flat
    return payload


def _persist_escalation_decision(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    pass_id: str,
    review_round: int,
    decision: Any,
    receipt_history: dict[str, Any],
) -> dict[str, Any]:
    """Persist a coherent row+manifest routing switch and its audit decision.

    ``manage_worktree_lane(update_routing)`` is the only permitted row writer:
    unlike upsert it cannot reset status or test_cmd.  If decision recording
    fails after the routing writes, compensate both stores.  The manifest write
    mutates only its five routing keys under the existing manifest flock, so
    work ownership, dependency, base-SHA, and bounded test metadata survive.
    """
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        atomic_update_manifest,
    )

    manifest_path = (
        Path(orchestrator_root).expanduser().resolve() / "config" / "lane-orchestration" / f"{task_ref}.json"
    )
    routing_keys = {
        "preferred_backend": "backend",
        "preferred_model": "model",
        "preferred_reasoning_effort": "effort",
        "preferred_speed": "speed",
        "preferred_tier": "tier",
    }

    def _write_manifest_quad(quad: Any) -> dict[str, tuple[bool, Any]]:
        previous: dict[str, tuple[bool, Any]] = {}

        def _apply(manifest: dict[str, Any]) -> None:
            lanes = manifest.get("lanes")
            lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
            if not isinstance(lane, dict):
                raise RuntimeError(f"lane {lane_id!r} missing from escalation manifest")
            for manifest_key, quad_key in routing_keys.items():
                previous[manifest_key] = (manifest_key in lane, lane.get(manifest_key))
                value = getattr(quad, quad_key)
                if value is None:
                    lane.pop(manifest_key, None)
                else:
                    lane[manifest_key] = value

        atomic_update_manifest(manifest_path, _apply)
        return previous

    def _restore_manifest(previous: dict[str, tuple[bool, Any]]) -> dict[str, Any]:
        def _apply(manifest: dict[str, Any]) -> None:
            lanes = manifest.get("lanes")
            lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
            if not isinstance(lane, dict):
                raise RuntimeError(f"lane {lane_id!r} missing from escalation manifest")
            for key, (was_present, value) in previous.items():
                if was_present:
                    lane[key] = value
                else:
                    lane.pop(key, None)

        atomic_update_manifest(manifest_path, _apply)
        return {"ok": True}

    def _write_row_quad(quad: Any) -> dict[str, Any]:
        return _lane_payload_dict(
            manage_worktree_lane(
                operation="update_routing",
                task_ref=task_ref,
                lane_id=lane_id,
                backend=quad.backend,
                model=quad.model,
                reasoning_effort=quad.effort,
                speed=quad.speed,
                tier=quad.tier,
            )
        )

    to_quad = decision.to_quad
    update = _write_row_quad(to_quad)
    if update.get("ok") is not True:
        raise RuntimeError(f"escalation routing update failed: {update.get('error') or update!r}")
    try:
        previous_manifest = _write_manifest_quad(to_quad)
    except Exception as exc:  # noqa: BLE001 -- restore the row to manifest authority
        rollback = _write_row_quad(decision.from_quad)
        rollback_note = "rollback succeeded" if rollback.get("ok") is True else f"rollback failed: {rollback!r}"
        raise RuntimeError(f"escalation manifest update failed: {exc}; {rollback_note}") from exc

    event_id = f"offload_escalation:{task_ref}:{session}:{pass_id}:{lane_id}:{review_round}"
    rationale = {
        "lane_id": lane_id,
        **decision.as_dict(),
        "receipt_history": {
            "turns": max(0, int(receipt_history.get("turns", 0))),
            "rounds": max(0, int(receipt_history.get("rounds", 0))),
            "tokens": max(0, int(receipt_history.get("tokens", 0))),
            "tokens_complete": bool(receipt_history.get("tokens_complete", True)),
            "high_findings": max(0, int(receipt_history.get("high_findings", 0))),
            "pass_ids": sorted({str(value) for value in receipt_history.get("pass_ids", ()) if str(value).strip()}),
        },
    }
    try:
        recorded = _lane_payload_dict(
            record_decision(
                session=session,
                task_ref=task_ref,
                event_id=event_id,
                decision=event_id,
                rationale=json.dumps(rationale, sort_keys=True),
                decision_origin="system",
                total_tokens=rationale["receipt_history"]["tokens"],
            )
        )
    except Exception as exc:  # noqa: BLE001 -- compensate an unaudited row write
        recorded = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    recorded_decision = recorded.get("decision")
    receipt_matches = (
        isinstance(recorded_decision, dict)
        and recorded_decision.get("task_ref") == task_ref
        and recorded_decision.get("session") == session
        and recorded_decision.get("decision") == event_id
        and recorded_decision.get("rationale") == json.dumps(rationale, sort_keys=True)
    )
    if recorded.get("ok") is not True or not receipt_matches:
        from_quad = decision.from_quad
        try:
            manifest_rollback = _restore_manifest(previous_manifest)
        except Exception as exc:  # noqa: BLE001 -- surface failed compensation
            manifest_rollback = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        # If the manifest could not roll back, retain the target row so the two
        # routing authorities remain coherent even though the audit write failed.
        if manifest_rollback.get("ok") is True:
            try:
                row_rollback = _write_row_quad(from_quad)
            except Exception as exc:  # noqa: BLE001 -- surface failed compensation
                row_rollback = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            if row_rollback.get("ok") is not True:
                # The row remains on the target quad.  Restore that same target
                # as manifest authority instead of returning with split routing.
                try:
                    _write_manifest_quad(to_quad)
                    manifest_rollback = {"ok": False, "error": "reapplied target after row rollback failed"}
                except Exception as exc:  # noqa: BLE001 -- report irreducible split state
                    manifest_rollback = {
                        "ok": False,
                        "error": f"target coherence restore failed: {type(exc).__name__}: {exc}",
                    }
        else:
            row_rollback = {"ok": False, "error": "skipped to preserve row/manifest target coherence"}
        rollback_note = (
            "rollback succeeded"
            if manifest_rollback.get("ok") is True and row_rollback.get("ok") is True
            else f"rollback incomplete: manifest={manifest_rollback!r}, row={row_rollback!r}"
        )
        raise RuntimeError(
            "escalation decision record failed: "
            f"{recorded.get('error') or ('idempotent receipt mismatch' if recorded.get('ok') else recorded)!r}; "
            f"{rollback_note}"
        )
    return rationale


def _history_timestamp(value: Any) -> float | None:
    """Normalize pass/SQLite UTC timestamps for accepted-lineage ordering."""

    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class _MatchingTerminalPasses(list[tuple[float, str, dict[str, Any]]]):
    """Terminal pass records with the completeness of their source scan."""

    complete: bool
    doubtful_pass_ids: tuple[str, ...]
    abandoned_pass_ids: tuple[str, ...]

    def __init__(
        self,
        records: list[tuple[float, str, dict[str, Any]]],
        *,
        complete: bool,
        doubtful_pass_ids: tuple[str, ...] = (),
        abandoned_pass_ids: tuple[str, ...] = (),
    ) -> None:
        super().__init__(records)
        self.complete = complete
        self.doubtful_pass_ids = doubtful_pass_ids
        self.abandoned_pass_ids = abandoned_pass_ids


_TERMINAL_NON_DONE_PASS_STATUSES = frozenset({"abandoned", "error", "failed", "refused"})
_PASS_HISTORY_RESULT_FIELDS = frozenset(
    {"abandoned_at", "dispatch_receipt", "producer_question_count", "token_accounting", "tokens"}
)


def _matching_terminal_passes(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    current_pass_id: str | None = None,
    orchestrator_root: Path | None = None,
) -> _MatchingTerminalPasses:
    """Read matching terminal ledgers and report unfinished same-session passes.

    If the derived lane index reports ``unknown``, scan the authoritative pass
    record files so recoverable history is not silently omitted. Unreadable or
    non-dict source files remain excluded using the same semantics as
    ``read_pass_state`` and make the returned history incomplete. Session
    identity is checked before pass status: legacy ``session: null`` records
    remain wildcard matches. A same-session ``running`` record is always
    doubtful here; only the engine, while holding the worker lock, may finalize
    it as terminal ``abandoned`` and account for its persisted progress.
    """

    records: list[tuple[float, str, dict[str, Any]]] = []
    abandoned_pass_ids: set[str] = set()
    lane_records = lane_pass_records(state_dir, task_ref=task_ref, lane_id=lane_id)
    candidates: list[tuple[str, dict[str, Any], float]] = []
    complete = lane_records.sources_complete
    doubtful_pass_ids = set(lane_records.doubtful_pass_ids)
    if lane_records.status == "unknown":
        for path in Path(state_dir).glob("offload-pass-*.json"):
            pass_id = path.name.removeprefix("offload-pass-").removesuffix(".json")
            try:
                persisted = json.loads(path.read_text(encoding="utf-8"))
                metadata = path.stat()
            except (OSError, UnicodeError, json.JSONDecodeError):
                complete = False
                doubtful_pass_ids.add(pass_id)
                continue
            if not isinstance(persisted, dict):
                complete = False
                doubtful_pass_ids.add(pass_id)
                continue
            record_task = persisted.get("task_ref")
            record_lane = persisted.get("lane_id")
            if (
                not isinstance(record_task, str)
                or not record_task
                or not isinstance(record_lane, str)
                or not record_lane
            ):
                complete = False
                doubtful_pass_ids.add(pass_id)
                continue
            if record_task == task_ref and record_lane == lane_id:
                candidates.append((pass_id, persisted, metadata.st_mtime))
    else:
        candidates.extend((record.pass_id, record.payload, record.mtime) for record in lane_records.records)

    for pass_id, persisted, mtime in candidates:
        if pass_id == current_pass_id:
            continue
        if not isinstance(persisted, dict):
            continue
        record_session = persisted.get("session")
        if record_session is not None and record_session != session:
            continue
        status = persisted.get("status")
        if status != "done":
            if isinstance(status, str) and status in _TERMINAL_NON_DONE_PASS_STATUSES:
                result = persisted.get("result")
                if not isinstance(result, dict) or not _PASS_HISTORY_RESULT_FIELDS.intersection(result):
                    # Preserve the legacy treatment of terminal status-only records.
                    # A terminal record that has history facts below must not be
                    # silently dropped from the pass-ledger aggregations.
                    continue
            elif status == "running":
                # Read-side scans never estimate spend for an unfinished pass.
                # The engine's lock-protected finalizer owns this transition.
                complete = False
                doubtful_pass_ids.add(pass_id)
                continue
            else:
                complete = False
                doubtful_pass_ids.add(pass_id)
                continue
        completed_at = _history_timestamp(persisted.get("terminal_at"))
        records.append((completed_at if completed_at is not None else mtime, pass_id, persisted))
        if status == "abandoned":
            abandoned_pass_ids.add(pass_id)
    records.sort(key=lambda record: (record[0], record[1]))
    return _MatchingTerminalPasses(
        records,
        complete=complete,
        doubtful_pass_ids=tuple(sorted(doubtful_pass_ids)),
        abandoned_pass_ids=tuple(sorted(abandoned_pass_ids)),
    )


def _pass_admitted_token_budget(persisted: dict[str, Any]) -> int | None:
    """Return a valid positive budget retained by a pass spec or result."""

    result = persisted.get("result")
    sources = [persisted.get("lane_spec")]
    if isinstance(result, dict):
        sources.extend((result.get("lane_spec"), result.get("tokens"), result.get("dispatch_receipt")))
        sources.append(result)
    for source in sources:
        if not isinstance(source, dict):
            continue
        value = source.get("token_budget")
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    return None


def _abandoned_pass_candidates(state_dir: Path, task_ref: str, lane_id: str) -> list[tuple[str, dict[str, Any]]]:
    lane_records = lane_pass_records(state_dir, task_ref=task_ref, lane_id=lane_id)
    if lane_records.status != "unknown":
        return [(record.pass_id, record.payload) for record in lane_records.records]
    candidates: list[tuple[str, dict[str, Any]]] = []
    for path in state_dir.glob("offload-pass-*.json"):
        pass_id = path.name.removeprefix("offload-pass-").removesuffix(".json")
        persisted = read_pass_state(state_dir, pass_id)
        if isinstance(persisted, dict):
            candidates.append((pass_id, persisted))
    return candidates


def _validated_abandoned_pass_current(
    state_dir: Path,
    pass_id: str,
    candidate: dict[str, Any],
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    expected_dispatch_id: Any,
) -> dict[str, Any] | None:
    if (
        candidate.get("status") != "running"
        or candidate.get("task_ref") != task_ref
        or candidate.get("lane_id") != lane_id
        or (candidate.get("session") is not None and candidate.get("session") != session)
    ):
        return None
    current = read_pass_state(state_dir, pass_id)
    if (
        not isinstance(current, dict)
        or current.get("status") != "running"
        or current.get("dispatch_id") != expected_dispatch_id
        or current.get("task_ref") != task_ref
        or current.get("lane_id") != lane_id
        or (current.get("session") is not None and current.get("session") != session)
    ):
        return None
    return current


def _abandoned_pass_token_accounting(
    current: dict[str, Any], progress: dict[str, Any], prior_tokens: dict[str, Any]
) -> tuple[str, int | None, int | None, bool, bool]:
    lane_spec = current.get("lane_spec") if isinstance(current.get("lane_spec"), dict) else {}
    resolved_backend = current.get("backend") or lane_spec.get("backend")
    telemetry_supported = progress.get("token_telemetry_supported")
    if not isinstance(telemetry_supported, bool):
        from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
            backend_supports_token_telemetry,
        )

        telemetry_supported = backend_supports_token_telemetry(
            str(resolved_backend) if isinstance(resolved_backend, str) else None
        )

    observed_value = progress.get("observed_tokens")
    tokens_observed = progress.get("tokens_observed") is True
    if observed_value is None and telemetry_supported:
        observed_value = prior_tokens.get("cumulative_total")
        tokens_observed = observed_value is not None
    observed_tokens = (
        observed_value
        if isinstance(observed_value, int) and not isinstance(observed_value, bool) and observed_value >= 0
        else None
    )
    call_boundary_final = "model_call_in_flight" in progress and progress.get("model_call_in_flight") is None
    token_budget = _pass_admitted_token_budget(current)

    if (
        telemetry_supported
        and call_boundary_final
        and observed_tokens is not None
        and (tokens_observed or observed_tokens > 0)
    ):
        return "observed", observed_tokens, token_budget, telemetry_supported, True
    if telemetry_supported and token_budget is not None:
        # Registry cycle bounds constrain turn count/time, not spend within the
        # in-flight call, so a ceiling cannot certify complete accounting here.
        return "ceiling", None, token_budget, telemetry_supported, call_boundary_final
    return "unknown", None, token_budget, telemetry_supported, False


def _build_abandoned_pass_result(current: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    progress_value = current.get("progress")
    progress = dict(progress_value) if isinstance(progress_value, dict) else {}
    prior_result_value = current.get("result")
    prior_result = dict(prior_result_value) if isinstance(prior_result_value, dict) else {}
    prior_tokens_value = prior_result.get("tokens")
    prior_tokens = dict(prior_tokens_value) if isinstance(prior_tokens_value, dict) else {}
    token_accounting, observed_tokens, token_budget, telemetry_supported, tokens_complete = (
        _abandoned_pass_token_accounting(current, progress, prior_tokens)
    )
    result_tokens = dict(prior_tokens)
    result_tokens.update(
        token_budget=token_budget,
        token_telemetry=telemetry_supported,
        cumulative_total=observed_tokens if token_accounting == "observed" else None,
    )
    receipt_value = prior_result.get("dispatch_receipt")
    dispatch_receipt = dict(receipt_value) if isinstance(receipt_value, dict) else {}
    if token_accounting == "observed":
        dispatch_receipt["observed_tokens"] = observed_tokens
    else:
        dispatch_receipt.pop("observed_tokens", None)
    dispatch_receipt.setdefault("turns", 0)
    dispatch_receipt.setdefault("rounds", 0)
    dispatch_receipt["tokens_complete"] = tokens_complete

    question_value = progress.get("producer_question_count")
    if question_value is None:
        question_value = prior_result.get("producer_question_count")
    question_count = (
        question_value
        if isinstance(question_value, int) and not isinstance(question_value, bool) and question_value >= 0
        else None
    )
    abandoned_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    result = {
        **prior_result,
        "outcome": "abandoned",
        "progress": progress,
        "producer_question_count": question_count,
        "tokens": result_tokens,
        "dispatch_receipt": dispatch_receipt,
        "token_accounting": token_accounting,
        "abandoned_at": abandoned_at,
    }
    if question_count is None:
        result.setdefault("question_count_escalation_emitted", False)
    return abandoned_at, result


def _publish_abandoned_pass_if_unchanged(
    state_dir: Path,
    pass_id: str,
    current: dict[str, Any],
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    abandoned_at: str,
    result: dict[str, Any],
) -> bool:
    latest = read_pass_state(state_dir, pass_id)
    if (
        not isinstance(latest, dict)
        or latest.get("status") != "running"
        or latest.get("dispatch_id") != current.get("dispatch_id")
        or latest.get("task_ref") != task_ref
        or latest.get("lane_id") != lane_id
        or (latest.get("session") is not None and latest.get("session") != session)
    ):
        return False
    write_pass_state(
        state_dir,
        pass_id,
        {**latest, "status": "abandoned", "terminal_at": abandoned_at, "result": result},
    )
    return True


def _finalize_abandoned_passes(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    current_pass_id: str,
    orchestrator_root: Path,
) -> tuple[str, ...]:
    """Finalize orphaned same-session running passes while the lane lock is held."""
    state_dir = Path(state_dir)
    if _worker_driver_lock_is_live(Path(orchestrator_root), lane_id, excluding_pid=os.getpid(), state_dir=state_dir):
        return ()
    finalized: list[str] = []
    for pass_id, candidate in _abandoned_pass_candidates(state_dir, task_ref, lane_id):
        if pass_id == current_pass_id:
            continue
        expected_dispatch_id = candidate.get("dispatch_id")
        current = _validated_abandoned_pass_current(
            state_dir,
            pass_id,
            candidate,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            expected_dispatch_id=expected_dispatch_id,
        )
        if current is None:
            continue
        abandoned_at, result = _build_abandoned_pass_result(current)
        if _publish_abandoned_pass_if_unchanged(
            state_dir,
            pass_id,
            current,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            abandoned_at=abandoned_at,
            result=result,
        ):
            finalized.append(pass_id)
    return tuple(sorted(finalized))


def _latest_accepted_output_boundary(records: list[tuple[float, str, dict[str, Any]]]) -> float | None:
    """Return the latest accepted-output completion, which closes a cost lineage."""

    accepted = [
        completed_at
        for completed_at, _pass_id, persisted in records
        if isinstance(persisted.get("result"), dict) and persisted["result"].get("outcome") == "handoff_ready"
    ]
    return max(accepted, default=None)


def _abandoned_pass_tokens_incomplete(persisted: dict[str, Any]) -> bool:
    result = persisted.get("result")
    if not isinstance(result, dict):
        return False
    if result.get("token_accounting") == "unknown":
        return True
    receipt = result.get("dispatch_receipt")
    if isinstance(receipt, dict) and receipt.get("tokens_complete") is False:
        return True
    return result.get("token_accounting") == "ceiling" and _pass_admitted_token_budget(persisted) is None


def _terminal_pass_receipt_tokens_incomplete(persisted: dict[str, Any]) -> bool:
    """Detect explicit incomplete accounting and a call left inside its boundary."""
    progress = persisted.get("progress")
    if isinstance(progress, dict) and progress.get("model_call_in_flight") is not None:
        return True
    result = persisted.get("result")
    if not isinstance(result, dict):
        return False
    for accounting in (result.get("tokens"), result.get("dispatch_receipt")):
        if isinstance(accounting, dict) and accounting.get("tokens_complete") is False:
            return True
    return False


def _persisted_escalation_receipt_history(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    current_pass_id: str | None = None,
    orchestrator_root: Path | None = None,
) -> dict[str, Any]:
    """Load receipt facts carried across explicit escalation redispatches."""
    prefix = "offload_escalation:"
    history: dict[str, Any] = {
        "turns": 0,
        "rounds": 0,
        "tokens": 0,
        "tokens_complete": True,
        "high_findings": 0,
        "escalated_from": None,
        "pass_ids": [],
        "routing_history": [],
        "tier": None,
        "history_complete": True,
        "doubtful_pass_ids": [],
        "abandoned_pass_ids": [],
    }
    terminal_records = _matching_terminal_passes(
        state_dir,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        current_pass_id=current_pass_id,
        orchestrator_root=orchestrator_root,
    )
    history["history_complete"] = terminal_records.complete
    history["doubtful_pass_ids"] = list(terminal_records.doubtful_pass_ids)
    history["abandoned_pass_ids"] = list(terminal_records.abandoned_pass_ids)
    accepted_boundary = _latest_accepted_output_boundary(terminal_records)
    active_terminal_records = [
        (completed_at, pass_id, persisted)
        for completed_at, pass_id, persisted in terminal_records
        if accepted_boundary is None or completed_at > accepted_boundary
    ]
    incomplete_terminal_tokens = any(
        _terminal_pass_receipt_tokens_incomplete(persisted)
        or (persisted.get("status") == "abandoned" and _abandoned_pass_tokens_incomplete(persisted))
        for _completed_at, _pass_id, persisted in active_terminal_records
    )
    if not terminal_records.complete:
        history["tokens_complete"] = False
    if incomplete_terminal_tokens:
        history["tokens_complete"] = False
    closed_pass_ids = {
        pass_id
        for completed_at, pass_id, _persisted in terminal_records
        if accepted_boundary is not None and completed_at <= accepted_boundary
    }

    def _nonnegative_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    try:
        with contextlib.closing(connect_handoff_db(_handoff_db_path(), read_only=True)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT rationale, total_tokens, created_at
                FROM decisions
                WHERE task_ref = ? AND session = ?
                  AND substr(decision, 1, ?) = ?
                ORDER BY id ASC
                """,
                (task_ref, session, len(prefix), prefix),
            ).fetchall()
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        logger.warning("escalation receipt history unavailable for %s/%s: %s", task_ref, lane_id, exc)
        history["history_complete"] = False
        history["tokens_complete"] = False
        return history

    legacy_history = dict(history)
    cumulative_history: dict[str, Any] | None = None
    for row in rows:
        decision_timestamp = _history_timestamp(row["created_at"])
        try:
            rationale = json.loads(str(row["rationale"] or ""))
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(rationale, dict) or str(rationale.get("lane_id") or "") != lane_id:
            continue
        # Daemon row-update audit events use the same decision-id family but
        # are not explicit escalation passes and carry no spend history.
        if rationale.get("trigger") == "row_update":
            continue
        receipt = rationale.get("receipt_history")
        if not isinstance(receipt, dict):
            continue
        pass_ids = sorted({str(value) for value in receipt.get("pass_ids", ()) if str(value).strip()})
        if accepted_boundary is not None:
            # SQLite timestamps have one-second precision while terminal
            # ledgers retain microseconds.  Whole seconds strictly before the
            # boundary are closed; an equal second is resolved by pass ids so
            # a redispatch immediately after acceptance is not discarded.
            if decision_timestamp is None or int(decision_timestamp) < int(accepted_boundary):
                continue
            if pass_ids:
                if all(pass_id in closed_pass_ids for pass_id in pass_ids):
                    continue
        from_quad = rationale.get("from_quad")
        if history["escalated_from"] is None and isinstance(from_quad, dict):
            history["escalated_from"] = dict(from_quad)
        stored_tokens = receipt.get("tokens")
        if stored_tokens is None:
            stored_tokens = row["total_tokens"]
        if pass_ids:
            # New receipts are cumulative and name the passes they cover.  The
            # latest one supersedes earlier overlapping escalation decisions,
            # avoiding double-counting the same producer pass.
            cumulative_history = {
                "turns": _nonnegative_int(receipt.get("turns")),
                "rounds": _nonnegative_int(receipt.get("rounds")),
                "tokens": _nonnegative_int(stored_tokens),
                "tokens_complete": bool(receipt.get("tokens_complete", True)),
                "high_findings": _nonnegative_int(receipt.get("high_findings")),
                "escalated_from": history["escalated_from"],
                "pass_ids": pass_ids,
                "routing_history": [
                    dict(segment) for segment in receipt.get("routing_history", ()) if isinstance(segment, dict)
                ],
                "tier": receipt.get("tier") if isinstance(receipt.get("tier"), str) else None,
            }
        elif cumulative_history is None:
            legacy_history["turns"] += _nonnegative_int(receipt.get("turns"))
            legacy_history["rounds"] += _nonnegative_int(receipt.get("rounds"))
            legacy_history["tokens"] += _nonnegative_int(stored_tokens)
            legacy_history["tokens_complete"] = bool(legacy_history["tokens_complete"]) and bool(
                receipt.get("tokens_complete", True)
            )
            legacy_history["high_findings"] += _nonnegative_int(receipt.get("high_findings"))
            legacy_history["escalated_from"] = history["escalated_from"]
            legacy_history["routing_history"].extend(
                dict(segment) for segment in receipt.get("routing_history", ()) if isinstance(segment, dict)
            )
            if isinstance(receipt.get("tier"), str):
                legacy_history["tier"] = receipt["tier"]
    if cumulative_history is not None:
        cumulative_history["history_complete"] = terminal_records.complete
        cumulative_history["doubtful_pass_ids"] = list(terminal_records.doubtful_pass_ids)
        cumulative_history["abandoned_pass_ids"] = list(terminal_records.abandoned_pass_ids)
        if not terminal_records.complete or incomplete_terminal_tokens:
            cumulative_history["tokens_complete"] = False
        return cumulative_history
    history.update(legacy_history)
    return history


def _prior_terminal_receipt_history(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    current_pass_id: str,
    excluded_pass_ids: set[str],
    orchestrator_root: Path | None = None,
) -> dict[str, Any]:
    """Aggregate local cost facts from prior matching terminal pass ledgers."""

    history: dict[str, Any] = {
        "turns": 0,
        "rounds": 0,
        "tokens": 0,
        "tokens_complete": True,
        "high_findings": 0,
        "pass_ids": [],
        "routing_history": [],
        "tier": None,
        "escalated_from": None,
        "history_complete": True,
        "doubtful_pass_ids": [],
        "abandoned_pass_ids": [],
    }
    records = _matching_terminal_passes(
        state_dir,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        current_pass_id=current_pass_id,
        orchestrator_root=orchestrator_root,
    )
    history["history_complete"] = records.complete
    history["doubtful_pass_ids"] = list(records.doubtful_pass_ids)
    history["abandoned_pass_ids"] = list(records.abandoned_pass_ids)
    if not records.complete:
        history["tokens_complete"] = False
    accepted_boundary = _latest_accepted_output_boundary(records)
    for completed_at, pass_id, persisted in records:
        if pass_id in excluded_pass_ids:
            continue
        if accepted_boundary is not None and completed_at <= accepted_boundary:
            continue
        result = persisted.get("result")
        if not isinstance(result, dict):
            history["tokens_complete"] = False
            history["pass_ids"].append(pass_id)
            continue
        receipt = result.get("dispatch_receipt")
        tokens = result.get("tokens")
        if not isinstance(receipt, dict) or not isinstance(tokens, dict):
            history["tokens_complete"] = False
            history["pass_ids"].append(pass_id)
            continue
        if _terminal_pass_receipt_tokens_incomplete(persisted) or (
            persisted.get("status") == "abandoned" and _abandoned_pass_tokens_incomplete(persisted)
        ):
            history["tokens_complete"] = False
        try:
            history["turns"] += max(0, int(receipt.get("turns") or 0))
            history["rounds"] += max(0, int(receipt.get("rounds") or 0))
            history["high_findings"] += max(0, int(receipt.get("high_finding_count") or 0))
        except (TypeError, ValueError):
            history["tokens_complete"] = False
        token_accounting = result.get("token_accounting")
        if token_accounting == "ceiling":
            token_total = _pass_admitted_token_budget(persisted)
        elif token_accounting == "unknown":
            token_total = None
        else:
            token_total = tokens.get("cumulative_total")
            if token_total is None:
                result_backend = result.get("backend")
                try:
                    alternate_total_key = token_telemetry_total_key_for_backend(
                        result_backend if isinstance(result_backend, str) else None
                    )
                except ValueError:
                    alternate_total_key = None
                    history["tokens_complete"] = False
                if alternate_total_key is not None:
                    token_total = tokens.get(alternate_total_key)
            if token_total is None:
                token_total = receipt.get("observed_tokens")
        if token_total is None:
            history["tokens_complete"] = False
        else:
            try:
                history["tokens"] += max(0, int(token_total))
            except (TypeError, ValueError):
                history["tokens_complete"] = False
        raw_routing_history = receipt.get("routing_history")
        appended_routes: list[dict[str, Any]] = []
        if isinstance(raw_routing_history, list):
            appended_routes = [dict(item) for item in raw_routing_history if isinstance(item, dict)]
            history["routing_history"].extend(appended_routes)
        route = receipt.get("escalated_from")
        if not isinstance(route, dict) and appended_routes:
            candidate = appended_routes[0].get("routing_effective")
            route = candidate if isinstance(candidate, dict) else None
        if not isinstance(route, dict):
            candidate = receipt.get("routing_effective")
            route = candidate if isinstance(candidate, dict) else None
        if history["escalated_from"] is None and isinstance(route, dict):
            history["escalated_from"] = dict(route)
        tier = receipt.get("tier")
        if not isinstance(tier, str) and isinstance(receipt.get("routing_effective"), dict):
            tier = receipt["routing_effective"].get("tier")
        if isinstance(tier, str):
            history["tier"] = tier
        history["pass_ids"].append(pass_id)
    return history


class _HistoryCount(int):
    """An integer history count carrying whether its source scan was complete."""

    complete: bool
    doubtful_pass_ids: tuple[str, ...]
    abandoned_pass_ids: tuple[str, ...]
    unknown_abandoned_pass_ids: tuple[str, ...]

    def __new__(
        cls,
        value: int,
        *,
        complete: bool,
        doubtful_pass_ids: tuple[str, ...] = (),
        abandoned_pass_ids: tuple[str, ...] = (),
        unknown_abandoned_pass_ids: tuple[str, ...] = (),
    ) -> _HistoryCount:
        instance = super().__new__(cls, value)
        instance.complete = complete
        instance.doubtful_pass_ids = doubtful_pass_ids
        instance.abandoned_pass_ids = abandoned_pass_ids
        instance.unknown_abandoned_pass_ids = unknown_abandoned_pass_ids
        return instance


def _persisted_producer_question_count(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    session: str,
    current_pass_id: str,
    orchestrator_root: Path | None = None,
) -> _HistoryCount:
    """Count producer questions from earlier explicit dispatches.

    A needs-guidance handoff terminates its pass before a review result exists.
    The terminal pass ledger is therefore the durable bridge that lets a later
    explicit redispatch reach the two-question escalation trigger.
    """

    total = 0
    unknown_abandoned_pass_ids: list[str] = []
    records = _matching_terminal_passes(
        state_dir,
        task_ref=task_ref,
        lane_id=lane_id,
        session=session,
        current_pass_id=current_pass_id,
        orchestrator_root=orchestrator_root,
    )
    accepted_boundary = _latest_accepted_output_boundary(records)
    for completed_at, pass_id, persisted in records:
        if accepted_boundary is not None and completed_at <= accepted_boundary:
            continue
        result = persisted.get("result")
        if not isinstance(result, dict):
            continue
        try:
            question_count = result.get("producer_question_count")
            if (
                persisted.get("status") == "abandoned"
                and (isinstance(question_count, bool) or not isinstance(question_count, int) or question_count < 0)
                and result.get("question_count_escalation_emitted") is not True
            ):
                unknown_abandoned_pass_ids.append(pass_id)
            else:
                total = max(total, max(0, int(question_count or 0)))
        except (TypeError, ValueError):
            continue
    return _HistoryCount(
        total,
        complete=records.complete,
        doubtful_pass_ids=records.doubtful_pass_ids,
        abandoned_pass_ids=records.abandoned_pass_ids,
        unknown_abandoned_pass_ids=tuple(sorted(unknown_abandoned_pass_ids)),
    )


def _mark_abandoned_question_count_escalated(state_dir: Path, pass_ids: Sequence[str]) -> None:
    """Durably remember that legacy unknown question history was surfaced once."""

    for pass_id in sorted({str(value) for value in pass_ids if str(value).strip()}):
        current = read_pass_state(state_dir, pass_id)
        if not isinstance(current, dict) or current.get("status") != "abandoned":
            continue
        result_value = current.get("result")
        if not isinstance(result_value, dict) or result_value.get("question_count_escalation_emitted") is True:
            continue
        result = dict(result_value)
        question_count = result.get("producer_question_count")
        if isinstance(question_count, int) and not isinstance(question_count, bool) and question_count >= 0:
            continue
        result["question_count_escalation_emitted"] = True
        result["question_count_escalation_emitted_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        latest = read_pass_state(state_dir, pass_id)
        if (
            not isinstance(latest, dict)
            or latest.get("status") != "abandoned"
            or latest.get("dispatch_id") != current.get("dispatch_id")
        ):
            continue
        write_pass_state(state_dir, pass_id, {**latest, "result": result})


def _turn_evidence_from_result(result: dict[str, Any]) -> Any:
    """Project one worker result onto the escalation evaluator's evidence."""
    from workbay_orchestrator_mcp.orchestration.lane_escalation import (  # noqa: PLC0415
        TurnEvidence,
    )

    raw = result.get("raw_payload")
    raw = raw if isinstance(raw, dict) else {}
    timing = raw.get("phase_timing")
    if not isinstance(timing, dict):
        timing = result.get("phase_timing") if isinstance(result.get("phase_timing"), dict) else {}
    tool_call_count = timing.get("tool_call_count")
    if tool_call_count is None:
        phases = timing.get("phases")
        agent_turn = phases.get("agent_turn") if isinstance(phases, dict) else None
        if isinstance(agent_turn, dict):
            tool_call_count = agent_turn.get("tool_call_count")

    result_path_value = raw.get("result_path")
    spool = Path(result_path_value).parent if isinstance(result_path_value, str) else None
    patch_bytes: int | None = None
    spec_path: Path | None = None
    phases_path: Path | None = None
    if spool is not None:
        patch_path = spool / "turn.patch"
        try:
            patch_bytes = patch_path.stat().st_size
        except OSError:
            patch_bytes = None
        spec_path = spool / "spec.json"
        phases_path = spool / "phases.json"
    return TurnEvidence(
        patch_bytes=patch_bytes,
        tool_call_count=(
            tool_call_count
            if isinstance(tool_call_count, int) and not isinstance(tool_call_count, bool) and tool_call_count >= 0
            else None
        ),
        spec_path=spec_path,
        phases_path=phases_path,
    )


def _review_escalation_findings(review_output: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
    """Use the same review boundary for routing and receipt HIGH counts."""
    from workbay_orchestrator_mcp.orchestration.lane_escalation import _finding_is_in_diff  # noqa: PLC0415

    if not isinstance(review_output, dict):
        return ()
    subject = review_output.get("review_subject")
    skip_reason = None
    if review_output.get("escalation_skipped") == "review_subject_unbounded":
        skip_reason = "review_subject_unbounded"
    elif isinstance(subject, dict) and subject.get("scope_kind") == "unresolved_base":
        skip_reason = "unresolved_base"
    if skip_reason is not None:
        findings = review_output.get("findings")
        high_count = (
            sum(1 for finding in findings if isinstance(finding, dict) and _finding_is_high(finding))
            if isinstance(findings, (list, tuple))
            else 0
        )
        logger.warning(
            "review escalation skipped reason=%s excluded_high_count=%d",
            skip_reason,
            high_count,
        )
        return ()
    findings = review_output.get("findings")
    if not isinstance(findings, (list, tuple)):
        return ()
    changed_files = review_output.get("changed_files")
    if changed_files is not None and not isinstance(changed_files, (list, tuple)):
        return ()
    return tuple(
        finding
        for finding in findings
        if isinstance(finding, dict)
        and (changed_files is None or _finding_is_in_diff(finding, changed_files, review_output.get("diff_ranges")))
    )


def _evaluate_pass_escalation(
    *,
    run_ctx: Any,
    backend: str,
    model: str | None,
    reasoning_effort: str,
    speed: str | None,
    review_round: int,
    review_output: dict[str, Any] | None,
    turns: list[Any],
    producer_question_count: int,
) -> Any:
    """Build one typed escalation state at both normal and blocked review exits."""
    from workbay_orchestrator_mcp.orchestration.codex_lane_config import CODEX_MODEL_TIERS  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_escalation import (  # noqa: PLC0415
        LaneEscalationState,
        evaluate_escalation,
    )
    from workbay_orchestrator_mcp.orchestration.lane_routing import RoutingQuad  # noqa: PLC0415

    current_model = str(getattr(run_ctx, "model", None) or model or "") or None
    current_tier = str(getattr(run_ctx, "tier", None) or "") or None
    if current_tier is None and current_model in CODEX_MODEL_TIERS:
        current_tier = CODEX_MODEL_TIERS[current_model].tier
    findings = _review_escalation_findings(review_output)
    return evaluate_escalation(
        LaneEscalationState(
            current_quad=RoutingQuad(
                backend=str(getattr(run_ctx, "backend", None) or backend or "") or None,
                model=current_model,
                effort=(
                    str(
                        getattr(run_ctx, "cycle_reasoning_effort", None)
                        or getattr(run_ctx, "reasoning_effort", None)
                        or reasoning_effort
                        or ""
                    )
                    or None
                ),
                speed=str(getattr(run_ctx, "speed", None) or speed or "") or None,
                tier=current_tier,
            ),
            review_round=review_round,
            review_findings=tuple(item for item in findings if isinstance(item, dict)),
            turns=tuple(turns),
            producer_question_count=producer_question_count,
        ),
        changed_files=review_output.get("changed_files") if isinstance(review_output, dict) else None,
        diff_ranges=review_output.get("diff_ranges") if isinstance(review_output, dict) else None,
    )


def _review_consumes_bounded_round(review_output: dict[str, Any] | None) -> bool:
    """Only actionable HIGH/MEDIUM review output consumes the repair cap."""
    if isinstance(review_output, dict) and review_output.get("converged") is True:
        return False
    findings = review_output.get("findings") if isinstance(review_output, dict) else None
    if not isinstance(findings, (list, tuple)):
        return True
    return any(isinstance(finding, dict) and is_blocking(finding) for finding in findings)


def _list_all_worktree_lanes(*, task_ref: str | None) -> dict[str, Any]:
    """Collect every worktree_lanes page before any mutation.

    When *task_ref* is set, pages the task-scoped list envelope. When absent,
    pages the whole table via the cross-task keyset helper in
    ``lane_worktree`` — ``manage_worktree_lane(list, task_ref=None)`` resolves
    to the active task and cannot serve a post-merge sweep.

    Offset/keyset paging can reshuffle under concurrent closes; collect first,
    then process. Cap pages so an unbounded ``has_more`` cannot hang —
    truncation is reported on the envelope rather than returned as a quiet
    short list.
    """
    if task_ref is None:
        return _list_all_worktree_lanes_cross_task()
    return _list_all_worktree_lanes_for_task(task_ref=task_ref)


def _list_all_worktree_lanes_for_task(*, task_ref: str) -> dict[str, Any]:
    """Collect every worktree_lanes page for a single *task_ref*."""
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    all_lanes: list[Any] = []
    offset = 0
    last_listed: dict[str, Any] = {}
    for _ in range(_MERGECLOSE_LIST_MAX_PAGES):
        listed = _lane_payload_dict(
            manage_worktree_lane(
                operation="list",
                task_ref=task_ref,
                status="all",
                limit=_MERGECLOSE_LIST_PAGE_SIZE,
                offset=offset,
            )
        )
        last_listed = listed
        if listed.get("ok") is not True:
            return listed
        page = listed.get("lanes")
        if not isinstance(page, list):
            return {
                "ok": False,
                "error": str(listed.get("error") or "list_worktree_lanes returned non-list lanes"),
            }
        all_lanes.extend(page)
        if listed.get("has_more") is not True:
            return {
                "ok": True,
                "lanes": all_lanes,
                "has_more": False,
                "total_matching": listed.get("total_matching"),
                "list_truncated": False,
                "pages_fetched": (offset // _MERGECLOSE_LIST_PAGE_SIZE) + 1,
            }
        if not page:
            break
        offset += len(page)
    else:
        # Page cap hit with has_more still true.
        return {
            "ok": True,
            "lanes": all_lanes,
            "has_more": True,
            "total_matching": last_listed.get("total_matching"),
            "list_truncated": True,
            "pages_fetched": _MERGECLOSE_LIST_MAX_PAGES,
        }
    # has_more claimed with an empty page — treat as truncated universe.
    return {
        "ok": True,
        "lanes": all_lanes,
        "has_more": True,
        "total_matching": last_listed.get("total_matching"),
        "list_truncated": True,
        "pages_fetched": max(1, (offset // _MERGECLOSE_LIST_PAGE_SIZE) + 1),
    }


def _list_all_worktree_lanes_cross_task() -> dict[str, Any]:
    """Page the entire ``worktree_lanes`` table with no active-task resolution.

    Mirrors the ownership-sweep completeness contract in ``lane_worktree``:
    ``has_more`` must be a real bool, and on the final page ``total_matching``
    must be an int equal to the collected length — otherwise fail closed
    rather than claiming a proven-complete universe. ``sqlite3.Error`` is
    named explicitly so a lock/schema fault becomes a structured list
    failure instead of escaping to the hook's catch-everything.
    """
    from workbay_orchestrator_mcp.orchestration.lane_worktree import (  # noqa: PLC0415
        _list_lanes,
    )

    all_lanes: list[Any] = []
    after_id: int | None = None
    last_total: object = None
    for page_idx in range(_MERGECLOSE_LIST_MAX_PAGES):
        try:
            env = _list_lanes(after_id=after_id, limit=_MERGECLOSE_LIST_PAGE_SIZE)
        except (ImportError, RuntimeError, AttributeError, OSError, sqlite3.Error) as exc:
            return {
                "ok": False,
                "error": f"cross_task_list_lanes failed: {type(exc).__name__}: {exc}",
            }
        if not isinstance(env, dict) or env.get("ok") is not True:
            detail = "cross_task_list_lanes envelope not ok"
            if isinstance(env, dict):
                data = env.get("data")
                if isinstance(data, dict) and data.get("error"):
                    detail = str(data.get("error"))
            return {"ok": False, "error": detail}
        data = env.get("data")
        if not isinstance(data, dict):
            return {"ok": False, "error": "cross_task_list_lanes data missing or malformed"}
        page = data.get("lanes")
        if not isinstance(page, list):
            return {"ok": False, "error": "cross_task_list_lanes returned non-list lanes"}
        all_lanes.extend(page)
        last_total = data.get("total_matching")
        has_more = data.get("has_more")
        if not isinstance(has_more, bool):
            return {
                "ok": False,
                "error": "cross_task_list_lanes has_more missing or non-bool",
            }
        if not has_more:
            # Completeness oracle (same discipline as lane_worktree ownership
            # sweep): final-page total must be an int and equal collected
            # length. Drift catches inserts above a DESC cursor and rows lost
            # to conversion filters on non-empty pages.
            total = last_total
            if type(total) is not int:
                return {
                    "ok": False,
                    "error": "cross_task_list_lanes total_matching missing or non-int",
                }
            if total != len(all_lanes):
                return {
                    "ok": True,
                    "lanes": all_lanes,
                    "has_more": False,
                    "total_matching": total,
                    "list_truncated": True,
                    "list_truncation_reason": (
                        f"total_matching drift: collected={len(all_lanes)} total_matching={total}"
                    ),
                    "pages_fetched": page_idx + 1,
                }
            return {
                "ok": True,
                "lanes": all_lanes,
                "has_more": False,
                "total_matching": last_total,
                "list_truncated": False,
                "pages_fetched": page_idx + 1,
            }
        if not page:
            break
        next_cursor = data.get("next_after_id")
        if type(next_cursor) is not int:
            return {
                "ok": False,
                "error": "cross_task_list_lanes next_after_id missing while has_more",
            }
        after_id = next_cursor
    else:
        return {
            "ok": True,
            "lanes": all_lanes,
            "has_more": True,
            "total_matching": last_total,
            "list_truncated": True,
            "pages_fetched": _MERGECLOSE_LIST_MAX_PAGES,
        }
    return {
        "ok": True,
        "lanes": all_lanes,
        "has_more": True,
        "total_matching": last_total,
        "list_truncated": True,
        "pages_fetched": max(1, (len(all_lanes) // _MERGECLOSE_LIST_PAGE_SIZE) + 1),
    }


def _lookup_worktree_lane(*, task_ref: str, lane_id: str) -> dict[str, Any] | None:
    """Return the worktree_lanes row for task_ref/lane_id, or None.

    Pages the list envelope until the id is found or the page cap is hit —
    a single-page scan silently misses older lanes on oversized tasks.
    """
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    offset = 0
    for _ in range(_MERGECLOSE_LIST_MAX_PAGES):
        listed = _lane_payload_dict(
            manage_worktree_lane(
                operation="list",
                task_ref=task_ref,
                status="all",
                limit=_MERGECLOSE_LIST_PAGE_SIZE,
                offset=offset,
            )
        )
        if listed.get("ok") is not True:
            return None
        lanes = listed.get("lanes")
        if not isinstance(lanes, list):
            return None
        for row in lanes:
            if isinstance(row, dict) and str(row.get("lane_id") or "") == lane_id:
                return row
        if listed.get("has_more") is not True:
            return None
        if not lanes:
            return None
        offset += len(lanes)
    return None


def next_lane_close_action(*, task_ref: str, lane_id: str) -> dict[str, Any]:
    """One-call gate close contract exposed on handoff_ready pass results.

    The sync offload flow has no daemon post-intake hook; the review gate
    closes the lane when it merges the slice by invoking
    ``close_offload_lane_merged`` (or the equivalent ``manage_worktree_lane``
    call documented here).
    """
    return {
        "tool": "manage_worktree_lane",
        "helper": "close_offload_lane_merged",
        "operation": "close",
        "status": "merged",
        "lane_id": lane_id,
        "task_ref": task_ref,
        "notes": "Closed by offload review gate post-merge (implementation note S13 / T26).",
    }


def next_lane_guidance_action(*, task_ref: str, lane_id: str, blockers: list[str]) -> dict[str, Any]:
    """Describe the follow-up needed for a landed worker result with open blockers."""
    return {
        "action": "provide_guidance",
        "task_ref": task_ref,
        "lane_id": lane_id,
        "blockers": list(blockers),
    }


_MERGE_CLOSE_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
_MERGE_CLOSE_NOTES = (
    "Closed by post-merge ancestry binder (MERGECLOSE-01); lane branch fully contained in integration tip."
)
_INTEGRATION_RECEIPT_RELDIR = Path(".git") / "workbay" / "integration-receipts"
_NON_ANCESTRAL_COUNTS_FILENAME = "non_ancestral_landing_counts.json"


def _worker_driver_lock_is_live(
    orchestrator_root: Path,
    lane_id: str,
    *,
    excluding_pid: int | None = None,
    state_dir: Path | None = None,
) -> bool:
    """Use the kernel probe, exempting only our actual acquired handle."""
    from .worker_lock import current_process_holds_worker_lock, lock_is_held

    lock_dir = Path(state_dir) if state_dir is not None else Path(orchestrator_root) / ".task-state"
    lock_path = lock_dir / f"worker-{lane_id}.lock"
    if excluding_pid == os.getpid() and current_process_holds_worker_lock(lock_path):
        return False
    return lock_is_held(lock_path)


def _absent_branch_stale_eligibility(
    row: dict[str, Any],
    *,
    lane_id: str,
    orchestrator_root: Path | None,
) -> tuple[bool, str]:
    """Run local presence probes, then make the pure stale decision."""
    from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
        absent_branch_stale_eligible,
    )

    raw_worktree_path = row.get("worktree_path")
    worktree_exists = False
    if isinstance(raw_worktree_path, str) and raw_worktree_path.strip():
        worktree_path = Path(raw_worktree_path)
        if not worktree_path.is_absolute() and orchestrator_root is not None:
            worktree_path = Path(orchestrator_root) / worktree_path
        try:
            worktree_path.lstat()
            worktree_exists = True
        except FileNotFoundError:
            worktree_exists = False
        except OSError:
            worktree_exists = True
    driver_live = orchestrator_root is None or _worker_driver_lock_is_live(orchestrator_root, lane_id)
    return absent_branch_stale_eligible(
        row,
        now_epoch=time.time(),
        worktree_exists=worktree_exists,
        driver_live=driver_live,
    )


def _integration_receipt_dir(orchestrator_root: Path) -> Path:
    return Path(orchestrator_root) / _INTEGRATION_RECEIPT_RELDIR


def _receipt_filename(task_ref: str, lane_id: str) -> str:
    def _safe(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
        return cleaned or "unknown"

    return f"{_safe(task_ref)}__{_safe(lane_id)}.json"


def _git_is_ancestor(orchestrator_root: Path, maybe_ancestor: str, tip: str) -> bool:
    if not maybe_ancestor or not tip:
        return False
    result = subprocess.run(
        ["git", "-C", str(orchestrator_root), "merge-base", "--is-ancestor", maybe_ancestor, tip],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _read_integration_receipt(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _persist_integration_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(serialized, encoding="utf-8")
    tmp.replace(path)


def _persist_non_ancestral_counts(path: Path, counts: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(counts, sort_keys=True, indent=2) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(serialized, encoding="utf-8")
    tmp.replace(path)


def list_integration_receipts(orchestrator_root: Path | str) -> list[dict[str, Any]]:
    """Return durable integration receipts stored under the integration repo."""
    directory = _integration_receipt_dir(Path(orchestrator_root))
    if not directory.is_dir():
        return []
    receipts: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        if path.name == _NON_ANCESTRAL_COUNTS_FILENAME:
            continue
        payload = _read_integration_receipt(path)
        if payload is not None:
            receipts.append(payload)
    return receipts


def non_ancestral_landing_counts(orchestrator_root: Path | str) -> dict[str, int]:
    """Return landing-time non-ancestral counts split by lane kind."""
    path = _integration_receipt_dir(Path(orchestrator_root)) / _NON_ANCESTRAL_COUNTS_FILENAME
    payload = _read_integration_receipt(path)
    if not payload:
        return {}
    counts: dict[str, int] = {}
    for key, value in payload.items():
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            counts[str(key)] = value
    return counts


def _resolved_receipt_lane_kind(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    supplied: str | None,
) -> str:
    if supplied in ("implement", "review"):
        return supplied
    kind, confirmed = _lane_kind(orchestrator_root, task_ref, lane_id)
    if confirmed and kind in ("implement", "review"):
        return kind
    return "unknown"


def _increment_non_ancestral_count(orchestrator_root: Path, lane_kind: str) -> None:
    path = _integration_receipt_dir(orchestrator_root) / _NON_ANCESTRAL_COUNTS_FILENAME
    counts = non_ancestral_landing_counts(orchestrator_root)
    counts[lane_kind] = int(counts.get(lane_kind) or 0) + 1
    _persist_non_ancestral_counts(path, counts)


def _integration_receipt_matches(
    orchestrator_root: Path | str,
    *,
    task_ref: str,
    lane_id: str,
    branch: str | None,
    observed_tip: str | None,
) -> bool:
    """True when a stored receipt names this lane branch at the observed tip."""
    if not branch or not observed_tip:
        return False
    path = _integration_receipt_dir(Path(orchestrator_root)) / _receipt_filename(task_ref, lane_id)
    receipt = _read_integration_receipt(path)
    if receipt is None:
        return False
    return (
        str(receipt.get("lane_id") or "") == lane_id
        and str(receipt.get("task_ref") or "") == task_ref
        and str(receipt.get("branch") or "") == branch
        and str(receipt.get("branch_tip") or "") == observed_tip
    )


def record_integration_landing(
    *,
    orchestrator_root: Path | str,
    task_ref: str,
    lane_id: str,
    branch: str,
    integration_ref: str = "HEAD",
    lane_kind: str | None = None,
) -> dict[str, Any]:
    """Write a durable receipt at the moment landing is known.

    Idempotent under replay. A receipt-write fault is recorded and never fails
    the landing — the content already landed; evidence is a bulkhead around it.
    """
    root = Path(orchestrator_root)
    observed_tip = _git_stdout(root, "rev-parse", branch)
    integration_rev = _git_stdout(root, "rev-parse", integration_ref)
    ancestral = _git_is_ancestor(root, observed_tip, integration_rev)
    kind = _resolved_receipt_lane_kind(root, task_ref, lane_id, lane_kind)
    receipt = {
        "lane_id": lane_id,
        "task_ref": task_ref,
        "branch": branch,
        "branch_tip": observed_tip,
        "integration_ref": integration_ref,
        "integration_rev": integration_rev,
        "lane_kind": kind,
        "ancestral": ancestral,
    }
    result: dict[str, Any] = {
        "ok": True,
        "receipt": receipt,
        "ancestral": ancestral,
    }
    path = _integration_receipt_dir(root) / _receipt_filename(task_ref, lane_id)
    existing = _read_integration_receipt(path)
    replay = (
        isinstance(existing, dict)
        and existing.get("branch") == branch
        and existing.get("branch_tip") == observed_tip
        and existing.get("integration_rev") == integration_rev
    )
    try:
        _persist_integration_receipt(path, receipt)
    except Exception as exc:  # noqa: BLE001 — receipt write must not fail landing
        logger.warning(
            "integration receipt write failed for lane %s: %s",
            lane_id,
            exc,
        )
        result["receipt_write_error"] = f"{type(exc).__name__}: {exc}"
        return result
    if not ancestral and not replay:
        try:
            _increment_non_ancestral_count(root, kind)
        except Exception as exc:  # noqa: BLE001 — counters must not fail landing
            logger.warning(
                "non-ancestral landing counter write failed for lane %s: %s",
                lane_id,
                exc,
            )
            result["counter_write_error"] = f"{type(exc).__name__}: {exc}"
    return result


_DEFAULT_CLOSE_INTEGRATION_REFS: tuple[str, ...] = ("main",)
_INTEGRATION_REF_GIT_TIMEOUT_SECONDS = 30.0


def _git_branch_ref_is_valid(name: str) -> bool:
    """True when ``git check-ref-format --branch`` accepts *name*."""
    try:
        result = subprocess.run(
            ["git", "check-ref-format", "--branch", name],
            capture_output=True,
            text=True,
            check=False,
            timeout=_INTEGRATION_REF_GIT_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0


def _coerce_integration_ref_list(value: object) -> list[str]:
    """Normalise a validated string or list of refs (strip only)."""
    if isinstance(value, str):
        return [value.strip()]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item).strip() for item in value]
    raise TypeError(f"integration_branch has invalid type {type(value).__name__}")


def _read_manifest_integration_refs(
    orchestrator_root: Path,
    task_ref: str | None,
) -> list[str] | None:
    """Read ``integration_branch`` from the task manifest. Missing → None.

    A present invalid value raises ``ManifestResolutionError`` rather than
    falling through to the default ``main`` (OBS-08 / API-11).
    """
    if not task_ref:
        return None
    path = orchestrator_root / "config" / "lane-orchestration" / f"{task_ref}.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(raw, dict) or "integration_branch" not in raw:
        return None
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        _validate_integration_branch,
    )
    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        ManifestResolutionError,
    )

    value = raw.get("integration_branch")
    try:
        _validate_integration_branch(value, path)
    except RuntimeError as exc:
        raise ManifestResolutionError(str(exc)) from exc
    refs = _coerce_integration_ref_list(value)
    for ref in refs:
        if not _git_branch_ref_is_valid(ref):
            raise ManifestResolutionError(
                f"task manifest {path} field integration_branch is not a valid branch name: {ref!r}"
            )
    return refs


def _resolve_close_integration_refs(
    *,
    orchestrator_root: Path,
    task_ref: str | None,
    integration_refs: Sequence[str] | None,
) -> tuple[list[str], str]:
    """Explicit argument, then manifest ``integration_branch``, then ``main``.

    A present invalid manifest value raises ``ManifestResolutionError``.
    """
    if integration_refs is not None:
        refs = [str(ref).strip() for ref in integration_refs if str(ref).strip()]
        return refs, "argument"
    declared = _read_manifest_integration_refs(orchestrator_root, task_ref)
    if declared:
        return list(declared), "manifest"
    return list(_DEFAULT_CLOSE_INTEGRATION_REFS), "default"


def _rev_parse_integration_ref(orchestrator_root: Path, ref: str) -> str:
    if not ref:
        return ""
    try:
        result = subprocess.run(
            ["git", "-C", str(orchestrator_root), "rev-parse", ref],
            capture_output=True,
            text=True,
            check=False,
            timeout=_INTEGRATION_REF_GIT_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _merge_close_envelope(
    *,
    ok: bool,
    closed: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    already_terminal: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    integration_refs: Sequence[str],
    integration_ref_source: str,
    error: str | None = None,
    list_truncated: bool = False,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": ok,
        "closed": closed,
        "skipped": skipped,
        "already_terminal": already_terminal,
        "failed": failed,
        "integration_refs": list(integration_refs),
        "integration_ref_source": integration_ref_source,
    }
    if error:
        result["error"] = error
    if list_truncated:
        result["list_truncated"] = True
    return result


def _trigger_background_retire_sweep(root: Path) -> dict[str, str]:
    """Start one bounded sweep, deferring an in-pass merge close until done is durable."""
    pass_state = _ACTIVE_OFFLOAD_PASS_SWEEP_STATE.get()
    if pass_state is not None:
        existing = pass_state.get("trigger")
        if isinstance(existing, dict):
            return dict(existing)
        if pass_state.get("terminal_durable") is not True:
            # A merge-close hook can run while the pass is still publishing its
            # result. Defer its sweep request to _finish so status=done is the
            # durable ordering edge, and the finalizer will spawn only once.
            return {"outcome": "deferred"}

    try:
        from workbay_orchestrator_mcp.orchestration.lane_retire_sweep import (  # noqa: PLC0415
            spawn_background_sweep,
        )

        sweep_result = spawn_background_sweep(root, respect_optout=True)
        trigger_result: dict[str, str] = {"outcome": str(sweep_result.get("outcome") or "unknown")}
        if sweep_result.get("error"):
            trigger_result["error"] = str(sweep_result["error"])
    except Exception as exc:  # noqa: BLE001 — retirement cleanup cannot change pass outcome
        trigger_result = {"outcome": "spawn_failed", "error": f"{type(exc).__name__}: {exc}"}

    if pass_state is not None:
        pass_state["trigger"] = dict(trigger_result)
    if trigger_result["outcome"] == "spawn_failed":
        logger.warning(
            "terminal pass retire sweep spawn failed: %s",
            trigger_result.get("error", "no error supplied"),
        )
    return trigger_result


def close_lanes_merged_into_integration(
    *,
    orchestrator_root: Path | str,
    task_ref: str | None = None,
    integration_refs: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Bind merge → row close: terminalise lanes whose branch is in the tip.

    Placement: invoked from the post-merge hook (same event that lands the
    branch), not from ``task-finish`` (operator memory) and not from the disk
    reaper (which only acts on already-terminal rows). Calls
    ``close_offload_lane_merged`` only — no worktree/branch deletion.

    Ancestry of the integration tip is the close discriminator, with one
    extra refusal: a branch cut from the tip that never received a commit
    also yields an empty containment range, so after containment succeeds
    the binder checks whether the branch ever diverged (manifest
    ``base_sha`` refusal-only, first-parent lineage, tip-vs-created).
    Never-started cuts are skipped — never closed as ``merged`` with a
    fabricated landing_sha. Unmerged branches are skipped with an explicit
    ``unmerged`` reason (never forced). Already-terminal rows are reported as
    ``already_terminal`` so a second call is an idempotent no-op ([OBS-08]:
    skips are visible; silent empty success is refused).

    When *task_ref* is omitted the sweep is cross-task (full lane table);
    an explicit *task_ref* still scopes to that task alone. A truncated list
    (page cap) sets ``ok`` false so a caller that only checks success cannot
    mistake an incomplete sweep for a clean pass.

    ``force=True`` on the inner close bypasses the gate's ``review``-only
    status check so a manual ``git merge`` of a still-``blocked`` lane can
    close; containment and the empty-cut refusal inside
    ``close_offload_lane_merged`` still refuse when the branch is not fully
    in the tip or never produced commits.

    *integration_refs* wins over the task-manifest ``integration_branch``
    (string or list), which wins over the default ``main``. The winning
    source is returned as ``integration_ref_source``. Ancestry is scored
    against each resolved ref; the first ref that contains the lane tip is
    recorded as ``containing_ref``.
    """
    root = Path(orchestrator_root)

    from workbay_orchestrator_mcp.orchestration.lane_postmerge import (  # noqa: PLC0415
        ManifestResolutionError,
    )
    from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
        StaleReason,
        close_lane_stale,
    )
    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        ABSENT_LANE_BRANCH_UNRECOVERABLE,
        ABSENT_LANE_BRANCH_WORK_LANDED,
        _lane_branch_contained_in,
        _lane_branch_never_produced_commits,
        _resolve_lane_branch,
    )

    try:
        resolved_refs, ref_source = _resolve_close_integration_refs(
            orchestrator_root=root,
            task_ref=task_ref,
            integration_refs=integration_refs,
        )
    except ManifestResolutionError as exc:
        return _merge_close_envelope(
            ok=False,
            closed=[],
            skipped=[],
            already_terminal=[],
            failed=[],
            integration_refs=[],
            integration_ref_source="manifest",
            error=f"invalid_manifest_integration_branch: {exc}",
        )
    resolved_tips = [(ref, _rev_parse_integration_ref(root, ref)) for ref in resolved_refs]
    usable_tips = [(ref, tip) for ref, tip in resolved_tips if tip]
    if not usable_tips:
        named = ", ".join(resolved_refs) or "HEAD"
        return _merge_close_envelope(
            ok=False,
            closed=[],
            skipped=[],
            already_terminal=[],
            failed=[],
            integration_refs=resolved_refs,
            integration_ref_source=ref_source,
            error=(f"integration_tip_unresolvable: cannot rev-parse {named} at orchestrator_root"),
        )

    listed = _list_all_worktree_lanes(task_ref=task_ref)
    if listed.get("ok") is not True:
        return _merge_close_envelope(
            ok=False,
            closed=[],
            skipped=[],
            already_terminal=[],
            failed=[],
            integration_refs=resolved_refs,
            integration_ref_source=ref_source,
            error=str(listed.get("error") or "list_worktree_lanes failed"),
        )

    closed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    already_terminal: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    list_truncated = listed.get("list_truncated") is True
    if list_truncated:
        # Incomplete universe must not read as a finished sweep [OBS-08].
        trunc_reason = listed.get("list_truncation_reason") or "list_page_cap_reached"
        failed.append(
            {
                "error": str(trunc_reason),
                "page_size": _MERGECLOSE_LIST_PAGE_SIZE,
                "max_pages": _MERGECLOSE_LIST_MAX_PAGES,
                "pages_fetched": listed.get("pages_fetched"),
                "lanes_collected": len(listed.get("lanes") or []),
                "total_matching": listed.get("total_matching"),
            }
        )

    for row in listed.get("lanes") or []:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        row_task = str(row.get("task_ref") or task_ref or "").strip()
        if not row_task:
            skipped.append(
                {
                    "lane_id": lane_id,
                    "reason": "missing_task_ref",
                }
            )
            continue
        if task_ref is not None and row_task != task_ref:
            continue

        status = str(row.get("status") or "").strip().lower()
        if status in _MERGE_CLOSE_TERMINAL_STATUSES:
            already_terminal.append(
                {
                    "lane_id": lane_id,
                    "task_ref": row_task,
                    "status": status,
                }
            )
            continue

        lane_branch = _resolve_lane_branch(
            root,
            row_task,
            lane_id,
            branch_hint=str(row.get("branch") or "") or None,
        )
        if not lane_branch:
            skipped.append(
                {
                    "lane_id": lane_id,
                    "task_ref": row_task,
                    "reason": "branch_unresolvable",
                }
            )
            continue

        observed_tip = _git_stdout(root, "rev-parse", lane_branch)
        receipt_matches = _integration_receipt_matches(
            root,
            task_ref=row_task,
            lane_id=lane_id,
            branch=lane_branch,
            observed_tip=observed_tip,
        )
        # Preliminary triage only: unknown continues to the never-produced
        # check and the strict authorship check below; it never authorises a
        # close by itself. A matching landing receipt is the exception: the
        # fact was recorded when landing was known, including squash landings.
        containing_ref: str | None = None
        containing_tip: str | None = None
        skip_payload: dict[str, Any] | None = None
        timeout_contained: bool | str | None = None
        stale_handled = False
        for ref, tip in usable_tips:
            contained = _lane_branch_contained_in(
                root,
                tip,
                lane_branch,
                task_ref=row_task,
                lane_id=lane_id,
            )
            if contained is False and not receipt_matches:
                continue
            if receipt_matches:
                containing_ref = ref
                containing_tip = tip
                break
            never_produced, empty_reason = _lane_branch_never_produced_commits(
                root,
                tip,
                lane_branch,
                task_ref=row_task,
                lane_id=lane_id,
                lane_created_at=row.get("created_at"),
            )
            if never_produced == ABSENT_LANE_BRANCH_WORK_LANDED:
                # Recorded landing is already an ancestor of the tip. A deleted
                # branch cannot be probed; the recorded SHA is the evidence.
                containing_ref = ref
                containing_tip = tip
                break
            if never_produced == ABSENT_LANE_BRANCH_UNRECOVERABLE:
                stale_eligible, eligibility_reason = _absent_branch_stale_eligibility(
                    row,
                    lane_id=lane_id,
                    orchestrator_root=root,
                )
                if not stale_eligible:
                    skip_payload = {
                        "lane_id": lane_id,
                        "task_ref": row_task,
                        "branch": lane_branch,
                        "reason": eligibility_reason,
                    }
                    break
                stale_handled = True
                stale_reason = empty_reason or "empty_lane_absent_branch_evidence_never_captured"
                stale_notes = f"Auto-closed stale by post-merge reaper: {stale_reason}."
                stale_result = close_lane_stale(
                    task_ref=row_task,
                    lane_id=lane_id,
                    reason=StaleReason.ABSENT_BRANCH_NO_EVIDENCE,
                    notes=stale_notes,
                    expected_status=str(row.get("status") or "").strip() or None,
                    expected_updated_at=row.get("updated_at"),
                    expected_row=dict(row),
                )
                if stale_result.get("ok") is True:
                    stale_lane = stale_result.get("lane")
                    stale_default_status = "closed_stale"
                    stale_status = (
                        str(stale_lane.get("status") or stale_default_status)
                        if isinstance(stale_lane, dict)
                        else stale_default_status
                    )
                    closed.append(
                        {
                            "lane_id": lane_id,
                            "task_ref": row_task,
                            "status": stale_status,
                            "landing_sha": None,
                            "reason": stale_reason,
                        }
                    )
                else:
                    failed.append(
                        {
                            "lane_id": lane_id,
                            "task_ref": row_task,
                            "error": str(stale_result.get("error") or "closed_stale close failed"),
                            "reason": stale_reason,
                        }
                    )
                break
            if never_produced is not False:
                # True: recorded evidence is not in the tip. None: the probe
                # failed. WORK_LANDED: recorded evidence is in the tip.
                # UNRECOVERABLE: the branch is absent and no evidence was
                # captured. Tagged returns are handled above; refuse the other
                # two rather than fabricate a landing_sha for unknown work.
                skip_payload = {
                    "lane_id": lane_id,
                    "task_ref": row_task,
                    "branch": lane_branch,
                    "reason": empty_reason or "empty_lane_branch_never_diverged",
                }
                break
            # Bulk offload has no trustworthy pre-landing target tip. Candidate-tip
            # equality is therefore ambiguous and must remain a visible skip rather
            # than fabricate a landing record.
            contained = _lane_branch_contained_in(
                root,
                tip,
                lane_branch,
                task_ref=row_task,
                lane_id=lane_id,
                data_loss_safety=True,
            )
            if contained is True:
                containing_ref = ref
                containing_tip = tip
                break
            if contained == "timeout":
                timeout_contained = contained
                continue

        if containing_ref is None:
            if stale_handled:
                continue
            if skip_payload is not None:
                skipped.append(skip_payload)
            elif timeout_contained == "timeout":
                skipped.append(
                    {
                        "lane_id": lane_id,
                        "task_ref": row_task,
                        "branch": lane_branch,
                        "reason": "containment_probe_timeout",
                        "contained": timeout_contained,
                    }
                )
            else:
                skipped.append(
                    {
                        "lane_id": lane_id,
                        "task_ref": row_task,
                        "branch": lane_branch,
                        "reason": "unmerged_commits_not_in_integration_tip",
                        "contained": False,
                    }
                )
            continue

        close_result = close_offload_lane_merged(
            task_ref=row_task,
            lane_id=lane_id,
            notes=_MERGE_CLOSE_NOTES,
            force=True,
            orchestrator_root=root,
            lane_row=row,
            landed_sha=containing_tip,
        )
        if close_result.get("ok") is True:
            closed.append(
                {
                    "lane_id": lane_id,
                    "task_ref": row_task,
                    "status": close_result.get("status") or "merged",
                    "landing_sha": close_result.get("landing_sha"),
                    "containing_ref": containing_ref,
                }
            )
        else:
            failed.append(
                {
                    "lane_id": lane_id,
                    "task_ref": row_task,
                    "error": str(close_result.get("error") or "close_offload_lane_merged failed"),
                    "containing_ref": containing_ref,
                }
            )

    # Truncation must fail the success flag — callers that only check ok
    # must not treat an incomplete sweep as a clean pass [OBS-08].
    result = _merge_close_envelope(
        ok=not list_truncated,
        closed=closed,
        skipped=skipped,
        already_terminal=already_terminal,
        failed=failed,
        integration_refs=resolved_refs,
        integration_ref_source=ref_source,
        error=(str(listed.get("list_truncation_reason") or "list_page_cap_reached") if list_truncated else None),
        list_truncated=list_truncated,
    )
    try:
        _trigger_background_retire_sweep(root)
    except Exception:  # noqa: BLE001 — post-merge cleanup cannot change close result
        pass
    return result


def close_offload_lane_merged(
    *,
    task_ref: str,
    lane_id: str,
    notes: str | None = None,
    force: bool = False,
    orchestrator_root: Path | str | None = None,
    lane_row: dict[str, Any] | None = None,
    lane_created_at: object | None = None,
    landed_sha: str | None = None,
) -> dict[str, Any]:
    """One-call gate close: terminal ``merged`` for a completed offload lane.

    Symmetric to the daemon post-intake close
    (``orchestrator_daemon`` → ``manage_worktree_lane(operation=close, status=merged)``).

    S13-A-01: refuses unless the lane is in status ``review`` (the state a green
    pass leaves it in) so a premature gate call cannot mark unreviewed work
    terminal-merged. ``force=True`` overrides that review-status gate for
    operator recovery only — it does **not** override containment or the
    empty-cut refusal below. An empty cut must stay open; fabricating a
    landing is unrecoverable. A distinct override for that refusal is
    intentionally not provided.

    internal / review H1–H3: capture the landing SHA from the
    **orchestrator root** (never the lane worktree — this helper performs no
    merge; the lane worktree tip is the worker branch), require the lane branch
    to be fully contained in that SHA before recording, refuse empty cuts that
    never produced commits, and withhold MERGED when a usable SHA is in hand
    but the ledger write fails. No usable SHA still allows MERGED so a
    pathologically git-less root does not wedge the lane.

    *landed_sha* is an integration tip already resolved by the caller (the
    batch binder passes its containing tip). When supplied, that exact evidence
    is written as ``landing_commit_sha``; the root ``HEAD`` fallback remains
    only for the legacy containment gate and is not recorded as landing proof.

    *lane_row* / *lane_created_at*: optional facts from a caller that already
    listed the row (the batch binder). When supplied they are preferred over a
    second task-scoped lookup so a pager miss cannot refuse a close the caller
    already authorised. They never override containment.
    """
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_stale_close import (  # noqa: PLC0415
        StaleReason,
        close_lane_stale,
    )
    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        ABSENT_LANE_BRANCH_UNRECOVERABLE,
        ABSENT_LANE_BRANCH_WORK_LANDED,
        _is_full_commit_sha,
        _lane_branch_contained_in,
        _lane_branch_never_produced_commits,
        _resolve_lane_branch,
        record_lane_landing,
    )

    row: dict[str, Any] | None
    if isinstance(lane_row, dict):
        row = lane_row
        if not str(row.get("status") or "").strip():
            row = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id) or row
    else:
        row = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
    if not force:
        current_status = str((row or {}).get("status") or "").strip() or None
        if current_status != "review":
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": (
                    f"lane_not_in_review: lane '{lane_id}' status is "
                    f"{current_status!r} (expected 'review'); refuse terminal "
                    "merged close — pass force=True to override"
                ),
            }

    # Capture SHA from the orchestrator/task-branch checkout only (H1). Never
    # read the lane worktree tip — close_offload_lane_merged is a status close,
    # not a merge, so the worker branch tip is frequently not on the task branch.
    repo_for_sha: Path | None = None
    if orchestrator_root is not None:
        candidate = Path(orchestrator_root)
        if candidate.exists():
            repo_for_sha = candidate
    if repo_for_sha is None:
        try:
            from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

            cfg = get_runtime_config()
            root = getattr(cfg, "git_workspace_root", None) or getattr(cfg, "workspace_root", None)
            if root is not None:
                repo_for_sha = Path(root)
        except Exception:  # noqa: BLE001
            repo_for_sha = None

    provided_landed_sha = landed_sha.strip() if _is_full_commit_sha(landed_sha) else None
    landed_sha = provided_landed_sha or ""
    task_branch = "main"
    if repo_for_sha is not None:
        # Prefer the shared capture helper when available for shape/detached-HEAD
        # rules; fall back to local _git_stdout so tests can patch this module.
        raw_sha = landed_sha or (_git_stdout(repo_for_sha, "rev-parse", "HEAD") or "")
        raw_branch = _git_stdout(repo_for_sha, "rev-parse", "--abbrev-ref", "HEAD") or ""
        if _is_full_commit_sha(raw_sha):
            landed_sha = raw_sha.strip()
        if raw_branch and raw_branch != "HEAD":
            task_branch = raw_branch
        else:
            task_branch = "main"

    if landed_sha and repo_for_sha is not None:
        lane_branch = _resolve_lane_branch(
            repo_for_sha,
            task_ref,
            lane_id,
            branch_hint=str((row or {}).get("branch") or "") or None,
        )
        # Prefer caller-supplied creation stamp (binder already verified the
        # row) over a second lookup that can collapse five failure modes into
        # "undecidable" and refuse an authorised close.
        created_at = lane_created_at if lane_created_at is not None else (row or {}).get("created_at")
        observed_tip = _git_stdout(repo_for_sha, "rev-parse", lane_branch) if lane_branch else ""
        receipt_matches = _integration_receipt_matches(
            repo_for_sha,
            task_ref=task_ref,
            lane_id=lane_id,
            branch=lane_branch,
            observed_tip=observed_tip,
        )
        # Empty-cut refusal lives here so every path to MERGED passes through
        # it — not only the batch binder. force= does not override. A matching
        # integration receipt, written at landing, is alternate evidence for
        # squash/reimplement landings whose tip is not an ancestor of HEAD.
        never_produced, empty_reason = _lane_branch_never_produced_commits(
            repo_for_sha,
            landed_sha,
            lane_branch,
            task_ref=task_ref,
            lane_id=lane_id,
            lane_created_at=created_at,
        )
        if receipt_matches:
            pass
        elif never_produced == ABSENT_LANE_BRANCH_WORK_LANDED:
            # Recorded landing is already an ancestor of the integration tip.
            # Do not consult the live branch ref: a deleted branch cannot
            # resolve, and the ancestral recorded SHA is the close evidence.
            pass
        elif never_produced == ABSENT_LANE_BRANCH_UNRECOVERABLE:
            stale_eligible, eligibility_reason = _absent_branch_stale_eligibility(
                row or {},
                lane_id=lane_id,
                orchestrator_root=repo_for_sha,
            )
            if not stale_eligible:
                return {
                    "ok": False,
                    "lane_id": lane_id,
                    "task_ref": task_ref,
                    "status": str((row or {}).get("status") or "").strip().lower() or None,
                    "error": f"{eligibility_reason}: refusing absent-branch stale close",
                    "reason": eligibility_reason,
                    "landing_sha": None,
                }
            stale_reason = empty_reason or "empty_lane_absent_branch_evidence_never_captured"
            stale_notes = f"Auto-closed stale by offload close: {stale_reason}."
            stale_result = close_lane_stale(
                task_ref=task_ref,
                lane_id=lane_id,
                reason=StaleReason.ABSENT_BRANCH_NO_EVIDENCE,
                notes=stale_notes,
                expected_status=str((row or {}).get("status") or "").strip() or None,
                expected_updated_at=(row or {}).get("updated_at"),
                expected_row=dict(row or {}),
            )
            if stale_result.get("ok") is True:
                stale_lane = stale_result.get("lane")
                stale_default_status = "closed_stale"
                return {
                    "ok": True,
                    "lane_id": lane_id,
                    "task_ref": task_ref,
                    "status": (
                        str(stale_lane.get("status") or stale_default_status)
                        if isinstance(stale_lane, dict)
                        else stale_default_status
                    ),
                    "lane": stale_lane if isinstance(stale_lane, dict) else None,
                    "landing_sha": None,
                    "reason": stale_reason,
                }
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": str(stale_result.get("error") or "closed_stale close failed"),
                "reason": stale_reason,
                "landing_sha": None,
            }
        elif never_produced is not False:
            return {
                "ok": False,
                "lane_id": lane_id,
                "task_ref": task_ref,
                "error": (
                    f"empty_lane_branch: lane '{lane_id}' never produced commits "
                    f"({empty_reason or 'empty_lane_branch_never_diverged'}); "
                    "refuse fabricated landing and MERGED close. "
                    "force= does not override this refusal. If you have confirmed "
                    "by hand that the work really is in the tip, close it through "
                    "the lane-management MCP with the manage_worktree_lane close "
                    "operation and a merged terminal status — that path never "
                    "enters this primitive. (Spelled out in prose on purpose: the "
                    "MERGED-writer inventory pin scans this file's source text, so "
                    "a literal call here would register as a fourth write site.)"
                ),
                "landing_sha": None,
            }
        else:
            # Explicit close has the same evidentiary limit as the bulk path. Even
            # force cannot turn candidate-tip equality into proof of lane authorship.
            contained = (
                _lane_branch_contained_in(
                    repo_for_sha,
                    landed_sha,
                    lane_branch,
                    task_ref=task_ref,
                    lane_id=lane_id,
                    data_loss_safety=True,
                )
                if lane_branch
                else None
            )
            if contained is not True and not receipt_matches:
                # H1/H2: do not stamp false evidence and do not write MERGED.
                return {
                    "ok": False,
                    "lane_id": lane_id,
                    "task_ref": task_ref,
                    "error": (
                        f"landing_not_contained: lane '{lane_id}' branch "
                        f"{lane_branch!r} is not fully contained in orchestrator tip "
                        f"{landed_sha[:12]}; refuse landing record and MERGED close"
                    ),
                    "landing_sha": landed_sha,
                }
            # Package import path already resolved above; tests patch record_lane_landing
            # on orchestrator_lanes.
            if not record_lane_landing(task_ref, lane_id, landed_sha, task_branch):
                # H3: SHA in hand but ledger write failed — do not write MERGED.
                return {
                    "ok": False,
                    "lane_id": lane_id,
                    "task_ref": task_ref,
                    "error": (
                        f"landing_record_failed: lane '{lane_id}' tip {landed_sha[:12]} "
                        "could not be recorded; withholding MERGED for retry"
                    ),
                    "landing_sha": landed_sha,
                }

    resolved_notes = notes if notes is not None else "Closed by offload review gate post-merge (implementation note S13 / T26)."
    payload = _lane_payload_dict(
        manage_worktree_lane(
            operation="close",
            lane_id=lane_id,
            status=LaneStatus.MERGED,
            notes=resolved_notes,
            task_ref=task_ref,
            landing_commit_sha=provided_landed_sha,
        )
    )
    if payload.get("ok") is True:
        lane_obj = payload.get("lane")
        lane = lane_obj if isinstance(lane_obj, dict) else {}
        result: dict[str, Any] = {
            "ok": True,
            "lane_id": lane_id,
            "task_ref": task_ref,
            "status": str(lane.get("status") or LaneStatus.MERGED),
            "lane": lane or None,
        }
        if landed_sha:
            result["landing_sha"] = landed_sha
        return result
    return {
        "ok": False,
        "lane_id": lane_id,
        "task_ref": task_ref,
        "error": str(payload.get("error") or "close_offload_lane_merged failed"),
    }


def _mark_lane_review_on_handoff_ready(
    *,
    task_ref: str,
    lane_id: str,
    worktree_path: Path,
) -> dict[str, Any]:
    """Transition the offload lane to status ``review`` after a green pass.

    Mirrors ``orchestrator_guidance.apply_guidance_resolution`` upsert usage so
    the gate sees an unambiguous closeable state. Failure is surfaced (never
    silent) but does not downgrade handoff_ready — the worker work already
    landed ([OBS-08]).
    """
    from workbay_handoff_mcp.enums import LaneStatus  # noqa: PLC0415

    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    existing = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
    branch = ""
    title = None
    objective = None
    owner_agent = None
    model = None
    backend = None
    reasoning_effort = None
    test_cmd = None
    path = str(worktree_path)
    if existing is not None:
        branch = str(existing.get("branch") or "")
        title = existing.get("title") if isinstance(existing.get("title"), str) else None
        objective = existing.get("objective") if isinstance(existing.get("objective"), str) else None
        owner_agent = existing.get("owner_agent") if isinstance(existing.get("owner_agent"), str) else None
        model = existing.get("model") if isinstance(existing.get("model"), str) else None
        backend = existing.get("backend") if isinstance(existing.get("backend"), str) else None
        reasoning_effort = (
            existing.get("reasoning_effort") if isinstance(existing.get("reasoning_effort"), str) else None
        )
        test_cmd = existing.get("test_cmd") if isinstance(existing.get("test_cmd"), str) else None
        raw_path = existing.get("worktree_path")
        if isinstance(raw_path, str) and raw_path.strip():
            path = raw_path.strip()

    if not branch:
        return {
            "ok": False,
            "status": None,
            "error": f"lane '{lane_id}' missing branch; cannot upsert status=review",
        }

    payload = _lane_payload_dict(
        manage_worktree_lane(
            operation="upsert",
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=path,
            branch=branch,
            title=title,
            objective=objective,
            owner_agent=owner_agent,
            model=model,
            backend=backend,
            reasoning_effort=reasoning_effort,
            test_cmd=test_cmd,
            status=LaneStatus.REVIEW,
            notes="Offload pass handoff_ready; lane awaiting review-gate close (implementation note S13 / T26).",
        )
    )
    if payload.get("ok") is True:
        lane_obj = payload.get("lane")
        lane = lane_obj if isinstance(lane_obj, dict) else {}
        return {
            "ok": True,
            "status": str(lane.get("status") or LaneStatus.REVIEW),
            "lane": lane or None,
        }
    return {
        "ok": False,
        "status": None,
        "error": str(payload.get("error") or "failed to set lane status=review"),
    }


def _retire_review_lane_at_pass_end(*, repo_root: Path, task_ref: str, lane_id: str) -> dict[str, Any]:
    """Best-effort per-lane epilogue after the terminal pass is durable."""
    from workbay_orchestrator_mcp.orchestration.lane_retire_sweep import (  # noqa: PLC0415
        retire_preserved_review_lane,
    )

    try:
        row = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
        if row is None:
            return {"ok": False, "reason": "lane_row_missing"}
        result = retire_preserved_review_lane(repo_root=repo_root, row=row)
    except Exception as exc:  # noqa: BLE001 — contain cleanup failure independently of review outcome
        result = {"ok": False, "reason": "review_retire_failed", "error": f"{type(exc).__name__}: {exc}"}
    if result.get("ok") is not True and result.get("reason") != "not_review_lane":
        logger.warning("review lane retirement deferred task_ref=%s lane_id=%s result=%s", task_ref, lane_id, result)
    return result


def _close_lane_on_review_complete(
    *,
    task_ref: str,
    lane_id: str,
    pre_exec_head: str,
) -> dict[str, Any]:
    """Close a clean review lane and publish its terminal branch fact.

    ``pre_exec_head`` is the evidence captured before the review turn.  The
    review worktree may be gone by the time this hook runs, so this helper must
    never re-probe Git for the branch tip.
    """
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_terminal_dispose import (  # noqa: PLC0415
        dispose_terminal_lane,
    )

    existing = _lookup_worktree_lane(task_ref=task_ref, lane_id=lane_id)
    if existing is None:
        error = f"lane '{lane_id}' not found; cannot close review_complete lane"
        logger.warning("%s task_ref=%s", error, task_ref)
        return {"ok": False, "status": None, "error": error}

    branch = str(existing.get("branch") or "").strip()
    tip_sha = str(pre_exec_head or "").strip()
    if not branch:
        error = f"lane '{lane_id}' missing branch; cannot close review_complete lane"
        logger.warning("%s task_ref=%s", error, task_ref)
        return {"ok": False, "status": None, "error": error}
    if not tip_sha:
        error = f"lane '{lane_id}' missing captured pre_exec_head; cannot close review_complete lane"
        logger.warning("%s task_ref=%s", error, task_ref)
        return {"ok": False, "status": None, "error": error}

    raw_notes = existing.get("notes")
    notes: dict[str, Any] = {}
    if isinstance(raw_notes, dict):
        notes = dict(raw_notes)
    elif isinstance(raw_notes, str) and raw_notes.strip():
        try:
            decoded_notes = json.loads(raw_notes)
        except (TypeError, ValueError):
            decoded_notes = None
        if isinstance(decoded_notes, dict):
            notes = dict(decoded_notes)
        else:
            notes["prose"] = raw_notes
    direct_subject = existing.get("review_subject")
    if "review_subject" not in notes and isinstance(direct_subject, dict):
        notes["review_subject"] = dict(direct_subject)
    notes["disposition_pending"] = "review"

    payload = _lane_payload_dict(
        manage_worktree_lane(
            operation="close",
            task_ref=task_ref,
            lane_id=lane_id,
            status="closed",
            notes=json.dumps(notes, sort_keys=True),
            branch_tip_sha=tip_sha,
        )
    )
    if payload.get("ok") is not True:
        error = str(payload.get("error") or "failed to close review_complete lane")
        logger.warning("review-complete lane close failed for %s/%s: %s", task_ref, lane_id, error)
        return {"ok": False, "status": None, "error": error}

    lane_obj = payload.get("lane")
    lane = lane_obj if isinstance(lane_obj, dict) else {}
    try:
        enqueued = dispose_terminal_lane(
            task_ref=task_ref,
            lane_id=lane_id,
            branch=branch,
            tip_sha=tip_sha,
            outcome="terminal_lane",
            disposition="review",
            lane_kind=str(existing.get("lane_kind") or "review"),
        )
    except Exception as exc:  # noqa: BLE001 - terminal publication is best-effort but observable
        enqueued = False
        logger.warning("review-complete terminal publication failed for %s/%s: %s", task_ref, lane_id, exc)
    if not enqueued:
        error = "review_complete lane closed but terminal disposition was not enqueued"
        logger.warning("%s task_ref=%s lane_id=%s", error, task_ref, lane_id)
        return {
            "ok": False,
            "status": str(lane.get("status") or "closed"),
            "lane": lane or None,
            "error": error,
        }
    return {
        "ok": True,
        "status": str(lane.get("status") or "closed"),
        "lane": lane or None,
        "disposition": "review",
    }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def capture_pass_model_discovery(backend_id: str, *, discovery: Any = None) -> Any:
    """Capture the model-discovery object that this pass will walk back to.

    Prefer an injected preflight result. Otherwise reuse the TTL cache
    (``probe=False``) so a just-run preflight is the source of truth. Never
    re-probe here — that would be a second source of weights.
    """
    if discovery is not None:
        return discovery
    if not backend_id:
        return None
    try:
        from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
            resolve_offbox_model,
        )

        return resolve_offbox_model(str(backend_id), probe=False)
    except (KeyError, RuntimeError):
        return None


def outcome_dispatch_receipt(
    backend_id: str,
    *,
    served_model: str | None,
    discovery: Any,
    process_ran: bool = False,
    routing_effective: dict[str, Any] | None = None,
    turns: int = 0,
    rounds: int = 0,
    escalated_from: dict[str, Any] | None = None,
    accepted_output_tokens: int | None = None,
    high_finding_count: int = 0,
    observed_tokens: int = 0,
    routing_history: list[dict[str, Any]] | None = None,
    tokens_complete: bool = True,
) -> dict[str, Any] | None:
    """Walk-back receipt for a finished pass using observed execution facts.

    Discovery is retained only as the pin provenance input.  Served weights,
    gate identity, effective routing, and cost are nulled until execution has
    actually been observed; a preflight refusal must not masquerade as a turn.
    """
    if not backend_id:
        return None
    try:
        from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
            build_offload_dispatch_receipt,
        )

        if discovery is None:
            # A receipt is passive telemetry. Do not turn a missing cache entry
            # into a fresh remote model probe while publishing a pass outcome.
            receipt = {
                "backend_id": backend_id,
                "resolved_model": served_model if process_ran else None,
                "served_model": served_model if process_ran else None,
                "tracked_pin": None,
                "pin_source": "unavailable",
                "discovery_warning": "model_discovery_failed",
            }
        else:
            receipt = build_offload_dispatch_receipt(
                backend_id,
                served_model=served_model if process_ran else None,
                discovery=discovery,
            )
        if not process_ran:
            receipt["resolved_model"] = None
            receipt["served_model"] = None
            receipt["gate_host"] = None
        receipt.update(
            {
                "observed_backend": (
                    str((routing_effective or {}).get("backend") or backend_id) if process_ran else None
                ),
                "tier": routing_effective.get("tier") if process_ran and routing_effective else None,
                "originating_tier": (
                    escalated_from.get("tier")
                    if isinstance(escalated_from, dict) and escalated_from.get("tier")
                    else routing_effective.get("tier")
                    if process_ran and routing_effective
                    else None
                ),
                "routing_effective": dict(routing_effective) if process_ran and routing_effective else None,
                "turns": max(0, int(turns)),
                "rounds": max(0, int(rounds)),
                "escalated_from": dict(escalated_from) if escalated_from else None,
                "cost_per_accepted_output": (
                    max(0, int(accepted_output_tokens))
                    if tokens_complete and accepted_output_tokens is not None
                    else None
                ),
                "high_finding_count": max(0, int(high_finding_count)),
                # Pass-local observed usage is retained even before an output
                # is accepted so a later escalation can aggregate its cost.
                "observed_tokens": max(0, int(observed_tokens)) if process_ran else 0,
                # Per-route execution segments prevent a multi-cycle pass from
                # rewriting junior work as if every turn used its final route.
                "routing_history": [dict(segment) for segment in (routing_history or ())] if process_ran else [],
                "tokens_complete": bool(tokens_complete),
            }
        )
        return receipt
    except (KeyError, TypeError, ValueError):
        return None


def _remote_implement_input_gate(
    *,
    repository_root: Path,
    lane_kind: str,
    backend: str,
    test_cmd: str | None,
    token_budget: int,
    model: str | None,
) -> tuple[str | None, str | None]:
    """Refuse unusable test_cmd / starved budget for COST_REMOTE implement lanes.

    Returns ``(refuse_error, warn_message)``. Review lanes and non-remote
    backends skip the gate. The numeric floor comes from ``check_token_budget``;
    this function never writes a literal floor. Rows may be empty inside the
    engine — the floor still applies.

    The warn band is unreachable here: ``recommend_token_budget([])`` always
    yields ``source='floor'``, and ``check_token_budget`` only warns when
    ``source == 'turn_metrics'``. This engine has no turn-metrics loader, so
    the warn arm is omitted rather than kept as a prefix it cannot emit.
    """
    if lane_kind != "implement" or cost_class_for_backend(backend) != COST_REMOTE:
        return None, None

    from workbay_orchestrator_mcp.orchestration.budget_floor import (  # noqa: PLC0415
        BudgetAdvice,
        check_token_budget,
        floor_for,
        recommend_token_budget,
    )
    from workbay_orchestrator_mcp.orchestration.lane_test_cmd import (  # noqa: PLC0415
        classify_remote_test_cmd,
    )

    capability_warnings: list[str] = []
    decision, reason = classify_remote_test_cmd(
        test_cmd,
        repository_root=repository_root,
        capability_warnings=capability_warnings,
    )
    if decision == "refuse":
        diagnostic = "; ".join((reason, *capability_warnings))
        return f"{TEST_CMD_UNUSABLE_PREFIX}{diagnostic}", None

    floor = floor_for(lane_kind)
    try:
        advice = recommend_token_budget(
            [],
            backend=backend,
            model=model,
            lane_kind=lane_kind,
        )
    except Exception:  # noqa: BLE001 — metrics are advisory; the floor still applies
        advice = BudgetAdvice(
            floor=floor,
            sample_lanes=0,
            p50=None,
            p90=None,
            recommended=floor,
            source="unavailable",
        )
    verdict = check_token_budget(token_budget, advice)
    if verdict.severity == "refuse":
        return f"{TOKEN_BUDGET_BELOW_FLOOR_PREFIX}{verdict.reason}", None
    if verdict.severity == "warn":
        capability_warnings.append(verdict.reason)
    return None, "; ".join(capability_warnings) or None


def _control_path_quarantine_outcome(exec_probe: dict[str, Any]) -> str | None:
    raw = exec_probe.get("raw_payload")
    outcome = raw.get("outcome") if isinstance(raw, dict) else None
    if outcome in ("control_path_quarantined", "control_path_review_required"):
        return outcome
    blockers = exec_probe.get("blockers")
    if isinstance(blockers, list):
        if "control_path_review_required" in blockers:
            return "control_path_review_required"
        if "control_path_quarantine" in blockers:
            return "control_path_quarantined"
    return None


def _execute_turn_was_served(
    *, cumulative_tokens: int, tokens_before: int, exec_probe: dict[str, Any], lane_id: str | None = None
) -> bool:
    """Use token spend or positive host-observed turns; unknown evidence is unserved."""
    if exec_probe.get("handoff_action") == "needs_guidance":
        quarantine_outcome = _control_path_quarantine_outcome(exec_probe)
        if quarantine_outcome is not None:
            raw = exec_probe.get("raw_payload")
            raw = raw if isinstance(raw, dict) else {}
            store = raw.get("control_path_quarantine_store")
            store = store if isinstance(store, dict) else {}
            logger.warning(
                "turn quarantined lane_id=%s outcome=%s store=%s",
                lane_id or "unknown",
                quarantine_outcome,
                store.get("patch_path") or raw.get("control_path_quarantine_store_error") or "unavailable",
            )
            return True
    if cumulative_tokens > tokens_before:
        return True
    from workbay_orchestrator_mcp.orchestration.adapters.remote_exec import _RECEIVER_NUM_TURNS_KEY, _coerce_count

    receiver_turns = exec_probe.get(_RECEIVER_NUM_TURNS_KEY)
    # Turn stamps require integers; the shared count validator also rejects
    # bool, negative, and implausible counts (and otherwise accepts floats).
    count = _coerce_count(receiver_turns) if isinstance(receiver_turns, int) else None
    if count is not None and count > 0:
        return True
    if exec_probe.get("handoff_action") == "needs_guidance":
        reason = (
            "absent" if _RECEIVER_NUM_TURNS_KEY not in exec_probe else "malformed" if count is None else "non-positive"
        )
        logger.warning(
            "needs_guidance turn classed unserved lane_id=%s: no token delta; receiver_num_turns %s",
            lane_id or "unknown",
            reason,
        )
    return False


def run_offload_pass_engine(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    session: str,
    worktree_path: Path,
    backend: str,
    model: str | None = None,
    reasoning_effort: str = "inherit",
    speed: str | None = None,
    tier: str | None = None,
    token_budget: int,
    timeout_seconds: float,
    max_review_cycles: int = 2,
    turn_timeout_seconds: float | None = None,
    grok_max_turns: int | None = None,
    session_mode: str = "fresh_turn",
    dry_run: bool = False,
    pass_id: str | None = None,
    state_dir: Path | None = None,
    test_cmd: str | None = None,
    dispatch_discovery: Any = None,
    capability_receipt_id: str | None = None,
    capability_config_digest: str | None = None,
    dispatch_id: str | None = None,
    detached_completion_job_id: str | None = None,
    detached_completion_start_head: str | None = None,
    canon_proposal_path: str | None = None,
) -> dict[str, Any]:
    # bool is a subclass of int; token_budget=True would pass isinstance(_, int) and
    # run a pass with an effective budget of 1 token. Reject it explicitly.
    if isinstance(token_budget, bool) or not isinstance(token_budget, int) or token_budget <= 0:
        raise ValueError("token_budget must be a positive integer (mandatory, fail-closed).")
    if timeout_seconds is None or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive (mandatory bounded wait).")
    if turn_timeout_seconds is not None and turn_timeout_seconds > timeout_seconds:
        raise ValueError("turn_timeout_seconds must not exceed the pass timeout_seconds.")
    if isinstance(max_review_cycles, bool) or not isinstance(max_review_cycles, int) or max_review_cycles < 1:
        raise ValueError("max_review_cycles must be a positive integer (>=1).")
    if speed is not None:
        profile = OFFLOAD_AGENT_PROFILES.get(backend)
        if profile is None or speed not in profile.allowed_speeds:
            advertised = tuple(profile.allowed_speeds) if profile is not None else ()
            raise ValueError(f"speed {speed!r} is not advertised by {backend!r}; advertised speeds: {advertised}")
    if tier is not None and tier not in ("junior", "senior"):
        raise ValueError("tier must be 'junior' or 'senior'.")

    started = time.monotonic()
    deadline = started + float(timeout_seconds)
    completion_deadline = _ACTIVE_COMPLETION_DEADLINE.get()
    if completion_deadline is not None:
        deadline = min(deadline, completion_deadline)
    wd = _worker_daemon_module()
    resolved_pass_id = pass_id or str(uuid.uuid4())
    from .review_cycle_dispatch import ACTIVE_DISPATCH, canonical_subject, prepare, settle_actual
    review_cycle_receipt = None
    review_cycle_context_token = None

    pass_started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    # Writer (this engine) and reader (await_offload_pass) must share one state_dir;
    # deriving it from orchestrator_root here while the reader uses RuntimeConfig
    # .state_dir is a split-brain (recovery reads a directory nothing was written to).
    resolved_state_dir = Path(state_dir) if state_dir is not None else Path(orchestrator_root) / ".task-state"
    detached_begin = os.environ.get("WORKBAY_REMOTE_EXEC_MODE", "attached") == "detached" and backend.endswith(
        "-remote"
    )
    detached_job_id: str | None = None
    detached_job_attempt: int | None = None
    detached_completion_record: dict[str, Any] | None = None
    # Capture once, before execute can consume the brief or a newer dispatch arrives.
    try:
        original_dispatch_id = _open_dispatch_id(task_ref, lane_id)
    except (OSError, sqlite3.Error):
        original_dispatch_id = None  # Explicitly unknown; dispatch rearming still requires an ID.
    # Bind once before the first immutable source-bearing publication.
    # Detached completion must retain its authenticated dispatch baseline;
    # an unbound legacy completion cannot acquire identity from current HEAD.
    start_head_ref: str | None = detached_completion_start_head
    if start_head_ref is None and detached_completion_job_id is None:
        try:
            start_head_ref = _git_stdout(Path(worktree_path), "rev-parse", "HEAD", deadline=deadline) or None
        except (OSError, RuntimeError, TypeError, ValueError):
            start_head_ref = None  # Early errors remain explicitly unbound.
    rescue_identity: dict[str, Any] = {
        "orchestrator_root": str(orchestrator_root),
        "started_at": pass_started_at,
        "dispatch_id": original_dispatch_id,
        "agent_stream_path": None,
        "worktree_path": str(worktree_path),
        "backend": backend,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "completion_config": {
            "backend": backend,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "speed": speed,
            "tier": tier,
            "token_budget": token_budget,
            "timeout_seconds": timeout_seconds,
            "max_review_cycles": max_review_cycles,
            "turn_timeout_seconds": turn_timeout_seconds,
            "session_mode": session_mode,
            "dry_run": dry_run,
            "pass_id": resolved_pass_id,
            "grok_max_turns": grok_max_turns,
            "test_cmd": test_cmd,
            "capability_receipt_id": capability_receipt_id,
            "capability_config_digest": capability_config_digest,
            "dispatch_id": dispatch_id,
            "start_head_ref": start_head_ref,
        },
    }
    pass_sweep_state: dict[str, Any] = {"terminal_durable": False, "trigger": None}
    # Durable authority this callback accepted at its first source-bearing
    # publication. Engine rewrites never change these fields.
    accepted = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
    publication_authority: dict[str, Any] = {
        "invocation_token": accepted.get("invocation_token"),
        "orchestrator_root": accepted.get("orchestrator_root") or str(orchestrator_root),
    }
    checkpoints: list[str] = []
    # ``checkpoints`` records implementation checkpoint commits only. Review
    # lanes deliberately leave their product uncommitted, so retirement paths
    # need a separate signal for any durable output they must reason about.
    durable_output = False
    verified_gate_evidence: dict[str, Any] | None = None
    dispatch_gate_warnings: list[str] = []
    run_ctx: Any = None
    budget_exhausted = False
    review_defer_reason: str | None = None
    review_defer_retries: int | None = None
    producer_question_count = 0

    # Token-governance mode, resolved once (internal).
    # A backend that emits token telemetry is governed by token_budget; one that
    # does not (grok-cli) is governed by the deadline + turn bounds, and the
    # downgrade is surfaced in every result payload so it is never silent.
    from workbay_orchestrator_mcp.orchestration.backend_registry import (  # noqa: PLC0415
        backend_supports_token_telemetry,
    )

    token_telemetry_supported = backend_supports_token_telemetry(backend)
    token_governance: dict[str, Any] = {
        "mode": "token_budget" if token_telemetry_supported else "degraded_turn_time",
        "enforced_by": "token_budget" if token_telemetry_supported else "turn_time_bounds",
        "token_telemetry": token_telemetry_supported,
    }

    def _live_token_telemetry_supported() -> bool:
        """Re-read telemetry support from the backend the pass is actually on.

        ``token_telemetry_supported`` is captured at dispatch. Execute may
        mutate ``run_ctx.backend`` via MCP_BACKEND_OVERRIDE; the payload
        path already re-resolves, and the governor gates must too.
        """
        live_backend = getattr(run_ctx, "backend", None) if run_ctx is not None else None
        if live_backend:
            return backend_supports_token_telemetry(str(live_backend))
        return token_telemetry_supported

    def _require_publication_authority() -> None:
        """Refuse once this callback's accepted durable authority has changed.

        A plain file read: no lease or write lock is held across execution.
        """
        durable = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
        changed = [key for key, value in publication_authority.items() if durable.get(key) != value]
        if changed:
            raise _PassAuthorityChanged(changed)

    def _publish_running_state(payload: dict[str, Any]) -> None:
        # The accepted token must reach the locked comparison at every write;
        # an earlier authority read cannot protect a later publication.
        if not write_pass_state(
            resolved_state_dir,
            resolved_pass_id,
            payload,
            expected_invocation_token=publication_authority["invocation_token"],
        ):
            raise _PassAuthorityChanged(("invocation_token",))

    _unset_model_call_boundary = object()

    def _persist_running_progress(*, model_call_in_flight: Any = _unset_model_call_boundary) -> None:
        """Write the latest question and observed-token progress before advancing."""

        if dry_run or run_ctx is None:
            return
        previous = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
        previous_progress = previous.get("progress")
        progress = dict(previous_progress) if isinstance(previous_progress, dict) else {}
        telemetry_supported = _live_token_telemetry_supported()
        try:
            cumulative_tokens = max(0, int(getattr(run_ctx, "cumulative_tokens", 0) or 0))
        except (TypeError, ValueError):
            cumulative_tokens = 0
        tokens_observed = progress.get("tokens_observed") is True
        if telemetry_supported and cumulative_tokens > 0:
            tokens_observed = True
        live_backend = str(getattr(run_ctx, "backend", None) or backend)
        progress.update(
            {
                "producer_question_count": max(0, int(producer_question_count)),
                "token_budget": token_budget,
                "token_telemetry_supported": telemetry_supported,
                "tokens_observed": tokens_observed,
                "observed_tokens": cumulative_tokens if tokens_observed else None,
                "backend": live_backend,
            }
        )
        if model_call_in_flight is not _unset_model_call_boundary:
            progress["model_call_in_flight"] = model_call_in_flight
        _publish_running_state(
            {
                **previous,
                **rescue_identity,
                "status": "running",
                "task_ref": task_ref,
                "lane_id": lane_id,
                "session": session,
                "orchestrator_root": str(orchestrator_root),
                "progress": progress,
            },
        )

    def _persist_inflight_progress_observation() -> dict[str, Any]:
        """Refresh observed usage without erasing an unfinished call boundary."""
        current = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
        progress_value = current.get("progress")
        progress = dict(progress_value) if isinstance(progress_value, dict) else {}
        if isinstance(progress.get("model_call_in_flight"), dict) and run_ctx is not None:
            # An exception can occur after the worker has updated its local usage
            # counter. Persist that lower bound while retaining the write-ahead
            # marker that says the call did not return successfully.
            _persist_running_progress()
            current = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
            progress_value = current.get("progress")
            progress = dict(progress_value) if isinstance(progress_value, dict) else {}
        return progress

    # Capture discovery once at pass start (preflight's cached result when warm)
    # and thread that object into every outcome receipt. Re-resolving after
    # MODEL_DISCOVERY_TTL_S would drop a failed-probe warning and can disagree
    # with the weights that actually ran.
    captured_discovery = capture_pass_model_discovery(backend, discovery=dispatch_discovery)
    persisted_escalation_history: dict[str, Any] = {
        "turns": 0,
        "rounds": 0,
        "tokens": 0,
        "tokens_complete": True,
        "escalated_from": None,
        "pass_ids": [],
        "history_complete": None,
        "doubtful_pass_ids": [],
        "abandoned_pass_ids": [],
    }
    pass_process_ran = False
    pass_routing_effective: dict[str, Any] | None = None
    pass_originating_routing: dict[str, Any] | None = None
    pass_observed_model: str | None = None
    pass_routing_history: list[dict[str, Any]] = []

    def _build_tokens_payload(run_ctx: Any) -> dict[str, Any]:
        return _build_token_telemetry_payload(
            run_ctx,
            backend=backend,
            lane_id=lane_id,
            token_budget=token_budget,
            token_telemetry_supported=token_telemetry_supported,
        )

    def _compute_commit_landed(*, since: str | None = None) -> bool:
        """True when HEAD advanced since ``since``, or since pass start if omitted.

        Pass-scoped (default) is for the outcome payload. The post-turn budget
        gate must pass this cycle's pre-execute HEAD so an earlier cycle's
        commit cannot mint ``completed_unreviewed`` for a turn that landed
        nothing.
        """
        baseline = start_head_ref if since is None else since
        if since is None and checkpoints:
            return True
        if not baseline:
            return False
        try:
            head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        return bool(head) and head != baseline

    def _capture_agent_stream() -> None:
        # remote_exec attributes the spool via raw_payload.result_path. Read it
        # even on execute's early-return/exception paths, before terminal publish.
        final_path = getattr(run_ctx, "final_result_path", None)
        if not final_path:
            return
        try:
            probe = wd._load_result(Path(final_path))
        except (OSError, ValueError, RuntimeError):
            return
        raw = probe.get("raw_payload") if isinstance(probe, dict) else None
        result_path = raw.get("result_path") if isinstance(raw, dict) else None
        if isinstance(result_path, str) and result_path:
            rescue_identity["agent_stream_path"] = str(Path(result_path).with_name("agent-stream.jsonl"))

    def _payload(
        outcome: str,
        *,
        run_ctx: Any = None,
        error: str | None = None,
        slice_closure: dict[str, Any] | None = None,
        self_verify: dict[str, Any] | None = None,
        post_review_gate: dict[str, Any] | None = None,
        composer_violation: dict[str, Any] | None = None,
        continuation_dispatch_id: str | None = None,
        failed_stage: str | None = None,
        reason: str | None = None,
        commit_landed: bool | None = None,
        review: str | None = None,
        findings: list[dict[str, Any]] | None = None,
        task_open_blockers: list[dict[str, Any]] | None = None,
        findings_scope: dict[str, Any] | None = None,
        raw_tail: str | None = None,
        receipt_turns: int = 0,
        receipt_rounds: int = 0,
        receipt_escalated_from: dict[str, Any] | None = None,
        receipt_prior_tokens: int = 0,
        receipt_prior_tokens_complete: bool = True,
        receipt_high_findings: int = 0,
        job_id: str | None = None,
        attempt: int | None = None,
    ) -> dict[str, Any]:
        failed_stage = _payload_failed_stage(failed_stage)
        _capture_agent_stream()
        pass_progress = _persist_inflight_progress_observation()
        model_call_marker = pass_progress.get("model_call_in_flight")
        model_call_incomplete_reason = None
        if isinstance(model_call_marker, dict):
            model_call_incomplete_reason = {
                "code": "model_call_failed_in_flight",
                "phase": str(model_call_marker.get("phase") or "unknown"),
                "cycle": model_call_marker.get("cycle"),
            }
        effective_effort = getattr(run_ctx, "execution_effective_effort", None)
        landed = _compute_commit_landed() if commit_landed is None else bool(commit_landed)
        result_backend = getattr(run_ctx, "backend", None) or backend
        result_model = getattr(run_ctx, "model", None) or model
        final_result = _safe_final_result(run_ctx) if run_ctx is not None else {}
        raw_result = final_result.get("raw_payload")
        raw_result = raw_result if isinstance(raw_result, dict) else {}
        routing_effective = raw_result.get("routing_effective")
        if not isinstance(routing_effective, dict):
            candidate = final_result.get("routing_effective")
            routing_effective = candidate if isinstance(candidate, dict) else None
        current_process_ran = bool(getattr(run_ctx, "current_process_ran", False))
        process_ran = pass_process_ran or current_process_ran
        if pass_process_ran:
            routing_effective = dict(pass_routing_effective) if pass_routing_effective else None
        observed_turns = max(0, int(receipt_turns))
        if not process_ran:
            routing_effective = None
        if process_ran and routing_effective is None:
            # Local adapters do not write a remote spec.  The mutable run
            # context is nevertheless the cycle-boundary value that executed.
            routing_effective = {
                "backend": str(result_backend) if result_backend else None,
                "model": str(result_model) if result_model else None,
                "effort": (
                    effective_effort if effective_effort and effective_effort != "inherit" else reasoning_effort
                ),
                "speed": getattr(run_ctx, "speed", None),
                "tier": getattr(run_ctx, "tier", None),
            }
        effective_escalated_from = receipt_escalated_from
        if (
            effective_escalated_from is None
            and pass_originating_routing is not None
            and routing_effective is not None
            and pass_originating_routing != routing_effective
        ):
            effective_escalated_from = dict(pass_originating_routing)
        observed_model = (
            pass_observed_model if pass_process_ran else final_result.get("response_model") or final_result.get("model")
        )
        if observed_model is None and process_ran and routing_effective is not None:
            observed_model = routing_effective.get("model")
        accepted_output_tokens = None
        cumulative_tokens = int(getattr(run_ctx, "cumulative_tokens", 0) or 0)
        observed_tokens_lower_bound = (
            cumulative_tokens if model_call_incomplete_reason is not None and cumulative_tokens > 0 else None
        )
        total_accepted_tokens = max(0, int(receipt_prior_tokens)) + cumulative_tokens
        if outcome == "handoff_ready" and landed and receipt_prior_tokens_complete and total_accepted_tokens > 0:
            accepted_output_tokens = total_accepted_tokens
        dispatch_receipt = outcome_dispatch_receipt(
            str(result_backend) if result_backend else "",
            served_model=str(observed_model) if observed_model else None,
            discovery=captured_discovery,
            process_ran=process_ran,
            routing_effective=routing_effective,
            turns=observed_turns,
            rounds=receipt_rounds,
            escalated_from=effective_escalated_from,
            accepted_output_tokens=accepted_output_tokens,
            high_finding_count=receipt_high_findings,
            observed_tokens=cumulative_tokens,
            routing_history=[
                *(dict(segment) for segment in persisted_escalation_history.get("routing_history", ())),
                *(dict(segment) for segment in pass_routing_history),
            ],
            tokens_complete=receipt_prior_tokens_complete and model_call_incomplete_reason is None,
        )
        tokens_payload = _build_tokens_payload(run_ctx)
        if model_call_incomplete_reason is not None:
            tokens_payload["tokens_complete"] = False
            tokens_payload["tokens_incomplete_reason"] = dict(model_call_incomplete_reason)
            if observed_tokens_lower_bound is None:
                if tokens_payload.get("token_telemetry") is True:
                    tokens_payload["cumulative_total"] = None
                if dispatch_receipt is not None:
                    dispatch_receipt.pop("observed_tokens", None)
            else:
                tokens_payload["observed_tokens_lower_bound"] = observed_tokens_lower_bound
                if dispatch_receipt is not None:
                    dispatch_receipt["observed_tokens_lower_bound"] = observed_tokens_lower_bound
            if dispatch_receipt is not None:
                dispatch_receipt["tokens_incomplete_reason"] = dict(model_call_incomplete_reason)
        style_debt, style_advisories = _extract_style_telemetry(post_review_gate, self_verify)
        self_verify_gate = _self_verify_gate_status(self_verify if isinstance(self_verify, dict) else None)
        next_wave_records: list[dict[str, Any]] = []
        next_wave_section = ""
        try:
            from workbay_orchestrator_mcp.orchestration.style_debt import (  # noqa: PLC0415
                next_wave_style_debt,
                render_style_debt_section,
            )

            carry_forward = next_wave_style_debt(resolved_state_dir, task_ref=task_ref)
            next_wave_records = [record.to_dict() for record in carry_forward]
            next_wave_section = render_style_debt_section(carry_forward)
        except Exception as exc:  # noqa: BLE001 - carry-forward is additive telemetry
            logger.warning("style debt carry-forward unavailable for %s/%s: %s", task_ref, lane_id, exc)
        result: dict[str, Any] = {
            **rescue_identity,
            "outcome": outcome,
            # WorkBay review/result consumers require this key even on timeout,
            # needs-guidance, and early preflight terminals.
            "summary": _pass_result_summary(final_result, outcome=outcome, error=error, reason=reason),
            "pass_id": resolved_pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "backend": result_backend,
            "model": result_model,
            "reasoning_effort": (
                effective_effort if effective_effort and effective_effort != "inherit" else reasoning_effort
            ),
            "tokens": tokens_payload,
            "token_governance": token_governance,
            "checkpoint_commits": list(checkpoints),
            "slice_closure": slice_closure if slice_closure is not None else {"recorded": False},
            "wall_seconds": round(time.monotonic() - started, 2),
            "retry_policy": "never_in_engine; recover via a new idempotent dispatch (dispatch_id)",
            # internal: always present so the gate branches without git archaeology.
            "commit_landed": landed,
            # Dual-axis discriminators (engoutcome): always populated so a caller
            # can tell work-landed+ceremony-failed from pure error without prose.
            "work_status": _work_status_for(landed),
            "ceremony_status": _ceremony_status_for(run_ctx),
            "failed_stage": failed_stage,
            # T4: always surface worker findings (empty list when none / listing failed).
            "findings": findings if findings is not None else [],
            # ``ok`` is a lane result, not a transport receipt.  A transport
            # must never fill this hole for an unverified or non-success pass.
            "ok": _pass_ok_from_outcome_and_self_verify_gate(outcome, self_verify_gate),
            "merge_status": "merge_ready" if outcome == "handoff_ready" else "not_merge_ready",
            "producer_question_count": producer_question_count,
            "history_complete": persisted_escalation_history.get("history_complete"),
            "doubtful_pass_ids": sorted(persisted_escalation_history.get("doubtful_pass_ids", ())),
            "abandoned_pass_ids": sorted(persisted_escalation_history.get("abandoned_pass_ids", ())),
            "style_debt": style_debt,
            "style_debt_reported": style_debt is not None,
            "style_advisories": style_advisories,
            "next_wave_style_debt": next_wave_records,
            "next_wave_style_debt_section": next_wave_section,
        }
        if task_open_blockers is not None:
            result["task_open_blockers"] = list(task_open_blockers)
        if findings_scope is not None:
            result["findings_scope"] = dict(findings_scope)
        if job_id is not None:
            result["job_id"] = job_id
        if attempt is not None:
            result["attempt"] = attempt
        if model_call_incomplete_reason is not None:
            result["token_accounting"] = "observed" if observed_tokens_lower_bound is not None else "unknown"
        # Preserve provisioning evidence on every receipt path, including an
        # admission refusal before execute (CARD-07 / fail-loudly).
        execute_result = _safe_final_result(run_ctx)
        if "blockers" in execute_result:
            result["blockers"] = execute_result["blockers"]
        context_blockers = getattr(run_ctx, "worker_blockers", None)
        if isinstance(context_blockers, list) and context_blockers:
            result["blockers"] = list(context_blockers)
        raw = execute_result.get("raw_payload") if isinstance(execute_result, dict) else None
        if isinstance(raw, dict):
            for key in (
                "failure_kind",
                "breaker_key",
                "retry_after_s",
                "failure_class",
                "retryable",
                "writable_roots_probe",
                "defer_reason",
            ):
                if key in raw:
                    result[key] = raw[key]
            # Remote dirty-work evidence is adapter-owned and must survive the
            # pass envelope instead of collapsing to the generic not_landed
            # status. Copy only the typed remainder fields emitted by the
            # adapter's durable local artifact boundary [DATA-13][PROV-01].
            if raw.get("work_status") == WORK_STATUS_UNCOMMITTED_SALVAGED:
                result["work_status"] = WORK_STATUS_UNCOMMITTED_SALVAGED
                for key in (
                    "uncommitted_remainder_present",
                    "uncommitted_remainder_path",
                    "uncommitted_remainder_bytes",
                ):
                    if key in raw:
                        result[key] = raw[key]
        if budget_exhausted:
            result["budget_exhausted"] = True
        if dispatch_receipt is not None:
            result["dispatch_receipt"] = dispatch_receipt
            result["resolved_model"] = dispatch_receipt.get("resolved_model")
            if dispatch_receipt.get("discovery_warning"):
                result.setdefault("warnings", [])
                if isinstance(result["warnings"], list):
                    result["warnings"].append(dispatch_receipt["discovery_warning"])
        if dispatch_gate_warnings:
            result.setdefault("warnings", [])
            if isinstance(result["warnings"], list):
                result["warnings"].extend(dispatch_gate_warnings)
        if error is not None:
            result["error"] = error
        # implementation note R9: mark a backend-internal transient error so the coordinator
        # re-dispatches mechanically instead of doing log forensics.
        if outcome == "error" and _is_backend_transient_error(error):
            result["backend_transient"] = True
        if reason is not None:
            result["reason"] = reason
        # E8FIX-A-03: surface the typed execute stop reason on every payload so
        # operators/gates can branch without re-parsing free-form error text.
        stop_reason_payload = getattr(run_ctx, "execute_stop_reason", None) if run_ctx is not None else None
        if stop_reason_payload:
            result["execute_stop_reason"] = str(stop_reason_payload)
        if self_verify is not None:
            result["self_verify"] = self_verify
            if isinstance(self_verify, dict) and "exit_code" in self_verify:
                result["self_verify_exit_code"] = self_verify.get("exit_code")
        if verified_gate_evidence is not None:
            result["verified_gate_evidence"] = dict(verified_gate_evidence)
        # Promote typed gate status so unrun vs red is readable from the pass
        # surface alone (findings 3 and 4) without parsing output_tail prose or
        # opening selfverify.json. Stamped unconditionally [PIPE-SV-01]: a pass
        # that never reached the phase reports ``not_reached`` rather than
        # omitting the key, so "unverified" is a value the surface carries
        # instead of a silence a reader has to interpret. [OBS-08]
        result["self_verify_gate"] = self_verify_gate
        if composer_violation is not None:
            result["composer_violation"] = composer_violation
        if continuation_dispatch_id is not None:
            result["continuation_dispatch_id"] = continuation_dispatch_id
        if review is not None:
            result["review"] = review
        if raw_tail is not None:
            result["raw_tail"] = raw_tail
        return result

    def _finish(result: dict[str, Any]) -> dict[str, Any]:
        if review_cycle_receipt is not None:
            cycle_final = _safe_final_result(run_ctx) if run_ctx is not None else {}
            cycle_raw = cycle_final.get("raw_payload") or {}
            cycle_result_path = cycle_raw.get("result_path") if isinstance(cycle_raw, dict) else None
            try:
                cycle_judgment = bool(cycle_result_path and Path(cycle_result_path).is_file()
                                      and Path(cycle_result_path).stat().st_size)
            except OSError:
                cycle_judgment = True  # an unreadable producer artifact is unknown
            settled = settle_actual(
                Path(worktree_path), review_cycle_receipt["identity"], outcome=result["outcome"],
                model_started=bool((result.get("dispatch_receipt") or {}).get("process_ran")),
                judgment=bool(pass_process_ran) or cycle_judgment, commit_landed=bool(result.get("commit_landed")),
                refusal_reason=result.get("reason"), harvest=result.get("findings_harvest"),
            )
            result["review_cycle"] = {"reservation": review_cycle_receipt["decision"],
                                      "settlement": asdict(settled)}
        # Persist the bare failure first so an advisory lookup (even a hung
        # embedding provider) cannot leave the pass `running`. Then attach
        # under a wall-clock bound and republish if the annotation landed.
        terminal_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        current_state = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
        progress_value = current_state.get("progress")
        terminal_state = {
            **rescue_identity,
            "status": "done",
            "task_ref": task_ref,
            "lane_id": lane_id,
            "session": session,
            "terminal_at": terminal_at,
            "result": result,
        }
        for key in ("job_id", "attempt"):
            if current_state.get(key) is not None:
                terminal_state[key] = current_state[key]
        if isinstance(progress_value, dict):
            # Keep the write-ahead marker visible after terminal publication so
            # receipt history can detect a call that never returned.
            terminal_state["progress"] = dict(progress_value)
        if not write_pass_state(
            resolved_state_dir,
            resolved_pass_id,
            terminal_state,
            expected_invocation_token=publication_authority["invocation_token"],
        ):
            return result
        if result.get("outcome") in {"review_complete", "review_incomplete", "needs_guidance"}:
            retirement = _retire_review_lane_at_pass_end(
                repo_root=Path(orchestrator_root),
                task_ref=task_ref,
                lane_id=lane_id,
            )
            if retirement.get("reason") != "not_review_lane":
                result["review_lane_retirement"] = retirement
                result["lane_status_transition"] = retirement
                result["lane_status"] = retirement.get("status")
        pass_sweep_state["terminal_durable"] = True
        # A terminal pass is itself a sweep opportunity, even when it did not
        # land a merge. Keep the status=done write above as the commit marker:
        # the sweep must never observe a pass whose terminal record is not yet
        # durable. A merge-close caller may already have attached this result
        # to the pass payload; in that case its single trigger is authoritative.
        trigger_result = pass_sweep_state.get("trigger")
        if not isinstance(trigger_result, dict):
            trigger_result = result.get("retire_sweep_trigger")
        if not isinstance(trigger_result, dict):
            trigger_result = _trigger_background_retire_sweep(Path(orchestrator_root))
        pass_sweep_state["trigger"] = dict(trigger_result)
        result["retire_sweep_trigger"] = dict(trigger_result)
        try:
            # Persist the spawn result too; the first terminal write remains
            # the ordering barrier observed by the detached sweep.
            if not write_pass_state(
                resolved_state_dir,
                resolved_pass_id,
                terminal_state,
                expected_invocation_token=publication_authority["invocation_token"],
            ):
                return result
        except Exception as exc:  # noqa: BLE001 — telemetry failure cannot change pass outcome
            logger.warning(
                "failed to persist terminal pass retire sweep result for %s/%s: %s",
                task_ref,
                lane_id,
                exc,
            )
        if result.get("reason") == "abandoned_pass_question_count_unknown":
            unknown_pass_ids = result.get("abandoned_pass_question_count_unknown_pass_ids", ())
            if isinstance(unknown_pass_ids, (list, tuple)):
                _mark_abandoned_question_count_escalated(resolved_state_dir, unknown_pass_ids)
        _attach_failure_prior_art(result)
        if "failure_prior_art" in result:
            write_pass_state(
                resolved_state_dir,
                resolved_pass_id,
                terminal_state,
                expected_invocation_token=publication_authority["invocation_token"],
            )
        return result

    def _execute_pass() -> dict[str, Any]:
        nonlocal run_ctx, start_head_ref, budget_exhausted, persisted_escalation_history, durable_output
        nonlocal review_defer_reason, review_defer_retries
        nonlocal detached_job_id, detached_job_attempt
        nonlocal verified_gate_evidence
        nonlocal review_cycle_receipt, review_cycle_context_token
        nonlocal producer_question_count
        nonlocal pass_process_ran, pass_routing_effective, pass_originating_routing, pass_observed_model
        _finalize_abandoned_passes(
            resolved_state_dir,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            current_pass_id=resolved_pass_id,
            orchestrator_root=Path(orchestrator_root),
        )
        # implementation note R7: per-pass engine self-integrity check. Refuse loudly with a
        # typed server_stale_restart_required outcome when the engine's own source
        # vanished since import, rather than crashing later on the lane_prompt.py
        # spawn (the 0108 concurrent-env-flip incident).
        integrity_note = _engine_source_integrity_note()
        if integrity_note is not None:
            return _payload("server_stale_restart_required", error=integrity_note)
        lane_state = wd.poll_lane_state(
            orchestrator_root=Path(orchestrator_root),
            task_ref=task_ref,
            lane_id=lane_id,
            worktree_path=Path(worktree_path),
        )
        if lane_state != "actionable":
            return _payload("no_actionable_work", error=f"lane state: {lane_state}; record a brief first")

        try:
            check_disk_headroom(Path(worktree_path), cost_class=cost_class_for_backend(backend))
        except OffloadPreflightError as exc:
            return _payload(exc.outcome, error=str(exc))

        # implementation note R3: an operator-declared review lane disambiguates the clean-tree
        # + needs_guidance shape (a completed review) from a wedged transport failure.
        # implementation note S4 / cs0166-r07-10: confirmed only when MCP params carry an
        # explicit kind. Unresolved → typed error (never fabricate implement).
        lane_kind, lane_kind_confirmed = _lane_kind(Path(orchestrator_root), task_ref, lane_id)
        if not lane_kind_confirmed or lane_kind not in ("implement", "review"):
            return _payload(
                "error",
                error=(
                    "lane_kind unavailable: MCP params did not resolve an explicit "
                    "implement|review kind (fail-closed; refusing implement default)"
                ),
                failed_stage="execute",
            )
        from workbay_orchestrator_mcp.orchestration.wave_dispatch import LaneSpec, LaneSpecError

        try:
            rescue_identity["lane_spec"] = asdict(
                LaneSpec(
                    lane_id=lane_id,
                    backend=backend,
                    model=model or "",
                    effort=reasoning_effort,
                    speed=speed,
                    token_budget=token_budget,
                    timeout_seconds=timeout_seconds,
                    lane_kind=lane_kind,
                )
            )
        except (TypeError, ValueError, LaneSpecError):
            # Preserve existing admission outcomes; rescue fails closed without a spec.
            pass
        _require_publication_authority()
        _publish_running_state(
            {**rescue_identity, "status": "running", "task_ref": task_ref, "lane_id": lane_id},
        )
        review_lane = lane_kind == "review"
        if backend.endswith("-remote") and not dry_run:
            try:
                cycle_branch = _git_stdout(Path(worktree_path), "rev-parse", "--abbrev-ref", "HEAD")
                cycle_subject = canonical_subject(
                    Path(worktree_path), resolved_state_dir, task_ref=task_ref, branch=cycle_branch,
                    pass_id=resolved_pass_id, orchestrator_root=Path(orchestrator_root),
                )
                cycle_id = detached_completion_job_id or detached_job_id or dispatch_id or resolved_pass_id
                if detached_completion_job_id or detached_completion_record is not None:
                    from .review_cycle_dispatch import _receipt_path
                    review_cycle_receipt = json.loads(_receipt_path(Path(worktree_path), cycle_id).read_text())
                    if review_cycle_receipt["identity"]["subject_branch"] != cycle_subject:
                        raise ValueError("review_cycle_completion_subject_mismatch")
                else:
                    review_cycle_receipt = prepare(
                        Path(worktree_path), dispatch_id=cycle_id, subject_branch=cycle_subject,
                        kind="gate" if review_lane else "implement", canon_proposal_path=canon_proposal_path,
                        settlement_owner="engine",
                    )
                if review_cycle_receipt["decision"]["status"] != "reserved":
                    refused = review_cycle_receipt["decision"]
                    review_cycle_receipt = None
                    return _payload("dispatch_refused", reason=refused["status"], error=refused["reason"])
                review_cycle_context_token = ACTIVE_DISPATCH.set(review_cycle_receipt)
                rescue_identity.update(review_cycle_dispatch_id=cycle_id, review_cycle_subject_branch=cycle_subject,
                                       review_cycle=review_cycle_receipt["decision"])
            except (OSError, ValueError, KeyError, RuntimeError) as exc:
                return _payload("dispatch_refused", reason="ledger_hold", error=str(exc))
        guidance_contract_invalid = False
        findings_harvest: dict[str, Any] | None = None
        # True when this pass used harvest as the *review product* classifier
        # (review_branch_applies). Only then is harvest_failed forbidden from
        # backfilling pre-existing open findings into the payload.
        review_product_harvest = False

        def _review_product_is_durable(candidate: Any) -> bool:
            if not review_lane or not isinstance(candidate, dict):
                return False
            # A review result is durable before terminal harvest only when its
            # blocked handoff actually landed. Failed-submit evidence can still
            # be harvested on a clean review path, but dirty/unharvested review
            # scratch must not become ceremony-landed output.
            if getattr(run_ctx, "handoff_exit", None) != 0:
                return False
            has_findings = isinstance(candidate.get("findings"), (list, tuple))
            has_findings_marker = any(
                isinstance(candidate.get(key), str) and "GROK_REVIEW_FINDINGS_JSON" in candidate[key]
                for key in ("details", "summary")
            )
            raw_payload = candidate.get("raw_payload")
            if isinstance(raw_payload, dict):
                has_findings_marker = has_findings_marker or any(
                    isinstance(raw_payload.get(key), str) and "GROK_REVIEW_FINDINGS_JSON" in raw_payload[key]
                    for key in ("details", "summary")
                )
            if not (has_findings or candidate.get("review_result_carrier") is True or has_findings_marker):
                return False
            from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                review_result_is_harvestable,
            )

            return review_result_is_harvestable(candidate)

        def _mark_durable_output(candidate: Any = None) -> None:
            nonlocal durable_output
            if checkpoints or _review_product_is_durable(candidate):
                durable_output = True
            if not start_head_ref:
                return
            try:
                current_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
            except (OSError, RuntimeError, TypeError, ValueError):
                return
            if current_head not in ("", start_head_ref):
                durable_output = True

        # Resolve the test command before sandbox/worktree work so the remote
        # implement dispatch gate can fail closed on the inputs (SECD-05 / FM-08).
        from workbay_orchestrator_mcp.orchestration.lane_test_cmd import (  # noqa: PLC0415
            classify_test_cmd,
        )

        # A cleared command (empty string) means run no test command at all.
        # That is distinct from not-supplied (None), which leaves the stored
        # row in place for later consumers. Both states go through normalize:
        # that helper repairs entity-encoded && only and does not collapse a
        # clear back to absent.
        # classify_test_cmd raises ValueError for the exact shell no-op 'true'
        # (V66TRI-08b4). Map that admission failure to dispatch_refused instead
        # of letting the outer crash handler launder it into outcome="error".
        try:
            _cmd_state, _cmd_text = classify_test_cmd(test_cmd)
        except ValueError as exc:
            return _payload(
                "dispatch_refused",
                error=f"{TEST_CMD_UNUSABLE_PREFIX}{exc}",
                failed_stage=None,
            )
        if _cmd_state == "held":
            raw_test_cmd = _cmd_text
        elif _cmd_state == "cleared":
            raw_test_cmd = ""
        else:
            raw_test_cmd = None
        # Typed outcome for an un-repairable test_cmd (OFFP2B-R1-01): never let
        # normalize ValueError reach the outer ``except Exception`` that would
        # launder it into ``outcome="error", error="offload pass crashed: ..."``.
        try:
            resolved_test_cmd = normalize_lane_test_cmd(raw_test_cmd)
        except ValueError as exc:
            return _payload(
                "error",
                error=f"test_cmd unrepairable: {exc}",
                failed_stage="execute",
            )
        refuse_error, budget_warning = _remote_implement_input_gate(
            repository_root=Path(orchestrator_root),
            lane_kind=lane_kind,
            backend=backend,
            test_cmd=resolved_test_cmd,
            token_budget=token_budget,
            model=model,
        )
        if refuse_error is not None:
            return _payload("dispatch_refused", error=refuse_error, failed_stage=None)
        if budget_warning is not None:
            dispatch_gate_warnings.append(budget_warning)

        # implementation note S3 [OBS-08]/T3]: ensure lane manifest before execute/bootstrap
        # (auto-materialize when possible; named error mentions materialize_*).
        try:
            branch_name = _git_stdout(Path(worktree_path), "rev-parse", "--abbrev-ref", "HEAD") or ""
            rescue_identity["branch"] = branch_name if branch_name != "HEAD" else None
            _require_publication_authority()
            _publish_running_state(
                {**rescue_identity, "status": "running", "task_ref": task_ref, "lane_id": lane_id},
            )
            # A concrete pass speed is not written onto the lane manifest.
            # Speed-less profiles still pass an explicit empty speed so the
            # partial caller pin can resolve without inventing a tier
            # (codex-subagent and any profile whose allowed_speeds is empty).
            manifest_kwargs: dict[str, str] = {}
            if speed is None:
                speed_profile = OFFLOAD_AGENT_PROFILES.get(backend)
                if speed_profile is not None and not speed_profile.allowed_speeds:
                    manifest_kwargs["preferred_speed"] = ""
            elif speed == "":
                manifest_kwargs["preferred_speed"] = ""
            manifest_ensure = ensure_lane_manifest_for_offload(
                orchestrator_root=Path(orchestrator_root),
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
                branch=branch_name if branch_name != "HEAD" else None,
                preferred_backend=backend,
                preferred_model=model,
                auto_materialize=True,
                **manifest_kwargs,
            )
        except _PassAuthorityChanged:
            raise
        except Exception as exc:  # noqa: BLE001 — never crash the pass on preflight glue
            return _payload(
                "error",
                error=f"no manifest for {lane_id}; run materialize_offload_lane_manifest ({exc})",
                failed_stage="execute",
            )
        if not manifest_ensure.get("ok"):
            # S2R-4: a typed policy refusal (remote_required) from the ensure
            # path keeps its discriminator instead of collapsing to error.
            _ensure_outcome = str(manifest_ensure.get("outcome") or "error")
            refusal_details = {}
            if _ensure_outcome in {"manifest_unreadable_would_drop_lanes", "manifest_quarantine_failed"}:
                # Manifest policy refusals share the public dispatch discriminator.
                refusal_details["reason"] = manifest_ensure.get("reason") or _ensure_outcome
                _ensure_outcome = "dispatch_refused"
            return _payload(
                _ensure_outcome,
                error=str(
                    manifest_ensure.get("error") or f"no manifest for {lane_id}; run materialize_offload_lane_manifest"
                ),
                failed_stage="execute" if _ensure_outcome == "error" else None,
                **refusal_details,
            )

        # Worker end-state baselines: closure is recorded only from a commit and a
        # worker report produced DURING this pass (freshness gate, PR-10).
        start_head = start_head_ref
        baseline_report_id = _max_worker_report_id(task_ref, lane_id)
        baseline_test_id = _max_verified_test_id(task_ref)

        # Union predicate: BOTH bounded families accept a per-cycle wall clock.
        # Gating on the grok-only predicate silently dropped the caller's
        # turn_timeout_seconds for the wall-clock-only family, so preflight's
        # "governed by turn/time bounds" note described a bound nothing applied.
        # ...but they carry it in DIFFERENT fields: the wall-clock-only family
        # uses adapter_timeout so its agent bound never becomes the local
        # TEST_CMD self-verify deadline (which reads grok_timeout).
        _cycle_timeout = int(turn_timeout_seconds) if turn_timeout_seconds else None
        grok_timeout = _cycle_timeout if backend_supports_token_budget_cycle_bounds(backend) else None
        adapter_timeout = _cycle_timeout if backend_supports_adapter_timeout_bounds(backend) else None
        # Converge a repaired command after the dispatch gate so a refused
        # input never writes. Peel still happens before bash -lc / --test-cmd.
        if resolved_test_cmd is not None and raw_test_cmd is not None and resolved_test_cmd != raw_test_cmd:
            _converge_stored_lane_test_cmd(
                task_ref=task_ref,
                lane_id=lane_id,
                test_cmd=resolved_test_cmd,
            )
        # Mint a worktree-bound receipt for *-remote backends from the
        # resolved cycle bounds the worker will carry. Ambient
        # WORKBAY_CAPABILITY_RECEIPT_ID is never a substitute. dry_run
        # still checks transport/identity (missing worktree transport is a
        # CARD-07 attestation failure) but does not persist a receipt or
        # enter worker setup — the attestation store is one current record.
        from workbay_orchestrator_mcp.orchestration.lane_routing import (  # noqa: PLC0415
            normalize_speed,
        )
        from workbay_orchestrator_mcp.orchestration.worker_daemon_ctl import (  # noqa: PLC0415
            WorktreeCapabilityReceiptRefusal,
            is_remote_worker_backend,
            mint_worktree_capability_receipt,
        )

        worker_receipt_id = (
            capability_receipt_id.strip()
            if isinstance(capability_receipt_id, str) and capability_receipt_id.strip()
            else None
        )
        worker_config_digest = (
            capability_config_digest.strip()
            if isinstance(capability_config_digest, str) and capability_config_digest.strip()
            else None
        )
        worker_dispatch_id = (
            dispatch_id.strip()
            if isinstance(dispatch_id, str) and dispatch_id.strip()
            else f"offload-pass:{task_ref}:{lane_id}:{resolved_pass_id}"
        )
        if not is_remote_worker_backend(backend):
            worker_receipt_id = None
            worker_config_digest = None
            worker_dispatch_id = None
        # A caller-supplied turn clock is the MODEL bound; this pass's deadline
        # stays the whole remote lifecycle (setup, model, verify, harvest).
        # Carry that one monotonic deadline to the adapter so turn_timeout_seconds
        # never becomes the whole-job/ledger bound (internal).
        # Turn-bounded (max_turns) families keep their established contract.
        remote_phase_budget = (
            RemotePhaseBudget.start(timeout_seconds, deadline_monotonic=deadline)
            if adapter_timeout is not None and turn_timeout_seconds and is_remote_worker_backend(backend)
            else None
        )
        canonical_speed = normalize_speed(backend, speed)
        config = wd.WorkerConfig(
            orchestrator_root=Path(orchestrator_root),
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            worktree_path=Path(worktree_path),
            pass_id=resolved_pass_id,
            max_review_cycles=max_review_cycles,
            single_pass=True,
            backend=backend,
            session_mode=session_mode,
            reasoning_effort=reasoning_effort,
            speed=canonical_speed,
            tier=tier,
            model=model,
            grok_timeout=grok_timeout,
            adapter_timeout=adapter_timeout,
            grok_max_turns=grok_max_turns,
            dry_run=dry_run,
            token_budget=token_budget,
            test_cmd=resolved_test_cmd,
            capability_receipt_id=worker_receipt_id,
            capability_config_digest=worker_config_digest,
            dispatch_id=worker_dispatch_id,
            remote_phase_budget=remote_phase_budget,
        )
        config = wd._resolve_grok_cycle_bounds(config)
        if is_remote_worker_backend(backend) and worker_receipt_id is None:
            minted = mint_worktree_capability_receipt(
                orchestrator_root=Path(orchestrator_root),
                backend=config.backend,
                model=config.model,
                speed=normalize_speed(config.backend, canonical_speed),
                reasoning_effort=config.reasoning_effort,
                worktree_path=config.worktree_path,
                grok_max_turns=config.grok_max_turns,
                grok_timeout=config.grok_timeout,
                adapter_timeout=config.adapter_timeout,
                token_budget=config.token_budget,
                lane_id=config.lane_id,
                task_ref=task_ref,
                pass_id=resolved_pass_id,
                dispatch_id=worker_dispatch_id,
                persist=not dry_run,
            )
            if isinstance(minted, WorktreeCapabilityReceiptRefusal):
                return _payload(
                    "error",
                    error=minted.error,
                    reason=minted.reason,
                    failed_stage="attestation",
                )
            if dry_run:
                return _payload("needs_guidance")
            config = replace(
                config,
                capability_receipt_id=minted.capability_receipt_id,
                capability_config_digest=minted.capability_config_digest,
                dispatch_id=minted.dispatch_id,
            )
        elif dry_run and is_remote_worker_backend(backend):
            return _payload("needs_guidance")
        stored_completion_config = rescue_identity.get("completion_config")
        if isinstance(stored_completion_config, dict):
            rescue_identity["completion_config"] = {
                **stored_completion_config,
                "backend": config.backend,
                "model": config.model,
                "reasoning_effort": config.reasoning_effort,
                "speed": config.speed,
                "tier": config.tier,
                "token_budget": config.token_budget,
                "max_review_cycles": config.max_review_cycles,
                "session_mode": config.session_mode,
                "dry_run": config.dry_run,
                "grok_max_turns": config.grok_max_turns,
                "test_cmd": config.test_cmd,
                "capability_receipt_id": config.capability_receipt_id,
                "capability_config_digest": config.capability_config_digest,
                "dispatch_id": config.dispatch_id,
            }
        from workbay_orchestrator_mcp.orchestration.review_ratchet import RatchetRefused, load_context

        try:
            ratchet_context = (load_context(Path(worktree_path), resolved_state_dir, task_ref=task_ref,
                               branch=branch_name, current_pass_id=resolved_pass_id,
                               before=pass_started_at, orchestrator_root=Path(orchestrator_root))
                               if review_lane else None)
        except RatchetRefused as exc:
            return _payload("dispatch_refused", reason=str(exc), failed_stage=None)
        run_ctx, _ = wd._setup_worker_run(config)
        run_ctx.review_ratchet_context = ratchet_context
        if ratchet_context is not None:
            rescue_identity.update(listed_finding_ids=ratchet_context.listed_finding_ids,
                                   prior_reviewed_tip=ratchet_context.prior_reviewed_tip)
        persisted_escalation_history = _persisted_escalation_receipt_history(
            resolved_state_dir,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            current_pass_id=resolved_pass_id,
            orchestrator_root=Path(orchestrator_root),
        )
        # Cost lineage is pass-ledger based, not escalation-decision based. A
        # failed/timed-out pass followed by an explicit successful redispatch
        # must retain its spend even when no escalation decision was written.
        covered_pass_ids = {str(value) for value in persisted_escalation_history.get("pass_ids", ())}
        prior_terminal_history = _prior_terminal_receipt_history(
            resolved_state_dir,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            current_pass_id=resolved_pass_id,
            excluded_pass_ids=covered_pass_ids,
            orchestrator_root=Path(orchestrator_root),
        )
        for field in ("turns", "rounds", "tokens", "high_findings"):
            persisted_escalation_history[field] = int(persisted_escalation_history.get(field, 0)) + int(
                prior_terminal_history.get(field, 0)
            )
        persisted_escalation_history["tokens_complete"] = bool(
            persisted_escalation_history.get("tokens_complete", True)
        ) and bool(prior_terminal_history.get("tokens_complete", True))
        persisted_escalation_history["history_complete"] = bool(
            persisted_escalation_history.get("history_complete", True)
        ) and bool(prior_terminal_history.get("history_complete", True))
        persisted_escalation_history["doubtful_pass_ids"] = sorted(
            {
                str(pass_id)
                for history in (persisted_escalation_history, prior_terminal_history)
                for pass_id in history.get("doubtful_pass_ids", ())
            }
        )
        persisted_escalation_history["abandoned_pass_ids"] = sorted(
            {
                str(pass_id)
                for history in (persisted_escalation_history, prior_terminal_history)
                for pass_id in history.get("abandoned_pass_ids", ())
            }
        )
        persisted_escalation_history["pass_ids"] = sorted(
            covered_pass_ids | {str(value) for value in prior_terminal_history.get("pass_ids", ())}
        )
        persisted_escalation_history["routing_history"].extend(
            dict(segment) for segment in prior_terminal_history.get("routing_history", ())
        )
        if persisted_escalation_history.get("escalated_from") is None and isinstance(
            prior_terminal_history.get("escalated_from"), dict
        ):
            persisted_escalation_history["escalated_from"] = dict(prior_terminal_history["escalated_from"])
        if isinstance(prior_terminal_history.get("tier"), str):
            persisted_escalation_history["tier"] = prior_terminal_history["tier"]

        outcome: str | None = None
        admission_defer_reason: str | None = None
        error_reason: str | None = None
        history_escalation_reason: str | None = None
        unknown_question_pass_ids: tuple[str, ...] = ()
        failed_stage: str | None = None
        # E8ROUTE-LOCAL-02: typed failure reason from raw_payload["failure_reason"],
        # carried on a distinct pass field so a gate *can* branch on equality
        # without a free-form parse. No in-repo gate branches on it today: the
        # key reaches operators via the MCP envelope (api.py `_offload_payload`
        # spreads additively and never allow-lists), and that is currently its
        # only consumer. Do not read this as evidence of a live in-repo gate.
        execute_failure_reason: str | None = None
        self_verify_result: dict[str, Any] | None = (
            {
                "skipped": True,
                "reason": "review lane does not run self-verify",
            }
            if review_lane
            else None
        )
        composer_violation_result: dict[str, Any] | None = None
        continuation_dispatch_id: str | None = None
        dispatch_reopened: bool | None = None
        reopen_error: str | None = None
        review_discriminator: str | None = None
        review_raw_tail: str | None = None
        rate_limit_detail: str | None = None
        rate_limit_reset: str | None = None
        rate_limit_reset_header: str | None = None
        transport_payload: dict[str, Any] = {}
        escalation_details: dict[str, Any] | None = None
        escalation_turns: list[Any] = []
        completed_review_rounds = 0
        current_pass_high_findings = 0
        budget_trip_cycle_landed = False

        def _attempt_escalation(review_output: dict[str, Any], *, review_round: int) -> tuple[bool, str | None]:
            """Evaluate and durably persist one boundary escalation decision."""

            nonlocal escalation_details, current_pass_high_findings
            if dry_run:
                return False, None
            typed_findings = _review_escalation_findings(review_output)
            current_pass_high_findings += sum(_finding_is_high(item) for item in typed_findings)
            escalation = _evaluate_pass_escalation(
                run_ctx=run_ctx,
                backend=backend,
                model=model,
                reasoning_effort=reasoning_effort,
                speed=speed,
                review_round=review_round,
                review_output=review_output,
                turns=escalation_turns,
                producer_question_count=producer_question_count,
            )
            if escalation is None:
                return False, None
            covered_pass_ids = {str(value) for value in persisted_escalation_history.get("pass_ids", ())}
            prior_terminal = _prior_terminal_receipt_history(
                resolved_state_dir,
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                current_pass_id=resolved_pass_id,
                excluded_pass_ids=covered_pass_ids,
                orchestrator_root=Path(orchestrator_root),
            )
            if not prior_terminal.get("history_complete", True):
                doubtful_pass_ids = prior_terminal.get("doubtful_pass_ids", ())
                source_detail = ", ".join(str(pass_id) for pass_id in doubtful_pass_ids)
                if not source_detail:
                    source_detail = "authoritative source scan incomplete"
                return True, f"persisted offload pass history became incomplete; refusing escalation ({source_detail})"
            try:
                escalation_details = _persist_escalation_decision(
                    orchestrator_root=Path(orchestrator_root),
                    task_ref=task_ref,
                    lane_id=lane_id,
                    session=session,
                    pass_id=resolved_pass_id,
                    review_round=review_round,
                    decision=escalation,
                    receipt_history={
                        "turns": int(persisted_escalation_history["turns"])
                        + int(prior_terminal["turns"])
                        + len(escalation_turns),
                        "rounds": int(persisted_escalation_history["rounds"])
                        + int(prior_terminal["rounds"])
                        + completed_review_rounds,
                        "tokens": int(persisted_escalation_history["tokens"])
                        + int(prior_terminal["tokens"])
                        + int(getattr(run_ctx, "cumulative_tokens", 0) or 0),
                        "tokens_complete": bool(persisted_escalation_history.get("tokens_complete", True))
                        and bool(prior_terminal.get("tokens_complete", True)),
                        "high_findings": int(persisted_escalation_history["high_findings"])
                        + int(prior_terminal["high_findings"])
                        + current_pass_high_findings,
                        "pass_ids": sorted(covered_pass_ids | set(prior_terminal["pass_ids"]) | {resolved_pass_id}),
                        "routing_history": [
                            *(dict(segment) for segment in persisted_escalation_history.get("routing_history", ())),
                            *(dict(segment) for segment in prior_terminal.get("routing_history", ())),
                            *(dict(segment) for segment in pass_routing_history),
                        ],
                        "tier": escalation.from_quad.tier,
                    },
                )
            except RuntimeError as exc:
                return True, f"escalation persistence failed: {exc}"
            return True, None

        producer_question_count = _persisted_producer_question_count(
            resolved_state_dir,
            task_ref=task_ref,
            lane_id=lane_id,
            session=session,
            current_pass_id=resolved_pass_id,
            orchestrator_root=Path(orchestrator_root),
        )
        unknown_question_pass_ids = producer_question_count.unknown_abandoned_pass_ids
        if unknown_question_pass_ids:
            history_escalation_reason = "abandoned_pass_question_count_unknown"
            outcome = OffloadOutcome.NEEDS_GUIDANCE.value
            failed_stage = "history"
            error_reason = (
                "a legacy abandoned pass has no persisted producer-question count; "
                "review its context before explicitly redispatching the lane"
            )
        history_complete = bool(persisted_escalation_history.get("history_complete", True)) and bool(
            producer_question_count.complete
        )
        if unknown_question_pass_ids:
            history_complete = False
        persisted_escalation_history["history_complete"] = history_complete
        persisted_escalation_history["doubtful_pass_ids"] = sorted(
            set(persisted_escalation_history.get("doubtful_pass_ids", ()))
            | set(producer_question_count.doubtful_pass_ids)
        )
        persisted_escalation_history["abandoned_pass_ids"] = sorted(
            set(persisted_escalation_history.get("abandoned_pass_ids", ()))
            | set(producer_question_count.abandoned_pass_ids)
        )
        producer_question_count = int(producer_question_count)
        if not history_complete and not unknown_question_pass_ids:
            persisted_escalation_history["tokens_complete"] = False
            doubtful_pass_ids = persisted_escalation_history["doubtful_pass_ids"]
            source_detail = (
                ", ".join(doubtful_pass_ids) if doubtful_pass_ids else "authoritative source scan incomplete"
            )
            outcome = "error"
            failed_stage = "history"
            error_reason = f"persisted offload pass history is incomplete; refusing a new turn ({source_detail})"
        # None = post-review lane-check never ran (review did not converge).
        post_review_gate: Any = None
        review_output: dict[str, Any] | None = None
        review_residual: dict[str, Any] | None = None
        for cycle in range(max_review_cycles if history_complete else 0):
            # A prior cycle's proof remains durable in the ledger, but cannot
            # be surfaced as evidence for a later product in this pass.
            verified_gate_evidence = None
            if run_ctx is not None:
                run_ctx.verified_gate_evidence = verified_gate_evidence
            run_ctx.cycle = cycle
            run_ctx.current_process_ran = False
            try:
                cycle_start_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD") or ""
            except (OSError, RuntimeError, TypeError, ValueError):
                cycle_start_head = ""
            # Pre-turn admission (fail-closed point 1 of 3).
            if _live_token_telemetry_supported() and run_ctx.cumulative_tokens >= token_budget:
                outcome = "token_budget_exceeded"
                break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
            # implementation note S1.3 / REV0192R12-B-2: per-cycle decision_ts (host wall
            # epoch seconds). Stamp once per cycle at the commit-to-dispatch
            # site, upstream of this cycle's admission_deferred park. Do NOT
            # stamp once before the loop — cycle N's pre_spawn must not absorb
            # cycle N-1's runtime.
            #
            # Admission park does not continue this loop: it checkpoints, sets
            # outcome=admission_deferred, and breaks. A later operator re-dispatch
            # builds a fresh WorkerRunContext and stamps here again; there is no
            # in-loop restamp path for the deferred cycle.
            run_ctx.decision_ts = int(time.time())
            # internal: host-memory re-check beside the budget/timeout gates.
            # If pressure rose since the last turn so a heavy spawn would now be
            # refused, park. Implementation dirt is checkpointed for a recoverable
            # deferred pass; review dirt remains review evidence for terminal
            # harvest instead of being turned into implementation output.
            if not dry_run and cycle > 0:
                park_reason = _host_admission_should_park(Path(orchestrator_root), backend=run_ctx.backend)
                if park_reason is not None:
                    if review_lane:
                        # A review lane's dirty tree is review evidence, not an
                        # implementation checkpoint. Retain it for the caller;
                        # terminal harvest below carries the review product.
                        # It is not durable output until that harvest succeeds.
                        outcome = "uncommitted_work"
                        failed_stage = "execute"
                        error_reason = (
                            f"review lane parked by {park_reason}; "
                            "implementation checkpoint refused; review product retained for harvest"
                        )
                    else:
                        # A checkpoint COMMIT can itself fail (lock contention, disk
                        # full, failing pre-commit hook). If it does, dirty work is
                        # NOT preserved — report uncommitted_work rather than claim a
                        # clean admission_deferred with recoverable state (OBS-08:
                        # silence is not success). Mirrors the sibling checkpoint
                        # call sites in this function.
                        checkpointed = _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, dry_run=dry_run)
                        _mark_durable_output()
                        outcome = "admission_deferred" if checkpointed else "uncommitted_work"
                    admission_defer_reason = "host_memory_guard"
                    break
            tokens_before = run_ctx.cumulative_tokens
            # Capture HEAD before execute so the blocked probe can distinguish a
            # transport/no-run failure (unchanged HEAD + clean tree) from real work
            # that later reported needs_guidance (dirty tree or landed commit).
            pre_exec_head = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
            _persist_running_progress(
                model_call_in_flight={
                    "cycle": cycle + 1,
                    "phase": "execute",
                    "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            )
            if detached_completion_record is not None and detached_completion_job_id is not None:
                execute_succeeded = _install_detached_result(
                    run_ctx=run_ctx,
                    job_record=detached_completion_record,
                    worktree_path=Path(worktree_path),
                    state_dir=resolved_state_dir,
                    job_id=detached_completion_job_id,
                    lane_kind=lane_kind,
                    start_head=detached_completion_start_head,
                )
            else:
                _require_publication_authority()
                execute_succeeded = wd._execute_phase(run_ctx)
            # Capture process evidence at the execute return boundary, before
            # branching on success. A transport may start and emit usage/routing
            # before post-process validation makes _execute_phase return False;
            # dropping that turn would understate history and cost receipts.
            exec_probe: dict[str, Any] = {}
            if not dry_run:
                try:
                    if getattr(run_ctx, "final_result_path", None):
                        exec_probe = wd._load_result(Path(run_ctx.final_result_path))
                except (OSError, json.JSONDecodeError, RuntimeError):
                    exec_probe = {}
            raw_exec_result = exec_probe.get("raw_payload")
            if isinstance(raw_exec_result, dict) and detached_begin:
                remote_outcome = raw_exec_result.get("outcome")
                observed_job_id = raw_exec_result.get("job_id")
                observed_attempt = raw_exec_result.get("attempt")
                if remote_outcome in (
                    OffloadOutcome.REMOTE_JOB_PENDING.value,
                    OffloadOutcome.ADMISSION_DEFERRED.value,
                ) or raw_exec_result.get("remote_job_submit_error"):
                    _persist_running_progress(model_call_in_flight=None)
                if remote_outcome == OffloadOutcome.REMOTE_JOB_PENDING.value:
                    if (
                        not isinstance(observed_job_id, str)
                        or not observed_job_id
                        or isinstance(observed_attempt, bool)
                        or not isinstance(observed_attempt, int)
                    ):
                        return _payload(
                            "error",
                            run_ctx=run_ctx,
                            error="detached submit returned an invalid job identity",
                            failed_stage="execute",
                        )
                    if detached_job_id is not None and observed_job_id != detached_job_id:
                        return _payload(
                            "error",
                            run_ctx=run_ctx,
                            error="detached submit returned a different job id than the persisted pass fence",
                            failed_stage="execute",
                        )
                    detached_job_id = observed_job_id
                    detached_job_attempt = observed_attempt
                    state_before_submit = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
                    _publish_running_state(
                        {
                            **state_before_submit,
                            **rescue_identity,
                            "status": "running",
                            "task_ref": task_ref,
                            "lane_id": lane_id,
                            "session": session,
                            "job_id": detached_job_id,
                            "attempt": detached_job_attempt,
                        },
                    )
                    return _payload(
                        OffloadOutcome.REMOTE_JOB_PENDING.value,
                        run_ctx=run_ctx,
                        failed_stage="execute",
                        job_id=detached_job_id,
                        attempt=detached_job_attempt,
                    )
                if remote_outcome == OffloadOutcome.ADMISSION_DEFERRED.value:
                    defer_reason = str(raw_exec_result.get("defer_reason") or "vm_admission_refused")
                    return _payload(
                        OffloadOutcome.ADMISSION_DEFERRED.value,
                        run_ctx=run_ctx,
                        error=admission_defer_summary(backend, defer_reason),
                        failed_stage="execute",
                        reason=defer_reason,
                        job_id=observed_job_id if isinstance(observed_job_id, str) else detached_job_id,
                        attempt=observed_attempt if isinstance(observed_attempt, int) else detached_job_attempt,
                    )
                submit_error = raw_exec_result.get("remote_job_submit_error")
                if isinstance(submit_error, str) and submit_error:
                    return _payload(
                        OffloadOutcome.ERROR.value,
                        run_ctx=run_ctx,
                        error=submit_error,
                        failed_stage="execute",
                        job_id=observed_job_id if isinstance(observed_job_id, str) else detached_job_id,
                        attempt=observed_attempt if isinstance(observed_attempt, int) else detached_job_attempt,
                    )
            guidance_contract_invalid = _guidance_contract_is_invalid(exec_probe)
            raw_payload = exec_probe.get("raw_payload") or {}
            current_process_ran = bool(getattr(run_ctx, "current_process_ran", False))
            if current_process_ran:
                escalation_turns.append(_turn_evidence_from_result(exec_probe))
                pass_process_ran = True
                raw_routing = raw_payload.get("routing_effective") if isinstance(raw_payload, dict) else None
                if not isinstance(raw_routing, dict):
                    raw_routing = exec_probe.get("routing_effective")
                if isinstance(raw_routing, dict):
                    pass_routing_effective = dict(raw_routing)
                else:
                    pass_routing_effective = {
                        "backend": str(getattr(run_ctx, "backend", None) or backend) or None,
                        "model": getattr(run_ctx, "model", None) or model,
                        "effort": (
                            getattr(run_ctx, "execution_effective_effort", None)
                            if getattr(run_ctx, "execution_effective_effort", None) not in (None, "inherit")
                            else getattr(run_ctx, "cycle_reasoning_effort", None) or reasoning_effort
                        ),
                        "speed": getattr(run_ctx, "speed", None),
                        "tier": getattr(run_ctx, "tier", None),
                    }
                if pass_originating_routing is None:
                    pass_originating_routing = dict(pass_routing_effective)
                observed = exec_probe.get("response_model") or exec_probe.get("model")
                pass_observed_model = str(observed or pass_routing_effective.get("model") or "") or None
                cycle_tokens = max(0, int(getattr(run_ctx, "cumulative_tokens", 0) or 0) - int(tokens_before))
                if pass_routing_history and pass_routing_history[-1].get("routing_effective") == pass_routing_effective:
                    pass_routing_history[-1]["turns"] = int(pass_routing_history[-1].get("turns", 0)) + 1
                    pass_routing_history[-1]["observed_tokens"] = (
                        int(pass_routing_history[-1].get("observed_tokens", 0)) + cycle_tokens
                    )
                else:
                    pass_routing_history.append(
                        {
                            "routing_effective": dict(pass_routing_effective),
                            "turns": 1,
                            "observed_tokens": cycle_tokens,
                        }
                    )
            # Persist the cycle-boundary token observation before any later
            # branch can stop this pass or begin review.
            _persist_running_progress(model_call_in_flight=None)
            if guidance_contract_invalid and not review_lane:
                # A bare guidance result with no durable work can terminate now.
                # Let a dirty tree or advanced HEAD proceed through review and
                # salvage so completed work retains its normal reclassification.
                try:
                    no_durable_work = (
                        not durable_output
                        and not _worktree_dirty(Path(worktree_path))
                        and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") == pre_exec_head
                    )
                except (OSError, RuntimeError, TypeError, ValueError):
                    no_durable_work = False
                if no_durable_work:
                    outcome = OffloadOutcome.GUIDANCE_CONTRACT_INVALID.value
                    failed_stage = "execute"
                    error_reason = "needs_guidance handoff had no substantive blockers"
                    break
            if not execute_succeeded:
                if getattr(run_ctx, "dispatch_breaker_refusal", None):
                    outcome = "dispatch_refused"
                    failed_stage = "execute"
                    error_reason = run_ctx.execute_error
                    if original_dispatch_id:
                        from workbay_orchestrator_mcp.api import _reopen_dispatch

                        try:
                            reopened = _reopen_dispatch(original_dispatch_id, task_ref, lane_id)
                        except (sqlite3.Error, OSError, RuntimeError) as exc:
                            dispatch_reopened = False
                            reopen_error = f"{type(exc).__name__}: {exc}".splitlines()[0]
                        else:
                            dispatch_reopened = bool(reopened)
                            if reopened:
                                continuation_dispatch_id = original_dispatch_id
                    break
                stop_reason = getattr(run_ctx, "execute_stop_reason", None)
                # Remote agents write in the VM sandbox; host tree may be clean while
                # the co-located sandbox still holds uncommitted work. Probe both.
                dirty_for_checkpoint = False
                try:
                    for probe_path in _salvage_probe_paths(
                        getattr(run_ctx, "backend", None) or backend, Path(worktree_path)
                    ):
                        if _worktree_dirty(probe_path):
                            dirty_for_checkpoint = True
                            break
                except WorktreeUnrecoverableError:
                    raise
                except RuntimeError:
                    dirty_for_checkpoint = _worktree_dirty(Path(worktree_path))
                if stop_reason in _STOP_REASONS_CHECKPOINT and dirty_for_checkpoint:
                    # Unrun vs red must not share an arm (CHECKPOINT-BLOCKED-BY-
                    # INCONCLUSIVE-SELFVERIFY-DISCARDS-COMPLETE-LANES). A red gate
                    # still blocks the early checkpoint (post-loop may salvage when
                    # stop_reason is salvage_eligible). Implementation lanes let an
                    # unrun gate fall through to salvage; review lanes retain their
                    # product without entering the implementation checkpoint path.
                    unrun_gate = False
                    # Held → run self-verify. Cleared ("") means run none.
                    # Absent (None) also skips here; the apply site may already
                    # have recovered a stored row onto config.test_cmd.
                    if not review_lane and config.test_cmd and not dry_run:
                        self_verify_result = wd._self_verify_phase(run_ctx)
                        if not self_verify_result.get("passed"):
                            wd._record_self_verify_blocker(
                                orchestrator_root=Path(orchestrator_root),
                                task_ref=task_ref,
                                lane_id=lane_id,
                                test_cmd=str(self_verify_result.get("command") or config.test_cmd),
                                output_tail=str(self_verify_result.get("output_tail") or ""),
                            )
                            if _self_verify_is_inconclusive(self_verify_result):
                                unrun_gate = True
                            else:
                                outcome = "self_verify_failed"
                                failed_stage = "self_verify"
                                error_reason = (
                                    f"resumable execute stop ({stop_reason}) checkpoint blocked: "
                                    f"self-verify failed on `{self_verify_result.get('command')}`"
                                )
                                break
                    if not review_lane:
                        salvaged = _salvage_checkpoint_if_dirty(
                            getattr(run_ctx, "backend", None) or backend,
                            Path(worktree_path),
                            lane_id,
                            checkpoints,
                            dry_run=dry_run,
                        )
                        _mark_durable_output()
                    else:
                        # A review lane's dirty tree is review evidence, not an
                        # implementation checkpoint. Retain it for the caller;
                        # terminal harvest below carries the review product.
                        outcome = "uncommitted_work"
                        failed_stage = "self_verify" if unrun_gate else "execute"
                        error_reason = (
                            f"review lane stopped with resumable work ({stop_reason}); "
                            "implementation checkpoint refused; review product retained for harvest"
                        )
                        break
                    if salvaged:
                        continuation_dispatch_id = _open_dispatch_id(task_ref, lane_id)
                        if unrun_gate:
                            outcome = "self_verify_inconclusive"
                            failed_stage = "self_verify"
                            error_reason = (
                                f"resumable execute stop ({stop_reason}) self-verify unrun "
                                f"(exit {self_verify_result.get('exit_code') if self_verify_result else None}; "
                                "zero tests executed) on "
                                f"`{(self_verify_result or {}).get('command')}`; "
                                "checkpoint preserved (unrun gate is not a red suite); "
                                "continue by re-dispatching with dispatch_lane_work(dispatch_id=<same>, "
                                "no brief) → continuation_armed, then run_offload_pass"
                            )
                        else:
                            outcome = "checkpoint"
                            error_reason = (
                                f"execute stopped with resumable work ({stop_reason}) and a "
                                "self-verified checkpoint preserved; "
                                "continue by re-dispatching with dispatch_lane_work(dispatch_id=<same>, "
                                "no brief) → continuation_armed, then run_offload_pass"
                            )
                        break
                    if unrun_gate:
                        # Work in hand but the checkpoint itself failed — still
                        # typed as uncommitted_work, never as a red suite.
                        outcome = "uncommitted_work"
                        failed_stage = "self_verify"
                        error_reason = (
                            f"resumable execute stop ({stop_reason}) self-verify unrun "
                            f"(exit {self_verify_result.get('exit_code') if self_verify_result else None}) "
                            "and checkpoint salvage failed"
                        )
                        break
                outcome = "error"
                failed_stage = "execute"
                # Prefer named execute cause (missing manifest → materialize_*) over
                # a generic status-log pointer (implementation note S3 / [OBS-08]).
                # E8FIX-A-01/A-03: every checkpoint-eligible stop reason is named in
                # the error string (not only max_turns). When the marker path never
                # set execute_error (adapter-exception-only assignment) and the tree
                # is clean, promote adapter blockers so the operator sees the real
                # cause instead of the generic execute-phase-failed string.
                named_exec = str(getattr(run_ctx, "execute_error", None) or "").strip()
                if stop_reason in _STOP_REASONS_CHECKPOINT:
                    blocker_text = ""
                    if not named_exec:
                        try:
                            if getattr(run_ctx, "final_result_path", None):
                                probe = wd._load_result(Path(run_ctx.final_result_path))
                                bl = probe.get("blockers") if isinstance(probe, dict) else None
                                if isinstance(bl, list):
                                    blocker_text = "; ".join(str(b).strip() for b in bl if str(b).strip())
                        except (OSError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
                            blocker_text = ""
                    if stop_reason == "max_turns":
                        # Preserve the historical max-turns phrasing used by probes.
                        error_reason = _max_turns_execute_error_reason(named_exec)
                    elif named_exec:
                        error_reason = f"execute stopped: {stop_reason} ({named_exec})"
                    elif blocker_text:
                        error_reason = f"execute stopped: {stop_reason}: {blocker_text}"
                    else:
                        error_reason = f"execute stopped: {stop_reason}"
                else:
                    error_reason = named_exec or "execute phase failed; see worker status/log for the failure stage"
                break
            # Post-turn reconciliation (point 3): a budgeted turn with no token
            # telemetry. This is a contract violation ONLY for a backend that
            # declares it emits token usage — for such a backend a zero delta
            # means the governor ran blind, so error out. A backend declared
            # telemetry-less (e.g. grok-cli, which self-meters only
            # approximately via session context-fill deltas — a different unit
            # not governed by token_budget) is not violating any contract; its
            # budget is enforced by the turn-count + deadline bounds in this same
            # loop, so a zero delta on a turn must NOT abort a
            # working turn (internal / TB-001; unifies
            # this with worker_daemon._accumulate_run_ctx_tokens' soft-warn).
            if not dry_run and run_ctx.cumulative_tokens == tokens_before and _live_token_telemetry_supported():
                outcome = "error"
                failed_stage = "execute"
                error_reason = "token telemetry missing on a budgeted turn (telemetry contract violation)"
                break
            # Execute-result blocked probe (width2 dogfood): a turn whose result
            # signals needs_guidance / admission_deferred AND left a clean tree
            # with unchanged HEAD never ran real work (transport/no-run failure).
            # In that case TEST_CMD would fail for the WRONG reason and mask the
            # actual blocker as self_verify_failed — classify before self-verify.
            # Real work (dirty tree or landed commit) must keep the historical
            # self-verify-before-commit ordering even if handoff_action is
            # needs_guidance. _review_phase then routes the blocked handoff /
            # defer as usual when the probe does fire.
            execute_blocked_reason: str | None = None
            probe_blockers_value = exec_probe.get("blockers")
            probe_blockers = probe_blockers_value if isinstance(probe_blockers_value, list) else []
            if (
                current_process_ran
                and exec_probe.get("handoff_action") == "needs_guidance"
                and _question_from(
                    str(exec_probe.get("outcome_provenance") or "") or None,
                    [str(item) for item in probe_blockers if str(item).strip()],
                    str(exec_probe.get("error") or "") or None,
                    raw_payload,
                    served=_execute_turn_was_served(
                        cumulative_tokens=run_ctx.cumulative_tokens,
                        tokens_before=tokens_before,
                        exec_probe=exec_probe,
                        lane_id=lane_id,
                    ),
                )
            ):
                producer_question_count += 1
                # The accepted question is progress intent: publish its count
                # before review or any later cycle can continue.
                _persist_running_progress()
            quarantine_outcome = _control_path_quarantine_outcome(exec_probe)
            if exec_probe.get("handoff_action") == "rate_limited":
                outcome = "rate_limited"
                failed_stage = "execute"
                if isinstance(raw_payload, dict):
                    detail_value = raw_payload.get("rate_limit_detail")
                    reset_value = raw_payload.get("rate_limit_reset")
                    reset_header_value = raw_payload.get("rate_limit_reset_header")
                    rate_limit_detail = str(detail_value) if detail_value is not None else None
                    rate_limit_reset = str(reset_value) if reset_value is not None else None
                    rate_limit_reset_header = str(reset_header_value) if reset_header_value is not None else None
                error_reason = rate_limit_detail or "remote provider quota exhausted"
                break
            if exec_probe.get("handoff_action") == "transport_failure":
                if isinstance(raw_payload, dict):
                    transport_payload = dict(raw_payload)
                outcome = "error"
                failed_stage = "execute"
                error_reason = _transport_failure_reason(exec_probe, raw_payload)
                break
            # implementation note R3 [PR-0155-02]: the execute-blocked probe's transport-failure
            # semantics apply to implement lanes only; a review lane's clean-tree +
            # needs_guidance is a completed review, typed at classification below.
            if not dry_run and not review_lane:
                signals_blocked = exec_probe.get("handoff_action") == "needs_guidance" or (
                    isinstance(raw_payload, dict) and raw_payload.get("admission_deferred")
                )
                if (
                    signals_blocked
                    and not _worktree_dirty(Path(worktree_path))
                    and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") == pre_exec_head
                ):
                    # implementation note S4: claim-vs-evidence is a nested refinement of this
                    # clean-tree + unchanged-HEAD guard — never a sibling of it, so a
                    # dirty tree or advanced HEAD can never reach the fabrication token.
                    if quarantine_outcome is not None:
                        from .adapters.remote_exec import _control_path_quarantine_text

                        quarantine_raw = dict(raw_payload) if isinstance(raw_payload, dict) else {}
                        quarantine_raw["outcome"] = quarantine_outcome
                        execute_blocked_reason = _control_path_quarantine_text(quarantine_raw)
                    elif (
                        lane_kind_confirmed
                        and lane_kind == "implement"
                        and not (isinstance(raw_payload, dict) and raw_payload.get("admission_deferred"))
                        and _claim_contradicted_by_evidence(exec_probe, raw_payload)
                    ):
                        execute_blocked_reason = "claim_contradicted_by_evidence"
                    else:
                        probe_blockers = exec_probe.get("blockers")
                        first_blocker = _first_nonempty_blocker(probe_blockers)
                        execute_blocked_reason = (
                            first_blocker or str(exec_probe.get("summary") or "") or "execute returned a blocked result"
                        )
                    # E8ROUTE-LOCAL-02 / E8RA-R1-01: structural consumer for the
                    # typed failure_reason channel already on raw_payload. Keep
                    # execute_blocked_reason as operator-facing prose (blockers[0]);
                    # do not recover the token by substring of that prose.
                    if isinstance(raw_payload, dict):
                        typed_fr = raw_payload.get("failure_reason")
                        if isinstance(typed_fr, str) and typed_fr.strip():
                            execute_failure_reason = typed_fr.strip()
            # Worker self-verify gate (backend-neutral): TEST_CMD must pass before commit.
            # Skipped for a blocked execute result — there is no verified work to gate.
            # Held → run. Cleared ("") → run none, as distinct from a stored row.
            tested_product_tree: str | None = None
            if execute_blocked_reason is None and not review_lane and not dry_run and config.test_cmd:
                product_tree_probe_failure: str | None = None
                try:
                    tested_product_tree = _worktree_tree_sha_with_retry(Path(worktree_path), deadline=deadline)
                except _GitEvidenceProbeTimeout:
                    product_tree_probe_failure = "product_tree_probe_timeout"
                if tested_product_tree is None:
                    product_tree_probe_failure = product_tree_probe_failure or "product_tree_probe_failed"
                    tip = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
                    worktree_state = _worktree_state_after_probe_timeout(Path(worktree_path), deadline=deadline)
                    verified_gate_evidence = {
                        "ok": False,
                        "persisted": False,
                        "reason": product_tree_probe_failure,
                        "task_ref": task_ref,
                        "lane_id": lane_id,
                        "pass_id": resolved_pass_id,
                        "tip": tip,
                        "worktree_state": worktree_state,
                        "command": str(config.test_cmd).strip(),
                    }
                    if run_ctx is not None:
                        run_ctx.verified_gate_evidence = verified_gate_evidence
                    outcome = "self_verify_inconclusive"
                    failed_stage = "self_verify"
                    head_text = tip or "unknown"
                    error_reason = (
                        "worker self-verify could not establish the product tree "
                        f"({product_tree_probe_failure}; HEAD {head_text}; worktree_state: {worktree_state})"
                    )
                    if worktree_state == "dirty":
                        error_reason += "; dirty work remains uncommitted"
                    break
                self_verify_result = wd._self_verify_phase(run_ctx)
                if not self_verify_result.get("passed"):
                    wd._record_self_verify_blocker(
                        orchestrator_root=Path(orchestrator_root),
                        task_ref=task_ref,
                        lane_id=lane_id,
                        test_cmd=str(self_verify_result.get("command") or config.test_cmd),
                        output_tail=str(self_verify_result.get("output_tail") or ""),
                    )
                    if _self_verify_is_inconclusive(self_verify_result):
                        outcome = "self_verify_inconclusive"
                        failed_stage = "self_verify"
                        error_reason = (
                            f"worker self-verify inconclusive on `{self_verify_result.get('command')}` "
                            f"(exit {self_verify_result.get('exit_code')}; zero tests executed — "
                            "host re-verification required, not a red suite)"
                        )
                        # Unrun gate: preserve dirty work before breaking. A red
                        # suite still refuses the commit (fall through to break
                        # without checkpointing below).
                        checkpointed = _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, dry_run=dry_run)
                        _mark_durable_output()
                        if not checkpointed:
                            outcome = "uncommitted_work"
                            failed_stage = "execute"
                            error_reason = (
                                f"self-verify unrun and the checkpoint commit failed; original: {error_reason}"
                            )
                        break
                    outcome = "self_verify_failed"
                    failed_stage = "self_verify"
                    error_reason = (
                        f"worker self-verify failed on `{self_verify_result.get('command')}` "
                        f"(exit {self_verify_result.get('exit_code')})"
                    )
                    break
            # Commit gate: implementation output is checkpointed before the next
            # review turn. Review-lane output stays uncommitted by design: it is
            # evidence for terminal harvest, never an implementation checkpoint.
            # dry_run never writes.
            checkpointed = True
            if not review_lane:
                checkpointed = _checkpoint_if_dirty(Path(worktree_path), lane_id, checkpoints, dry_run=dry_run)
            _mark_durable_output()
            if not checkpointed:
                outcome = "uncommitted_work"
                failed_stage = "execute"
                error_reason = "execute left the worktree dirty and the checkpoint commit failed"
                break
            if (
                self_verify_result is not None
                and self_verify_result.get("passed") is True
                and not review_lane
                and not dry_run
                and config.test_cmd
            ):
                gate_verification = dict(self_verify_result)
                gate_verification["product_tree"] = tested_product_tree
                gate_verification["execution_ran"] = current_process_ran
                if gate_verification.get("off_box_delegated") is True:
                    # [PERF-10] The cycle result is already in memory; reuse its
                    # capture/lineage rather than serially reopening that file.
                    # Reuse this cycle's adapter result snapshot: the existing
                    # consumer binds its four-key capture and raw provenance to
                    # the same return object. Avoid a second result-file read.
                    gate_verification["producer_capture"] = exec_probe.get("off_box_self_verify")
                    adapter_payload = raw_payload if isinstance(raw_payload, dict) else {}
                    gate_verification["source_provenance"] = adapter_payload.get("source_provenance")
                    gate_verification["producer_remainder_evidence"] = {
                        key: adapter_payload[key]
                        for key in (
                            "uncommitted_remainder_present",
                            "uncommitted_remainder_read_error",
                            "uncommitted_remainder_path",
                            "uncommitted_remainder_bytes",
                            "work_status",
                        )
                        if key in adapter_payload
                    }
                    gate_test_command = str(config.test_cmd).strip()
                else:
                    # Worker self-verify records the actual shell command after
                    # its hermetic environment prefix. Compare and persist that
                    # command, while the env helper preserves the lane TEST_CMD.
                    raw_test_command = str(config.test_cmd).strip()
                    verify_home = wd.hermetic_self_verify_home(Path(worktree_path))
                    gate_test_command, _injected_env = wd.inject_hermetic_test_cmd_env(
                        raw_test_command,
                        home=verify_home,
                        backend=getattr(run_ctx, "backend", None) or backend,
                    )
                expected_tip = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
                if detached_completion_record is not None and detached_completion_job_id is not None:
                    detached_spec = detached_completion_record.get("spec") or {}
                    detached_paths = _detached_artifact_paths(detached_spec.get("argv"), Path(worktree_path))
                    checkpoint_refusal = _detached_checkpoint_refusal(
                        worktree_path=Path(worktree_path),
                        patch_file=detached_paths.get("patch"),
                        job_id=detached_completion_job_id,
                        start_head=detached_completion_start_head,
                        checkpoint_head=_recorded_detached_checkpoint(
                            read_pass_state(resolved_state_dir, resolved_pass_id) or {}, detached_completion_job_id
                        ),
                        expected_tip=expected_tip,
                    )
                    if checkpoint_refusal is not None:
                        outcome = OffloadOutcome.NEEDS_GUIDANCE.value
                        failed_stage = "self_verify"
                        error_reason = checkpoint_refusal
                        break
                    checkpoint_state = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
                    if (
                        checkpoint_state.get("detached_checkpoint") is None
                        or (checkpoint_state.get("detached_checkpoint") or {}).get("prepared") is True
                    ):
                        _publish_running_state(
                            {
                                **checkpoint_state,
                                "detached_checkpoint": {"job_id": detached_completion_job_id, "head": expected_tip},
                            },
                        )
                # [GRPH-01][GRPH-31] Verification → checkpoint → durable row is
                # a causal chain; this row is the prerequisite consumed by review.
                # [GRPH-09] It shares ledger state with review, so serialize one
                # bounded writer before the reviewer can read that state.
                verified_gate_evidence = persist_verified_lane_gate(
                    task_ref,
                    lane_id,
                    session,
                    resolved_pass_id,
                    Path(worktree_path),
                    expected_tip,
                    gate_test_command,
                    gate_verification,
                    deadline=deadline,
                )
                if run_ctx is not None:
                    run_ctx.verified_gate_evidence = verified_gate_evidence
                if not verified_gate_evidence.get("persisted"):
                    outcome = "error"
                    failed_stage = "self_verify"
                    error_reason = (
                        "verified lane gate could not be persisted before review: "
                        f"{verified_gate_evidence.get('reason') or 'unknown persistence failure'}"
                    )
                    break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
            if _live_token_telemetry_supported() and run_ctx.cumulative_tokens >= token_budget:
                budget_trip_cycle_landed = _compute_commit_landed(since=cycle_start_head)
                outcome = "token_budget_exceeded"
                break
            # A producer question is observable before review.  Evaluate the
            # cross-dispatch trigger here so _review_phase cannot submit a
            # blocked handoff and raise StopIteration before escalation runs.
            # Quarantine requires review of the stored bytes. Preserve the
            # normal blocked handoff (and lane_message), never retry a producer.
            if execute_blocked_reason is not None and quarantine_outcome is None:
                escalated, escalation_error = _attempt_escalation({}, review_round=cycle + 1)
                if escalated:
                    if escalation_error is not None:
                        outcome = "error"
                        failed_stage = "review"
                        error_reason = escalation_error
                    else:
                        outcome = "escalated"
                        failed_stage = None
                        error_reason = None
                    break
            # Persist the exact carrier location and review subject before the
            # runner can publish a verdict or begin coordinator writes.
            rescue_identity.update(
                base_sha=start_head_ref, tip_sha=_git_stdout(Path(worktree_path), "rev-parse", "HEAD")
            )
            rescue_identity["review_carrier_path"] = (
                str(run_ctx.final_result_path) if run_ctx.final_result_path else None
            )
            _publish_running_state(
                {
                    **rescue_identity,
                    "status": "running",
                    "task_ref": task_ref,
                    "lane_id": lane_id,
                    "session": session,
                    "orchestrator_root": str(orchestrator_root),
                },
            )
            _persist_running_progress(
                model_call_in_flight={
                    "cycle": cycle + 1,
                    "phase": "review",
                    "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            )
            review_defer_terminal = False
            try:
                while True:
                    try:
                        review_output = wd._review_phase(run_ctx)
                    except StopIteration as stop:
                        candidate_reason = _review_admission_defer_reason(stop)
                        if (
                            candidate_reason is None
                            or not _compute_commit_landed(since=cycle_start_head)
                            or not _self_verify_not_failed(self_verify_result)
                        ):
                            raise
                        review_defer_reason = candidate_reason
                        if review_defer_retries is None:
                            review_defer_retries = 0
                        _persist_running_progress(model_call_in_flight=None)
                        _mark_durable_output(review_output)
                        _mark_durable_output(_safe_final_result(run_ctx))
                        if review_defer_retries >= len(REVIEW_DEFER_RETRY_DELAYS_SECONDS):
                            outcome = "completed_unreviewed"
                            failed_stage = "review"
                            error_reason = None
                            review_defer_terminal = True
                            break
                        delay = REVIEW_DEFER_RETRY_DELAYS_SECONDS[review_defer_retries]
                        if time.monotonic() + delay >= deadline:
                            outcome = "completed_unreviewed"
                            failed_stage = "review"
                            error_reason = None
                            review_defer_terminal = True
                            break
                        _review_defer_wait(delay)
                        if time.monotonic() >= deadline:
                            outcome = "completed_unreviewed"
                            failed_stage = "review"
                            error_reason = None
                            review_defer_terminal = True
                            break
                        review_defer_retries += 1
                        _persist_running_progress(
                            model_call_in_flight={
                                "cycle": cycle + 1,
                                "phase": "review",
                                "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                            }
                        )
                        continue
                    from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                        review_result_is_harvestable,
                    )

                    # Validate the original document before adding durable write metadata.
                    review_harvestable = review_result_is_harvestable(review_output)
                    review_output = _stamp_schema_degraded_review(
                        review_output,
                        task_ref=task_ref,
                        lane_id=lane_id,
                        session=session,
                        orchestrator_root=Path(orchestrator_root),
                    )
                    break
                if review_defer_terminal:
                    break
                _persist_running_progress(model_call_in_flight=None)
                _mark_durable_output(review_output)
            except StopIteration as stop:
                blocked_kind = str(stop.args[0]) if stop.args else "needs_guidance"
                if blocked_kind in {
                    "admission_deferred",
                    "redispatch_exhausted",
                    "needs_guidance",
                    "scope_violation",
                }:
                    # These controlled StopIteration exits occur before a review
                    # model call. Their review write-ahead marker is not evidence
                    # of an unfinished call, including when execute reports no
                    # usage delta.
                    _persist_running_progress(model_call_in_flight=None)
                _mark_durable_output(review_output)
                _mark_durable_output(_safe_final_result(run_ctx))
                # _review_phase raises only after submitting a BLOCKED handoff
                # (needs_guidance / scope_violation, outcome="failed"). A clean
                # exit code means the SUBMISSION succeeded, not that the work is
                # merge-ready — the lane is waiting on the orchestrator.
                # S3b-2 also raises StopIteration("admission_deferred") for a
                # retryable VM memory-pressure defer (no handoff submitted).
                if blocked_kind == "admission_deferred":
                    # Retryable VM memory-pressure defer from the remote adapter: no commit
                    # landed, recoverable via a fresh re-dispatch. Clean deferred outcome
                    # (mirrors the pre-turn admission_deferred); no salvage checkpoint needed.
                    outcome = "admission_deferred"
                    final_result = _safe_final_result(run_ctx)
                    raw_payload = final_result.get("raw_payload") if isinstance(final_result, dict) else None
                    admission_defer_reason = (
                        str(raw_payload.get("defer_reason"))
                        if isinstance(raw_payload, dict) and raw_payload.get("defer_reason")
                        else "unknown"
                    )
                    failed_stage = "execute"
                    error_reason = admission_defer_summary(
                        str(getattr(run_ctx, "backend", None) or backend), admission_defer_reason
                    )
                    break
                if not dry_run and quarantine_outcome is None:
                    escalated, escalation_error = _attempt_escalation({}, review_round=cycle + 1)
                    if escalated:
                        if escalation_error is not None:
                            outcome = "error"
                            failed_stage = "review"
                            error_reason = escalation_error
                        else:
                            outcome = "escalated"
                            failed_stage = None
                            error_reason = None
                        break
                # Review product is decided by harvest, not by handoff-submit exit.
                # handoff_exit is the submit subprocess status only; a review lane may
                # already have recorded findings via _harvest_review_findings. Do not
                # gate the clean-tree review branch on exit 0 (surface submit failure
                # on the payload instead). composer_violation_quarantined stays under
                # the exit-0 guard. `not checkpoints` is required for the review
                # branch because it asks specifically whether an implementation
                # checkpoint exists; pre_exec_head is re-baselined inside the
                # cycle loop, so a multi-cycle lane that already checkpointed
                # can still satisfy HEAD == pre_exec_head.
                try:
                    review_branch_applies = (
                        review_lane
                        and not checkpoints
                        and not _worktree_dirty(Path(worktree_path))
                        and _git_stdout(Path(worktree_path), "rev-parse", "HEAD") == pre_exec_head
                    )
                except WorktreeUnrecoverableError:
                    raise
                except RuntimeError as guard_exc:
                    outcome = "error"
                    failed_stage = "review"
                    error_reason = f"review-lane classification guard failed: {guard_exc}"
                    break
                if review_branch_applies:
                    # implementation note R3 [PR-0155-02/OBS-04]: a review lane that changed
                    # nothing is a COMPLETED review when the findings block is
                    # parseable (even an empty array = "reviewed clean") AND the
                    # reviewer declared a complete verdict, regardless of whether
                    # the handoff-submit subprocess exited 0.
                    # Fail-closed [SECD-05]: only recorded / reviewed_clean may
                    # become review_complete, and only with an explicit complete
                    # verdict; harvest_failed never can. An incomplete review
                    # keeps its findings (durable_output) and leaves the lane open.
                    _capture_agent_stream()
                    rescue_identity.update(base_sha=start_head_ref, tip_sha=pre_exec_head)
                    _publish_running_state(
                        {
                            **rescue_identity,
                            "status": "running",
                            "task_ref": task_ref,
                            "lane_id": lane_id,
                        },
                    )
                    final_result = _safe_final_result(run_ctx)
                    findings_harvest = _harvest_review_findings(
                        final_result,
                        _safe_latest_worker_report(task_ref, lane_id),
                        task_ref=task_ref,
                        lane_id=lane_id,
                        session=session,
                        orchestrator_root=Path(orchestrator_root),
                        receipt_path=resolved_state_dir / f"review-receipt-{resolved_pass_id}.json",
                        receipt_identity={
                            **rescue_identity,
                            "pass_id": resolved_pass_id,
                            "base_sha": start_head_ref,
                            "tip_sha": pre_exec_head,
                        },
                    )
                    review_product_harvest = True
                    completion = _review_completion(final_result)
                    if (
                        _harvest_verdict(findings_harvest) in _PASSING_HARVEST_VERDICTS
                        and completion["complete"]
                        and not findings_harvest.get("unrecorded_ids")
                    ):
                        # Every complete, fully recorded pass gets this durable
                        # per-pass receipt, with or without findings. Harvest-written
                        # finding rows name no pass, so they cannot prove this pass
                        # alone. Receipt persistence is separate from the
                        # findings block's review outcome; retirement still requires
                        # its own durable evidence.
                        from workbay_handoff_mcp.core import record_review_run  # noqa: PLC0415
                        from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

                        branch = str(rescue_identity.get("branch") or "")
                        verdict = completion["verdict"] if completion["verdict"] in _REVIEW_COMPLETE_TOKENS else None
                        expected_run = {
                            "task_ref": task_ref,
                            "session": session,
                            "subject_path": branch,
                            "subject_kind": "branch",
                            "review_mode": "branch",
                            "branch": branch,
                            "commit_sha": pre_exec_head,
                            "verdict": verdict,
                        }
                        review_run_receipt: dict[str, str | None] = {"status": "recorded", "error": None}
                        try:
                            # Replays may reuse only this pass's matching row;
                            # the writer rejects duplicate review_run_id values.
                            with _get_db_connection() as conn:
                                existing_run = conn.execute(
                                    "SELECT * FROM review_runs WHERE review_run_id = ?", (resolved_pass_id,)
                                ).fetchone()
                            if existing_run is None:
                                recorded_run = _lane_payload_dict(
                                    record_review_run(
                                        review_run_id=resolved_pass_id,
                                        task_ref=task_ref,
                                        session=session,
                                        subject_path=branch,
                                        subject_kind="branch",
                                        review_mode="branch",
                                        verdict=verdict,
                                        actor={"lane_id": lane_id, "branch": branch, "commit_sha": pre_exec_head},
                                    )
                                )
                                if recorded_run.get("ok") is not True:
                                    review_run_receipt = {
                                        "status": "refused",
                                        "error": str(recorded_run.get("error") or "review run record refused"),
                                    }
                            elif any(existing_run[key] != value for key, value in expected_run.items()):
                                review_run_receipt = {
                                    "status": "identity_mismatch",
                                    "error": "review run receipt identity mismatch",
                                }
                        except Exception as exc:  # noqa: BLE001 — receipt failure must not relabel findings
                            review_run_receipt = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
                        findings_harvest = {**findings_harvest, "review_run_receipt": review_run_receipt}
                        if review_run_receipt["status"] != "recorded":
                            logger.warning(
                                "review run receipt %s task_ref=%s lane_id=%s: %s",
                                review_run_receipt["status"],
                                task_ref,
                                lane_id,
                                review_run_receipt["error"],
                            )
                        else:
                            findings_harvest = {**findings_harvest, "review_run_id": resolved_pass_id}
                    if _harvest_block_parsed(findings_harvest):
                        durable_output = True
                        if completion["complete"]:
                            outcome = "review_complete"
                            failed_stage = None
                            error_reason = None
                        else:
                            outcome = "needs_guidance"
                            failed_stage = "review"
                            error_reason = f"review not complete ({completion['reason']}); findings recorded"
                    elif run_ctx.handoff_exit == 0:
                        # A reviewer that reported nothing structured genuinely
                        # needs guidance ([CON-05]: needs_guidance meanings kept).
                        outcome = "needs_guidance"
                        failed_stage = "review"
                        if isinstance(findings_harvest, dict) and findings_harvest.get("reason") in {
                            "record_failed",
                            "record_pending",
                        }:
                            # Block WAS parseable; persistence failed (grok/F5) — do
                            # not claim "no parseable findings block".
                            error_reason = "review findings block parseable but recording failed (record_failed)"
                        elif (
                            isinstance(findings_harvest, dict)
                            and findings_harvest.get("reason") == "harvest_source_missing"
                        ):
                            error_reason = "review findings source unavailable (harvest_source_missing)"
                        else:
                            error_reason = "review lane submitted no parseable findings block"
                    else:
                        # No product + failed submit: still an error (rescue is scoped
                        # to a review that actually produced its findings block).
                        # Preserve record_failed specificity so a parseable block
                        # whose WRITE failed is not mislabeled as "no parseable block".
                        outcome = "error"
                        failed_stage = "review"
                        if isinstance(findings_harvest, dict) and findings_harvest.get("reason") in {
                            "record_failed",
                            "record_pending",
                        }:
                            error_reason = (
                                "review findings block parseable but recording failed (record_failed); "
                                "handoff submit also failed"
                            )
                        elif (
                            isinstance(findings_harvest, dict)
                            and findings_harvest.get("reason") == "harvest_source_missing"
                        ):
                            error_reason = "review findings source unavailable (harvest_source_missing); handoff submit also failed"
                        else:
                            error_reason = _handoff_failure_reason(run_ctx)
                elif run_ctx.handoff_exit == 0:
                    violation = wd._grok_build_contamination_info(run_ctx)
                    # implementation note S2: Composer attestation retired. Only real
                    # grok-build contamination quarantines durable output
                    # ([OBS-08]); missing/format-drift attestation is no longer
                    # a pass outcome branch.
                    if (
                        violation is not None
                        and durable_output
                        and str(violation.get("branch") or "") == "contamination"
                    ):
                        composer_violation_result = violation
                        outcome = "composer_violation_quarantined"
                        failed_stage = "attestation"
                        error_reason = (
                            "grok-build contamination after a self-verified checkpoint; "
                            f"branch={violation.get('branch')}; commit preserved for orchestrator review"
                        )
                    else:
                        outcome = "needs_guidance"
                        # A pre-review blocked EXECUTE result (transport failure /
                        # no-commit) is an execute-stage failure with its real
                        # blocker text, not a generic review-stage handoff.
                        failed_stage = "execute" if execute_blocked_reason else "review"
                        error_reason = execute_blocked_reason or (
                            f"worker handed a blocked result back for guidance ({blocked_kind})"
                        )
                else:
                    if blocked_kind == "needs_guidance" and _worker_requested_guidance(run_ctx):
                        outcome = "needs_guidance"
                        failed_stage = "execute" if execute_blocked_reason else "review"
                        error_reason = _handoff_failure_reason(run_ctx)
                    else:
                        outcome = "error"
                        failed_stage = "review"
                        error_reason = _handoff_failure_reason(run_ctx)
                break
            except (RuntimeError, TypeError, ValueError, json.JSONDecodeError, OSError) as exc:
                from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                    ReviewTransportFailure,
                )

                outcome = "error"
                if isinstance(exc, ReviewTransportFailure):
                    transport_payload = {
                        "transport_failure": True,
                        "provider_error_code": exc.provider_error_code,
                        "retryable": exc.retryable,
                    }
                _mark_durable_output(_safe_final_result(run_ctx))
                failed_stage = "review"
                error_reason = f"review phase failed: {exc}"
                review_product_harvest = bool(_safe_final_result(run_ctx).get("review_result_carrier"))
                break
            # Capture smoke-review degrade discriminator (T1 / [OBS-08]).
            if isinstance(review_output, dict):
                review_status = review_output.get("review")
                if isinstance(review_status, str) and review_status:
                    review_discriminator = review_status
                raw_tail_value = review_output.get("raw_tail")
                if isinstance(raw_tail_value, str) and raw_tail_value:
                    review_raw_tail = raw_tail_value
            if review_discriminator == "refused":
                # A refusal is not review feedback for another execute cycle.
                # Recovery, when allowed, belongs to a fresh bounded dispatch.
                transport_payload = {
                    "refusal_cause": review_output.get("refusal_cause") or "refusal_without_cause",
                    "retryable": review_output.get("retryable") is True,
                }
                outcome = "error"
                failed_stage = "review"
                error_reason = f"review refused: {transport_payload['refusal_cause']}"
                break
            completion = _review_completion(
                {key: value for key, value in review_output.items() if key != "review_result_carrier"}
            )
            carrier_completion = _review_completion({**review_output, "review_result_carrier": True})
            if (
                not carrier_completion["complete"]
                or not review_harvestable
                or completion["reason"] in {REVIEW_COMPLETION_INCOMPLETE, REVIEW_COMPLETION_INVALID}
                or completion["verdict"] == "fail"
                or review_output.get("verdict") in {"fail", "incomplete"}
            ):
                outcome = OffloadOutcome.REVIEW_INCOMPLETE.value
                failed_stage = "review"
                error_reason = "review did not produce a complete passing product"
                break
            subject = review_output.get("review_subject")
            if isinstance(subject, dict) and subject.get("scope_kind") == "unresolved_base":
                # No convergence override can establish a missing review boundary.
                outcome = OffloadOutcome.REVIEW_INCOMPLETE.value
                failed_stage = "review"
                error_reason = "review subject base could not be resolved"
                break
            review_findings = review_output.get("findings") if isinstance(review_output, dict) else ()
            if not isinstance(review_findings, (list, tuple)):
                review_findings = ()
            typed_findings = _link_prior_findings(
                [item for item in review_findings if isinstance(item, dict)],
                task_ref=task_ref,
                orchestrator_root=Path(orchestrator_root),
            )
            review_output = {**review_output, "findings": typed_findings}
            from workbay_orchestrator_mcp.orchestration.review_runner import (  # noqa: PLC0415
                blocking_findings as review_blocking_findings,
            )

            blocking_findings = review_blocking_findings(typed_findings)
            if review_output.get("result_parse") == "degraded" and not blocking_findings:
                outcome = OffloadOutcome.REVIEW_INCOMPLETE.value
                failed_stage = "review"
                error_reason = "degraded review cannot establish completion"
                break
            if (
                typed_findings
                and all(
                    str(item.get("severity") or "").strip().lower() in _VALID_HARVEST_SEVERITIES
                    for item in typed_findings
                )
                and not blocking_findings
            ):
                # D6's predicate is authoritative at every severity. Convergence
                # only stops rework; the real self-verify and final gate still run.
                review_output = dict(review_output)
                review_output["converged"] = True
                review_output["tech_debt_findings"] = typed_findings
            elif blocking_findings and not review_output.get("converged", False):
                completed_review_rounds += 1

            escalated, escalation_error = _attempt_escalation(review_output, review_round=cycle + 1)
            if escalated:
                if escalation_error is not None:
                    outcome = "error"
                    failed_stage = "review"
                    error_reason = escalation_error
                else:
                    outcome = "escalated"
                    failed_stage = None
                    error_reason = None
                break
            if review_output.get("converged", False):
                check_ok = wd._verify_phase(run_ctx, review_output)
                post_review_gate = check_ok
                wd._handoff_phase(run_ctx, check_ok)
                # handoff_exit==0 only means the submission landed. _handoff_phase
                # submits a needs_guidance handoff when verification failed, so a
                # clean exit with check_ok False is a blocked result, NOT ready.
                # Classify the post-review gate before submit-exit: a failed gate
                # plus a failed (or unknown) submit stays a verification class,
                # not ceremony_failed (REV3-FIX-CEREMONYCLASS-01 F1).
                if not check_ok:
                    # BR-0108-S1-01: skipped_unparseable only softens smoke-review
                    # parse; it never overrides lane-check failure ([OBS-08]).
                    # A typed verification outcome needs receiver evidence that
                    # this pass actually ran. Preserve worker blockers in the
                    # result payload, but do not let them hide completed work.
                    outcome, error_reason = _post_review_gate_outcome(
                        check_ok,
                        pass_process_ran=pass_process_ran,
                    )
                    failed_stage = "review"
                elif run_ctx.handoff_exit != 0:
                    outcome = "error"
                    failed_stage = "handoff"
                    error_reason = "final handoff failed after a converged review"
                else:
                    outcome = "handoff_ready"
                break
        else:
            if history_complete:
                outcome = OffloadOutcome.REVIEW_INCOMPLETE.value
                failed_stage = "review"
                error_reason = f"review did not converge after {max_review_cycles} cycles"
                review_residual = _exhaustion_review_residual(review_output, wd=wd, run_ctx=run_ctx)

        # Surface max-turns when it collapsed into a generic review-stage error
        # (remote agent died with max turns; host tree stayed clean so the early
        # checkpoint arm never fired). Prefer the typed stop reason over a
        # handoff-shape mislabel (OFFLOAD-SALVAGE-PROBES-LOCAL-TREE…).
        stop_reason_final = getattr(run_ctx, "execute_stop_reason", None) if run_ctx is not None else None
        if (
            outcome in {OffloadOutcome.ERROR.value, OffloadOutcome.REVIEW_INCOMPLETE.value}
            and failed_stage == "review"
            and (
                stop_reason_final == "max_turns"
                or (isinstance(error_reason, str) and "max turns" in error_reason.casefold())
            )
        ):
            failed_stage = "execute"
            # Bare default only: execute_error is written solely on the adapter-
            # exception path, which always returns False from _execute_phase and
            # is consumed by the early execute-stop arm (break before review).
            # Reaching this reclass arm requires failed_stage == "review", which
            # requires execute returned True — empty intersection with a set field.
            error_reason = _max_turns_execute_error_reason()
            if run_ctx is not None and not getattr(run_ctx, "execute_stop_reason", None):
                run_ctx.execute_stop_reason = "max_turns"

        # Land status BEFORE salvage: ceremony_failed is about durable output the
        # handoff failed to report, not about engine salvage of a dirty tree after
        # the fact (dirty-tree control must stay bare error). ``checkpoints`` is
        # intentionally narrower: review findings can be durable without an
        # implementation checkpoint.
        landed_before_salvage = durable_output

        # Salvage checkpoint: preserve partial implementation work for timeout /
        # budget / error so it is referenced in the outcome rather than lost; a
        # failed salvage downgrades to uncommitted_work. Review lanes retain their
        # dirty review product for terminal harvest instead. Capture the budget trip
        # BEFORE any downgrade so the budget-exceeded handler still fires (the
        # downgrade would flip the guard).
        budget_tripped = outcome == "token_budget_exceeded"
        # implementation note S3a / exit-8: also salvage when self-verify failed after
        # remote exit-3 (agent_exit_with_work). Deliberately excludes max_turns
        # and wall_clock_expiry so the F1 invariant (red time-bound stops stay
        # dirty/uncommitted) holds for both local and remote wall clocks.
        if outcome in ("timeout", "token_budget_exceeded", "error") or (
            outcome == "self_verify_failed" and getattr(run_ctx, "execute_stop_reason", None) in _SALVAGE_STOP_REASONS
        ):
            # A review-lane classification guard failure already means git status
            # was unreadable; re-probing dirty for salvage would raise again and
            # wrongly reclassify the typed review error as uncommitted_work.
            classification_guard_failed = (
                outcome == "error"
                and failed_stage == "review"
                and isinstance(error_reason, str)
                and error_reason.startswith("review-lane classification guard failed:")
            )
            if not classification_guard_failed and failed_stage != "history" and not review_lane:
                try:
                    # Remote backends: probe co-located sandbox first (host worktree
                    # is clean by construction for pre-commit deaths).
                    salvaged = _salvage_checkpoint_if_dirty(
                        getattr(run_ctx, "backend", None) if run_ctx is not None else backend,
                        Path(worktree_path),
                        lane_id,
                        checkpoints,
                        dry_run=dry_run,
                    )
                except RuntimeError as exc:
                    salvaged = False
                    error_reason = f"{outcome}: checkpoint salvage failed: {exc}"
                # Salvage is another durability boundary. Refresh the explicit
                # signal so the post-salvage reclassification below can reason
                # about a salvage checkpoint without consulting ``checkpoints``.
                _mark_durable_output()
                if not salvaged:
                    error_reason = error_reason or f"{outcome}: partial work could not be checkpointed"
                    outcome = "uncommitted_work"
                    failed_stage = failed_stage or "execute"
        if budget_tripped:
            budget_work_landed = _budget_trip_work_landed(
                failed_stage=failed_stage,
                commit_landed=budget_trip_cycle_landed,
                self_verify_result=self_verify_result,
            )
            wd._handle_token_budget_exceeded(run_ctx, work_landed=budget_work_landed)
            # Landed green work whose budget ran out before review is completed
            # work awaiting review, not a budget failure — provided the final
            # handoff the handler just ran did not itself fail (ceremony stays
            # visible as the typed budget stop when it does).
            if (
                budget_work_landed
                and outcome == "token_budget_exceeded"
                and not (
                    getattr(run_ctx, "handoff_attempted", False) is True and getattr(run_ctx, "handoff_exit", None) != 0
                )
            ):
                budget_exhausted = True
                outcome = "completed_unreviewed"
                failed_stage = None
                error_reason = None

        slice_closure: dict[str, Any] | None = None
        # Scope before slice closure so an own open HIGH is not recorded as
        # complete, and a lookup failure is not stored as a clean pass.
        lane_findings_scope = _scope_lane_open_findings(
            task_ref=task_ref,
            lane_id=lane_id,
            orchestrator_root=Path(orchestrator_root),
        )
        outcome_before_scope = outcome
        outcome, failed_stage, error_reason = _apply_lane_findings_scope(
            lane_findings_scope,
            outcome=outcome,
            failed_stage=failed_stage,
            error_reason=error_reason,
        )
        if (
            outcome_before_scope == OffloadOutcome.HANDOFF_READY.value
            and outcome == OffloadOutcome.VERIFICATION_FAILED.value
            and error_reason == _LANE_VERIFICATION_FAILED_REASON
            and lane_findings_scope.lane_high
        ):
            lane_findings_scope = replace(lane_findings_scope, downgraded_from=OffloadOutcome.HANDOFF_READY.value)
        # Closure from the verified commit even when smoke review degraded (T1).
        if outcome == "handoff_ready":
            slice_closure, closure_error = _record_worker_closure(
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                backend=run_ctx.backend,
                model=run_ctx.model,
                worktree_path=Path(worktree_path),
                start_head=start_head,
                baseline_report_id=baseline_report_id,
                pass_id=resolved_pass_id,
                verified_gate_command=(
                    str(verified_gate_evidence.get("command"))
                    if isinstance(verified_gate_evidence, dict) and verified_gate_evidence.get("persisted")
                    else None
                ),
            )
            if closure_error is not None:
                outcome = "error"
                failed_stage = "handoff"
                error_reason = closure_error
            # implementation note S2 [REF-19]: handoff_ready_unattested collapsed → handoff_ready.

        # Engoutcome finding 1: work landed + downstream ceremony failure must
        # not collapse to bare error/needs_guidance (orchestrators may discard
        # the committed product). Use pre-salvage land only so a dirty-tree
        # salvage checkpoint does not launder into ceremony_failed [OBS-08].
        if outcome in ("error", "needs_guidance"):
            outcome = _maybe_reclassify_ceremony_failed(
                outcome=outcome,
                failed_stage=failed_stage,
                commit_landed=landed_before_salvage,
                self_verify_result=self_verify_result,
                run_ctx=run_ctx,
                post_review_gate=post_review_gate,
            )

        # OL-PF2: remaining green durable output with a no-question blocked
        # handoff is completed_unreviewed. This post-salvage signal includes an
        # inspectable salvage checkpoint and harvestable review product.
        landed_for_reclass = durable_output
        if outcome == "needs_guidance":
            outcome, failed_stage, error_reason = _maybe_reclassify_completed_unreviewed(
                outcome=outcome,
                failed_stage=failed_stage,
                commit_landed=landed_for_reclass,
                self_verify_result=self_verify_result,
                error_reason=error_reason,
                run_ctx=run_ctx,
                wd=wd,
                task_ref=task_ref,
                lane_id=lane_id,
                baseline_report_id=baseline_report_id,
            )

        salvage_candidate: dict[str, Any] | None = None
        if outcome == "needs_guidance":
            salvage_candidate = _evaluate_malformed_handoff_salvage(
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
                start_head=start_head,
                baseline_test_id=baseline_test_id,
                baseline_report_id=baseline_report_id,
            )
            if salvage_candidate is not None:
                _record_salvage_audit_decision(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    session=session,
                    evidence=salvage_candidate,
                )

        # The parser records empty blockers as advisory so adapters can pass the
        # handoff onward. Classify only a final blocked implementation pass here:
        # review products, deferred admissions, and durable completed work have
        # already taken their more specific outcomes above.
        if outcome == "needs_guidance" and not review_lane and guidance_contract_invalid:
            outcome = OffloadOutcome.GUIDANCE_CONTRACT_INVALID.value
            failed_stage = failed_stage or "execute"
            error_reason = "needs_guidance handoff had no substantive blockers"

        # T26 / implementation note S13: handoff_ready → lane status "review" + expose
        # one-call gate close. Error/needs_guidance leave status untouched
        # (no false merged). [OBS-08][CON-04]
        lane_status_transition: dict[str, Any] | None = None
        next_lane_action: dict[str, Any] | None = None
        if outcome == "handoff_ready":
            lane_status_transition = _mark_lane_review_on_handoff_ready(
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=Path(worktree_path),
            )
            next_lane_action = next_lane_close_action(task_ref=task_ref, lane_id=lane_id)
        elif outcome == "needs_guidance" and landed_before_salvage and _worker_requested_guidance(run_ctx):
            next_lane_action = next_lane_guidance_action(
                task_ref=task_ref,
                lane_id=lane_id,
                blockers=_worker_guidance_blockers(run_ctx),
            )
        # implementation note R2/R3: harvest review-lane GROK_REVIEW_FINDINGS_JSON before listing
        # so recorded rows appear in payload["findings"]. A review lane already
        # computed + reused this during outcome classification (do not re-run); every
        # other terminal computes it here. Fail-open — never changes outcome /
        # failed_stage.
        if findings_harvest is None:
            harvest_report: dict[str, Any] | str | None = None
            try:
                harvest_report = _latest_worker_report(task_ref, lane_id)
            except Exception:  # noqa: BLE001 — report load is best-effort for harvest
                harvest_report = None
            _capture_agent_stream()
            try:
                review_tip = _git_stdout(Path(worktree_path), "rev-parse", "HEAD")
            except (OSError, RuntimeError, TypeError, ValueError):
                review_tip = None
            rescue_identity.update(base_sha=start_head_ref, tip_sha=review_tip)
            if review_lane or review_product_harvest:
                _publish_running_state(
                    {
                        **rescue_identity,
                        "status": "running",
                        "task_ref": task_ref,
                        "lane_id": lane_id,
                    },
                )
            findings_harvest = _harvest_review_findings(
                _safe_final_result(run_ctx),
                harvest_report,
                task_ref=task_ref,
                lane_id=lane_id,
                session=session,
                orchestrator_root=Path(orchestrator_root),
                receipt_path=(
                    resolved_state_dir / f"review-receipt-{resolved_pass_id}.json"
                    if review_lane or review_product_harvest
                    else None
                ),
                receipt_identity={
                    **rescue_identity,
                    "pass_id": resolved_pass_id,
                    "base_sha": start_head_ref,
                    "tip_sha": rescue_identity["tip_sha"],
                },
                salvage_unstructured=review_lane or review_product_harvest,
            )

        # Typed harvest verdict [AGT-21][OBS-08]: distinguish reviewed-clean
        # (validated empty array) from harvest-broken so a lost review can never
        # look like a clean merge signal. Emit the real verdict for any review
        # lane (or any terminal that classified on a review product); only
        # non-review terminals emit not_attempted. Findings suppression stays
        # keyed on review_product_harvest alone — widening it would erase
        # pre-existing open rows on unrelated review-lane failure paths.
        if review_lane or review_product_harvest:
            harvest_verdict = _harvest_verdict(findings_harvest)
            review_completion = _review_completion(_safe_final_result(run_ctx))
        else:
            harvest_verdict = HARVEST_VERDICT_NOT_ATTEMPTED
            review_completion = None
        # T4: surface worker-recorded findings on every terminal payload — EXCEPT
        # when a review-product harvest failed: backfilling pre-existing open rows
        # for the task_ref would make a broken review look productive. Implement
        # lanes and non-review terminals keep the usual collect-on-terminal path.
        findings_suppressed_reason: str | None = None
        findings_open_count_at_terminal: int | None = None
        if review_product_harvest and harvest_verdict == HARVEST_VERDICT_HARVEST_FAILED:
            # Count open rows for the discriminator only — do not resurrect them
            # into the payload findings list [OBS-08][AGT-21].
            findings_open_count_at_terminal = len(_collect_pass_findings(task_ref=task_ref))
            findings_suppressed_reason = (
                "salvaged_unstructured"
                if isinstance(findings_harvest, dict) and findings_harvest.get("status") == "salvaged_unstructured"
                else "harvest_failed"
            )
            pass_findings: list[dict[str, Any]] = []
        else:
            # Harvest can record rows after the pre-closure scope. Refresh a
            # successful lookup so those rows are visible, then downgrade again
            # if the refreshed page is this lane's own open HIGH or is incomplete.
            if lane_findings_scope.ok:
                previous_downgrade = lane_findings_scope.downgraded_from
                lane_findings_scope = _scope_lane_open_findings(
                    task_ref=task_ref,
                    lane_id=lane_id,
                    orchestrator_root=Path(orchestrator_root),
                )
                if previous_downgrade == OffloadOutcome.HANDOFF_READY.value and lane_findings_scope.lane_high:
                    lane_findings_scope = replace(lane_findings_scope, downgraded_from=previous_downgrade)
                outcome_before_refresh = outcome
                outcome, failed_stage, error_reason = _apply_lane_findings_scope(
                    lane_findings_scope,
                    outcome=outcome,
                    failed_stage=failed_stage,
                    error_reason=error_reason,
                )
                if (
                    outcome_before_refresh == OffloadOutcome.HANDOFF_READY.value
                    and outcome == OffloadOutcome.VERIFICATION_FAILED.value
                    and error_reason == _LANE_VERIFICATION_FAILED_REASON
                    and lane_findings_scope.lane_high
                ):
                    lane_findings_scope = replace(
                        lane_findings_scope, downgraded_from=OffloadOutcome.HANDOFF_READY.value
                    )
            pass_findings = list(lane_findings_scope.lane_findings) if lane_findings_scope.ok else []
        scoped_blockers = list(lane_findings_scope.task_open_blockers) if lane_findings_scope.ok else []
        payload = _payload(
            outcome,
            run_ctx=run_ctx,
            error=error_reason,
            slice_closure=slice_closure,
            self_verify=self_verify_result,
            post_review_gate=post_review_gate if isinstance(post_review_gate, dict) else None,
            composer_violation=composer_violation_result,
            continuation_dispatch_id=continuation_dispatch_id,
            failed_stage=failed_stage,
            review=review_discriminator,
            findings=pass_findings,
            task_open_blockers=scoped_blockers,
            findings_scope=_findings_scope_document(lane_findings_scope),
            raw_tail=review_raw_tail,
            receipt_turns=int(persisted_escalation_history["turns"]) + len(escalation_turns),
            receipt_rounds=int(persisted_escalation_history["rounds"]) + completed_review_rounds,
            receipt_escalated_from=(
                escalation_details.get("from_quad")
                if isinstance(escalation_details, dict)
                else persisted_escalation_history.get("escalated_from")
            ),
            receipt_prior_tokens=int(persisted_escalation_history["tokens"]),
            receipt_prior_tokens_complete=bool(persisted_escalation_history.get("tokens_complete", True)),
            receipt_high_findings=(int(persisted_escalation_history["high_findings"]) + current_pass_high_findings),
            reason=history_escalation_reason,
        )
        if review_residual is not None:
            payload["review_residual"] = review_residual
        payload["findings_harvest"] = findings_harvest
        if outcome == "dispatch_refused" and getattr(run_ctx, "dispatch_breaker_refusal", None):
            payload["ok"] = False
            payload.update(run_ctx.dispatch_breaker_refusal)
            if dispatch_reopened is not None:
                payload["dispatch_reopened"] = dispatch_reopened
            if reopen_error is not None:
                payload["reopen_error"] = reopen_error
        payload["harvest_verdict"] = harvest_verdict
        if review_completion is not None:
            payload["review_verdict"] = review_completion["verdict"]
            payload["review_completion_reason"] = review_completion["reason"]
        # Finding persistence is post-pass ceremony.  A uniqueness collision or
        # other batch write failure must never rewrite the worker's established
        # outcome or erase commit_landed; expose the independent ceremony axis.
        if isinstance(findings_harvest, dict) and findings_harvest.get("reason") in {"record_failed", "record_pending"}:
            payload["ceremony_status"] = CEREMONY_STATUS_FAILED
        if outcome == "admission_deferred" and admission_defer_reason:
            payload["defer_reason"] = admission_defer_reason
        elif review_defer_reason is not None:
            payload["defer_reason"] = review_defer_reason
        if review_defer_retries is not None:
            payload["review_defer_retries"] = review_defer_retries
        if outcome == "rate_limited":
            payload["rate_limit_detail"] = rate_limit_detail
            payload["rate_limit_reset"] = rate_limit_reset
            payload["retry_after_s"] = _retry_after_seconds(rate_limit_reset, header=rate_limit_reset_header)
        if transport_payload:
            for field_name in (
                "provider_error_code",
                "transport_failure",
                "auth_failed",
                "policy_refused",
                "refusal_cause",
                "retryable",
            ):
                if field_name in transport_payload:
                    payload[field_name] = transport_payload[field_name]
        # E8ROUTE-LOCAL-02: typed channel distinct from free-form error prose.
        if execute_failure_reason:
            payload["failure_reason"] = execute_failure_reason
        if history_escalation_reason:
            payload["failure_reason"] = history_escalation_reason
            payload["abandoned_pass_question_count_unknown_pass_ids"] = list(unknown_question_pass_ids)
        if findings_suppressed_reason is not None:
            payload["findings_suppressed_reason"] = findings_suppressed_reason
            payload["findings_open_count_at_terminal"] = findings_open_count_at_terminal
        # Degrade loudly [AGT-10]: handoff_submit_failed is a fact about the submit
        # subprocess, not a property of one outcome. Surface the discriminator on
        # every terminal payload whenever a submit was attempted and returned nonzero.
        # handoff_attempted is required because handoff_exit defaults to 1 (fail-closed).
        if (
            run_ctx is not None
            and getattr(run_ctx, "handoff_attempted", False)
            and getattr(run_ctx, "handoff_exit", 0) not in (0, None)
        ):
            payload["handoff_submit_failed"] = True
            payload["handoff_exit"] = int(run_ctx.handoff_exit)
        handoff_stderr_head = getattr(run_ctx, "handoff_stderr_head", None)
        if isinstance(handoff_stderr_head, str) and handoff_stderr_head:
            payload["handoff_stderr_head"] = handoff_stderr_head
        if salvage_candidate is not None:
            payload["salvage_candidate"] = salvage_candidate
        if lane_status_transition is not None:
            payload["lane_status"] = lane_status_transition.get("status")
            payload["lane_status_transition"] = lane_status_transition
        if next_lane_action is not None:
            payload["next_lane_action"] = next_lane_action
        if escalation_details is not None:
            payload["escalation"] = escalation_details
        return payload

    # Per-lane exclusive lock: the engine drives _setup_worker_run/_execute_phase
    # directly, bypassing the daemon main()'s WorkerLock. Without this, a concurrent
    # daemon single-pass and an engine pass could act on the same lane at once.
    lock = wd.WorkerLock(lane_id, resolved_state_dir)
    sweep_context_token = _ACTIVE_OFFLOAD_PASS_SWEEP_STATE.set(pass_sweep_state)
    detached_job_context_token = _ACTIVE_DETACHED_JOB_ID.set(detached_completion_job_id)
    checkpoint_pass_token = _ACTIVE_DETACHED_CHECKPOINT_PASS.set(
        (resolved_state_dir, resolved_pass_id) if detached_completion_job_id is not None else None
    )
    try:
        if not lock.acquire():
            if detached_completion_job_id is not None:
                # No pass started: do not persist a terminal receipt via _finish.
                return {
                    "ok": True,
                    "terminal": False,
                    "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                    "reason": "lane_locked",
                    "job_id": detached_completion_job_id,
                }
            return _finish(
                _payload(
                    "no_actionable_work",
                    error=f"lane '{lane_id}' is locked by another worker/daemon; pass not started",
                )
            )
        try:
            if detached_begin or detached_completion_job_id is not None:
                previous_pass = read_pass_state(resolved_state_dir, resolved_pass_id) or {}
                previous_result = previous_pass.get("result")
                previous_job_id = previous_pass.get("job_id")
                previous_attempt = previous_pass.get("attempt")
                previous_is_pending = (
                    isinstance(previous_result, dict)
                    and previous_result.get("outcome") == OffloadOutcome.REMOTE_JOB_PENDING.value
                )
                if (
                    previous_pass.get("status") == "done"
                    and isinstance(previous_result, dict)
                    and not previous_is_pending
                ):
                    # A pass id is an idempotency key. Once a begin returned a
                    # terminal receipt, redispatch returns that receipt rather
                    # than opening another attempt under the same id.
                    return dict(previous_result)
                is_requested_completion = (
                    detached_completion_job_id is not None and previous_job_id == detached_completion_job_id
                )
                if detached_completion_job_id is not None and not is_requested_completion:
                    return _finish(
                        _payload(
                            "error",
                            error="detached completion job does not match the pass state job id",
                            failed_stage="execute",
                            job_id=detached_completion_job_id,
                        )
                    )
                if (
                    isinstance(previous_job_id, str)
                    and previous_job_id
                    and not isinstance(previous_attempt, bool)
                    and isinstance(previous_attempt, int)
                ):
                    ledger = RemoteJobLedger(resolved_state_dir / "remote-jobs")
                    try:
                        existing_job = ledger.read_job(previous_job_id)
                    except RemoteJobLedgerError as exc:
                        if exc.reason != "job_not_found" or previous_is_pending:
                            return _finish(
                                _payload(
                                    "error",
                                    error=f"detached pass job ledger is unavailable: {exc.reason}",
                                    failed_stage="execute",
                                    job_id=previous_job_id,
                                    attempt=previous_attempt,
                                )
                            )
                        existing_job = None
                    except OSError as exc:
                        return _finish(
                            _payload(
                                "error",
                                error=f"detached pass job ledger could not be read: {type(exc).__name__}: {exc}",
                                failed_stage="execute",
                                job_id=previous_job_id,
                                attempt=previous_attempt,
                            )
                        )
                    if existing_job is not None:
                        stage = existing_job.get("stage")
                        job_is_open = stage in {"intent", "submitted"} or (
                            stage == "done" and existing_job.get("rc") == 0
                        )
                        if (
                            is_requested_completion
                            and stage in {"done", "completed"}
                            and (stage == "completed" or existing_job.get("rc") == 0)
                        ):
                            detached_completion_record = existing_job
                            detached_job_id = previous_job_id
                            detached_job_attempt = previous_attempt
                        elif job_is_open and (previous_pass.get("status") == "running" or previous_is_pending):
                            detached_job_id = previous_job_id
                            detached_job_attempt = previous_attempt
                            return _finish(
                                _payload(
                                    OffloadOutcome.REMOTE_JOB_PENDING.value,
                                    failed_stage="execute",
                                    job_id=detached_job_id,
                                    attempt=detached_job_attempt,
                                )
                            )
                        if previous_is_pending and detached_completion_record is None:
                            return _finish(
                                _payload(
                                    "error",
                                    error=f"detached pass job is no longer open (stage={stage})",
                                    failed_stage="execute",
                                    job_id=previous_job_id,
                                    attempt=previous_attempt,
                                )
                            )
                        if (
                            previous_pass.get("status") == "running"
                            and stage == "done"
                            and detached_completion_record is None
                        ):
                            if existing_job.get("rc") == 75:
                                return _finish(
                                    _payload(
                                        OffloadOutcome.ADMISSION_DEFERRED.value,
                                        error=admission_defer_summary(backend, "vm_admission_refused"),
                                        failed_stage="execute",
                                        reason="vm_admission_refused",
                                        job_id=previous_job_id,
                                        attempt=previous_attempt,
                                    )
                                )
                            return _finish(
                                _payload(
                                    OffloadOutcome.ERROR.value,
                                    error=f"detached remote job submit closed with rc={existing_job.get('rc')}",
                                    failed_stage="execute",
                                    job_id=previous_job_id,
                                    attempt=previous_attempt,
                                )
                            )
                        if (
                            previous_pass.get("status") == "running"
                            and stage in {"completed", "dead_letter"}
                            and detached_completion_record is None
                        ):
                            return _finish(
                                _payload(
                                    "error",
                                    error=f"detached pass job cannot be adopted (stage={stage})",
                                    failed_stage="execute",
                                    job_id=previous_job_id,
                                    attempt=previous_attempt,
                                )
                            )
                if detached_completion_record is None:
                    identity = RemoteJobIdentity(
                        task_ref=task_ref,
                        lane_id=lane_id,
                        pass_id=resolved_pass_id,
                        attempt=0,
                    )
                    detached_job_id = identity.job_id
                    detached_job_attempt = identity.attempt
            _publish_running_state(
                {
                    **rescue_identity,
                    "status": "running",
                    "task_ref": task_ref,
                    "lane_id": lane_id,
                    "session": session,
                    **(
                        {"job_id": detached_job_id, "attempt": detached_job_attempt}
                        if detached_job_id is not None and detached_job_attempt is not None
                        else {}
                    ),
                },
            )
            try:
                result = _execute_pass()
            except _PassAuthorityChanged as exc:
                result = _payload("error", run_ctx=run_ctx, error=str(exc), failed_stage="execute")
                if "invocation_token" in exc.fields:
                    # Another invocation owns the durable record; never overwrite it.
                    return result
            except WorktreeUnrecoverableError as exc:
                result = _payload(
                    "worktree_unrecoverable",
                    run_ctx=run_ctx,
                    error=str(exc),
                    failed_stage="execute",
                )
            except Exception as exc:  # noqa: BLE001 - a crash must publish terminal state, never leave the pass 'running'
                result = _payload("error", run_ctx=run_ctx, error=f"offload pass crashed: {type(exc).__name__}: {exc}")
            return _finish(result)
        finally:
            lock.release()
    finally:
        if review_cycle_context_token is not None:
            ACTIVE_DISPATCH.reset(review_cycle_context_token)
        _ACTIVE_DETACHED_CHECKPOINT_PASS.reset(checkpoint_pass_token)
        _ACTIVE_DETACHED_JOB_ID.reset(detached_job_context_token)
        _ACTIVE_OFFLOAD_PASS_SWEEP_STATE.reset(sweep_context_token)


def complete_remote_pass(
    job_id: str, *, state_dir: Path, orchestrator_root: Path, completion_deadline: float | None = None
) -> dict[str, Any]:
    """Enforce one whole-completion deadline, including engine persistence.

    The per-job lock also contains synchronous calls that fail to honor timeout:
    an expired adoption lease cannot admit another collector until they unwind.
    """
    from .remote_job_reconcile import RemoteJobReconcileError, _locked  # noqa: PLC0415

    started = time.monotonic()
    try:
        job = RemoteJobLedger(Path(state_dir) / "remote-jobs").read_job(job_id)
    except (OSError, RemoteJobLedgerError):
        # Preserve the existing typed ledger refusal; no completion runs.
        return _complete_remote_pass(job_id, state_dir=state_dir, orchestrator_root=orchestrator_root)
    if completion_deadline is None:
        spec = job.get("spec") or {}
        spec = spec if isinstance(spec, dict) else {}
        state = read_pass_state(Path(state_dir), str(spec.get("pass_id"))) or {}
        completion_deadline = started + completion_timeout_seconds(state)
    token = _ACTIVE_COMPLETION_DEADLINE.set(completion_deadline)
    try:
        _completion_remaining()
        try:
            with _locked(
                Path(state_dir) / "remote-jobs" / f".{job_id}.completion.lock",
                timeout_seconds=_completion_remaining(),
            ):
                _completion_remaining()
                return _complete_remote_pass(job_id, state_dir=state_dir, orchestrator_root=orchestrator_root)
        except RemoteJobReconcileError:
            _completion_remaining()
            raise
    finally:
        _ACTIVE_COMPLETION_DEADLINE.reset(token)


_REHARVEST_TRIGGER_OUTCOME = OffloadOutcome.SELF_VERIFY_INCONCLUSIVE.value


def _reharvest_open(pass_state: dict[str, Any], job_id: str) -> bool:
    """True only for a pass that ``reharvest_terminal_pass`` reopened for this exact job.

    The pass must be fenced to the job, and the marker must carry the superseded
    receipts the locked reopen authenticated for that same job. Anything less is
    an ordinary completion with its ordinary review policy.
    """
    marker = pass_state.get(_REHARVEST_KEY)
    if pass_state.get("job_id") != job_id or not isinstance(marker, dict) or marker.get("job_id") != job_id:
        return False
    superseded = marker.get("superseded_results")
    return (
        isinstance(superseded, list)
        and bool(superseded)
        and all(isinstance(receipt, dict) and receipt.get("job_id") == job_id for receipt in superseded)
    )


def reharvest_terminal_pass(job_id: str, *, pass_id: str, state_dir: Path, orchestrator_root: Path) -> dict[str, Any]:
    """Serialize eligibility, checkpoint binding, reopen and completion with collectors."""
    from .remote_job_reconcile import _locked  # noqa: PLC0415

    try:
        # Validate the ledger key before using it in a lock pathname. The locked
        # implementation re-reads all mutable eligibility state under that lock.
        RemoteJobLedger(Path(state_dir) / "remote-jobs").read_job(job_id)
    except (OSError, RemoteJobLedgerError) as exc:
        return {
            "ok": False,
            "terminal": True,
            "outcome": "reharvest_refused",
            "reason": exc.reason if isinstance(exc, RemoteJobLedgerError) else type(exc).__name__,
            "job_id": job_id,
            "pass_id": pass_id,
        }
    started = time.monotonic()
    state = read_pass_state(Path(state_dir), pass_id) or {}
    token = _ACTIVE_COMPLETION_DEADLINE.set(started + completion_timeout_seconds(state))
    try:
        with _locked(
            Path(state_dir) / "remote-jobs" / f".{job_id}.completion.lock",
            timeout_seconds=_completion_remaining(),
        ):
            _completion_remaining()
            return _reharvest_terminal_pass_locked(
                job_id, pass_id=pass_id, state_dir=state_dir, orchestrator_root=orchestrator_root
            )
    finally:
        _ACTIVE_COMPLETION_DEADLINE.reset(token)


def _reharvest_terminal_pass_locked(
    job_id: str, *, pass_id: str, state_dir: Path, orchestrator_root: Path
) -> dict[str, Any]:
    """Re-harvest a stranded self-verify refusal from its retained artifacts, never the VM.

    Narrow by design. The ledger record must be this exact job for this exact pass,
    completed with rc 0, and the recorded terminal must be an inconclusive self-verify
    (the signature of a detached capture that was never consumed; a red verdict is not
    eligible). The retained ``result.json`` and ``--selfverify-out`` files must still be
    on disk. The superseded receipt is kept in the reopen marker, and then the ordinary
    harvest writes the new terminal receipt. No model, host test, or collection runs.
    """

    def refused(reason: str) -> dict[str, Any]:
        return {
            "ok": False,
            "terminal": True,
            "outcome": "reharvest_refused",
            "reason": reason,
            "job_id": job_id,
            "pass_id": pass_id,
        }

    ledger = RemoteJobLedger(Path(state_dir) / "remote-jobs")
    try:
        job = ledger.read_job(job_id)
    except (OSError, RemoteJobLedgerError) as exc:
        return refused(exc.reason if isinstance(exc, RemoteJobLedgerError) else type(exc).__name__)
    spec = job.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    argv = spec.get("argv")
    if (
        spec.get("pass_id") != pass_id
        or not isinstance(argv, list)
        or len(argv) < 2
        or argv[1] != "build"
        or not all(isinstance(spec.get(key), str) and spec.get(key) for key in ("task_ref", "lane_id"))
        or isinstance(spec.get("attempt"), bool)
        or not isinstance(spec.get("attempt"), int)
    ):
        return refused("identity_mismatch")
    identity = RemoteJobIdentity(
        task_ref=spec["task_ref"], lane_id=spec["lane_id"], pass_id=pass_id, attempt=spec["attempt"]
    )
    if identity.job_id != job_id:
        return refused("identity_mismatch")
    if job.get("stage") not in {"done", "completed"} or job.get("rc") != 0 or job.get("remote_state") == "lost":
        return refused("job_not_harvestable")

    pass_state = read_pass_state(Path(state_dir), pass_id) or {}
    recorded = pass_state.get("result")
    if pass_state.get("job_id") != job_id or not isinstance(recorded, dict) or recorded.get("job_id") != job_id:
        return refused("pass_identity_mismatch")

    def receipt_matches_identity(receipt: Any) -> bool:
        # Older receipts can omit labels, but present labels must agree with the
        # native identity recomputed from the ledger (including integer attempts).
        return (
            isinstance(receipt, dict)
            and receipt.get("job_id") == job_id
            and ("pass_id" not in receipt or receipt["pass_id"] == pass_id)
            and (
                "attempt" not in receipt
                or (
                    isinstance(receipt["attempt"], int)
                    and not isinstance(receipt["attempt"], bool)
                    and receipt["attempt"] == identity.attempt
                )
            )
        )

    if not receipt_matches_identity(recorded):
        return refused("pass_identity_mismatch")
    if recorded.get("outcome") != _REHARVEST_TRIGGER_OUTCOME or recorded.get("failed_stage") != "self_verify":
        return refused("not_a_stranded_self_verify_refusal")

    marker = pass_state.get(_REHARVEST_KEY)
    if _REHARVEST_KEY in pass_state:
        # Authenticate history before any result write. The immutable writer
        # would otherwise keep an invalid marker while losing the terminal.
        if (
            not isinstance(marker, dict)
            or marker.get("job_id") != job_id
            or not isinstance(marker.get("superseded_results"), list)
            or not marker["superseded_results"]
            or not all(receipt_matches_identity(receipt) for receipt in marker["superseded_results"])
        ):
            return refused("reharvest_marker_invalid")
    worktree_text = pass_state.get("worktree_path")
    if (
        not isinstance(worktree_text, str)
        or not worktree_text
        or not isinstance(pass_state.get("completion_config"), dict)
    ):
        return refused("pass_identity_incomplete")
    worktree = Path(worktree_text).expanduser().resolve()
    paths = _detached_artifact_paths(argv, worktree)
    if paths.get("result") is None or not paths["result"].is_file():
        return refused("retained_result_missing")
    if paths.get("selfverify") is None or not paths["selfverify"].is_file():
        return refused("retained_capture_missing")
    start_head = pass_state["completion_config"].get("start_head_ref")
    provenance_failure = _stamp_detached_source_provenance(
        {}, paths.get("provenance"), worktree_path=worktree, start_head=start_head
    )
    if provenance_failure is not None:
        return refused(f"source_provenance_{provenance_failure}")
    checkpoint_head = _recorded_detached_checkpoint(pass_state, job_id)
    if checkpoint_head is None:
        # Older terminal receipts predate the durable checkpoint field.
        checkpoints = recorded.get("checkpoint_commits")
        if isinstance(checkpoints, list) and checkpoints:
            checkpoint_head = checkpoints[-1]
    expected_tip = _git_stdout(worktree, "rev-parse", "HEAD")
    checkpoint_refusal = _detached_checkpoint_refusal(
        worktree_path=worktree,
        patch_file=paths.get("patch"),
        job_id=job_id,
        start_head=start_head,
        checkpoint_head=checkpoint_head,
        expected_tip=expected_tip,
    )
    if checkpoint_refusal is not None:
        return refused(checkpoint_refusal)

    superseded = list(marker["superseded_results"]) if isinstance(marker, dict) else []
    superseded.append(dict(recorded))
    write_pass_state(
        Path(state_dir),
        pass_id,
        {
            **pass_state,
            "status": "done",
            "result": {
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": spec["attempt"],
                "pass_id": pass_id,
                "task_ref": spec["task_ref"],
                "lane_id": spec["lane_id"],
                "summary": "Re-harvesting retained detached artifacts.",
            },
            _REHARVEST_KEY: {**(marker or {}), "job_id": job_id, "superseded_results": superseded},
            **(
                {"detached_checkpoint": {"job_id": job_id, "head": checkpoint_head or expected_tip}}
                if checkpoint_head is not None or expected_tip != start_head
                else {}
            ),
        },
    )
    # The same completion lock is still held; do not recursively acquire it.
    # Retained recovery starts no model work: a review needing a new turn is refused.
    fence_token = _fence_retained_reharvest_review(job_id)
    try:
        return _complete_remote_pass(job_id, state_dir=Path(state_dir), orchestrator_root=Path(orchestrator_root))
    finally:
        _RETAINED_REHARVEST_REVIEW_JOB.reset(fence_token)


RETAINED_REHARVEST_REVIEW_REQUIRED = "retained_reharvest_review_required"
_RETAINED_REHARVEST_REVIEW_JOB: ContextVar[str | None] = ContextVar("retained_reharvest_review_job", default=None)


class _RetainedReharvestReviewFence:
    """``run_review`` entry that refuses a new review model turn during retained reharvest.

    Outside a reharvest context every call is delegated unchanged, so ordinary and
    newly executed passes keep their review policy. Inside one, the runner's own
    typed refusal shape is returned before any prompt, prior-work lookup, or adapter
    turn; the engine records it as a non-retryable review refusal next to the landed
    checkpoint and verified gate evidence, which stay in custody for the independent
    gate. Reviews needing no model turn (review lanes and ``self_review: false``
    external-gate lanes) never reach the runner, so they still complete.
    """

    def __init__(self, run_review: Any) -> None:
        self.run_review = run_review

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        job_id = _RETAINED_REHARVEST_REVIEW_JOB.get()
        if job_id is None:
            return self.run_review(*args, **kwargs)
        return {
            "kind": "refused",
            "refusal_cause": RETAINED_REHARVEST_REVIEW_REQUIRED,
            "retryable": False,
            "review": "refused",
            "findings": [],
            "converged": False,
            "excluded_findings_count": 0,
            "out_of_diff_findings": [],
            "summary": (
                f"retained reharvest of job {job_id} refused a new review model turn; "
                "its verified checkpoint awaits independent review"
            ),
        }


def _fence_retained_reharvest_review(job_id: str) -> Any:
    """Interpose the fence on every loaded ``review_runner`` and scope it to this context.

    The worker review phase imports ``run_review`` from the bare ``review_runner``
    module on each call, so the fence sits on that attribute (and on the package
    identity, mirroring ``_review_admission_defer_reason``). It is installed once
    and never removed: removal would unfence a concurrent reharvest in another
    thread, while a delegating fence is inert for every other context. A failed
    import raises here, before the completion, so recovery stays fail-closed.
    """
    import importlib  # noqa: PLC0415

    _worker_daemon_module()  # puts the bare review_runner import path in place
    importlib.import_module("review_runner")
    for module_name in ("review_runner", "workbay_orchestrator_mcp.orchestration.review_runner"):
        module = sys.modules.get(module_name)
        current = getattr(module, "run_review", None) if module is not None else None
        if current is not None and not isinstance(current, _RetainedReharvestReviewFence):
            module.run_review = _RetainedReharvestReviewFence(current)
    return _RETAINED_REHARVEST_REVIEW_JOB.set(job_id)


def _lost_remote_job_terminal(job_id: str) -> dict[str, Any]:
    return {
        "ok": False,
        "outcome": "transport_failure",
        "failed_stage": "execute",
        "summary": "Detached remote job was lost before collection.",
        "error": f"remote job lost job_id={job_id}",
        "commit_landed": False,
        "work_status": "not_landed",
        "ceremony_status": "not_attempted",
        "findings": [],
    }


_COLLECT_STDERR_TAIL_BYTES = 2048
_COLLECT_STDERR_TRUNCATION_MARKER = "...[truncated]..."


def _collect_stderr_tail(stderr: object) -> str:
    """Return a bounded UTF-8 stderr tail for a failed collect; absent or non-text stderr yields ''."""
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    if not isinstance(stderr, str):
        return ""
    text = stderr.strip()
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= _COLLECT_STDERR_TAIL_BYTES:
        return text
    tail = encoded[-_COLLECT_STDERR_TAIL_BYTES:].decode("utf-8", errors="replace")
    return f"{_COLLECT_STDERR_TRUNCATION_MARKER}{tail}"


def _complete_remote_pass(job_id: str, *, state_dir: Path, orchestrator_root: Path) -> dict[str, Any]:
    """Idempotently collect and finish a previously submitted detached pass.

    A status timeout or ambiguous status remains pending. Only a confirmed
    ``done`` row is collected; a confirmed lost job and VM admission deferral
    become durable terminal outcomes.
    """
    from workbay_orchestrator_mcp.orchestration.adapters import remote_job_runner  # noqa: PLC0415

    resolved_state_dir = Path(state_dir)
    root = Path(orchestrator_root)
    ledger = RemoteJobLedger(resolved_state_dir / "remote-jobs")
    try:
        job = ledger.read_job(job_id)
    except (OSError, RemoteJobLedgerError) as exc:
        reason = exc.reason if isinstance(exc, RemoteJobLedgerError) else type(exc).__name__
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "summary": "Detached job ledger could not be read.",
            "error": f"detached job ledger is unavailable: {reason}",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }
    spec = job.get("spec")
    if not isinstance(spec, dict):
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "summary": "Detached job ledger record is malformed.",
            "error": "detached job ledger record has no job spec",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }
    task_ref = spec.get("task_ref")
    lane_id = spec.get("lane_id")
    pass_id = spec.get("pass_id")
    attempt = spec.get("attempt")
    argv = spec.get("argv")
    if (
        not all(isinstance(value, str) and value for value in (task_ref, lane_id, pass_id))
        or isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or isinstance(argv, list) is False
        or len(argv) < 2
        or not isinstance(argv[0], str)
    ):
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "summary": "Detached job ledger identity is malformed.",
            "error": "detached job ledger record has an invalid identity or build command",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }
    identity = RemoteJobIdentity(task_ref=task_ref, lane_id=lane_id, pass_id=pass_id, attempt=attempt)
    if identity.job_id != job_id or argv[1] != "build":
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "summary": "Detached job identity does not match its ledger record.",
            "error": "detached job id or build command does not match the persisted identity",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }

    pass_state = read_pass_state(resolved_state_dir, pass_id) or {}
    if pass_state.get("job_id") not in (None, job_id):
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached job does not match its pass-state fence.",
            "error": "detached job id conflicts with the persisted pass state",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }

    def persist_terminal(result: dict[str, Any]) -> dict[str, Any]:
        _completion_remaining()
        terminal = {
            **result,
            "ok": result.get("ok", result.get("outcome") == "handoff_ready"),
            "terminal": True,
            "job_id": job_id,
            "attempt": attempt,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
        }
        latest_pass_state = read_pass_state(resolved_state_dir, pass_id) or pass_state
        write_pass_state(
            resolved_state_dir,
            pass_id,
            {
                **latest_pass_state,
                "status": "done",
                "task_ref": task_ref,
                "lane_id": lane_id,
                "job_id": job_id,
                "attempt": attempt,
                "result": terminal,
            },
        )
        report_summary = str(terminal.get("summary") or terminal.get("error") or terminal.get("outcome"))
        _completion_remaining()
        _worker_daemon_module()._record_terminal_outcome(
            orchestrator_root=root,
            task_ref=task_ref,
            lane_id=lane_id,
            session=str(pass_state.get("session") or f"{task_ref}-{lane_id}"),
            summary=report_summary,
            outcome=str(terminal.get("outcome") or "error"),
        )
        latest = ledger.read_job(job_id)
        if latest.get("stage") != "completed":
            _completion_remaining()
            ledger.mark_completed(job_id)
        return terminal

    stage = job.get("stage")
    if stage == "done" and job.get("remote_state") == "lost":
        return persist_terminal(_lost_remote_job_terminal(job_id))

    recorded_result = pass_state.get("result")
    if pass_state.get("status") == "done" and isinstance(recorded_result, dict):
        recorded_outcome = recorded_result.get("outcome")
        if recorded_outcome != OffloadOutcome.REMOTE_JOB_PENDING.value:
            return persist_terminal(dict(recorded_result)) if job.get("stage") != "completed" else dict(recorded_result)
    reharvest_open = _reharvest_open(pass_state, job_id)
    if job.get("stage") == "completed" and not reharvest_open:
        return {
            "ok": False,
            "terminal": True,
            "outcome": "transport_failure",
            "failed_stage": "execute",
            "job_id": job_id,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached job is complete but its pass outcome is missing.",
            "error": "completed ledger record has no terminal pass result",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "failed",
            "findings": [],
        }

    worktree_text = pass_state.get("worktree_path")
    completion_config = pass_state.get("completion_config")
    if not isinstance(worktree_text, str) or not worktree_text or not isinstance(completion_config, dict):
        failure = {
            "ok": False,
            "terminal": True,
            "outcome": "needs_guidance",
            "failed_stage": "execute",
            "job_id": job_id,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached pass cannot be resumed because its execution identity is incomplete.",
            "error": "persisted detached pass is missing worktree_path or completion_config",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }
        if job.get("stage") in {"intent", "submitted"}:
            _completion_remaining()
            ledger.mark_done(job_id, 2)
        return persist_terminal(failure)
    worktree_path = Path(worktree_text).expanduser().resolve()
    if not worktree_path.is_dir():
        failure = {
            "ok": False,
            "terminal": True,
            "outcome": "worktree_unrecoverable",
            "failed_stage": "execute",
            "job_id": job_id,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached pass worktree is unavailable for harvest.",
            "error": f"detached pass worktree does not exist: {worktree_path}",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "not_attempted",
            "findings": [],
        }
        if job.get("stage") in {"intent", "submitted"}:
            _completion_remaining()
            ledger.mark_done(job_id, 2)
        return persist_terminal(failure)

    env = {**os.environ, **identity.to_env(), "WORKBAY_HANDOFF_STATE_DIR": str(resolved_state_dir)}
    timeout_value = spec.get("bound_seconds")
    timeout = float(timeout_value) if isinstance(timeout_value, int) and not isinstance(timeout_value, bool) else 60.0
    timeout = max(1.0, min(timeout, 300.0))
    if stage == "done" or reharvest_open:
        vm_rc = job.get("rc")
    else:
        status_command = [argv[0], "status", "--job-id", job_id]
        try:
            status = remote_job_runner._run_child(
                status_command,
                cwd=str(worktree_path),
                env=env,
                timeout=min(timeout, 30.0, _completion_remaining()),
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": True,
                "terminal": False,
                "status": "running",
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": attempt,
                "pass_id": pass_id,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "summary": "Detached job status is unknown; ask again.",
            }
        if status.returncode != 0:
            return {
                "ok": True,
                "terminal": False,
                "status": "running",
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": attempt,
                "pass_id": pass_id,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "summary": "Detached job status could not be confirmed; ask again.",
                "status_error": (status.stderr or "").strip()[-500:],
            }
        try:
            status_row = json.loads(status.stdout or "")
        except (TypeError, json.JSONDecodeError):
            status_row = None
        if not isinstance(status_row, dict) or status_row.get("job_id") != job_id:
            status_row = None
        if status_row is None or status_row.get("observer_error") is not None:
            return {
                "ok": True,
                "terminal": False,
                "status": "running",
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": attempt,
                "pass_id": pass_id,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "summary": "Detached job status is unknown; ask again.",
            }
        remote_state = status_row.get("state")
        if remote_state == "lost":
            _completion_remaining()
            ledger.mark_done(job_id, 1, lost=True)
            return persist_terminal(_lost_remote_job_terminal(job_id))
        if remote_state != "done":
            return {
                "ok": True,
                "terminal": False,
                "status": "running",
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": attempt,
                "pass_id": pass_id,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "summary": "Detached remote job is still open.",
            }
        vm_rc = status_row.get("rc")
        if isinstance(vm_rc, bool) or not isinstance(vm_rc, int):
            return {
                "ok": True,
                "terminal": False,
                "status": "running",
                "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
                "job_id": job_id,
                "attempt": attempt,
                "pass_id": pass_id,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "summary": "Detached job exit status is unknown; ask again.",
            }
        _completion_remaining()
        ledger.mark_done(job_id, vm_rc)

    if isinstance(vm_rc, bool) or not isinstance(vm_rc, int):
        return {
            "ok": True,
            "terminal": False,
            "status": "running",
            "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
            "job_id": job_id,
            "attempt": attempt,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached job exit status is unknown; ask again.",
        }
    if vm_rc == 75:
        return persist_terminal(
            {
                "ok": False,
                "outcome": OffloadOutcome.ADMISSION_DEFERRED.value,
                "failed_stage": "execute",
                "reason": "vm_admission_refused",
                "summary": admission_defer_summary(str(spec.get("backend") or "remote"), "vm_admission_refused"),
                "error": admission_defer_summary(str(spec.get("backend") or "remote"), "vm_admission_refused"),
                "commit_landed": False,
                "work_status": "not_landed",
                "ceremony_status": "not_attempted",
                "findings": [],
            }
        )
    if vm_rc != 0:
        return persist_terminal(
            {
                "ok": False,
                "outcome": "transport_failure",
                "failed_stage": "execute",
                "summary": "Detached remote job exited unsuccessfully.",
                "error": f"detached remote job exited rc={vm_rc}",
                "commit_landed": False,
                "work_status": "not_landed",
                "ceremony_status": "not_attempted",
                "findings": [],
            }
        )

    try:
        # A re-harvest consumes the artifacts already retained locally (validated by
        # reharvest_terminal_pass); the VM is not contacted again for them.
        collected = (
            subprocess.CompletedProcess(argv, 0, "", "")
            if reharvest_open
            else remote_job_runner.collect_only(
                argv,
                cwd=str(worktree_path),
                env=env,
                timeout=min(timeout, _completion_remaining()),
            )
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": True,
            "terminal": False,
            "status": "running",
            "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
            "job_id": job_id,
            "attempt": attempt,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached job collection timed out; ask again.",
        }
    if collected.returncode == 75:
        return {
            "ok": True,
            "terminal": False,
            "status": "running",
            "outcome": OffloadOutcome.REMOTE_JOB_PENDING.value,
            "job_id": job_id,
            "attempt": attempt,
            "pass_id": pass_id,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "summary": "Detached job is not ready to collect; ask again.",
        }
    if collected.returncode != 0:
        return persist_terminal(
            {
                "ok": False,
                "outcome": "transport_failure",
                "failed_stage": "execute",
                "summary": "Detached remote job artifacts could not be collected.",
                "error": f"detached remote job collection exited rc={collected.returncode}",
                # Same key _is_transport_only_carrier reads as adapter-stamped stderr provenance.
                "transport_stderr_tail": _collect_stderr_tail(collected.stderr),
                "commit_landed": False,
                "work_status": "not_landed",
                "ceremony_status": "not_attempted",
                "findings": [],
            }
        )

    engine_args = dict(completion_config)
    _completion_remaining()
    engine_args.pop("start_head_ref", None)
    engine_args.update(
        {
            "orchestrator_root": root,
            "task_ref": task_ref,
            "lane_id": lane_id,
            "session": str(pass_state.get("session") or f"{task_ref}-{lane_id}"),
            "worktree_path": worktree_path,
            "pass_id": pass_id,
            "state_dir": resolved_state_dir,
            "test_cmd": completion_config.get("test_cmd"),
            "detached_completion_job_id": job_id,
            "detached_completion_start_head": completion_config.get("start_head_ref"),
        }
    )
    # A reopened retained receipt is artifact-only on every completion entry. The
    # direct reharvest's own fence ends with its call, so a collector replaying an
    # interrupted reharvest takes the restriction from the durable reopen marker.
    fence_token = _fence_retained_reharvest_review(job_id) if reharvest_open else None
    try:
        result = run_offload_pass_engine(**engine_args)
    finally:
        if fence_token is not None:
            _RETAINED_REHARVEST_REVIEW_JOB.reset(fence_token)
    _completion_remaining()
    if isinstance(result, dict) and result.get("terminal") is False:
        return result
    if not isinstance(result, dict):
        result = {
            "ok": False,
            "outcome": "error",
            "failed_stage": "execute",
            "summary": "Detached pass completion returned an invalid result.",
            "error": "offload engine returned a non-object completion result",
            "commit_landed": False,
            "work_status": "not_landed",
            "ceremony_status": "failed",
            "findings": [],
        }
    return persist_terminal(result)
