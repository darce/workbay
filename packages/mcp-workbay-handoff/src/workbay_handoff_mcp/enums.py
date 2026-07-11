from __future__ import annotations

import re
from enum import StrEnum

_MODEL_LABEL_PATTERNS = (
    (
        # Claude model IDs have two minor-version conventions:
        #   1. dotted:         claude-opus-4.1          → Claude Opus 4.1
        #   2. dash-separated: claude-opus-4-7          → Claude Opus 4.7
        # Dash-separated minor must not collide with date suffixes:
        #   claude-opus-4-0520  → Claude Opus 4  (0520 is a date, not minor)
        #   claude-sonnet-4-20250514 → Claude Sonnet 4
        # The (?!\d) negative lookahead bounds the minor to 1-2 digits followed
        # by a non-digit boundary, which date suffixes (4+ digits) cannot satisfy.
        re.compile(
            r"^claude-(opus|sonnet|haiku)-(\d+(?:\.\d+)?)(?:-(\d{1,2})(?!\d))?(?:[-_].*)?$",
            re.IGNORECASE,
        ),
        lambda match: (
            f"Claude {match.group(1).title()} {match.group(2)}" + (f".{match.group(3)}" if match.group(3) else "")
        ),
    ),
    (
        re.compile(r"^gpt-(\d+(?:\.\d+)?)(?:[-_].*)?$", re.IGNORECASE),
        lambda match: f"GPT-{match.group(1)}",
    ),
    (
        re.compile(r"^(o\d+)(?:[-_].*)?$", re.IGNORECASE),
        lambda match: match.group(1).lower(),
    ),
    (
        # grok-composer-<ver>[-fast] -> "Grok Composer <ver>[ Fast]". Recognition
        # only (implementation note D5): gives grok worker rows a canonical, single-sourced
        # label so D4 can derive the attribution identity via
        # normalize_model_identity. Versions follow the claude conventions:
        # multi-dot (grok-composer-2.5.1) and dash-separated minor
        # (grok-composer-2-5 → 2.5, with the same (?!\d) bound so date suffixes
        # are not mistaken for a minor). The "-fast" tier is recognized either
        # version-adjacent or trailing after intermediate segments (e.g. a
        # date); the lazy middle group keeps a trailing -fast from being
        # swallowed as suffix. Non-composer grok slugs fall through to
        # verbatim passthrough.
        re.compile(
            r"^grok-composer-(\d+(?:\.\d+)*)(?:-(\d{1,2})(?!\d))?(-fast)?(?:[-_].*?)?(-fast)?$",
            re.IGNORECASE,
        ),
        lambda match: (
            f"Grok Composer {match.group(1)}"
            + (f".{match.group(2)}" if match.group(2) else "")
            + (" Fast" if match.group(3) or match.group(4) else "")
        ),
    ),
    (
        # grok-build -> "Grok Build" so any Composer-only contamination that
        # reaches an audit trail is legible rather than a raw slug.
        re.compile(r"^grok-build(?:[-_].*)?$", re.IGNORECASE),
        lambda _match: "Grok Build",
    ),
)
_NON_IDENTITY_REASONING_LEVELS = frozenset({"auto", "default", "inherit"})


def normalize_model_label(model: str | None) -> str | None:
    normalized = str(model or "").strip()
    if not normalized:
        return None
    for pattern, render_label in _MODEL_LABEL_PATTERNS:
        match = pattern.match(normalized)
        if match is not None:
            return render_label(match)
    return normalized


def normalize_reasoning_level(reasoning_level: str | None) -> str | None:
    normalized = str(reasoning_level or "").strip().lower()
    if not normalized:
        return None
    return normalized


def normalize_model_identity(model_label: str | None, reasoning_level: str | None) -> str | None:
    normalized_label = str(model_label or "").strip() or None
    normalized_reasoning = normalize_reasoning_level(reasoning_level)
    if normalized_reasoning in _NON_IDENTITY_REASONING_LEVELS:
        normalized_reasoning = None
    if normalized_label and normalized_reasoning:
        return f"{normalized_label} {normalized_reasoning}"
    return normalized_label


class HandoffStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    REVIEW = "review"
    DONE = "done"


class BlockerStatus(StrEnum):
    OPEN = "open"
    RESOLVED = "resolved"


class ActionStatus(StrEnum):
    PENDING = "pending"
    DONE = "done"
    SKIPPED = "skipped"


class FindingStatus(StrEnum):
    OPEN = "open"
    FIXED = "fixed"
    WONTFIX = "wontfix"
    DEFERRED = "deferred"
    # internal: two-state lifecycle. RESOLVED_ON_BRANCH is anchored on the
    # actor (or operator-verified) commit; INTEGRATED is integrate-managed and
    # must not be set via a direct ``update`` write.
    RESOLVED_ON_BRANCH = "resolved_on_branch"
    INTEGRATED = "integrated"
    SUPERSEDED = "superseded"


class FindingSeverity(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ReviewMode(StrEnum):
    BRANCH = "branch"
    RELEASE_AUDIT = "release_audit"
    PLANNING = "planning"


class ReviewKind(StrEnum):
    BRANCH = "branch"
    PLANNING = "planning"


class ReviewScopeSource(StrEnum):
    SLICE_PACKET = "slice_packet"
    BRANCH_DIFF = "branch_diff"


class LaneStatus(StrEnum):
    PLANNED = "planned"
    ACTIVE = "active"
    BLOCKED = "blocked"
    REVIEW = "review"
    MERGED = "merged"
    CLOSED = "closed"


class ReportStatus(StrEnum):
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    SUPERSEDED = "superseded"


class MessageStatus(StrEnum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    CLOSED = "closed"


class LaneMessageDirection(StrEnum):
    ORCHESTRATOR_TO_WORKER = "orchestrator_to_worker"
    WORKER_TO_ORCHESTRATOR = "worker_to_orchestrator"


class PlanCursorState(StrEnum):
    DISPATCHED = "dispatched"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    ESCALATED = "escalated"
    EXPIRED = "expired"


class WorkerEventName(StrEnum):
    DAEMON_START = "daemon_start"
    DAEMON_STOP = "daemon_stop"
    HANDOFF_SUBPROCESS_FAILED = "handoff_subprocess_failed"
    HANDOFF_RETRY_START = "handoff_retry_start"
    HANDOFF_RETRY_COMPLETE = "handoff_retry_complete"
    HANDOFF_RETRY_FAILED = "handoff_retry_failed"
    DORMANT_ENTERED = "dormant_entered"
    DORMANT_EXITED = "dormant_exited"
    POLL_ERROR = "poll_error"
    MCP_BACKEND_OVERRIDE = "mcp_backend_override"
    MCP_MODEL_OVERRIDE = "mcp_model_override"
    MCP_EFFORT_OVERRIDE = "mcp_effort_override"
    CYCLE_START = "cycle_start"
    FIX_PROMPT_FAILED = "fix_prompt_failed"
    REASONING_EFFORT_SELECTED = "reasoning_effort_selected"
    EXEC_SPAWNED = "exec_spawned"
    EXEC_HEARTBEAT = "exec_heartbeat"
    SUBAGENT_TURN_OBSERVED = "subagent_turn_observed"
    SUBAGENT_TURN_COMPLETE = "subagent_turn_complete"
    EXEC_START = "exec_start"
    EXEC_FAILED = "exec_failed"
    EXEC_COMPLETE = "exec_complete"
    ARTIFACT_INDEXED = "artifact_indexed"
    CONTEXT_PRESSURE = "context_pressure"
    NEEDS_GUIDANCE = "needs_guidance"
    HANDOFF_FAILED = "handoff_failed"
    SCOPE_VIOLATION = "scope_violation"
    REVIEW_START = "review_start"
    REVIEW_FAILED = "review_failed"
    REVIEW_COMPLETE = "review_complete"
    FINDING_DIFF = "finding_diff"
    VERIFICATION_START = "verification_start"
    VERIFICATION_COMPLETE = "verification_complete"
    FIX_CYCLE_NEEDED = "fix_cycle_needed"
    REVIEW_EXHAUSTED = "review_exhausted"
    TOKEN_BUDGET_EXCEEDED = "token_budget_exceeded"
    EXHAUSTION_STREAK = "exhaustion_streak"
    LANE_EXHAUSTION_FORCED_STOP = "lane_exhaustion_forced_stop"
    POLL_SLEEP = "poll_sleep"
