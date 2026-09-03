"""Shared terminal-convergence contracts (implementation note S1a).

These models are the exact-SHA wire types for worker outcomes, review
attempts, candidate disposition, merge capability, semantic context, and
ship-stage cleanup. Persistence and orchestration live elsewhere; this
module only validates payload shape and structurally illegal combinations.

Canon:
- Exact SHA is identity; a branch name is never a substitute.
- Landed work and ceremony status are independent.
- Legacy candidate state stays ``unknown`` until an explicit initialize.
- Remote cleanup is ``not_applicable`` iff no remote receipts exist.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StringConstraints,
    WithJsonSchema,
    model_validator,
)

from .handoff import TaskRef

# Portable Draft 2020-12 string constraints. Python `$` matches before a
# final LF, so fixed-length types pin minLength/maxLength and variable
# types add `not: {pattern: CR/LF}`. Do not emit Python-only `\Z`.
_CRLF_PATTERN = r"[\r\n]"
_SHA40_PATTERN = r"^[0-9a-f]{40}$"
_SHA40_SCHEMA = {
    "maxLength": 40,
    "minLength": 40,
    "pattern": _SHA40_PATTERN,
    "type": "string",
}
Sha40 = Annotated[
    str,
    StringConstraints(max_length=40, min_length=40, pattern=_SHA40_PATTERN),
    WithJsonSchema(_SHA40_SCHEMA),
]
_SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
_SHA256_SCHEMA = {
    "maxLength": 71,
    "minLength": 71,
    "pattern": _SHA256_PATTERN,
    "type": "string",
}
Sha256Digest = Annotated[
    str,
    StringConstraints(max_length=71, min_length=71, pattern=_SHA256_PATTERN),
    WithJsonSchema(_SHA256_SCHEMA),
]
_NONBLANK_REF_PATTERN = r"^[^\r\n]*\S[^\r\n]*$"
_NONBLANK_REF_RE = re.compile(_NONBLANK_REF_PATTERN)
_NONBLANK_REF_SCHEMA = {
    "minLength": 1,
    "not": {"pattern": _CRLF_PATTERN},
    "pattern": _NONBLANK_REF_PATTERN,
    "type": "string",
}
_UNIT_SCORE_SCHEMA = {
    "maximum": 1.0,
    "minimum": -1.0,
    "type": "number",
}
_MERGE_CAPABILITY_BOUND_FIELDS = (
    "task_ref",
    "candidate_sha",
    "review_run_id",
    "close_check_id",
    "verified_test_ids",
    "open_findings",
)
# Timezone-bearing RFC 3339 / ISO-8601. Draft 2020-12 ignores `format`
# unless a format checker is installed, so the pattern is the schema
# enforcement. Compatible with AwareDatetime receipts (`...+00:00` / `Z`).
# Runtime string input uses this same pattern; do not add a second grammar.
_AWARE_DATETIME_PATTERN = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
)
_AWARE_DATETIME_RE = re.compile(_AWARE_DATETIME_PATTERN)
_AWARE_DATETIME_SCHEMA = {
    "format": "date-time",
    "not": {"pattern": _CRLF_PATTERN},
    "pattern": _AWARE_DATETIME_PATTERN,
    "type": "string",
}


def _require_aware_rfc3339_grammar(value: object) -> object:
    if isinstance(value, str) and _AWARE_DATETIME_RE.fullmatch(value) is None:
        raise ValueError(
            "timestamp must be timezone-bearing RFC3339 (T separator, Z or ±HH:MM)"
        )
    return value


def _reject_json_boolean(value: object) -> object:
    if type(value) is bool:
        raise ValueError("integer evidence cannot be a JSON boolean")
    return value


def _is_nonblank_ref(value: object) -> bool:
    return isinstance(value, str) and _NONBLANK_REF_RE.fullmatch(value) is not None


AwareRfc3339 = Annotated[
    AwareDatetime,
    BeforeValidator(_require_aware_rfc3339_grammar),
    WithJsonSchema(_AWARE_DATETIME_SCHEMA),
]
_CLOSED_TIP_DISPOSITIONS = ("integrated", "rejected_with_named_evidence")
_TERMINAL_REVIEW_KINDS = frozenset({"verdict", "timeout", "unparseable"})


def _json_schema_all_of(
    *clauses: dict[str, Any],
) -> Callable[[dict[str, Any]], None]:
    def _apply(schema: dict[str, Any]) -> None:
        schema.setdefault("allOf", []).extend(clauses)

    return _apply


CeremonyKind = Literal[
    "stopped",
    "exhausted",
    "failed",
    "transport_closed",
    "compaction_failed",
    "mcp_write_rejected",
]
ReviewTerminalKind = Literal["verdict", "timeout", "unparseable", "transport_failure"]
ReviewVerdict = Literal["pass", "pass_with_findings", "fail"]
TipDispositionKind = Literal[
    "unknown",
    "undisposed",
    "integrated",
    "rejected_with_named_evidence",
]
LaneContextStatus = Literal["selected", "unavailable"]
InvocationMode = Literal["onnx_query", "stored_vector_query", "degraded"]
ScoreSemantics = Literal["cosine_rank"]
RemoteCleanupStatus = Literal["not_applicable", "required", "verified", "failed"]


class CandidateControlState(str, Enum):
    """Finite candidate control machine. Legacy rows read as ``unknown``."""

    unknown = "unknown"
    implementing = "implementing"
    candidate_pending = "candidate_pending"
    candidate_validated = "candidate_validated"
    reviewed = "reviewed"
    shipping = "shipping"
    done = "done"
    rejected_with_evidence = "rejected_with_evidence"
    blocked_external = "blocked_external"


class CandidateEvent(str, Enum):
    """Events admitted by the candidate control machine."""

    initialize = "initialize"
    land = "land"
    validate = "validate"
    review = "review"
    ship = "ship"
    reject = "reject"
    complete = "complete"
    block = "block"
    unblock = "unblock"


_BLOCK_RESTORE_STATES = frozenset(
    {
        CandidateControlState.implementing,
        CandidateControlState.candidate_pending,
        CandidateControlState.candidate_validated,
        CandidateControlState.reviewed,
        CandidateControlState.shipping,
        CandidateControlState.rejected_with_evidence,
    }
)

_TERMINAL_STATES = frozenset({CandidateControlState.done})

_LEGAL_TRANSITIONS: dict[
    tuple[CandidateControlState, CandidateEvent], CandidateControlState
] = {
    (
        CandidateControlState.unknown,
        CandidateEvent.initialize,
    ): CandidateControlState.implementing,
    (
        CandidateControlState.implementing,
        CandidateEvent.land,
    ): CandidateControlState.candidate_pending,
    (CandidateControlState.candidate_pending, CandidateEvent.validate): (
        CandidateControlState.candidate_validated
    ),
    (
        CandidateControlState.candidate_validated,
        CandidateEvent.review,
    ): CandidateControlState.reviewed,
    (
        CandidateControlState.reviewed,
        CandidateEvent.ship,
    ): CandidateControlState.shipping,
    (
        CandidateControlState.shipping,
        CandidateEvent.complete,
    ): CandidateControlState.done,
    (CandidateControlState.candidate_pending, CandidateEvent.reject): (
        CandidateControlState.rejected_with_evidence
    ),
    (CandidateControlState.candidate_validated, CandidateEvent.reject): (
        CandidateControlState.rejected_with_evidence
    ),
    (CandidateControlState.reviewed, CandidateEvent.reject): (
        CandidateControlState.rejected_with_evidence
    ),
    (CandidateControlState.rejected_with_evidence, CandidateEvent.initialize): (
        CandidateControlState.implementing
    ),
}


def _admitted_to_state(
    state: CandidateControlState,
    event: CandidateEvent,
    *,
    blocked_from_state: CandidateControlState | None = None,
) -> CandidateControlState | None:
    """Return the unique admitted target, or ``None`` if the pair is refused.

    Shared by the state-first oracle and ``TransitionAdmission`` so proof
    construction cannot invent a triple the oracle would not emit.
    """
    if (
        state is CandidateControlState.unknown
        and event is not CandidateEvent.initialize
    ):
        return None
    if event is CandidateEvent.block and state not in _TERMINAL_STATES:
        return CandidateControlState.blocked_external
    if event is CandidateEvent.unblock:
        if (
            state is CandidateControlState.blocked_external
            and blocked_from_state is not None
            and blocked_from_state in _BLOCK_RESTORE_STATES
        ):
            return blocked_from_state
        return None
    return _LEGAL_TRANSITIONS.get((state, event))


class ShipStage(str, Enum):
    """Ordered ship-transaction cursor. ``wb ship`` persists one of these."""

    preflight = "preflight"
    merge = "merge"
    integration_verify = "integration_verify"
    mark_done = "mark_done"
    close_lanes = "close_lanes"
    archive = "archive"
    remove_worktree = "remove_worktree"
    delete_branch = "delete_branch"
    local_reap = "local_reap"
    remote_reap_verify = "remote_reap_verify"
    render = "render"
    complete = "complete"


class TransitionRefusedError(ValueError):
    """Undefined ``(state, event)`` pair; callers must record telemetry."""


class _ValidatedEvolutionMixin:
    """Refuse unvalidated ``model_copy(update=...)``; require ``validated_copy``."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
    )

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy this instance without bypassing ownership validators.

        Pydantic's default ``model_copy(update=...)`` constructs without
        validators. That update path is refused; callers must use
        ``validated_copy``. A no-update copy remains a safe clone of an
        already-valid instance.
        """
        if update:
            raise ValueError(
                f"{type(self).__name__}.model_copy(update=...) bypasses validators; "
                "use validated_copy"
            )
        return super().model_copy(deep=deep)  # type: ignore[misc]

    def validated_copy(self, **updates: Any) -> Self:
        """Return a re-validated copy.

        Direct field updates must go through this helper. Raw
        ``model_copy(update=...)`` is refused because it bypasses validators.
        Nested instances are rejected or accepted by
        ``revalidate_instances="always"`` during ``model_validate``.
        """
        payload = self.model_dump()  # type: ignore[attr-defined]
        payload.update(updates)
        return type(self).model_validate(payload)  # type: ignore[attr-defined]


class TransitionAdmission(_ValidatedEvolutionMixin, BaseModel):
    """Successful state-first admission of one candidate event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_state: CandidateControlState
    event: CandidateEvent
    to_state: CandidateControlState

    @model_validator(mode="after")
    def _triple_is_admitted(self) -> TransitionAdmission:
        blocked_from_state = (
            self.to_state if self.event is CandidateEvent.unblock else None
        )
        expected = _admitted_to_state(
            self.from_state,
            self.event,
            blocked_from_state=blocked_from_state,
        )
        if expected is not self.to_state:
            raise ValueError(
                "inadmissible transition "
                f"{self.from_state.value} + {self.event.value} -> "
                f"{self.to_state.value}"
            )
        return self


class LandedWork(_ValidatedEvolutionMixin, BaseModel):
    """Product-work provenance, independent of orchestration ceremony."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commit_sha: Sha40 | None = Field(
        default=None,
        description="Exact 40-char lowercase hex SHA of the landed commit, if any.",
    )
    ref: str | None = Field(
        default=None, description="Git ref that holds the landed commit."
    )
    tests_passed: StrictBool | None = Field(
        default=None,
        description="Whether the worker's recorded tests passed for this tip.",
    )


class CeremonyStatus(_ValidatedEvolutionMixin, BaseModel):
    """Orchestration/reporting ceremony, independent of landed work."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: CeremonyKind | None = Field(
        default=None,
        description="How the worker run ended as ceremony; null when ceremony succeeded.",
    )
    retryable: StrictBool = Field(
        default=False, description="Whether the ceremony failure is retryable."
    )


class WorkerOutcomeV2(_ValidatedEvolutionMixin, BaseModel):
    """Typed worker result: landed work and ceremony status are independent."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra=_json_schema_all_of(
            {
                "if": {
                    "properties": {"merge_ready": {"const": True}},
                    "required": ["merge_ready"],
                },
                "then": {
                    "properties": {
                        "landed": {
                            "properties": {
                                "commit_sha": _SHA40_SCHEMA,
                                "ref": _NONBLANK_REF_SCHEMA,
                            },
                            "required": ["commit_sha", "ref"],
                        }
                    }
                },
            }
        ),
    )

    landed: LandedWork
    ceremony: CeremonyStatus
    merge_ready: StrictBool = Field(
        default=False,
        description="True only when this outcome is an explicit merge-ready claim.",
    )

    @model_validator(mode="after")
    def _merge_ready_requires_identity(self) -> WorkerOutcomeV2:
        if self.merge_ready:
            if self.landed.commit_sha is None:
                raise ValueError("merge_ready requires landed.commit_sha")
            if not _is_nonblank_ref(self.landed.ref):
                raise ValueError("merge_ready requires landed.ref")
        return self


class PartialFinding(_ValidatedEvolutionMixin, BaseModel):
    """Salvaged finding preserved independently of verdict serialization."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    stable_id: str = Field(min_length=1)
    severity: str = Field(min_length=1)
    description: str
    source_offset: StrictInt = Field(ge=0)


class ReviewAttemptOutcomeV2(_ValidatedEvolutionMixin, BaseModel):
    """One review attempt against an exact candidate SHA and bounded lens."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra=_json_schema_all_of(
            {
                "if": {
                    "properties": {"terminal_kind": {"const": "verdict"}},
                    "required": ["terminal_kind"],
                },
                "then": {
                    "properties": {
                        "verdict": {
                            "enum": ["pass", "pass_with_findings", "fail"],
                            "type": "string",
                        }
                    },
                    "required": ["verdict"],
                },
                "else": {"properties": {"verdict": {"type": "null"}}},
            },
            {
                "if": {
                    "properties": {
                        "terminal_kind": {"enum": ["verdict", "unparseable"]}
                    },
                    "required": ["terminal_kind"],
                },
                "then": {
                    "properties": {
                        "analysis_ended_at": _AWARE_DATETIME_SCHEMA,
                        "finalization_started_at": _AWARE_DATETIME_SCHEMA,
                    },
                    "required": ["analysis_ended_at", "finalization_started_at"],
                },
            },
            {
                "if": {
                    "properties": {"terminal_kind": {"const": "timeout"}},
                    "required": ["terminal_kind"],
                },
                "then": {
                    "properties": {"analysis_ended_at": _AWARE_DATETIME_SCHEMA},
                    "required": ["analysis_ended_at"],
                },
            },
            {
                "if": {
                    "properties": {"terminal_kind": {"const": "transport_failure"}},
                    "required": ["terminal_kind"],
                },
                "then": {
                    "properties": {
                        "analysis_ended_at": {"type": "null"},
                        "finalization_started_at": {"type": "null"},
                    }
                },
            },
            {
                "if": {
                    "properties": {
                        "terminal_kind": {"enum": ["verdict", "timeout", "unparseable"]}
                    },
                    "required": ["terminal_kind"],
                },
                "then": {"properties": {"retryable": {"const": False}}},
            },
        ),
    )

    candidate_sha: Sha40
    lens_id: str = Field(min_length=1)
    input_digest: Sha256Digest
    input_bytes: StrictInt = Field(gt=0)
    finding_fingerprint: str = Field(min_length=1)
    terminal_kind: ReviewTerminalKind
    verdict: ReviewVerdict | None = None
    partial_findings: tuple[PartialFinding, ...] = Field(default_factory=tuple)
    analysis_budget_seconds: StrictInt = Field(gt=0)
    finalization_budget_seconds: StrictInt = Field(gt=0)
    analysis_ended_at: AwareRfc3339 | None = None
    finalization_started_at: AwareRfc3339 | None = None
    retryable: StrictBool = False

    @model_validator(mode="after")
    def _receipt_invariants(self) -> ReviewAttemptOutcomeV2:
        if self.terminal_kind == "verdict":
            if self.verdict is None:
                raise ValueError("verdict terminal_kind requires a verdict")
        elif self.verdict is not None:
            raise ValueError("non-verdict terminal_kind cannot carry a verdict")
        if self.terminal_kind == "transport_failure":
            if (
                self.analysis_ended_at is not None
                or self.finalization_started_at is not None
            ):
                raise ValueError(
                    "transport_failure requires analysis_ended_at and "
                    "finalization_started_at to be null"
                )
            return self
        if self.analysis_ended_at is None:
            raise ValueError(f"{self.terminal_kind} requires analysis_ended_at")
        if (
            self.terminal_kind in {"verdict", "unparseable"}
            and self.finalization_started_at is None
        ):
            raise ValueError(f"{self.terminal_kind} requires finalization_started_at")
        if (
            self.finalization_started_at is not None
            and self.finalization_started_at < self.analysis_ended_at
        ):
            raise ValueError("finalization_started_at must be >= analysis_ended_at")
        if self.terminal_kind in _TERMINAL_REVIEW_KINDS and self.retryable:
            raise ValueError(
                f"{self.terminal_kind} is terminal and requires retryable=false"
            )
        return self


class LandedTip(_ValidatedEvolutionMixin, BaseModel):
    """One landed tip under a candidate, with a monotone disposition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commit_sha: Sha40
    ref: str | None = None
    disposition: TipDispositionKind


class CandidateDisposition(_ValidatedEvolutionMixin, BaseModel):
    """Exact-SHA candidate record. Legacy rows remain ``unknown`` until initialized."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra=_json_schema_all_of(
            {
                "if": {"properties": {"control_state": {"const": "unknown"}}},
                "then": {
                    "properties": {
                        "blocked_from_state": {"type": "null"},
                        "landed_tips": {
                            "items": {
                                "properties": {"disposition": {"const": "unknown"}}
                            }
                        },
                    }
                },
                "else": {
                    "properties": {"candidate_sha": _SHA40_SCHEMA},
                    "required": ["candidate_sha"],
                },
            },
            {
                "if": {
                    "properties": {"control_state": {"const": "blocked_external"}},
                    "required": ["control_state"],
                },
                "then": {
                    "properties": {
                        "blocked_from_state": {
                            "enum": sorted(
                                state.value for state in _BLOCK_RESTORE_STATES
                            ),
                            "type": "string",
                        }
                    },
                    "required": ["blocked_from_state"],
                },
                "else": {"properties": {"blocked_from_state": {"type": "null"}}},
            },
            {
                "if": {
                    "properties": {"control_state": {"enum": ["done", "shipping"]}},
                    "required": ["control_state"],
                },
                "then": {
                    "properties": {
                        "landed_tips": {
                            "items": {
                                "properties": {
                                    "disposition": {
                                        "enum": list(_CLOSED_TIP_DISPOSITIONS)
                                    }
                                },
                                "required": ["disposition"],
                            },
                            "minItems": 1,
                            "type": "array",
                        }
                    },
                    "required": ["landed_tips"],
                },
            },
            {
                "if": {
                    "properties": {
                        "control_state": {"const": "rejected_with_evidence"}
                    },
                    "required": ["control_state"],
                },
                "then": {
                    "properties": {
                        "landed_tips": {
                            "contains": {
                                "properties": {
                                    "disposition": {
                                        "const": "rejected_with_named_evidence"
                                    }
                                },
                                "required": ["disposition"],
                            },
                            "items": {
                                "properties": {
                                    "disposition": {
                                        "enum": list(_CLOSED_TIP_DISPOSITIONS)
                                    }
                                },
                                "required": ["disposition"],
                            },
                            "minContains": 1,
                            "minItems": 1,
                            "type": "array",
                        }
                    },
                    "required": ["landed_tips"],
                },
            },
        ),
    )

    task_ref: TaskRef
    candidate_sha: Sha40 | None = None
    landed_tips: tuple[LandedTip, ...] = Field(default_factory=tuple)
    control_state: CandidateControlState = CandidateControlState.unknown
    blocked_from_state: CandidateControlState | None = Field(
        default=None,
        description="Control state restored by unblock; required iff blocked_external.",
    )
    finding_fingerprint: str = ""
    repeated_state_count: StrictInt = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _legacy_unknown_and_sha(self) -> CandidateDisposition:
        if self.control_state is CandidateControlState.unknown:
            for tip in self.landed_tips:
                if tip.disposition != "unknown":
                    raise ValueError(
                        "legacy unknown state cannot infer tip disposition from report prose"
                    )
        elif self.candidate_sha is None:
            raise ValueError("control_state other than unknown requires candidate_sha")
        return self

    @model_validator(mode="after")
    def _blocked_from_restore_state(self) -> CandidateDisposition:
        if self.control_state is CandidateControlState.blocked_external:
            if self.blocked_from_state not in _BLOCK_RESTORE_STATES:
                raise ValueError(
                    "blocked_external requires blocked_from_state to restore"
                )
        elif self.blocked_from_state is not None:
            raise ValueError("blocked_from_state is only valid while blocked_external")
        return self

    @model_validator(mode="after")
    def _terminal_tip_dispositions(self) -> CandidateDisposition:
        closed_states = {
            CandidateControlState.done,
            CandidateControlState.shipping,
            CandidateControlState.rejected_with_evidence,
        }
        if self.control_state in closed_states:
            label = (
                "rejected_with_evidence"
                if self.control_state is CandidateControlState.rejected_with_evidence
                else "done/shipping"
            )
            if not self.landed_tips:
                raise ValueError(f"{label} requires at least one closed landed tip")
            for tip in self.landed_tips:
                if tip.disposition not in _CLOSED_TIP_DISPOSITIONS:
                    raise ValueError(
                        f"{label} tips must be integrated or "
                        "rejected_with_named_evidence"
                    )
        if self.control_state is CandidateControlState.rejected_with_evidence:
            if not any(
                tip.disposition == "rejected_with_named_evidence"
                for tip in self.landed_tips
            ):
                raise ValueError(
                    "rejected_with_evidence requires a rejected_with_named_evidence tip"
                )
        return self


class MergeCapability(_ValidatedEvolutionMixin, BaseModel):
    """Single-use, commit-bound merge capability. ``open_findings`` is structurally 0."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_ref: TaskRef
    candidate_sha: Sha40
    review_run_id: str = Field(min_length=1)
    close_check_id: str = Field(min_length=1)
    verified_test_ids: tuple[StrictInt, ...] = Field(default_factory=tuple)
    open_findings: Annotated[Literal[0], BeforeValidator(_reject_json_boolean)] = 0
    consumed_at: AwareRfc3339 | None = None

    def consume(self, *, at: datetime) -> MergeCapability:
        if at is None:
            raise ValueError("consume requires a timezone-aware datetime")
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("consume requires a timezone-aware datetime")
        if self.consumed_at is not None:
            raise ValueError("merge capability already consumed")
        return super().validated_copy(consumed_at=at)

    def validated_copy(self, **updates: Any) -> MergeCapability:
        if "consumed_at" in updates:
            raise ValueError(
                "merge capability consumed_at can only change via consume()"
            )
        copied = super().validated_copy(**updates)
        for field in _MERGE_CAPABILITY_BOUND_FIELDS:
            if getattr(copied, field) != getattr(self, field):
                raise ValueError(f"merge capability cannot change {field}")
        return copied


class ContextEntry(_ValidatedEvolutionMixin, BaseModel):
    """One ranked semantic entity in a coordinator-built context packet."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    entity_kind: str = Field(min_length=1)
    entity_id: str = Field(min_length=1)
    task_ref: TaskRef | None = None
    score: Annotated[
        StrictInt | StrictFloat,
        Field(ge=-1.0, le=1.0, allow_inf_nan=False),
        WithJsonSchema(_UNIT_SCORE_SCHEMA),
    ]
    snippet: str


class LaneContextPacket(_ValidatedEvolutionMixin, BaseModel):
    """Coordinator-precomputed semantic packet. Workers never load ONNX."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra=_json_schema_all_of(
            {
                "if": {
                    "properties": {"status": {"const": "unavailable"}},
                    "required": ["status"],
                },
                "then": {
                    "properties": {
                        "entries": {"maxItems": 0},
                        "reason": {"minLength": 1, "type": "string"},
                    },
                    "required": ["reason"],
                },
                "else": {
                    "properties": {
                        "entries": {"minItems": 1},
                        "score_semantics": {
                            "const": "cosine_rank",
                            "type": "string",
                        },
                    },
                    "required": ["entries", "score_semantics"],
                },
            }
        ),
    )

    status: LaneContextStatus
    reason: str | None = None
    provider_class: str | None = None
    query_model_id: str | None = None
    artifact_digest: Sha256Digest | None = None
    store_model_ids: tuple[str, ...] = Field(default_factory=tuple)
    invocation_mode: InvocationMode
    server_start_sha: Sha40 | None = None
    server_started_at: AwareRfc3339 | None = None
    score_semantics: ScoreSemantics | None = None
    entries: tuple[ContextEntry, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def _status_invariants(self) -> LaneContextPacket:
        if self.status == "unavailable":
            if not self.reason:
                raise ValueError("unavailable requires a typed reason")
            if self.entries:
                raise ValueError("unavailable packet cannot carry entries")
        else:
            if not self.entries:
                raise ValueError("selected packet requires entries")
            if self.score_semantics != "cosine_rank":
                raise ValueError("selected packet requires score_semantics cosine_rank")
        return self


class ShipCleanupPostcondition(_ValidatedEvolutionMixin, BaseModel):
    """Cleanup postcondition bound to a ship-stage cursor.

    Remote cleanup is required iff remote-resource receipts exist. No
    receipts must be explicitly ``not_applicable``; ``complete`` cannot
    hide a required-but-unverified remote release.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra=_json_schema_all_of(
            {
                "if": {
                    "properties": {"remote_receipts_present": {"const": False}},
                    "required": ["remote_receipts_present"],
                },
                "then": {"properties": {"remote_cleanup": {"const": "not_applicable"}}},
                "else": {
                    "properties": {
                        "remote_cleanup": {"enum": ["required", "verified", "failed"]}
                    }
                },
            },
            {
                "if": {
                    "properties": {
                        "remote_receipts_present": {"const": True},
                        "stage": {"const": "complete"},
                    },
                    "required": ["stage", "remote_receipts_present"],
                },
                "then": {"properties": {"remote_cleanup": {"const": "verified"}}},
            },
        ),
    )

    stage: ShipStage
    remote_receipts_present: StrictBool
    remote_cleanup: RemoteCleanupStatus

    @model_validator(mode="after")
    def _cleanup_matches_receipts(self) -> ShipCleanupPostcondition:
        if not self.remote_receipts_present:
            if self.remote_cleanup != "not_applicable":
                raise ValueError("no remote receipts must be explicitly not_applicable")
            return self
        if self.remote_cleanup == "not_applicable":
            raise ValueError("remote receipts require cleanup, not not_applicable")
        if self.stage is ShipStage.complete and self.remote_cleanup != "verified":
            raise ValueError(
                "complete requires verified remote cleanup when receipts exist"
            )
        return self


def admit_candidate_transition(
    state: CandidateControlState,
    event: CandidateEvent,
    *,
    blocked_from_state: CandidateControlState | None = None,
) -> TransitionAdmission:
    """Admit a candidate event or raise ``TransitionRefusedError``.

    Legacy ``unknown`` accepts only explicit ``initialize``. Undefined pairs
    refuse; they never infer a next state from report prose. ``unblock``
    restores ``blocked_from_state`` and is refused without that prior state.
    """
    to_state = _admitted_to_state(state, event, blocked_from_state=blocked_from_state)
    if to_state is None:
        if (
            state is CandidateControlState.unknown
            and event is not CandidateEvent.initialize
        ):
            raise TransitionRefusedError(
                f"legacy unknown state refuses inferred {event.value}; initialize with exact SHA first"
            )
        raise TransitionRefusedError(
            f"undefined transition {state.value} + {event.value}"
        )
    return TransitionAdmission(from_state=state, event=event, to_state=to_state)
