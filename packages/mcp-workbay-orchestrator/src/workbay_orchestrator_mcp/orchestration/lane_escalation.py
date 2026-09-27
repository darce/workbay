"""Typed junior-to-senior escalation at the worker-cycle boundary.

This module only decides whether a lane should move to the next routing tier.
It never dispatches a worker.  The caller must persist the returned quad and
end the current pass so the changed model is used by a new explicit dispatch.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.codex_lane_config import CODEX_MODEL_TIERS
from workbay_orchestrator_mcp.orchestration.lane_routing import RoutingQuad


class EscalationTrigger(str, Enum):
    """Closed vocabulary of evidence that permits automatic escalation."""

    REVIEW_HIGH_AFTER_ROUND_1 = "review_high_after_round_1"
    TWO_ZERO_BYTE_TURNS_WITH_TOOL_CALLS = "two_zero_byte_turns_with_tool_calls"
    PRODUCER_ASKED_QUESTION_TWICE = "producer_asked_question_twice"


@dataclass(frozen=True, slots=True)
class TurnEvidence:
    """The small, transport-independent turn facts used by the evaluator."""

    patch_bytes: int | None = None
    tool_call_count: int | None = None
    asked_question: bool = False
    spec_path: Path | None = None
    phases_path: Path | None = None


@dataclass(frozen=True, slots=True)
class LaneEscalationState:
    """All facts needed to evaluate a lane without consulting mutable globals."""

    current_quad: RoutingQuad
    review_round: int = 0
    review_findings: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    turns: tuple[TurnEvidence, ...] = field(default_factory=tuple)
    producer_question_count: int = 0


@dataclass(frozen=True, slots=True)
class EscalationDecision:
    """A typed request to persist the next quad for a later dispatch."""

    trigger: EscalationTrigger
    from_quad: RoutingQuad
    to_quad: RoutingQuad
    boundary: str = "cycle_start"

    def as_dict(self) -> dict[str, Any]:
        return {
            "from_quad": self.from_quad.as_dict(),
            "to_quad": self.to_quad.as_dict(),
            "trigger": self.trigger.value,
            "boundary": self.boundary,
        }


def _service_speed(allowed_service_tiers: frozenset[str]) -> str | None:
    return "standard" if "default" in allowed_service_tiers else None


def _codex_ladder() -> tuple[RoutingQuad, ...]:
    entitled = (row for row in CODEX_MODEL_TIERS.values() if row.entitled)
    ordered = sorted(entitled, key=lambda row: (row.tier == "senior", row.slug))
    return tuple(
        RoutingQuad(
            backend="codex-remote",
            model=row.slug,
            effort=row.default_effort,
            speed=_service_speed(row.allowed_service_tiers),
            tier=row.tier,
        )
        for row in ordered
    )


# Junior rows precede senior rows.  There is intentionally no reverse edge.
ESCALATION_LADDER: dict[str, tuple[RoutingQuad, ...]] = {
    "codex-remote": _codex_ladder(),
}


def _clean_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _turn_from(value: object) -> TurnEvidence:
    if isinstance(value, TurnEvidence):
        return value
    if not isinstance(value, Mapping):
        return TurnEvidence()
    spec_path = value.get("spec_path")
    phases_path = value.get("phases_path")
    return TurnEvidence(
        patch_bytes=_clean_int(value.get("patch_bytes")),
        tool_call_count=_clean_int(value.get("tool_call_count")),
        asked_question=value.get("asked_question") is True,
        spec_path=Path(spec_path) if isinstance(spec_path, (str, Path)) else None,
        phases_path=Path(phases_path) if isinstance(phases_path, (str, Path)) else None,
    )


def _quad_from(value: object) -> RoutingQuad:
    if isinstance(value, RoutingQuad):
        return value
    if not isinstance(value, Mapping):
        return RoutingQuad()
    return RoutingQuad(
        backend=_clean_text(value.get("backend")),
        model=_clean_text(value.get("model")),
        effort=_clean_text(value.get("effort") or value.get("reasoning_effort")),
        speed=_clean_text(value.get("speed")),
        tier=_clean_text(value.get("tier")),
    )


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _state_from(value: LaneEscalationState | Mapping[str, Any]) -> LaneEscalationState:
    if isinstance(value, LaneEscalationState):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("lane_state must be LaneEscalationState or a mapping")
    findings_value = value.get("review_findings", value.get("findings", ()))
    findings: tuple[Mapping[str, Any], ...] = ()
    if isinstance(findings_value, Sequence) and not isinstance(findings_value, (str, bytes)):
        findings = tuple(item for item in findings_value if isinstance(item, Mapping))
    turns_value = value.get("turns", value.get("turn_evidence", ()))
    turns: tuple[TurnEvidence, ...] = ()
    if isinstance(turns_value, Sequence) and not isinstance(turns_value, (str, bytes)):
        turns = tuple(_turn_from(item) for item in turns_value)
    return LaneEscalationState(
        current_quad=_quad_from(value.get("current_quad", value.get("routing"))),
        review_round=_clean_int(value.get("review_round")) or 0,
        review_findings=findings,
        turns=turns,
        producer_question_count=_clean_int(value.get("producer_question_count")) or 0,
    )


def _read_json(path: Path | None) -> Mapping[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _turn_hit_wall_clock_bound(turn: TurnEvidence) -> bool:
    """True only with paired, machine-readable timeout and duration evidence."""

    spec = _read_json(turn.spec_path)
    phases_envelope = _read_json(turn.phases_path)
    if spec is None or phases_envelope is None:
        return False
    timeout = spec.get("lane_timeout_s")
    phases = phases_envelope.get("phases", phases_envelope)
    agent_turn = phases.get("agent_turn") if isinstance(phases, Mapping) else None
    duration = agent_turn.get("duration_s") if isinstance(agent_turn, Mapping) else None
    if isinstance(timeout, bool) or isinstance(duration, bool):
        return False
    try:
        timeout_s = float(timeout)  # type: ignore[arg-type]
        duration_s = float(duration)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return timeout_s > 0 and duration_s >= timeout_s


def _next_quad(current: RoutingQuad) -> RoutingQuad | None:
    if current.tier != "junior" or not current.backend:
        return None
    ladder = ESCALATION_LADDER.get(current.backend, ())
    for row in ladder:
        if row.tier == "senior":
            return row
    return None


def _has_high_finding(findings: Sequence[Mapping[str, Any]]) -> bool:
    return any(str(item.get("severity") or "").strip().upper() == "HIGH" for item in findings)


def evaluate_escalation(
    lane_state: LaneEscalationState | Mapping[str, Any],
) -> EscalationDecision | None:
    """Return the junior lane's next routing quad when a typed trigger fires.

    Wall-clock evidence has precedence over every escalation trigger.  A turn
    killed at its declared transport bound is not evidence that a more capable
    model would succeed.  A counted zero tool-call turn is likewise never an
    escalation signal.
    """

    state = _state_from(lane_state)
    next_quad = _next_quad(state.current_quad)
    if next_quad is None:
        return None

    # Only the latest turn can explain the terminal state under evaluation.
    if state.turns and _turn_hit_wall_clock_bound(state.turns[-1]):
        return None

    trigger: EscalationTrigger | None = None
    if state.review_round == 1 and _has_high_finding(state.review_findings):
        trigger = EscalationTrigger.REVIEW_HIGH_AFTER_ROUND_1
    elif len(state.turns) >= 2 and all(
        not _turn_hit_wall_clock_bound(turn)
        and turn.patch_bytes == 0
        and turn.tool_call_count is not None
        and turn.tool_call_count > 0
        for turn in state.turns[-2:]
    ):
        trigger = EscalationTrigger.TWO_ZERO_BYTE_TURNS_WITH_TOOL_CALLS
    else:
        question_count = state.producer_question_count + sum(turn.asked_question for turn in state.turns)
        if question_count >= 2:
            trigger = EscalationTrigger.PRODUCER_ASKED_QUESTION_TWICE

    if trigger is None:
        return None
    return EscalationDecision(
        trigger=trigger,
        from_quad=state.current_quad,
        to_quad=next_quad,
    )


__all__ = [
    "ESCALATION_LADDER",
    "EscalationDecision",
    "EscalationTrigger",
    "LaneEscalationState",
    "TurnEvidence",
    "evaluate_escalation",
]
