"""Data-driven token-budget floor and recommendation for remote lanes.

No imports from api or lanes. Floor lookup reads ``WORKBAY_MIN_REMOTE_TOKEN_BUDGET``
from the injected ``env`` mapping, or from ``os.environ`` when ``env`` is omitted.
Percentiles come from measured per-lane totals (MEAS-06 / MEAS-08); fewer than
five samples fall back to the floor (BIAS-05). Every verdict names the derived
wall-clock so a bound that is also a clock is shown as a clock (OPS-16 / MEAS-07).
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

MIN_REMOTE_IMPLEMENT_TOKEN_BUDGET = 120_000
MIN_REMOTE_REVIEW_TOKEN_BUDGET = 80_000
MIN_REMOTE_TOKEN_BUDGET_ENV = "WORKBAY_MIN_REMOTE_TOKEN_BUDGET"
RECOMMENDED_BUDGET_CAP = 400_000
RECOMMENDATION_HEADROOM = 1.2
BUDGET_ROUNDING = 10_000
MIN_PERCENTILE_SAMPLES = 5
LANE_PREP_PHASE = "lane_prep"

_SEVERITY_OK = "ok"
_SEVERITY_WARN = "warn"
_SEVERITY_REFUSE = "refuse"
_SOURCE_FLOOR = "floor"
_SOURCE_TURN_METRICS = "turn_metrics"


@dataclass(frozen=True)
class BudgetAdvice:
    """Recommended token budget derived from a floor and optional percentiles."""

    floor: int
    sample_lanes: int
    p50: int | None
    p90: int | None
    recommended: int
    source: str


@dataclass(frozen=True)
class BudgetVerdict:
    """ok / warn / refuse decision for a configured token budget."""

    severity: str
    reason: str


def floor_for(lane_kind: str, env: Mapping[str, str] | None = None) -> int:
    """Return the token-budget floor for *lane_kind*.

    Honours ``WORKBAY_MIN_REMOTE_TOKEN_BUDGET`` when it is a positive int *at or
    above* the kind default. The override is a raiser only:
    ``max(kind_default, override)``. Non-integer values raise ``ValueError``
    (a typo such as ``120k`` must not silently keep 120000/80000). Non-positive
    integers are ignored. When *env* is omitted, reads ``os.environ`` (the
    production default).
    """
    kind = str(lane_kind or "").strip().lower()
    kind_default = (
        MIN_REMOTE_REVIEW_TOKEN_BUDGET if kind == "review" else MIN_REMOTE_IMPLEMENT_TOKEN_BUDGET
    )
    environ = os.environ if env is None else env
    raw = str(environ.get(MIN_REMOTE_TOKEN_BUDGET_ENV) or "").strip()
    if raw:
        try:
            override = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{MIN_REMOTE_TOKEN_BUDGET_ENV} must be a positive integer, got {raw!r}"
            ) from exc
        if override > 0:
            return max(kind_default, override)
    return kind_default


def per_lane_totals(rows: Sequence[Any]) -> dict[tuple[str, str], int]:
    """Sum ``total_tokens`` per ``(task_ref, lane_id)``.

    Skips ``lane_prep`` rows and rows whose ``total_tokens`` is not an int.
    """
    totals: dict[tuple[str, str], int] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("phase") == LANE_PREP_PHASE:
            continue
        tokens = row.get("total_tokens")
        if isinstance(tokens, bool) or not isinstance(tokens, int):
            continue
        key = (str(row.get("task_ref") or ""), str(row.get("lane_id") or ""))
        totals[key] = totals.get(key, 0) + tokens
    return totals


def _nearest_rank(sorted_values: Sequence[int], percentile: float) -> int:
    """Nearest-rank percentile (1-based ``ceil(p/100 * n)``); *sorted_values* nonempty."""
    n = len(sorted_values)
    rank = max(1, min(n, math.ceil(percentile / 100.0 * n)))
    return int(sorted_values[rank - 1])


def _round_up_to(value: int, step: int = BUDGET_ROUNDING) -> int:
    if step <= 0:
        return value
    remainder = value % step
    if remainder == 0:
        return value
    return value + (step - remainder)


def recommend_token_budget(
    rows: Sequence[Any],
    *,
    backend: str,
    model: str | None = None,
    lane_kind: str = "implement",
    env: Mapping[str, str] | None = None,
) -> BudgetAdvice:
    """Recommend a token budget from per-lane totals for *backend* (and *model*).

    Percentiles are nearest-rank over per-lane totals. ``recommended`` is
    ``clamp(int(p90 * 1.2), floor, 400_000)`` rounded up to the nearest 10_000.
    Fewer than five sample lanes → ``source="floor"``, no percentiles.
    *env* is forwarded to :func:`floor_for`; omitted means ``os.environ``.
    """
    floor = floor_for(lane_kind, env=env)
    filtered: list[Any] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("backend") != backend:
            continue
        if model is not None and row.get("model") != model:
            continue
        filtered.append(row)
    totals = per_lane_totals(filtered)
    sample_lanes = len(totals)
    if sample_lanes < MIN_PERCENTILE_SAMPLES:
        return BudgetAdvice(
            floor=floor,
            sample_lanes=sample_lanes,
            p50=None,
            p90=None,
            recommended=floor,
            source=_SOURCE_FLOOR,
        )
    values = sorted(totals.values())
    p50 = _nearest_rank(values, 50)
    p90 = _nearest_rank(values, 90)
    scaled = int(p90 * RECOMMENDATION_HEADROOM)
    clamped = max(floor, min(RECOMMENDED_BUDGET_CAP, scaled))
    recommended = min(RECOMMENDED_BUDGET_CAP, max(floor, _round_up_to(clamped)))
    return BudgetAdvice(
        floor=floor,
        sample_lanes=sample_lanes,
        p50=p50,
        p90=p90,
        recommended=recommended,
        source=_SOURCE_TURN_METRICS,
    )


def _wall_clock(bounds: Mapping[str, Any] | None) -> tuple[Any, Any]:
    if not isinstance(bounds, Mapping):
        return None, None
    return bounds.get("max_turns"), bounds.get("timeout")


def _format_reason(
    *,
    token_budget: int,
    bound_label: str,
    bound_value: int,
    sample_lanes: int,
    max_turns: Any,
    timeout: Any,
    relation: str,
) -> str:
    clock_parts: list[str] = []
    if max_turns is not None:
        clock_parts.append(f"max_turns={max_turns}")
    if timeout is not None:
        clock_parts.append(f"timeout={timeout}")
    clock = ", ".join(clock_parts) if clock_parts else "timeout=None"
    return (
        f"token_budget={token_budget} {relation} {bound_label} {bound_value} "
        f"with sample_lanes={sample_lanes} ({clock})"
    )


def check_token_budget(
    token_budget: int,
    advice: BudgetAdvice,
    *,
    bounds_fn: Callable[[int], Mapping[str, int]] | None = None,
) -> BudgetVerdict:
    """Classify *token_budget* against *advice*.

    ``refuse`` when below the floor; ``warn`` when ``source == "turn_metrics"``
    and below the recommendation; ``ok`` otherwise (equal to the floor is not a
    refusal). *bounds_fn* defaults to :func:`derive_grok_single_cycle_bounds`.
    Callers with an offload profile should inject a function that calls
    :func:`derive_single_cycle_bounds` with that profile's bound kind and
    ``timeout_cap`` so adapter-timeout backends do not name a grok max_turns.
    Injected so this module stays testable without I/O.
    """
    if bounds_fn is None:
        from workbay_orchestrator_mcp.orchestration.offload_profiles import (  # noqa: PLC0415
            derive_grok_single_cycle_bounds,
        )

        bounds_fn = derive_grok_single_cycle_bounds
    try:
        bounds: Mapping[str, int] | None = bounds_fn(token_budget)
    except Exception:  # noqa: BLE001 — still emit a verdict if bounds cannot be derived
        bounds = None
    max_turns, timeout = _wall_clock(bounds)
    sample_lanes = advice.sample_lanes
    if token_budget < advice.floor:
        return BudgetVerdict(
            severity=_SEVERITY_REFUSE,
            reason=_format_reason(
                token_budget=token_budget,
                bound_label="floor",
                bound_value=advice.floor,
                sample_lanes=sample_lanes,
                max_turns=max_turns,
                timeout=timeout,
                relation="is below",
            ),
        )
    if advice.source == _SOURCE_TURN_METRICS and token_budget < advice.recommended:
        return BudgetVerdict(
            severity=_SEVERITY_WARN,
            reason=_format_reason(
                token_budget=token_budget,
                bound_label="recommended",
                bound_value=advice.recommended,
                sample_lanes=sample_lanes,
                max_turns=max_turns,
                timeout=timeout,
                relation="is below",
            ),
        )
    bound_label = "recommended" if advice.source == _SOURCE_TURN_METRICS else "floor"
    bound_value = advice.recommended if advice.source == _SOURCE_TURN_METRICS else advice.floor
    return BudgetVerdict(
        severity=_SEVERITY_OK,
        reason=_format_reason(
            token_budget=token_budget,
            bound_label=bound_label,
            bound_value=bound_value,
            sample_lanes=sample_lanes,
            max_turns=max_turns,
            timeout=timeout,
            relation="meets",
        ),
    )
