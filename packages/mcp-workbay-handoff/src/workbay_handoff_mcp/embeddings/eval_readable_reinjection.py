"""Readable-value efficacy eval for semantic reinjection (internal).

Compares the two reinjection delivery arms under the same fixed character budget:

  arm A ("current")  = recency-ordered opaque ``relevant: kind:id`` refs
  arm B ("readable") = budgeted readable snippets via ``build_semantic_reinjection_packet``

Reports readable relevant-concept coverage, coverage per emitted char, duplicate rate,
below-floor noise count, and total emitted chars. Eval-only scaffolding — not imported
by the SessionStart hook hot path.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import numpy as np

from .eval_recall import ConceptRef, select_current_recency
from .reinjection import (
    ReinjectionConfig,
    _ref_kind,
    build_semantic_reinjection_packet,
    render_readable_relevant_lines,
)
from .store import SupportsEmbed


@dataclass(frozen=True)
class ArmReadableMetrics:
    """One arm's readable-value eval outcome under a fixed char budget."""

    arm: str
    readable_coverage: float
    coverage_per_char: float
    duplicate_rate: float
    below_floor_count: int
    emitted_chars: int
    within_budget: bool
    selected_refs: tuple[ConceptRef, ...]


def _opaque_relevant_line(selected: list[ConceptRef]) -> str:
    if not selected:
        return ""
    refs = ", ".join(f"{kind}:{entity_id}" for kind, entity_id in selected)
    return f"relevant: {refs}"


def _readable_coverage(selected_refs: list[ConceptRef], relevant: list[ConceptRef], *, readable: bool) -> float:
    relevant_refs = {(_ref_kind(kind), entity_id) for kind, entity_id in relevant}
    if not relevant_refs or not selected_refs:
        return 0.0
    if not readable:
        return 0.0
    hits = sum(1 for ref in selected_refs if ref in relevant_refs)
    return hits / len(relevant_refs)


def _duplicate_rate(selected_refs: list[ConceptRef]) -> float:
    if not selected_refs:
        return 0.0
    unique = len(set(selected_refs))
    return (len(selected_refs) - unique) / len(selected_refs)


def evaluate_current_arm(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    relevant: list[ConceptRef],
    chars_budget: int,
    top_k: int,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> ArmReadableMetrics:
    """Arm A: recency opaque refs trimmed to the shared char budget."""
    selected: list[ConceptRef] = []
    for k in range(1, top_k + 1):
        trial = select_current_recency(conn, task_ref, top_k=k, entity_kinds=entity_kinds, model_id=model_id)
        line = _opaque_relevant_line(trial)
        if len(line) <= chars_budget:
            selected = trial
        else:
            break
    emitted = len(_opaque_relevant_line(selected))
    coverage = _readable_coverage(selected, relevant, readable=False)
    return ArmReadableMetrics(
        arm="current",
        readable_coverage=coverage,
        coverage_per_char=coverage / max(1, emitted),
        duplicate_rate=_duplicate_rate(selected),
        below_floor_count=0,
        emitted_chars=emitted,
        within_budget=emitted <= chars_budget,
        selected_refs=tuple(selected),
    )


def evaluate_readable_arm(
    conn: sqlite3.Connection,
    *,
    provider: SupportsEmbed,
    anchor: np.ndarray,
    task_ref: str,
    relevant: list[ConceptRef],
    visible_texts: list[str],
    chars_budget: int,
    config: ReinjectionConfig | None = None,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> ArmReadableMetrics:
    """Arm B: readable snippet packet under the same char budget."""
    cfg = config or ReinjectionConfig.from_env()
    result = build_semantic_reinjection_packet(
        conn,
        task_ref=task_ref,
        provider=provider,
        persisted_anchor=anchor,
        visible_texts=visible_texts,
        semantic_content_budget_chars=chars_budget,
        config=cfg,
        entity_kinds=entity_kinds,
    )
    lines = render_readable_relevant_lines(result.selected)
    emitted = len("\n".join(lines)) if lines else 0
    selected_refs = [(item.kind, item.id) for item in result.selected]
    coverage = _readable_coverage(
        [(kind, entity_id) for kind, entity_id in selected_refs],
        relevant,
        readable=True,
    )
    below_floor = sum(1 for item in result.selected if item.score < cfg.min_score)
    return ArmReadableMetrics(
        arm="readable",
        readable_coverage=coverage,
        coverage_per_char=coverage / max(1, emitted),
        duplicate_rate=_duplicate_rate(selected_refs),
        below_floor_count=below_floor,
        emitted_chars=emitted,
        within_budget=emitted <= chars_budget and result.chars_used <= chars_budget,
        selected_refs=tuple(selected_refs),
    )


def evaluate_readable_reinjection(
    conn: sqlite3.Connection,
    *,
    provider: SupportsEmbed,
    anchor: np.ndarray,
    task_ref: str,
    relevant: list[ConceptRef],
    visible_texts: list[str],
    chars_budget: int,
    top_k: int,
    config: ReinjectionConfig | None = None,
    entity_kinds: tuple[str, ...] | None = None,
    model_id: str | None = None,
) -> dict[str, ArmReadableMetrics]:
    """Run both arms under the same budget and return comparable metrics."""
    rel = [(str(k), str(i)) for k, i in relevant]
    cfg = config or ReinjectionConfig.from_env()
    return {
        "current": evaluate_current_arm(
            conn,
            task_ref=task_ref,
            relevant=rel,
            chars_budget=chars_budget,
            top_k=top_k,
            entity_kinds=entity_kinds,
            model_id=model_id or provider.model_id,
        ),
        "readable": evaluate_readable_arm(
            conn,
            provider=provider,
            anchor=anchor,
            task_ref=task_ref,
            relevant=rel,
            visible_texts=visible_texts,
            chars_budget=chars_budget,
            config=cfg,
            entity_kinds=entity_kinds,
            model_id=model_id or provider.model_id,
        ),
    }


def apply_readable_gate(arms: dict[str, ArmReadableMetrics]) -> dict[str, object]:
    """Pre-registered gate: adopt readable arm iff coverage improves without noise regressions."""
    current = arms["current"]
    readable = arms["readable"]
    coverage_improved = readable.readable_coverage > current.readable_coverage
    coverage_per_char_improved = readable.coverage_per_char > current.coverage_per_char
    dup_ok = readable.duplicate_rate <= current.duplicate_rate
    floor_ok = readable.below_floor_count <= current.below_floor_count
    adopt = readable.within_budget and coverage_improved and coverage_per_char_improved and dup_ok and floor_ok
    return {
        "recommendation": "adopt" if adopt else "hold",
        "rule": (
            "adopt readable arm iff within_budget AND readable_coverage improves AND "
            "coverage/char improves AND duplicate_rate and below_floor_count do not regress"
        ),
        "readable_coverage_current": current.readable_coverage,
        "readable_coverage_readable": readable.readable_coverage,
        "coverage_per_char_current": current.coverage_per_char,
        "coverage_per_char_readable": readable.coverage_per_char,
        "duplicate_rate_current": current.duplicate_rate,
        "duplicate_rate_readable": readable.duplicate_rate,
        "below_floor_current": current.below_floor_count,
        "below_floor_readable": readable.below_floor_count,
        "emitted_chars_current": current.emitted_chars,
        "emitted_chars_readable": readable.emitted_chars,
        "within_budget_readable": readable.within_budget,
        "coverage_improved": coverage_improved,
        "coverage_per_char_improved": coverage_per_char_improved,
        "duplicate_rate_ok": dup_ok,
        "below_floor_ok": floor_ok,
    }
