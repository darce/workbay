"""Readable semantic reinjection: resolve, select, render, packet (internal)."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Literal, TypedDict, cast

import numpy as np

from .ranking import ScoredConceptVector, compose_anchor, rank_candidate_vectors_by_anchor
from .store import SupportsEmbed, text_hash

SkipReason = Literal[
    "disabled",
    "provider_unavailable",
    "anchor_unavailable",
    "no_embeddings",
    "below_threshold",
    "budget_exhausted",
    "redundancy_filtered",
    "source_unresolved",
    "error",
]
ResultStatus = Literal["selected", "skipped", "degraded"]

_REF_KINDS = {
    "decision.rationale": "decision",
    "finding.description": "finding",
    "finding.fix": "finding",
    "finding.resolution_notes": "finding",
    "blocker.description": "blocker",
    "compaction.prose_residual": "compaction",
}

_RESOLVER_SPECS: dict[str, tuple[str, str, str, str | None]] = {
    "decision.rationale": ("decisions", "id", "rationale", "decision"),
    "finding.description": ("review_findings", "id", "description", "finding_id"),
    "finding.fix": ("review_findings", "id", "fix", "finding_id"),
    "finding.resolution_notes": ("review_findings", "id", "resolution_notes", "finding_id"),
    "blocker.description": ("blockers", "id", "description", None),
    "compaction.prose_residual": ("session_compactions", "compaction_id", "prose_residual", "compaction_id"),
}


def _clamp_int(value: str | None, *, default: int, lo: int, hi: int) -> int:
    if value is None or not str(value).strip():
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if lo <= parsed <= hi else default


def _clamp_float(value: str | None, *, default: float, lo: float, hi: float) -> float:
    if value is None or not str(value).strip():
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if lo <= parsed <= hi else default


# Bounds for the semantic refresh char budget. Shared by ``ReinjectionConfig.from_env``
# and any caller-supplied budget override (e.g. the ``semantic_reinjection_packet`` MCP
# tool) so an explicit override cannot bypass the env-derived clamp.
REFRESH_BUDGET_CHARS_MIN: int = 0
REFRESH_BUDGET_CHARS_MAX: int = 10000


@dataclass(frozen=True)
class ReinjectionConfig:
    max_concepts: int = 8
    candidate_pool: int = 32
    min_score: float = 0.35
    max_score_drop: float = 0.20
    mmr_relevance_weight: float = 0.70
    max_redundancy: float = 0.92
    snippet_chars: int = 120
    notify_allowance_chars: int = 220
    refresh_budget_chars: int = 1500

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ReinjectionConfig:
        source = env if env is not None else os.environ
        max_concepts = _clamp_int(source.get("WORKBAY_REINJECT_SEMANTIC_TOP_K"), default=8, lo=1, hi=32)
        candidate_pool = _clamp_int(source.get("WORKBAY_REINJECT_SEMANTIC_CANDIDATE_POOL"), default=32, lo=1, hi=256)
        if candidate_pool < max_concepts:
            candidate_pool = max_concepts
        return cls(
            max_concepts=max_concepts,
            candidate_pool=candidate_pool,
            min_score=_clamp_float(source.get("WORKBAY_REINJECT_SEMANTIC_MIN_SCORE"), default=0.35, lo=-1.0, hi=1.0),
            max_score_drop=_clamp_float(
                source.get("WORKBAY_REINJECT_SEMANTIC_MAX_SCORE_DROP"), default=0.20, lo=0.0, hi=2.0
            ),
            mmr_relevance_weight=_clamp_float(
                source.get("WORKBAY_REINJECT_SEMANTIC_MMR_WEIGHT"), default=0.70, lo=0.0, hi=1.0
            ),
            max_redundancy=_clamp_float(
                source.get("WORKBAY_REINJECT_SEMANTIC_MAX_REDUNDANCY"), default=0.92, lo=-1.0, hi=1.0
            ),
            snippet_chars=_clamp_int(source.get("WORKBAY_REINJECT_SEMANTIC_SNIPPET_CHARS"), default=120, lo=40, hi=500),
            notify_allowance_chars=_clamp_int(
                source.get("WORKBAY_REINJECT_NOTIFY_ALLOWANCE_CHARS"), default=220, lo=80, hi=500
            ),
            refresh_budget_chars=_clamp_int(
                source.get("WORKBAY_REINJECT_SEMANTIC_REFRESH_BUDGET_CHARS"),
                default=1500,
                lo=REFRESH_BUDGET_CHARS_MIN,
                hi=REFRESH_BUDGET_CHARS_MAX,
            ),
        )


@dataclass(frozen=True)
class ResolvedConceptSource:
    entity_kind: str
    entity_id: str
    text: str
    label: str


@dataclass(frozen=True)
class SelectedConcept:
    kind: str
    id: str
    label: str
    snippet: str
    score: float
    emitted_chars: int


@dataclass
class SemanticReinjectionResult:
    status: ResultStatus
    skip_reason: SkipReason | None
    model_id: str | None
    selected: list[SelectedConcept] = field(default_factory=list)
    chars_used: int = 0
    chars_budget: int = 0
    score_hi: float | None = None
    score_lo: float | None = None
    anchor_sources: list[str] = field(default_factory=list)
    effective_config: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        return cast(
            "dict[str, object]",
            json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        )


class _ResultMetadata(TypedDict):
    """Shape of the shared keyword args splatted into ``SemanticReinjectionResult``."""

    anchor_sources: list[str]
    effective_config: dict[str, object]


def _ref_kind(entity_kind: str) -> str:
    return _REF_KINDS.get(entity_kind, entity_kind.split(".", 1)[0])


def resolve_concept_sources(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    keys: Sequence[tuple[str, str]],
) -> list[ResolvedConceptSource]:
    resolved: list[ResolvedConceptSource] = []
    for entity_kind, entity_id in keys:
        spec = _RESOLVER_SPECS.get(entity_kind)
        if spec is None:
            continue
        table, id_col, text_col, label_col = spec
        row = conn.execute(
            f"SELECT {text_col}, {label_col if label_col else id_col} AS label_col "
            f"FROM {table} WHERE {id_col} = ? AND task_ref = ?",
            (entity_id, task_ref),
        ).fetchone()
        if row is None:
            continue
        raw_text = row[0]
        if raw_text is None or not str(raw_text).strip():
            continue
        text = str(raw_text)
        embed_row = conn.execute(
            "SELECT text_hash FROM concept_embeddings WHERE entity_kind = ? AND entity_id = ?",
            (entity_kind, entity_id),
        ).fetchone()
        if embed_row is None or str(embed_row[0]) != text_hash(text):
            continue
        if label_col is None:
            label = f"blocker-{entity_id}"
        else:
            label_val = row[1]
            label = str(label_val) if label_val is not None else entity_id
        resolved.append(ResolvedConceptSource(entity_kind, entity_id, text, label))
    return resolved


def sanitize_snippet(text: str) -> str:
    collapsed = re.sub(r"\s+", " ", text.strip())
    collapsed = collapsed.replace("```", "'''")
    collapsed = collapsed.replace("`", "'")
    collapsed = "".join(ch for ch in collapsed if ch >= " " or ch == " ")
    return collapsed.strip()


def render_snippet(text: str, *, ref_kind: str, ref_id: str, snippet_chars: int) -> str:
    clean = sanitize_snippet(text)
    suffix = f" [{ref_kind}:{ref_id}]"
    body_budget = max(0, snippet_chars - len(suffix))
    if len(clean) <= body_budget:
        body = clean
    else:
        cut = clean[:body_budget]
        if " " in cut:
            cut = cut.rsplit(" ", 1)[0]
        body = cut.rstrip()
    return f"{body}{suffix}"


def _max_redundancy(candidate: np.ndarray, selected: list[np.ndarray], seeds: list[np.ndarray]) -> float:
    peers = selected + seeds
    if not peers:
        return 0.0
    sims = [float(np.dot(candidate, peer)) for peer in peers]
    return max(sims)


def select_concepts(
    candidates: Sequence[ScoredConceptVector],
    *,
    resolved: Mapping[tuple[str, str], ResolvedConceptSource],
    seed_vectors: Sequence[np.ndarray],
    config: ReinjectionConfig,
    chars_budget: int,
    provider: SupportsEmbed,
) -> list[SelectedConcept]:
    del provider  # reserved for future seed embedding in packet builder
    if not candidates:
        return []
    filtered = [c for c in candidates if c.score >= config.min_score]
    if not filtered:
        return []
    filtered.sort(key=lambda c: (-c.score, c.entity_kind, c.entity_id))
    top_score = filtered[0].score
    windowed = [c for c in filtered if c.score >= top_score - config.max_score_drop]
    remaining = list(windowed)
    selected_vectors: list[np.ndarray] = []
    seed_dim = candidates[0].vector.shape[0] if candidates else 0
    seeds = [
        np.asarray(v, dtype="<f4").reshape(-1)
        for v in seed_vectors
        if np.asarray(v, dtype="<f4").reshape(-1).shape[0] == seed_dim
    ]
    chosen: list[SelectedConcept] = []
    chars_used = 0
    lam = config.mmr_relevance_weight

    while remaining and len(chosen) < config.max_concepts:
        best_idx = -1
        best_mmr = float("-inf")
        for idx, candidate in enumerate(remaining):
            redundancy = _max_redundancy(candidate.vector, selected_vectors, seeds)
            if redundancy > config.max_redundancy and (selected_vectors or seeds):
                continue
            mmr = lam * candidate.score - (1.0 - lam) * redundancy
            if mmr > best_mmr:
                best_mmr = mmr
                best_idx = idx
        if best_idx < 0:
            break
        candidate = remaining.pop(best_idx)
        source = resolved.get((candidate.entity_kind, candidate.entity_id))
        if source is None:
            continue
        ref_kind = _ref_kind(candidate.entity_kind)
        snippet = render_snippet(
            source.text,
            ref_kind=ref_kind,
            ref_id=candidate.entity_id,
            snippet_chars=config.snippet_chars,
        )
        line_chars = len(f"- {source.label}: {snippet}")
        if not chosen:
            needed = len("relevant:\n") + line_chars
        else:
            needed = 1 + line_chars
        if chars_used + needed > chars_budget:
            continue
        emitted = needed
        chosen.append(
            SelectedConcept(
                kind=ref_kind,
                id=candidate.entity_id,
                label=source.label,
                snippet=snippet,
                score=candidate.score,
                emitted_chars=emitted,
            )
        )
        selected_vectors.append(candidate.vector)
        chars_used += emitted
    return chosen


def _infer_skip_reason(
    candidates: Sequence[ScoredConceptVector],
    resolved: Mapping[tuple[str, str], ResolvedConceptSource],
    *,
    config: ReinjectionConfig,
    chars_budget: int,
    seed_vectors: Sequence[np.ndarray],
) -> SkipReason:
    eligible = [c for c in candidates if c.score >= config.min_score and (c.entity_kind, c.entity_id) in resolved]
    if not eligible:
        if any(c.score >= config.min_score for c in candidates):
            return "source_unresolved"
        return "below_threshold"
    eligible.sort(key=lambda c: (-c.score, c.entity_kind, c.entity_id))
    top_score = eligible[0].score
    eligible = [c for c in eligible if c.score >= top_score - config.max_score_drop]
    header = len("relevant:\n")
    fits_alone = False
    selectable = False
    seed_dim = eligible[0].vector.shape[0]
    seeds = [
        np.asarray(v, dtype="<f4").reshape(-1)
        for v in seed_vectors
        if np.asarray(v, dtype="<f4").reshape(-1).shape[0] == seed_dim
    ]
    for candidate in eligible:
        source = resolved[(candidate.entity_kind, candidate.entity_id)]
        ref_kind = _ref_kind(candidate.entity_kind)
        snippet = render_snippet(
            source.text,
            ref_kind=ref_kind,
            ref_id=candidate.entity_id,
            snippet_chars=config.snippet_chars,
        )
        line_chars = len(f"- {source.label}: {snippet}")
        if header + line_chars <= chars_budget:
            fits_alone = True
            redundancy = _max_redundancy(candidate.vector, [], seeds)
            if redundancy <= config.max_redundancy or not seeds:
                selectable = True
    if not fits_alone:
        return "budget_exhausted"
    if not selectable:
        return "redundancy_filtered"
    return "budget_exhausted"


def render_readable_relevant_lines(selected: Sequence[SelectedConcept]) -> list[str]:
    if not selected:
        return []
    lines = ["relevant:"]
    for item in selected:
        lines.append(f"- {item.label}: {item.snippet}")
    return lines


def build_semantic_reinjection_packet(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    provider: SupportsEmbed | None,
    persisted_anchor: np.ndarray | None,
    visible_texts: Sequence[str],
    semantic_content_budget_chars: int,
    config: ReinjectionConfig,
    entity_kinds: tuple[str, ...] | None = None,
) -> SemanticReinjectionResult:
    budget = max(0, int(semantic_content_budget_chars))
    effective_config = asdict(config)
    anchor_sources: list[str] = []
    if provider is not None:
        if persisted_anchor is not None and np.asarray(persisted_anchor).reshape(-1).shape[0] == provider.dim:
            anchor_sources.append("persisted_compaction")
        if any(str(text).strip() for text in visible_texts):
            anchor_sources.append("visible_text")
    result_metadata: _ResultMetadata = {
        "anchor_sources": anchor_sources,
        "effective_config": effective_config,
    }
    if provider is None:
        return SemanticReinjectionResult(
            status="skipped",
            skip_reason="provider_unavailable",
            model_id=None,
            chars_budget=budget,
            **result_metadata,
        )
    anchor = compose_anchor(provider, persisted_anchor=persisted_anchor, texts=list(visible_texts))
    if anchor is None:
        return SemanticReinjectionResult(
            status="skipped",
            skip_reason="anchor_unavailable",
            model_id=provider.model_id,
            chars_budget=budget,
            **result_metadata,
        )
    fetch_pool = min(256, max(config.candidate_pool, config.candidate_pool * 8))
    ranked_candidates = rank_candidate_vectors_by_anchor(
        conn,
        anchor,
        task_ref,
        candidate_pool=fetch_pool,
        entity_kinds=entity_kinds or tuple(_RESOLVER_SPECS),
        model_id=provider.model_id,
    )
    if not ranked_candidates:
        return SemanticReinjectionResult(
            status="skipped",
            skip_reason="no_embeddings",
            model_id=provider.model_id,
            chars_budget=budget,
            **result_metadata,
        )
    resolved_list = resolve_concept_sources(
        conn,
        task_ref=task_ref,
        keys=[(c.entity_kind, c.entity_id) for c in ranked_candidates],
    )
    resolved_map = {(r.entity_kind, r.entity_id): r for r in resolved_list}
    candidates = [
        candidate for candidate in ranked_candidates if (candidate.entity_kind, candidate.entity_id) in resolved_map
    ][: config.candidate_pool]
    seed_vectors: list[np.ndarray] = []
    for text in visible_texts:
        if not text or not str(text).strip():
            continue
        try:
            vec = np.asarray(provider.embed([str(text)])[0], dtype="<f4").reshape(-1)
        except Exception:
            continue
        if vec.shape[0] != provider.dim:
            continue
        seed_vectors.append(vec)
    selected = select_concepts(
        candidates,
        resolved=resolved_map,
        seed_vectors=seed_vectors,
        config=config,
        chars_budget=budget,
        provider=provider,
    )
    if not selected:
        reason_candidates = candidates if candidates else ranked_candidates
        return SemanticReinjectionResult(
            status="skipped",
            skip_reason=_infer_skip_reason(
                reason_candidates,
                resolved_map,
                config=config,
                chars_budget=budget,
                seed_vectors=seed_vectors,
            ),
            model_id=provider.model_id,
            chars_budget=budget,
            **result_metadata,
        )
    scores = [item.score for item in selected]
    rendered = render_readable_relevant_lines(selected)
    chars_used = len("\n".join(rendered)) if rendered else 0
    return SemanticReinjectionResult(
        status="selected",
        skip_reason=None,
        model_id=provider.model_id,
        selected=selected,
        chars_used=chars_used,
        chars_budget=budget,
        score_hi=max(scores),
        score_lo=min(scores),
        **result_metadata,
    )
