"""Simplified reciprocal-rank fusion for implementation note retrieval-eval arm C.

This is a **simplified** RRF (k=60, original-query 2× weighting) fusing arm A
(semantic) and arm B (BM25). It is **not** the full five-component qmd shape
(position weights 0.75/0.60/0.40, top-rank bonus, strong-signal bypass). A
C-underperformance verdict is therefore about *this* simplified RRF, not about
hybrid retrieval as a class.

Offline / batch only — never imported by the reinjection hot path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Hashable, TypeVar

RefT = TypeVar("RefT", bound=Hashable)

# qmd RRF constant (upstream store.ts); keep pinned for reproducibility.
RRF_K = 60

# Original-query weight multiplier (qmd applies 2× to the original-query list
# relative to expanded variants). Both A and B rankings are produced from the
# original query only, so each list is weighted by this factor.
ORIGINAL_QUERY_WEIGHT = 2.0

# Human-readable disclaimer for reports and verdicts.
SIMPLIFIED_RRF_DISCLAIMER = (
    "Arm C is a SIMPLIFIED RRF (k=60, original-query 2×) over A+B only — not the "
    "full five-component qmd shape (position weights, top-rank bonus, strong-signal "
    "bypass). Underperformance of C is about this simplified fusion, not hybrid "
    "retrieval as a class."
)


def rrf_score_map(
    ranked_lists: Sequence[Sequence[RefT]],
    *,
    k: int = RRF_K,
    weights: Sequence[float] | None = None,
) -> dict[RefT, float]:
    """Accumulate RRF scores for each ref across ranked lists.

    ``score(d) = sum_i  weight_i / (k + rank_i(d))`` with 1-based ranks.
    A ref missing from a list contributes nothing from that list.
    """
    if k <= 0:
        raise ValueError(f"RRF k must be positive, got {k}")
    if not ranked_lists:
        return {}
    if weights is None:
        weights = tuple(ORIGINAL_QUERY_WEIGHT for _ in ranked_lists)
    if len(weights) != len(ranked_lists):
        raise ValueError(f"weights length {len(weights)} != ranked_lists length {len(ranked_lists)}")

    scores: dict[RefT, float] = {}
    for weight, ranked in zip(weights, ranked_lists, strict=True):
        seen: set[RefT] = set()
        for rank, ref in enumerate(ranked, start=1):
            if ref in seen:
                continue
            seen.add(ref)
            scores[ref] = scores.get(ref, 0.0) + float(weight) / float(k + rank)
    return scores


def rrf_fuse(
    ranked_lists: Sequence[Sequence[RefT]],
    *,
    k: int = RRF_K,
    weights: Sequence[float] | None = None,
    top_k: int | None = None,
) -> list[RefT]:
    """Fuse ranked lists with simplified RRF; return refs sorted by score desc.

    Ties break on the string form of the ref for determinism.
    """
    scores = rrf_score_map(ranked_lists, k=k, weights=weights)
    ordered = sorted(scores.items(), key=lambda item: (-item[1], str(item[0])))
    refs = [ref for ref, _ in ordered]
    if top_k is None:
        return refs
    if top_k <= 0:
        return []
    return refs[:top_k]


def rrf_fuse_semantic_lexical(
    semantic_ranked: Sequence[RefT],
    lexical_ranked: Sequence[RefT],
    *,
    k: int = RRF_K,
    original_query_weight: float = ORIGINAL_QUERY_WEIGHT,
    top_k: int | None = None,
) -> list[RefT]:
    """Fuse arm A (semantic) + arm B (lexical) with original-query 2× weights."""
    return rrf_fuse(
        (semantic_ranked, lexical_ranked),
        k=k,
        weights=(original_query_weight, original_query_weight),
        top_k=top_k,
    )


def rrf_score_breakdown(
    scores: Mapping[RefT, float],
) -> list[tuple[RefT, float]]:
    """Deterministic (score desc, ref str) view of an RRF score map."""
    return sorted(scores.items(), key=lambda item: (-item[1], str(item[0])))
