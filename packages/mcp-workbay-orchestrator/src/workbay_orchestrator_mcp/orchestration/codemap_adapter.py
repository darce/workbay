"""Qualifying adapter over codebase-memory-mcp ("codemap") for orchestrator use.

Codemap's graph is useful; its API contract is not safe for automated consumers.
Silent enum acceptance, silent truncation, false absences on test-only callers,
an unbacked ``data_flow`` mode, and a co-occurrence ``semantic_query`` that never
says "no match" all produce answers that *look* definitive. This module does not
reimplement the graph — it wraps an injected transport and returns a
:class:`QualifiedResult` that carries its own trustworthiness.

Transport is always injected (``Callable[[str, dict], dict]``). Tests supply a
stub that returns canned envelopes; production may bind a CLI/MCP client. No
subprocess invocation lives inside the functions under test.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, MutableMapping, Sequence

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

CodemapTransport = Callable[[str, Mapping[str, Any]], Mapping[str, Any]]
"""``transport(tool_name, payload) -> envelope`` — no I/O assumed."""

CommitAncestry = Callable[[str, str], bool]
"""``commit_ancestry(candidate_sha, head_sha) -> True`` if *candidate* is an
ancestor of *head* (or equal). The one grounded staleness signal available
outside the binary. Callers typically bind ``git merge-base --is-ancestor``."""


class Completeness(str, Enum):
    """How complete the returned item set is believed to be."""

    COMPLETE = "complete"
    TRUNCATED = "truncated"
    UNKNOWN = "unknown"


class IndexState(str, Enum):
    """Whether the codemap index is safe to treat as current relative to HEAD."""

    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class QualifiedResult:
    """A codemap answer that carries its own trustworthiness.

    Rules of thumb for consumers:

    * Only act on ``absence_claim_allowed is True`` when concluding "nothing
      exists" (no callers, no matches, dead code).
    * ``completeness is TRUNCATED`` means more rows may exist; never treat the
      list as exhaustive.
    * ``completeness is UNKNOWN`` means the envelope shape itself is untrustworthy
      (e.g. missing expected keys after a silent enum reject upstream).
    * ``notes`` are human-readable qualifications — surface them, do not drop them.
    """

    items: list[Any]
    completeness: Completeness
    absence_claim_allowed: bool
    index_state: IndexState
    notes: list[str] = field(default_factory=list)
    raw: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": list(self.items),
            "completeness": self.completeness.value,
            "absence_claim_allowed": self.absence_claim_allowed,
            "index_state": self.index_state.value,
            "notes": list(self.notes),
            "raw": dict(self.raw) if self.raw is not None else None,
        }


# ---------------------------------------------------------------------------
# Errors (typed, loud — never silent)
# ---------------------------------------------------------------------------


class CodemapAdapterError(ValueError):
    """Base for adapter-side rejections (raised before or instead of a call)."""


class InvalidEnumError(CodemapAdapterError):
    """Closed vocabulary violated — raised *before* the transport is called."""

    def __init__(self, param: str, value: Any, valid: Sequence[str]) -> None:
        self.param = param
        self.value = value
        self.valid = tuple(valid)
        # Preserve caller-provided order (docs/agents expect inbound|outbound|both).
        valid_str = "|".join(valid)
        super().__init__(
            f"Invalid {param}={value!r}; valid values are {valid_str}. "
            f"Codemap silently accepts unknown enums and returns an empty-looking "
            f"envelope — the adapter refuses to call rather than launder a false absence."
        )


class AdvertisedButUnbackedError(CodemapAdapterError):
    """A mode/feature is advertised by codemap but unbacked by the graph census."""

    def __init__(self, feature: str, detail: str) -> None:
        self.feature = feature
        self.detail = detail
        super().__init__(
            f"Codemap feature {feature!r} is advertised but unbacked: {detail}. "
            f"The adapter refuses silent degradation."
        )


class CappedApiRefuseError(CodemapAdapterError):
    """Caller asked for a COMPLETE / absence-licensing answer via the capped API."""

    def __init__(self, tool: str, alternative: str) -> None:
        self.tool = tool
        self.alternative = alternative
        super().__init__(
            f"{tool} is silently capped (no has_more/total/offset) and cannot "
            f"license a COMPLETE answer or an absence claim. Use {alternative} "
            f"instead (Cypher enumeration returns correct counts), or call with "
            f"require_complete=False and accept a qualified non-exhaustive result."
        )


# ---------------------------------------------------------------------------
# Closed vocabularies
# ---------------------------------------------------------------------------

VALID_TRACE_DIRECTIONS: frozenset[str] = frozenset({"inbound", "outbound", "both"})
# Modes that silently degrade to plain CALLS when the edge type is absent.
_UNBACKED_TRACE_MODES: frozenset[str] = frozenset({"data_flow", "data-flow", "dataflow"})

# Default page size observed on the live binary (silent hard cap).
DEFAULT_TRACE_LIMIT = 100

# Tools
TOOL_TRACE_PATH = "trace_path"
TOOL_QUERY_GRAPH = "query_graph"
TOOL_SEMANTIC_QUERY = "semantic_query"
TOOL_SEARCH_GRAPH = "search_graph"
TOOL_INDEX_STATUS = "index_status"

# Notes (stable strings tests / callers may match)
NOTE_MISSING_CALLERS_KEY = (
    "envelope missing 'callers' key — not equivalent to zero callers; "
    "often produced by codemap after an invalid direction enum is silently accepted"
)
NOTE_TRUNCATION_HEURISTIC = (
    "len(items) == limit: treating as TRUNCATED (heuristic — exactly-N is "
    "indistinguishable from truncated-at-N; codemap exposes no has_more/total/offset)"
)
NOTE_INDEX_STALE_NOT_ANCESTOR = (
    "index head_sha is not an ancestor of caller HEAD — index_state=STALE; "
    "absence claims are not licensed"
)
NOTE_INDEX_STALE_LAG = (
    "index head_sha is a strict ancestor of caller HEAD (index lags) — "
    "index_state=STALE; absence claims are not licensed"
)
NOTE_INDEX_UNKNOWN = (
    "index freshness unknown (missing head_sha and/or index sha, or no "
    "commit_ancestry checker) — absence claims are not licensed"
)
NOTE_SEMANTIC_ADVISORY = (
    "semantic_query is a per-repo co-occurrence space, not a pretrained model; "
    "results are advisory and never license an absence claim. Prefer BM25 "
    "search_graph(--query) for deterministic lexical hits."
)
NOTE_INCLUDE_TESTS_DEFAULT = (
    "include_tests defaulted to True for caller/blast-radius queries "
    "(codemap's own default is False, which hides test-only callers)"
)
NOTE_ROUTED_TO_QUERY_GRAPH = (
    "require_complete=True: routed to query_graph (Cypher) because trace_path "
    "is silently capped and cannot license COMPLETE / absence claims"
)
NOTE_QUERY_DETERMINISTIC = "query class=deterministic (structural graph traversal)"
NOTE_QUERY_SEMANTIC = "query class=semantic (advisory co-occurrence)"
NOTE_QUERY_EXHAUSTIVE_GRAPH = "query class=deterministic exhaustive (query_graph Cypher)"


# ---------------------------------------------------------------------------
# Index freshness (commit_ancestry is the grounded signal)
# ---------------------------------------------------------------------------


def _sha_equal(a: str, b: str) -> bool:
    """Prefix-tolerant SHA equality (full vs abbreviated)."""
    a = a.strip().lower()
    b = b.strip().lower()
    if not a or not b:
        return False
    return a == b or a.startswith(b) or b.startswith(a)


def resolve_index_state(
    *,
    index_sha: str | None,
    head_sha: str | None,
    commit_ancestry: CommitAncestry | None = None,
) -> tuple[IndexState, list[str]]:
    """Classify index freshness relative to the caller's HEAD.

    Grounded rules:

    * Both SHAs present and equal → :attr:`IndexState.FRESH`.
    * Both present, unequal, and ``commit_ancestry(index, head)`` is False →
      :attr:`IndexState.STALE` (diverged / rewritten).
    * Both present, unequal, and ``commit_ancestry(index, head)`` is True →
      :attr:`IndexState.STALE` (index lags HEAD; still unsafe for absence).
    * Missing SHAs or no ancestry checker when unequal → :attr:`IndexState.UNKNOWN`.

    Absence claims require FRESH. STALE and UNKNOWN both set
    ``absence_claim_allowed=False``.
    """
    notes: list[str] = []
    idx = (index_sha or "").strip() or None
    head = (head_sha or "").strip() or None
    if idx is None or head is None:
        notes.append(NOTE_INDEX_UNKNOWN)
        return IndexState.UNKNOWN, notes
    if _sha_equal(idx, head):
        return IndexState.FRESH, notes
    if commit_ancestry is None:
        notes.append(NOTE_INDEX_UNKNOWN)
        return IndexState.UNKNOWN, notes
    try:
        is_anc = bool(commit_ancestry(idx, head))
    except Exception as exc:  # noqa: BLE001 — surface as unknown, never crash
        notes.append(f"{NOTE_INDEX_UNKNOWN} (commit_ancestry raised: {exc})")
        return IndexState.UNKNOWN, notes
    if not is_anc:
        notes.append(NOTE_INDEX_STALE_NOT_ANCESTOR)
        return IndexState.STALE, notes
    # Strict ancestor: index lags HEAD.
    notes.append(NOTE_INDEX_STALE_LAG)
    return IndexState.STALE, notes


def extract_index_sha(envelope: Mapping[str, Any] | None) -> str | None:
    """Pull an index commit sha from an index_status-like envelope."""
    if not isinstance(envelope, Mapping):
        return None
    for key in ("head_sha", "commit_sha", "git_sha", "indexed_commit", "revision"):
        raw = envelope.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return None


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_trace_direction(direction: str) -> str:
    """Reject unknown ``trace_path`` directions *before* the transport call.

    Codemap accepts only ``inbound|outbound|both``. Values like ``\"in\"`` /
    ``\"out\"`` / ``\"banana\"`` are silently accepted by the binary and return
    an envelope with no ``callers`` key — indistinguishable from zero callers.
    """
    if direction not in VALID_TRACE_DIRECTIONS:
        # Fixed doc order: inbound|outbound|both (not alphabetical).
        raise InvalidEnumError("direction", direction, ("inbound", "outbound", "both"))
    return direction


def validate_trace_mode(mode: str | None) -> str | None:
    """Refuse advertised-but-unbacked modes (notably ``data_flow``).

    Live edge census contains zero ``DATA_FLOWS`` rows; ``mode=data_flow``
    silently degrades to plain ``CALLS``. The adapter raises rather than
    laundering that degradation.
    """
    if mode is None:
        return None
    normalized = str(mode).strip()
    if not normalized:
        return None
    if normalized.lower() in _UNBACKED_TRACE_MODES:
        raise AdvertisedButUnbackedError(
            "mode=data_flow",
            "edge census contains zero DATA_FLOWS rows; the binary silently "
            "degrades to plain CALLS",
        )
    return normalized


# ---------------------------------------------------------------------------
# Completeness + absence licensing
# ---------------------------------------------------------------------------


def _qualify_items(
    items: list[Any],
    *,
    limit: int | None,
    index_state: IndexState,
    notes: list[str],
    deterministic: bool,
    exhaustive: bool,
    envelope_ok: bool,
) -> QualifiedResult:
    """Apply completeness / absence rules to a retrieved item list.

    Strict rules:

    * Missing / unparseable envelope → ``UNKNOWN``, absence not allowed.
    * ``len(items) == limit`` → ``TRUNCATED``, absence not allowed (heuristic).
    * Fewer than limit + deterministic + exhaustive + fresh → ``COMPLETE``,
      absence allowed (including empty results).
    * Semantic (non-deterministic) → absence never allowed, even when empty.
    * Stale / unknown index → absence never allowed.
    """
    notes = list(notes)

    if not envelope_ok:
        return QualifiedResult(
            items=items,
            completeness=Completeness.UNKNOWN,
            absence_claim_allowed=False,
            index_state=index_state,
            notes=notes,
        )

    if limit is not None and limit > 0 and len(items) == limit:
        notes.append(NOTE_TRUNCATION_HEURISTIC)
        completeness = Completeness.TRUNCATED
    elif limit is not None and limit > 0 and len(items) < limit:
        completeness = Completeness.COMPLETE
    elif limit is None and exhaustive:
        # Uncapped exhaustive enumeration (e.g. query_graph with no page limit).
        completeness = Completeness.COMPLETE
    else:
        completeness = Completeness.UNKNOWN
        notes.append(
            "completeness unknown: no finite limit provided and query not marked exhaustive"
        )

    absence_ok = (
        completeness is Completeness.COMPLETE
        and index_state is IndexState.FRESH
        and deterministic
        and exhaustive
    )
    if not deterministic:
        notes.append(NOTE_QUERY_SEMANTIC)
    if index_state is IndexState.STALE and NOTE_INDEX_STALE_LAG not in notes and NOTE_INDEX_STALE_NOT_ANCESTOR not in notes:
        notes.append("index_state=STALE — absence claims are not licensed")
    if index_state is IndexState.UNKNOWN and NOTE_INDEX_UNKNOWN not in notes:
        notes.append(NOTE_INDEX_UNKNOWN)

    return QualifiedResult(
        items=items,
        completeness=completeness,
        absence_claim_allowed=absence_ok,
        index_state=index_state,
        notes=notes,
    )


def _normalize_item_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, tuple):
        return list(raw)
    return [raw]


def _build_payload(base: MutableMapping[str, Any], **extra: Any) -> dict[str, Any]:
    out = dict(base)
    for k, v in extra.items():
        if v is not None:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# Public query surfaces
# ---------------------------------------------------------------------------


def trace_callers(
    transport: CodemapTransport,
    *,
    target: str,
    direction: str = "inbound",
    include_tests: bool | None = None,
    limit: int = DEFAULT_TRACE_LIMIT,
    mode: str | None = None,
    project: str | None = None,
    head_sha: str | None = None,
    index_sha: str | None = None,
    commit_ancestry: CommitAncestry | None = None,
    require_complete: bool = False,
    depth: int | None = None,
) -> QualifiedResult:
    """Qualified caller / blast-radius lookup via ``trace_path``.

    Safe defaults
    -------------
    * ``include_tests`` defaults to **True**. Codemap's native default is False,
      which returns zero callers for symbols whose only callers are tests
      (measured: ``record_review_finding`` ≈ 129 test callers). Pass
      ``include_tests=False`` explicitly to opt out; the override is recorded
      in ``notes``.

    Completeness routing
    --------------------
    ``trace_path`` is silently capped (measured at 100; no ``has_more`` /
    ``total`` / ``offset``). When the caller needs a COMPLETE answer
    (``require_complete=True``), this function **refuses** with
    :class:`CappedApiRefuseError` naming ``query_graph`` as the alternative
    rather than auto-routing — the Cypher query is caller-owned so we do not
    invent a traversal that may not match their intent. Use
    :func:`query_graph_qualified` for exhaustive enumeration.

    Validation
    ----------
    * ``direction`` must be ``inbound|outbound|both`` — ``\"in\"`` raises.
    * ``mode=data_flow`` raises :class:`AdvertisedButUnbackedError`.
    """
    validate_trace_direction(direction)
    validate_trace_mode(mode)

    if require_complete:
        raise CappedApiRefuseError(TOOL_TRACE_PATH, TOOL_QUERY_GRAPH)

    tests_flag: bool
    notes: list[str] = [NOTE_QUERY_DETERMINISTIC]
    if include_tests is None:
        tests_flag = True
        notes.append(NOTE_INCLUDE_TESTS_DEFAULT)
    else:
        tests_flag = bool(include_tests)
        if not tests_flag:
            notes.append(
                "include_tests=False (explicit override) — test-only callers will be omitted"
            )

    index_state, index_notes = resolve_index_state(
        index_sha=index_sha,
        head_sha=head_sha,
        commit_ancestry=commit_ancestry,
    )
    notes.extend(index_notes)

    payload = _build_payload(
        {
            "target": target,
            "direction": direction,
            "include_tests": tests_flag,
            "limit": limit,
        },
        mode=mode,
        project=project,
        depth=depth,
    )
    # Some codemap builds take `function` / `symbol` rather than `target`; pass
    # both so the stub/binary can pick. The adapter does not re-shape beyond this.
    payload.setdefault("function", target)
    payload.setdefault("symbol", target)

    envelope = dict(transport(TOOL_TRACE_PATH, payload))

    # Missing 'callers' key is NOT zero callers — UNKNOWN completeness.
    if "callers" not in envelope:
        notes.append(NOTE_MISSING_CALLERS_KEY)
        return QualifiedResult(
            items=[],
            completeness=Completeness.UNKNOWN,
            absence_claim_allowed=False,
            index_state=index_state,
            notes=notes,
            raw=envelope,
        )

    items = _normalize_item_list(envelope.get("callers"))
    # trace_path at a finite limit is *not* exhaustive — even a short page is
    # only COMPLETE relative to the page, and may license absence only when the
    # page is under-full (heuristic: not truncated) on a fresh index.
    # Under-full + fresh + deterministic ⇒ we treat the page as exhaustive for
    # this query shape (the binary returned fewer than its cap).
    result = _qualify_items(
        items,
        limit=limit,
        index_state=index_state,
        notes=notes,
        deterministic=True,
        # A under-limit page from a deterministic capped API is the best
        # exhaustive signal available without query_graph.
        exhaustive=True,
        envelope_ok=True,
    )
    return QualifiedResult(
        items=result.items,
        completeness=result.completeness,
        absence_claim_allowed=result.absence_claim_allowed,
        index_state=result.index_state,
        notes=result.notes,
        raw=envelope,
    )


def query_graph_qualified(
    transport: CodemapTransport,
    *,
    query: str,
    project: str | None = None,
    head_sha: str | None = None,
    index_sha: str | None = None,
    commit_ancestry: CommitAncestry | None = None,
    limit: int | None = None,
    result_key: str = "results",
) -> QualifiedResult:
    """Qualified Cypher ``query_graph`` call for exhaustive enumeration.

    Prefer this over :func:`trace_callers` whenever the orchestrator needs a
    COMPLETE answer or an absence claim. Measured: Cypher returned correct
    counts where ``trace_path`` silently capped at 100.
    """
    notes: list[str] = [NOTE_QUERY_EXHAUSTIVE_GRAPH, NOTE_ROUTED_TO_QUERY_GRAPH]
    index_state, index_notes = resolve_index_state(
        index_sha=index_sha,
        head_sha=head_sha,
        commit_ancestry=commit_ancestry,
    )
    notes.extend(index_notes)

    payload = _build_payload({"query": query}, project=project, limit=limit)
    envelope = dict(transport(TOOL_QUERY_GRAPH, payload))

    if result_key not in envelope and "rows" not in envelope and "nodes" not in envelope:
        notes.append(
            f"envelope missing result key {result_key!r} (also tried 'rows'/'nodes') — "
            "not equivalent to zero rows"
        )
        return QualifiedResult(
            items=[],
            completeness=Completeness.UNKNOWN,
            absence_claim_allowed=False,
            index_state=index_state,
            notes=notes,
            raw=envelope,
        )

    raw_items = envelope.get(result_key)
    if raw_items is None:
        raw_items = envelope.get("rows", envelope.get("nodes"))
    items = _normalize_item_list(raw_items)

    result = _qualify_items(
        items,
        limit=limit,
        index_state=index_state,
        notes=notes,
        deterministic=True,
        exhaustive=True,
        envelope_ok=True,
    )
    return QualifiedResult(
        items=result.items,
        completeness=result.completeness,
        absence_claim_allowed=result.absence_claim_allowed,
        index_state=result.index_state,
        notes=result.notes,
        raw=envelope,
    )


def semantic_query_qualified(
    transport: CodemapTransport,
    *,
    query: str | Sequence[str],
    project: str | None = None,
    head_sha: str | None = None,
    index_sha: str | None = None,
    commit_ancestry: CommitAncestry | None = None,
    limit: int | None = None,
) -> QualifiedResult:
    """Qualified ``semantic_query`` — always advisory, never licenses absence.

    Codemap's ``semantic_query`` is a per-repo co-occurrence space, not a
    pretrained embedding model. Measured: OOV terms still return hits at low
    cosine rather than signalling no-match. Empty results therefore must never
    set ``absence_claim_allowed=True``. Prefer BM25 ``search_graph`` with
    ``--query`` for deterministic lexical retrieval.
    """
    notes: list[str] = [NOTE_SEMANTIC_ADVISORY, NOTE_QUERY_SEMANTIC]
    index_state, index_notes = resolve_index_state(
        index_sha=index_sha,
        head_sha=head_sha,
        commit_ancestry=commit_ancestry,
    )
    notes.extend(index_notes)

    q: Any = list(query) if isinstance(query, (list, tuple)) else query
    payload = _build_payload({"semantic_query": q, "query": q}, project=project, limit=limit)
    envelope = dict(transport(TOOL_SEMANTIC_QUERY, payload))

    # Accept a few common result keys from the binary / CLI wrappers.
    raw_items = (
        envelope.get("semantic_results")
        if "semantic_results" in envelope
        else envelope.get("results", envelope.get("hits", envelope.get("items")))
    )
    items = _normalize_item_list(raw_items)

    # Even with a full/empty page: never exhaustive for absence purposes.
    result = _qualify_items(
        items,
        limit=limit,
        index_state=index_state,
        notes=notes,
        deterministic=False,
        exhaustive=False,
        envelope_ok=True,
    )
    # Belt-and-suspenders: force absence_claim_allowed False for semantic.
    return QualifiedResult(
        items=result.items,
        completeness=result.completeness,
        absence_claim_allowed=False,
        index_state=result.index_state,
        notes=result.notes,
        raw=envelope,
    )


def search_graph_bm25(
    transport: CodemapTransport,
    *,
    query: str,
    project: str | None = None,
    head_sha: str | None = None,
    index_sha: str | None = None,
    commit_ancestry: CommitAncestry | None = None,
    limit: int | None = None,
) -> QualifiedResult:
    """Qualified BM25 / ``--query`` ``search_graph`` — preferred over semantic.

    Lexical and deterministic relative to the index. Absence is only licensed
    when the result page is under-full, the index is FRESH, and a finite limit
    was applied (same truncation heuristic as ``trace_path``).
    """
    notes: list[str] = [
        NOTE_QUERY_DETERMINISTIC,
        "search_graph BM25/--query mode (preferred over semantic_query for lexical hits)",
    ]
    index_state, index_notes = resolve_index_state(
        index_sha=index_sha,
        head_sha=head_sha,
        commit_ancestry=commit_ancestry,
    )
    notes.extend(index_notes)

    payload = _build_payload({"query": query}, project=project, limit=limit)
    envelope = dict(transport(TOOL_SEARCH_GRAPH, payload))

    if "results" not in envelope and "hits" not in envelope:
        notes.append(
            "envelope missing 'results'/'hits' key — not equivalent to zero hits"
        )
        return QualifiedResult(
            items=[],
            completeness=Completeness.UNKNOWN,
            absence_claim_allowed=False,
            index_state=index_state,
            notes=notes,
            raw=envelope,
        )

    items = _normalize_item_list(envelope.get("results", envelope.get("hits")))
    result = _qualify_items(
        items,
        limit=limit if limit is not None else DEFAULT_TRACE_LIMIT,
        index_state=index_state,
        notes=notes,
        deterministic=True,
        exhaustive=True,
        envelope_ok=True,
    )
    return QualifiedResult(
        items=result.items,
        completeness=result.completeness,
        absence_claim_allowed=result.absence_claim_allowed,
        index_state=result.index_state,
        notes=result.notes,
        raw=envelope,
    )


def fetch_index_sha(
    transport: CodemapTransport,
    *,
    project: str | None = None,
) -> str | None:
    """Best-effort ``index_status`` → head_sha via the injected transport."""
    payload = _build_payload({}, project=project)
    envelope = dict(transport(TOOL_INDEX_STATUS, payload))
    return extract_index_sha(envelope)


__all__ = [
    "AdvertisedButUnbackedError",
    "CappedApiRefuseError",
    "CodemapAdapterError",
    "CodemapTransport",
    "CommitAncestry",
    "Completeness",
    "DEFAULT_TRACE_LIMIT",
    "IndexState",
    "InvalidEnumError",
    "QualifiedResult",
    "TOOL_INDEX_STATUS",
    "TOOL_QUERY_GRAPH",
    "TOOL_SEARCH_GRAPH",
    "TOOL_SEMANTIC_QUERY",
    "TOOL_TRACE_PATH",
    "VALID_TRACE_DIRECTIONS",
    "extract_index_sha",
    "fetch_index_sha",
    "query_graph_qualified",
    "resolve_index_state",
    "search_graph_bm25",
    "semantic_query_qualified",
    "trace_callers",
    "validate_trace_direction",
    "validate_trace_mode",
]
