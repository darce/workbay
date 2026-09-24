"""Classification and queue admission for terminal lane branch disposal.

This module is deliberately a producer only.  Classification consumes an
already-computed branch reclaim verdict and caller-supplied observations; it
does not repeat git or database probes.  Queue admission records a retryable
fact for the existing branch-reclaim consumer and never deletes a ref.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable

from . import branch_reclaim_queue
from .lane_reclaim import ReclaimVerdict


class BranchDisposition(StrEnum):
    """The first-match reason a terminal lane can be disposed."""

    MERGED_ANCESTRY = "merged_ancestry"
    NEVER_STARTED = "never_started"
    REVIEW_PERSISTED = "review_persisted"
    REVIEW_UNPRODUCED = "review_unproduced"
    CONTENT_LANDED = "content_landed"
    SUPERSEDED_BY_SIBLING = "superseded_by_sibling"
    HELD_UNIQUE_WORK = "held_unique_work"
    UNDECIDABLE = "undecidable"

    def __eq__(self, other: object) -> bool:
        """Keep ref-bearing superseded labels comparable to their taxonomy member."""

        if isinstance(other, BranchDisposition):
            left = getattr(self, "_canonical_member", self)
            right = getattr(other, "_canonical_member", other)
            return left is right
        canonical = getattr(self, "_canonical_member", None)
        if canonical is not None and other == canonical.value:
            return True
        return str.__eq__(self, other)

    def __hash__(self) -> int:
        canonical = getattr(self, "_canonical_member", self)
        return str.__hash__(canonical)

    @property
    def sibling_ref(self) -> str | None:
        """Return the local head carried by a superseded label, if any."""

        value = getattr(self, "_sibling_ref", None)
        return value if isinstance(value, str) else None


_TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
_IMPLEMENT_LANE_KIND = "implement"
_REVIEW_LANE_KIND = "review"
_SUPPORTED_LANE_KINDS = frozenset({_IMPLEMENT_LANE_KIND, _REVIEW_LANE_KIND})
# Keep this allowlist aligned with lane_reclaim.lane_branch_reclaimable: its
# canonical production-path filter excludes only lane scaffolding, so authored
# output under both of these trees remains production content.
_REVIEW_PRODUCTION_PATH_PREFIXES = ("docs/reviews/", "docs/assessments/")
_CRASH_TERMINAL_MARKERS = frozenset(
    {
        "crash",
        "crashed",
        "crash_terminal",
        "process_crash",
        "process_crashed",
        "worker_crash",
        "worker_crashed",
    }
)


def _text(value: object) -> str:
    if isinstance(value, StrEnum):
        value = value.value
    return value.strip() if isinstance(value, str) else ""


def _token(value: object) -> str:
    return _text(value).lower().replace("-", "_")


def _first_value(*values: object) -> object:
    for value in values:
        if value is not None:
            return value
    return None


def _as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1"}:
            return True
        if normalized in {"false", "no", "0"}:
            return False
    return None


def _as_paths(value: object) -> list[str] | None:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [item for item in value if isinstance(item, str) and item.strip()]
    if isinstance(value, set):
        return [item for item in value if isinstance(item, str) and item.strip()]
    return None


def _normalize_ref(value: object) -> str:
    ref = _text(value).replace("\\", "/")
    if ref.startswith("refs/heads/"):
        return ref[len("refs/heads/") :]
    return ref


def _looks_like_ref(value: str) -> bool:
    normalized = value.replace("\\", "/")
    if normalized.startswith("refs/"):
        return True
    if normalized.startswith((".", "/", "docs/", "packages/", "src/")):
        return False
    return bool(normalized)


def _survivor_ref_names(raw: object) -> list[str]:
    """Extract ref names from the verdict's list or path/ref mapping shape."""

    if isinstance(raw, Mapping):
        names: list[str] = []
        for key, value in raw.items():
            key_text = _text(key)
            if key_text and _looks_like_ref(key_text):
                names.append(key_text)
            values = _as_paths(value)
            if values:
                names.extend(value for value in values if _looks_like_ref(value))
        return list(dict.fromkeys(names))
    values = _as_paths(raw)
    return list(dict.fromkeys(values or []))


def _review_paths_have_survivors(raw: object, production_paths: list[str]) -> bool:
    """Accept both the current ref-list observation and path/ref test shapes."""

    if not production_paths:
        return False
    if isinstance(raw, Mapping):
        normalized_paths = {path.replace("\\", "/").removeprefix("./") for path in production_paths}
        matched: set[str] = set()
        for key, value in raw.items():
            key_text = _text(key).replace("\\", "/").removeprefix("./")
            values = _as_paths(value) or []
            value_paths = {
                candidate.replace("\\", "/").removeprefix("./")
                for candidate in values
                if candidate.startswith((".", "/", "docs/", "packages/", "src/"))
            }
            if key_text in normalized_paths and values:
                matched.add(key_text)
            matched.update(normalized_paths.intersection(value_paths))
        if matched == normalized_paths:
            return True
        # lane_branch_reclaimable currently records candidate ref names, not a
        # per-path map. A non-empty mapping of refs is sufficient because the
        # verdict's empty unpreserved_paths already proves path preservation.
        return bool(_survivor_ref_names(raw))
    return bool(_survivor_ref_names(raw))


def _under_reviews(path: str) -> bool:
    normalized = path.replace("\\", "/").removeprefix("./")
    return any(
        normalized.startswith(prefix) and len(normalized) > len(prefix) for prefix in _REVIEW_PRODUCTION_PATH_PREFIXES
    )


def _lane_kind_details(row: Mapping[str, Any]) -> tuple[str, bool]:
    """Resolve the lane kind while distinguishing legacy absence from garbage."""

    if "lane_kind" not in row:
        # Old rows omitted the column and historically followed implement-lane
        # classification. Preserve that compatibility rule independently of
        # an explicit but unsupported value.
        return _IMPLEMENT_LANE_KIND, False
    return _token(row.get("lane_kind")), True


def _evidence_present(explicit: object, row: Mapping[str, Any]) -> bool:
    value = _first_value(
        explicit,
        row.get("review_receipt_recorded"),
        row.get("review_receipt_present"),
        row.get("has_review_receipt"),
        row.get("review_evidence"),
        row.get("parsed_harvest"),
        row.get("parsed_harvest_block"),
        row.get("harvest_block_present"),
        row.get("harvest_recorded"),
        row.get("review_receipt"),
        row.get("review_receipt_status"),
    )
    if isinstance(value, Mapping):
        return value.get("status") == "recorded" or value.get("recorded") is True
    return _as_bool(value) is True or _token(value) == "recorded"


def _finding_count(explicit: object, row: Mapping[str, Any]) -> int | None:
    value = _first_value(
        explicit,
        row.get("review_findings_count"),
        row.get("review_finding_count"),
        row.get("finding_count"),
        row.get("findings_count"),
        row.get("review_findings"),
    )
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return len(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _no_commits_between_base_and_tip(
    explicit: object,
    row: Mapping[str, Any],
    observed: Mapping[str, Any],
) -> bool:
    value = _first_value(
        explicit,
        row.get("no_commits"),
        row.get("no_commits_between_base_and_tip"),
        row.get("no_commits_between_base_tip"),
        row.get("never_started"),
        observed.get("no_commits"),
        observed.get("no_commits_between_base_and_tip"),
    )
    resolved = _as_bool(value)
    if resolved is not None:
        return resolved
    commit_count = _first_value(row.get("commit_count"), observed.get("commit_count"))
    if commit_count is not None:
        try:
            return int(commit_count) == 0
        except (TypeError, ValueError):
            pass
    for candidate in (
        row.get("commits_between_base_and_tip"),
        observed.get("commits_between_base_and_tip"),
    ):
        paths = _as_paths(candidate)
        if paths is not None:
            return not paths
    base = _first_value(
        row.get("base_sha"),
        row.get("lane_base_sha"),
        row.get("base_commit_sha"),
        observed.get("base_sha"),
    )
    tip = _first_value(
        row.get("tip_sha"),
        row.get("branch_tip_sha"),
        row.get("branch_sha"),
        observed.get("tip_sha"),
        observed.get("branch_sha"),
    )
    return bool(_text(base) and _text(tip) and _text(base) == _text(tip))


def _lane_row_data(
    lane_row: Mapping[str, Any] | None,
    lane: Mapping[str, Any] | None,
    row: Mapping[str, Any] | None,
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for source in (lane_row, lane, row):
        if isinstance(source, Mapping):
            result.update(source)
        elif source is not None and hasattr(source, "__dict__"):
            result.update(vars(source))
    result.update(extra)
    return result


def _row_is_nonterminal_with_missing_worktree(
    row: Mapping[str, Any],
    observed: Mapping[str, Any],
) -> bool:
    status = _token(_first_value(row.get("status"), observed.get("status")))
    explicitly_nonterminal = _as_bool(_first_value(row.get("non_terminal"), row.get("is_nonterminal")))
    nonterminal = explicitly_nonterminal is True or (bool(status) and status not in _TERMINAL_STATUSES)
    if not nonterminal:
        return False

    live_ref = _as_bool(
        _first_value(
            row.get("live_ref"),
            row.get("branch_ref_live"),
            row.get("branch_exists"),
            row.get("ref_exists"),
            row.get("has_live_ref"),
            observed.get("live_ref"),
        )
    )
    if live_ref is None:
        live_ref = bool(
            _text(
                _first_value(
                    row.get("branch"),
                    row.get("branch_ref"),
                    row.get("branch_tip_sha"),
                    observed.get("branch"),
                    observed.get("branch_sha"),
                )
            )
        )
    if not live_ref:
        return False

    worktree_missing = _as_bool(
        _first_value(
            row.get("worktree_missing"),
            row.get("missing_worktree"),
            observed.get("worktree_missing"),
        )
    )
    if worktree_missing is None:
        worktree_exists = _as_bool(
            _first_value(
                row.get("worktree_exists"),
                row.get("has_worktree"),
                row.get("worktree_present"),
                observed.get("worktree_exists"),
            )
        )
        if worktree_exists is not None:
            worktree_missing = not worktree_exists
    if worktree_missing is None:
        path = _first_value(row.get("worktree_path"), observed.get("worktree_path"))
        if path is not None:
            worktree_missing = not bool(_text(path))
        elif "worktree_path" in row or "worktree_path" in observed:
            worktree_missing = True
    return worktree_missing is True


def _superseded_label(ref: str) -> BranchDisposition:
    """Return a ref-bearing StrEnum instance without adding a taxonomy member."""

    label = f"{BranchDisposition.SUPERSEDED_BY_SIBLING.value}:{ref}"
    result = str.__new__(BranchDisposition, label)
    result._name_ = BranchDisposition.SUPERSEDED_BY_SIBLING.name
    result._value_ = label
    result._canonical_member = BranchDisposition.SUPERSEDED_BY_SIBLING
    result._sibling_ref = ref
    return result


@dataclass(frozen=True)
class _ClassificationContext:
    """Immutable observations consumed by the ordered disposition table."""

    row: Mapping[str, Any]
    observed: Mapping[str, Any]
    reclaimable: bool
    is_ancestor: bool
    no_commits: bool
    lane_kind: str
    lane_kind_present: bool
    evidence: bool
    findings: int | None
    production_paths: list[str] | None
    survivor_refs: tuple[str, ...]
    unpreserved_paths: list[str] | None
    review_paths_persisted: bool
    integration_survivor: bool
    sibling_refs: tuple[str, ...]


def _build_classification_context(
    verdict: ReclaimVerdict,
    row_data: Mapping[str, Any],
    is_ancestor: bool | None,
    no_commits: bool | None,
    review_evidence: bool | None,
    review_findings_count: int | None,
    integration_ref: str | None,
) -> _ClassificationContext:
    observed_raw = getattr(verdict, "observed", {})
    observed = dict(observed_raw) if isinstance(observed_raw, Mapping) else {}
    reclaimable = getattr(verdict, "reclaimable", False) is True
    resolved_ancestor = (
        _as_bool(_first_value(is_ancestor, row_data.get("is_ancestor"), observed.get("is_ancestor"))) is True
    )
    resolved_no_commits = _no_commits_between_base_and_tip(no_commits, row_data, observed)
    production_paths = _as_paths(observed.get("production_paths"))
    survivor_refs = tuple(_survivor_ref_names(observed.get("survivor_refs")))
    unpreserved_paths = _as_paths(observed.get("unpreserved_paths"))
    lane_kind, lane_kind_present = _lane_kind_details(row_data)
    evidence = _evidence_present(review_evidence, row_data)
    findings = _finding_count(review_findings_count, row_data)
    review_paths_persisted = (
        production_paths is not None
        and bool(production_paths)
        and all(_under_reviews(path) for path in production_paths)
        and _review_paths_have_survivors(observed.get("survivor_refs"), production_paths)
    )
    resolved_integration = _normalize_ref(
        _first_value(integration_ref, row_data.get("integration_ref"), observed.get("integration_ref")) or "main"
    )
    lane_branch = _normalize_ref(
        _first_value(row_data.get("branch"), row_data.get("branch_ref"), observed.get("branch"))
    )
    integration_survivor = any(_normalize_ref(ref) == resolved_integration for ref in survivor_refs)
    sibling_refs = tuple(
        ref
        for ref in survivor_refs
        if _normalize_ref(ref) not in {resolved_integration, lane_branch} and _looks_like_ref(ref)
    )
    return _ClassificationContext(
        row=row_data,
        observed=observed,
        reclaimable=reclaimable,
        is_ancestor=resolved_ancestor,
        no_commits=resolved_no_commits,
        lane_kind=lane_kind,
        lane_kind_present=lane_kind_present,
        evidence=evidence,
        findings=findings,
        production_paths=production_paths,
        survivor_refs=survivor_refs,
        unpreserved_paths=unpreserved_paths,
        review_paths_persisted=review_paths_persisted,
        integration_survivor=integration_survivor,
        sibling_refs=sibling_refs,
    )


def _is_unknown_lane_kind(context: _ClassificationContext) -> bool:
    return context.lane_kind_present and context.lane_kind not in _SUPPORTED_LANE_KINDS


def _is_never_started(context: _ClassificationContext) -> bool:
    return context.no_commits


def _is_merged_ancestry(context: _ClassificationContext) -> bool:
    return context.is_ancestor


def _is_review_persisted(context: _ClassificationContext) -> bool:
    return (
        context.lane_kind == _REVIEW_LANE_KIND
        and context.evidence
        and context.reclaimable
        and context.review_paths_persisted
        and not context.unpreserved_paths
    )


def _is_review_unproduced(context: _ClassificationContext) -> bool:
    return (
        context.lane_kind == _REVIEW_LANE_KIND
        and context.reclaimable
        and context.evidence
        and context.production_paths == []
        and context.findings == 0
    )


def _is_content_landed(context: _ClassificationContext) -> bool:
    return (
        not context.is_ancestor
        and context.reclaimable
        and bool(context.production_paths)
        and context.integration_survivor
        and not context.unpreserved_paths
        and bool(context.survivor_refs)
    )


def _is_superseded_by_sibling(context: _ClassificationContext) -> bool:
    return (
        not context.is_ancestor
        and context.reclaimable
        and bool(context.production_paths)
        and bool(context.sibling_refs)
        and not context.unpreserved_paths
        and not context.integration_survivor
    )


def _is_held_unique_work(context: _ClassificationContext) -> bool:
    return (not context.reclaimable and bool(context.unpreserved_paths)) or _row_is_nonterminal_with_missing_worktree(
        context.row, context.observed
    )


def _always_true(_context: _ClassificationContext) -> bool:
    return True


_ClassificationPredicate = Callable[[_ClassificationContext], bool]
_CLASSIFICATION_RULES: tuple[tuple[_ClassificationPredicate, BranchDisposition], ...] = (
    (_is_unknown_lane_kind, BranchDisposition.UNDECIDABLE),
    # Never-started must precede ancestry: base == tip can satisfy both.
    (_is_never_started, BranchDisposition.NEVER_STARTED),
    (_is_merged_ancestry, BranchDisposition.MERGED_ANCESTRY),
    (_is_review_persisted, BranchDisposition.REVIEW_PERSISTED),
    (_is_review_unproduced, BranchDisposition.REVIEW_UNPRODUCED),
    (_is_content_landed, BranchDisposition.CONTENT_LANDED),
    (_is_superseded_by_sibling, BranchDisposition.SUPERSEDED_BY_SIBLING),
    (_is_held_unique_work, BranchDisposition.HELD_UNIQUE_WORK),
    (_always_true, BranchDisposition.UNDECIDABLE),
)


def _resolve_table_disposition(context: _ClassificationContext, disposition: BranchDisposition) -> BranchDisposition:
    if disposition is BranchDisposition.SUPERSEDED_BY_SIBLING:
        return _superseded_label(context.sibling_refs[0])
    return disposition


def classify_terminal_lane(
    verdict: ReclaimVerdict,
    lane_row: Mapping[str, Any] | None = None,
    is_ancestor: bool | None = None,
    no_commits: bool | None = None,
    review_evidence: bool | None = None,
    review_findings_count: int | None = None,
    integration_ref: str | None = None,
    *,
    lane: Mapping[str, Any] | None = None,
    row: Mapping[str, Any] | None = None,
    **row_fields: Any,
) -> BranchDisposition:
    """Classify a branch from a previously computed reclaim verdict.

    The booleans in this signature are caller-owned observations. In
    particular, ``is_ancestor`` must be obtained by the actuator's existing
    ancestry probe, and review evidence/findings must come from the caller's
    receipt/harvest read. No filesystem, git, or database operation belongs in
    this function.
    """

    row_data = _lane_row_data(lane_row, lane, row, row_fields)
    context = _build_classification_context(
        verdict,
        row_data,
        is_ancestor,
        no_commits,
        review_evidence,
        review_findings_count,
        integration_ref,
    )
    for predicate, disposition in _CLASSIFICATION_RULES:
        if predicate(context):
            return _resolve_table_disposition(context, disposition)
    return BranchDisposition.UNDECIDABLE


def _normalized_outcome(outcome: object, crash_terminal: object = None) -> tuple[str, bool | None]:
    """Read outcome and crash fields from one shared mapping contract."""

    if isinstance(outcome, Mapping):
        token_value = _first_value(outcome.get("outcome"), outcome.get("status"), outcome.get("handoff_action"))
        crash_values = (crash_terminal, outcome.get("crash_terminal"), outcome.get("crashed"))
    else:
        token_value = outcome
        crash_values = (crash_terminal,)
    flags = [_as_bool(value) for value in crash_values]
    if any(flag is True for flag in flags):
        crash_flag: bool | None = True
    elif any(flag is False for flag in flags):
        crash_flag = False
    else:
        crash_flag = None
    return _token(token_value), crash_flag


def _outcome_is_crash_terminal(outcome: object, crash_terminal: object) -> bool:
    token, normalized_crash = _normalized_outcome(outcome, crash_terminal)
    if normalized_crash is True:
        return True
    if not token:
        return True
    return token in _CRASH_TERMINAL_MARKERS or "crash" in token


def _outcome_token(outcome: object) -> str:
    token, _normalized_crash = _normalized_outcome(outcome)
    return token


def dispose_terminal_lane(
    task_ref: str,
    lane_id: str,
    branch: str,
    tip_sha: str | None = None,
    outcome: object = None,
    *,
    authorized_sha: str | None = None,
    branch_tip_sha: str | None = None,
    sha: str | None = None,
    tip: str | None = None,
    branch_sha: str | None = None,
    terminal_outcome: object = None,
    crash_terminal: bool | None = None,
    is_crash_terminal: bool | None = None,
    log: Callable[..., Any] | None = None,
    observed_at: str | None = None,
    lane_kind: str | None = None,
    disposition: BranchDisposition | str | None = None,
    **_context: Any,
) -> bool:
    """Enqueue one retryable terminal-lane fact, without deleting anything.

    Queue identity is the existing ``(task_ref, lane_id, authorized_sha)``
    tuple. The queue producer's stable decision id therefore coalesces repeat
    calls without requiring a read or a second persistence mechanism here.
    """

    resolved_sha = next(
        (value for value in (tip_sha, authorized_sha, branch_tip_sha, sha, tip, branch_sha) if _text(value)),
        None,
    )
    resolved_outcome = _first_value(outcome, terminal_outcome)
    if not _text(task_ref) or not _text(lane_id) or not _text(branch) or not _text(resolved_sha):
        return False
    if _outcome_token(resolved_outcome) in {"error", "needs_guidance"} or _outcome_is_crash_terminal(
        resolved_outcome, _first_value(crash_terminal, is_crash_terminal)
    ):
        return False
    return branch_reclaim_queue.enqueue_branch_reclaim_outcome(
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        authorized_sha=_text(resolved_sha),
        reason="terminal_lane",
        observed_at=observed_at,
        log=log,
        _producer=branch_reclaim_queue.PRODUCER_TERMINAL_LANE,
    )


__all__ = [
    "BranchDisposition",
    "classify_terminal_lane",
    "dispose_terminal_lane",
]
