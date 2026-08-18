#!/usr/bin/env python3
"""Work→graph compiler: findings + operator items → lane manifest (implementation note S1).

Pure core (``compile_work_manifest``) is deterministic JSON-in / dict-out with no
I/O. Twin provisioning, admit-time blockers, and ``save_manifest`` live in the
thin ``__main__`` / CLI layer.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS, BackendSpec
from workbay_orchestrator_mcp.orchestration.host_resources import (
    COST_REMOTE,
    COST_REMOTE_API,
    HostMemoryPolicy,
    load_host_memory_policy,
)
from workbay_orchestrator_mcp.orchestration.lane_manifest import (
    _normalize_owned_path,
    _owned_path_roots_overlap,
    atomic_update_manifest,
    save_manifest,
    validate_manifest,
)

VERIFY_NAMESPACE = "__verify__"
# Wave-scoped sentinel for the single shared no_reviewer gate when local
# reviewers are exhausted (WIDTH-114 / WIDTH-117). One definition only —
# emission and holds_resolved_by_twin both derive the gate id from this, so
# the emitted id and the recognised id cannot diverge.
_WAVE_UNCOVERED_SENTINEL = "wave_uncovered"
_SHARED_WAVE_NO_REVIEWER_GATE_ID = f"{_WAVE_UNCOVERED_SENTINEL}{VERIFY_NAMESPACE}no_reviewer"
# Max local verify-twin reviewers minted per backend per compile. Mirrors
# HostMemoryPolicy.per_backend_local_cap — one concurrent local lane per
# backend name, not a shared pool of one across all backends (WIDTH-27).
# Off-box reviewers scale unboundedly; only local ones are capped.
# Instantiate policy for the default: dataclass class attrs are descriptors.
# Compile may receive an injected HostMemoryPolicy; these module defaults apply
# when none is supplied (WIDTH-44).
_MAX_LOCAL_REVIEWERS_PER_BACKEND = HostMemoryPolicy().per_backend_local_cap
# Compile-wide ceiling on local reviewers across all backends (WIDTH-42).
# Distinct from the per-backend bound. Derived after select_review_backends is
# defined (WIDTH-65): count backends selection can actually return that also
# pass _is_eligible_local_reviewer — not every eligible CLI in the registry
# (grok-cli is eligible but never selected while grok-remote represents the
# family). Not HostMemoryPolicy.max_width (admission width, not fan-out).
_MAX_LOCAL_REVIEWERS_TOTAL: int  # assigned after select_review_backends (below)
# Severity order for need-first local-reviewer allocation (WIDTH-101).
# Lower rank = higher need; unknown / missing severity sorts as low.
_SEVERITY_RANK: dict[str, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}
DEFAULT_DONE = "Ready for orchestrator branch review with lane-local verification complete."
FINDINGS_CLI_TIMEOUT_SECONDS = 30.0
_REMEDY_MENU = (
    "Remedies: (1) repartition owned file_paths so roots neither nest nor collide; "
    "(2) fuse the items with operator fuse_with; "
    "(3) order them with operator depends_on so the overlap is comparable."
)
_NARROW_PATHS_REMEDY = (
    "Remedies: narrow your owned file_paths so the lane prefixes fewer peers, "
    "or raise max_conflict_degree / WORKBAY_COMPILER_MAX_CONFLICT_DEGREE."
)
DEFAULT_MAX_CONFLICT_DEGREE = 8


class WorkGraphCompilerError(RuntimeError):
    """Escapable compiler refusal (reserved id, parent/child collision, max files, …)."""


@dataclass(frozen=True)
class CompileResult:
    """Pure compile output: manifest + side-effect descriptors for the CLI layer."""

    manifest: dict[str, Any]
    twin_provisioning: list[dict[str, Any]] = field(default_factory=list)
    blockers: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Union-find
# ---------------------------------------------------------------------------


class _UnionFind:
    def __init__(self, ids: list[str]) -> None:
        self._parent: dict[str, str] = {i: i for i in ids}

    def find(self, x: str) -> str:
        parent = self._parent
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Attach larger root under smaller for mild determinism (canonical is min member).
        if ra < rb:
            self._parent[rb] = ra
        else:
            self._parent[ra] = rb

    def components(self) -> dict[str, list[str]]:
        """Map an arbitrary root → sorted member ids."""
        buckets: dict[str, list[str]] = defaultdict(list)
        for member in sorted(self._parent):
            buckets[self.find(member)].append(member)
        for members in buckets.values():
            members.sort()
        return dict(sorted(buckets.items(), key=lambda kv: min(kv[1])))


# ---------------------------------------------------------------------------
# Work-item ingestion
# ---------------------------------------------------------------------------


def work_items_from_findings(findings_json: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """Map ``review_findings --operation list --status open`` envelope → work items.

    Findings are flat rows under ``data.findings`` (list output has no nested
    ``details`` object). Operator-only fields are never emitted.
    """
    if isinstance(findings_json, list):
        findings = findings_json
    elif isinstance(findings_json, dict):
        data = findings_json.get("data")
        if isinstance(data, dict):
            findings = data.get("findings") or []
        else:
            findings = findings_json.get("findings") or []
    else:
        raise WorkGraphCompilerError("findings_json must be a list or envelope dict")

    if not isinstance(findings, list):
        raise WorkGraphCompilerError("findings payload must be a list")

    items: list[dict[str, Any]] = []
    for raw in findings:
        if not isinstance(raw, dict):
            raise WorkGraphCompilerError("each finding must be an object")
        finding_id = raw.get("finding_id")
        if finding_id is None or str(finding_id).strip() == "":
            raise WorkGraphCompilerError("finding missing finding_id")
        file_path = raw.get("file_path")
        if not isinstance(file_path, str) or not file_path.strip():
            raise WorkGraphCompilerError(f"finding {finding_id!r} missing file_path")
        severity = raw.get("severity")
        if not isinstance(severity, str) or not severity.strip():
            raise WorkGraphCompilerError(f"finding {finding_id!r} missing severity")
        description = raw.get("description")
        summary = description if isinstance(description, str) else ""
        items.append(
            {
                "id": str(finding_id),
                "kind": "finding",
                "file_paths": [file_path],
                "severity": severity.strip(),
                "summary": summary,
            }
        )
    return items


def _item_roots(item: dict[str, Any]) -> list[str]:
    roots: list[str] = []
    file_paths = item.get("file_paths") or []
    if not isinstance(file_paths, list):
        raise WorkGraphCompilerError(f"work item {item.get('id')!r} file_paths must be a list")
    for raw in file_paths:
        if not isinstance(raw, str) or not raw.strip():
            raise WorkGraphCompilerError(f"work item {item.get('id')!r} has empty file_path")
        try:
            roots.append(_normalize_owned_path(raw))
        except ValueError as exc:
            raise WorkGraphCompilerError(f"work item {item.get('id')!r} file_path refused: {exc}") from exc
    # GRPH-03: empty roots → implement lane with owned_paths=[] skips scope
    # enforcement. Only compiler-emitted __verify__ twins may have empty owned_paths.
    if not roots:
        raise WorkGraphCompilerError(
            f"work item {item.get('id')!r} must declare non-empty file_paths "
            f"(empty owned_paths skips lane scope enforcement; only verify twins "
            f"may own the empty set)"
        )
    return roots


def _refuse_reserved_ids(work_items: list[dict[str, Any]]) -> None:
    for item in work_items:
        item_id = str(item.get("id") or "")
        if VERIFY_NAMESPACE in item_id:
            raise WorkGraphCompilerError(f"work-item id {item_id!r} contains reserved namespace {VERIFY_NAMESPACE!r}")


def _validate_work_items(work_items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for item in work_items:
        if not isinstance(item, dict):
            raise WorkGraphCompilerError("each work item must be an object")
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id.strip():
            raise WorkGraphCompilerError("work item missing non-empty id")
        if item_id in by_id:
            raise WorkGraphCompilerError(f"duplicate work-item id {item_id!r}")
        kind = item.get("kind")
        if kind not in ("finding", "operator"):
            raise WorkGraphCompilerError(f"work item {item_id!r} kind must be 'finding' or 'operator'")
        if kind == "finding":
            for forbidden in ("depends_on", "produces", "consumes", "fuse_with"):
                if forbidden in item and item.get(forbidden) not in (None, [], ()):
                    raise WorkGraphCompilerError(
                        f"finding work item {item_id!r} must not carry operator field {forbidden!r}"
                    )
        by_id[item_id] = item
        _item_roots(item)  # validate early
    return by_id


# ---------------------------------------------------------------------------
# Merge / fuse / refuse
# ---------------------------------------------------------------------------


def _build_union(by_id: dict[str, dict[str, Any]]) -> _UnionFind:
    ids = sorted(by_id)
    uf = _UnionFind(ids)

    # Exact-equal normalized root edges.
    root_to_ids: dict[str, list[str]] = defaultdict(list)
    for item_id in ids:
        for root in _item_roots(by_id[item_id]):
            root_to_ids[root].append(item_id)
    for _root, members in sorted(root_to_ids.items(), key=lambda kv: kv[0]):
        members_sorted = sorted(set(members))
        if len(members_sorted) < 2:
            continue
        anchor = members_sorted[0]
        for other in members_sorted[1:]:
            uf.union(anchor, other)

    # Operator fuse_with edges.
    for item_id in ids:
        item = by_id[item_id]
        if item.get("kind") != "operator":
            continue
        fuse = item.get("fuse_with") or []
        if not isinstance(fuse, list):
            raise WorkGraphCompilerError(f"operator {item_id!r} fuse_with must be a list")
        for partner in fuse:
            if not isinstance(partner, str) or partner not in by_id:
                raise WorkGraphCompilerError(f"operator {item_id!r} fuse_with references unknown id {partner!r}")
            uf.union(item_id, partner)

    return uf


def _refuse_parent_child_collisions(
    by_id: dict[str, dict[str, Any]],
    uf: _UnionFind,
    canon: dict[str, str],
    depends_on: dict[str, list[str]],
    *,
    allow_conflict_waves: bool = False,
) -> list[tuple[str, str]]:
    """Detect parent/child owned-path collisions between incomparable lanes.

    When *allow_conflict_waves* is False (default), raises exactly as pre-S2.
    When True, returns sorted undirected pairs of **post-union work-lane ids**
    (empty list when none). Exact-equal roots still union earlier and never
    appear here (``lr == rr`` is skipped — those are fuse material).
    """
    pairs: set[tuple[str, str]] = set()
    ids = sorted(by_id)
    for i, left_id in enumerate(ids):
        for right_id in ids[i + 1 :]:
            if uf.find(left_id) == uf.find(right_id):
                continue
            # A parent/child overlap is legal when the two lanes are depends_on-
            # comparable — validate_manifest accepts it, and it is remedy (3) in the
            # menu below. Only refuse a not-fused AND depends_on-incomparable overlap.
            if _lanes_comparable(depends_on, canon[left_id], canon[right_id]):
                continue
            left_roots = _item_roots(by_id[left_id])
            right_roots = _item_roots(by_id[right_id])
            for lr in left_roots:
                for rr in right_roots:
                    if lr == rr:
                        continue
                    if _owned_path_roots_overlap(lr, rr):
                        if not allow_conflict_waves:
                            raise WorkGraphCompilerError(
                                f"parent/child owned-path collision between {left_id!r} ({lr!r}) "
                                f"and {right_id!r} ({rr!r}) without fuse_with. {_REMEDY_MENU}"
                            )
                        a, b = sorted((canon[left_id], canon[right_id]))
                        if a != b:
                            pairs.add((a, b))
    return sorted(pairs)


def partition_conflicts(
    work_lane_ids: list[str],
    pairs: list[tuple[str, str]],
    *,
    max_conflict_degree: int | None = None,
) -> tuple[list[list[str]], dict[str, int], dict[str, Any]]:
    """Colour the conflict graph and return edges, colours, diagnostics.

    * ``conflict_edges`` — sorted, deduped undirected pairs (enforcement authority).
    * ``colour_classes`` — Welsh–Powell certificate only ([GRPH-09]); never used
      to emit ``depends_on``.
    * diagnostics: ``n_classes`` (greedy upper bound), ``clique_lower_bound``
      (constructive greedy clique from the max-degree vertex neighbourhood —
      makespan floor), ``density``, ``max_degree``.

    Degeneracy guard: a lane whose degree exceeds *max_conflict_degree* refuses
    with the narrow-your-paths remedy. When *max_conflict_degree* is None the
    guard is skipped (CLI resolves ``WORKBAY_COMPILER_MAX_CONFLICT_DEGREE``).
    """
    lane_ids = sorted({str(lid) for lid in work_lane_ids if str(lid)})
    adj: dict[str, set[str]] = {lid: set() for lid in lane_ids}
    edge_set: set[tuple[str, str]] = set()
    for pair in pairs:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = str(pair[0]), str(pair[1])
        if not a or not b or a == b:
            continue
        if a not in adj:
            adj[a] = set()
        if b not in adj:
            adj[b] = set()
        adj[a].add(b)
        adj[b].add(a)
        edge_set.add(tuple(sorted((a, b))))  # type: ignore[arg-type]

    conflict_edges = [[a, b] for a, b in sorted(edge_set)]

    degrees = {lid: len(adj.get(lid, ())) for lid in adj}
    max_degree = max(degrees.values()) if degrees else 0

    if max_conflict_degree is not None:
        if max_conflict_degree < 0:
            raise WorkGraphCompilerError("max_conflict_degree must be >= 0 when set")
        for lid in sorted(degrees):
            if degrees[lid] > max_conflict_degree:
                raise WorkGraphCompilerError(
                    f"lane {lid!r} conflicts with {degrees[lid]} others "
                    f"(max_conflict_degree={max_conflict_degree}); {_NARROW_PATHS_REMEDY}"
                )

    # Welsh–Powell: order by descending degree, tie lane_id ascending; first-fit.
    order = sorted(adj, key=lambda lid: (-degrees[lid], lid))
    colour_classes: dict[str, int] = {}
    for lid in order:
        forbidden = {colour_classes[n] for n in adj[lid] if n in colour_classes}
        colour = 0
        while colour in forbidden:
            colour += 1
        colour_classes[lid] = colour

    n = len(adj)
    m = len(edge_set)
    density = (2.0 * m / (n * (n - 1))) if n >= 2 else 0.0
    n_classes = len(set(colour_classes.values())) if colour_classes else 0
    clique_lower_bound = _constructive_clique_lower_bound(adj)

    diagnostics: dict[str, Any] = {
        "n_classes": n_classes,
        "clique_lower_bound": clique_lower_bound,
        "density": density,
        "max_degree": max_degree,
    }
    return conflict_edges, colour_classes, diagnostics


def _constructive_clique_lower_bound(adj: dict[str, set[str]]) -> int:
    """Greedy clique starting from the max-degree vertex (makespan floor)."""
    if not adj:
        return 0
    # Max degree, then ascending lane_id for determinism.
    start = min(adj, key=lambda v: (-len(adj[v]), v))
    clique: list[str] = [start]
    for candidate in sorted(adj[start]):
        if all(candidate in adj[member] for member in clique):
            clique.append(candidate)
    return len(clique)


def merge_preserved_lane_keys(
    old: dict[str, Any],
    new: dict[str, Any],
    keys: tuple[str, ...] = ("base_sha",),
) -> dict[str, Any]:
    """Copy selected per-lane keys from *old* into *new* when the lane still exists.

    Used by the compiler CLI recompile path to preserve provisioned ``base_sha``
    pins across a fresh compile of the same task_ref.
    """
    old_lanes = old.get("lanes") if isinstance(old, dict) else None
    new_lanes = new.get("lanes") if isinstance(new, dict) else None
    if not isinstance(old_lanes, dict) or not isinstance(new_lanes, dict):
        return new
    for lane_id, new_lane in new_lanes.items():
        if not isinstance(new_lane, dict):
            continue
        old_lane = old_lanes.get(lane_id)
        if not isinstance(old_lane, dict):
            continue
        for key in keys:
            if key in old_lane and key not in new_lane:
                new_lane[key] = old_lane[key]
    return new


def _enforce_max_lane_files(
    by_id: dict[str, dict[str, Any]],
    components: dict[str, list[str]],
    *,
    max_files: int | None,
) -> None:
    if max_files is None:
        return
    if max_files < 1:
        raise WorkGraphCompilerError("WORKBAY_COMPILER_MAX_LANE_FILES must be >= 1 when set")
    for members in components.values():
        # Count the roots that actually BECOME owned_paths (via _lane_owned_paths,
        # which drops the whole-tree '' sentinel when other roots exist) — not raw
        # file_paths, and not a naive root set that would over-count a mixed
        # ''+real-root lane against its emitted owned_paths length.
        total = len(_lane_owned_paths(by_id, members))
        if total > max_files:
            canon = min(members)
            raise WorkGraphCompilerError(
                f"lane {canon!r} owns {total} paths exceeding WORKBAY_COMPILER_MAX_LANE_FILES={max_files}"
            )


def _canonical_map(components: dict[str, list[str]]) -> dict[str, str]:
    """Map every member id → lexicographically smallest member (lane id)."""
    mapping: dict[str, str] = {}
    for members in components.values():
        canon = min(members)
        for m in members:
            mapping[m] = canon
    return mapping


# ---------------------------------------------------------------------------
# Edge derivation + lane emission
# ---------------------------------------------------------------------------


def _default_branch(task_ref: str, lane_id: str) -> str:
    return f"codex/{task_ref}-{lane_id}"


def _default_worktree(task_ref: str, lane_id: str) -> str:
    return f"{{orchestrator_root}}-{task_ref}-{lane_id}"


def _derive_edges_and_state(
    by_id: dict[str, dict[str, Any]],
    canon: dict[str, str],
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
    """Return (depends_on, state_writes, state_reads) for work lanes only."""
    depends_on: dict[str, list[str]] = defaultdict(list)
    state_writes: dict[str, list[str]] = defaultdict(list)
    state_reads: dict[str, list[str]] = defaultdict(list)

    def _add_dep(consumer: str, producer: str) -> None:
        if consumer == producer:
            return
        bucket = depends_on[consumer]
        if producer not in bucket:
            bucket.append(producer)

    def _add_key(bucket: dict[str, list[str]], lane: str, key: str) -> None:
        if key not in bucket[lane]:
            bucket[lane].append(key)

    # (a) operator depends_on, rewritten through union representative.
    for item_id in sorted(by_id):
        item = by_id[item_id]
        if item.get("kind") != "operator":
            continue
        raw_deps = item.get("depends_on") or []
        if not isinstance(raw_deps, list):
            raise WorkGraphCompilerError(f"operator {item_id!r} depends_on must be a list")
        consumer = canon[item_id]
        for dep in raw_deps:
            if not isinstance(dep, str) or dep not in canon:
                raise WorkGraphCompilerError(f"operator {item_id!r} depends_on references unknown id {dep!r}")
            _add_dep(consumer, canon[dep])

    # (b) produces / consumes → state keys + consumer→producer depends_on.
    producers_of: dict[str, list[str]] = defaultdict(list)
    for item_id in sorted(by_id):
        item = by_id[item_id]
        if item.get("kind") != "operator":
            continue
        produces = item.get("produces") or []
        if not isinstance(produces, list):
            raise WorkGraphCompilerError(f"operator {item_id!r} produces must be a list")
        lane = canon[item_id]
        for key in produces:
            if not isinstance(key, str) or not key.strip():
                raise WorkGraphCompilerError(f"operator {item_id!r} produces entries must be non-empty strings")
            _add_key(state_writes, lane, key)
            if lane not in producers_of[key]:
                producers_of[key].append(lane)

    for item_id in sorted(by_id):
        item = by_id[item_id]
        if item.get("kind") != "operator":
            continue
        consumes = item.get("consumes") or []
        if not isinstance(consumes, list):
            raise WorkGraphCompilerError(f"operator {item_id!r} consumes must be a list")
        lane = canon[item_id]
        for key in consumes:
            if not isinstance(key, str) or not key.strip():
                raise WorkGraphCompilerError(f"operator {item_id!r} consumes entries must be non-empty strings")
            _add_key(state_reads, lane, key)
            for producer in producers_of.get(key, []):
                _add_dep(lane, producer)

    # Deterministic sort of every prereq list.
    depends_sorted = {k: sorted(v) for k, v in sorted(depends_on.items()) if v}
    writes_sorted = {k: sorted(v) for k, v in sorted(state_writes.items()) if v}
    reads_sorted = {k: sorted(v) for k, v in sorted(state_reads.items()) if v}
    return depends_sorted, writes_sorted, reads_sorted


def _lane_owned_paths(by_id: dict[str, dict[str, Any]], members: list[str]) -> list[str]:
    roots: set[str] = set()
    for mid in members:
        for root in _item_roots(by_id[mid]):
            roots.add(root)
    # Never emit the whole-tree sentinel for a work lane (overlaps every peer).
    if "" in roots and len(roots) > 1:
        roots.discard("")
    return sorted(roots)


def _lane_summary(by_id: dict[str, dict[str, Any]], members: list[str]) -> str:
    parts = []
    for mid in sorted(members):
        summary = by_id[mid].get("summary")
        if isinstance(summary, str) and summary.strip():
            parts.append(summary.strip())
    return "; ".join(parts) if parts else f"work lane {min(members)}"


def _topo_merge_order(lane_ids: list[str], depends_on: dict[str, list[str]]) -> list[str]:
    """Deterministic topo order: prereqs before dependents; ties by lane_id."""
    id_set = set(lane_ids)
    indegree: dict[str, int] = {lid: 0 for lid in lane_ids}
    children: dict[str, list[str]] = defaultdict(list)
    for consumer, prereqs in depends_on.items():
        if consumer not in id_set:
            continue
        for prereq in prereqs:
            if prereq not in id_set:
                continue
            children[prereq].append(consumer)
            indegree[consumer] = indegree.get(consumer, 0) + 1
    for kids in children.values():
        kids.sort()

    ready = sorted(lid for lid, deg in indegree.items() if deg == 0)
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in children.get(node, []):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort()
    if len(order) != len(lane_ids):
        # Cycle — still emit a deterministic residual order; validate_manifest will refuse.
        remaining = sorted(lid for lid in lane_ids if lid not in order)
        order.extend(remaining)
    return order


def _downstream_from_depends_on(lane_ids: list[str], depends_on: dict[str, list[str]]) -> dict[str, list[str]]:
    """Reverse of ``depends_on``: ``downstream[u]`` = lanes that declare ``u`` as a prereq.

    Merge-order *position* does NOT imply a dependency; only ``depends_on`` does. A
    merge-order suffix would falsely list independent parallel lanes as each other's
    dependents, and the daemon's landing-recovery (``downstream_lanes`` →
    ``manifest['downstream'][lane]``) would then refresh/rebase unrelated lanes.
    """
    downstream: dict[str, list[str]] = {lid: [] for lid in lane_ids}
    for consumer in sorted(depends_on):
        if consumer not in downstream:
            continue
        for prereq in depends_on[consumer]:
            if prereq in downstream and consumer not in downstream[prereq]:
                downstream[prereq].append(consumer)
    return {lid: sorted(deps) for lid, deps in downstream.items()}


def _depends_reaches(depends_on: dict[str, list[str]], src: str, dst: str) -> bool:
    """True when ``src`` transitively depends on ``dst`` (``dst`` is an ancestor of ``src``)."""
    if src == dst:
        return True
    seen = {src}
    stack = list(depends_on.get(src, []))
    while stack:
        node = stack.pop()
        if node == dst:
            return True
        if node in seen:
            continue
        seen.add(node)
        stack.extend(depends_on.get(node, []))
    return False


def _lanes_comparable(depends_on: dict[str, list[str]], a: str, b: str) -> bool:
    """Mirror ``lane_manifest._depends_on_reaches``: comparable = one reaches the other."""
    return a == b or _depends_reaches(depends_on, a, b) or _depends_reaches(depends_on, b, a)


def _is_verify_twin(lane_id: str) -> bool:
    return VERIFY_NAMESPACE in lane_id


def _severity_rank(severity: str | None) -> int:
    """Map a work-item severity string to allocation order (WIDTH-101).

    critical < high < medium < low. Missing / unknown values sort as low so
    undeclared severity never outranks an explicit higher-severity lane.
    """
    if not isinstance(severity, str) or not severity.strip():
        return _SEVERITY_RANK["low"]
    return _SEVERITY_RANK.get(severity.strip().lower(), _SEVERITY_RANK["low"])


def _worst_severity(severities: list[str | None]) -> str:
    """Highest-need severity among members (for fused lanes).

    Always returns a canonical value from ``{critical, high, medium, low}``.
    Out-of-vocabulary strings rank as low for allocation (via ``_severity_rank``)
    and must not leak raw into ``lane_severities`` (WIDTH-116).
    """
    if not severities:
        return "low"
    best = min(severities, key=_severity_rank)
    if isinstance(best, str) and best.strip():
        key = best.strip().lower()
        if key in _SEVERITY_RANK:
            return key
        return "low"
    return "low"


def _backend_is_off_box(spec: BackendSpec) -> bool:
    """True when a backend is declared off-box (capability or COST_REMOTE)."""
    return bool(spec.capabilities.dispatchable_off_box) or spec.cost_class == COST_REMOTE


def _is_eligible_local_reviewer(spec: BackendSpec) -> bool:
    """Local reviewer eligibility: kind==cli and cost_class==COST_REMOTE_API.

    Derived from declared fields only (REF-24) — never a backend-name check.
    Against the live registry this admits claude-code / codex-cli / cursor-cli
    / grok-cli and excludes bridge, api, in-process, and COST_HEAVY backends.
    """
    return spec.kind == "cli" and spec.cost_class == COST_REMOTE_API


def _rank_key(spec: BackendSpec, name: str) -> tuple[int, int, str]:
    """Sort key for reviewer selection: off-box first, then rank, then name.

    The name element makes equal-rank ties explicit and stable (WIDTH-47);
    registry declaration order is never the silent fallback.
    """
    return (0 if _backend_is_off_box(spec) else 1, int(spec.review_rank), name)


def select_review_backends(
    impl_backend: str,
    *,
    backends: dict[str, BackendSpec] | None = None,
) -> tuple[str, ...]:
    """Select complementary verify-twin backends for an implementer.

    Returns one representative per model family that DIFFERS from the
    implementer's family, preferring off-box backends within and across
    families, then by declared ``review_rank`` (lower preferred). Reads the
    passed-in ``backends`` mapping so a registry addition becomes selectable
    without editing this function (internal).

    Reviewer eligibility is declared per registry row (``BackendSpec.review_eligible``).
    A future backend may be registered as an implement transport without
    silently becoming a reviewer; only rows with ``review_eligible=True``
    compete for a model-family slot.

    Never falls back to a same-family twin — when no complementary off-box
    reviewer exists the off-box subset is empty (honest gap; no self-review).

    A non-empty ``impl_backend`` absent from the registry is an unknown name:
    return no candidates rather than fail open with ``impl_family=None``
    (WIDTH-43). Undeclared implementers pass ``""`` and get the full
    complementary set (family filter inactive because no family is known).
    """
    registry = BACKENDS if backends is None else backends
    # Unknown non-empty name: refuse to select (do not defeat the family filter).
    if impl_backend and impl_backend not in registry:
        return ()
    impl_spec = registry.get(impl_backend)
    impl_family = impl_spec.model_family if impl_spec is not None else None

    by_family: dict[str, list[str]] = {}
    for name, spec in registry.items():
        if name == impl_backend:
            continue
        # Declared per-row: implement-only transports must not capture a
        # model-family reviewer slot over real local reviewers (implementation note).
        if not spec.review_eligible:
            continue
        fam = spec.model_family
        if not fam:
            continue
        if impl_family is not None and fam == impl_family:
            continue
        by_family.setdefault(fam, []).append(name)

    def _rank(name: str) -> tuple[int, int, str]:
        return _rank_key(registry[name], name)

    chosen: list[str] = []
    for fam in sorted(by_family):
        reps = sorted(by_family[fam], key=_rank)
        chosen.append(reps[0])
    chosen.sort(key=_rank)
    return tuple(chosen)


def _reachable_local_reviewer_names(
    *,
    backends: dict[str, BackendSpec] | None = None,
) -> frozenset[str]:
    """Local reviewers selection can actually yield (WIDTH-65).

    Intersection of (1) names returned by ``select_review_backends`` for some
    implementer and (2) ``_is_eligible_local_reviewer``. Family representation
    prefers off-box, so e.g. ``grok-cli`` is eligible but never selected while
    ``grok-remote`` owns the grok family.
    """
    registry = BACKENDS if backends is None else backends
    reachable: set[str] = set()
    for impl in list(registry) + [""]:
        for name in select_review_backends(impl, backends=registry):
            spec = registry.get(name)
            if spec is None or _backend_is_off_box(spec):
                continue
            if not _is_eligible_local_reviewer(spec):
                continue
            reachable.add(name)
    return frozenset(reachable)


# Compile-wide total = reachable local reviewers, not mere eligibility (WIDTH-65).
_MAX_LOCAL_REVIEWERS_TOTAL = len(_reachable_local_reviewer_names())


def _emit_verify_twins(
    *,
    work_lane_ids: list[str],
    lanes: dict[str, dict[str, Any]],
    depends_on: dict[str, list[str]],
    task_ref: str,
    host_memory_policy: HostMemoryPolicy | None = None,
    lane_severities: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Add twin lanes + diamond depends_on; return (provisioning, blockers, diagnostics).

    Reviewer backends are selected by declared model_family complementarity
    (``select_review_backends``), not a hardcoded name tuple. Locally-gated
    reviewers are capped per backend across the compile (mirrors
    HostMemoryPolicy.per_backend_local_cap) AND by a compile-wide total
    (``_MAX_LOCAL_REVIEWERS_TOTAL``); at most one local reviewer is minted per
    implement lane; off-box reviewers may scale. Undeclared implementers surface
    a non-gating diagnostic (complementarity unverifiable) rather than a
    completion blocker. Unknown preferred_backend names are named in diagnostics
    AND held by a blocker (WIDTH-62) — they do not fail open. Implement lanes
    that receive no reviewer are named in diagnostics AND held by a namespaced
    blocker (WIDTH-92) — a diagnostic alone gates nothing. Local pick among
    candidates is least-loaded-first with ``review_rank`` as tie-break
    (WIDTH-64). Bounded local slots are allocated severity-first (WIDTH-101):
    critical > high > medium > low, then lane_id for a stable tie-break.
    A ``per_backend_local_cap <= 0`` disables the per-backend bound the way
    admission does (WIDTH-63); the compile-wide total still binds.
    """
    provisioning: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    twin_ids: list[str] = []
    twins_by_parent: dict[str, list[str]] = defaultdict(list)
    # Per-backend emission counts — not a compile-global local pool (WIDTH-27).
    local_emitted_by_backend: dict[str, int] = defaultdict(int)
    local_emitted_total = 0
    per_backend_cap = (
        int(host_memory_policy.per_backend_local_cap)
        if host_memory_policy is not None
        else _MAX_LOCAL_REVIEWERS_PER_BACKEND
    )
    total_cap = _MAX_LOCAL_REVIEWERS_TOTAL
    severities = lane_severities or {}
    # WIDTH-101: spend bounded local slots on highest-severity work first.
    # Lane-id is only the deterministic tie-break — never the primary key.
    allocation_order = sorted(
        work_lane_ids,
        key=lambda lid: (_severity_rank(severities.get(lid)), lid),
    )

    for lane_id in allocation_order:
        parent = lanes[lane_id]
        parent_branch = str(parent["branch"])
        impl_backend = parent.get("preferred_backend")
        # Declaration only affects WHICH reviewer is complementary; local host
        # capacity is tracked per backend and compile-wide (internal).
        declared = bool(impl_backend)
        if not declared:
            diagnostics.append(
                f"implement lane {lane_id}: implementer undeclared; reviewer complementarity unverifiable"
            )
            candidates = select_review_backends("")
        else:
            impl_name = str(impl_backend)
            if impl_name not in BACKENDS:
                # WIDTH-43: name the unknown backend; do not fail open.
                # WIDTH-62: also hold the lane — a diagnostic alone gates nothing.
                diagnostics.append(
                    f"implement lane {lane_id}: unknown preferred_backend {impl_name!r}; "
                    "reviewer complementarity refused (backend absent from registry)"
                )
                # Hold via a namespaced key (WIDTH-76): twin_lane_id is the
                # emit_blockers dedup key. Bare lane_id collides with the lane's
                # own blockers and gets swallowed as already_open; a key in the
                # verify namespace cannot. lane_id stays on the descriptor so a
                # future resolver can map the hold back to the implement work.
                hold_id = f"{lane_id}{VERIFY_NAMESPACE}unknown_backend"
                blockers.append(
                    {
                        "twin_lane_id": hold_id,
                        "lane_id": lane_id,
                        "description": (
                            f"implement lane {lane_id}: unknown preferred_backend "
                            f"{impl_name!r}; holding until a registered backend is declared"
                        ),
                    }
                )
                candidates = select_review_backends(impl_name)
            else:
                candidates = select_review_backends(impl_name)

        # WIDTH-64: among eligible local candidates under caps, least-loaded
        # first; review_rank (via _rank_key) is the tie-break only.
        chosen_local: str | None = None
        if local_emitted_total < total_cap:
            local_pool: list[str] = []
            for backend in candidates:
                spec = BACKENDS.get(backend)
                if spec is None or not spec.model_family:
                    continue
                if spec.cost_class == COST_REMOTE:
                    continue
                if not _is_eligible_local_reviewer(spec):
                    continue
                # Admission: per_backend_local_cap > 0 is required to enforce;
                # <= 0 means the per-backend bound is off (WIDTH-63).
                if per_backend_cap > 0 and local_emitted_by_backend[backend] >= per_backend_cap:
                    continue
                local_pool.append(backend)
            if local_pool:
                chosen_local = min(
                    local_pool,
                    key=lambda name: (
                        local_emitted_by_backend[name],
                        _rank_key(BACKENDS[name], name),
                    ),
                )

        for backend in candidates:
            spec = BACKENDS.get(backend)
            if spec is None or not spec.model_family:
                continue
            is_local = spec.cost_class != COST_REMOTE
            if is_local:
                if backend != chosen_local:
                    continue
            suffix = spec.model_family
            twin_id = f"{lane_id}{VERIFY_NAMESPACE}{suffix}"
            if twin_id in lanes:
                raise WorkGraphCompilerError(f"twin lane id {twin_id!r} collides with an existing lane")
            if twin_id in twin_ids:
                raise WorkGraphCompilerError(f"duplicate twin lane id {twin_id!r}")
            twin_ids.append(twin_id)
            twins_by_parent[lane_id].append(twin_id)
            if is_local:
                local_emitted_by_backend[backend] += 1
                local_emitted_total += 1
            branch = f"{parent_branch}-verify-{suffix}"
            worktree_path = _default_worktree(task_ref, twin_id)
            lanes[twin_id] = {
                "branch": branch,
                "worktree_path": worktree_path,
                "owned_paths": [],
                "test_commands": [],
                "preferred_backend": backend,
                "title": f"Verify {lane_id} ({suffix})",
                "objective": f"Adversarial verify-twin for implement lane {lane_id}.",
            }
            depends_on[twin_id] = [lane_id]
            provisioning.append(
                {
                    "lane_id": twin_id,
                    "lane_kind": "review",
                    "preferred_backend": backend,
                    "worktree_path": worktree_path,
                    "branch": branch,
                }
            )
            blockers.append(
                {
                    "twin_lane_id": twin_id,
                    "description": f"verify twin {twin_id} pending for {lane_id}",
                }
            )

    # Bounded coverage: name each implement lane that received no reviewer
    # AND hold it (WIDTH-92). A diagnostic alone gates nothing (WIDTH-62).
    # Key discipline matches the unknown-backend hold: lane-derived AND inside
    # VERIFY_NAMESPACE so emit_admit_time_blockers cannot swallow it as
    # already_open against an unrelated bare-lane blocker (WIDTH-76/78/103).
    # Lanes that already have a verify twin must not be held (anti-overreach).
    #
    # WIDTH-114: a hold whose twin id is absent from merge_order and from
    # registrable twin_provisioning is a wedge — the resolver can never see
    # it. Shape: one shared, dispatchable ``lane_kind=review`` twin per wave
    # (id contains ``__verify__`` so both resolver sources match). Every
    # uncovered lane's blocker names that single id; when the twin reports
    # ``finished``, all those holds close together. O(1) registrable growth
    # (pin 4) — not one twin per uncovered lane. The shared twin is a real
    # lane (in ``lanes`` → merge_order) and a real provisioning entry
    # (``provision_twins`` registers it → worktree list).
    #
    # WIDTH-115: match already-held by exact ``lane_id`` on the descriptor —
    # never prefix-match twin_lane_id (a neighbouring ``a__verify`` twin would
    # suppress lane ``a``). Twin-pending blockers omit ``lane_id``; treat that
    # as not held.
    uncovered_for_hold: list[str] = []
    for lane_id in sorted(work_lane_ids):
        if not twins_by_parent.get(lane_id):
            diagnostics.append(
                f"implement lane {lane_id}: no reviewer assigned "
                "(bounded local reviewer coverage; off-box reviewers unavailable)"
            )
            # Skip a second hold when the unknown-backend path already held
            # this lane — one namespaced gate is enough; still emit the
            # diagnostic above so silence never masks a gap.
            already_held = any(str(b.get("lane_id") or "") == lane_id for b in blockers)
            if already_held:
                continue
            uncovered_for_hold.append(lane_id)

    if uncovered_for_hold:
        # Stable, collision-free id outside any work-lane prefix so coverage
        # helpers (split/startswith on VERIFY_NAMESPACE) do not attribute this
        # twin to a specific implement lane. Reason token remains no_reviewer
        # so WIDTH-115's held filter still matches.
        shared_hold_id = _SHARED_WAVE_NO_REVIEWER_GATE_ID
        if shared_hold_id in lanes or shared_hold_id in twin_ids:
            raise WorkGraphCompilerError(
                f"shared no_reviewer twin id {shared_hold_id!r} collides with an existing lane"
            )
        # Prefer a remote/off-box backend so the shared gate does not consume a
        # bounded local reviewer slot; fall back to any registered backend.
        shared_backend = next(
            (name for name, spec in BACKENDS.items() if spec.cost_class == COST_REMOTE),
            next(iter(BACKENDS), ""),
        )
        if not shared_backend:
            raise WorkGraphCompilerError("no backend available for shared no_reviewer twin")
        shared_branch = f"{_default_branch(task_ref, _WAVE_UNCOVERED_SENTINEL)}-verify-no_reviewer"
        shared_worktree = _default_worktree(task_ref, shared_hold_id)
        twin_ids.append(shared_hold_id)
        lanes[shared_hold_id] = {
            "branch": shared_branch,
            "worktree_path": shared_worktree,
            "owned_paths": [],
            "test_commands": [],
            "preferred_backend": shared_backend,
            "title": "Verify uncovered lanes (shared no_reviewer gate)",
            "objective": (
                "Shared adversarial verify-twin for implement lanes that received "
                "no per-lane reviewer under the local reviewer bound."
            ),
        }
        # Do NOT list uncoverd implement lanes as depends_on parents: helpers
        # that attribute reviewers via depends_on would treat this shared gate
        # as each lane's per-lane twin (self-review / "ungated" probes). The
        # gate is wave-scoped; readiness is enforced by the blockers, not by
        # the diamond parent edge.
        depends_on[shared_hold_id] = []
        provisioning.append(
            {
                "lane_id": shared_hold_id,
                "lane_kind": "review",
                "preferred_backend": shared_backend,
                "worktree_path": shared_worktree,
                "branch": shared_branch,
            }
        )
        for lane_id in uncovered_for_hold:
            # WIDTH-78: twin_lane_id must be per-lane and inside VERIFY_NAMESPACE
            # so emit_admit_time_blockers does not collapse N holds onto one row
            # and so the key cannot collide with a bare-lane blocker.
            #
            # WIDTH-114: every hold must name an id the resolver can see. The
            # shared twin is the single dispatchable review worker (O(1)
            # registrable). Hold markers stay OUT of ``lanes`` so coverage
            # probes (startswith lane+VERIFY_NAMESPACE) still treat the
            # implement lane as uncovered / held rather than "has a twin".
            # ``compile_work_manifest`` appends the hold ids to merge_order
            # after validate so resolver source 1 can see them; emit rewrites
            # actor.lane_id to the shared twin so finishing it closes every
            # hold together.
            hold_id = f"{lane_id}{VERIFY_NAMESPACE}no_reviewer"
            if hold_id in lanes or hold_id in twin_ids:
                raise WorkGraphCompilerError(f"no_reviewer hold id {hold_id!r} collides with an existing lane")
            blockers.append(
                {
                    "twin_lane_id": hold_id,
                    "lane_id": lane_id,
                    "resolve_as_twin": shared_hold_id,
                    "description": (
                        f"implement lane {lane_id}: no reviewer assigned "
                        "(bounded local reviewer coverage; off-box reviewers unavailable); "
                        f"held on shared twin {shared_hold_id}"
                    ),
                }
            )

    # Uniqueness assert before validate_manifest.
    if len(twin_ids) != len(set(twin_ids)):
        raise WorkGraphCompilerError("emitted twin ids are not unique")
    for tid in twin_ids:
        if twin_ids.count(tid) != 1:
            raise WorkGraphCompilerError(f"emitted twin id {tid!r} is not unique")

    # Route downstream implement dependents through twins (diamond, acyclic).
    # Do not augment verify-twin dependents (would self-edge / twin-twin).
    for consumer in sorted(depends_on):
        if _is_verify_twin(consumer):
            continue
        prereqs = list(depends_on[consumer])
        extras: list[str] = []
        for prereq in prereqs:
            if prereq in work_lane_ids:
                for twin_id in twins_by_parent.get(prereq, []):
                    if twin_id not in prereqs and twin_id not in extras:
                        extras.append(twin_id)
        if extras:
            depends_on[consumer] = sorted(set(prereqs) | set(extras))

    # Sort every depends_on list deterministically.
    for key in list(depends_on):
        depends_on[key] = sorted(depends_on[key])

    provisioning.sort(key=lambda d: d["lane_id"])
    blockers.sort(key=lambda d: d["twin_lane_id"])
    return provisioning, blockers, diagnostics


# ---------------------------------------------------------------------------
# Public pure core
# ---------------------------------------------------------------------------


def holds_resolved_by_twin(twin_lane_id: str, blocker_keys: Iterable[str]) -> list[str]:
    """Return the blocker keys that finishing *twin_lane_id* closes.

    Pure binding shared by the compiler pins and the daemon resolver — no I/O,
    no handoff calls, no module state. Binding is derived from id shape only
    (never from a descriptor field) so the two sides cannot drift:

    - A twin always closes its **own** row when that key is present.
    - The **shared wave gate** (exact id ``_SHARED_WAVE_NO_REVIEWER_GATE_ID``,
      built once from ``_WAVE_UNCOVERED_SENTINEL``) additionally closes every
      per-lane ``no_reviewer`` hold in *blocker_keys*. One finished gate stands
      in for all uncovered lanes. Match is exact — not a prefix — so a hold
      whose id merely starts with the sentinel never inherits gate authority.
    - A **per-lane** twin (``lane00__verify__codex``) closes only its own row.
      Closing unrelated ``no_reviewer`` holds would rubber-stamp lanes it never
      reviewed.
    - Unknown / unrelated ids close nothing but themselves.
    """
    twin = str(twin_lane_id or "")
    keys = [str(k) for k in blocker_keys]
    closed: list[str] = []
    if twin and twin in keys:
        closed.append(twin)

    no_reviewer_suffix = f"{VERIFY_NAMESPACE}no_reviewer"
    # Exact id match against the single shared definition used at emission
    # (WIDTH-117). A prefix test would let a per-lane hold whose id merely
    # starts with the sentinel inherit the gate's authority.
    if twin == _SHARED_WAVE_NO_REVIEWER_GATE_ID:
        for key in keys:
            if key.endswith(no_reviewer_suffix) and key not in closed:
                closed.append(key)
    return closed


def compile_work_manifest(
    work_items: list[dict[str, Any]],
    *,
    task_ref: str,
    max_lane_files: int | None = None,
    emit_verify_twins: bool = True,
    allow_conflict_waves: bool = False,
    max_conflict_degree: int | None = None,
    host_memory_policy: HostMemoryPolicy | None = None,
) -> CompileResult:
    """Compile work items into a validate_manifest-clean lane manifest.

    Pure: no DB, no filesystem, no env reads. Same input + kwargs → byte-identical
    manifest JSON. Resolve ``WORKBAY_COMPILER_MAX_LANE_FILES`` and
    ``WORKBAY_COMPILER_MAX_CONFLICT_DEGREE`` at the CLI boundary only (never here).

    ``host_memory_policy`` (WIDTH-44): optional injected policy for the
    per-backend local reviewer cap. When omitted, module defaults apply so the
    pure core stays free of contract I/O.

    *allow_conflict_waves* / *max_conflict_degree* are append-only (implementation note S2).
    Default ``allow_conflict_waves=False`` is byte-identical to the pre-S2 path:
    parent/child prefix collisions still raise ``WorkGraphCompilerError``.
    """
    if not isinstance(task_ref, str) or not task_ref.strip():
        raise WorkGraphCompilerError("task_ref must be a non-empty string")
    task_ref = task_ref.strip()
    if not isinstance(work_items, list):
        raise WorkGraphCompilerError("work_items must be a list")
    if not work_items:
        raise WorkGraphCompilerError("work_items must be non-empty")

    _refuse_reserved_ids(work_items)
    by_id = _validate_work_items(work_items)

    uf = _build_union(by_id)
    components = uf.components()
    canon = _canonical_map(components)

    depends_on, state_writes, state_reads = _derive_edges_and_state(by_id, canon)
    # Parent/child owned-path overlaps between depends_on-INCOMPARABLE lanes:
    # default raises; with allow_conflict_waves collect pairs for colouring.
    # Runs AFTER union/canonicalisation and BEFORE twin emission so twins
    # (empty owned_paths) are never edge endpoints and never coloured.
    collision_pairs = _refuse_parent_child_collisions(
        by_id,
        uf,
        canon,
        depends_on,
        allow_conflict_waves=allow_conflict_waves,
    )
    _enforce_max_lane_files(by_id, components, max_files=max_lane_files)

    # Build work lanes (canonical id = min member).
    members_by_lane: dict[str, list[str]] = defaultdict(list)
    for member, lane_id in canon.items():
        members_by_lane[lane_id].append(member)
    for lane_id in members_by_lane:
        members_by_lane[lane_id] = sorted(members_by_lane[lane_id])

    work_lane_ids = sorted(members_by_lane)
    lanes: dict[str, dict[str, Any]] = {}
    # Severity from work items only (WIDTH-101); compile stays pure — no env/FS.
    lane_severities: dict[str, str] = {}
    # WIDTH-116: name out-of-vocabulary severities on the diagnostics channel.
    severity_diagnostics: list[str] = []
    for lane_id in work_lane_ids:
        members = members_by_lane[lane_id]
        owned = _lane_owned_paths(by_id, members)
        if owned == [""]:
            # Single whole-tree root — still emit (solo lane); multi-lane will fail validate.
            owned = [""]
        lane: dict[str, Any] = {
            "branch": _default_branch(task_ref, lane_id),
            "worktree_path": _default_worktree(task_ref, lane_id),
            "owned_paths": owned,
            "test_commands": [],
            "title": lane_id,
            "objective": _lane_summary(by_id, members),
        }
        # Propagate work-item preferred_backend onto the implement lane so
        # reviewer selection can be complementary (internal). Members
        # are already sorted; first declared backend wins for determinism.
        for mid in members:
            pb = by_id[mid].get("preferred_backend")
            if isinstance(pb, str) and pb.strip():
                lane["preferred_backend"] = pb.strip()
                break
        if lane_id in state_writes:
            lane["state_writes"] = list(state_writes[lane_id])
        if lane_id in state_reads:
            lane["state_reads"] = list(state_reads[lane_id])
        lanes[lane_id] = lane
        member_sevs: list[str | None] = []
        for mid in members:
            raw_sev = by_id[mid].get("severity")
            if isinstance(raw_sev, str) and raw_sev.strip():
                if raw_sev.strip().lower() not in _SEVERITY_RANK:
                    severity_diagnostics.append(f"work item {mid}: unrecognised severity {raw_sev!r}; ranked as low")
                member_sevs.append(raw_sev)
            else:
                member_sevs.append(None)
        lane_severities[lane_id] = _worst_severity(member_sevs)

    conflict_edges: list[list[str]] = []
    colour_classes: dict[str, int] = {}
    if allow_conflict_waves:
        conflict_edges, colour_classes, _diagnostics = partition_conflicts(
            work_lane_ids,
            collision_pairs,
            max_conflict_degree=max_conflict_degree,
        )

    # depends_on only over work lanes so far (mutable copy for twin augmentation).
    dep_map: dict[str, list[str]] = {k: list(v) for k, v in depends_on.items()}

    provisioning: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    diagnostics: list[str] = list(severity_diagnostics)
    if emit_verify_twins:
        twin_provisioning, twin_blockers, twin_diagnostics = _emit_verify_twins(
            work_lane_ids=work_lane_ids,
            lanes=lanes,
            depends_on=dep_map,
            task_ref=task_ref,
            host_memory_policy=host_memory_policy,
            lane_severities=lane_severities,
        )
        provisioning = twin_provisioning
        blockers = twin_blockers
        diagnostics = list(severity_diagnostics) + list(twin_diagnostics)

    all_lane_ids = sorted(lanes)
    merge_order = _topo_merge_order(all_lane_ids, dep_map)
    downstream = _downstream_from_depends_on(all_lane_ids, dep_map)

    # Drop empty depends_on entries; sort keys for stable JSON.
    depends_clean = {k: list(v) for k, v in sorted(dep_map.items()) if v}

    manifest: dict[str, Any] = {
        "task_ref": task_ref,
        "default_done_definition": DEFAULT_DONE,
        "merge_order": merge_order,
        "routing": [],
        "lanes": {lid: lanes[lid] for lid in sorted(lanes)},
        "downstream": {k: list(v) for k, v in sorted(downstream.items())},
        "depends_on": depends_clean,
    }
    # Only emit conflict keys when the opt-in flag is set — keeps the default
    # path byte-identical to pre-S2 (row 7).
    if allow_conflict_waves:
        manifest["conflict_edges"] = conflict_edges
        # Colour work lanes only (twins never coloured).
        manifest["colour_classes"] = {
            lid: colour_classes[lid] for lid in sorted(colour_classes) if lid in work_lane_ids
        }

    # Operator-input errors surface from validate_manifest as RuntimeError; re-wrap
    # as the documented escapable compiler refusal (row-4/8 cycle & state conflicts).
    try:
        validate_manifest(manifest, Path("<compiled-manifest>"))
    except RuntimeError as exc:
        raise WorkGraphCompilerError(f"compiled manifest failed validation: {exc}") from exc
    return CompileResult(
        manifest=manifest,
        twin_provisioning=provisioning,
        blockers=blockers,
        diagnostics=diagnostics,
    )


# ---------------------------------------------------------------------------
# CLI side effects: provision twins + admit-time blockers + save
# ---------------------------------------------------------------------------


def _open_blockers_for_task(task_ref: str) -> list[dict[str, Any]]:
    """Unlimited open-blocker list via handoff_close_check (not the capped state view).

    Fail-closed: invalid / not-ok / schema-invalid responses raise so admit-time
    blocker emission cannot re-INSERT duplicates after a silent empty read.
    Returns ``[]`` only when the response is well-formed with a valid (possibly
    empty) ``items`` list.
    """
    from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415

    result = handoff_api.handoff_close_check(task_ref=task_ref, enforce=False)
    if not isinstance(result, dict) or result.get("ok") is False:
        raise WorkGraphCompilerError(f"handoff_close_check failed/invalid for {task_ref!r}: {result!r}")
    data = result.get("data") if isinstance(result.get("data"), dict) else result
    checks = data.get("checks") if isinstance(data, dict) else None
    if not isinstance(checks, dict):
        raise WorkGraphCompilerError(f"handoff_close_check schema-invalid for {task_ref!r}: missing checks: {result!r}")
    open_blockers = checks.get("open_blockers")
    if not isinstance(open_blockers, dict):
        raise WorkGraphCompilerError(
            f"handoff_close_check schema-invalid for {task_ref!r}: open_blockers not a dict: {result!r}"
        )
    items = open_blockers.get("items")
    if not isinstance(items, list):
        raise WorkGraphCompilerError(
            f"handoff_close_check schema-invalid for {task_ref!r}: items not a list: {result!r}"
        )
    return [item for item in items if isinstance(item, dict)]


def emit_admit_time_blockers(
    *,
    task_ref: str,
    blockers: list[dict[str, Any]],
    workspace_root: Path | str | None = None,
    session: str = "work-graph-compiler",
) -> list[dict[str, Any]]:
    """Record an OPEN task-scoped blocker per twin (idempotent on lane_id).

    Copies the ``_record_breaker_blocker`` pattern (record_event event_kind=blocker)
    and requires ``actor.lane_id=T`` so the blockers.lane_id column is populated.
    Skips INSERT when an open blocker for ``lane_id==T`` already exists.
    """
    from workbay_handoff_mcp import api as handoff_api  # noqa: PLC0415

    if workspace_root is not None:
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        handoff_api.configure_runtime(RuntimeConfig.for_repo(Path(workspace_root)))

    existing = _open_blockers_for_task(task_ref)
    open_lane_ids = {
        str(row.get("lane_id"))
        for row in existing
        if str(row.get("status") or "").lower() == "open" and row.get("lane_id")
    }

    emitted: list[dict[str, Any]] = []
    for desc in sorted(blockers, key=lambda d: str(d.get("twin_lane_id") or "")):
        twin_id = str(desc.get("twin_lane_id") or "").strip()
        if not twin_id:
            continue
        if twin_id in open_lane_ids:
            emitted.append({"twin_lane_id": twin_id, "skipped": True, "reason": "already_open"})
            continue
        description = str(desc.get("description") or f"verify twin {twin_id} pending")
        result = handoff_api.record_event(
            event={  # type: ignore[arg-type]
                "event_kind": "blocker",
                "session": session,
                "operation": "add",
                "description": description,
                "task_ref": task_ref,
                "actor": {"lane_id": twin_id},
            }
        )
        open_lane_ids.add(twin_id)
        emitted.append({"twin_lane_id": twin_id, "skipped": False, "result_ok": bool(result.get("ok"))})
    return emitted


def provision_twins(
    *,
    task_ref: str,
    twin_provisioning: list[dict[str, Any]],
    orchestrator_root: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Upsert each twin as lane_kind=review and pin preferred_backend."""
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.offload_preflight import (  # noqa: PLC0415
        materialize_offload_lane_manifest,
    )

    root = Path(orchestrator_root).expanduser().resolve() if orchestrator_root else Path.cwd().resolve()
    results: list[dict[str, Any]] = []
    for desc in sorted(twin_provisioning, key=lambda d: str(d.get("lane_id") or "")):
        lane_id = str(desc["lane_id"])
        branch = str(desc["branch"])
        worktree_path = str(desc["worktree_path"])
        backend = str(desc["preferred_backend"])
        upsert = manage_worktree_lane(
            operation="upsert",
            lane_id=lane_id,
            worktree_path=worktree_path,
            branch=branch,
            lane_kind="review",
            task_ref=task_ref,
            backend=backend,
        )
        try:
            materialize_offload_lane_manifest(
                orchestrator_root=root,
                task_ref=task_ref,
                lane_id=lane_id,
                worktree_path=worktree_path,
                branch=branch,
                preferred_backend=backend,
            )
            pin_ok = True
            pin_error = None
        except Exception as exc:  # noqa: BLE001 — surface pin failure without aborting others
            pin_ok = False
            pin_error = str(exc)
        results.append(
            {
                "lane_id": lane_id,
                "upsert": upsert,
                "pin_ok": pin_ok,
                "pin_error": pin_error,
            }
        )
    return results


def _fetch_open_findings_via_cli() -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["mcp-workbay-handoff", "review_findings", "--operation", "list", "--status", "open"],
            check=False,
            capture_output=True,
            text=True,
            timeout=FINDINGS_CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"review_findings list timed out after {FINDINGS_CLI_TIMEOUT_SECONDS}s") from exc
    if proc.returncode != 0:
        raise SystemExit(f"review_findings list failed (exit {proc.returncode}): {proc.stderr or proc.stdout}")
    raw = proc.stdout.strip()
    if not raw:
        return {"data": {"findings": []}}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"review_findings list returned non-JSON: {exc}") from exc


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compile work items into a lane orchestration manifest.")
    parser.add_argument("--task-ref", required=True, help="Task ref for the emitted manifest.")
    parser.add_argument(
        "--operator-items",
        help="Optional JSON file of operator work items (list or {items:[...]}).",
    )
    parser.add_argument(
        "--findings-json",
        help="Optional findings envelope JSON file (skips mcp-workbay-handoff subprocess).",
    )
    parser.add_argument(
        "--manifest-dir",
        help="Directory for save_manifest (default: config/lane-orchestration under orchestrator root).",
    )
    parser.add_argument(
        "--orchestrator-root",
        default=".",
        help="Workspace root for provisioning + default manifest dir.",
    )
    parser.add_argument(
        "--skip-provision",
        action="store_true",
        help="Do not upsert twin worktree lanes / materialize pins.",
    )
    parser.add_argument(
        "--skip-blockers",
        action="store_true",
        help="Do not emit admit-time blockers for twins.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print the compiled manifest JSON to stdout.",
    )
    parser.add_argument(
        "--no-verify-twins",
        action="store_true",
        help="Compile without verify-twin emission (debug / tests only).",
    )
    parser.add_argument(
        "--allow-conflict-waves",
        action="store_true",
        help="Opt in to conflict_edges + colour_classes instead of refusing parent/child collisions.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.findings_json:
        findings_payload = json.loads(Path(args.findings_json).read_text(encoding="utf-8"))
    else:
        findings_payload = _fetch_open_findings_via_cli()

    work_items = work_items_from_findings(findings_payload)
    if args.operator_items:
        raw = json.loads(Path(args.operator_items).read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            ops = raw.get("items") or raw.get("work_items") or []
        else:
            ops = raw
        if not isinstance(ops, list):
            raise SystemExit("--operator-items must be a JSON list or {items:[...]}")
        work_items.extend(ops)

    if not work_items:
        raise SystemExit("no work items to compile (no open findings and no operator items)")

    # Env → kwarg at the CLI boundary only (pure core does not read os.environ).
    env_raw = os.environ.get("WORKBAY_COMPILER_MAX_LANE_FILES")
    max_lane_files: int | None = None
    if env_raw is not None and str(env_raw).strip() != "":
        try:
            max_lane_files = int(str(env_raw).strip())
        except ValueError as exc:
            raise SystemExit(f"WORKBAY_COMPILER_MAX_LANE_FILES must be an int, got {env_raw!r}") from exc

    # implementation note S2: max conflict degree resolved at CLI only (default 8).
    max_conflict_degree: int | None = None
    if args.allow_conflict_waves:
        deg_raw = os.environ.get("WORKBAY_COMPILER_MAX_CONFLICT_DEGREE")
        if deg_raw is None or str(deg_raw).strip() == "":
            max_conflict_degree = DEFAULT_MAX_CONFLICT_DEGREE
        else:
            try:
                max_conflict_degree = int(str(deg_raw).strip())
            except ValueError as exc:
                raise SystemExit(f"WORKBAY_COMPILER_MAX_CONFLICT_DEGREE must be an int, got {deg_raw!r}") from exc

    # WIDTH-44: resolve host_memory at the CLI boundary (same chokepoint as
    # WORKBAY_COMPILER_MAX_LANE_FILES). Pure compile stays free of contract I/O;
    # injection keeps compile-time emission aligned with admission-time policy.
    root = Path(args.orchestrator_root).expanduser().resolve()
    host_memory_policy = load_host_memory_policy(root)

    result = compile_work_manifest(
        work_items,
        task_ref=args.task_ref,
        max_lane_files=max_lane_files,
        emit_verify_twins=not args.no_verify_twins,
        allow_conflict_waves=bool(args.allow_conflict_waves),
        max_conflict_degree=max_conflict_degree,
        host_memory_policy=host_memory_policy,
    )

    # GRPH-03: capture side-effect results and refuse before save_manifest so a
    # failed twin pin or admit-time blocker cannot leave a "successful" compile
    # without the S4 close-gate blockers.
    if not args.skip_provision and result.twin_provisioning:
        twin_results = provision_twins(
            task_ref=args.task_ref,
            twin_provisioning=result.twin_provisioning,
            orchestrator_root=root,
        )
        failed_pins = [row for row in twin_results if isinstance(row, dict) and not row.get("pin_ok")]
        if failed_pins:
            details = "; ".join(f"{row.get('lane_id')}: {row.get('pin_error') or 'pin failed'}" for row in failed_pins)
            raise SystemExit(f"provision_twins pin failed — manifest not saved: {details}")
    if not args.skip_blockers and result.blockers:
        blocker_results = emit_admit_time_blockers(
            task_ref=args.task_ref,
            blockers=result.blockers,
            workspace_root=root,
        )
        failed_blockers = [row for row in blocker_results if isinstance(row, dict) and row.get("result_ok") is False]
        if failed_blockers:
            details = "; ".join(str(row.get("twin_lane_id") or row.get("lane_id") or "?") for row in failed_blockers)
            raise SystemExit(f"emit_admit_time_blockers failed (result_ok=False) — manifest not saved: {details}")

    # WIDTH-45: surface compile diagnostics on stderr so --stdout stays
    # machine-parseable while ungated lanes are not silent.
    for message in result.diagnostics:
        print(message, file=sys.stderr)

    manifest_dir = Path(args.manifest_dir).expanduser() if args.manifest_dir else None
    if args.stdout:
        print(json.dumps(result.manifest, indent=2) + "\n", end="")
    else:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
            manifest_dir as resolve_manifest_dir,
        )

        resolved_dir = resolve_manifest_dir(
            orchestrator_root=str(root) if manifest_dir is None else None,
            manifest_dir=manifest_dir,
        )
        path = resolved_dir / f"{args.task_ref}.json"
        if path.exists():
            # Recompile preservation: merge base_sha under the manifest flock.
            compiled = result.manifest

            def _preserve(loaded: dict[str, Any]) -> None:
                merged = merge_preserved_lane_keys(loaded, compiled, keys=("base_sha",))
                loaded.clear()
                loaded.update(merged)

            atomic_update_manifest(path, _preserve)
        else:
            path = save_manifest(
                result.manifest,
                orchestrator_root=str(root) if manifest_dir is None else None,
                manifest_dir=manifest_dir,
            )
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
