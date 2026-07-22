"""Passive ready-set view over a lane ``depends_on`` graph (internal).

``compute_ready_set`` answers which lanes could start right now and which of
those matter most — by descendant-count ranking and by structural criticality.

This module is read-only and internal. It never dispatches, never mutates
caller state, and never performs I/O.

Citations
---------
``[CON-11]`` (check-then-act atomicity)
    The ready-set is a snapshot computed at one instant and is stale the moment
    it is returned. It must never be treated as authority to dispatch. The
    authoritative gate remains ``lane_dependency_satisfied`` /
    ``collect_unsatisfied_dependencies``, evaluated at dispatch time by the
    caller. Computing eligibility here and acting later is a classic TOCTOU
    window; this view documents the window rather than closing it.

``[GRPH-05]`` / directed bottleneck criticality
    ``critical`` is a **directed** scheduling SPOF flag, not undirected
    articulation. Lane *X* is critical iff there exists at least one other
    declared lane *P* such that every path from any dependency-root to *P*
    in the scheduling digraph (prereq → dependent) passes through *X*.
    Equivalently: removing *X* strictly increases the set of declared lanes
    that have no path from any remaining root. Bound: O(V·(V+E)) remove-and-
    recheck (lane graphs are small). Undirected Tarjan cut vertices are
    unsound for this signal (e.g. diamond root is directed-critical but not
    an undirected articulation point).
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any


def _lane_ids(manifest: Mapping[str, Any]) -> list[str]:
    """Stable sorted lane identifiers from a manifest dict."""
    lanes = manifest.get("lanes")
    if not isinstance(lanes, dict):
        return []
    return sorted(lid for lid in lanes if isinstance(lid, str) and lid)


def _depends_on_map(manifest: Mapping[str, Any]) -> dict[str, list[str] | None]:
    """Normalized lane → direct-prereq adjacency.

    Normalization matches ``orchestrator_lanes._depends_on_map`` for list-valued
    entries: keep string tokens with ``p.strip()`` truthy (whitespace-only
    dropped). Non-list prereq values are recorded as ``None`` so eligibility
    fails closed rather than treating the lane as unconstrained ([DOM-06]).
    """
    raw = manifest.get("depends_on")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str] | None] = {}
    for lane, prereqs in raw.items():
        if not isinstance(lane, str) or not lane:
            continue
        if not isinstance(prereqs, list):
            # Invalid shape: refuse unconstrained readiness for this lane.
            out[lane] = None
            continue
        cleaned = [p for p in prereqs if isinstance(p, str) and p.strip()]
        out[lane] = cleaned
    return out


def _depends_on_ancestors(
    depends_on: Mapping[str, list[str] | None],
    lane_id: str,
) -> list[str]:
    """Transitive ``depends_on`` ancestors of *lane_id* (dispatch-aligned).

    Mirrors ``orchestrator_lanes.depends_on_ancestors``: DFS over lane→prereq
    adjacency, cycle-safe via a visited set, excludes *lane_id* itself.
    Lanes with invalid (``None``) adjacency contribute no outbound edges.
    """
    if not lane_id:
        return []
    ancestors: list[str] = []
    seen: set[str] = {lane_id}
    raw = depends_on.get(lane_id)
    stack: list[str] = list(raw) if isinstance(raw, list) else []
    while stack:
        node = stack.pop()
        if not isinstance(node, str) or not node or node in seen:
            continue
        seen.add(node)
        ancestors.append(node)
        nxt = depends_on.get(node)
        if not isinstance(nxt, list):
            continue
        for prereq in nxt:
            if isinstance(prereq, str) and prereq and prereq not in seen:
                stack.append(prereq)
    return ancestors


def _collect_unknown_lanes(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str] | None],
) -> list[str]:
    """Sorted lane tokens referenced in ``depends_on`` but absent from lanes."""
    declared = set(lane_ids)
    unknown: set[str] = set()
    for lane, prereqs in depends_on.items():
        if lane not in declared:
            unknown.add(lane)
        if not isinstance(prereqs, list):
            continue
        for p in prereqs:
            if isinstance(p, str) and p and p not in declared:
                unknown.add(p)
    return sorted(unknown)


def _declared_depends_on(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str] | None],
) -> dict[str, list[str]]:
    """Adjacency restricted to declared lanes (ghosts dropped for structure)."""
    declared = set(lane_ids)
    out: dict[str, list[str]] = {}
    for lane, prereqs in depends_on.items():
        if lane not in declared or not isinstance(prereqs, list):
            continue
        out[lane] = [p for p in prereqs if p in declared]
    return out


def _descendant_counts(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str]],
) -> dict[str, int]:
    """Count declared lanes that transitively depend on each lane.

    Only nodes in *lane_ids* participate. Ghost keys/prereqs in the raw map
    must not create phantom reverse edges or inflate counts.
    """
    reverse: dict[str, list[str]] = {lid: [] for lid in lane_ids}
    for lane, prereqs in depends_on.items():
        if lane not in reverse:
            continue
        for prereq in prereqs:
            if prereq not in reverse:
                continue
            reverse[prereq].append(lane)

    counts: dict[str, int] = {}
    for lid in lane_ids:
        seen: set[str] = set()
        stack = list(reverse.get(lid, []))
        while stack:
            node = stack.pop()
            if node in seen or node == lid or node not in reverse:
                continue
            seen.add(node)
            stack.extend(reverse.get(node, []))
        counts[lid] = len(seen)
    return counts


def _scheduling_forward(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str]],
) -> dict[str, list[str]]:
    """Prereq → dependents adjacency among declared lanes only."""
    declared = set(lane_ids)
    fwd: dict[str, list[str]] = {lid: [] for lid in lane_ids}
    for lane, prereqs in depends_on.items():
        if lane not in declared:
            continue
        for prereq in prereqs:
            if prereq in declared and prereq != lane:
                fwd[prereq].append(lane)
    return fwd


def _dependency_roots(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str]],
) -> list[str]:
    """Lanes with no declared-lane prerequisites (scheduling sources)."""
    declared = set(lane_ids)
    roots: list[str] = []
    for lid in lane_ids:
        prereqs = depends_on.get(lid, [])
        if not any(p in declared for p in prereqs):
            roots.append(lid)
    return roots


def _reachable_from_roots(
    roots: Collection[str],
    fwd: Mapping[str, list[str]],
    *,
    forbidden: str | None = None,
) -> set[str]:
    """Nodes reachable from *roots* in the scheduling digraph, skipping *forbidden*."""
    seen: set[str] = set()
    stack = [r for r in roots if r != forbidden]
    while stack:
        node = stack.pop()
        if node == forbidden or node in seen:
            continue
        seen.add(node)
        for dep in fwd.get(node, ()):
            if dep != forbidden and dep not in seen:
                stack.append(dep)
    return seen


def _directed_critical_lanes(
    lane_ids: Collection[str],
    depends_on: Mapping[str, list[str]],
) -> set[str]:
    """Directed scheduling bottlenecks ([GRPH-05] directed form).

    Lane *X* is critical when removing *X* strictly increases the set of
    declared lanes with no path from any original dependency root (roots are
    fixed from the full graph; *X* is never re-promoted into a root by
    dropping edges). Bound O(V·(V+E)).
    """
    if len(lane_ids) < 2:
        return set()

    lane_set = set(lane_ids)
    fwd = _scheduling_forward(lane_ids, depends_on)
    roots = _dependency_roots(lane_ids, depends_on)

    baseline = _reachable_from_roots(roots, fwd, forbidden=None)
    baseline_orphans = lane_set - baseline

    critical: set[str] = set()
    for x in lane_ids:
        remaining = lane_set - {x}
        reachable = _reachable_from_roots(roots, fwd, forbidden=x)
        after_orphans = remaining - reachable
        before_orphans = baseline_orphans - {x}
        if after_orphans - before_orphans:
            critical.add(x)
    return critical


def _transitive_prereqs_satisfied(
    lane_id: str,
    depends_on: Mapping[str, list[str] | None],
    satisfied: Collection[str],
) -> bool:
    """True when every transitive ancestor is in *satisfied* (dispatch-aligned).

    Invalid adjacency (``None``) fails closed — lane is never ready.
    """
    raw = depends_on.get(lane_id)
    if raw is None and lane_id in depends_on:
        return False
    sat = set(satisfied)
    for prereq in _depends_on_ancestors(depends_on, lane_id):
        if prereq not in sat:
            return False
    return True


def dispatch_order(
    lane_ids: list[str],
    depends_on: Mapping[str, list[str]],
) -> list[str]:
    """Return a deterministic fan-out permutation of *all* *lane_ids*.

    Ranks by ``(-descendant_count, lane_id)`` over the declared-lane graph
    (ghosts dropped). Does not filter by satisfaction/completion — that is
    ``compute_ready_set``'s job. Pure; never mutates *lane_ids*.
    """
    declared = _declared_depends_on(lane_ids, depends_on)
    counts = _descendant_counts(lane_ids, declared)
    return sorted(list(lane_ids), key=lambda lid: (-counts.get(lid, 0), lid))


def compute_ready_set(
    manifest: Mapping[str, Any],
    *,
    satisfied: Collection[str] = (),
    completed: Collection[str] = (),
) -> dict[str, Any]:
    """Return ready lanes ranked by descendant count, with directed criticality.

    Parameters
    ----------
    manifest
        Lane orchestration manifest dict. Read for ``lanes`` and optional
        ``depends_on`` only. Never mutated.
    satisfied
        Lane ids whose dependency obligations are discharged (successfully
        landed / treated as done for dependents). A failed upstream is simply
        absent from this collection, so dependents of that upstream are not
        ready. Must include the full ancestor closure for a dependent to be
        ready (same transitive predicate as dispatch).
    completed
        Lane ids that have already finished. Completed lanes are excluded from
        the ready set even when their prerequisites are satisfied.

    Returns
    -------
    dict
        ::

            {
                "ready": [
                    {
                        "lane_id": str,
                        "descendant_count": int,
                        "critical": bool,  # directed bottleneck (see module doc)
                    },
                    ...
                ],
                "unknown_lanes": [str, ...],  # depends_on tokens not in lanes
            }

        ``ready`` ordered by descending ``descendant_count``, then ascending
        ``lane_id`` (total order; fully deterministic). Empty input →
        ``{"ready": [], "unknown_lanes": []}``.

    Notes
    -----
    Pure: no filesystem, git, network, or MCP access. All satisfaction facts
    arrive via *satisfied* / *completed*. The result is a snapshot ([CON-11]);
    re-evaluate ``lane_dependency_satisfied`` at dispatch time.
    """
    # Materialize caller collections once; never write back.
    satisfied_set = {s for s in satisfied if isinstance(s, str) and s}
    completed_set = {c for c in completed if isinstance(c, str) and c}

    lane_ids = _lane_ids(manifest)
    depends_raw = _depends_on_map(manifest)
    unknown_lanes = _collect_unknown_lanes(lane_ids, depends_raw)
    # Structural facts use declared-lane edges only (ghosts cannot inflate).
    depends_declared = _declared_depends_on(lane_ids, depends_raw)

    descendant_counts = _descendant_counts(lane_ids, depends_declared)
    critical_lanes = _directed_critical_lanes(lane_ids, depends_declared)

    ready: list[dict[str, Any]] = []
    for lid in lane_ids:
        if lid in completed_set:
            continue
        # Transitive ancestors (full map, so undeclared prereqs fail closed).
        if not _transitive_prereqs_satisfied(lid, depends_raw, satisfied_set):
            continue
        ready.append(
            {
                "lane_id": lid,
                "descendant_count": int(descendant_counts.get(lid, 0)),
                "critical": lid in critical_lanes,
            }
        )

    # Rank: most descendants first; lane_id breaks ties (P2 + P6).
    ready.sort(key=lambda e: (-e["descendant_count"], e["lane_id"]))
    return {
        "ready": ready,
        "unknown_lanes": list(unknown_lanes),
    }
