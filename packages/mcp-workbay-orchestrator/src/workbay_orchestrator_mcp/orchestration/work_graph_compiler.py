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
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_manifest import (
    _normalize_owned_path,
    _owned_path_roots_overlap,
    save_manifest,
    validate_manifest,
)

VERIFY_NAMESPACE = "__verify__"
TWIN_BACKENDS: tuple[tuple[str, str], ...] = (
    ("grok", "grok-remote"),
    ("claude", "claude-code"),
)
DEFAULT_DONE = "Ready for orchestrator branch review with lane-local verification complete."
FINDINGS_CLI_TIMEOUT_SECONDS = 30.0
_REMEDY_MENU = (
    "Remedies: (1) repartition owned file_paths so roots neither nest nor collide; "
    "(2) fuse the items with operator fuse_with; "
    "(3) order them with operator depends_on so the overlap is comparable."
)


class WorkGraphCompilerError(RuntimeError):
    """Escapable compiler refusal (reserved id, parent/child collision, max files, …)."""


@dataclass(frozen=True)
class CompileResult:
    """Pure compile output: manifest + side-effect descriptors for the CLI layer."""

    manifest: dict[str, Any]
    twin_provisioning: list[dict[str, Any]] = field(default_factory=list)
    blockers: list[dict[str, Any]] = field(default_factory=list)


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
            raise WorkGraphCompilerError(
                f"work item {item.get('id')!r} file_path refused: {exc}"
            ) from exc
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
            raise WorkGraphCompilerError(
                f"work-item id {item_id!r} contains reserved namespace {VERIFY_NAMESPACE!r}"
            )


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
                raise WorkGraphCompilerError(
                    f"operator {item_id!r} fuse_with references unknown id {partner!r}"
                )
            uf.union(item_id, partner)

    return uf


def _refuse_parent_child_collisions(
    by_id: dict[str, dict[str, Any]],
    uf: _UnionFind,
    canon: dict[str, str],
    depends_on: dict[str, list[str]],
) -> None:
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
                        raise WorkGraphCompilerError(
                            f"parent/child owned-path collision between {left_id!r} ({lr!r}) "
                            f"and {right_id!r} ({rr!r}) without fuse_with. {_REMEDY_MENU}"
                        )


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
                raise WorkGraphCompilerError(
                    f"operator {item_id!r} depends_on references unknown id {dep!r}"
                )
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


def _downstream_from_depends_on(
    lane_ids: list[str], depends_on: dict[str, list[str]]
) -> dict[str, list[str]]:
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


def _emit_verify_twins(
    *,
    work_lane_ids: list[str],
    lanes: dict[str, dict[str, Any]],
    depends_on: dict[str, list[str]],
    task_ref: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Add twin lanes + diamond depends_on; return (provisioning, blockers)."""
    provisioning: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    twin_ids: list[str] = []

    for lane_id in sorted(work_lane_ids):
        parent = lanes[lane_id]
        parent_branch = str(parent["branch"])
        for suffix, backend in TWIN_BACKENDS:
            twin_id = f"{lane_id}{VERIFY_NAMESPACE}{suffix}"
            if twin_id in lanes:
                raise WorkGraphCompilerError(f"twin lane id {twin_id!r} collides with an existing lane")
            if twin_id in twin_ids:
                raise WorkGraphCompilerError(f"duplicate twin lane id {twin_id!r}")
            twin_ids.append(twin_id)
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
                for suffix, _backend in TWIN_BACKENDS:
                    twin_id = f"{prereq}{VERIFY_NAMESPACE}{suffix}"
                    if twin_id not in prereqs and twin_id not in extras:
                        extras.append(twin_id)
        if extras:
            depends_on[consumer] = sorted(set(prereqs) | set(extras))

    # Sort every depends_on list deterministically.
    for key in list(depends_on):
        depends_on[key] = sorted(depends_on[key])

    provisioning.sort(key=lambda d: d["lane_id"])
    blockers.sort(key=lambda d: d["twin_lane_id"])
    return provisioning, blockers


# ---------------------------------------------------------------------------
# Public pure core
# ---------------------------------------------------------------------------


def compile_work_manifest(
    work_items: list[dict[str, Any]],
    *,
    task_ref: str,
    max_lane_files: int | None = None,
    emit_verify_twins: bool = True,
) -> CompileResult:
    """Compile work items into a validate_manifest-clean lane manifest.

    Pure: no DB, no filesystem, no env reads. Same input + kwargs → byte-identical
    manifest JSON. Resolve ``WORKBAY_COMPILER_MAX_LANE_FILES`` at the CLI boundary
    and pass it via ``max_lane_files`` (validated ``>= 1`` when set).
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
    # Refuse parent/child owned-path overlaps only between depends_on-INCOMPARABLE
    # lanes (needs the derived edges); mirrors validate_manifest's acceptance rule.
    _refuse_parent_child_collisions(by_id, uf, canon, depends_on)
    _enforce_max_lane_files(by_id, components, max_files=max_lane_files)

    # Build work lanes (canonical id = min member).
    members_by_lane: dict[str, list[str]] = defaultdict(list)
    for member, lane_id in canon.items():
        members_by_lane[lane_id].append(member)
    for lane_id in members_by_lane:
        members_by_lane[lane_id] = sorted(members_by_lane[lane_id])

    work_lane_ids = sorted(members_by_lane)
    lanes: dict[str, dict[str, Any]] = {}
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
        if lane_id in state_writes:
            lane["state_writes"] = list(state_writes[lane_id])
        if lane_id in state_reads:
            lane["state_reads"] = list(state_reads[lane_id])
        lanes[lane_id] = lane

    # depends_on only over work lanes so far (mutable copy for twin augmentation).
    dep_map: dict[str, list[str]] = {k: list(v) for k, v in depends_on.items()}

    provisioning: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    if emit_verify_twins:
        provisioning, blockers = _emit_verify_twins(
            work_lane_ids=work_lane_ids,
            lanes=lanes,
            depends_on=dep_map,
            task_ref=task_ref,
        )

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

    # Operator-input errors surface from validate_manifest as RuntimeError; re-wrap
    # as the documented escapable compiler refusal (row-4/8 cycle & state conflicts).
    try:
        validate_manifest(manifest, Path("<compiled-manifest>"))
    except RuntimeError as exc:
        raise WorkGraphCompilerError(f"compiled manifest failed validation: {exc}") from exc
    return CompileResult(manifest=manifest, twin_provisioning=provisioning, blockers=blockers)


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
        raise WorkGraphCompilerError(
            f"handoff_close_check failed/invalid for {task_ref!r}: {result!r}"
        )
    data = result.get("data") if isinstance(result.get("data"), dict) else result
    checks = data.get("checks") if isinstance(data, dict) else None
    if not isinstance(checks, dict):
        raise WorkGraphCompilerError(
            f"handoff_close_check schema-invalid for {task_ref!r}: missing checks: {result!r}"
        )
    open_blockers = checks.get("open_blockers")
    if not isinstance(open_blockers, dict):
        raise WorkGraphCompilerError(
            f"handoff_close_check schema-invalid for {task_ref!r}: "
            f"open_blockers not a dict: {result!r}"
        )
    items = open_blockers.get("items")
    if not isinstance(items, list):
        raise WorkGraphCompilerError(
            f"handoff_close_check schema-invalid for {task_ref!r}: "
            f"items not a list: {result!r}"
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
        raise SystemExit(
            f"review_findings list timed out after {FINDINGS_CLI_TIMEOUT_SECONDS}s"
        ) from exc
    if proc.returncode != 0:
        raise SystemExit(
            f"review_findings list failed (exit {proc.returncode}): {proc.stderr or proc.stdout}"
        )
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
            raise SystemExit(
                f"WORKBAY_COMPILER_MAX_LANE_FILES must be an int, got {env_raw!r}"
            ) from exc

    result = compile_work_manifest(
        work_items,
        task_ref=args.task_ref,
        max_lane_files=max_lane_files,
        emit_verify_twins=not args.no_verify_twins,
    )

    root = Path(args.orchestrator_root).expanduser().resolve()
    # GRPH-03: capture side-effect results and refuse before save_manifest so a
    # failed twin pin or admit-time blocker cannot leave a "successful" compile
    # without the S4 close-gate blockers.
    if not args.skip_provision and result.twin_provisioning:
        twin_results = provision_twins(
            task_ref=args.task_ref,
            twin_provisioning=result.twin_provisioning,
            orchestrator_root=root,
        )
        failed_pins = [
            row for row in twin_results if isinstance(row, dict) and not row.get("pin_ok")
        ]
        if failed_pins:
            details = "; ".join(
                f"{row.get('lane_id')}: {row.get('pin_error') or 'pin failed'}"
                for row in failed_pins
            )
            raise SystemExit(f"provision_twins pin failed — manifest not saved: {details}")
    if not args.skip_blockers and result.blockers:
        blocker_results = emit_admit_time_blockers(
            task_ref=args.task_ref,
            blockers=result.blockers,
            workspace_root=root,
        )
        failed_blockers = [
            row
            for row in blocker_results
            if isinstance(row, dict) and row.get("result_ok") is False
        ]
        if failed_blockers:
            details = "; ".join(
                str(row.get("twin_lane_id") or row.get("lane_id") or "?")
                for row in failed_blockers
            )
            raise SystemExit(
                f"emit_admit_time_blockers failed (result_ok=False) — "
                f"manifest not saved: {details}"
            )

    manifest_dir = Path(args.manifest_dir).expanduser() if args.manifest_dir else None
    if args.stdout:
        print(json.dumps(result.manifest, indent=2) + "\n", end="")
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
