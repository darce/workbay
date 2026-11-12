"""Read-only lineage observations and a pure, deterministic graph projection.

The flat graph preserves cycles and multiple parents; it is not a scheduler.
See docs/contracts/fleet-lineage-v1.md for the future fleet CLI join contract.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import time
from collections import defaultdict, deque
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import quote

MAX_SOURCE_RECORDS = 10_000
MAX_NODES = 2_000


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False)


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _id(kind, *parts):
    return kind + ":" + ":".join(quote(part, safe="") for part in parts)


def _source_issues(source):
    """Read quality is independent of graph identity and producer status."""
    if not isinstance(source, Mapping):
        return {"source_unknown"}
    state = source.get("state")
    issues = set()
    if state != "fresh":
        issues.add("source_" + (state if state in ("stale", "partial", "unknown") else "unknown"))
    if source.get("partial"):
        issues.add("source_partial")
    return issues


def _quality_state(state, issues):
    if state not in ("fresh", "partial", "stale") or "source_unknown" in issues:
        return "unknown"
    if state == "stale" or "source_stale" in issues:
        return "stale"
    return "partial" if state == "partial" or "source_partial" in issues else state


def read_lineage_snapshot(root, *, runtime=None) -> dict:
    """Bounded live host reads only; no cache writes, admission probes or SSH.

    Retain raw producer fields omitted by B0's metrics projection. Reuse its
    no-follow file budget, deadline/observer slot, and read-only DB machinery.
    """
    # Keep the pure builder independent of its future fleet_snapshot re-export.
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.shared_schema import bounded_handoff_read

    from . import fleet_snapshot as observation

    root = Path(root).absolute()
    runtime = runtime or RuntimeConfig.for_repo(root)
    seed = dict(
        schema_version=1,
        state="unknown",
        partial=True,
        limits_hit=[],
        source_issues=[],
        lanes=[],
        manifest_lanes=[],
        passes=[],
        ledger=observation._unknown(),
        cache=dict(state="unknown", reason="not_read_live_observation"),
    )

    def collect(budget, publish):
        source_issues = set()

        def read(path):
            data, source = observation._read(path, budget)
            source = dict(source, time_basis="file_mtime")
            stamp = observation._timestamp(source.get("observed_at"))
            if stamp is None or stamp > time.time():
                source["state"] = "unknown"
                source.setdefault("reason", "invalid_source_timestamp")
            source_issues.update(_source_issues(source))
            # Publish even for empty manifests or an interrupted collection.
            publish(source_issues=sorted(source_issues))
            return data, source

        def paths(directory):
            entries = []
            try:
                with os.scandir(directory) as scan:
                    for entry in scan:
                        budget.check()
                        if len(entries) >= MAX_SOURCE_RECORDS:
                            budget.issues.add("source_records")
                            break
                        entries.append(Path(entry.path))
            except OSError as exc:
                budget.issues.add(f"directory:{type(exc).__name__}")
            return sorted(entries)

        try:
            lanes = bounded_handoff_read(
                runtime.db_path,
                lambda conn: [
                    dict(row)
                    for row in conn.execute(
                        "SELECT * FROM worktree_lanes ORDER BY id LIMIT ?", (MAX_SOURCE_RECORDS + 1,)
                    )
                ],
                deadline=budget.deadline,
            )
            if len(lanes) > MAX_SOURCE_RECORDS:
                budget.issues.add("db_rows")
            publish(lanes=lanes[:MAX_SOURCE_RECORDS], ledger=dict(state="fresh", time_basis="database_read"))
        except Exception as exc:
            budget.issues.add(f"ledger:{type(exc).__name__}")
            publish(ledger=observation._unknown(type(exc).__name__))

        manifests = []
        for path in paths(root / "config/lane-orchestration"):
            if path.suffix != ".json":
                continue
            data, source = read(path)
            if data is None:
                budget.issues.add("manifest_unreadable")
                continue
            rows = data.get("lanes")
            if not isinstance(rows, dict):
                budget.issues.add("manifest_lanes_invalid")
                continue
            for lane_id, entry in rows.items():
                budget.check()
                if len(manifests) >= MAX_SOURCE_RECORDS:
                    budget.issues.add("manifest_rows")
                    break
                if isinstance(entry, dict):
                    manifests.append(dict(entry, task_ref=data.get("task_ref"), lane_id=lane_id, source=source))
                else:
                    budget.issues.add("manifest_lane_invalid")
        publish(manifest_lanes=manifests)

        passes = []
        for path in paths(runtime.state_dir):
            if not (path.name.startswith("offload-pass-") and path.suffix == ".json"):
                continue
            data, source = read(path)
            # A malformed producer record still has an observed pass-file ID.
            data = dict(data or {})
            config = data.get("completion_config")
            config = config if isinstance(config, dict) else {}
            pass_id = data.get("pass_id") or config.get("pass_id") or path.name[len("offload-pass-") : -5]
            passes.append(dict(data, pass_id=pass_id, source=source))
        publish(passes=passes, state="fresh" if not budget.issues else "unknown")

    result = observation._bounded(seed, collect, observation.REFRESH_SECONDS)
    # _bounded's partial flag describes collection limits/errors. Source age
    # must survive separately, without claiming rows were omitted or invalid.
    source_issues = set(result["source_issues"])
    result.update(
        state=_quality_state(result["state"], source_issues), partial=result["partial"] or bool(source_issues)
    )
    return result


def build_lineage_tree(snapshot, *, root_id=None, limit=200) -> dict:
    """Project observed identity/lineage without mutating the snapshot or stores.

    Edges point container -> contained and consumer -> lineage input. Root
    filtering follows both kinds, with a visited set even for malformed cycles.
    """
    limit = max(1, min(MAX_NODES, int(limit)))
    nodes, edges, issues = {}, set(), set()
    lane_records = defaultdict(list)
    pass_records = []
    source_issues = snapshot.get("source_issues", [])
    if isinstance(source_issues, list):
        issues.update(item for item in source_issues if isinstance(item, str))
    else:
        issues.add("source_unknown")
    if "ledger" in snapshot:
        issues.update(_source_issues(snapshot["ledger"]))

    def node(kind, parts, *, context="", **fields):
        if not all(_text(part) for part in parts):
            digest = hashlib.sha256(_json([kind, parts, context]).encode()).hexdigest()[:24]
            identity = f"unknown:{kind}:{digest}"
            nodes.setdefault(
                identity,
                dict(
                    id=identity,
                    kind="unknown",
                    expected_kind=kind,
                    state="unknown",
                    reason="identity_missing",
                    parent="unknown",
                ),
            )
            issues.add("identity_missing")
            return identity
        identity = _id(kind, *parts)
        nodes.setdefault(identity, dict(id=identity, kind=kind, parent="unknown", **fields))
        return identity

    def edge(source, target, relation="contains", inferred=False):
        edges.add((source, target, relation, inferred))

    def rows(key):
        value = snapshot.get(key, [])
        if not isinstance(value, list):
            issues.add(f"invalid_{key}")
            return []
        if len(value) > MAX_SOURCE_RECORDS:
            issues.add("source_records")
        bounded = value[:MAX_SOURCE_RECORDS]
        for row in bounded:
            if isinstance(row, Mapping) and "source" in row:
                issues.update(_source_issues(row["source"]))
        return bounded

    for source in ("lanes", "manifest_lanes"):
        seen = set()
        for index, row in enumerate(rows(source)):
            if not isinstance(row, Mapping):
                issues.add("lane_invalid")
                continue
            task, lane = row.get("task_ref"), row.get("lane_id")
            identity = node("lane", [task, lane], context=[source, index], task_ref=task, lane_id=lane)
            if identity in seen:
                nodes[identity]["ambiguous"] = True
                issues.add("duplicate_lane_id")
            seen.add(identity)
            lane_records[identity].append(row)
            edge(node("task", [task], context=identity), identity)

    seen = set()
    for index, row in enumerate(rows("passes")):
        if not isinstance(row, Mapping):
            issues.add("pass_invalid")
            continue
        config = row.get("completion_config")
        config = config if isinstance(config, Mapping) else {}
        pass_id = row.get("pass_id") or config.get("pass_id")
        identity = node("pass", [pass_id], context=index, pass_id=pass_id)
        if identity in seen:
            nodes[identity]["ambiguous"] = True
            issues.add("duplicate_pass_id")
        seen.add(identity)
        task, lane, session = row.get("task_ref"), row.get("lane_id"), row.get("session")
        lane_node = node("lane", [task, lane], context=identity, task_ref=task, lane_id=lane)
        task_node = node("task", [task], context=identity)
        edge(task_node, lane_node)
        edge(lane_node, identity)
        if _text(session):
            edge(node("session", [session]), task_node)
        # Do not manufacture a session from an owner_agent, root or filename.
        if _text(row.get("job_id")):
            edge(identity, node("job", [row["job_id"]], children="unknown"))
        pass_records.append((lane_node, row))

    # Only observed branch equality can recover old review lineage. More than
    # one match stays ambiguous; no suffix/prefix/ref normalization is evidence.
    branches = defaultdict(set)
    for identity, records in lane_records.items():
        for row in records:
            if _text(row.get("task_ref")) and _text(row.get("branch")):
                branches[(row.get("task_ref"), row["branch"])].add(identity)
        for field in ("branch", "owner_agent"):
            values = sorted({r[field] for r in records if _text(r.get(field))})
            if len(values) == 1:
                nodes[identity][field] = values[0]
            elif values:
                nodes[identity][field] = "unknown"
                nodes[identity]["ambiguous"] = True
                nodes[identity][field + "_candidates"] = values
                issues.add("conflicting_lane_identity")

    def target_lane(task, lane, context):
        identity = _id("lane", task, lane) if _text(task) and _text(lane) else None
        if identity in nodes:
            return identity
        # Missing targets are typed unknowns, never asserted existing lanes.
        result = node("lane", [None, None], context=[context, task, lane])
        nodes[result].update(reason="target_missing", task_ref=task, lane_id=lane)
        return result

    relation_count = 0

    def declared(identity, row):
        nonlocal relation_count
        lineage = row.get("lineage", [])
        if not isinstance(lineage, list):
            issues.add("lineage_invalid")
            return
        if len(lineage) > MAX_SOURCE_RECORDS:
            issues.add("source_records")
        for item in lineage[:MAX_SOURCE_RECORDS]:
            if relation_count >= MAX_SOURCE_RECORDS:
                issues.add("relation_records")
                return
            relation_count += 1
            if (
                not isinstance(item, Mapping)
                or not _text(item.get("relation"))
                or item["relation"] not in {"reviews", "fixes"}
            ):
                issues.add("lineage_invalid")
                continue
            target = item.get("target")
            if not isinstance(target, Mapping) or target.get("kind") != "lane":
                issues.add("lineage_target_invalid")
                continue
            edge(identity, target_lane(target.get("task_ref"), target.get("lane_id"), identity), item["relation"])

    def has_reviews(row):
        lineage = row.get("lineage")
        return (
            "reviews" in row
            or isinstance(lineage, list)
            and any(isinstance(item, Mapping) and item.get("relation") == "reviews" for item in lineage)
        )

    pass_reviews = {identity for identity, row in pass_records if has_reviews(row)}
    for identity, records in lane_records.items():
        for row in records:
            declared(identity, row)
            supersedes = row.get("supersedes", [])
            if isinstance(supersedes, list):
                for target in supersedes[:MAX_SOURCE_RECORDS]:
                    if relation_count >= MAX_SOURCE_RECORDS:
                        issues.add("relation_records")
                        break
                    relation_count += 1
                    edge(identity, target_lane(row.get("task_ref"), target, identity), "supersedes")
            else:
                issues.add("supersedes_invalid")
        explicit_review = identity in pass_reviews or any(has_reviews(r) for r in records)
        if not explicit_review:
            refs = {r.get("review_of") for r in records if _text(r.get("review_of"))}
            matches = set()
            for ref in refs:
                task = records[0].get("task_ref")
                if _text(task):
                    matches.update(branches.get((task, ref), set()) - {identity})
            if len(matches) == 1 and len(refs) == 1 and not any(nodes[m].get("ambiguous") for m in matches):
                edge(identity, next(iter(matches)), "reviews", True)
            elif matches:
                nodes[identity]["ambiguous"] = True
                nodes[identity]["parent_candidates"] = sorted(matches)
                issues.add("review_parent_ambiguous")
    for identity, row in pass_records:
        declared(identity, row)

    adjacency = defaultdict(set)
    parents = defaultdict(set)
    indegree = dict.fromkeys(nodes, 0)
    for source, target, relation, _ in edges:
        adjacency[source].add(target)
        indegree[target] += 1
        # Lane.parent names the lineage input; other kinds use containment.
        if relation != "contains":
            parents[source].add(target)
        elif nodes[target]["kind"] != "lane":
            parents[target].add(source)
    for identity, values in parents.items():
        nodes[identity]["parent"] = next(iter(values)) if len(values) == 1 else "unknown"
        if len(values) > 1:
            nodes[identity]["parent_candidates"] = sorted(values)

    # Kahn's check reports cycles without claiming a legal execution order.
    ready = deque(sorted(n for n, degree in indegree.items() if degree == 0))
    visited = 0
    # Multiplicity is per typed edge, not per adjacency entry.
    outgoing = defaultdict(list)
    for source, target, _, _ in sorted(edges):
        outgoing[source].append(target)
    while ready:
        current = ready.popleft()
        visited += 1
        for target in outgoing[current]:
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
    if visited != len(nodes):
        issues.add("cycle_observed")

    selected = set(nodes)
    if root_id is not None:
        selected = set()
        pending = [root_id] if root_id in nodes else []
        while pending:
            current = pending.pop()
            if current in selected:
                continue
            selected.add(current)
            pending.extend(sorted(adjacency[current] - selected, reverse=True))
    ordered = sorted(selected)
    if root_id in selected:
        ordered.remove(root_id)
        ordered.insert(0, root_id)
    retained = set(ordered[:limit])
    truncated = len(ordered) > limit or bool({"source_records", "relation_records"} & issues)
    return dict(
        schema_version=1,
        state=_quality_state(snapshot.get("state", "unknown"), issues),
        partial=bool(snapshot.get("partial", False) or issues or truncated),
        root_id=root_id,
        root_state="known" if root_id is None or root_id in nodes else "unknown",
        nodes=[copy.deepcopy(nodes[n]) for n in ordered[:limit]],
        edges=[
            dict(source=s, target=t, relation=r, inferred=i)
            for s, t, r, i in sorted(edges)
            if s in retained and t in retained
        ],
        issues=sorted(issues),
        truncated=truncated,
        limit=limit,
        matched_nodes=len(ordered),
        cache=copy.deepcopy(snapshot.get("cache", dict(state="unknown", reason="not_observed"))),
    )


def bound_tree_bytes(tree, byte_cap=65_536) -> dict:
    """Whole-node truncation with no dangling edges; includes envelope bytes."""
    cap = max(1_024, min(65_536, int(byte_cap)))
    result = copy.deepcopy(tree)
    result["byte_cap"] = cap
    while len(_json(result).encode()) > cap:
        result.update(truncated=True, partial=True)
        if result["nodes"]:
            removed = result["nodes"].pop()["id"]
            result["edges"] = [e for e in result["edges"] if removed not in (e["source"], e["target"])]
        else:
            # Even a caller's oversized root ID must not escape the cap.
            result = dict(
                schema_version=1,
                state="unknown",
                partial=True,
                truncated=True,
                root_state="unknown",
                reason="byte_cap",
                nodes=[],
                edges=[],
                byte_cap=cap,
                cache=dict(state="unknown", reason="not_returned"),
            )
    return result
