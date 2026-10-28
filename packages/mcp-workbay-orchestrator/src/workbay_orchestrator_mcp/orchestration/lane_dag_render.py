"""Deterministic, read-only ASCII and JSON rendering of lane DAG manifests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_manifest import (
    _normalize_owned_path,
    _owned_path_roots_overlap,
)
from workbay_orchestrator_mcp.orchestration.lane_ready_set import (
    _declared_depends_on,
    _depends_on_map,
    _remaining_chain_lengths,
    _scheduling_forward,
    compute_layers,
    compute_slack,
)


def _owned_paths(lane: object) -> list[str]:
    if not isinstance(lane, Mapping):
        return []
    raw = lane.get("owned_paths")
    if not isinstance(raw, list):
        return []
    paths: set[str] = set()
    for value in raw:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            paths.add(_normalize_owned_path(value))
        except ValueError:
            # Rendering is an inspection surface, not a second manifest
            # validator. Preserve the token so invalid input remains visible.
            paths.add(value.strip())
    return sorted(paths)


def _overlap_constraint(left: list[str], right: list[str]) -> str | None:
    overlaps: list[str] = []
    for left_path in left:
        for right_path in right:
            if _owned_path_roots_overlap(left_path, right_path):
                if left_path == right_path:
                    overlaps.append(left_path)
                elif left_path.startswith(f"{right_path}/"):
                    overlaps.append(right_path)
                else:
                    overlaps.append(left_path)
    if not overlaps:
        return None
    return f"owned_paths:{','.join(sorted(set(overlaps)))}"


def _critical_path(
    lane_ids: list[str],
    depends_on: Mapping[str, list[str]],
) -> list[str]:
    if not lane_ids:
        return []
    remaining = _remaining_chain_lengths(lane_ids, depends_on)
    forward = _scheduling_forward(lane_ids, depends_on)
    current = min(lane_ids, key=lambda lane_id: (-remaining[lane_id], lane_id))
    path = [current]
    while remaining[current] > 0:
        candidates = [child for child in forward.get(current, ()) if remaining.get(child) == remaining[current] - 1]
        if not candidates:
            break
        current = min(candidates)
        path.append(current)
    return path


def _empty_graph(task_ref: str, width: int) -> dict[str, Any]:
    return {
        "task_ref": task_ref,
        "vertices": {},
        "edges": [],
        "layers": [],
        "layer_widths": [],
        "critical_path": [],
        "critical_path_length": 0,
        "makespan_floor_lane_durations": 0,
        "admitted_width": width,
    }


def _build_vertices(
    lane_ids: list[str],
    lanes: Mapping[str, Any],
    slack: Mapping[str, int],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    vertices: dict[str, dict[str, Any]] = {}
    paths_by_lane: dict[str, list[str]] = {}
    for lane_id in lane_ids:
        lane = lanes[lane_id]
        paths = _owned_paths(lane)
        paths_by_lane[lane_id] = paths
        tier = None
        difficulty = None
        if isinstance(lane, Mapping):
            tier = lane.get("tier", lane.get("preferred_tier"))
            difficulty = lane.get("difficulty")
        vertices[lane_id] = {
            "owned_paths": paths,
            "tier": tier,
            "difficulty": difficulty,
            "slack": slack.get(lane_id, 0),
        }
    return vertices, paths_by_lane


def _data_edge(
    producer: str,
    consumer: str,
    lanes: Mapping[str, Any],
) -> dict[str, Any]:
    consumer_lane = lanes.get(consumer)
    reads = set(consumer_lane.get("state_reads") or ()) if isinstance(consumer_lane, Mapping) else set()
    producer_lane = lanes.get(producer)
    writes = set(producer_lane.get("state_writes") or ()) if isinstance(producer_lane, Mapping) else set()
    named_fields = sorted(str(value) for value in reads & writes if isinstance(value, str) and value)
    return {
        "from": producer,
        "to": consumer,
        "kind": "data",
        "fields": named_fields or ["depends_on"],
        "precedence": True,
    }


def _data_edges(
    depends_on: Mapping[str, list[str]],
    lanes: Mapping[str, Any],
) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for consumer in sorted(depends_on):
        for producer in sorted(set(depends_on[consumer])):
            edges.append(_data_edge(producer, consumer, lanes))
    return edges


def _declared_resource_pairs(
    raw_conflicts: object,
    lanes: Mapping[str, Any],
) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    if not isinstance(raw_conflicts, list):
        return pairs
    for raw in raw_conflicts:
        if not isinstance(raw, (list, tuple)) or len(raw) != 2:
            continue
        left, right = str(raw[0]), str(raw[1])
        if left in lanes and right in lanes and left != right:
            pairs.add(tuple(sorted((left, right))))
    return pairs


def _resource_pairs(
    manifest: Mapping[str, Any],
    lane_ids: list[str],
    lanes: Mapping[str, Any],
    paths_by_lane: Mapping[str, list[str]],
) -> set[tuple[str, str]]:
    pairs = _declared_resource_pairs(manifest.get("conflict_edges"), lanes)
    for index, left in enumerate(lane_ids):
        for right in lane_ids[index + 1 :]:
            if _overlap_constraint(paths_by_lane[left], paths_by_lane[right]) is not None:
                pairs.add((left, right))
    return pairs


def _resource_edges(
    resource_pairs: set[tuple[str, str]],
    paths_by_lane: Mapping[str, list[str]],
) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for left, right in sorted(resource_pairs):
        constraint = _overlap_constraint(paths_by_lane[left], paths_by_lane[right]) or "owned_paths:declared_conflict"
        edges.append(
            {
                "from": left,
                "to": right,
                "kind": "resource",
                "constraint": constraint,
                "precedence": False,
            }
        )
    return edges


def _layer_rows(layers: list[list[str]]) -> list[dict[str, Any]]:
    return [{"layer": index, "lane_ids": layer, "width": len(layer)} for index, layer in enumerate(layers, start=1)]


def _render_ascii(
    task_ref: str,
    width: int,
    lane_ids: list[str],
    layer_rows: list[dict[str, Any]],
    path: list[str],
    edges: list[dict[str, Any]],
    vertices: Mapping[str, Mapping[str, Any]],
) -> str:
    lines = [f"DAG {task_ref or '<unknown>'}: {len(lane_ids)} lanes; admitted width {width}"]
    for row in layer_rows:
        lines.append(f"L{row['layer']} (width {row['width']}): {' '.join(row['lane_ids'])}")
    lines.append(f"critical path ({len(path)} lane-durations): {' -> '.join(path)}")
    for edge in edges:
        if edge["kind"] == "data":
            lines.append(f"{edge['from']} -> {edge['to']} data({','.join(edge['fields'])})")
        else:
            lines.append(f"{edge['from']} --- {edge['to']} resource({edge['constraint']}) [not precedence]")
    for lane_id in lane_ids:
        vertex = vertices[lane_id]
        lines.append(
            f"{lane_id}: tier={vertex['tier'] or 'unset'} slack={vertex['slack']} "
            f"owned_paths={','.join(vertex['owned_paths']) or '-'}"
        )
    return "\n".join(lines)


def render_lane_dag(
    manifest: Mapping[str, Any],
    *,
    admitted_width: int | None = None,
) -> dict[str, Any]:
    """Return ``{"ascii": str, "json": object}`` for a lane manifest.

    Data edges alone feed Kahn layering and the critical path.  Resource edges
    are separately labelled ``precedence=false`` so an owned-path clique can
    never silently serialize the work graph ([GRPH-32]).
    """
    raw_lanes = manifest.get("lanes")
    lanes = raw_lanes if isinstance(raw_lanes, Mapping) else {}
    lane_ids = sorted(lane_id for lane_id in lanes if isinstance(lane_id, str) and lane_id)
    width = max(0, int(admitted_width or 0))
    task_ref = str(manifest.get("task_ref") or "")
    if not lane_ids:
        graph = _empty_graph(task_ref, width)
        return {"ascii": f"DAG empty: 0 lanes; admitted width {width}", "json": graph}

    layers = compute_layers(manifest)
    slack = compute_slack(manifest)
    raw_depends = _depends_on_map(manifest)
    depends_on = _declared_depends_on(lane_ids, raw_depends)
    vertices, paths_by_lane = _build_vertices(lane_ids, lanes, slack)
    edges = _data_edges(depends_on, lanes)
    resource_pairs = _resource_pairs(manifest, lane_ids, lanes, paths_by_lane)
    edges.extend(_resource_edges(resource_pairs, paths_by_lane))
    edges.sort(key=lambda edge: (edge["kind"], edge["from"], edge["to"]))

    path = _critical_path(lane_ids, depends_on)
    layer_rows = _layer_rows(layers)
    graph = {
        "task_ref": task_ref,
        "vertices": vertices,
        "edges": edges,
        "layers": layer_rows,
        "layer_widths": [len(layer) for layer in layers],
        "critical_path": path,
        "critical_path_length": len(path),
        "makespan_floor_lane_durations": len(path),
        "admitted_width": width,
    }

    return {
        "ascii": _render_ascii(task_ref, width, lane_ids, layer_rows, path, edges, vertices),
        "json": graph,
    }
