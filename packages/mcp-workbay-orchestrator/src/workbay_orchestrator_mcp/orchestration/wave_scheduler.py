"""Pure wave-DAG validation and scheduling helpers.

Dependency edges describe precedence. Owned-path overlaps form a separate
conflict graph and only constrain concurrent admission; they never add DAG
edges.
"""

from __future__ import annotations

import fnmatch
import heapq
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

_CONFLICT_HOLDING_STATES = frozenset({"running", "collected", "gated"})
_BLOCKING_STATES = frozenset({"failed", "parked"})


@dataclass(frozen=True)
class LaneSpec:
    """The scheduling-relevant declaration for one lane."""

    depends_on: tuple[str, ...] = ()
    paths: tuple[str, ...] = ()
    weight: int = 1


@dataclass(frozen=True)
class WaveSpec:
    """An immutable, normalized wave DAG."""

    wave: str
    lanes: Mapping[str, LaneSpec]

    def __post_init__(self) -> None:
        if not isinstance(self.wave, str):
            raise TypeError("wave must be a string")
        if not isinstance(self.lanes, Mapping):
            raise TypeError("lanes must be a mapping")

        normalized: dict[str, LaneSpec] = {}
        for lane_id, raw_lane in self.lanes.items():
            if not isinstance(lane_id, str) or not lane_id:
                raise ValueError("lane ids must be non-empty strings")
            normalized[lane_id] = _coerce_lane(raw_lane, lane_id)
        object.__setattr__(self, "lanes", MappingProxyType(dict(sorted(normalized.items()))))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WaveSpec:
        """Build an immutable spec from the documented mapping shape."""
        if not isinstance(data, Mapping):
            raise TypeError("wave spec must be a mapping")
        if "wave" not in data:
            raise ValueError("wave spec requires a wave string")
        wave = data["wave"]
        if not isinstance(wave, str):
            raise TypeError("wave must be a string")
        raw_lanes = data.get("lanes")
        if not isinstance(raw_lanes, Mapping):
            raise TypeError("lanes must be a mapping")
        return cls(wave=wave, lanes=raw_lanes)


def _string_tuple(value: object, field: str, lane_id: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"lane {lane_id!r} {field} must be a sequence of strings")
    values: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"lane {lane_id!r} {field} entries must be non-empty strings")
        values.append(item)
    return tuple(sorted(set(values)))


def _coerce_lane(raw_lane: object, lane_id: str) -> LaneSpec:
    if isinstance(raw_lane, LaneSpec):
        raw_fields: Mapping[str, object] = {
            "depends_on": raw_lane.depends_on,
            "paths": raw_lane.paths,
            "weight": raw_lane.weight,
        }
    elif isinstance(raw_lane, Mapping):
        raw_fields = raw_lane
    else:
        raise TypeError("each lane must be a mapping or LaneSpec")
    depends_on = _string_tuple(raw_fields.get("depends_on", ()), "depends_on", lane_id)
    paths = _string_tuple(raw_fields.get("paths", ()), "paths", lane_id)
    weight = raw_fields.get("weight", 1)
    if isinstance(weight, bool) or not isinstance(weight, int):
        raise TypeError(f"lane weight must be an integer: {weight!r}")
    if weight < 0:
        raise ValueError("lane weight must be non-negative")
    return LaneSpec(depends_on=depends_on, paths=paths, weight=weight)


def _coerce_spec(spec: WaveSpec | Mapping[str, Any]) -> WaveSpec:
    if isinstance(spec, WaveSpec):
        return spec
    return WaveSpec.from_dict(spec)


def _non_canonical_owned_paths(spec: WaveSpec) -> list[dict[str, object]]:
    problems: list[dict[str, object]] = []
    for lane_id, lane in spec.lanes.items():
        for path in lane.paths:
            if "\\" in path or any(segment in {"", ".", ".."} for segment in path.split("/")):
                problems.append({"type": "non_canonical_path", "lane_id": lane_id, "path": path})
    return problems


def _cycle_problems(spec: WaveSpec) -> list[dict[str, object]]:
    """Return deterministic, ordered cycles, excluding separately typed self edges."""
    lane_ids = set(spec.lanes)
    forward: dict[str, list[str]] = {lane_id: [] for lane_id in spec.lanes}
    for lane_id, lane in spec.lanes.items():
        for dependency in lane.depends_on:
            if dependency in lane_ids and dependency != lane_id:
                forward[dependency].append(lane_id)
    adjacency = {lane_id: tuple(sorted(children)) for lane_id, children in forward.items()}
    color: dict[str, int] = {}
    path: list[str] = []
    path_index: dict[str, int] = {}
    cycles: set[tuple[str, ...]] = set()

    for root in sorted(spec.lanes):
        if color.get(root, 0):
            continue
        color[root] = 1
        path_index[root] = len(path)
        path.append(root)
        stack: list[tuple[str, Any]] = [(root, iter(adjacency[root]))]
        while stack:
            node, children = stack[-1]
            try:
                child = next(children)
            except StopIteration:
                stack.pop()
                color[node] = 2
                path_index.pop(node, None)
                path.pop()
                continue

            child_color = color.get(child, 0)
            if child_color == 0:
                color[child] = 1
                path_index[child] = len(path)
                path.append(child)
                stack.append((child, iter(adjacency[child])))
            elif child_color == 1:
                cycle = path[path_index[child] :]
                rotations = [tuple(cycle[index:] + cycle[:index]) for index in range(len(cycle))]
                cycles.add(min(rotations))

    return [{"type": "cycle", "lane_ids": list(cycle)} for cycle in sorted(cycles)]


def validate(spec: WaveSpec | Mapping[str, Any]) -> list[dict[str, object]]:
    """Return typed graph problems; an empty list means the DAG is valid."""
    wave_spec = _coerce_spec(spec)
    problems: list[dict[str, object]] = []
    lane_ids = set(wave_spec.lanes)

    for lane_id, lane in wave_spec.lanes.items():
        for dependency in lane.depends_on:
            if dependency == lane_id:
                problems.append({"type": "self_dependency", "lane_id": lane_id})
            elif dependency not in lane_ids:
                problems.append({"type": "dangling_dependency", "lane_id": lane_id, "dependency": dependency})

    problems.extend(_non_canonical_owned_paths(wave_spec))
    problems.extend(_cycle_problems(wave_spec))
    return problems


def _topological_order(spec: WaveSpec) -> list[str] | None:
    """Return a lane-id-tied Kahn order, or ``None`` for an invalid graph."""
    if validate(spec):
        return None

    indegree = {lane_id: len(lane.depends_on) for lane_id, lane in spec.lanes.items()}
    forward: dict[str, list[str]] = {lane_id: [] for lane_id in spec.lanes}
    for lane_id, lane in spec.lanes.items():
        for dependency in lane.depends_on:
            forward[dependency].append(lane_id)

    ready = [lane_id for lane_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        lane_id = heapq.heappop(ready)
        order.append(lane_id)
        for child in forward[lane_id]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)

    return order if len(order) == len(spec.lanes) else None


def _glob_literal_prefix(pattern: str) -> str:
    for index, char in enumerate(pattern):
        if char in "*?[":
            return pattern[:index]
    return pattern


def _path_overlap(left: str, right: str) -> bool:
    if left == right:
        return True

    left_dir = left.rstrip("/")
    right_dir = right.rstrip("/")
    if (
        left_dir
        and right_dir
        and (left_dir == right_dir or left_dir.startswith(f"{right_dir}/") or right_dir.startswith(f"{left_dir}/"))
    ):
        return True

    if fnmatch.fnmatchcase(left, right) or fnmatch.fnmatchcase(right, left):
        return True

    if not any(char in "*?[" for char in left) or not any(char in "*?[" for char in right):
        return False

    # Matching each pattern against the other pattern's text cannot detect
    # intersections such as [ab] with [a-c]. If their fixed prefixes do not
    # contradict each other, conservatively keep the lanes out of one frontier.
    left_prefix = _glob_literal_prefix(left)
    right_prefix = _glob_literal_prefix(right)
    return left_prefix.startswith(right_prefix) or right_prefix.startswith(left_prefix)


def conflicts(spec: WaveSpec | Mapping[str, Any]) -> set[frozenset[str]]:
    """Return pairwise path overlaps without adding precedence edges."""
    wave_spec = _coerce_spec(spec)
    invalid_paths = _non_canonical_owned_paths(wave_spec)
    if invalid_paths:
        problem = invalid_paths[0]
        raise ValueError(f"non_canonical_path for lane {problem['lane_id']!r}: {problem['path']!r}")

    lane_ids = sorted(wave_spec.lanes)
    result: set[frozenset[str]] = set()
    for index, left_id in enumerate(lane_ids):
        for right_id in lane_ids[index + 1 :]:
            left = wave_spec.lanes[left_id]
            right = wave_spec.lanes[right_id]
            if any(_path_overlap(left_path, right_path) for left_path in left.paths for right_path in right.paths):
                result.add(frozenset({left_id, right_id}))
    return result


def blocked(spec: WaveSpec | Mapping[str, Any], states: Mapping[str, str]) -> dict[str, str]:
    """Map each descendant of failed/parked work to its nearest blocking ancestor.

    If equally near blockers exist, the lexicographically first lane id is used.
    """
    wave_spec = _coerce_spec(spec)
    if validate(wave_spec):
        return {}

    results: dict[str, str] = {}
    for lane_id, lane in wave_spec.lanes.items():
        queue = deque((dependency, 1) for dependency in lane.depends_on)
        seen: set[str] = set()
        candidates: list[tuple[int, str]] = []
        while queue:
            ancestor, distance = queue.popleft()
            if ancestor in seen:
                continue
            seen.add(ancestor)
            if states.get(ancestor, "pending") in _BLOCKING_STATES:
                candidates.append((distance, ancestor))
            queue.extend((dependency, distance + 1) for dependency in wave_spec.lanes[ancestor].depends_on)
        if candidates:
            results[lane_id] = min(candidates)[1]
    return results


def _remaining_path_lengths(spec: WaveSpec, order: Sequence[str]) -> dict[str, int]:
    forward: dict[str, list[str]] = {lane_id: [] for lane_id in spec.lanes}
    for lane_id, lane in spec.lanes.items():
        for dependency in lane.depends_on:
            forward[dependency].append(lane_id)

    remaining: dict[str, int] = {}
    for lane_id in reversed(order):
        remaining[lane_id] = max((remaining[child] + 1 for child in forward[lane_id]), default=0)
    return remaining


def frontier(
    spec: WaveSpec | Mapping[str, Any],
    states: Mapping[str, str],
    *,
    width: int | None = None,
) -> list[str]:
    """Choose a deterministic maximal conflict-free set of currently ready lanes."""
    wave_spec = _coerce_spec(spec)
    order = _topological_order(wave_spec)
    if order is None:
        return []

    path_conflicts = conflicts(wave_spec)
    busy = {lane_id for lane_id, state in states.items() if state in _CONFLICT_HOLDING_STATES}
    blocked_lanes = set(blocked(wave_spec, states))
    candidates: list[str] = []
    for lane_id, lane in wave_spec.lanes.items():
        if states.get(lane_id, "pending") != "pending" or lane_id in blocked_lanes:
            continue
        if not all(states.get(dependency, "pending") == "integrated" for dependency in lane.depends_on):
            continue
        if any(frozenset({lane_id, other}) in path_conflicts for other in busy):
            continue
        candidates.append(lane_id)

    remaining = _remaining_path_lengths(wave_spec, order)
    candidates.sort(key=lambda lane_id: (-remaining[lane_id], lane_id))

    selected: list[str] = []
    for lane_id in candidates:
        if any(frozenset({lane_id, chosen}) in path_conflicts for chosen in selected):
            continue
        selected.append(lane_id)

    if width is not None:
        if isinstance(width, bool) or not isinstance(width, int):
            raise TypeError("width must be an integer or None")
        selected = selected[: max(0, width)]
    return selected


def integration_order(spec: WaveSpec | Mapping[str, Any], done: set[str]) -> list[str]:
    """Topologically order green lanes whose full prerequisite chains are done."""
    wave_spec = _coerce_spec(spec)
    if validate(wave_spec):
        return []

    candidates = set(done) & set(wave_spec.lanes)
    indegree = {lane_id: len(lane.depends_on) for lane_id, lane in wave_spec.lanes.items() if lane_id in candidates}
    forward: dict[str, list[str]] = {lane_id: [] for lane_id in candidates}
    for lane_id in candidates:
        for dependency in wave_spec.lanes[lane_id].depends_on:
            if dependency in candidates:
                forward[dependency].append(lane_id)

    ready = [lane_id for lane_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        lane_id = heapq.heappop(ready)
        order.append(lane_id)
        for child in forward[lane_id]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    return order


def levels(spec: WaveSpec | Mapping[str, Any]) -> dict[str, int]:
    """Return each lane's zero-based longest-path layer, or empty for an invalid DAG."""
    wave_spec = _coerce_spec(spec)
    order = _topological_order(wave_spec)
    if order is None:
        return {}

    result: dict[str, int] = {}
    for lane_id in order:
        dependencies = wave_spec.lanes[lane_id].depends_on
        result[lane_id] = 0 if not dependencies else 1 + max(result[dependency] for dependency in dependencies)
    return dict(sorted(result.items()))


def critical_path(spec: WaveSpec | Mapping[str, Any]) -> list[str]:
    """Return a deterministic maximum-weight source-to-sink path."""
    wave_spec = _coerce_spec(spec)
    order = _topological_order(wave_spec)
    if order is None or not order:
        return []

    forward: dict[str, list[str]] = {lane_id: [] for lane_id in wave_spec.lanes}
    for lane_id, lane in wave_spec.lanes.items():
        for dependency in lane.depends_on:
            forward[dependency].append(lane_id)

    weight_from: dict[str, int] = {}
    next_lane: dict[str, str] = {}
    for lane_id in reversed(order):
        children = forward[lane_id]
        if not children:
            weight_from[lane_id] = wave_spec.lanes[lane_id].weight
            continue
        best_child = min(children, key=lambda child: (-weight_from[child], child))
        weight_from[lane_id] = wave_spec.lanes[lane_id].weight + weight_from[best_child]
        next_lane[lane_id] = best_child

    roots = [lane_id for lane_id, lane in wave_spec.lanes.items() if not lane.depends_on]
    current = min(roots, key=lambda lane_id: (-weight_from[lane_id], lane_id))
    path = [current]
    while current in next_lane:
        current = next_lane[current]
        path.append(current)
    return path


def width_profile(spec: WaveSpec | Mapping[str, Any]) -> list[int]:
    """Return lane counts indexed by zero-based longest-path layer."""
    lane_levels = levels(spec)
    if not lane_levels:
        return []
    profile = [0] * (max(lane_levels.values()) + 1)
    for level in lane_levels.values():
        profile[level] += 1
    return profile
