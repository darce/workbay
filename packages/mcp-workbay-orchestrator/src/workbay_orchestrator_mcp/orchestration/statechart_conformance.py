#!/usr/bin/env python3
"""Static conformance validator for the lifecycle statechart.

Spec-only structural checks on the declarative chart. Not a runtime interpreter;
production code must not import this module as a transition engine. Properties
are scoped to what the algorithms encode (see check docstrings) — they do not
claim full design-note §2 semantic enforcement or gate totality beyond gates[].
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

# Grounded evidence classes only (design note §3 + machine_observation extension).
# model_prose is deliberately absent.
ALLOWED_EVIDENCE_CLASSES: frozenset[str] = frozenset(
    {
        "commit_ancestry",
        "test_exit_code",
        "findings_count",
        "machine_terminal_outcome",
        "machine_observation",
    }
)

# Schema'd keys on outer/child transition objects and guards. Cross-level child
# refs are allowed only via child_evidence_sources / child_state_refs on guards.
_TRANSITION_SCHEMA_KEYS: frozenset[str] = frozenset(
    {"id", "from", "to", "guard", "guards"}
)
_GUARD_SCHEMA_KEYS: frozenset[str] = frozenset(
    {
        "predicate_id",
        "evidence_class",
        "description",
        "child_evidence_sources",
        "child_state_refs",
    }
)

CHART_FILENAME = "lifecycle_statechart.json"


class ConformanceError(Exception):
    """Raised when a chart fails one or more conformance properties."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = list(errors)
        message = "; ".join(self.errors) if self.errors else "conformance failure"
        super().__init__(message)


@dataclass
class ConformanceReport:
    ok: bool
    errors: list[str] = field(default_factory=list)

    def raise_if_failed(self) -> None:
        if not self.ok:
            raise ConformanceError(self.errors)


def chart_path() -> Path:
    return Path(__file__).resolve().parent / CHART_FILENAME


def load_chart(path: Path | None = None) -> dict[str, Any]:
    target = path if path is not None else chart_path()
    with target.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ConformanceError(["chart root must be a JSON object"])
    return data


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _targets(transition: Mapping[str, Any]) -> list[str]:
    raw = transition.get("to")
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, str)]
    return []


def _iter_guards(transition: Mapping[str, Any]) -> list[dict[str, Any]]:
    guards: list[dict[str, Any]] = []
    single = transition.get("guard")
    if isinstance(single, dict):
        guards.append(single)
    multi = transition.get("guards")
    if isinstance(multi, list):
        for item in multi:
            if isinstance(item, dict):
                guards.append(item)
    return guards


def _machine_edges(transitions: Sequence[Mapping[str, Any]]) -> list[tuple[str, str, str]]:
    """Return (transition_id, from_state, to_state) edges."""
    edges: list[tuple[str, str, str]] = []
    for transition in transitions:
        if not isinstance(transition, Mapping):
            continue
        tid = transition.get("id")
        src = transition.get("from")
        if not isinstance(tid, str) or not isinstance(src, str):
            continue
        for dest in _targets(transition):
            edges.append((tid, src, dest))
    return edges


def _reachable(initial: str, edges: Sequence[tuple[str, str, str]]) -> set[str]:
    adjacency: dict[str, list[str]] = {}
    for _, src, dest in edges:
        adjacency.setdefault(src, []).append(dest)
    seen: set[str] = set()
    stack = [initial]
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, []))
    return seen


def _can_reach_terminal(
    states: Sequence[str],
    terminals: Sequence[str],
    edges: Sequence[tuple[str, str, str]],
) -> dict[str, bool]:
    reverse: dict[str, list[str]] = {}
    for _, src, dest in edges:
        reverse.setdefault(dest, []).append(src)
    can: set[str] = set(terminals)
    stack = list(terminals)
    while stack:
        node = stack.pop()
        for pred in reverse.get(node, []):
            if pred not in can:
                can.add(pred)
                stack.append(pred)
    return {state: state in can for state in states}


def _collect_transition_index(chart: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for key in ("outer", "child"):
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            continue
        transitions = machine.get("transitions", [])
        if not isinstance(transitions, list):
            continue
        for transition in transitions:
            if isinstance(transition, dict) and isinstance(transition.get("id"), str):
                index[transition["id"]] = transition
    return index


def check_machine_hygiene(chart: Mapping[str, Any]) -> list[str]:
    """Graph hygiene: initial ∈ states, terminals ⊆ states, endpoints ∈ states."""
    errors: list[str] = []
    for key in ("outer", "child"):
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            errors.append(f"{key}: missing machine definition")
            continue
        machine_id = machine.get("id", key)
        states = _as_str_list(machine.get("states"))
        state_set = set(states)
        if not states:
            errors.append(f"{machine_id}: states list is empty")
            continue

        initial = machine.get("initial")
        if not isinstance(initial, str):
            errors.append(f"{machine_id}: missing initial state")
        elif initial not in state_set:
            errors.append(
                f"{machine_id}: initial {initial!r} is not in states[]"
            )

        terminals = _as_str_list(machine.get("terminals"))
        for terminal in terminals:
            if terminal not in state_set:
                errors.append(
                    f"{machine_id}: terminal {terminal!r} not in states[]"
                )

        transitions = machine.get("transitions", [])
        if not isinstance(transitions, list):
            errors.append(f"{machine_id}: transitions must be a list")
            continue
        for transition in transitions:
            if not isinstance(transition, Mapping):
                continue
            tid = transition.get("id", "<unknown>")
            src = transition.get("from")
            if isinstance(src, str) and src not in state_set:
                errors.append(
                    f"{machine_id} transition {tid!r}: from {src!r} not in states[]"
                )
            for dest in _targets(transition):
                if dest not in state_set:
                    errors.append(
                        f"{machine_id} transition {tid!r}: to {dest!r} not in states[]"
                    )
    return errors


def check_reachability(chart: Mapping[str, Any]) -> list[str]:
    """Property (b): no unreachable states from each machine's initial state."""
    errors: list[str] = []
    for key in ("outer", "child"):
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            errors.append(f"{key}: missing machine definition")
            continue
        machine_id = machine.get("id", key)
        initial = machine.get("initial")
        states = _as_str_list(machine.get("states"))
        transitions = machine.get("transitions", [])
        if not isinstance(initial, str):
            errors.append(f"{machine_id}: missing initial state")
            continue
        if initial not in set(states):
            # Hygiene owns this; skip BFS over undeclared initial.
            continue
        if not isinstance(transitions, list):
            errors.append(f"{machine_id}: transitions must be a list")
            continue
        edges = _machine_edges(transitions)
        reached = _reachable(initial, edges)
        for state in states:
            if state not in reached:
                errors.append(
                    f"{machine_id}: unreachable state {state!r} from initial {initial!r}"
                )
    return errors


def check_terminal_reachability(chart: Mapping[str, Any]) -> list[str]:
    """Property (a): every state reaches a terminal (per machine)."""
    errors: list[str] = []
    for key in ("outer", "child"):
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            errors.append(f"{key}: missing machine definition")
            continue
        machine_id = machine.get("id", key)
        states = _as_str_list(machine.get("states"))
        terminals = _as_str_list(machine.get("terminals"))
        transitions = machine.get("transitions", [])
        if not terminals:
            errors.append(f"{machine_id}: terminals list is empty")
            continue
        if not isinstance(transitions, list):
            errors.append(f"{machine_id}: transitions must be a list")
            continue
        # Terminal membership is enforced by check_machine_hygiene; still require
        # terminals that are declared states before reverse-BFS.
        state_set = set(states)
        valid_terminals = [t for t in terminals if t in state_set]
        if not valid_terminals:
            errors.append(f"{machine_id}: no terminals that are members of states[]")
            continue
        edges = _machine_edges(transitions)
        reach_map = _can_reach_terminal(states, valid_terminals, edges)
        for state, ok in reach_map.items():
            if not ok:
                errors.append(
                    f"{machine_id}: state {state!r} cannot reach any terminal "
                    f"in {terminals!r}"
                )
    return errors


def check_gate_injectivity(chart: Mapping[str, Any]) -> list[str]:
    """Property (c): gate injectivity + unique gate ids + no ungated closed inbound.

    Enforces: each gate maps to an existing guarded transition; at most one gate
    per transition; gate ids are unique; outer terminal inbounds are gated.
    Does not require every guarded transition to have a gate (non-close edges may
    be ungated by design).
    """
    errors: list[str] = []
    transition_index = _collect_transition_index(chart)
    gates = chart.get("gates", [])
    if not isinstance(gates, list):
        return ["gates must be a list"]

    seen_transitions: dict[str, str] = {}
    seen_gate_ids: set[str] = set()
    gated_transition_ids: set[str] = set()

    for gate in gates:
        if not isinstance(gate, Mapping):
            errors.append("gate entry must be an object")
            continue
        gate_id = gate.get("id")
        transition_id = gate.get("transition_id")
        if not isinstance(gate_id, str) or not gate_id:
            errors.append("gate missing id")
            continue
        if gate_id in seen_gate_ids:
            errors.append(f"duplicate gate id {gate_id!r}")
            continue
        seen_gate_ids.add(gate_id)
        if not isinstance(transition_id, str) or not transition_id:
            errors.append(f"gate {gate_id!r}: missing transition_id")
            continue
        if transition_id not in transition_index:
            errors.append(
                f"gate {gate_id!r}: orphan reference to unknown transition {transition_id!r}"
            )
            continue
        if transition_id in seen_transitions:
            errors.append(
                f"gate {gate_id!r}: duplicate gate on transition {transition_id!r} "
                f"(already gated by {seen_transitions[transition_id]!r})"
            )
            continue
        seen_transitions[transition_id] = gate_id
        gated_transition_ids.add(transition_id)
        transition = transition_index[transition_id]
        if not _iter_guards(transition):
            errors.append(
                f"gate {gate_id!r}: referenced transition {transition_id!r} has no guard"
            )

    outer = chart.get("outer")
    if isinstance(outer, Mapping):
        terminals = set(_as_str_list(outer.get("terminals")))
        transitions = outer.get("transitions", [])
        if isinstance(transitions, list):
            for transition in transitions:
                if not isinstance(transition, Mapping):
                    continue
                tid = transition.get("id")
                if not isinstance(tid, str):
                    continue
                for dest in _targets(transition):
                    if dest in terminals and tid not in gated_transition_ids:
                        errors.append(
                            f"ungated inbound transition {tid!r} to closed-terminal {dest!r}"
                        )
    return errors


# Backward-compatible alias for callers still using the old property (c) name.
check_gate_bijection = check_gate_injectivity


def _child_state_refs_from_guard(guard: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in ("child_evidence_sources", "child_state_refs"):
        raw = guard.get(key)
        if isinstance(raw, list):
            refs.extend(item for item in raw if isinstance(item, str))
    return refs


def check_parent_guard_contract(chart: Mapping[str, Any]) -> list[str]:
    """Property (d): terminal set cover + terminals-only refs (encoding fields only).

    Scope (honest): only the documented encoding fields child_evidence_sources and
    child_state_refs are treated as cross-level refs. Checks: (i) refs ⊆ child
    terminals; (ii) every child terminal appears ≥1 time across outer guards;
    (iii) unknown keys on outer transitions/guards are rejected (blocks alternate
    child-state channels outside the two-field encoding). Does not claim full
    design-note §2 "no parent may read child-internal state" semantic enforcement,
    nor a 1:1 terminal↔predicate bijection.
    """
    errors: list[str] = []
    child = chart.get("child")
    outer = chart.get("outer")
    if not isinstance(child, Mapping) or not isinstance(outer, Mapping):
        return ["outer and child machines are required for parent-guard contract"]

    child_terminals = set(_as_str_list(child.get("terminals")))
    child_states = set(_as_str_list(child.get("states")))
    if not child_terminals:
        errors.append("child machine has no terminals")

    referenced: set[str] = set()
    transitions = outer.get("transitions", [])
    if not isinstance(transitions, list):
        return errors + ["outer transitions must be a list"]

    for transition in transitions:
        if not isinstance(transition, Mapping):
            continue
        tid = transition.get("id", "<unknown>")
        # Unknown-key scan: only schema keys allowed. Child state names are never
        # schema keys, so a key equal to a child state is rejected here as well.
        for key in transition:
            if key not in _TRANSITION_SCHEMA_KEYS:
                detail = (
                    f" matches child state name"
                    if key in child_states
                    else ""
                )
                errors.append(
                    f"outer transition {tid!r}: unknown key {key!r}{detail} "
                    f"(only schema keys {sorted(_TRANSITION_SCHEMA_KEYS)} allowed)"
                )
        for guard in _iter_guards(transition):
            for key in guard:
                if key not in _GUARD_SCHEMA_KEYS:
                    detail = (
                        f" matches child state name"
                        if key in child_states
                        else ""
                    )
                    errors.append(
                        f"outer transition {tid!r}: unknown guard key {key!r}{detail} "
                        f"(cross-level refs only via child_evidence_sources / "
                        f"child_state_refs; schema keys {sorted(_GUARD_SCHEMA_KEYS)})"
                    )
            for ref in _child_state_refs_from_guard(guard):
                if ref not in child_states:
                    errors.append(
                        f"outer transition {tid!r}: child ref {ref!r} is not a child state"
                    )
                    continue
                if ref not in child_terminals:
                    errors.append(
                        f"outer transition {tid!r}: parent guard references non-terminal "
                        f"child state {ref!r} (only terminals {sorted(child_terminals)} allowed)"
                    )
                    continue
                referenced.add(ref)

    for terminal in sorted(child_terminals):
        if terminal not in referenced:
            errors.append(
                f"child terminal {terminal!r} never appears in any outer guard evidence source"
            )
    return errors


def check_nested_region(chart: Mapping[str, Any]) -> list[str]:
    """Nested-region consistency: child_machine id, parent_states membership, exit evidence."""
    errors: list[str] = []
    outer = chart.get("outer")
    child = chart.get("child")
    if not isinstance(outer, Mapping):
        return ["outer machine required for nested_region check"]
    if not isinstance(child, Mapping):
        return ["child machine required for nested_region check"]

    region = outer.get("nested_region")
    if region is None:
        return []
    if not isinstance(region, Mapping):
        return ["outer.nested_region must be an object"]

    child_id = child.get("id")
    declared_child = region.get("child_machine")
    if not isinstance(declared_child, str):
        errors.append("nested_region.child_machine must be a string")
    elif not isinstance(child_id, str) or declared_child != child_id:
        errors.append(
            f"nested_region.child_machine {declared_child!r} does not match "
            f"child.id {child_id!r}"
        )

    outer_states = set(_as_str_list(outer.get("states")))
    parent_states = _as_str_list(region.get("parent_states"))
    parent_set = set(parent_states)
    for state in parent_states:
        if state not in outer_states:
            errors.append(
                f"nested_region.parent_states member {state!r} is not in outer.states[]"
            )

    child_terminals = set(_as_str_list(child.get("terminals")))
    transitions = outer.get("transitions", [])
    if not isinstance(transitions, list) or not parent_set:
        return errors

    for transition in transitions:
        if not isinstance(transition, Mapping):
            continue
        tid = transition.get("id", "<unknown>")
        src = transition.get("from")
        if not isinstance(src, str) or src not in parent_set:
            continue
        dests = _targets(transition)
        leaves = [d for d in dests if d not in parent_set]
        if not leaves:
            continue
        # Leaving the in-flight region requires child-terminal evidence on the guard.
        evidence: set[str] = set()
        for guard in _iter_guards(transition):
            evidence.update(_child_state_refs_from_guard(guard))
        if not evidence:
            errors.append(
                f"outer transition {tid!r}: leaves nested_region parent_states "
                f"toward {leaves!r} without child_evidence_sources / child_state_refs"
            )
            continue
        non_terminal = sorted(evidence - child_terminals)
        if non_terminal:
            errors.append(
                f"outer transition {tid!r}: nested-region exit evidence includes "
                f"non-terminals {non_terminal}"
            )
    return errors


def check_evidence_classes(chart: Mapping[str, Any]) -> list[str]:
    """Property (e): every guard evidence_class is in the grounded set.

    If the chart declares allowed_evidence_classes, guards are checked against
    intersection(declared, global floor). Declared entries outside the global
    floor are reported as errors.
    """
    errors: list[str] = []
    declared = chart.get("allowed_evidence_classes")
    allowed = set(ALLOWED_EVIDENCE_CLASSES)
    if isinstance(declared, list):
        declared_set = {item for item in declared if isinstance(item, str)}
        for item in declared_set:
            if item not in ALLOWED_EVIDENCE_CLASSES:
                errors.append(
                    f"chart allowed_evidence_classes includes non-grounded class {item!r}"
                )
        # Enforce declared ∩ global for guard checks (subset is intentional).
        if declared_set:
            allowed = declared_set & set(ALLOWED_EVIDENCE_CLASSES)

    for key in ("outer", "child"):
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            continue
        machine_id = machine.get("id", key)
        transitions = machine.get("transitions", [])
        if not isinstance(transitions, list):
            continue
        for transition in transitions:
            if not isinstance(transition, Mapping):
                continue
            tid = transition.get("id", "<unknown>")
            for guard in _iter_guards(transition):
                evidence_class = guard.get("evidence_class")
                if not isinstance(evidence_class, str):
                    errors.append(
                        f"{machine_id} transition {tid!r}: guard missing evidence_class"
                    )
                    continue
                if evidence_class not in allowed:
                    errors.append(
                        f"{machine_id} transition {tid!r}: evidence_class "
                        f"{evidence_class!r} not in grounded set "
                        f"{sorted(ALLOWED_EVIDENCE_CLASSES)}"
                    )
    return errors


def validate_chart(chart: Mapping[str, Any]) -> ConformanceReport:
    """Run conformance properties; return a structured report."""
    errors: list[str] = []
    errors.extend(check_machine_hygiene(chart))
    errors.extend(check_terminal_reachability(chart))
    errors.extend(check_reachability(chart))
    errors.extend(check_gate_injectivity(chart))
    errors.extend(check_parent_guard_contract(chart))
    errors.extend(check_nested_region(chart))
    errors.extend(check_evidence_classes(chart))
    return ConformanceReport(ok=not errors, errors=errors)


def validate_chart_or_raise(chart: Mapping[str, Any]) -> ConformanceReport:
    report = validate_chart(chart)
    report.raise_if_failed()
    return report


def validate_shipped_chart() -> ConformanceReport:
    return validate_chart(load_chart())


__all__ = [
    "ALLOWED_EVIDENCE_CLASSES",
    "ConformanceError",
    "ConformanceReport",
    "chart_path",
    "check_evidence_classes",
    "check_gate_bijection",
    "check_gate_injectivity",
    "check_machine_hygiene",
    "check_nested_region",
    "check_parent_guard_contract",
    "check_reachability",
    "check_terminal_reachability",
    "load_chart",
    "validate_chart",
    "validate_chart_or_raise",
    "validate_shipped_chart",
]
