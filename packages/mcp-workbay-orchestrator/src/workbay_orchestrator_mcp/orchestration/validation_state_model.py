"""Bounded MODEL/property evidence for the lifecycle edge-membership checker.

No guard interpretation, database access, lifecycle dispatch, or readiness policy.
See docs/contracts/validation-state-model-v1.md for the finite abstraction.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import random
import tempfile
import time
from collections import deque
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from . import statechart_conformance as conformance

MACHINES = ("outer", "child")
PROPERTIES = ("order", "rename", "malformed", "illegal_edge")
EXIT_CODES = {"pass": 0, "fail": 1, "unknown": 2}
Checker = Callable[[Mapping[str, Any], str, str, str], None]


class _Stopped(Exception):
    pass


class _Invalid(ValueError):
    pass


class _Clock:
    def __init__(self, seconds: float):
        self.started = time.monotonic()
        self.seconds = seconds

    def check(self):
        if time.monotonic() - self.started >= self.seconds:
            raise _Stopped("deadline")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_path(path: Path | None) -> Path:
    try:
        return (path if path is not None else conformance.chart_path()).resolve()
    except (RuntimeError, ValueError) as exc:
        # Python 3.12 reports filesystem symlink loops as RuntimeError, not ELOOP.
        # Invalid filesystem paths can raise ValueError. Translate only here,
        # never around the model checker.
        raise OSError(str(exc)) from exc


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _bounds(max_depth, max_states, deadline_seconds, seed):
    for key, value in (("max_depth", max_depth), ("max_states", max_states)):
        if type(value) is not int or value <= 0:
            raise _Invalid(f"{key} must be a positive finite integer")
    if type(deadline_seconds) not in (int, float) or not math.isfinite(deadline_seconds) or deadline_seconds <= 0:
        raise _Invalid("deadline_seconds must be positive and finite")
    if type(seed) is not int:
        raise _Invalid("seed must be an integer (not bool)")


def _new_report(bounds):
    return {
        "schema": "validation-state-model@1",
        "category": "MODEL",
        "status": "unknown",
        "scope": {
            "machines": list(MACHINES),
            "semantics": "edge-membership-only",
            "abstraction": "finite states with shortest reachable witnesses; all ordered state pairs plus unknown",
            "self_transitions": "all known states, including terminals",
            "terminals": "labels; outgoing declared edges remain legal",
            "limitations": [
                "guards remain with predicate owners",
                "no live database, timing, or liveness policy validation",
                "no cross-machine product or guard synchronization",
                "no full proposal/readiness qualification",
            ],
        },
        "bounds": _json_safe(bounds),
        "identity": {},
        "machines": {},
        "properties": {p: {"status": "unknown", "comparisons": 0} for p in PROPERTIES},
        "coverage": {
            "complete": False,
            "states": {"checked": 0, "total": 0},
            "pairs": {"checked": 0, "total": 0},
            "exploration_exhausted": False,
        },
        "counterexamples": [],
        "reasons": [],
    }


def _reason(report, kind, detail):
    report["reasons"].append({"kind": kind, "detail": detail})


def _name(value, where):
    if not isinstance(value, str) or not value:
        raise _Invalid(f"{where} must be a nonempty string")
    return value


def _names(value, where, *, nonempty=True):
    if not isinstance(value, list) or (nonempty and not value):
        raise _Invalid(f"{where} must be {'a nonempty' if nonempty else 'a'} list")
    for item in value:
        _name(item, where)
    if len(set(value)) != len(value):
        raise _Invalid(f"{where} contains duplicates")
    return sorted(value)


def _derive(chart, clock):
    """Independent parser: never call the production edge helpers or reachability walks."""
    if not isinstance(chart, Mapping):
        raise _Invalid("chart must be an object")
    _name(chart.get("id"), "chart.id")
    _name(chart.get("version"), "chart.version")
    model = {}
    for key in MACHINES:
        clock.check()
        machine = chart.get(key)
        if not isinstance(machine, Mapping):
            raise _Invalid(f"{key} must be an object")
        _name(machine.get("id"), f"{key}.id")
        states = _names(machine.get("states"), f"{key}.states")
        initial = _name(machine.get("initial"), f"{key}.initial")
        terminals = _names(machine.get("terminals"), f"{key}.terminals")
        if initial not in states or not set(terminals) <= set(states):
            raise _Invalid(f"{key}: initial and terminals must be known states")
        transitions = machine.get("transitions")
        if not isinstance(transitions, list):
            raise _Invalid(f"{key}.transitions must be a list")
        edges, ids = [], set()
        for index, transition in enumerate(transitions):
            clock.check()
            where = f"{key}.transitions[{index}]"
            if not isinstance(transition, Mapping):
                raise _Invalid(f"{where} must be an object")
            event = _name(transition.get("id"), f"{where}.id")
            if event in ids:
                raise _Invalid(f"{where}: duplicate transition id {event}")
            ids.add(event)
            source = _name(transition.get("from"), f"{where}.from")
            target = transition.get("to")
            targets = [target] if isinstance(target, str) else target
            # Duplicated list targets have the same membership semantics as one target.
            if not isinstance(targets, list) or not targets:
                raise _Invalid(f"{where}.to must name at least one state")
            for dest in targets:
                clock.check()
                _name(dest, f"{where}.to")
                if source not in states or dest not in states:
                    raise _Invalid(f"{where}: endpoints must be known states")
                edges.append((event, source, dest))
        model[key] = {"states": states, "initial": initial, "terminals": terminals, "edges": sorted(set(edges))}
    return model


def _walk(model, max_depth, max_states, clock):
    """State quotient exploration. Revisited states close cycles; no infinite trace enumeration."""
    results, reasons, used = {}, [], 0
    for key in MACHINES:
        m = model[key]
        paths = {}
        results[key] = paths
        if used >= max_states:
            reasons.append(f"{key}: max_states")
            continue
        paths[m["initial"]] = [m["initial"]]
        used += 1
        queue = deque([m["initial"]])
        successors = {s: sorted({t for _, src, t in m["edges"] if src == s}) for s in m["states"]}
        while queue:
            clock.check()
            source = queue.popleft()
            for target in successors[source]:
                clock.check()
                if target in paths:
                    continue
                if len(paths[source]) > max_depth:
                    reasons.append(f"{key}: max_depth at {source} -> {target}")
                    continue
                if used >= max_states:
                    reasons.append(f"{key}: max_states at {source} -> {target}")
                    continue
                paths[target] = paths[source] + [target]
                used += 1
                queue.append(target)
    return results, sorted(set(reasons))


def _unknown(states):
    result = "model_unknown"
    while result in states:
        result += "_"
    return result


def _allowed(model, source, target):
    return (source == target and source in model["states"]) or any(
        src == source and dest == target for _, src, dest in model["edges"]
    )


def _observe(checker, chart, key, source, target, clock):
    clock.check()
    try:
        checker(chart, key, source, target)
        outcome = "accept"
    except conformance.IllegalTransition:
        outcome = "reject"
    except conformance.ConformanceError:
        outcome = "malformed"
    clock.check()
    return outcome


def _counterexample(report, prop, key, source, target, expected, observed, paths, **extra):
    path = paths.get(source, [source]) + [target]
    report["counterexamples"].append(
        {
            "property": prop,
            "machine": key,
            "source": source,
            "target": target,
            "expected": expected,
            "actual": observed,
            "path": path,
            "path_kind": "reachable_prefix_plus_probe" if source in paths else "unreachable_source_probe",
            **extra,
        }
    )


def _compare(report, prop, chart, model, paths, checker, clock, mapping=None):
    """All-pair checker evidence against the independent immutable relation."""
    evidence = {}
    for key in MACHINES:
        m = model[key]
        states = m["states"] + [_unknown(m["states"])]
        evidence[key] = []
        for source in states:
            for target in states:
                clock.check()
                expected = "accept" if _allowed(m, source, target) else "reject"
                src = mapping[key]["states"].get(source, source) if mapping else source
                dest = mapping[key]["states"].get(target, target) if mapping else target
                observed = _observe(checker, chart, key, src, dest, clock)
                evidence[key].append({"from": source, "to": target, "expected": expected, "actual": observed})
                if prop == "membership":
                    report["coverage"]["pairs"]["checked"] += 1
                else:
                    report["properties"][prop]["comparisons"] += 1
                if expected != observed:
                    _counterexample(
                        report,
                        prop,
                        key,
                        source,
                        target,
                        expected,
                        observed,
                        paths[key],
                        mapped_probe={"from": src, "to": dest},
                    )
    return evidence


def _ordered(value, rng):
    if isinstance(value, dict):
        keys = list(value)
        rng.shuffle(keys)
        return {k: _ordered(value[k], rng) for k in keys}
    if isinstance(value, list):
        # Only transition/state/target order is changed below. Other lists may have order semantics.
        return [_ordered(v, rng) for v in value]
    return value


def _variants(chart, model, seed):
    rng = random.Random(seed)
    ordered = _ordered(copy.deepcopy(chart), rng)
    renamed = copy.deepcopy(chart)
    mapping = {}
    for key in MACHINES:
        m = ordered[key]
        # Reverse as well as shuffle, so even tiny lists are exercised deterministically.
        m["transitions"].reverse()
        m["states"].reverse()
        m["terminals"].reverse()
        for t in m["transitions"]:
            if isinstance(t["to"], list):
                t["to"].reverse()
        names = list(model[key]["states"])
        rng.shuffle(names)
        state_map = {s: f"renamed_{key}_{i}" for i, s in enumerate(names)}
        event_map = {e: f"event_{key}_{i}" for i, e in enumerate(sorted({e for e, _, _ in model[key]["edges"]}))}
        mapping[key] = {"states": state_map, "events": event_map}
        m = renamed[key]
        m["states"] = [state_map[s] for s in m["states"]]
        m["initial"] = state_map[m["initial"]]
        m["terminals"] = [state_map[s] for s in m["terminals"]]
        for t in m["transitions"]:
            t["id"] = event_map[t["id"]]
            t["from"] = state_map[t["from"]]
            t["to"] = [state_map[s] for s in t["to"]] if isinstance(t["to"], list) else state_map[t["to"]]
    return ordered, renamed, mapping


def _metamorphic(report, chart, model, paths, checker, clock, max_depth, max_states, seed):
    ordered, renamed, mapping = _variants(chart, model, seed)
    report["properties"]["rename"]["mapping"] = mapping
    for prop, variant in (("order", ordered), ("rename", renamed)):
        derived = _derive(variant, clock)
        variant_paths, cutoffs = _walk(derived, max_depth, max_states, clock)
        if cutoffs:
            raise _Stopped(f"{prop}: exploration budget")
        for key in MACHINES:
            expected = {mapping[key]["states"][s] for s in paths[key]} if prop == "rename" else set(paths[key])
            report["properties"][prop]["comparisons"] += 1
            if set(variant_paths[key]) != expected:
                _counterexample(
                    report,
                    prop,
                    key,
                    model[key]["initial"],
                    model[key]["initial"],
                    sorted(expected),
                    sorted(variant_paths[key]),
                    paths[key],
                )
        _compare(report, prop, variant, model, paths, checker, clock, mapping if prop == "rename" else None)
        report["properties"][prop]["status"] = (
            "fail" if any(c["property"] == prop for c in report["counterexamples"]) else "pass"
        )

    # Independent malformed-only path: the extra declared state has no valid inbound edge.
    # Production assert_transition eagerly rejects malformed transitions (except known self loops).
    # Production check_reachability must keep this state unreachable as well.
    malformed = [
        None,
        {},
        {"id": "bad", "from": "start", "to": []},
        {"id": "bad", "from": "start", "to": ["island", 7]},
        {"from": "start", "to": "island"},
    ]
    for key in MACHINES:
        for bad in malformed:
            clock.check()
            variant = copy.deepcopy(chart)
            variant[key] = {
                "id": f"probe_{key}",
                "states": ["start", "done", "island"],
                "initial": "start",
                "terminals": ["done"],
                "transitions": [{"id": "good", "from": "start", "to": "done"}, bad],
            }
            errors = conformance.check_reachability(variant)
            clock.check()
            report["properties"]["malformed"]["comparisons"] += 1
            if not any("unreachable state 'island'" in e and f"probe_{key}" in e for e in errors):
                _counterexample(
                    report,
                    "malformed",
                    key,
                    "start",
                    "island",
                    "unreachable",
                    "reachable",
                    {"start": ["start"]},
                    malformed_transition=bad,
                )
            observed = _observe(checker, variant, key, "start", "island", clock)
            report["properties"]["malformed"]["comparisons"] += 1
            if observed != "malformed":
                _counterexample(
                    report,
                    "malformed",
                    key,
                    "start",
                    "island",
                    "malformed",
                    observed,
                    {"start": ["start"]},
                    malformed_transition=bad,
                )
    report["properties"]["malformed"]["status"] = (
        "fail" if any(c["property"] == "malformed" for c in report["counterexamples"]) else "pass"
    )

    # Negative control: a changed checker chart is compared to the original relation.
    # A real newly accepted edge must discriminate; this is not a model-authored checker.
    for key in MACHINES:
        m = model[key]
        source = m["initial"]
        target = next((s for s in m["states"] if not _allowed(m, source, s)), _unknown(m["states"]))
        variant = copy.deepcopy(chart)
        variant[key]["transitions"].append({"id": "model_injected_illegal", "from": source, "to": target})
        observed = _observe(checker, variant, key, source, target, clock)
        report["properties"]["illegal_edge"]["comparisons"] += 1
        witness = {
            "machine": key,
            "source": source,
            "target": target,
            "expected": "reject",
            "actual": observed,
            "path": paths[key][source] + [target],
            "injected": True,
        }
        report["properties"]["illegal_edge"].setdefault("detected_counterexamples", []).append(witness)
        if observed != "accept":
            _counterexample(report, "illegal_edge", key, source, target, "accept", observed, paths[key])
    report["properties"]["illegal_edge"]["status"] = (
        "fail" if any(c["property"] == "illegal_edge" for c in report["counterexamples"]) else "pass"
    )


def check_model(
    chart: Mapping[str, Any],
    *,
    max_depth: int,
    max_states: int,
    deadline_seconds: float,
    seed: int = 0,
    checker: Checker | None = None,
) -> dict[str, Any]:
    """Compare the actual checker with an independently derived finite relation.

    Required positive bounds cannot be disabled. ``max_states`` caps each two-machine
    exploration (base/order/rename), including initial states. Deadline is shared.
    A supplied checker is an instrumentation/test seam; it must return promptly.
    Only IllegalTransition is a membership rejection; unexpected errors are UNKNOWN.
    """
    report = _new_report(
        {"max_depth": max_depth, "max_states": max_states, "deadline_seconds": deadline_seconds, "seed": seed}
    )
    started = time.monotonic()
    try:
        _bounds(max_depth, max_states, deadline_seconds, seed)
        clock = _Clock(deadline_seconds)
        # Bind source bytes as well as chart bytes; callers still own immutable source custody.
        report["identity"] = {
            "model_sha256": _sha(Path(__file__)),
            "checker_sha256": _sha(Path(conformance.__file__)),
            "checker": "statechart_conformance.assert_transition",
            "checker_mode": "supplied" if checker is not None else "actual",
            "chart_id": chart.get("id") if isinstance(chart, Mapping) else None,
            "chart_version": chart.get("version") if isinstance(chart, Mapping) else None,
        }
        chart = copy.deepcopy(chart)
        clock.check()
        model = _derive(chart, clock)
        try:
            report["identity"]["chart_semantic_sha256"] = hashlib.sha256(_canonical(chart)).hexdigest()
        except (ValueError, TypeError) as exc:
            raise _Invalid(f"chart must contain finite JSON values: {exc}") from exc
        report["coverage"]["states"]["total"] = sum(len(m["states"]) for m in model.values())
        report["coverage"]["pairs"]["total"] = sum((len(m["states"]) + 1) ** 2 for m in model.values())
        paths, cutoffs = _walk(model, max_depth, max_states, clock)
        for key in MACHINES:
            m = model[key]
            report["machines"][key] = {
                "initial": m["initial"],
                "terminals": m["terminals"],
                "states": m["states"],
                "paths": paths[key],
                "unreachable_or_unexplored": sorted(set(m["states"]) - set(paths[key])),
                "transitions": [{"id": e, "from": s, "to": t} for e, s, t in m["edges"]],
                "known_self_edges": [[s, s] for s in m["states"]],
            }
        report["coverage"]["states"]["checked"] = sum(len(p) for p in paths.values())
        report["coverage"]["exploration_exhausted"] = not cutoffs
        for cutoff in cutoffs:
            _reason(report, "budget_exhausted", cutoff)
        if report["coverage"]["states"]["checked"] < report["coverage"]["states"]["total"]:
            _reason(report, "partial_coverage", "not every declared state reached in both machines")
        # No budget is silently skipped to get a PASS. Unknown properties remain in the denominator.
        if not cutoffs:
            active_checker = conformance.assert_transition if checker is None else checker
            evidence = _compare(report, "membership", chart, model, paths, active_checker, clock)
            for key in MACHINES:
                report["machines"][key]["membership"] = evidence[key]
            _metamorphic(report, chart, model, paths, active_checker, clock, max_depth, max_states, seed)
        clock.check()
        report["coverage"]["complete"] = (
            not report["reasons"]
            and report["coverage"]["pairs"]["checked"] == report["coverage"]["pairs"]["total"]
            and all(p["status"] == "pass" for p in report["properties"].values())
        )
        report["status"] = (
            "fail" if report["counterexamples"] else ("pass" if report["coverage"]["complete"] else "unknown")
        )
    except _Invalid as exc:
        report["status"] = "fail"
        _reason(report, "invalid_input", str(exc))
    except _Stopped as exc:
        report["status"] = "fail" if report["counterexamples"] else "unknown"
        report["coverage"]["complete"] = False
        _reason(report, "budget_exhausted", str(exc))
    except Exception as exc:
        report["status"] = "fail" if report["counterexamples"] else "unknown"
        report["coverage"]["complete"] = False
        _reason(report, "checker_unavailable", f"{type(exc).__name__}: {exc}")
    report["timing"] = {"elapsed_seconds": time.monotonic() - started}
    return report


def run_model(
    chart_path: str | Path | None = None,
    *,
    max_depth: int,
    max_states: int,
    deadline_seconds: float,
    seed: int = 0,
) -> dict[str, Any]:
    """Read explicit chart bytes, or the immutable shipped default, and bind their SHA."""
    bounds = dict(max_depth=max_depth, max_states=max_states, deadline_seconds=deadline_seconds, seed=seed)
    report = _new_report(bounds)
    started = time.monotonic()
    try:
        _bounds(**bounds)
        path = _resolve_path(Path(chart_path) if chart_path is not None else None)
        report["identity"]["chart_path"] = str(path)
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        report["identity"]["chart_sha256"] = digest
        report["identity"]["model_sha256"] = _sha(Path(__file__))
        report["identity"]["checker_sha256"] = _sha(Path(conformance.__file__))
        chart = json.loads(raw)
        remaining = deadline_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise _Stopped("deadline loading chart")
        report = check_model(chart, max_depth=max_depth, max_states=max_states, deadline_seconds=remaining, seed=seed)
        report["bounds"] = bounds
        report["identity"].update(chart_path=str(path), chart_sha256=digest)
    except (_Invalid, ValueError, TypeError) as exc:
        report["status"] = "fail"
        _reason(report, "invalid_input", str(exc))
    except (OSError, _Stopped) as exc:
        report["status"] = "unknown"
        _reason(report, "chart_unavailable" if isinstance(exc, OSError) else "budget_exhausted", str(exc))
    report["timing"] = {"elapsed_seconds": time.monotonic() - started}
    return report


def write_report(report: Mapping[str, Any], destination: str | Path) -> None:
    """Atomically replace destination after writing and fsyncing a sibling tempfile."""
    path = Path(destination)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(report, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException as exc:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as cleanup_error:
                exc.add_note(f"temporary report cleanup failed for {temporary}: {cleanup_error}")
        raise


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _Invalid(message)


def _json_safe(value):
    # Invalid NaN/Infinity inputs must still yield a strict JSON failure report.
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--chart", type=Path, help="defaults to shipped lifecycle_statechart.json")
    parser.add_argument("--max-depth", type=int, required=True)
    parser.add_argument("--max-states", type=int, required=True)
    parser.add_argument("--deadline-seconds", type=float, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--report", type=Path)
    args = None
    try:
        args = parser.parse_args(argv)
        report = run_model(
            args.chart,
            max_depth=args.max_depth,
            max_states=args.max_states,
            deadline_seconds=args.deadline_seconds,
            seed=args.seed,
        )
    except _Invalid as exc:
        report = _new_report({})
        report["status"] = "fail"
        _reason(report, "invalid_input", str(exc))
    report = _json_safe(report)
    if args is not None and args.report is not None:
        try:
            # Do not destroy the chart with an accidentally identical output path.
            destination = _resolve_path(args.report)
            chart_path = report["identity"].get("chart_path")
            if chart_path is None:
                # Invalid bounds can return before loading; still protect that input.
                # An unresolvable input cannot alias a successfully resolved output.
                try:
                    chart_path = str(_resolve_path(args.chart))
                except OSError:
                    pass
            if str(destination) == chart_path:
                raise _Invalid("report must not overwrite chart input")
            write_report(report, args.report)
        except (OSError, _Invalid) as exc:
            report["status"] = "fail" if report["status"] == "fail" or isinstance(exc, _Invalid) else "unknown"
            report["coverage"]["complete"] = False
            detail = "\n".join([str(exc), *getattr(exc, "__notes__", [])])
            _reason(report, "invalid_input" if isinstance(exc, _Invalid) else "report_unavailable", detail)
    print(json.dumps(report, sort_keys=True, allow_nan=False))
    return EXIT_CODES[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
