#!/usr/bin/env python3
"""Generate one TLA+ module per machine from ``lifecycle_statechart.json``.

Promoted from the gdntla00 prototype (decision 48617). Stdlib only, and free of
package imports, so it runs both as
``python -m workbay_orchestrator_mcp.orchestration.statechart_tla`` and as a
plain script (``python -I statechart_tla.py``). The generated model is a pure
function of the chart's membership relation, so it cannot drift from the chart
[REF-09][ARCH-15]: ``--check`` exits 1 when files on disk differ from what the
chart generates now, or when a generated-namespace file is stale.

Defaults: ``--chart`` is the shipped ``lifecycle_statechart.json`` next to this
module; ``--out`` is ``docs/specs/tla`` of the checkout that holds this module
(the ancestor whose ``docs/specs`` carries the hand-written statechart spec).

Per machine (``outer`` -> ``TaskMachine``, ``child`` -> ``LaneMachine``):

* ``States``, ``Events``, ``Initial``, ``Terminals``, ``Edges`` (the expanded
  ``(source, event, target)`` set: list targets expanded, ``self_transitions``
  included, duplicates collapsed);
* one action per declared triple, named and keyed state first
  (``Step_<source>__<event>__<target>``) [GRPH-27]; ``Next`` is exactly their
  disjunction, so an undeclared pair has no enabling action;
* ``Init``, ``Spec``, ``TypeOK``, ``StepDeclared`` (cross-check: every step TLC
  takes is in ``Edges``), ``TerminalAbsorbing`` (emitted when the chart declares
  terminals) and one vacuity canary per terminal (``Canary_<terminal>``);
* cfgs: ``<M>.cfg`` (TypeOK, StepDeclared), ``<M>__absorbing.cfg``,
  ``<M>__canary_<terminal>.cfg``; plus ``generated.json`` (per-file sha256).

The generator owns exactly the ``<M>.tla`` / ``<M>[__*].cfg`` namespace of its
two modules and ``generated.json``; hand-written specs in the same directory
(``LaneLandReap.tla``, ``expected_verdicts.json``) are never read or removed.
Chart names must match ``[a-z][a-z0-9_]*`` or the chart is refused, so nothing
from the chart can inject TLA+ text or a path.

Output is byte-stable: every set and action list is sorted, and the header
digest covers the canonical machine content, not chart byte order.

Exit codes: 0 ok / written, 1 drift (``--check``) or invalid chart, 2 I/O
failure or no default ``--out`` resolvable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

GENERATOR_NAME = "statechart_tla.py"
GENERATOR_VERSION = "1"
GENERATOR_COMMAND = "make tla-generate"
CHART_FILENAME = "lifecycle_statechart.json"
MANIFEST_FILENAME = "generated.json"
OUT_SUBDIR = Path("docs") / "specs" / "tla"
# The hand-written statechart spec marks the repo's docs/specs (same anchor as statechart_render).
ANCHOR = Path("docs") / "specs" / "orchestrator-lifecycle-statechart-spec.md"
NAME_RE_TEXT = "[a-z][a-z0-9_]*"
MACHINE_KEYS = (("outer", "TaskMachine"), ("child", "LaneMachine"))
NO_EVENT = "__none__"  # cannot collide: chart names must match [a-z][a-z0-9_]*


class ChartError(ValueError):
    """The chart cannot be translated without guessing."""


def _is_name(value: object) -> bool:
    if not isinstance(value, str) or not value or len(value) > 128:
        return False
    if not ("a" <= value[0] <= "z"):
        return False
    return all(ch == "_" or "a" <= ch <= "z" or "0" <= ch <= "9" for ch in value)


def _name(value: object, where: str) -> str:
    if not _is_name(value):
        raise ChartError(f"{where}: {value!r} does not match {NAME_RE_TEXT}")
    return value  # type: ignore[return-value]


def machine_model(machine: object, where: str) -> dict[str, Any]:
    """Return the membership model of one machine; refuse anything ambiguous."""
    if not isinstance(machine, Mapping):
        raise ChartError(f"{where}: machine is not an object")
    machine_id = _name(machine.get("id"), f"{where}.id")
    raw_states = machine.get("states")
    if not isinstance(raw_states, list) or not raw_states:
        raise ChartError(f"{where}.states: nonempty list required")
    states = [_name(s, f"{where}.states[]") for s in raw_states]
    if len(set(states)) != len(states):
        raise ChartError(f"{where}.states: duplicate state")
    state_set = set(states)
    initial = _name(machine.get("initial"), f"{where}.initial")
    if initial not in state_set:
        raise ChartError(f"{where}.initial: {initial!r} is not a declared state")
    raw_terminals = machine.get("terminals", [])
    if not isinstance(raw_terminals, list):
        raise ChartError(f"{where}.terminals: list required")
    terminals = sorted({_name(t, f"{where}.terminals[]") for t in raw_terminals})
    for terminal in terminals:
        if terminal not in state_set:
            raise ChartError(f"{where}.terminals: {terminal!r} is not a declared state")
    transitions = machine.get("transitions")
    if not isinstance(transitions, list):
        raise ChartError(f"{where}.transitions: list required")
    edges: set[tuple[str, str, str]] = set()
    for index, transition in enumerate(transitions):
        at = f"{where}.transitions[{index}]"
        if not isinstance(transition, Mapping):
            raise ChartError(f"{at}: object required")
        _name(transition.get("id"), f"{at}.id")
        source = _name(transition.get("from"), f"{at}.from")
        event = _name(transition.get("event"), f"{at}.event")
        raw_to = transition.get("to")
        targets = raw_to if isinstance(raw_to, list) else [raw_to]
        if not targets:
            raise ChartError(f"{at}.to: empty target list")
        for target in targets:
            target = _name(target, f"{at}.to")
            for endpoint in (source, target):
                if endpoint not in state_set:
                    raise ChartError(f"{at}: {endpoint!r} is not a declared state")
            edges.add((source, event, target))
    raw_self = machine.get("self_transitions", [])
    if not isinstance(raw_self, list):
        raise ChartError(f"{where}.self_transitions: list required")
    for index, entry in enumerate(raw_self):
        at = f"{where}.self_transitions[{index}]"
        if not isinstance(entry, Mapping):
            raise ChartError(f"{at}: object required")
        state = _name(entry.get("state"), f"{at}.state")
        if state not in state_set:
            raise ChartError(f"{at}: {state!r} is not a declared state")
        edges.add((state, _name(entry.get("event"), f"{at}.event"), state))
    if not edges:
        raise ChartError(f"{where}: no transitions")
    return {
        "id": machine_id,
        "states": sorted(states),
        "initial": initial,
        "terminals": terminals,
        "events": sorted({event for _, event, _ in edges}),
        "edges": sorted(edges),
    }


def chart_models(chart: Mapping[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if not isinstance(chart, Mapping):
        raise ChartError("chart: object required")
    return [(module, machine_model(chart.get(key), key)) for key, module in MACHINE_KEYS]


def _digest(model: Mapping[str, Any]) -> str:
    canonical = json.dumps(model, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _set(items: list[str], indent: str = "    ") -> str:
    if not items:
        return "{}"
    return "{\n" + ",\n".join(f'{indent}"{item}"' for item in items) + "\n}"


def action_name(source: str, event: str, target: str) -> str:
    return f"Step_{source}__{event}__{target}"


def render_module(module: str, model: Mapping[str, Any], *, chart_id: str, chart_version: str) -> str:
    edges = model["edges"]
    lines = [
        f"{'-' * 4} MODULE {module} {'-' * 4}",
        f"\\* GENERATED by {GENERATOR_NAME} v{GENERATOR_VERSION}; do not edit. Regenerate: {GENERATOR_COMMAND}.",
        f"\\* chart {chart_id} {chart_version}, machine {model['id']}, model sha256 {_digest(model)}",
        f"\\* {len(model['states'])} states, {len(edges)} declared (source, event, target) triples.",
        "EXTENDS Naturals",
        "",
        "VARIABLES state, last_event, prev_state",
        "vars == <<state, last_event, prev_state>>",
        "",
        f'NoEvent == "{NO_EVENT}"',
        f"States == {_set(model['states'])}",
        f"Events == {_set(model['events'])}",
        f'Initial == "{model["initial"]}"',
        f"Terminals == {_set(model['terminals'])}",
        "Edges == {",
        ",\n".join(f'    <<"{s}", "{e}", "{t}">>' for s, e, t in edges),
        "}",
        "",
        "Init == state = Initial /\\ last_event = NoEvent /\\ prev_state = NoEvent",
        "",
        "\\* One action per declared triple, keyed (state, event) state first [GRPH-27].",
    ]
    for source, event, target in edges:
        lines.append(
            f"{action_name(source, event, target)} ==\n"
            f'    /\\ state = "{source}"\n'
            f'    /\\ last_event\' = "{event}"\n'
            f'    /\\ state\' = "{target}"\n'
            "    /\\ prev_state' = state"
        )
    lines.append("")
    lines.append("Next ==")
    lines.extend(f"    \\/ {action_name(s, e, t)}" for s, e, t in edges)
    lines += [
        "",
        "Spec == Init /\\ [][Next]_vars",
        "",
        "TypeOK ==",
        "    /\\ state \\in States",
        "    /\\ last_event \\in Events \\cup {NoEvent}",
        "    /\\ prev_state \\in States \\cup {NoEvent}",
        "",
        "\\* Cross-check between Next and Edges: every step taken is a declared triple.",
        "StepDeclared == prev_state = NoEvent \\/ <<prev_state, last_event, state>> \\in Edges",
    ]
    if model["terminals"]:
        lines += [
            "",
            "\\* A step never leaves a declared terminal (self-transitions stay).",
            "TerminalAbsorbing == prev_state \\in Terminals => state = prev_state",
            "",
            "\\* Vacuity canaries: each MUST be violated (the terminal is reachable).",
        ]
        lines.extend(f'Canary_{terminal} == state /= "{terminal}"' for terminal in model["terminals"])
    lines += ["", "=" * 12, ""]
    return "\n".join(lines)


def _cfg(invariants: list[str]) -> str:
    body = ["SPECIFICATION Spec"]
    body.extend(f"INVARIANT {name}" for name in invariants)
    body.append("CHECK_DEADLOCK FALSE")
    return "\n".join(body) + "\n"


def render_files(chart: Mapping[str, Any]) -> dict[str, str]:
    """Return ``{file name: content}`` for every generated module and cfg."""
    chart_id = chart.get("id") if isinstance(chart, Mapping) else None
    chart_version = chart.get("version") if isinstance(chart, Mapping) else None
    if not isinstance(chart_id, str) or not chart_id or not isinstance(chart_version, str) or not chart_version:
        raise ChartError("chart: nonempty string id and version required")
    if any(ch in chart_id + chart_version for ch in "\r\n"):
        raise ChartError("chart: id/version must be one line")
    files: dict[str, str] = {}
    for module, model in chart_models(chart):
        files[f"{module}.tla"] = render_module(module, model, chart_id=chart_id, chart_version=chart_version)
        files[f"{module}.cfg"] = _cfg(["TypeOK", "StepDeclared"])
        if model["terminals"]:
            files[f"{module}__absorbing.cfg"] = _cfg(["TerminalAbsorbing"])
            for terminal in model["terminals"]:
                files[f"{module}__canary_{terminal}.cfg"] = _cfg([f"Canary_{terminal}"])
    manifest = {
        "generator": f"{GENERATOR_NAME} v{GENERATOR_VERSION}",
        "files": {name: hashlib.sha256(text.encode()).hexdigest() for name, text in sorted(files.items())},
    }
    files[MANIFEST_FILENAME] = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    return dict(sorted(files.items()))


def _atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _owned(name: str) -> bool:
    """True for a plain file name in the generator's namespace (never a path)."""
    if name == MANIFEST_FILENAME:
        return True
    if "/" in name or "\\" in name or name.startswith("."):
        return False
    for _, module in MACHINE_KEYS:
        for suffix in (".tla", ".cfg"):
            if name == f"{module}{suffix}":
                return True
        if name.startswith(f"{module}__") and name.endswith(".cfg") and _is_name(name[len(module) + 2 : -4]):
            return True
    return False


def _stale_generated(out: Path, files: Mapping[str, str]) -> list[str]:
    """Owned files on disk (or in the previous manifest) that this chart no longer produces.

    Only names inside the generator's own namespace count, so a tampered
    manifest cannot point removal at a hand-written spec or outside ``out``.
    """
    names: set[str] = set()
    try:
        previous = json.loads((out / MANIFEST_FILENAME).read_text(encoding="utf-8"))
        listed = previous.get("files", {}) if isinstance(previous, dict) else {}
        names |= {name for name in listed if isinstance(name, str)} if isinstance(listed, dict) else set()
    except (OSError, ValueError):
        pass
    try:
        names |= {entry.name for entry in out.iterdir() if entry.is_file()}
    except OSError:
        pass
    return sorted(name for name in names if _owned(name) and name not in files and (out / name).is_file())


def default_chart() -> Path:
    return Path(__file__).resolve().with_name(CHART_FILENAME)


def default_out() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        if (parent / ANCHOR).is_file():
            return parent / OUT_SUBDIR
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--chart", type=Path, default=None, help=f"chart JSON (default: shipped {CHART_FILENAME})")
    parser.add_argument("--out", type=Path, default=None, help=f"output directory (default: <checkout>/{OUT_SUBDIR})")
    parser.add_argument("--check", action="store_true", help="exit 1 if files on disk differ from the chart")
    args = parser.parse_args(argv)
    chart_file = args.chart if args.chart is not None else default_chart()
    out = args.out if args.out is not None else default_out()
    if out is None:
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": "out_unresolved",
                    "detail": f"no ancestor of {Path(__file__).resolve()} contains {ANCHOR}; pass --out",
                }
            )
        )
        return 2
    try:
        chart = json.loads(chart_file.read_text(encoding="utf-8"))
    except OSError as exc:
        print(json.dumps({"status": "error", "reason": "chart_unreadable", "detail": str(exc)}))
        return 2
    except ValueError as exc:
        print(json.dumps({"status": "error", "reason": "chart_invalid_json", "detail": str(exc)}))
        return 1
    try:
        files = render_files(chart)
    except ChartError as exc:
        print(json.dumps({"status": "error", "reason": "chart_invalid", "detail": str(exc)}))
        return 1
    stale = _stale_generated(out, files)
    if args.check:
        drift = []
        for name, text in files.items():
            try:
                current = (out / name).read_bytes().decode("utf-8")
            except (OSError, UnicodeDecodeError):
                current = None
            if current != text:
                drift.append(name)
        drift += stale
        print(json.dumps({"status": "drift" if drift else "ok", "drift": sorted(drift), "stale": stale}))
        return 1 if drift else 0
    try:
        out.mkdir(parents=True, exist_ok=True)
        for name, text in files.items():
            _atomic_write(out / name, text)
        for name in stale:
            (out / name).unlink(missing_ok=True)
    except OSError as exc:
        print(json.dumps({"status": "error", "reason": "write_failed", "detail": str(exc)}))
        return 2
    print(json.dumps({"status": "written", "files": sorted(files), "removed": stale}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
