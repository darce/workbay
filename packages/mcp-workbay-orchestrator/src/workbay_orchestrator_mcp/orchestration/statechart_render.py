#!/usr/bin/env python3
"""Generate the lifecycle statechart docs from the one authored JSON chart.

``lifecycle_statechart.json`` is the only authored source. This module renders
``docs/specs/orchestrator-lifecycle-statechart.md`` (tables + per-machine
Mermaid) and ``.mmd`` (both machines) from it, and ``--check`` fails closed when
either file drifts. Rendering is pure: no timestamps, no environment reads, and
output is independent of list/key order in the chart.

Exit codes: 0 in sync / written; 1 drift (stale paths printed); 2 chart
unreadable or invalid, no ancestor holding the spec anchor (nothing written), or
a ``--write`` I/O failure (each file is replaced atomically, so it is old or new).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from .statechart_conformance import (
    CHART_FILENAME,
    MACHINE_KEYS,
    ConformanceError,
    _iter_guards,
    _machine_edges,
    _targets,
    chart_path,
    render_mermaid,
    validate_chart,
)

GENERATOR_COMMAND = "make statechart-render"
MODULE_COMMAND = "python -m workbay_orchestrator_mcp.orchestration.statechart_render --write"
MARKDOWN_FILENAME = "orchestrator-lifecycle-statechart.md"
MERMAID_FILENAME = "orchestrator-lifecycle-statechart.mmd"
DOCS_SUBDIR = Path("docs") / "specs"
# The hand-written spec marks the repo's docs/specs; any other docs/specs is not ours.
ANCHOR_FILENAME = "orchestrator-lifecycle-statechart-spec.md"
_EMPTY = "—"
# A sibling render temp this old belongs to a killed run; a live --write replaces its temp within milliseconds.
_STALE_TEMP_SECONDS = 60.0
# Per-run random part of a render temp name, so no two runs share one (equal pids in separate PID namespaces).
_TEMP_TOKEN_BYTES = 8


def _cell(value: str) -> str:
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    return text.replace("|", "\\|").replace("\n", "<br>")


def _span(text: str) -> str:
    """One CommonMark code span holding ``text`` exactly; empty text has no span and yields ``""``."""
    if not text:
        return ""
    fence = "`" * (max((len(run) for run in re.findall("`+", text)), default=0) + 1)
    # CommonMark strips one space from each side when both are present, so pad to keep the value exact.
    pad = " " if "`" in (text[:1], text[-1:]) or (text[:1] == text[-1:] == " " and text.strip()) else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def _code(value: str, *, table: bool = False) -> str:
    """CommonMark code span(s) for ``value``, or ``_EMPTY`` when there is nothing to show.

    In a table cell each line is its own span and the spans are joined by ``<br>`` (a ``<br>`` inside a span
    renders literally); ``|`` is escaped only there, where GFM splits table cells. Elsewhere lines join with a space.
    """
    if table:
        lines = value.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        spans = [_span(line.replace("|", "\\|")) for line in lines]
    else:
        spans = [_span(" ".join(value.splitlines()))]
    return "<br>".join(spans) if any(spans) else _EMPTY


def _codes(values: Sequence[str], *, table: bool = False) -> str:
    return ", ".join(_code(value, table=table) for value in values) if values else _EMPTY


def _row(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    return [_row(header), _row(["---"] * len(header)), *(_row(row) for row in rows)]


def _gates_by_transition(chart: Mapping[str, Any]) -> dict[str, list[str]]:
    joined: dict[str, list[str]] = {}
    for gate in chart.get("gates", []):
        if isinstance(gate, Mapping) and isinstance(gate.get("transition_id"), str):
            joined.setdefault(gate["transition_id"], []).append(str(gate.get("id", "")))
    return {tid: sorted(ids) for tid, ids in joined.items()}


def _transition_rows(machine: Mapping[str, Any], gates: Mapping[str, list[str]]) -> list[list[str]]:
    keyed: list[tuple[tuple[str, tuple[str, ...], str], list[str]]] = []
    for transition in machine["transitions"]:
        tid, src = transition["id"], transition["from"]
        targets = tuple(sorted(_targets(transition)))
        guards = _iter_guards(transition)
        predicates = sorted({str(guard.get("predicate_id", "")) for guard in guards})
        classes = sorted({str(guard.get("evidence_class", "")) for guard in guards})
        cells = [
            _code(tid, table=True),
            _code(src, table=True),
            _codes(targets, table=True),
            _codes(predicates, table=True),
            _codes(classes, table=True),
            _codes(gates.get(tid, []), table=True),
        ]
        keyed.append(((src, targets, tid), cells))
    return [cells for _, cells in sorted(keyed, key=lambda item: item[0])]


def _machine_section(chart: Mapping[str, Any], key: str, gates: Mapping[str, list[str]]) -> list[str]:
    machine = chart[key]
    lines = [
        f"## {key}: `{machine['id']}`",
        "",
        f"- Initial: {_code(machine['initial'])}",
        f"- Terminals: {_codes(sorted(machine['terminals']))}",
    ]
    region = machine.get("nested_region")
    if isinstance(region, Mapping):
        lines.append(
            f"- Nested region {_code(str(region.get('id', '')))}: child machine "
            f"{_code(str(region.get('child_machine', '')))} inside "
            f"{_codes(sorted(str(state) for state in region.get('parent_states', [])))}"
        )
    lines += ["", "### Diagram", "", "```mermaid", render_mermaid(chart, machines=(key,)).rstrip("\n"), "```", ""]
    lines += ["### Transitions", ""]
    header = ["id", "from", "to", "guard predicate(s)", "evidence class(es)", "gate"]
    lines += _table(header, _transition_rows(machine, gates))
    lines.append("")
    if key == "outer":
        projection = machine.get("projection", {})
        rows = [
            [
                _code(state, table=True),
                _code(projection[state], table=True) if isinstance(projection.get(state), str) else _EMPTY,
            ]
            for state in sorted(machine["states"])
        ]
        lines += ["### Projection", "", *_table(["state", "HandoffStatus"], rows), ""]
    return lines


def _region_states(machine: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[str]] | None:
    """The outer nested region and its declared parent states, or None when there is nothing to group."""
    region = machine.get("nested_region")
    if not isinstance(region, Mapping):
        return None
    inner = set(machine["states"]) & {state for state in region.get("parent_states", []) if isinstance(state, str)}
    return (region, inner) if inner else None


def _note_text(text: str) -> str:
    """One Mermaid note line: no newline, and no ``:`` / ``;`` / ``#`` that would end or mangle it."""
    return re.sub(r"\s*[:;#]\s*", ", ", " ".join(text.split())).strip(", ")


def render_mermaid_overview(chart: Mapping[str, Any]) -> str:
    """Outer machine with ``nested_region.parent_states`` grouped in one composite; pure and order independent.

    Edges and aliases match ``render_mermaid(chart, machines=("outer",))``. An edge with both ends in the
    region is drawn inside the composite; every other edge, including ``[*]`` ones, is drawn at top level.

    Raises ``ConformanceError`` with the validation report's errors if any part of the chart is invalid.
    """
    validate_chart(chart).raise_if_failed()
    machine = chart["outer"]
    prefix = machine["id"]
    grouped = _region_states(machine)
    region, inner = grouped if grouped is not None else ({}, set())
    edges = sorted(_machine_edges(machine["transitions"]), key=lambda edge: (edge[1], edge[2], edge[0]))
    lines = ["stateDiagram-v2"]
    lines += [f'    state "{state}" as {prefix}_{state}' for state in sorted(set(machine["states"]) - inner)]
    if grouped is not None:
        region_id = str(region.get("id", ""))
        lines.append(f"    state {region_id} {{")
        lines += [f'        state "{state}" as {prefix}_{state}' for state in sorted(inner)]
        lines += [
            f"        {prefix}_{src} --> {prefix}_{dest}: {tid}" for tid, src, dest in edges if {src, dest} <= inner
        ]
        lines.append("    }")
        child = chart.get("child")
        terminals = sorted(child.get("terminals", [])) if isinstance(child, Mapping) else []
        description = str(region.get("description", "")).split(". ")[0].rstrip(".")
        note = f"{region.get('child_machine', '')} runs here. {description}"
        note = _note_text(note + (f" ({', '.join(terminals)})" if terminals else ""))
        lines.append(f"    note right of {region_id}: {note}")
    lines.append(f"    [*] --> {prefix}_{machine['initial']}")
    lines += [f"    {prefix}_{src} --> {prefix}_{dest}: {tid}" for tid, src, dest in edges if not {src, dest} <= inner]
    lines += [f"    {prefix}_{terminal} --> [*]" for terminal in sorted(machine["terminals"])]
    return "\n".join(lines) + "\n"


def render_markdown(chart: Mapping[str, Any], *, chart_sha256: str) -> str:
    """Render the human-readable statechart doc; pure and input-order independent.

    Raises ``ConformanceError`` with the validation report's errors if any part of the chart is invalid.
    """
    validate_chart(chart).raise_if_failed()
    lines = [
        f"<!-- GENERATED from {CHART_FILENAME} by {GENERATOR_COMMAND}; do not edit -->",
        "# Orchestrator lifecycle statechart",
        "",
    ]
    description = chart.get("description")
    if isinstance(description, str) and description:
        lines += [_cell(description), ""]
    lines += [
        f"- Chart id: {_code(str(chart.get('id', '')))}",
        f"- Version: {_code(str(chart.get('version', '')))}",
        f"- Chart sha256 (file bytes of `{CHART_FILENAME}`): `{chart_sha256}`",
        f"- Generator: `{GENERATOR_COMMAND}`",
        f"- Equivalent: `{MODULE_COMMAND}`",
        "",
        "## Conformance",
        "",
    ]
    lines.append("`validate_chart`: `ok`")
    lines.append("")
    if _region_states(chart["outer"]) is not None:
        lines += ["## Overview", "", "```mermaid", render_mermaid_overview(chart).rstrip("\n"), "```", ""]
    gates = _gates_by_transition(chart)
    for key in MACHINE_KEYS:
        lines += _machine_section(chart, key, gates)
    gate_rows = sorted(
        [
            _code(str(gate.get("id", "")), table=True),
            _cell(str(gate.get("surface", ""))),
            _code(str(gate.get("transition_id", "")), table=True),
        ]
        for gate in chart.get("gates", [])
        if isinstance(gate, Mapping)
    )
    lines += ["## Gates", "", *_table(["id", "surface", "transition"], gate_rows), ""]
    exclusion_rows = sorted(
        [_cell(str(item.get("surface", ""))), _cell(str(item.get("reason", "")))]
        for item in chart.get("gate_exclusions", [])
        if isinstance(item, Mapping)
    )
    lines += ["## Gate exclusions", "", *_table(["surface", "reason"], exclusion_rows)]
    return "\n".join(lines) + "\n"


def _default_out_dir() -> Path | None:
    for parent in Path(__file__).resolve().parents:
        if (parent / DOCS_SUBDIR / ANCHOR_FILENAME).is_file():
            return parent / DOCS_SUBDIR
    return None


def _sweep_stale_temps(path: Path) -> None:
    """Unlink sibling render temps (left by killed runs) whose mtime is over ``_STALE_TEMP_SECONDS`` from now.

    Only ``.<name>.<pid>.<token>.tmp`` and the older ``.<name>.<pid>.tmp`` match, with ASCII digits and lowercase
    hex; a match is unlinked when its mtime is that far in the past or in the future (a clock stepped back, or a
    skewed network FS). Symlinks are judged by their own mtime and unlinked, never followed. A match that is not a
    regular file or a symlink (a directory, FIFO, socket or device) is no temp of ours and is left alone, never
    recursed into or removed; so is one that became a directory between the stat and the unlink. Residual risk:
    after a clock step or skew over the limit a live run's temp can be swept. Temp names are per run, so that cannot
    make two runs share a file; the live run's ``os.replace`` fails, it exits 2, and its target keeps the old bytes.
    """
    pattern = re.compile(rf"\.{re.escape(path.name)}\.[0-9]+(?:\.[0-9a-f]{{{2 * _TEMP_TOKEN_BYTES}}})?\.tmp")
    now = time.time()
    with os.scandir(path.parent) as entries:
        for entry in entries:
            if not pattern.fullmatch(entry.name):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    continue
                info = entry.stat(follow_symlinks=False)
                if not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
                    continue
                if abs(now - info.st_mtime) > _STALE_TEMP_SECONDS:
                    os.unlink(entry.path)
            except (FileNotFoundError, IsADirectoryError):
                pass  # Another run reclaimed it first, or it is a directory since the stat (Linux EISDIR).
            except PermissionError:
                # macOS reports unlink of a directory as EPERM; a real permission failure on a file still raises.
                try:
                    now_dir = stat.S_ISDIR(os.lstat(entry.path).st_mode)
                except FileNotFoundError:
                    continue  # Gone since the unlink failed: another run reclaimed it.
                if not now_dir:
                    raise


def _write_atomic(path: Path, data: bytes) -> None:
    """Write a sibling temp file, then ``os.replace``; on failure the target is untouched and the temp removed.

    The temp name carries this run's pid and a random token, so ``O_EXCL`` (mode ``"xb"``) fails only if another
    run holds the name; that ``FileExistsError`` propagates without touching the file (``--write`` exits 2). The
    temp's fd is owned by its file object from creation, so no bare fd number is ever closed here. Residual risk: an
    interrupt inside ``open`` after it creates the file, or before ``handle`` is bound, leaks the temp (the file
    object closes its own fd); the next run's sweep reclaims it.
    """
    _sweep_stale_temps(path)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(_TEMP_TOKEN_BYTES)}.tmp")
    handle = open(tmp, "xb")  # O_WRONLY | O_CREAT | O_EXCL, mode 0o666 under the umask.
    try:
        with handle:
            handle.write(data)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _render_outputs(chart_file: Path, out_dir: Path) -> dict[Path, bytes]:
    """Return {path: bytes}; raise ValueError for an unreadable or invalid chart."""
    try:
        raw = chart_file.read_bytes()
        chart = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read chart {chart_file}: {exc}") from exc
    if not isinstance(chart, dict):
        raise ValueError(f"chart {chart_file}: root must be a JSON object")
    report = validate_chart(chart)
    if not report.ok:
        raise ValueError(f"chart {chart_file} fails validate_chart: " + "; ".join(report.errors))
    try:
        markdown = render_markdown(chart, chart_sha256=hashlib.sha256(raw).hexdigest())
        mermaid = render_mermaid(chart)
    except (KeyError, TypeError, AttributeError, ConformanceError) as exc:
        raise ValueError(f"chart {chart_file} cannot be rendered: missing or malformed {exc}") from exc
    return {
        out_dir / MARKDOWN_FILENAME: markdown.encode("utf-8"),
        out_dir / MERMAID_FILENAME: mermaid.encode("utf-8"),
    }


def _stale(path: Path, expected: bytes) -> bool:
    try:
        return path.read_bytes() != expected
    except OSError:
        return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="regenerate the .md and .mmd docs")
    mode.add_argument("--check", action="store_true", help="exit 1 if either doc drifts from the chart")
    parser.add_argument("--chart", type=Path, default=None, help="chart JSON (default: shipped chart)")
    parser.add_argument("--out-dir", type=Path, default=None, help="output directory (default: repo docs/specs)")
    args = parser.parse_args(argv)

    out_dir = args.out_dir if args.out_dir is not None else _default_out_dir()
    if out_dir is None:
        anchor = DOCS_SUBDIR / ANCHOR_FILENAME
        print(f"statechart-render: no ancestor of {Path(__file__).resolve()} contains {anchor}", file=sys.stderr)
        return 2
    try:
        outputs = _render_outputs(args.chart if args.chart is not None else chart_path(), out_dir)
    except ValueError as exc:
        print(f"statechart-render: {exc}", file=sys.stderr)
        return 2

    stale = [path for path, expected in outputs.items() if _stale(path, expected)]
    if args.check:
        if stale:
            for path in stale:
                print(f"statechart-render: stale {path}", file=sys.stderr)
            print(f"statechart-render: remedy: {GENERATOR_COMMAND}", file=sys.stderr)
            return 1
        print("statechart-render: generated docs are in sync")
        return 0
    target = out_dir
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        for target in stale:
            _write_atomic(target, outputs[target])
            print(f"statechart-render: wrote {target}")
    except OSError as exc:
        print(f"statechart-render: cannot write {target}: {exc}", file=sys.stderr)
        return 2
    return 0


__all__ = ["main", "render_markdown", "render_mermaid_overview"]


if __name__ == "__main__":
    raise SystemExit(main())
