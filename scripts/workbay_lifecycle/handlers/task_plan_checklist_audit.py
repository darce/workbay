"""``task-plan-checklist-audit`` subcommand (internal).

Read-only inspection of task-plan `- [ ]` / `- [x]` state against
recorded handoff evidence. Reuses the internal ``sync_task_plan_checklist``
parse/resolve layers but never writes — the receipt carries one row
per discovered plan file with the counts an operator needs to decide
whether a historical backfill is worth running.

Plan discovery for a task_ref:

1. ``handoff state --sections identity`` lookup of the active row's
   ``task_plan_abs_path`` / ``task_plan_path``.
2. Archived snapshot lookup of the same fields (archived tasks are
   in scope — the internal..68 historical set is largely archived).
3. Filesystem glob ``<workspace_root>/packages/*/docs/tasks/<TASK_REF>-*-task-plan.md``
   so colliding task_refs (e.g. internal in two packages) surface as
   independent rows instead of collapsing onto the first DB hit.

Explicit ``--plans=<path>`` invocations skip discovery entirely.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from . import _common
from . import sync_task_plan_checklist as sync_handler

_TASK_REF_RE = re.compile(r"^[A-Z][A-Z0-9_-]*$")


def _split_csv(values: list[str] | None) -> list[str]:
    """Split ``--tasks A,B C`` style argv into a flat ordered list."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        for token in re.split(r"[\s,]+", raw):
            token = token.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
    return out


def _discover_active_plan_path(
    workspace_root: Path, task_ref: str
) -> str | None:
    """Best-effort active-state lookup. ``None`` on any failure."""
    return sync_handler._lookup_stored_plan_path(workspace_root, task_ref)


def _discover_archived_plan_path(
    workspace_root: Path, task_ref: str
) -> str | None:
    """Try to read ``task_plan_path`` from an archived snapshot.

    Falls back through whatever shape the ``archive get`` CLI returns
    (``task_plan_abs_path`` preferred, ``task_plan_path`` fallback).
    Returns ``None`` on any failure so callers can degrade to glob
    discovery.
    """
    handoff_root = sync_handler.resolve_handoff_workspace_root(workspace_root)
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(handoff_root),
        "archive",
        "--operation", "get",
        "--task-ref", task_ref,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return None
    for key in ("task_plan_abs_path", "task_plan_path"):
        value = data.get(key)
        if isinstance(value, str) and value:
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = workspace_root / candidate
            return str(candidate)
    return None


def _discover_plan_paths_by_glob(
    workspace_root: Path, task_ref: str
) -> list[str]:
    """Filesystem fallback: glob the canonical task-plan layout.

    Matches ``packages/*/docs/tasks/<TASK_REF>-*-task-plan.md`` so two
    plans sharing one task_ref (the internal collision) both appear.
    Returned paths are absolute. Order is stable (sorted by path).
    """
    matches: set[str] = set()
    glob_pattern = f"packages/*/docs/tasks/{task_ref}-*-task-plan.md"
    for plan in workspace_root.glob(glob_pattern):
        if plan.is_file():
            matches.add(str(plan.resolve()))
    return sorted(matches)


def discover_plans_for_task(
    workspace_root: Path, task_ref: str
) -> tuple[list[str], list[str]]:
    """Return ``(plan_paths, sources)`` for a single task_ref.

    ``sources`` is a parallel list whose entries are
    ``active`` / ``archive`` / ``glob``. The glob pass always runs so
    colliding refs surface even when the DB only knows about one of
    the two plan files.
    """
    plans: list[str] = []
    sources: list[str] = []
    seen: set[str] = set()

    active = _discover_active_plan_path(workspace_root, task_ref)
    if active and active not in seen:
        plans.append(active)
        sources.append("active")
        seen.add(active)

    archived = _discover_archived_plan_path(workspace_root, task_ref)
    if archived and archived not in seen:
        plans.append(archived)
        sources.append("archive")
        seen.add(archived)

    for path in _discover_plan_paths_by_glob(workspace_root, task_ref):
        if path in seen:
            continue
        plans.append(path)
        sources.append("glob")
        seen.add(path)

    return plans, sources


def _normalize_plan_path(workspace_root: Path, raw: str) -> Path:
    plan = Path(raw)
    if not plan.is_absolute():
        plan = workspace_root / plan
    return plan


def _audit_one_plan(
    workspace_root: Path,
    task_ref: str,
    plan_path: Path,
    source: str,
) -> dict[str, Any]:
    """Run parse / resolve against a single plan and return the row.

    No writes: the apply layer is never called. The row shape is the
    audit contract surface — callers (Makefile target, downstream tests)
    rely on the keys being stable.
    """
    row: dict[str, Any] = {
        "task_ref": task_ref,
        "plan_path": str(plan_path),
        "plan_source": source,
        "already_ticked": 0,
        "tick_candidates": 0,
        "kept": 0,
        "unresolved": 0,
        "stretch_skipped": 0,
        "warnings": [],
    }
    if not plan_path.is_file():
        row["error"] = "plan_not_found"
        return row
    try:
        text = plan_path.read_text(encoding="utf-8")
    except OSError as exc:
        row["error"] = f"plan_read_failed: {exc!s}"
        return row

    parsed = sync_handler.parse(text)
    evidence, projection, warning = sync_handler._query_handoff_evidence(
        workspace_root, task_ref
    )
    resolutions = sync_handler.resolve(parsed, evidence)

    already_ticked = 0
    tick_candidates = 0
    kept = 0
    unresolved = 0
    stretch_skipped = 0
    for item in parsed.items:
        resolution = resolutions.get(item.line_index)
        if resolution is None:
            continue
        if item.section_class == sync_handler.SECTION_STRETCH:
            if not item.already_ticked:
                stretch_skipped += 1
            else:
                already_ticked += 1
            continue
        if resolution.action == sync_handler.RESOLUTION_ALREADY_TICKED:
            already_ticked += 1
        elif resolution.action == sync_handler.RESOLUTION_TICK:
            tick_candidates += 1
        elif resolution.action == sync_handler.RESOLUTION_UNRESOLVED:
            unresolved += 1
        else:
            kept += 1

    row["already_ticked"] = already_ticked
    row["tick_candidates"] = tick_candidates
    row["kept"] = kept
    row["unresolved"] = unresolved
    row["stretch_skipped"] = stretch_skipped
    row["handoff_projection"] = projection
    if warning:
        row["warnings"].append(warning)
    return row


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle task-plan-checklist-audit", add_help=True
    )
    parser.add_argument(
        "--tasks", dest="tasks", action="append", default=None,
        help=(
            "One or more task refs (space or comma separated). Each ref's "
            "stored plan path is resolved from active and archived state, "
            "with a filesystem glob fallback so colliding refs surface as "
            "independent rows."
        ),
    )
    parser.add_argument(
        "--plans", dest="plans", action="append", default=None,
        help=(
            "Explicit plan paths (space or comma separated). Skips "
            "discovery; each path is audited as-is. Task ref is derived "
            "from the filename when not paired with --tasks."
        ),
    )
    parser.add_argument(
        "--json", dest="emit_json", action="store_true", default=False,
        help="Reserved for parity with other lifecycle handlers; this "
        "command always emits JSON.",
    )
    args = parser.parse_args(argv)

    workspace_root = sync_handler.resolve_workspace_root()

    tasks = _split_csv(args.tasks)
    plans = _split_csv(args.plans)

    rows: list[dict[str, Any]] = []

    # Pre-compute task -> sources from --tasks discovery so we can dedupe
    # against explicit --plans entries.
    for task_ref in tasks:
        ref = task_ref.strip().upper()
        if not _TASK_REF_RE.match(ref):
            rows.append({
                "task_ref": task_ref,
                "plan_path": None,
                "plan_source": "invalid",
                "error": "task_ref_invalid",
                "already_ticked": 0,
                "tick_candidates": 0,
                "kept": 0,
                "unresolved": 0,
                "stretch_skipped": 0,
                "warnings": [],
            })
            continue
        discovered, sources = discover_plans_for_task(workspace_root, ref)
        if not discovered:
            rows.append({
                "task_ref": ref,
                "plan_path": None,
                "plan_source": "unresolved",
                "error": "plan_unresolved",
                "already_ticked": 0,
                "tick_candidates": 0,
                "kept": 0,
                "unresolved": 0,
                "stretch_skipped": 0,
                "warnings": [],
            })
            continue
        for path, source in zip(discovered, sources):
            rows.append(_audit_one_plan(
                workspace_root, ref, _normalize_plan_path(workspace_root, path), source
            ))

    for raw_plan in plans:
        plan_path = _normalize_plan_path(workspace_root, raw_plan)
        # Derive task_ref from filename: internal-... -> internal.
        # Falls back to "" when the filename does not match.
        derived = ""
        m = re.match(r"^([A-Z][A-Z0-9-]*?)-", plan_path.name)
        if m:
            candidate = m.group(1)
            # Keep extending tokens as long as the next segment is a
            # number, so "internal" wins over just "internal".
            parts = plan_path.name.split("-")
            if len(parts) >= 2 and parts[0].isupper() and parts[1].isdigit():
                derived = f"{parts[0]}-{parts[1]}"
            else:
                derived = candidate
        rows.append(_audit_one_plan(
            workspace_root,
            derived or "UNKNOWN",
            plan_path,
            "flag",
        ))

    receipt: dict[str, Any] = {
        "ok": True,
        "command": "task-plan-checklist-audit",
        "workspace_root": str(workspace_root),
        "rows": rows,
        "totals": {
            "rows": len(rows),
            "tick_candidates": sum(int(r.get("tick_candidates", 0)) for r in rows),
            "unresolved": sum(int(r.get("unresolved", 0)) for r in rows),
            "already_ticked": sum(int(r.get("already_ticked", 0)) for r in rows),
        },
    }

    _common.emit(receipt)
    return 0
