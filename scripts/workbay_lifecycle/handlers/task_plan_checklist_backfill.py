"""``task-plan-checklist-backfill`` subcommand (internal).

Bulk historical reconciler: discovers task plans for the supplied
``--tasks`` refs (and / or ``--plans`` paths), runs the internal
``sync_task_plan_checklist`` parse/resolve/apply pipeline against each,
and — under ``--apply`` — flips ticks the recorded handoff evidence
supports. Default mode is dry-run.

Collision invariant: when a single ``task_ref`` resolves to more than
one plan file (the internal shape — two unrelated plans both using
``internal``), the bare ``Slice N`` anchor stops being authoritative —
a ``close_slice`` decision recorded for one plan would otherwise tick
the other plan's bare ``Slice N`` box. The backfill sets
``Evidence.suppress_bare_slice_refs`` for that ref so attribution falls
back to plan-specific anchors (file paths, decision ids, commands,
make-targets). The receipt's ``slice_ref_suppressed`` field lists every
ref where this kicked in so operators can tell collision-affected
runs apart from clean ones.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import replace
from pathlib import Path
from typing import Any

from . import _common
from . import sync_task_plan_checklist as sync_handler
from . import task_plan_checklist_audit as audit_handler

_TASK_REF_RE = re.compile(r"^[A-Z][A-Z0-9_-]*$")


def _derive_task_ref_from_filename(name: str) -> str:
    parts = name.split("-")
    if len(parts) >= 2 and parts[0].isupper() and parts[1].isdigit():
        return f"{parts[0]}-{parts[1]}"
    return ""


def _backfill_one_plan(
    workspace_root: Path,
    task_ref: str,
    plan_path: Path,
    source: str,
    apply_changes: bool,
    suppress_bare_slice_refs: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "task_ref": task_ref,
        "plan_path": str(plan_path),
        "plan_source": source,
        "applied": False,
        "ticked": 0,
        "kept": 0,
        "unresolved": 0,
        "already_ticked": 0,
        "stretch_skipped": 0,
        "slice_ref_suppressed": suppress_bare_slice_refs,
        "warnings": [],
    }
    if not plan_path.is_file():
        row["error"] = "plan_not_found"
        return row
    try:
        original_text = plan_path.read_text(encoding="utf-8")
    except OSError as exc:
        row["error"] = f"plan_read_failed: {exc!s}"
        return row

    parsed = sync_handler.parse(original_text)
    evidence, projection, warning = sync_handler._query_handoff_evidence(
        workspace_root, task_ref
    )
    if suppress_bare_slice_refs:
        evidence = replace(evidence, suppress_bare_slice_refs=True)

    resolutions = sync_handler.resolve(parsed, evidence)
    rewritten = sync_handler.apply(original_text, resolutions)
    counts = sync_handler._classify_counts(resolutions)
    diff_entries = sync_handler._diff_preview(original_text, rewritten, resolutions)

    stretch_skipped = 0
    for item in parsed.items:
        if (
            item.section_class == sync_handler.SECTION_STRETCH
            and not item.already_ticked
        ):
            stretch_skipped += 1

    wrote = False
    if apply_changes and rewritten != original_text:
        plan_path.write_text(rewritten, encoding="utf-8")
        wrote = True

    row.update({
        "applied": wrote,
        "ticked": counts["ticked"],
        "kept": counts["kept"],
        "unresolved": counts["unresolved"],
        "already_ticked": counts["already_ticked"],
        "stretch_skipped": stretch_skipped,
        "diff": diff_entries,
        "handoff_projection": projection,
    })
    if warning:
        row["warnings"].append(warning)
    return row


def _normalize_plan_path(workspace_root: Path, raw: str) -> Path:
    plan = Path(raw)
    if not plan.is_absolute():
        plan = workspace_root / plan
    return plan


def _has_existing_plan_collision(workspace_root: Path, plan_paths: list[str]) -> bool:
    """Return True when more than one discovered plan actually exists.

    Completed tasks may still have an active-row ``task_plan_abs_path``
    pointing at a deleted linked worktree plus a glob-discovered copy on
    main. That is stale-path fallout, not a true multi-plan collision,
    and it must not suppress bare ``Slice N`` anchors for the surviving
    plan file.
    """
    existing: set[str] = set()
    for raw in plan_paths:
        path = _normalize_plan_path(workspace_root, raw)
        if not path.is_file():
            continue
        try:
            existing.add(str(path.resolve()))
        except OSError:
            existing.add(str(path))
    return len(existing) > 1


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle task-plan-checklist-backfill", add_help=True
    )
    parser.add_argument(
        "--tasks", dest="tasks", action="append", default=None,
        help=(
            "One or more task refs (space or comma separated). Each ref's "
            "plan paths are discovered via active / archive lookups plus a "
            "filesystem glob (so colliding refs surface both plans)."
        ),
    )
    parser.add_argument(
        "--plans", dest="plans", action="append", default=None,
        help=(
            "Explicit plan paths (space or comma separated). Skips discovery; "
            "task ref is derived from filename."
        ),
    )
    parser.add_argument(
        "--apply", dest="apply_changes", action="store_true", default=False,
        help="Rewrite plans in place. Without this flag the run is dry-run.",
    )
    parser.add_argument(
        "--json", dest="emit_json", action="store_true", default=False,
        help="Reserved for parity; this command always emits JSON.",
    )
    args = parser.parse_args(argv)

    workspace_root = sync_handler.resolve_workspace_root()

    tasks = audit_handler._split_csv(args.tasks)
    plans = audit_handler._split_csv(args.plans)

    rows: list[dict[str, Any]] = []
    suppressed_refs: list[str] = []

    for task_ref in tasks:
        ref = task_ref.strip().upper()
        if not _TASK_REF_RE.match(ref):
            rows.append({
                "task_ref": task_ref,
                "plan_path": None,
                "plan_source": "invalid",
                "error": "task_ref_invalid",
                "applied": False,
                "ticked": 0,
                "kept": 0,
                "unresolved": 0,
                "already_ticked": 0,
                "stretch_skipped": 0,
                "slice_ref_suppressed": False,
                "warnings": [],
            })
            continue
        discovered, sources = audit_handler.discover_plans_for_task(
            workspace_root, ref
        )
        if not discovered:
            rows.append({
                "task_ref": ref,
                "plan_path": None,
                "plan_source": "unresolved",
                "error": "plan_unresolved",
                "applied": False,
                "ticked": 0,
                "kept": 0,
                "unresolved": 0,
                "already_ticked": 0,
                "stretch_skipped": 0,
                "slice_ref_suppressed": False,
                "warnings": [],
            })
            continue
        # Collision invariant: >1 existing plans for the same task_ref
        # means a bare ``Slice N`` anchor is ambiguous across plans.
        # Missing stale paths from archived/deleted worktrees do not
        # create ambiguity for the one surviving plan file.
        collision = _has_existing_plan_collision(workspace_root, discovered)
        if collision and ref not in suppressed_refs:
            suppressed_refs.append(ref)
        for path, source in zip(discovered, sources):
            rows.append(_backfill_one_plan(
                workspace_root,
                ref,
                _normalize_plan_path(workspace_root, path),
                source,
                args.apply_changes,
                suppress_bare_slice_refs=collision,
            ))

    for raw_plan in plans:
        plan_path = _normalize_plan_path(workspace_root, raw_plan)
        derived = _derive_task_ref_from_filename(plan_path.name)
        explicit_collision = False
        if derived:
            explicit_collision = (
                len(audit_handler._discover_plan_paths_by_glob(workspace_root, derived))
                > 1
            )
            if explicit_collision and derived not in suppressed_refs:
                suppressed_refs.append(derived)
        rows.append(_backfill_one_plan(
            workspace_root,
            derived or "UNKNOWN",
            plan_path,
            "flag",
            args.apply_changes,
            suppress_bare_slice_refs=explicit_collision,
        ))

    receipt: dict[str, Any] = {
        "ok": True,
        "command": "task-plan-checklist-backfill",
        "workspace_root": str(workspace_root),
        "dry_run": not args.apply_changes,
        "slice_ref_suppressed": suppressed_refs,
        "rows": rows,
        "totals": {
            "rows": len(rows),
            "applied": sum(1 for r in rows if r.get("applied")),
            "ticked": sum(int(r.get("ticked", 0)) for r in rows),
            "unresolved": sum(int(r.get("unresolved", 0)) for r in rows),
            "already_ticked": sum(int(r.get("already_ticked", 0)) for r in rows),
        },
    }
    _common.emit(receipt)
    return 0
