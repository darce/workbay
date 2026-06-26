"""Backfill ``task_plan_path`` on live MCP rows (internal).

Walks every active handoff row, skips rows that already have
``task_plan_path`` set, globs ``docs/**/*<task-id>*.md`` for the
remainder, and registers the unique match via
``register_plan_path``. Multi-match rows are reported and left untouched
so the operator can disambiguate; an explicit ``--task TASK=path``
override always wins over the glob.

Idempotent by construction: re-running on a fully populated DB is a
no-op because already-set rows are skipped before the glob runs.

Designed to be invoked via
``uvx --from mcp-workbay-handoff python -m workbay_handoff_mcp.scripts.backfill_plan_paths``
so consumers do not need a prior ``pip install`` step.
"""

from __future__ import annotations

import argparse
import sys

from ..plan_resolve import (
    PlanRegistrationError,
    discover_plan_path_candidates,
    list_active_task_locations,
    register_plan_path,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_handoff_mcp.scripts.backfill_plan_paths",
        description="Populate task_plan_path on active rows by globbing docs/**/*<task-id>*.md.",
    )
    parser.add_argument(
        "--task",
        action="append",
        default=[],
        metavar="TASK_REF=path",
        help=(
            "Operator override for a specific task. Repeatable. The "
            "explicit path wins over the glob even when the glob would "
            "have produced a unique match."
        ),
    )
    return parser


def _parse_overrides(raw_overrides: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for entry in raw_overrides:
        if "=" not in entry:
            raise SystemExit(f"backfill_plan_paths: --task expects TASK_REF=path, got {entry!r}")
        task_ref, _, plan_path = entry.partition("=")
        task_ref = task_ref.strip()
        plan_path = plan_path.strip()
        if not task_ref or not plan_path:
            raise SystemExit(f"backfill_plan_paths: --task entry {entry!r} has empty task_ref or path")
        overrides[task_ref] = plan_path
    return overrides


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    overrides = _parse_overrides(args.task)

    locations = list_active_task_locations(include_unset_path=True)
    populated = 0
    skipped_multi = 0
    skipped_zero = 0

    for loc in locations:
        if loc.path is not None and loc.task_ref not in overrides:
            continue  # already set; idempotent skip

        if loc.task_ref in overrides:
            chosen = overrides[loc.task_ref]
        else:
            candidates = discover_plan_path_candidates(loc.task_ref)
            if not candidates:
                print(
                    f"backfill_plan_paths: no docs/**/*{loc.task_ref.lower()}*.md candidates "
                    f"for {loc.task_ref}; skipping (pass --task {loc.task_ref}=<path> to set explicitly).",
                    file=sys.stderr,
                )
                skipped_zero += 1
                continue
            if len(candidates) > 1:
                joined = "\n  ".join(candidates)
                print(
                    f"backfill_plan_paths: multiple plan candidates for {loc.task_ref}; "
                    f"skipping (pass --task {loc.task_ref}=<path> to disambiguate):\n  {joined}",
                    file=sys.stderr,
                )
                skipped_multi += 1
                continue
            chosen = candidates[0]

        try:
            register_plan_path(task_ref=loc.task_ref, plan_path=chosen)
        except PlanRegistrationError as exc:
            print(
                f"backfill_plan_paths: register_plan_path failed for {loc.task_ref}: {exc}",
                file=sys.stderr,
            )
            continue
        populated += 1

    print(f"backfill_plan_paths: populated={populated} skipped_multi={skipped_multi} skipped_zero={skipped_zero}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
