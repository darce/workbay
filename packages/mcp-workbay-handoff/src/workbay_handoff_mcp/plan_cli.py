"""``python3 -m workbay_handoff_mcp.plan_cli`` — plan-target CLI.

Thin entrypoint that backs ``Makefile.d/plans.mk`` recipes:

    plan-show:
        $(WORKBAY_HANDOFF_PLAN_CLI) show --task $(TASK)

    The launcher token (``WORKBAY_HANDOFF_PLAN_CLI``) defaults to
``python3 -m workbay_handoff_mcp.plan_cli``, resolved from the handoff
package that ``workbay-bootstrap`` installs alongside the MCP server
(git-only delivery — no PyPI ``uvx`` / ``pip install`` step). The recipe
shells through here, never recreating the resolver logic.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

from .config import RuntimeConfig
from .handoff_state import get_handoff_state
from .plan_resolve import (
    PlanPathNotRegistered,
    PlanRegistrationError,
    discover_plan_path_candidates,
    list_active_task_locations,
    plan_show_command,
    register_plan_path,
    resolve_plan_location,
)
from .runtime import configure_runtime


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_handoff_mcp.plan_cli",
        description="MCP-resolved task plan tooling (plan-show, plan-edit, plans-list).",
    )
    parser.add_argument(
        "--workspace-root",
        help="Override WORKBAY_HANDOFF_WORKSPACE_ROOT for one invocation.",
    )
    parser.add_argument("--state-dir")
    parser.add_argument("--current-task-path")
    parser.add_argument("--dashboard-path")
    sub = parser.add_subparsers(dest="subcommand", required=True)

    show = sub.add_parser(
        "show",
        help="Print the resolved plan via `git show <branch>:<path>`.",
    )
    show.add_argument(
        "--task",
        dest="task_ref",
        default=None,
        help="Task ref to resolve. Defaults to the active task.",
    )
    # internal: mutually-exclusive prefer-mode flags. `--auto`
    # is the default and matches resolve_plan_location's `prefer="auto"`
    # so make plan-show keeps doing the right thing on coordinator
    # checkouts. `--baseline` forces the main view; `--working-copy`
    # forces the feature-branch view.
    show_prefer = show.add_mutually_exclusive_group()
    show_prefer.add_argument(
        "--baseline",
        dest="prefer",
        action="store_const",
        const="baseline",
        help="Read the plan from main (post-acceptance view).",
    )
    show_prefer.add_argument(
        "--working-copy",
        dest="prefer",
        action="store_const",
        const="working_copy",
        help="Read the plan from the task's target_branch (pre-acceptance view).",
    )
    show_prefer.add_argument(
        "--auto",
        dest="prefer",
        action="store_const",
        const="auto",
        help="Default: read from main when accepted, else from target_branch.",
    )
    show.set_defaults(prefer="auto")

    edit = sub.add_parser(
        "edit",
        help="Open the resolved plan in $EDITOR against target_worktree_path.",
    )
    edit.add_argument(
        "--task",
        dest="task_ref",
        default=None,
        help="Task ref to resolve. Defaults to the active task.",
    )

    plans_list = sub.add_parser(
        "list",
        help="Print every active task's plan location, one block per task.",
    )
    plans_list.add_argument(
        "--include-unset-path",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Include rows with no task_plan_path; emit a warning line for each. "
            "Pass --no-include-unset-path to suppress unset rows."
        ),
    )

    register = sub.add_parser(
        "register",
        help="Persist task_plan_path on the active row (internal).",
    )
    register.add_argument(
        "--task",
        dest="task_ref",
        required=True,
        help="Task ref whose active row should receive the plan path.",
    )
    register.add_argument(
        "--plan",
        dest="plan_path",
        default=None,
        help=(
            "Workspace-relative plan path to register. When omitted, the "
            "CLI globs docs/**/*<task-id>*.md and registers the unique "
            "match; multiple matches fail with a disambiguation hint."
        ),
    )
    return parser


def _cmd_show(task_ref: str | None, prefer: str = "auto") -> int:
    try:
        location = resolve_plan_location(task_ref=task_ref, prefer=prefer)  # type: ignore[arg-type]
    except PlanPathNotRegistered as exc:
        print(f"plan-show: {exc}", file=sys.stderr)
        return 2
    if not location.exists_on_branch:
        print(
            "plan-show: plan not committed on target branch — "
            f"branch={location.branch!r}, path={location.path!r}. "
            "Fetch the branch (`git fetch origin <branch>`) or commit the "
            "plan on it before retrying.",
            file=sys.stderr,
        )
        return 3
    proc = subprocess.run(
        plan_show_command(branch=location.branch or "", path=location.path or ""),
        check=False,
    )
    return proc.returncode


def _cmd_edit(task_ref: str | None) -> int:
    """Open the resolved plan in ``$EDITOR`` against the linked worktree.

    Reads ``task_plan_abs_path`` and ``target_worktree_path`` from the
    enriched identity envelope. Fails non-zero with an actionable
    pointer at ``make task-start`` when ``target_worktree_path`` is
    unset or absent on disk — the operator's worktree is the editing
    surface; we never fall back to the workspace root.
    """
    envelope = get_handoff_state(task_ref=task_ref, sections="identity")
    data = envelope.get("data") if isinstance(envelope, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if not isinstance(active, dict):
        print(
            f"plan-edit: no active handoff state for task_ref={task_ref!r}.",
            file=sys.stderr,
        )
        return 2

    plan_path = active.get("task_plan_path")
    if not isinstance(plan_path, str) or not plan_path.strip():
        print(
            f"plan-edit: task_plan_path is unset for task_ref={active.get('task_ref')!r}. "
            "Set it via set_handoff_state(task_plan_path='docs/plans/...').",
            file=sys.stderr,
        )
        return 2

    worktree = active.get("target_worktree_path")
    if not isinstance(worktree, str) or not worktree.strip():
        print(
            f"plan-edit: target_worktree_path is unset for task_ref={active.get('task_ref')!r}. "
            "Run `make task-start TASK=<ref> OBJECTIVE=...` to create the linked worktree.",
            file=sys.stderr,
        )
        return 3
    if not Path(worktree).is_dir():
        print(
            f"plan-edit: target_worktree_path does not exist on disk: {worktree!r}. "
            "Run `make task-start TASK=<ref> OBJECTIVE=...` to recreate the linked worktree.",
            file=sys.stderr,
        )
        return 3

    abs_path = active.get("task_plan_abs_path")
    if not isinstance(abs_path, str) or not abs_path.strip():
        print(
            f"plan-edit: could not resolve absolute plan path for task_ref={active.get('task_ref')!r}.",
            file=sys.stderr,
        )
        return 2

    # $EDITOR may carry shell-style flags (e.g. "code --wait", "subl -w").
    # shlex.split lets those reach the editor instead of being treated as
    # part of the executable name. BR-internal.
    editor_argv = shlex.split(os.environ.get("EDITOR") or "vi")
    if not editor_argv:
        editor_argv = ["vi"]
    proc = subprocess.run([*editor_argv, abs_path], check=False)
    return proc.returncode


def _cmd_list(include_unset_path: bool) -> int:
    """Print every active task's plan location, one block per task.

    Iterates ``list_active_task_locations`` and emits one block per
    ``PlanLocation``. Rows with no ``task_plan_path`` get a single
    ``WARNING:`` line naming the missing field; the loop continues so
    one unset row never hides the rest.
    """
    locations = list_active_task_locations(include_unset_path=include_unset_path)
    for loc in locations:
        path_field = loc.path if loc.path is not None else "<unset>"
        print(
            f"=== task_ref={loc.task_ref} branch={loc.branch or '<unset>'} "
            f"path={path_field} exists={'true' if loc.exists_on_branch else 'false'} ==="
        )
        if loc.path is None:
            print(
                f"WARNING: task_plan_path is unset for {loc.task_ref}; "
                "set it via set_handoff_state(task_plan_path='docs/plans/...')."
            )
    return 0


def _cmd_register(task_ref: str, plan_path: str | None) -> int:
    """Resolve a plan path (explicit or via glob) and persist it on the row.

    implementation note contract: never silently leaves ``task_plan_path`` unset.
    Explicit ``--plan`` always wins. When omitted the CLI globs
    ``docs/**/*<task-id>*.md`` and refuses to write on zero or multiple
    matches, naming the candidates so the operator can pass ``--plan``
    explicitly on the retry.
    """
    resolved = plan_path
    if resolved is None:
        candidates = discover_plan_path_candidates(task_ref)
        if not candidates:
            print(
                f"plan-register: no docs/**/*{task_ref.lower()}*.md candidates found; pass --plan <path> explicitly.",
                file=sys.stderr,
            )
            return 2
        if len(candidates) > 1:
            joined = "\n  ".join(candidates)
            print(
                f"plan-register: multiple plan candidates for {task_ref!r}; "
                f"pass --plan <path> to disambiguate:\n  {joined}",
                file=sys.stderr,
            )
            return 2
        resolved = candidates[0]
    try:
        register_plan_path(task_ref=task_ref, plan_path=resolved)
    except PlanRegistrationError as exc:
        print(f"plan-register: {exc}", file=sys.stderr)
        return 2
    print(f"plan-register: task_ref={task_ref} task_plan_path={resolved}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_runtime(RuntimeConfig.from_args(args))
    if args.subcommand == "show":
        return _cmd_show(args.task_ref, getattr(args, "prefer", "auto"))
    if args.subcommand == "edit":
        return _cmd_edit(args.task_ref)
    if args.subcommand == "list":
        return _cmd_list(args.include_unset_path)
    if args.subcommand == "register":
        return _cmd_register(args.task_ref, args.plan_path)
    parser.error(f"Unknown subcommand: {args.subcommand}")


if __name__ == "__main__":
    sys.exit(main())
