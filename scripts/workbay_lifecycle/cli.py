"""Lifecycle runner argparse dispatch (internal).

Three handler categories live here:

* :data:`STUB_HANDLERS` — subcommands whose real bodies land in later
  slices. Their stub emits a visibly failing ``not_implemented``
  receipt and returns exit code 2 so an operator or agent invoking
  them sees explicit failure rather than fake-green behavior.
* :data:`SKILL_BROADCAST_HANDLERS` — ``plan-review`` and ``plan-analyze``
  delegate to in-session skills (no MCP CLI subcommand exists for
  those reviews); the wrappers print structured guidance and emit a
  ``workflow_intent`` event for handoff replay.
* :data:`SHELL_OUT_HANDLERS` — ``review-run``, ``handoff-review-run``,
  and ``handoff-close-check`` shell out to the matching
  ``mcp-workbay-handoff`` subcommand and propagate its exit code.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

import projection_queue
import resolver

from handlers import (
    attest,
    backfill_plan_acceptance,
    close_check,
    context,
    doctor,
    finalize_plan,
    plan_accept,
    plan_done,
    project_events_replay,
    provision_env,
    review_ready,
    shell_out,
    skill_broadcast,
    slice_commit,
    slice_start,
    status,
    sync_task_plan_checklist,
    plan_status,
    task_finish,
    task_reap,
    task_plan_checklist_audit,
    task_plan_checklist_backfill,
    task_start,
    tasks,
)

STUB_HANDLERS: dict[str, str] = {}

SKILL_BROADCAST_HANDLERS: tuple[str, ...] = ("plan-review", "plan-analyze")

SHELL_OUT_HANDLERS: tuple[str, ...] = (
    "review-run",
    "handoff-review-run",
    "handoff-close-check",
)


def _emit_stub(name: str, owning_slice: str) -> int:
    receipt = {
        "ok": False,
        "command": name,
        "status": "not_implemented",
        "owning_slice": owning_slice,
    }
    json.dump(receipt, sys.stdout)
    sys.stdout.write("\n")
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one lifecycle subcommand, converting an operator Ctrl-C into a
    clean exit code 130 with a one-line message instead of letting a raw
    traceback escape through a gate's blocking subprocess call. implementation note C1."""
    try:
        return _dispatch(argv)
    except KeyboardInterrupt:
        sys.stderr.write(
            "\nlifecycle: interrupted (exit 130); if a mutating command was "
            "running, verify task/handoff state before retrying.\n"
        )
        return 130


def _dispatch(argv: Sequence[str] | None = None) -> int:
    raw = list(argv) if argv is not None else sys.argv[1:]
    if not raw:
        sys.stderr.write("usage: lifecycle <subcommand> [options]\n")
        return 2

    command, rest = raw[0], raw[1:]

    from handlers import _common as common_mod  # noqa: PLC0415

    common_mod.maybe_auto_drain_projection_spool(command)
    common_mod.maybe_auto_drain_dead_letter(command)
    repo = resolver.repo_root()
    if repo is not None:
        import session_heartbeat as session_heartbeat_mod  # noqa: PLC0415

        session_heartbeat_mod.touch_heartbeat(repo)
        session_heartbeat_mod.gc_heartbeats(repo)
        common_mod.maybe_auto_reap_stale_rows(command)
        preflight_receipt = projection_queue.projection_preflight(repo, command)
        if preflight_receipt is not None:
            json.dump(preflight_receipt, sys.stdout)
            sys.stdout.write("\n")
            return 2

    if command == "context":
        return context.run(rest)

    if command == "task-start":
        return task_start.run(rest)

    if command == "task-finish":
        return task_finish.run(rest)

    if command == "task-reap":
        return task_reap.run(rest)

    if command == "finalize-plan":
        return finalize_plan.run(rest)

    if command == "slice-start":
        return slice_start.run(rest)

    if command == "provision-env":
        return provision_env.run(rest)

    if command == "slice-commit":
        return slice_commit.run(rest)

    if command == "status":
        return status.run(rest)

    if command == "doctor":
        return doctor.run(rest)

    if command == "tasks":
        return tasks.run(rest)

    if command == "project-events-replay":
        return project_events_replay.run(rest)

    if command == "review-ready":
        return review_ready.run(rest)

    if command == "close-check":
        return close_check.run(rest)

    if command == "sync-task-plan-checklist":
        return sync_task_plan_checklist.run(rest)

    if command == "plan-status":
        return plan_status.run(rest)

    if command == "attest":
        return attest.run(rest)

    if command == "plan-accept":
        return plan_accept.run(rest)

    if command == "plan-done":
        return plan_done.run(rest)

    if command == "plan-accept-backfill":
        return backfill_plan_acceptance.run(rest)

    if command == "task-plan-checklist-audit":
        return task_plan_checklist_audit.run(rest)

    if command == "task-plan-checklist-backfill":
        return task_plan_checklist_backfill.run(rest)

    if command in STUB_HANDLERS:
        # argparse only validates the lone --json flag the stubs accept.
        parser = argparse.ArgumentParser(prog=f"lifecycle {command}", add_help=True)
        parser.add_argument("--json", action="store_true", default=False)
        parser.parse_args(rest)
        return _emit_stub(command, STUB_HANDLERS[command])

    if command in SKILL_BROADCAST_HANDLERS:
        return skill_broadcast.run(command, rest)

    if command in SHELL_OUT_HANDLERS:
        return shell_out.run(command, rest)

    sys.stderr.write(f"unknown subcommand: {command}\n")
    return 2
