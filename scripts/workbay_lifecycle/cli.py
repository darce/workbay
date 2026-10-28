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
import os
import sys
from collections.abc import Sequence
from pathlib import Path

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
    "handoff-set",
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


# Exit codes for codemap-reindex ([AGT-21]): distinct nonzero for distinct outcomes.
_CODEMAP_EXIT_OK = 0
_CODEMAP_EXIT_FAILED = 1
_CODEMAP_EXIT_USAGE = 2
_CODEMAP_EXIT_HELD = 3
_CODEMAP_EXIT_PENDING_REMAINING = 4

# Transient failures that retain the queue may schedule one follow-up spawn (RB-02).
_CODEMAP_FOLLOWUP_STATUSES = frozenset({"timeout", "failed", "pending_remaining"})
_CODEMAP_FOLLOWUP_ENV = "WORKBAY_CODEMAP_REINDEX_FOLLOWUP"
# Shared with task_finish spawn debounce (RB-07): held for the life of this process.
_CODEMAP_SPAWN_GATE_NAME = "codemap-reindex.spawn.lock"


def _hold_codemap_spawn_gate(db_path: Path) -> object | None:
    """Hold the spawn-debounce flock for the lifetime of this runner process.

    task_finish uses LOCK_NB on the same path; while we hold LOCK_EX, concurrent
    finishes skip Popen (request alone is enough). Kernel releases on exit.
    """
    import fcntl  # noqa: PLC0415

    state_dir = db_path.parent
    gate_path = state_dir / _CODEMAP_SPAWN_GATE_NAME
    try:
        gate_fd = open(gate_path, "a+", encoding="utf-8")  # noqa: SIM115
        try:
            fcntl.flock(gate_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Another runner already holds it; proceed without the gate.
            gate_fd.close()
            return None
        return gate_fd
    except OSError:
        return None


def _codemap_exit_for_status(status: str) -> int:
    """Map runner status → process exit code. Only ok/empty are exit 0."""
    if status in {"ok", "empty"}:
        return _CODEMAP_EXIT_OK
    if status == "held":
        return _CODEMAP_EXIT_HELD
    if status == "pending_remaining":
        return _CODEMAP_EXIT_PENDING_REMAINING
    return _CODEMAP_EXIT_FAILED


def _maybe_spawn_codemap_followup(
    *,
    repo_path: Path,
    db_path: Path,
    repo_instance_id: str,
    status: str,
) -> bool:
    """RB-02: one-shot detached follow-up when a terminal failure retains work.

    Bounded by env ``WORKBAY_CODEMAP_REINDEX_FOLLOWUP`` so a failing chain cannot
    fork forever. Returns True when a child was spawned.
    """
    if status not in _CODEMAP_FOLLOWUP_STATUSES:
        return False
    if os.environ.get(_CODEMAP_FOLLOWUP_ENV) == "1":
        return False
    lifecycle_pkg = Path(__file__).resolve().parent
    env = os.environ.copy()
    env[_CODEMAP_FOLLOWUP_ENV] = "1"
    log_path = db_path.parent / "codemap-reindex.log"
    task_finish._rotate_codemap_reindex_log(log_path)
    try:
        # Late import keeps the optional surface lightweight.
        import subprocess  # noqa: PLC0415

        with open(log_path, "a", encoding="utf-8") as log_f:
            subprocess.Popen(
                [
                    sys.executable,
                    str(lifecycle_pkg),
                    "codemap-reindex",
                    "--repo-instance-id",
                    repo_instance_id,
                    "--db-path",
                    str(db_path),
                    "--repo-path",
                    str(repo_path),
                ],
                cwd=str(repo_path),
                stdin=subprocess.DEVNULL,
                stdout=log_f,
                stderr=log_f,
                start_new_session=True,
                env=env,
            )
        return True
    except OSError as exc:
        sys.stderr.write(f"codemap-reindex: followup spawn failed: {exc}\n")
        return False


def _run_codemap_reindex(argv: Sequence[str]) -> int:
    """implementation note S2: detached-runner entry + operator escape hatch.

    Exit contract ([AGT-21]):
      0  ok / empty — indexed or nothing to do
      1  failed / timeout / fenced / cli_missing
      2  usage / import / missing db
      3  held — another live process owns the lease (not "indexed now")
      4  pending_remaining — drain budget exhausted with queue non-empty
    """
    parser = argparse.ArgumentParser(prog="lifecycle codemap-reindex", add_help=True)
    parser.add_argument("--repo-path", dest="repo_path", default="")
    parser.add_argument("--db-path", dest="db_path", default="")
    parser.add_argument("--repo-instance-id", dest="repo_instance_id", default="")
    parser.add_argument(
        "--sha",
        dest="sha",
        default="",
        help="Optional SHA to queue before running (operator escape hatch).",
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(list(argv))

    repo = resolver.repo_root()
    workspace = resolver.canonical_workspace_root(repo) if repo is not None else None
    if workspace is None and repo is not None:
        workspace = repo
    repo_path = Path(args.repo_path).resolve() if args.repo_path else workspace
    if repo_path is None:
        sys.stderr.write("codemap-reindex: repo path required (not in a git repo)\n")
        return _CODEMAP_EXIT_USAGE

    if args.db_path:
        db_path = Path(args.db_path).resolve()
    else:
        root = workspace or repo_path
        db_path = root / ".task-state" / "handoff.db"
    if not db_path.is_file():
        sys.stderr.write(f"codemap-reindex: handoff.db missing at {db_path}\n")
        return _CODEMAP_EXIT_USAGE

    try:
        from workbay_handoff_mcp.codemap_runner import run_reindex_once  # noqa: PLC0415
        from workbay_handoff_mcp.codemap_lease import (  # noqa: PLC0415
            request_reindex,
            resolve_repo_instance_id,
        )
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"codemap-reindex: import failed: {exc}\n")
        return _CODEMAP_EXIT_USAGE

    # RB-06: resolve against the SAME db_path used for request/run — never a
    # hard-coded workspace handoff.db that can diverge from --db-path.
    explicit_id = (args.repo_instance_id or "").strip()
    resolve_repo = workspace or repo_path
    try:
        resolved_id = resolve_repo_instance_id(db_path, repo_path=resolve_repo)
    except Exception as exc:  # noqa: BLE001
        # An explicit id can still run when resolution is impossible (e.g. no
        # resolvable git common dir); only the auto path is fatal here.
        if not explicit_id:
            sys.stderr.write(f"codemap-reindex: repo_instance_id resolve failed: {exc}\n")
            return _CODEMAP_EXIT_USAGE
        resolved_id = ""
    if explicit_id:
        if resolved_id and explicit_id != resolved_id:
            sys.stderr.write(
                f"codemap-reindex: --repo-instance-id {explicit_id!r} does not match the "
                f"instance resolved for repo_path {repo_path} ({resolved_id!r}); refusing to run\n"
            )
            return _CODEMAP_EXIT_USAGE
        repo_instance_id = explicit_id
    else:
        repo_instance_id = resolved_id

    sha = (args.sha or "").strip()
    if sha:
        try:
            request_reindex(db_path, repo_instance_id=repo_instance_id, sha=sha)
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"codemap-reindex: request failed: {exc}\n")
            return _CODEMAP_EXIT_USAGE

    # RB-07: hold spawn gate so concurrent task-finish callers skip Popen.
    # Keep the fd open until process exit (kernel releases flock).
    _spawn_gate = _hold_codemap_spawn_gate(db_path)

    # Frozen contract: no holder_pid / pid_alive / terminate on the runner.
    result = run_reindex_once(
        db_path,
        repo_instance_id=repo_instance_id,
        repo_path=repo_path,
    )
    _ = _spawn_gate  # retain until exit
    payload = result.to_dict()
    # ok means "index is current for this invocation" — only ok/empty ([AGT-21]).
    # held is deferred work, pending_remaining is partial drain: both ok=False.
    payload["ok"] = result.status in {"ok", "empty"}
    payload["command"] = "codemap-reindex"

    followup = _maybe_spawn_codemap_followup(
        repo_path=repo_path,
        db_path=db_path,
        repo_instance_id=repo_instance_id,
        status=result.status,
    )
    if followup:
        payload["followup_spawned"] = True
        if not args.emit_json:
            sys.stderr.write(
                f"codemap-reindex: status={result.status}; scheduled one follow-up runner\n"
            )
    elif result.status not in {"ok", "empty"}:
        if not args.emit_json:
            sys.stderr.write(
                f"codemap-reindex: status={result.status}"
                + (f" detail={result.detail}" if result.detail else "")
                + "\n"
            )

    # Always emit a machine-readable receipt (operator + detached runner).
    json.dump(payload, sys.stdout)
    sys.stdout.write("\n")
    return _codemap_exit_for_status(result.status)


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

    if command == "codemap-reindex":
        return _run_codemap_reindex(rest)

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
