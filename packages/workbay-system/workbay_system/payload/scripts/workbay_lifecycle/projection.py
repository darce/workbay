"""Projection adapter wrapper (internal).

Mirrors a lifecycle-runner decision into the handoff DB via
``mcp-workbay-handoff event record``. When the adapter is unavailable
the call must spool the payload to
``.task-state/pending-workflow-events.jsonl`` so a later
``make project-events-replay`` can retry it. Adapter failure never
blocks the primary git operation.

Status mapping:

* ``synced`` — the underlying CLI exited 0 *and* did not return a loud
  ``ok:false`` envelope. (The handoff CLI prints its JSON envelope and
  always exits 0, so a zero-exit ``ok:false`` is treated as a rejection,
  not a sync — see :func:`_rejection_data`.) The returned id is the one
  parsed out of the CLI's JSON response when present, otherwise the
  supplied ``decision_id`` (caller-id fallback matches the
  skill-broadcast wrapper).
* ``spooled`` — the CLI ran and rejected the payload (any non-zero
  returncode that is *not* a CLI-unreachable signal). The payload is
  appended to the pending-events spool so a later replay can retry,
  and the receipt surfaces the loud failure to the operator.
* ``pending`` — the CLI could not be invoked at all (missing binary
  or timeout). The payload is still spooled, but the receipt routes
  the case as a transient unreachability rather than a contract
  rejection.
* ``dropped`` — the adapter failed *and* the spool could not park the
  event: the live spool is at its hard byte limit and the dead-letter
  sink is also full, so the load-shedding last resort discarded it. No
  replay will recover it; the receipt reports ``dropped`` (never a false
  ``spooled``/``pending``) and a loud stderr line demands operator action.
* ``error`` — the caller passed a malformed payload (e.g. empty
  ``decision_id``); short-circuits before the adapter is touched.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import resolver
import projection_queue

from handlers import _common

# Returncodes that ``run_subprocess`` synthesises when the CLI cannot be
# invoked at all (missing binary -> 127, timeout -> 124). Anything else
# non-zero means the CLI ran and rejected the payload.
_CLI_UNREACHABLE_RETURNCODES = frozenset({124, 127})


def _classify_returncode(returncode: int) -> str:
    """Map a ``run_subprocess`` returncode to a projection status.

    Returns ``"synced"`` on success, ``"pending"`` when the CLI was
    unreachable, and ``"spooled"`` when the CLI ran but rejected the
    payload. internal introduces the ``spooled`` split so
    operator-visible receipts distinguish loud rejections from
    transient unreachability.
    """
    if returncode == 0:
        return "synced"
    # A negative returncode means run_subprocess's child was killed by a signal
    # (SIGKILL/OOM -> -9, SIGTERM -> -15): genuine unreachability, not a logical
    # ok:false rejection, so it must feed the breaker the same as 124/127.
    if returncode < 0 or returncode in _CLI_UNREACHABLE_RETURNCODES:
        return "pending"
    return "spooled"


def _rejection_data(stdout: str) -> dict[str, Any] | None:
    """Return the ``data`` block of a zero-exit ``ok:false`` envelope, else None.

    The handoff CLI prints its JSON envelope and *always exits 0* (``_print_json``
    never sets a returncode), so a rejected write — e.g. ``set`` refusing an
    insert for a missing objective, or an update that needs an
    ``expected_revision`` — would otherwise be misclassified as ``synced`` by
    :func:`_classify_returncode`. Returns the rejection's ``data`` dict (possibly
    empty) when ``stdout`` decodes to a dict carrying an explicit ``ok == False``;
    returns ``None`` on a non-rejection (``ok`` truthy or absent), non-dict, or
    unparseable output (e.g. a bare ``echo ok`` from a stub) so only a clearly
    rejected envelope downgrades the status. Callers inspect the returned
    ``data`` (e.g. ``current_revision``) to tell a benign rejection from a real
    one.
    """
    try:
        parsed = json.loads(stdout)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(parsed, dict) or parsed.get("ok") is not False:
        return None
    data = parsed.get("data")
    return data if isinstance(data, dict) else {}


def _spool(repo_root: Path, payload: dict[str, Any]) -> bool:
    # The circuit breaker gates *commands* at preflight; it must never divert the
    # in-flight event away from the durable spool here. Dead-lettering on
    # breaker-open silently lost replayable events (the dead-letter sink has no
    # replay-back path) and, with the default failure threshold of 1, the very
    # first failed projection dead-lettered its own payload. The hard byte limit
    # is the only legitimate drop boundary (bounded disk); every other event
    # spools for a later ``project-events-replay`` to drain.
    #
    # Returns False only when ``spool_append`` SHED the event — live spool at its
    # hard byte limit AND the dead-letter sink also full (the load-shedding last
    # resort). The event is then dropped, not parked, so callers must not report
    # it as durably ``spooled``/``pending``.
    return projection_queue.spool_append(
        repo_root,
        payload,
        reason="projection_spool_hard_limit",
    )


def _warn_shed(repo_root: Path, kind: str) -> None:
    """Emit one loud line when a projection event is shed (dropped, not parked)."""
    sys.stderr.write(
        f"projection event kind={kind!r} was SHED (dropped): the live projection "
        "spool and the dead-letter sink are both at their hard byte limit, so it "
        "could not be durably queued and no replay will recover it. Quarantine/"
        "drain the projection spool to restore durability. Operator action "
        "required.\n"
    )


def project_decision(
    repo_root: Path,
    *,
    decision_id: str,
    rationale: str,
    session: str,
    task_ref: str | None = None,
) -> tuple[str, str | None]:
    """Project a decision event into the handoff DB.

    Returns ``(status, returned_id)``. ``status`` is one of
    ``"synced"``, ``"spooled"``, ``"pending"``, or ``"error"``.
    ``returned_id`` is the decision id reported by the adapter (or the
    supplied ``decision_id`` when the adapter response is unparseable
    on a zero-exit invocation), and ``None`` when the call was spooled,
    pending, or rejected.
    """
    if not decision_id or not session:
        return "error", None

    worktree_branch = resolver.current_branch(repo_root)
    worktree_head = resolver.head_sha(repo_root)
    argv: list[str] = _common.handoff_command_argv(
        repo_root,
        "event",
        "--event-kind", "decision",
        "--session", session,
        "--decision", decision_id,
        "--rationale", rationale,
        *_common.worktree_write_context_argv(repo_root, task_ref=task_ref),
    )

    proc = _common.run_subprocess(argv, timeout=_common.handoff_timeout())
    status = _classify_returncode(proc.returncode)
    # Same exit-0-on-rejection contract as project_state_sync: the CLI exits 0
    # even when ``record_event`` rejects the payload, so a zero-exit ``ok:false``
    # must spool rather than masquerade as a synced decision id.
    if status == "synced" and _rejection_data(proc.stdout) is not None:
        status = "spooled"
    if status == "synced":
        projection_queue.record_projection_success(repo_root)
    elif status == "pending":
        # Only genuine CLI-unreachability (rc 124/127 -> "pending") feeds the
        # retry-storm breaker. A loud per-payload rejection ("spooled") means the
        # CLI is up and answering, so it must not trip the breaker and lock out
        # unrelated mutating commands; the event is spooled for replay either way.
        projection_queue.record_projection_failure(repo_root, returncode=proc.returncode)
    if status != "synced":
        spool_payload: dict[str, Any] = {
            "kind": "decision",
            "decision_id": decision_id,
            "rationale": rationale,
            "session": session,
        }
        if task_ref:
            spool_payload["task_ref"] = task_ref
        if worktree_branch:
            spool_payload["branch"] = worktree_branch
        if worktree_head:
            spool_payload["commit_sha"] = worktree_head
        if not _spool(repo_root, spool_payload):
            _warn_shed(repo_root, "decision")
            return "dropped", None
        return status, None

    try:
        parsed = json.loads(proc.stdout)
    except (json.JSONDecodeError, ValueError):
        return "synced", decision_id

    candidate: Any = decision_id
    if isinstance(parsed, dict):
        data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
        decision_obj = data.get("decision") if isinstance(data, dict) else None
        candidate = (
            (data.get("decision_id") if isinstance(data, dict) else None)
            or (decision_obj.get("id") if isinstance(decision_obj, dict) else None)
            or decision_id
        )
    return "synced", str(candidate)


def project_test_result(
    repo_root: Path,
    *,
    session: str,
    command: str,
    passed: bool,
    exit_code: int | None = None,
    result: str | None = None,
    task_ref: str | None = None,
) -> str:
    """Project a test_result event into the handoff DB.

    Mirrors :func:`project_decision`'s shell-out pattern: shells out to
    ``mcp-workbay-handoff event --event-kind test_result``. On adapter
    failure, spools a ``test_result`` payload to the pending file so a
    later ``project-events-replay`` can drain it. Returns ``"synced"``,
    ``"spooled"`` (CLI ran and rejected), or ``"pending"`` (CLI
    unreachable).
    """
    if not session or not command:
        return "error"
    worktree_branch = resolver.current_branch(repo_root)
    worktree_head = resolver.head_sha(repo_root)
    argv: list[str] = _common.handoff_command_argv(
        repo_root,
        "event",
        "--event-kind", "test_result",
        "--session", session,
        "--command", command,
        *_common.worktree_write_context_argv(repo_root, task_ref=task_ref),
    )
    if passed:
        argv.append("--passed")
    if exit_code is not None:
        argv.extend(["--exit-code", str(exit_code)])
    if result:
        argv.extend(["--result", result])

    proc = _common.run_subprocess(argv, timeout=_common.handoff_timeout())
    status = _classify_returncode(proc.returncode)
    # Same exit-0-on-rejection contract as project_state_sync: a zero-exit
    # ``ok:false`` from ``record_event`` must spool rather than report synced.
    if status == "synced" and _rejection_data(proc.stdout) is not None:
        status = "spooled"
    if status == "synced":
        projection_queue.record_projection_success(repo_root)
    elif status == "pending":
        # Only genuine CLI-unreachability (rc 124/127 -> "pending") feeds the
        # retry-storm breaker. A loud per-payload rejection ("spooled") means the
        # CLI is up and answering, so it must not trip the breaker and lock out
        # unrelated mutating commands; the event is spooled for replay either way.
        projection_queue.record_projection_failure(repo_root, returncode=proc.returncode)
    if status != "synced":
        spool_payload: dict[str, Any] = {
            "kind": "test_result",
            "session": session,
            "command": command,
            "passed": passed,
            "exit_code": exit_code,
            "result": result,
        }
        if task_ref:
            spool_payload["task_ref"] = task_ref
        # Persist actor provenance alongside the payload so replay can
        # re-issue the event with matching --branch/--commit-sha and
        # avoid the drift the online path already guards against.
        if worktree_branch:
            spool_payload["branch"] = worktree_branch
        if worktree_head:
            spool_payload["commit_sha"] = worktree_head
        if not _spool(repo_root, spool_payload):
            _warn_shed(repo_root, "test_result")
            return "dropped"
    return status


def project_state_sync(
    repo_root: Path,
    *,
    task_ref: str,
    target_branch: str,
    target_worktree_path: str,
    task_plan_path: str | None,
    objective: str | None = None,
    status: str = "in_progress",
) -> str:
    """Project a handoff-state sync into the handoff DB.

    Shells out to ``mcp-workbay-handoff set`` with the git-derived task
    ref / branch / worktree path / plan path so the handoff state row
    follows the started task. ``objective`` is forwarded as ``--objective``
    so the very first ``set`` for a brand-new ``task_ref`` can INSERT the
    row — ``set_handoff_state`` rejects an insert with no objective, and
    because the CLI always exits 0 that rejection would otherwise be
    silently misclassified as ``synced`` (the row never lands). Falls back
    to spooling a ``state_sync`` payload to
    ``.task-state/pending-workflow-events.jsonl`` when the adapter is
    unavailable *or* returns a loud ``ok:false`` envelope. Returns
    ``"synced"``, ``"spooled"`` (CLI ran and rejected), or ``"pending"``
    (CLI unreachable).
    """
    argv = _common.handoff_command_argv(
        repo_root,
        "set",
        "--task-ref", task_ref,
        "--target-branch", target_branch,
        "--target-worktree-path", target_worktree_path,
        "--status", status,
    )
    if objective is not None:
        argv.extend(["--objective", objective])
    if task_plan_path:
        argv.extend(["--task-plan-path", task_plan_path])
    if target_branch:
        argv.extend(["--branch", target_branch])
    target_head = resolver.head_sha(Path(target_worktree_path))
    if target_head:
        argv.extend(["--commit-sha", target_head])

    proc = _common.run_subprocess(argv, timeout=_common.handoff_timeout())
    projection_status = _classify_returncode(proc.returncode)
    # The CLI exits 0 even when it rejects the write, so a zero-exit
    # ``ok:false`` envelope must be reclassified rather than reported as a
    # false ``synced`` (the silent no-op behind internal-*). One ok:false
    # case is benign for task-start: when a row already exists, the CLI
    # rejects the objective-less update with ``current_revision`` set
    # ("expected_revision is required"). task-start's invariant — a live row
    # exists for ``task_ref`` — already holds, so report ``synced`` WITHOUT
    # spooling a ``state_sync`` entry the replay path cannot drain (it only
    # re-issues ``test_result``). Any other ok:false is a real rejection and
    # is spooled + surfaced.
    if projection_status == "synced":
        rejection = _rejection_data(proc.stdout)
        if rejection is not None and rejection.get("current_revision") is None:
            projection_status = "spooled"
    if projection_status == "synced":
        projection_queue.record_projection_success(repo_root)
    elif projection_status == "pending":
        # Only genuine CLI-unreachability (rc 124/127 -> "pending") feeds the
        # retry-storm breaker; a loud per-payload rejection ("spooled") keeps it
        # closed so a single rejection cannot lock out unrelated commands.
        projection_queue.record_projection_failure(repo_root, returncode=proc.returncode)
    if projection_status != "synced":
        spooled = _spool(
            repo_root,
            {
                "kind": "state_sync",
                "task_ref": task_ref,
                "target_branch": target_branch,
                "target_worktree_path": target_worktree_path,
                "task_plan_path": task_plan_path,
                "objective": objective,
                "status": status,
                "branch": target_branch,
                "commit_sha": target_head,
            },
        )
        if not spooled:
            _warn_shed(repo_root, "state_sync")
            return "dropped"
    return projection_status
