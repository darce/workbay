"""``project-events-replay`` subcommand (internal.2).

Drains the offline spool ``.task-state/pending-workflow-events.jsonl``
by replaying each entry through the canonical handoff CLI in original
order. Drained entries are removed; entries whose CLI invocation
returned non-zero stay in the spool so a later replay can retry them.

The handler is read-mostly (no git mutation) and still emits the
canonical receipt schema so consumers can diff fixtures by-byte.
``test_result`` and ``state_sync`` entries are drained by re-issuing the
original projection through the canonical handoff CLI; ``decision`` kinds
still round-trip through their own projection helper and will land
alongside in a follow-up sub-slice.

Replay classification applies the same exit-0-on-``ok:false`` guard the
online projection path uses (internal): the handoff CLI prints its
JSON envelope and *always exits 0*, so a returncode-only check would
drain a rejected write. A zero-exit ``ok:false`` is therefore treated as
a rejection (kept spooled) — except the benign ``state_sync`` case where
the row already exists (``data.current_revision`` present, i.e. the
update needs an ``expected_revision``): that means the desired row is
live, so the entry is drained.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import resolver

from . import _common

PENDING_REL = Path(".task-state") / "pending-workflow-events.jsonl"


def _build_test_result_argv(
    repo: Path, payload: dict[str, Any]
) -> list[str]:
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "event",
        "--event-kind", "test_result",
        "--session", str(payload.get("session", "")),
        "--command", str(payload.get("command", "")),
    )
    # Forward branch/commit_sha actor overrides persisted on the spool
    # entry so replayed verified_tests rows carry the same worktree
    # provenance the online path would have written. Without this the
    # replay drains the entry but lands the row under whatever branch
    # /commit the canonical workspace happens to be on.
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    if payload.get("passed"):
        argv.append("--passed")
    exit_code = payload.get("exit_code")
    if exit_code is not None:
        argv.extend(["--exit-code", str(exit_code)])
    result = payload.get("result")
    if result:
        argv.extend(["--result", str(result)])
    return argv


def _build_state_sync_argv(repo: Path, payload: dict[str, Any]) -> list[str]:
    """Rebuild the ``set`` argv from a spooled ``state_sync`` entry.

    Mirrors :func:`projection.project_state_sync`'s online argv so a drained
    entry lands the identical handoff_state row: required identity flags plus
    the optional ``--objective`` / ``--task-plan-path`` and the
    ``--branch`` / ``--commit-sha`` actor overrides persisted on the entry.
    """
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "set",
        "--task-ref", str(payload.get("task_ref", "")),
        "--target-branch", str(payload.get("target_branch", "")),
        "--target-worktree-path", str(payload.get("target_worktree_path", "")),
        "--status", str(payload.get("status") or "in_progress"),
    )
    # ``objective`` mirrors the online ``objective is not None`` guard: an
    # empty string is a valid (empty) objective and is forwarded; a missing /
    # null key omits the flag (an update preserves the stored objective).
    objective = payload.get("objective")
    if isinstance(objective, str):
        argv.extend(["--objective", objective])
    task_plan_path = payload.get("task_plan_path")
    if isinstance(task_plan_path, str) and task_plan_path:
        argv.extend(["--task-plan-path", task_plan_path])
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    return argv


_CLI_UNREACHABLE_RETURNCODES = frozenset({124, 127})


def _replay_entry(repo: Path, entry: dict[str, Any]) -> str:
    kind = entry.get("kind")
    if kind == "test_result":
        argv = _build_test_result_argv(repo, entry)
    elif kind == "state_sync":
        argv = _build_state_sync_argv(repo, entry)
    else:
        # Unknown / not-yet-supported kinds stay pending so the next
        # replay (after the matching kind handler ships) can drain them.
        return "pending"
    proc = _common.run_subprocess(argv)
    if proc.returncode in _CLI_UNREACHABLE_RETURNCODES:
        return "pending"
    if proc.returncode != 0:
        # internal: CLI ran and rejected the payload — keep it in
        # the spool but surface the loud failure as ``spooled``.
        return "spooled"
    # internal: the handoff CLI exits 0 even when it rejects the write, so
    # a zero-exit ``ok:false`` must not be drained as a false ``synced``. Reuse
    # the online projection's rejection parser. A benign ``state_sync``
    # rejection — the row already exists, so the objective-less update needs an
    # ``expected_revision`` (``current_revision`` present) — means the desired
    # row is live, so drain it. Any other ``ok:false`` is a real rejection and
    # stays spooled.
    import projection  # lazy import: avoid a handlers<->projection import cycle

    rejection = projection._rejection_data(proc.stdout)
    if rejection is None or rejection.get("current_revision") is not None:
        return "synced"
    return "spooled"


def _emit(repo: Path, *, drained: int, pending_remaining: int,
          replay_results: list[dict[str, str]],
          handoff_projection: str) -> None:
    branch = resolver.current_branch(repo) or ""
    head = resolver.head_sha(repo) or ""
    derived_task_ref = resolver.derive_task_ref(
        branch, known_task_refs=_common._live_task_refs(repo)
    )
    receipt = {
        "ok": True,
        "command": "project-events-replay",
        "task_ref": derived_task_ref,
        "branch": branch,
        "worktree_path": str(repo),
        "head": head,
        "handoff_projection": handoff_projection,
        "events": ["events_replayed"] if drained or pending_remaining else ["events_replayed"],
        "drained": drained,
        "pending_remaining": pending_remaining,
        "replay_results": replay_results,
    }
    _common.emit(receipt)


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle project-events-replay", add_help=True
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    spool = repo / PENDING_REL

    if not spool.exists():
        _emit(
            repo,
            drained=0,
            pending_remaining=0,
            replay_results=[],
            handoff_projection="synced",
        )
        return 0

    raw_lines = spool.read_text().splitlines()
    entries: list[dict[str, Any]] = []
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(entry, dict):
            entries.append(entry)

    drained = 0
    remaining: list[dict[str, Any]] = []
    replay_results: list[dict[str, str]] = []
    saw_spooled = False
    for entry in entries:
        status = _replay_entry(repo, entry)
        replay_results.append(
            {"kind": str(entry.get("kind", "unknown")), "status": status}
        )
        if status == "synced":
            drained += 1
        else:
            remaining.append(entry)
            if status == "spooled":
                saw_spooled = True

    if remaining:
        spool.write_text(
            "".join(json.dumps(e, sort_keys=False) + "\n" for e in remaining)
        )
    else:
        spool.unlink(missing_ok=True)

    # internal: prefer ``spooled`` when *any* entry was actively
    # rejected by the CLI; otherwise fall back to ``pending`` for
    # transient unreachability. ``synced`` only when nothing remains.
    if not remaining:
        handoff_projection = "synced"
    elif saw_spooled:
        handoff_projection = "spooled"
    else:
        handoff_projection = "pending"
    _emit(
        repo,
        drained=drained,
        pending_remaining=len(remaining),
        replay_results=replay_results,
        handoff_projection=handoff_projection,
    )
    return 0
