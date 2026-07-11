"""Skill-broadcast wrappers for plan-review and plan-analyze (implementation note).

These targets do not shell out to a CLI subcommand because no
``mcp-workbay-handoff plan-review`` / ``plan-analyze`` subcommand exists.
Instead they print structured guidance instructing the operator or
agent to invoke the corresponding ``/<skill>`` in-session, and emit a
``workflow_intent`` event via ``mcp-workbay-handoff event record`` so the
intent is replayable from handoff. When MCP is offline, the intent is
spooled to ``.task-state/pending-workflow-events.jsonl`` at repo root
and the receipt reports ``intent_event_id: null`` with
``handoff_projection: "spooled"`` (CLI ran and rejected) or
``"pending"`` (CLI unreachable). Exit 0 on success or spool; exit
non-zero only if cwd is not a git repo.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
from pathlib import Path
from typing import Any

import projection_queue

from . import _common

SKILL_BY_COMMAND: dict[str, str] = {
    "plan-review": "planning-review",
    "plan-analyze": "plan-analyze",
}

# A numbered plan doc leads with its zero-padded ordinal (``docs/plans/0081-*.md``).
_NUMBERED_PLAN_RE = re.compile(r"^(\d+)[-_]")
# A task-plan doc leads with the task ref (``internal-*.md``): uppercase
# segments separated by hyphens ending in a numeric suffix.
_TASK_REF_RE = re.compile(r"^([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*-\d+)")


def _utc_now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d%H%M%S")


def _derive_plan_ref(doc: str) -> str:
    """Derive a workflow-intent plan ref from the analyzed doc path.

    ``docs/plans/0081-foo.md`` -> ``plan0081``; a task-plan doc such as
    ``docs/tasks/internal-foo.md`` -> the lowercased task-ref slug
    ``wb-steady-prov-01``; anything unrecognised -> ``planunknown`` (never the
    hardcoded ``plan0009`` literal the ids used to carry regardless of the doc).
    """
    if not doc:
        return "planunknown"
    name = Path(doc).name
    numbered = _NUMBERED_PLAN_RE.match(name)
    if numbered:
        return f"plan{numbered.group(1)}"
    task = _TASK_REF_RE.match(name)
    if task:
        return task.group(1).lower()
    return "planunknown"


def _build_decision_id(skill: str, doc: str) -> str:
    return f"claude_workflow_intent_{_derive_plan_ref(doc)}_{skill}_{_utc_now_iso()}"


def _spool_pending_event(repo_root: Path, payload: dict[str, Any]) -> bool:
    # Route this writer through the SAME hard-limit gate as projection._spool.
    # workflow_intent is the dominant spool writer -- it was ~80% of the 3.9 GiB
    # crash artifact -- so it must share every backpressure guard: the hard byte
    # limit + dead-letter cap it here, and it is now a first-class replay kind
    # (project_events_replay._build_workflow_intent_argv) that drains and dedups
    # like any other event (the spool_append event_id stamp keys exactly-once
    # replay), so a spooled intent is recoverable rather than unbounded.
    #
    # Returns False when ``spool_append`` SHED the event (live spool and
    # dead-letter sink both at the hard byte limit): the intent was dropped, not
    # parked, so the receipt must not claim it is recoverable.
    return projection_queue.spool_append(
        repo_root,
        payload,
        reason="projection_spool_hard_limit",
    )


_CLI_UNREACHABLE_RETURNCODES = frozenset({124, 127})


def _record_intent_event(
    decision_id: str, skill: str, doc: str
) -> tuple[str | None, str, int]:
    """Return (intent_event_id, projection_status, returncode).

    intent_event_id is None on failure. internal: distinguish ``spooled``
    (CLI ran and rejected) from ``pending`` (CLI unreachable) so the receipt
    surfaces loud failures to the operator instead of burying them under
    transient unreachability. The returncode is returned so the caller can feed
    the retry-storm breaker symmetrically (only genuine unreachability counts).
    """
    rationale = f"invoke /{skill} for {doc}"
    proc = _common.run_subprocess(
        [
            _common.mcp_handoff_bin(),
            "event",
            "--event-kind",
            "decision",
            "--session",
            decision_id,
            "--decision",
            decision_id,
            "--rationale",
            rationale,
        ]
    )
    if proc.returncode != 0:
        # Mirror projection._classify_returncode: a negative returncode means the
        # child was signal-killed (SIGKILL/OOM -> -9) -- genuine unreachability,
        # not a logical ok:false rejection -- so it must feed the breaker like
        # 124/127, never be misread as a loud per-payload "spooled" rejection.
        unreachable = proc.returncode < 0 or proc.returncode in _CLI_UNREACHABLE_RETURNCODES
        status = "pending" if unreachable else "spooled"
        return None, status, proc.returncode
    # Best-effort id extraction. The CLI emits JSON with the id; fall back
    # to the decision_id itself when the response is unparseable.
    try:
        parsed = json.loads(proc.stdout)
        candidate = (
            parsed.get("data", {}).get("decision_id")
            or parsed.get("data", {}).get("decision", {}).get("id")
            or decision_id
        )
        return str(candidate), "synced", 0
    except json.JSONDecodeError:
        return decision_id, "synced", 0


def run(command: str, argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog=f"lifecycle {command}", add_help=True)
    parser.add_argument("--doc", required=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    skill = SKILL_BY_COMMAND[command]
    repo_root = _common.repo_root()
    if repo_root is None:
        receipt = {
            "ok": False,
            "command": command,
            "delegation_mode": "in_session_skill",
            "delegated_to": f"skill:{skill}",
            "intent_event_id": None,
            "handoff_projection": "error",
            "events": [],
            "error": "not_in_git_repo",
        }
        _common.emit(receipt)
        return 2

    decision_id = _build_decision_id(skill, args.doc)
    intent_event_id, projection, returncode = _record_intent_event(decision_id, skill, args.doc)

    # Feed the retry-storm breaker symmetrically: skill_broadcast is the dominant
    # handoff-CLI caller, so the breaker must observe ITS failures, not just the
    # mutating-command projections. Only genuine unreachability ("pending", rc
    # 124/127 or a signal-kill) trips it; a loud per-payload rejection ("spooled")
    # keeps it closed, and a sync resets it -- mirroring projection.py.
    if projection == "synced":
        projection_queue.record_projection_success(repo_root)
    elif projection == "pending":
        projection_queue.record_projection_failure(repo_root, returncode=returncode)

    if intent_event_id is None:
        spooled = _spool_pending_event(
            repo_root,
            {
                "kind": "workflow_intent",
                "command": command,
                "skill": skill,
                "doc": args.doc,
                "decision_id": decision_id,
            },
        )
        if not spooled:
            # Shed: spool + dead-letter sink both full. The intent was dropped,
            # not parked, so report ``dropped`` instead of a false ``spooled``/
            # ``pending`` and surface a loud line for operator action.
            projection = "dropped"
            sys.stderr.write(
                f"workflow_intent for /{skill} {args.doc} was SHED (dropped): the "
                "projection spool and dead-letter sink are both at their hard byte "
                "limit, so it could not be durably queued and no replay will "
                "recover it. Quarantine/drain the projection spool. Operator "
                "action required.\n"
            )

    if intent_event_id is not None:
        intent_event = "workflow_intent_recorded"
    elif projection == "dropped":
        intent_event = "workflow_intent_dropped"
    else:
        intent_event = "workflow_intent_spooled"

    receipt = {
        "ok": True,
        "command": command,
        "doc": args.doc,
        "delegation_mode": "in_session_skill",
        "delegated_to": f"skill:{skill}",
        "intent_event_id": intent_event_id,
        "handoff_projection": projection,
        "events": [intent_event],
    }

    if not args.emit_json:
        sys.stderr.write(
            f"plan-{'review' if skill == 'planning-review' else 'analyze'} "
            f"is a skill-broadcast wrapper. Invoke `/{skill} {args.doc}` "
            f"in-session to run the actual review.\n"
        )

    _common.emit(receipt)
    return 0
