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
import sys
from pathlib import Path
from typing import Any

from . import _common

SKILL_BY_COMMAND: dict[str, str] = {
    "plan-review": "planning-review",
    "plan-analyze": "plan-analyze",
}

PENDING_EVENTS_REL = Path(".task-state") / "pending-workflow-events.jsonl"


def _utc_now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d%H%M%S")


def _build_decision_id(skill: str) -> str:
    return f"claude_workflow_intent_plan0009_{skill}_{_utc_now_iso()}"


def _spool_pending_event(repo_root: Path, payload: dict[str, Any]) -> None:
    target = repo_root / PENDING_EVENTS_REL
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=False) + "\n")


_CLI_UNREACHABLE_RETURNCODES = frozenset({124, 127})


def _record_intent_event(decision_id: str, skill: str, doc: str) -> tuple[str | None, str]:
    """Return (intent_event_id, projection_status). intent_event_id is None on failure.

    internal: distinguish ``spooled`` (CLI ran and rejected) from
    ``pending`` (CLI unreachable) so the receipt surfaces loud failures
    to the operator instead of burying them under transient
    unreachability.
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
        status = "pending" if proc.returncode in _CLI_UNREACHABLE_RETURNCODES else "spooled"
        return None, status
    # Best-effort id extraction. The CLI emits JSON with the id; fall back
    # to the decision_id itself when the response is unparseable.
    try:
        parsed = json.loads(proc.stdout)
        candidate = (
            parsed.get("data", {}).get("decision_id")
            or parsed.get("data", {}).get("decision", {}).get("id")
            or decision_id
        )
        return str(candidate), "synced"
    except json.JSONDecodeError:
        return decision_id, "synced"


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

    decision_id = _build_decision_id(skill)
    intent_event_id, projection = _record_intent_event(decision_id, skill, args.doc)

    if intent_event_id is None:
        _spool_pending_event(
            repo_root,
            {
                "kind": "workflow_intent",
                "command": command,
                "skill": skill,
                "doc": args.doc,
                "decision_id": decision_id,
            },
        )

    receipt = {
        "ok": True,
        "command": command,
        "doc": args.doc,
        "delegation_mode": "in_session_skill",
        "delegated_to": f"skill:{skill}",
        "intent_event_id": intent_event_id,
        "handoff_projection": projection,
        "events": ["workflow_intent_recorded" if intent_event_id else "workflow_intent_spooled"],
    }

    if not args.emit_json:
        sys.stderr.write(
            f"plan-{'review' if skill == 'planning-review' else 'analyze'} "
            f"is a skill-broadcast wrapper. Invoke `/{skill} {args.doc}` "
            f"in-session to run the actual review.\n"
        )

    _common.emit(receipt)
    return 0
