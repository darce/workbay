"""``attest`` lifecycle subcommand (internal S3)."""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any

import projection

from . import _common

_CRITERION_RE = re.compile(r"^[a-z0-9_]+$")


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle attest", add_help=True)
    parser.add_argument("--task", dest="task", required=True)
    parser.add_argument("--criterion", dest="criterion", required=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit({"ok": False, "command": "attest", "error": "not_in_git_repo"})
        return 2

    task_ref = args.task.strip().upper()
    criterion = args.criterion.strip()
    if not _CRITERION_RE.match(criterion):
        _common.emit({
            "ok": False,
            "command": "attest",
            "error": "criterion_invalid",
            "criterion": criterion,
        })
        return 2

    decision_id = f"attestation:{criterion}"
    session = decision_id
    status, returned = projection.project_decision(
        repo,
        decision_id=decision_id,
        rationale=f"Attestation recorded for criterion {criterion}.",
        session=session,
        task_ref=task_ref,
    )
    receipt: dict[str, Any] = {
        "ok": status in ("synced", "spooled"),
        "command": "attest",
        "task_ref": task_ref,
        "criterion": criterion,
        "decision_id": decision_id,
        "projection_status": status,
        "decision_row_id": returned,
    }
    if status == "error":
        receipt["ok"] = False
        receipt["error"] = "projection_failed"
    if not args.emit_json:
        sys.stderr.write(
            f"attest: task_ref={task_ref} criterion={criterion} status={status}\n"
        )
    _common.emit(receipt)
    return 0 if receipt["ok"] else 2
