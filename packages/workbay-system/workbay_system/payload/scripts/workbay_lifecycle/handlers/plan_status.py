"""``plan-status`` lifecycle subcommand (internal S2)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import _common
from . import sync_task_plan_checklist as sync_handler


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle plan-status", add_help=True)
    parser.add_argument("--task", dest="task", required=True)
    parser.add_argument("--plan", dest="plan", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit({"ok": False, "command": "plan-status", "error": "not_in_git_repo"})
        return 2

    task_ref = args.task.strip().upper()
    plan_path = Path(args.plan) if args.plan.strip() else None
    payload = sync_handler.project_plan_checklist(repo, task_ref, plan_path=plan_path)
    receipt = {"command": "plan-status", **payload}
    if not args.emit_json:
        counts = payload.get("counts", {})
        sys.stderr.write(
            f"plan-status: task_ref={task_ref} ok={payload.get('ok')} counts={counts}\n"
        )
    _common.emit(receipt)
    return 0 if payload.get("ok") else 2
