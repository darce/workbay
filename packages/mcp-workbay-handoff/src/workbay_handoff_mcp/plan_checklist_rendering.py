"""internal S2: plan_checklist projection surface."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .runtime import get_runtime_config


def render_plan_checklist(task_ref: str, *, plan_path: str | None = None) -> dict[str, Any]:
    runtime = get_runtime_config()
    root = Path(runtime.git_workspace_root or runtime.workspace_root)
    lifecycle_pkg = root / "packages/workbay-system/workbay_system/payload/scripts/workbay/lifecycle"
    argv = [sys.executable, str(lifecycle_pkg), "plan-status", "--task", task_ref, "--json"]
    if plan_path:
        argv.extend(["--plan", plan_path])
    proc = subprocess.run(argv, capture_output=True, text=True, check=False, cwd=str(root))
    if proc.returncode != 0:
        return {
            "ok": False,
            "tool": "render_handoff",
            "data": {"error": "plan_status_failed", "stderr": proc.stderr[:500]},
            "task_ref": task_ref,
        }
    try:
        payload = json.loads(proc.stdout)
    except (json.JSONDecodeError, ValueError):
        # Degrade to the advisory contract (count_evidenced_unticked_boxes -> None)
        # rather than letting a decode error propagate into the enforced close
        # gate and crash it on contaminated/non-JSON stdout.
        return {
            "ok": False,
            "tool": "render_handoff",
            "data": {"error": "plan_status_unparseable", "stdout": proc.stdout[:500]},
            "task_ref": task_ref,
        }
    return {
        "ok": bool(payload.get("ok")),
        "tool": "render_handoff",
        "data": {"kind": "plan_checklist", "projection": payload},
        "task_ref": task_ref,
    }


def count_evidenced_unticked_boxes(task_ref: str) -> int | None:
    """Return evidenced-but-unticked box count, or None when plan lookup fails (advisory)."""

    envelope = render_plan_checklist(task_ref)
    if not envelope.get("ok"):
        return None
    projection = (envelope.get("data") or {}).get("projection") or {}
    if not projection.get("ok"):
        return None
    items = projection.get("items") or []
    return sum(
        1
        for item in items
        if isinstance(item, dict)
        and item.get("projected") == "tick"
        and not item.get("doc_ticked")
    )
