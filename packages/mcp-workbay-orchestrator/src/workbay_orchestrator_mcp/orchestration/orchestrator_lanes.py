"""Lane operations: dispatch, poll, intake, refresh, and cross-lane verification."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from workbay_protocol import resolve_env_alias

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _env import pythonpath_env
from orchestrator_helpers import _require_dict_payload

# ---------------------------------------------------------------------------
# Dispatch, poll, intake
# ---------------------------------------------------------------------------


def _run_handoff_dispatch(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run ``review_dispatch.py`` and return its JSON output."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "review_dispatch.py"),
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
    ]
    if dry_run:
        cmd.append("--dry-run")
    env = pythonpath_env(orchestrator_root)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"review_dispatch.py failed (exit {result.returncode}):\n{result.stderr.strip()}")
    data = json.loads(result.stdout)
    if not isinstance(data, dict):
        raise TypeError("review_dispatch.py stdout returned non-object JSON payload.")
    return data


def _lane_has_unmerged_commits(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
) -> bool:
    """Return True if the lane branch has commits not yet on the current branch."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config or not config.get("branch"):
        return False
    branch = config["branch"]
    result = subprocess.run(
        ["git", "log", "--oneline", f"HEAD..{branch}"],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.returncode == 0 and result.stdout.strip())


def _sort_by_manifest_merge_order(ready: list[str], manifest_order: list[str]) -> list[str]:
    """Sort *ready* lanes by the manifest merge order, unknown lanes last."""
    order_map = {lane: i for i, lane in enumerate(manifest_order)}
    return sorted(ready, key=lambda lane: order_map.get(lane, len(manifest_order)))


def _intake_lane(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Run ``make lane-intake`` for a single lane.  Returns True on success."""
    cmd = [
        "make",
        "lane-intake",
        f"TASK={task_ref}",
        f"LANE={lane_id}",
    ]
    if dry_run:
        cmd.append("DRY_RUN=1")
    result = subprocess.run(
        cmd,
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Downstream refresh and cross-lane verification
# ---------------------------------------------------------------------------


def _refresh_downstream(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    downstream: list[str],
    *,
    dry_run: bool = False,
) -> list[tuple[str, bool]]:
    """Refresh each downstream lane.  Returns list of (lane, success) pairs."""
    results: list[tuple[str, bool]] = []
    for dep in downstream:
        cmd = [
            "make",
            "lane-refresh",
            f"TASK={task_ref}",
            f"LANE={dep}",
        ]
        if dry_run:
            cmd.append("DRY_RUN=1")
        r = subprocess.run(
            cmd,
            cwd=orchestrator_root,
            capture_output=True,
            text=True,
            check=False,
        )
        results.append((dep, r.returncode == 0))
    return results


def _resolve_lane_worktree(orchestrator_root: Path, task_ref: str, lane_id: str) -> Optional[Path]:
    """Resolve the worktree path for a lane from the manifest."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if config and config.get("worktree_path"):
        return Path(config["worktree_path"])
    return None


def _lane_has_capacity(task_ref: str, lane_id: str) -> bool:
    """Return True when a lane has no open dispatch, no pending lane action, and no open plan cursor."""
    from workbay_orchestrator_mcp.lanes import get_lane_activity, lane_communication, plan_cursor  # noqa: PLC0415

    messages_payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            status="open",
            limit=200,
            fields="direction",
        ),
        source=f"lane_communication(list capacity messages:{lane_id})",
    )
    if messages_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {lane_id}.")
    for row in messages_payload.get("messages", []):
        if isinstance(row, dict) and row.get("direction") == "orchestrator_to_worker":
            return False

    activity_payload = _require_dict_payload(
        get_lane_activity(
            task_ref=task_ref,
            lane_id=lane_id,
            sections="actions",
            fields="status",
            limit_actions=50,
        ),
        source=f"get_lane_activity(capacity:{lane_id})",
    )
    if activity_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to fetch lane activity for {lane_id}.")
    for row in activity_payload.get("actions", []):
        if isinstance(row, dict) and row.get("status") == "pending":
            return False

    cursor_payload = _require_dict_payload(
        plan_cursor(
            operation="list",
            task_ref=task_ref,
            state="dispatched",
            lane_id=lane_id,
            limit=20,
            fields="plan_item_id",
        ),
        source=f"plan_cursor(list capacity:{lane_id})",
    )
    if cursor_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list plan cursors for {lane_id}.")
    return not bool(cursor_payload.get("cursors"))


def _complete_lane_plan_cursor(
    task_ref: str, lane_id: str, *, worker_message_id: Optional[int] = None
) -> Optional[dict[str, Any]]:
    """Mark the newest dispatched plan cursor for a lane complete."""
    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    payload = _require_dict_payload(
        plan_cursor(
            operation="list",
            task_ref=task_ref,
            state="dispatched",
            lane_id=lane_id,
            limit=20,
            fields="plan_item_id,summary,source_heading",
        ),
        source=f"plan_cursor(list complete:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list plan cursors for {lane_id}.")
    rows = payload.get("cursors", [])
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0]
    if not isinstance(row, dict):
        return None
    update = _require_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=str(row.get("plan_item_id") or ""),
            state="completed",
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            summary=str(row.get("summary") or ""),
            source_heading=str(row.get("source_heading") or "") or None,
        ),
        source=f"plan_cursor(upsert complete:{lane_id})",
    )
    if update.get("ok") is not True:
        raise RuntimeError(f"Failed to complete plan cursor for {lane_id}.")
    cursor = update.get("cursor")
    return cursor if isinstance(cursor, dict) else None


# ---------------------------------------------------------------------------
# fresh_worktree provisioning (redispatch_mode: fresh_worktree)
# ---------------------------------------------------------------------------


# A fresh worktree created outside ``make task-start`` still wants a
# worktree-root ``.venv`` so a bare ``pytest`` resolves locally. The lifecycle
# ``provision-env`` entry point is located via ``WORKBAY_LIFECYCLE_DIR``, else
# ``scripts/workbay_lifecycle`` under the orchestrator repo root.
WORKBAY_LIFECYCLE_DIR_ENV = "WORKBAY_LIFECYCLE_DIR"


def _lifecycle_dir(orchestrator_root: Path) -> Optional[Path]:
    """Resolve the lifecycle scripts dir via the shared discovery rule."""
    override = resolve_env_alias(WORKBAY_LIFECYCLE_DIR_ENV)
    candidate = Path(override) if override else orchestrator_root / "scripts" / "workbay_lifecycle"
    return candidate if candidate.is_dir() else None


def _provision_root_venv(orchestrator_root: Path, worktree: Path) -> dict[str, Any]:
    """Provision the new worktree's root ``.venv`` via ``provision-env``.

    Returns a status dict (``invoked`` / ``absent`` / ``failed``) so callers
    and tests can distinguish "ran provisioning" from "silently did nothing".
    Never raises and never aborts fresh-lane creation.
    """
    lifecycle_dir = _lifecycle_dir(orchestrator_root)
    if lifecycle_dir is None:
        sys.stderr.write(
            "orchestrator: lifecycle provisioning entry point not found "
            f"(set {WORKBAY_LIFECYCLE_DIR_ENV} or add scripts/workbay_lifecycle "
            f"under {orchestrator_root}); run manually before tests: "
            f"python <lifecycle> provision-env --worktree {worktree}\n"
        )
        return {"status": "absent", "worktree": str(worktree)}
    proc = subprocess.run(
        [
            sys.executable,
            str(lifecycle_dir),
            "provision-env",
            "--worktree",
            str(worktree),
            "--json",
        ],
        cwd=str(orchestrator_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(
            f"orchestrator: provision-env failed (exit {proc.returncode}) for "
            f"{worktree}; run manually: python {lifecycle_dir} provision-env "
            f"--worktree {worktree}\n"
        )
        return {
            "status": "failed",
            "worktree": str(worktree),
            "returncode": proc.returncode,
        }
    return {"status": "invoked", "worktree": str(worktree)}


def _provision_fresh_worktree(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> Optional[Path]:
    """Create a clean sibling worktree for a lane branched from the orchestrator HEAD.

    Returns the new worktree path, or ``None`` if provisioning failed or was skipped.
    The new worktree is created as a sibling of *orchestrator_root* with a
    timestamped suffix so concurrent lanes never collide.
    """
    import datetime as _dt

    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config:
        return None

    # Resolve the base branch (current HEAD of the orchestrator root)
    head_result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    base_branch = head_result.stdout.strip() or "main"

    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    fresh_branch = f"codex/{task_ref}-{lane_id}-fresh-{timestamp}"
    fresh_wt = orchestrator_root.parent / f"{orchestrator_root.name}-{lane_id}-fresh-{timestamp}"

    if dry_run:
        return fresh_wt

    result = subprocess.run(
        ["git", "worktree", "add", "-b", fresh_branch, str(fresh_wt), base_branch],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    # internal: provision the new worktree's root ``.venv`` so lane
    # workers get worktree-local pytest resolution. Best-effort: an absent or
    # failing entry point warns but does not unwind the created worktree.
    _provision_root_venv(orchestrator_root, fresh_wt)

    return fresh_wt
