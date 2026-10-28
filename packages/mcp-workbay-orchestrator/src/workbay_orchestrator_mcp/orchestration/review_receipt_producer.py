"""Canonical preceding-harvest receipt for internal (worker self-review) cycles.

Publishes the existing offload receipt and its independently retained pass state,
the pair ``review_ratchet.load_context`` already consumes. No parallel history.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held


class ReceiptRefused(RuntimeError):
    """Named producer failure; a recorded review without its receipt is never green."""


def publish_internal_receipt(
    *,
    orchestrator_root: Path,
    worktree_path: Path,
    task_ref: str,
    lane_id: str,
    branch: str,
    base_sha: str,
    tip_sha: str,
    backend: str,
    model: str,
    reasoning_effort: str,
    started_at: str,
    operation_token: str,
    recorded_ids: list[str],
    review_context: Any = None,
) -> dict[str, Any]:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.shared_schema import _get_db_connection
    from workbay_protocol.review_impact import is_blocking

    from .offload_pass import _review_identity_complete, read_pass_state, write_pass_state
    from .pass_rescue import _atomic_write
    from .review_ratchet import unresolved_listed

    # Refuse before spawning Git while this thread holds a SQLite write lock.
    assert_no_write_lock_held("subprocess")
    head = subprocess.run(
        ["git", "-C", str(worktree_path), "rev-parse", "--verify", "HEAD^{commit}"],
        capture_output=True, text=True, check=False,
    ).stdout.strip()
    # The receipt attests the commit the model actually reviewed, not a later tip.
    if head != tip_sha:
        raise ReceiptRefused("review_receipt_tip_moved")
    pass_id = f"internal-review-{operation_token}"
    identity = dict(
        pass_id=pass_id, task_ref=task_ref, lane_id=lane_id, base_sha=base_sha, tip_sha=tip_sha, branch=branch,
        backend=backend, model=model, reasoning_effort=reasoning_effort, dispatch_id=None, agent_stream_path=None,
        worktree_path=str(worktree_path), started_at=started_at,
    )
    if not branch or not _review_identity_complete(identity):
        raise ReceiptRefused("review_receipt_identity_incomplete")
    # Named blockers still unresolved after this round stay named for the next one.
    carried = (
        unresolved_listed(Path(orchestrator_root), review_context, task_ref=task_ref)
        if review_context is not None else []
    )
    ids = list(dict.fromkeys([*recorded_ids, *carried]))
    configure_runtime(RuntimeConfig.for_repo(Path(orchestrator_root)))
    with _get_db_connection() as conn:
        rows = {row["finding_id"]: dict(row)
                for row in conn.execute("SELECT * FROM review_findings WHERE task_ref = ?", (task_ref,))}
    harvest = dict(
        status="recorded",
        finding_ids=ids,
        unrecorded_ids=[],
        blocking_findings=[
            rows[fid] for fid in ids
            if fid in rows and (fid in carried or (rows[fid]["status"] == "open" and is_blocking(rows[fid])))
        ],
        unverified_listed_finding_ids=carried,
    )
    state_dir = Path(orchestrator_root) / ".task-state"
    # Independent engine copy first: a receipt without state, or state without a
    # receipt, both refuse on the next load rather than reading as a first round.
    if not write_pass_state(state_dir, pass_id, {**identity, "status": "done", "pass_kind": "internal_review",
                                                 "findings_harvest": harvest}):
        raise ReceiptRefused("review_receipt_state_unpublished")
    state = read_pass_state(state_dir, pass_id) or {}
    if state.get("findings_harvest") != json.loads(json.dumps(harvest)):
        raise ReceiptRefused("review_receipt_state_unpublished")
    path = state_dir / f"review-receipt-{pass_id}.json"
    receipt = dict(identity=identity, status="recorded", harvest=harvest, operation_token=operation_token,
                   source="internal_review")
    _atomic_write(path, json.dumps(receipt))
    return {"pass_id": pass_id, "path": str(path), "tip_sha": tip_sha, "listed_finding_ids": carried}
