"""Lane-scoped publication permission shared with the no-work reaper.

Primary33284 S3/D-liveness and review25 R1/R2 require per-lane resources:
never put a lifecycle lease on handoff.db, a manifest, or the pass store.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held
from workbay_protocol.lifecycle_coordination import LifecycleLease


class LanePassPublicationRefused(RuntimeError):
    """Publication could not acquire non-destructive permission in its budget."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def lane_publication_resource(state_dir: Path, task_ref: str, lane_id: str) -> Path:
    key = hashlib.sha256(f"{task_ref}\0{lane_id}".encode()).hexdigest()[:32]
    return Path(state_dir) / "lane-publication" / key


def lane_publication_lease(
    state_dir: Path, task_ref: str, lane_id: str, *, exclusive: bool = False, deadline: float
) -> LifecycleLease:
    assert_no_write_lock_held("lane publication lease")
    return LifecycleLease(
        lane_publication_resource(state_dir, task_ref, lane_id),
        exclusive=exclusive,
        timeout=1.0,
        deadline=deadline,
    )
