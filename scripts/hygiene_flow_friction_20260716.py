#!/usr/bin/env python3
"""One-shot hygiene for implementation note S6 (internal).

Three idempotent operations against the repo's runtime task state:

1. Bulk-resolve the open ``pass-task`` self-verify blockers — 32 identical
   test-lane rows ("Worker self-verify failed for lane 'worker' on `exit 1`")
   mined in the 2026-07-16 triage. Real lanes never used task_ref
   ``pass-task``; the rows are eval-suite noise drowning the blocker surface.
2. Delete the 0-byte ``.workbay/handoff.db`` stub (the live DB is
   ``.task-state/handoff.db``; the stub only misleads path probes).
3. Drop a harvested-disposition sidecar next to the terminal dead-letter
   spool ``.task-state/agent-errors-spool.deadletter.jsonl`` (records were
   already mined into the 0146 grounding; the spool stays in place so
   writers keep appending).

Re-running is a no-op: resolved blockers are skipped by the open-rows query,
the stub is only removed when present and empty, the sidecar is only written
once.

Usage:  python scripts/hygiene_flow_friction_20260716.py [--repo-root PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

NOISE_TASK_REF = "pass-task"
NOISE_PREFIX = "Worker self-verify failed for lane 'worker' on `exit 1`"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    root = args.repo_root.resolve()

    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.decisions import report_blocker
    from workbay_handoff_mcp.shared_schema import _connect_handoff_sqlite

    configure_runtime(RuntimeConfig.for_repo(root))

    receipt: dict[str, object] = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(root),
        "dry_run": args.dry_run,
    }

    conn = _connect_handoff_sqlite(root / ".task-state" / "handoff.db")
    try:
        rows = conn.execute(
            "SELECT id FROM blockers WHERE status = 'open'"
            " AND task_ref = ? AND description LIKE ? || '%' ORDER BY id",
            (NOISE_TASK_REF, NOISE_PREFIX),
        ).fetchall()
    finally:
        conn.close()
    blocker_ids = [int(r[0]) for r in rows]
    receipt["open_noise_blocker_ids"] = blocker_ids

    resolved: list[int] = []
    if not args.dry_run:
        for bid in blocker_ids:
            result = report_blocker(
                operation="resolve",
                blocker_id=bid,
                task_ref=NOISE_TASK_REF,
            )
            payload = result if isinstance(result, dict) else json.loads(result)
            if payload.get("ok"):
                resolved.append(bid)
            else:
                print(f"blocker {bid}: resolve failed: {payload}", file=sys.stderr)
    receipt["resolved_blocker_ids"] = resolved

    stub = root / ".workbay" / "handoff.db"
    stub_removed = False
    if stub.is_file() and stub.stat().st_size == 0:
        if not args.dry_run:
            stub.unlink()
        stub_removed = True
    receipt["stub_removed"] = stub_removed

    deadletter = root / ".task-state" / "agent-errors-spool.deadletter.jsonl"
    sidecar = deadletter.with_suffix(".jsonl.harvested")
    sidecar_written = False
    if deadletter.is_file() and not sidecar.exists():
        if not args.dry_run:
            sidecar.write_text(
                "harvested 2026-07-16 into implementation note grounding (258 terminal records); "
                "spool left in place for future appends. plan0146 S6.\n",
                encoding="utf-8",
            )
        sidecar_written = True
    receipt["deadletter_sidecar_written"] = sidecar_written

    print(json.dumps(receipt, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
