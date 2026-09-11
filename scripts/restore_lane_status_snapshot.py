#!/usr/bin/env python3
"""Revert lane rows closed by a reaper drain back to a pre-drain status snapshot.

Rollback arm for internal. The drain
(``reap_blocked_lanes(apply=True)``) writes ``status``/``notes`` only, so restoring
those two columns from the snapshot returns the registry to its pre-drain state.

A row is reverted only when its *current* status is ``closed_stale`` and the snapshot
disagrees. That guard keeps the restore from clobbering a lane a human legitimately
closed or merged after the snapshot was taken.

Dry-run by default; pass ``--apply`` to write.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

CLOSED_STALE = "closed_stale"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path, help="pre-drain snapshot JSON")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(".task-state/handoff.db"),
        help="handoff database (default: .task-state/handoff.db)",
    )
    parser.add_argument("--apply", action="store_true", help="write; otherwise dry-run")
    args = parser.parse_args(argv)

    if not args.snapshot.is_file():
        print(f"snapshot not found: {args.snapshot}", file=sys.stderr)
        return 2
    if not args.db.is_file():
        print(f"database not found: {args.db}", file=sys.stderr)
        return 2

    payload = json.loads(args.snapshot.read_text())
    want = {int(row["id"]): row for row in payload.get("rows", [])}
    if not want:
        print("snapshot holds no rows; nothing to restore")
        return 0

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    try:
        planned: list[tuple[int, str, str]] = []
        skipped: list[tuple[int, str]] = []
        for lane_pk, snap in sorted(want.items()):
            cur = conn.execute("SELECT status FROM worktree_lanes WHERE id = ?", (lane_pk,)).fetchone()
            if cur is None:
                skipped.append((lane_pk, "row gone"))
                continue
            current = str(cur["status"] or "")
            target = str(snap.get("status") or "")
            if current == target:
                continue
            if current != CLOSED_STALE:
                # Not a drain write — a human or another path moved this row.
                skipped.append((lane_pk, f"current={current!r} not {CLOSED_STALE}"))
                continue
            planned.append((lane_pk, current, target))

        for lane_pk, current, target in planned:
            print(f"lane {lane_pk}: {current} -> {target}")
        for lane_pk, why in skipped:
            print(f"lane {lane_pk}: skipped ({why})")

        if not args.apply:
            print(f"\nDRY RUN — would revert {len(planned)} row(s), skip {len(skipped)}")
            return 0

        with conn:
            for lane_pk, _current, target in planned:
                conn.execute(
                    "UPDATE worktree_lanes SET status = ?, notes = ? WHERE id = ? AND status = ?",
                    (target, "restored from pre-drain snapshot", lane_pk, CLOSED_STALE),
                )
        print(f"\nAPPLIED — reverted {len(planned)} row(s), skipped {len(skipped)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
