"""Operator-run exact-round grants; consumption lives in the ledger transaction."""
from __future__ import annotations

import argparse
import json
import os
import pwd
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .review_cycle_ledger import KINDS, read_approvals, subject_accounting


def _write_grant(directory: Path, record: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(record, sort_keys=True, indent=2).encode() + b"\n"
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".grant-", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary, directory / (record["approval_id"] + ".json"))
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    grant = commands.add_parser("grant")
    listing = commands.add_parser("list")
    for command in (grant, listing):
        command.add_argument("--repo", type=Path, required=True)
    grant.add_argument("--subject", required=True)
    listing.add_argument("--subject")
    grant.add_argument("--kind", choices=sorted(KINDS), required=True)
    grant.add_argument("--round", type=int, required=True)
    grant.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    if args.command == "grant":
        if not sys.stdin.isatty():
            parser.error("operator approval requires a TTY on stdin")
        if not args.reason.strip() or not args.subject.strip():
            parser.error("subject and reason must be non-empty")
    try:
        # Use the live authority and policy, including linked-worktree handling.
        from .review_cycle_dispatch import runtime_ledger

        ledger = runtime_ledger(args.repo)
        if args.command == "list":
            records = read_approvals(ledger.path.parent)
            print(json.dumps([record for record in records
                              if args.subject is None or record["subject_branch"] == args.subject], sort_keys=True))
            return 0

        def operation(data):
            from .review_cycle_ledger import LedgerDecision, _Unchanged

            next_round = subject_accounting(data, args.subject, ledger.policy)["charged_rows"][args.kind] + 1
            if args.round < next_round:
                raise _Unchanged(LedgerDecision("ledger_hold", f"round must be at least the subject's next round ({next_round})"))
            record = dict(approval_id=uuid.uuid4().hex, subject_branch=args.subject, kind=args.kind,
                          round=args.round, reason=args.reason.strip(), approved_by=pwd.getpwuid(os.getuid()).pw_name,
                          created_at=datetime.now(timezone.utc).isoformat())
            # The ledger lock serializes the next-round check against reservations.
            # Grant files are atomic; only reserve() can consume one on a new row.
            _write_grant(ledger.path.parent / "review_cycle_approvals", record)
            raise _Unchanged(LedgerDecision("granted", row=record))

        result = ledger._transaction(operation)
        if result.status != "granted":
            parser.error(result.reason)
        print(json.dumps(result.row, sort_keys=True))
        return 0
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
