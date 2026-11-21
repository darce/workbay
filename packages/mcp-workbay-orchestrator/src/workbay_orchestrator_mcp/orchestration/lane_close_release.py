"""Consume native close evidence at the two review-cycle holder boundaries."""

from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
import subprocess
import time
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.enums import TERMINAL_LANE_STATUSES
from workbay_handoff_mcp.lane_close_evidence import (
    CloseEvidence,
    ClosureIdentity,
    append_close_evidence,
    is_post_upgrade_closure,
    read_close_evidence,
)
from workbay_handoff_mcp.shared_schema import HANDOFF_SCHEMA_VERSION, connect_handoff_db, handoff_schema_version
from workbay_handoff_mcp.shared_write_context import acquire_flock
from workbay_protocol.lifecycle_coordination import LifecycleLeaseTimeout

from . import review_cycle_dispatch as cycle
from .lane_close_backfill import _issuer, _open_history_snapshot
from .lane_close_observation import (
    COMBINED_WITNESS,
    CloseObservationHold,
    check_combined_history,
    check_measurement_generation,
    check_pass_opening,
    check_production_history,
    collect_close_measurement,
    collect_historical_close,
    historical_close_matches,
    read_dispatch_receipt,
    read_native_ledger,
    receipt_names_issuer,
    require,
    utc_now,
)
from .lane_publication_fence import lane_publication_lease
from .review_cycle_ledger import _Hold
from .review_cycle_reconciliation import EvidenceHold, _dispatch_lock, _holder_ledger, _host

_EMPTY = "closed_without_contribution"
_UNAVAILABLE = (
    EvidenceHold,
    LifecycleLeaseTimeout,
    _Hold,
    OSError,
    ValueError,
    KeyError,
    TypeError,
    AttributeError,
    sqlite3.Error,
    subprocess.SubprocessError,
)


def _fence(locks: ExitStack, runtime: RuntimeConfig, closure: ClosureIdentity) -> None:
    lease = locks.enter_context(
        lane_publication_lease(
            runtime.state_dir,
            closure.task_ref,
            closure.lane_id,
            exclusive=True,
            deadline=time.monotonic() + 0.1,
        )
    )
    require(not lease.borrowed, "issuer_fence_busy")


def _open_db(locks: ExitStack, runtime: RuntimeConfig, *, writer: bool = False) -> sqlite3.Connection:
    db = runtime.db_path
    info = db.stat(follow_symlinks=False)
    require(
        db.resolve(strict=True) == db
        and stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and not info.st_mode & 0o022,
        "handoff_custody_unproven",
    )
    if writer:
        conn = locks.enter_context(closing(connect_handoff_db(db, existing_only=True, deadline=time.monotonic() + 1)))
        conn.execute("BEGIN IMMEDIATE")
    else:
        conn = _open_history_snapshot(locks, db, read_only=True)
    require(handoff_schema_version(conn) == HANDOFF_SCHEMA_VERSION, "close_evidence_cutover_required")
    return conn


def _records(conn: sqlite3.Connection, closure: ClosureIdentity) -> dict:
    require(_issuer(conn, closure.fields()) == closure, "issuer_identity_changed")
    return read_close_evidence(conn, closure)


def _eligible_measurement(conn: sqlite3.Connection, closure: ClosureIdentity, records: dict) -> bool:
    if "legacy_registration" in records or not is_post_upgrade_closure(conn, closure):
        return False
    older = conn.execute(
        "SELECT 1 FROM worktree_lane_close_evidence WHERE task_ref=? AND lane_id=? "
        "AND lane_row_id=? AND identity_seq=? AND kind='legacy_registration' LIMIT 1",
        (closure.task_ref, closure.lane_id, closure.row_id, closure.identity_seq),
    ).fetchone()
    return older is None


def _receipt_ids(private: Path, closure: ClosureIdentity) -> list[str]:
    directory = private / "review_cycle_dispatch"
    return sorted(
        path.stem
        for path in directory.glob("*.json")
        if receipt_names_issuer(read_dispatch_receipt(path, private)[0], closure)
    )


def _observe(
    primary: Path,
    private: Path,
    runtime: RuntimeConfig,
    conn: sqlite3.Connection,
    closure: ClosureIdentity,
    receipt_ids: list[str],
    data: dict,
    records: dict,
    previous: CloseEvidence | None,
) -> dict:
    if previous is not None and previous.kind == "historical_backfill":
        enrollment = records.get("legacy_registration")
        require(
            enrollment is not None and enrollment.record_id == previous.facts["legacy_registration_id"],
            "issuer_not_enrolled",
        )
        facts = collect_historical_close(
            primary,
            private,
            runtime.state_dir,
            closure,
            receipt_ids,
            data,
            previous=previous.facts,
        )
        if facts.get("witness_kind") != COMBINED_WITNESS:
            check_pass_opening(conn, closure, facts)
            check_production_history(conn, closure, facts["base_sha"])
            return facts
    else:
        require(_eligible_measurement(conn, closure, records), "issuer_not_eligible_for_measurement")
        check_measurement_generation(conn, closure)
        facts = collect_close_measurement(
            primary,
            private,
            runtime.state_dir,
            closure,
            receipt_ids,
            data,
            previous=previous.facts if previous else None,
        )
    check_pass_opening(conn, closure, facts)
    facts["sqlite_source"] = check_combined_history(conn, closure, facts["base_sha"])
    return facts


def verified_close_release(
    repo: Path,
    path: Path,
    receipt: dict,
    digest: str,
    proof: dict,
    *,
    holder: cycle._ReceiptHolder | None = None,
) -> CloseEvidence | None:
    """Authenticate a holder against its exact terminal closure, measuring once if eligible."""
    branch = receipt.get("source_branch")
    if (
        receipt.get("state") != "transport_started"
        or receipt.get("identity", {}).get("kind") not in {"implement", "gate"}
    ):
        return None
    try:
        primary, private = _host(repo)
        if holder is None:
            holder = cycle._receipt_holder(primary, receipt, branch)
        lane = holder.lane
        if (
            holder.mismatch
            or holder.owner_unproven
            or lane is None
            or lane.get("status") not in TERMINAL_LANE_STATUSES
            or lane["status"] == "merged"
            or lane.get("branch") != branch
        ):
            return None
        closure = ClosureIdentity.from_lane(lane)
        runtime = RuntimeConfig.for_repo(primary)
        if not receipt_names_issuer(receipt, closure) or path.parent != private / "review_cycle_dispatch":
            return None
        with ExitStack() as locks:
            _fence(locks, runtime, closure)
            reader = _open_db(locks, runtime)
            records = _records(reader, closure)
            record = records.get("measurement") or records.get("historical_backfill")
            if record is None and not _eligible_measurement(reader, closure, records):
                return None
            if record is not None and record.facts["disposition"] != _EMPTY:
                return None
            receipt_ids = record.facts["receipt_ids"] if record else _receipt_ids(private, closure)
            if path.stem not in receipt_ids:
                return None
            for stem in receipt_ids:
                locks.enter_context(_dispatch_lock(path.with_name(stem + ".json")))
            ledger = _holder_ledger(primary)
            lock = locks.enter_context(ledger.path.with_suffix(".json.lock").open("rb"))
            acquire_flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            current_receipt, source = read_dispatch_receipt(path, private)
            if current_receipt != receipt or source["sha256"] != digest:
                return None
            data = read_native_ledger(ledger, private)
            try:
                facts = _observe(primary, private, runtime, reader, closure, receipt_ids, data, records, record)
            except CloseObservationHold as exc:
                if record is not None:
                    return None
                facts = dict(
                    repo=str(primary),
                    receipt_ids=receipt_ids,
                    disposition="unknown",
                    reasons=[str(exc)],
                    terminal_tip_sha=None,
                    observation=exc.diagnostic,
                )
            if record is None:
                reader.rollback()
                writer = _open_db(locks, runtime, writer=True)
                with writer:
                    current = _records(writer, closure)
                    require(
                        current == records and _eligible_measurement(writer, closure, current),
                        "issuer_identity_changed",
                    )
                    if facts["disposition"] == _EMPTY:
                        check_measurement_generation(writer, closure)
                        check_pass_opening(writer, closure, facts)
                        require(
                            check_combined_history(writer, closure, facts["base_sha"]) == facts["sqlite_source"],
                            "close_observation_changed",
                        )
                    record = append_close_evidence(writer, CloseEvidence("measurement", closure, utc_now(), facts))
            if (
                record.facts["disposition"] == _EMPTY
                and facts["integration_branch"] == proof["integration_branch"]
                and record.facts["receipt_sha256s"].get(path.stem) == digest
                and historical_close_matches(record.facts, facts)
            ):
                return record
    except _UNAVAILABLE:
        return None
    return None


@contextmanager
def close_admission_verifier(repo: Path, records: list[CloseEvidence]):
    """Hold issuer and receipt fences through the native reserve transaction."""
    try:
        primary, private = _host(repo)
        runtime = RuntimeConfig.for_repo(primary)
        admitted = sorted(
            {record.record_id: record for record in records}.values(),
            key=lambda record: (record.closure.task_ref, record.closure.lane_id),
        )
        with ExitStack() as locks:
            # The same ordering as collection/backfill: lane, receipts, ledger.
            for record in admitted:
                _fence(locks, runtime, record.closure)
            for stem in sorted({stem for record in admitted for stem in record.facts["receipt_ids"]}):
                locks.enter_context(_dispatch_lock(private / "review_cycle_dispatch" / (stem + ".json")))

            def verify(data: dict) -> None:
                """Remeasure using the ledger data already locked by reserve."""
                try:
                    for record in admitted:
                        closure = record.closure
                        with ExitStack() as readers:
                            conn = _open_db(readers, runtime)
                            current = _records(conn, closure)
                            require(current.get(record.kind) == record, "close_observation_changed")
                            facts = _observe(
                                primary,
                                private,
                                runtime,
                                conn,
                                closure,
                                record.facts["receipt_ids"],
                                data,
                                current,
                                record,
                            )
                            require(historical_close_matches(record.facts, facts), "close_observation_changed")
                except _UNAVAILABLE as exc:
                    raise _Hold("review_cycle_close_evidence_changed") from exc

            yield verify
    except _UNAVAILABLE as exc:
        raise _Hold("review_cycle_close_evidence_changed") from exc
