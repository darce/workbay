"""Local operator entry point for a measured historical close."""

from __future__ import annotations

import fcntl
import json
import os
import pwd
import sqlite3
import stat
import subprocess
import time
from contextlib import ExitStack, closing
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from workbay_handoff_mcp.config import RuntimeConfig
from workbay_handoff_mcp.enums import TERMINAL_LANE_STATUSES
from workbay_handoff_mcp.import_export import registration_history
from workbay_handoff_mcp.lane_close_evidence import (
    CloseEvidence,
    ClosureIdentity,
    EvidenceStoreError,
    append_close_evidence,
    read_close_evidence,
)
from workbay_handoff_mcp.shared_schema import HANDOFF_SCHEMA_VERSION, connect_handoff_db, handoff_schema_version
from workbay_handoff_mcp.shared_write_context import acquire_flock
from workbay_protocol.lifecycle_coordination import LifecycleLeaseTimeout

from .lane_close_observation import (
    COMBINED_WITNESS,
    DIGEST,
    OID,
    CloseObservationHold,
    check_combined_history,
    check_pass_opening,
    check_production_history,
    collect_historical_close,
    historical_close_matches,
    read_native_ledger,
    require,
    unique_json,
    utc_now,
)
from .lane_publication_fence import lane_publication_lease
from .review_cycle_ledger import _Hold
from .review_cycle_reconciliation import _dispatch_lock, _holder_ledger, _host


def read_identifiers(path: Path, *, sources: list | None = None) -> dict:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "invalid_record", disposition="denied")
            raw = stream.read(65537)
        require(len(raw) <= 65536, "invalid_record", disposition="denied")
        data = json.loads(raw, object_pairs_hook=unique_json)
    except CloseObservationHold:
        raise
    except (OSError, ValueError) as exc:
        raise CloseObservationHold("invalid_record", disposition="denied") from exc
    require(isinstance(data, dict), "invalid_record", disposition="denied")
    for field in data:
        require(
            field in {"task_ref", "lane_id", "receipt_ids", "expected_tip"},
            "unknown_field",
            field=field,
            disposition="denied",
        )
    for field in ("task_ref", "lane_id"):
        value = data.get(field)
        require(
            isinstance(value, str) and value.strip() and "\0" not in value,
            "invalid_identifier",
            field=field,
            disposition="denied",
        )
    receipts = data.get("receipt_ids")
    require(
        isinstance(receipts, list)
        and receipts
        and all(isinstance(value, str) and DIGEST.fullmatch(value) for value in receipts)
        and len(set(receipts)) == len(receipts),
        "invalid_identifier",
        field="receipt_ids",
        disposition="denied",
    )
    if "expected_tip" in data:
        tip = data["expected_tip"]
        require(
            isinstance(tip, str) and OID.fullmatch(tip),
            "invalid_identifier",
            field="expected_tip",
            disposition="denied",
        )
    if sources is not None:
        sources.append(dict(locator=str(path.absolute()), schema="backfill-close-identifiers@1",
                            bytes=len(raw), sha256=sha256(raw).hexdigest()))
    return {**data, "receipt_ids": sorted(receipts)}


def _issuer(conn: sqlite3.Connection, data: dict) -> ClosureIdentity:
    row = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (data["task_ref"], data["lane_id"])
    ).fetchone()
    if row is None:
        archived = [item for item in registration_history(conn, data["task_ref"]) if item["lane_id"] == data["lane_id"]]
        require(len(archived) <= 1, "issuer_registration_ambiguous")
        row = archived[0] if archived else None
    require(
        row is not None and row["status"] in TERMINAL_LANE_STATUSES,
        "issuer_not_closed",
        disposition="denied",
    )
    require(row["status"] != "merged", "issuer_merged", disposition="contradiction")
    require(row["landing_commit_sha"] is None, "issuer_landing_present", disposition="contradiction")
    require(dict(row).get("reclaim_fence") is None, "issuer_fence_busy")
    return ClosureIdentity.from_lane(row)


def _check_prior(records: dict) -> None:
    require("measurement" not in records, "issuer_has_close_measurement", disposition="denied")


def _open_history_snapshot(locks: ExitStack, db: Path, *, read_only: bool) -> sqlite3.Connection:
    """Bound SQL lock waits independently of filesystem and Git observation time."""
    conn = locks.enter_context(closing(connect_handoff_db(db, read_only=read_only, existing_only=not read_only)))
    conn.execute("PRAGMA busy_timeout=1000")
    if read_only:
        conn.execute("PRAGMA query_only=ON")
    # The read snapshot covers all corroboration. For backfill, WAL promotes
    # it at the append or refuses the write if a concurrent commit superseded it.
    conn.execute("BEGIN")
    return conn


def _backfill(repo: Path, data: dict, request_source: dict) -> dict:
    primary, authority = _host(repo)
    runtime = RuntimeConfig.for_repo(primary)
    db = runtime.db_path
    info = db.stat(follow_symlinks=False)
    require(
        db.resolve(strict=True) == db
        and stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and not info.st_mode & 0o022,
        "handoff_custody_unproven",
    )
    with ExitStack() as locks:
        reader = locks.enter_context(closing(connect_handoff_db(db, read_only=True, deadline=time.monotonic() + 1)))
        require(handoff_schema_version(reader) == HANDOFF_SCHEMA_VERSION, "close_evidence_cutover_required")
        reader.execute("BEGIN")
        closure = _issuer(reader, data)
        records = read_close_evidence(reader, closure)
        _check_prior(records)
        enrollment = records.get("legacy_registration")
        old = records.get("historical_backfill")
        if enrollment is None:
            earlier = reader.execute(
                "SELECT 1 FROM worktree_lane_close_evidence WHERE kind='legacy_registration' "
                "AND task_ref=? AND lane_id=? LIMIT 1",
                (closure.task_ref, closure.lane_id),
            ).fetchone()
            require(earlier is None, "issuer_reopened", source="sqlite:worktree_lane_close_evidence")
        reader.rollback()
        lease = locks.enter_context(
            lane_publication_lease(
                runtime.state_dir,
                data["task_ref"],
                data["lane_id"],
                exclusive=True,
                deadline=time.monotonic() + 1,
            )
        )
        require(not lease.borrowed, "issuer_fence_busy")
        ledger = _holder_ledger(primary)
        receipts_dir = authority / "review_cycle_dispatch"
        for stem in data["receipt_ids"]:
            locks.enter_context(_dispatch_lock(receipts_dir / (stem + ".json")))
        # Native producers acquire receipt before ledger. Keep both until commit.
        lock = locks.enter_context(ledger.path.with_suffix(".json.lock").open("rb"))
        acquire_flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        ledger_sources = []
        ledger_data = read_native_ledger(ledger, authority, sources=ledger_sources)

        def observe() -> dict:
            return collect_historical_close(
                primary, authority, runtime.state_dir, closure, data["receipt_ids"], ledger_data,
                ledger_source=ledger_sources[0], previous=old.facts if old else None,
            )

        facts = observe()
        combined = facts.get("witness_kind") == COMBINED_WITNESS
        expected = data.get("expected_tip")
        derived_tip = facts["base_sha"] if combined else facts["terminal_tip_sha"]
        require(
            expected is None or expected == derived_tip,
            "expected_tip_mismatch",
            disposition="unknown" if combined else "contradiction",
        )
        require(enrollment is not None, "issuer_not_enrolled", disposition="unknown" if combined else "denied")
        if not combined:
            require(
                datetime.fromisoformat(facts["terminal_observed_at"]) <= datetime.fromisoformat(enrollment.created_at),
                "terminal_cut_after_enrollment",
                terminal_tip_sha=None,
            )
        require(facts == observe(), "close_observation_changed")
        git_checked_at = utc_now()
        if db.stat(follow_symlinks=False).st_ino != info.st_ino:
            raise CloseObservationHold("handoff_identity_changed")
        writer = _open_history_snapshot(locks, db, read_only=False)
        with writer:
            require(handoff_schema_version(writer) == HANDOFF_SCHEMA_VERSION, "close_evidence_cutover_required")
            require(_issuer(writer, data) == closure, "issuer_identity_changed")
            check_pass_opening(writer, closure, facts)
            if combined:
                facts["sqlite_source"] = check_combined_history(writer, closure, facts["base_sha"])
            else:
                check_production_history(writer, closure, facts["base_sha"])
            records = read_close_evidence(writer, closure)
            _check_prior(records)
            require(records.get("legacy_registration") == enrollment, "issuer_identity_changed")
            old = records.get("historical_backfill")
            if old:
                # The live probe already proved ancestry at the current head.
                # A later integration commit does not alter the original audit.
                require(
                    historical_close_matches(old.facts, facts),
                    "close_observation_changed",
                )
                return dict(backfill_id=old.record_id)
            if combined:
                facts["source_manifest"].append(request_source)
                facts["file_sha256s"][request_source["locator"]] = request_source["sha256"]
            uid = os.getuid()
            record = CloseEvidence(
                "historical_backfill",
                closure,
                utc_now(),
                {
                    **facts,
                    "input": data,
                    "git_checked_at": git_checked_at,
                    "legacy_registration_id": enrollment.record_id,
                    "operator_uid": uid,
                    "operator_login": pwd.getpwuid(uid).pw_name,
                },
            )
            return dict(backfill_id=append_close_evidence(writer, record).record_id)


def backfill_close(repo: Path, path: Path, *, local_tty: bool) -> tuple[int, dict]:
    """Return a typed refusal or the ID of the committed immutable record."""
    if not local_tty or os.getuid() != os.geteuid():
        return 2, dict(reason="local_operator_required", disposition="denied")
    try:
        sources = []
        data = read_identifiers(path, sources=sources)
        return 0, _backfill(repo, data, sources[0])
    except CloseObservationHold as exc:
        return 2, exc.diagnostic
    except EvidenceStoreError as exc:
        return 2, dict(reason=str(exc), disposition="unknown")
    except LifecycleLeaseTimeout:
        return 2, dict(reason="issuer_fence_busy", disposition="unknown")
    except BlockingIOError:
        return 2, dict(reason="native_evidence_busy", disposition="unknown")
    except (
        _Hold,
        KeyError,
        AttributeError,
        OSError,
        RuntimeError,
        ValueError,
        TypeError,
        sqlite3.Error,
        subprocess.SubprocessError,
    ) as exc:
        return 2, dict(reason="close_evidence_unavailable", disposition="unknown", error_type=type(exc).__name__)
