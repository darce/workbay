"""Bounded public inspect/materialize/preview/apply of a paid native attempt's terminal evidence.

Run with JSON on stdin from the repository. Every operation takes ``operation``
and the original detached ``job_id``. ``materialize`` also requires inspect's
``expected_terminal_revision`` and ``expected_receipt_sha256``; ``apply``
requires preview's ``expected_terminal_revision``, ``expected_receipt_sha256``
and ``expected_evidence_sha256``. No locators, counts, routes, tokens or
results are request parameters: every fact derives from the native producers.

- RemoteJobLedger job record and its spec argv (terminal stage and rc);
- the original pass-state receipt and its first-publication lane_cycle_binding;
- the review-cycle dispatch receipt (``native_dispatch`` nonce/ref, reservation)
  and its pending ledger row;
- the retained artifacts named by the spec argv (source provenance, stream);
- the canonical ``worktree_lanes`` row.

``inspect`` and ``preview`` never create a file, directory or lock and never
rewrite one. ``materialize`` stages the SAME terminal job once through the
original carrier's governed pure staging mode (``terminal_history_materialization``)
and creates only absent spec-named artifacts. ``apply`` publishes ONE versioned
enrichment intent (``terminal_history_contract``) and settles only the same
original pending reservation through ``review_cycle_dispatch.settle_actual``.

Lock order (each acquisition immediate, else a typed hold): job completion
lock -> dispatch receipt lock -> lane publication lease SH -> per-pass
publication lock. Under them the observation is re-derived and compared with
the caller's CAS digests; publication happens inside a short handoff-DB
``BEGIN IMMEDIATE`` after re-reading the lane row, so the lane epoch writer
cannot interleave. No DB or lifecycle lease is held across remote I/O. Nothing
is reserved, spawned, re-executed or model-tested here, and the original
pass-state, dispatch receipt and job ledger bytes are never rewritten.

Canon 0.26.2: DATA-14 no dual writes (one intent; readers re-derive from native
records); CON-11 check-then-act under one fence; GRPH-14 provenance lineage;
SECD-05 fail-safe defaults (typed holds, unknown usage stays unknown); RES-02
bounded waits; AGT-21 typed outcome contract.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from . import offload_pass_recovery as recovery
from . import review_cycle_dispatch as dispatch
from . import terminal_history_contract as contract
from . import terminal_history_materialization as materialization
from .lane_lifecycle_contracts import JOB_ID_RE, RemoteJobIdentity
from .offload_pass import _detached_artifact_paths, _pass_state_path
from .offload_pass_recovery import EvidenceHold
from .remote_job_ledger import RemoteJobLedger
from .review_cycle_ledger import ReviewCycleLedger, _Hold

logger = logging.getLogger(__name__)

_SHA = re.compile(r"[0-9a-f]{64}")
_CAS_FIELDS = ("terminal_revision", "receipt_sha256", "evidence_sha256")
_MATERIALIZE_CAS_FIELDS = ("terminal_revision", "receipt_sha256")
_REVISION_SCHEMA = "workbay/native-terminal-revision@1"
# Reservation facts that settlement never changes.
_RESERVATION_FIELDS = ("dispatch_id", "subject_branch", "kind", "decision", "round", "required_route", "activated_at")
_SETTLEMENT_FIELDS = (
    "dispatch_id",
    "subject_branch",
    "kind",
    "round",
    "outcome_class",
    "completion",
    "settlement_status",
    "settled_sequence",
)
_LANE_FIELDS = ("task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")
_RECORD_LIMIT = 16 * 1024 * 1024
# Immediate contention: a busy fence is a typed hold, never a wait (RES-02).
_FENCE_SECONDS = 0.05


def _hold(reason: str, job_id: str | None = None, **extra: Any) -> dict[str, Any]:
    value: dict[str, Any] = {"status": "hold", "reason": reason}
    if job_id is not None:
        value["job_id"] = job_id
    value.update(extra)
    return value


@dataclass(frozen=True)
class _Observation:
    """One authenticated reading of every producer record for a job."""

    job_id: str
    pass_id: str
    task_ref: str
    lane_id: str
    attempt: int
    rc: int
    spec: dict[str, Any]
    worktree: Path
    pass_path: Path
    receipt_sha256: str
    receipt: dict[str, Any]
    dispatch_receipt_path: Path
    dispatch_receipt_sha256: str
    terminal_revision: str
    lane: dict[str, Any]
    identity: dict[str, str]
    row: dict[str, Any]
    outcome: str
    completion: dict[str, Any]
    artifact_paths: dict[str, Path]
    evidence: recovery.NativeTerminalEvidence | None
    result: dict[str, Any] | None
    canonical: dict[str, Any]


def _expected_completion(outcome: str) -> dict[str, Any]:
    """The completion ``settle_actual(model_started=True)`` records for ``outcome``.

    A started model dominates infrastructure forgiveness, so a non-verdict
    outcome is charged as ``error`` (the native settlement rule).
    """
    from .pass_outcome_class import PassOutcomeClass, classify_pass_outcome  # noqa: PLC0415

    if classify_pass_outcome(outcome) != PassOutcomeClass.VERDICT:
        outcome = "error"
    return {"outcome": outcome, "refusal_reason": None, "blocking_ids": [], "blocking_fingerprints": []}


def _cycle_ledger(repo: Path) -> dict[str, Any]:
    """Read-only, integrity-checked review-cycle ledger (no activation write)."""
    try:
        ledger = ReviewCycleLedger(dispatch.authority(repo) / "review_cycle_ledger.json", dispatch.trusted_policy())
        return ledger._read()
    except (_Hold, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise EvidenceHold("reservation_unavailable") from exc


def _git_tree(repo: Path, head: str) -> str:
    try:
        return dispatch._git(repo, "rev-parse", "--verify", "--quiet", f"{head}^{{tree}}")
    except (OSError, subprocess.SubprocessError):
        return ""


def _same_authority(worktree: Path, repo: Path) -> bool:
    try:
        return dispatch.authority(worktree).resolve() == dispatch.authority(repo).resolve()
    except (OSError, subprocess.SubprocessError):
        return False


def _db_uri(repo: Path, mode: str) -> tuple[str, Path]:
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

    db_path = RuntimeConfig.for_repo(repo).db_path
    return "file:" + quote(str(db_path)) + f"?mode={mode}", Path(db_path)


def _lane_rows(conn: sqlite3.Connection, row_id: int) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(record) for record in conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (row_id,)).fetchall()]


def _lane_cycle_row(repo: Path, binding: dict[str, Any]) -> dict[str, Any]:
    """Canonical ``worktree_lanes`` row via read-only SQLite (no schema/init write)."""
    uri, db_path = _db_uri(repo, "ro")
    try:
        with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=_FENCE_SECONDS)) as conn:
            records = _lane_rows(conn, binding["row_id"])
    except sqlite3.Error as exc:
        raise EvidenceHold("lane_cycle_unavailable", str(db_path)) from exc
    if len(records) != 1:
        raise EvidenceHold("lane_epoch_mismatch", f"worktree_lanes id={binding['row_id']}")
    return records[0]


def _read_record(path: Path, *, missing: str, unreadable: str) -> tuple[dict[str, Any], bytes]:
    try:
        raw = recovery.regular_file_bytes(path, limit=_RECORD_LIMIT)
        value = json.loads(raw)
    except FileNotFoundError as exc:
        raise EvidenceHold(missing, str(path)) from exc
    except (OSError, ValueError) as exc:
        raise EvidenceHold(unreadable, str(path)) from exc
    if not isinstance(value, dict):
        raise EvidenceHold(unreadable, str(path))
    return value, raw


def _job_record(state_dir: Path, job_id: str) -> dict[str, Any]:
    job, _raw = _read_record(
        RemoteJobLedger(state_dir / "remote-jobs")._job_path(job_id),
        missing="job_not_found",
        unreadable="job_ledger_unreadable",
    )
    return job


def _authenticate(repo: Path, state_dir: Path, job_id: str, *, evidence: bool = True) -> _Observation:
    """Derive the whole observation from producer records, or raise a typed hold.

    ``evidence=False`` (inspect/materialize) authenticates every record except
    the retained artifacts, which may be absent until materialized.
    """
    # 1. Native job ledger: a terminal, nonzero, non-admission exit of this identity.
    job = _job_record(state_dir, job_id)
    spec = job.get("spec")
    spec = spec if isinstance(spec, dict) else {}
    task_ref, lane_id, pass_id = spec.get("task_ref"), spec.get("lane_id"), spec.get("pass_id")
    attempt, argv = spec.get("attempt"), spec.get("argv")
    if (
        not all(isinstance(value, str) and value for value in (task_ref, lane_id, pass_id))
        or type(attempt) is not int
        or not isinstance(argv, list)
        or len(argv) < 2
        or argv[1] != "build"
        or spec.get("job_id") != job_id
        or RemoteJobIdentity(task_ref=task_ref, lane_id=lane_id, pass_id=pass_id, attempt=attempt).job_id != job_id
        or not isinstance(spec.get("branch"), str)
    ):
        raise EvidenceHold("job_identity_invalid")
    if job.get("stage") not in {"done", "completed"}:
        raise EvidenceHold("job_not_terminal")
    rc = job.get("rc")
    if job.get("remote_state") == "lost" or type(rc) is not int or rc in (0, 75):
        # rc 0 has the fenced reharvest path; rc 75 never reached the model.
        raise EvidenceHold("job_not_repairable")

    # 2. The original thin receipt, bound to the same identity.
    pass_path = _pass_state_path(state_dir, pass_id)
    pass_state, raw_pass = _read_record(pass_path, missing="pass_receipt_missing", unreadable="pass_receipt_unreadable")
    recorded = pass_state.get("result")
    config = pass_state.get("completion_config")
    worktree_text = pass_state.get("worktree_path")
    if (
        pass_state.get("status") != "done"
        or pass_state.get("job_id") != job_id
        or pass_state.get("pass_id", pass_id) != pass_id
        or (pass_state.get("task_ref"), pass_state.get("lane_id")) != (task_ref, lane_id)
        or type(pass_state.get("attempt")) is not int
        or pass_state["attempt"] != attempt
        or pass_state.get("review_cycle_dispatch_id", job_id) != job_id
        or not isinstance(worktree_text, str)
        or not worktree_text
        or not isinstance(config, dict)
        or not isinstance(recorded, dict)
        or recorded.get("job_id") != job_id
        or any(
            key in recorded and recorded[key] != value
            for key, value in (("pass_id", pass_id), ("task_ref", task_ref), ("lane_id", lane_id))
        )
        or ("attempt" in recorded and (type(recorded["attempt"]) is not int or recorded["attempt"] != attempt))
    ):
        raise EvidenceHold("pass_identity_mismatch", str(pass_path))
    if (
        recorded.get("outcome") != "transport_failure"
        or recorded.get("failed_stage") != "execute"
        or recorded.get("error") != f"detached remote job exited rc={rc}"
    ):
        raise EvidenceHold("terminal_receipt_not_repairable", str(pass_path))
    worktree = Path(worktree_text).expanduser()

    # 3. Host-retained dispatch receipt and the same pending charged reservation.
    receipt_path, receipt, raw_receipt = recovery.read_dispatch_receipt(repo, job_id)
    identity = receipt.get("identity")
    decision = receipt.get("decision")
    reserved = decision.get("row") if isinstance(decision, dict) else None
    if not isinstance(identity, dict) or not isinstance(reserved, dict) or identity.get("dispatch_id") != job_id:
        raise EvidenceHold("dispatch_identity_mismatch", str(receipt_path))
    identity = {key: identity.get(key) for key in ("dispatch_id", "subject_branch", "kind")}
    managed = receipt.get("managed_identity")
    if managed is not None and managed != dict(
        task_ref=task_ref, lane_id=lane_id, pass_id=pass_id, worktree_path=str(worktree.resolve())
    ):
        raise EvidenceHold("pass_identity_mismatch", str(receipt_path))
    row = _cycle_ledger(repo)["rows"].get(job_id)
    if (
        not isinstance(row, dict)
        or row.get("decision") != "reserved"
        or any(row.get(key) != reserved.get(key) for key in _RESERVATION_FIELDS)
        or (row.get("subject_branch"), row.get("kind")) != (identity["subject_branch"], identity["kind"])
    ):
        raise EvidenceHold("reservation_mismatch", str(receipt_path))
    if identity["kind"] == "gate":
        # A gate verdict needs a canonical harvest; retained transport evidence has none.
        raise EvidenceHold("gate_harvest_required")
    outcome = str(recorded["outcome"])
    completion = _expected_completion(outcome)
    if row.get("completion") is not None and row["completion"] != completion:
        raise EvidenceHold("reservation_settled_differently")

    # 4. Lane cycle: the first-publication binding against today's canonical row.
    binding = pass_state.get("lane_cycle_binding")
    if not (
        isinstance(binding, dict)
        and binding.get("v") == 1
        and type(binding.get("row_id")) is int
        and type(binding.get("cycle_epoch")) is int
        and (binding.get("task_ref"), binding.get("lane_id")) == (task_ref, lane_id)
    ):
        raise EvidenceHold("lane_cycle_unavailable", f"{pass_path}: first-publication lane_cycle_binding")
    if (binding.get("branch"), binding.get("worktree_path")) != (spec["branch"], worktree_text) or not _same_authority(
        worktree, repo
    ):
        raise EvidenceHold("pass_identity_mismatch", str(pass_path))
    registered = _lane_cycle_row(repo, binding)
    if any(registered.get(key) != binding.get(key) for key in _LANE_FIELDS):
        raise EvidenceHold("lane_epoch_mismatch", f"worktree_lanes id={binding['row_id']}")
    lane = {"row_id": binding["row_id"], **{key: registered.get(key) for key in _LANE_FIELDS}}
    artifact_paths = _detached_artifact_paths(argv, worktree.resolve())

    terminal_revision = recovery.canonical_sha256(
        {
            "schema": _REVISION_SCHEMA,
            # Terminal facts only: done -> completed bookkeeping is not a new revision.
            "job": {key: job.get(key) for key in ("spec", "rc", "remote_state", "unit", "submitted_at", "done_at")},
            "dispatch_receipt_sha256": recovery.sha256_hex(raw_receipt),
            "reservation": {key: row.get(key) for key in _RESERVATION_FIELDS},
            "lane_cycle": lane,
        }
    )
    canonical: dict[str, Any] = {
        # The identity every producer agreed on (GRPH-14), for the operator.
        "job_id": job_id,
        "pass_id": pass_id,
        "task_ref": task_ref,
        "lane_id": lane_id,
        "attempt": attempt,
        "branch": spec["branch"],
        "rc": rc,
        "lane_cycle": {"row_id": binding["row_id"], "cycle_epoch": binding["cycle_epoch"]},
    }
    native_evidence = None
    proposed = None
    if evidence:
        # 5. Retained native evidence through the shared derivation.
        native_evidence = recovery.derive_native_terminal_evidence(
            job_id=job_id,
            rc=rc,
            spec=spec,
            receipt=receipt,
            artifact_paths=artifact_paths,
            evidence_root=state_dir,
            completion_config=config,
            resolve_tree=lambda head: _git_tree(repo, head),
        )
        proposed = recovery.enriched_terminal_result(recorded, native_evidence)
        enriched_keys = (recovery.ENRICHMENT_KEYS - {"terminal_evidence"}) & recorded.keys()
        first_hold = recorded.get("terminal_evidence")
        if (enriched_keys and recorded != proposed) or (
            not enriched_keys
            and first_hold is not None
            and (not isinstance(first_hold, dict) or first_hold.get("status") != "unavailable")
        ):
            raise EvidenceHold("terminal_history_conflict", str(pass_path))
        canonical.update(
            native_dispatch=dict(native_evidence.native_dispatch),
            source=dict(native_evidence.source),
            route=dict(native_evidence.route),
            review_cycle=dict(native_evidence.review_cycle),
        )
    return _Observation(
        job_id=job_id,
        pass_id=pass_id,
        task_ref=task_ref,
        lane_id=lane_id,
        attempt=attempt,
        rc=rc,
        spec=spec,
        worktree=worktree,
        pass_path=pass_path,
        receipt_sha256=recovery.sha256_hex(raw_pass),
        receipt=receipt,
        dispatch_receipt_path=receipt_path,
        dispatch_receipt_sha256=recovery.sha256_hex(raw_receipt),
        terminal_revision=terminal_revision,
        lane=lane,
        identity=identity,
        row=row,
        outcome=outcome,
        completion=completion,
        artifact_paths=artifact_paths,
        evidence=native_evidence,
        result=proposed,
        canonical=canonical,
    )


def _intent(observation: _Observation) -> dict[str, Any]:
    """The one versioned source-bound enrichment/settlement intent (no clock: replay is identical)."""
    assert observation.evidence is not None and observation.result is not None
    return contract.build_intent(
        canonical=observation.canonical,
        terminal_revision=observation.terminal_revision,
        evidence=observation.evidence,
        original_receipt={"path": str(observation.pass_path), "sha256": observation.receipt_sha256},
        dispatch_receipt={
            "path": str(observation.dispatch_receipt_path),
            "sha256": observation.dispatch_receipt_sha256,
        },
        settlement={
            "identity": dict(observation.identity),
            "round": observation.row.get("round"),
            "outcome": observation.outcome,
            "model_started": True,
            "completion": dict(observation.completion),
        },
        result=observation.result,
    )


def _existing_intent(state_dir: Path, job_id: str, digest: str) -> bool:
    found = recovery.read_enrichment_intents(state_dir, job_id)
    if found is None:
        raise EvidenceHold("enrichment_intent_invalid", str(recovery.intent_dir(state_dir)))
    if any(existing != digest for _path, existing, _intent in found):
        raise EvidenceHold("enrichment_conflict", str(recovery.intent_dir(state_dir)))
    return bool(found)


def _missing_artifacts(observation: _Observation) -> list[str]:
    return sorted(name for name, path in observation.artifact_paths.items() if not os.path.lexists(path))


def _inspect(repo: Path, state_dir: Path, job_id: str) -> dict[str, Any]:
    observation = _authenticate(repo, state_dir, job_id, evidence=False)
    supported, reason = materialization.carrier_supported(observation.spec.get("argv"))
    return {
        "status": "inspect",
        "job_id": job_id,
        "pass_id": observation.pass_id,
        "rc": observation.rc,
        "expected_terminal_revision": observation.terminal_revision,
        "expected_receipt_sha256": observation.receipt_sha256,
        "original_receipt": {"path": str(observation.pass_path), "sha256": observation.receipt_sha256},
        "canonical": observation.canonical,
        "missing_artifacts": _missing_artifacts(observation),
        "materialization": {"supported": supported, "reason": reason},
        "effects": {"rewritten": [], "reserved": False, "executed": False},
    }


def _preview(repo: Path, state_dir: Path, job_id: str) -> dict[str, Any]:
    observation = _authenticate(repo, state_dir, job_id)
    intent = _intent(observation)
    digest = recovery.canonical_sha256(intent)
    published = _existing_intent(state_dir, job_id, digest)
    return {
        "status": "preview",
        "job_id": job_id,
        "pass_id": observation.pass_id,
        "expected_terminal_revision": observation.terminal_revision,
        "expected_receipt_sha256": observation.receipt_sha256,
        "expected_evidence_sha256": intent["evidence_sha256"],
        "original_receipt": dict(intent["original_receipt"]),
        "canonical": observation.canonical,
        "result": observation.result,
        "effects": {
            "intent": {
                "path": str(recovery.intent_path(state_dir, job_id, digest)),
                "sha256": digest,
                "published": published,
            },
            "settlement": {
                **intent["settlement"],
                "status": "pending" if observation.row.get("completion") is None else "settled",
            },
            "rewritten": [],
            "reserved": False,
            "executed": False,
        },
    }


@contextlib.contextmanager
def _dispatch_lock(receipt_path: Path) -> Iterator[None]:
    """The producer's stable dispatch lock, non-blocking; never created here."""
    try:
        with receipt_path.with_suffix(".lock").open("r+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise EvidenceHold("dispatch_busy", str(receipt_path)) from exc
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
    except FileNotFoundError as exc:
        raise EvidenceHold("dispatch_receipt_missing", str(receipt_path)) from exc


@contextlib.contextmanager
def _lane_publication(state_dir: Path, task_ref: str, lane_id: str) -> Iterator[None]:
    """Native per-lane publication permission (SH); the reaper's destructive EX excludes it."""
    from workbay_protocol.lifecycle_coordination import (  # noqa: PLC0415
        LifecycleLeaseTimeout,
        LifecycleOwnershipError,
    )

    from .lane_publication_fence import lane_publication_lease  # noqa: PLC0415

    try:
        lease = lane_publication_lease(state_dir, task_ref, lane_id, deadline=time.monotonic() + _FENCE_SECONDS)
    except (LifecycleLeaseTimeout, LifecycleOwnershipError) as exc:
        raise EvidenceHold("lane_publication_busy") from exc
    with lease:
        if lease.borrowed:
            raise EvidenceHold("lane_publication_destructive_owner")
        yield


@contextlib.contextmanager
def _pass_publication(pass_path: Path) -> Iterator[None]:
    """The same per-pass EX ``write_pass_state`` takes after lane SH."""
    from .lane_publication_fence import LanePassPublicationRefused  # noqa: PLC0415
    from .offload_pass import _pass_publication_lock  # noqa: PLC0415

    try:
        with _pass_publication_lock(pass_path, deadline=time.monotonic() + _FENCE_SECONDS):
            yield
    except LanePassPublicationRefused as exc:
        if exc.reason != "lane_pass_publication_busy":
            raise
        raise EvidenceHold("pass_publication_busy", str(pass_path)) from exc


@contextlib.contextmanager
def _fenced(repo: Path, state_dir: Path, job_id: str) -> Iterator[None]:
    """Completion -> dispatch -> lane SH -> pass EX, all immediate (CON-11, RES-02)."""
    from .remote_job_reconcile import RemoteJobReconcileError, _locked  # noqa: PLC0415

    jobs = state_dir / "remote-jobs"
    if not jobs.is_dir():
        raise EvidenceHold("job_not_found")
    # Unlocked identity read only names the lock resources; everything is
    # re-derived under them before any CAS or publication.
    spec = _job_record(state_dir, job_id).get("spec")
    task_ref, lane_id, pass_id = (spec.get(key) for key in ("task_ref", "lane_id", "pass_id")) if isinstance(
        spec, dict
    ) else (None, None, None)
    if not all(isinstance(value, str) and value for value in (task_ref, lane_id, pass_id)):
        raise EvidenceHold("job_identity_invalid")
    receipt_path = recovery.dispatch_receipt_path(repo, job_id)
    with contextlib.ExitStack() as stack:
        try:
            stack.enter_context(_locked(jobs / f".{job_id}.completion.lock", timeout_seconds=0.0))
        except RemoteJobReconcileError as exc:
            raise EvidenceHold("completion_busy") from exc
        stack.enter_context(_dispatch_lock(receipt_path))
        stack.enter_context(_lane_publication(state_dir, task_ref, lane_id))
        stack.enter_context(_pass_publication(_pass_state_path(state_dir, pass_id)))
        yield


@contextlib.contextmanager
def _epoch_fence(repo: Path, observation: _Observation) -> Iterator[None]:
    """Publish while the lane-cycle writer is excluded and the row still matches.

    A ``BEGIN IMMEDIATE`` on the canonical handoff DB takes its write lock, so
    a status/epoch change either commits before the re-read (typed
    ``lane_epoch_mismatch``) or waits until this short, local-only publication
    ends. No row is written; the transaction always rolls back.
    """
    uri, db_path = _db_uri(repo, "rw")
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=_FENCE_SECONDS, isolation_level=None)
    except sqlite3.Error as exc:
        raise EvidenceHold("lane_cycle_unavailable", str(db_path)) from exc
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise EvidenceHold("lane_epoch_busy", str(db_path)) from exc
        try:
            records = _lane_rows(conn, observation.lane["row_id"])
            if len(records) != 1 or any(records[0].get(key) != observation.lane[key] for key in _LANE_FIELDS):
                raise EvidenceHold("lane_epoch_mismatch", f"worktree_lanes id={observation.lane['row_id']}")
            yield
        finally:
            conn.execute("ROLLBACK")
    finally:
        conn.close()


def _cas(request: dict[str, Any], observation: _Observation, fields: tuple[str, ...]) -> str | None:
    actual = {
        "terminal_revision": observation.terminal_revision,
        "receipt_sha256": observation.receipt_sha256,
        "evidence_sha256": observation.evidence.evidence_sha256 if observation.evidence is not None else None,
    }
    for field in fields:
        if request[f"expected_{field}"] != actual[field]:
            return f"stale_{field}"
    return None


def _publish_intent(state_dir: Path, job_id: str, digest: str, raw: bytes) -> None:
    """One content-addressed 0600 intent in a private directory, created exclusively."""
    root = recovery.intent_dir(state_dir)
    from .remote_job_ledger import _mkdir_durable  # noqa: PLC0415

    _mkdir_durable(root)
    os.chmod(root, 0o700)
    target = recovery.intent_path(state_dir, job_id, digest)
    fd, temporary = tempfile.mkstemp(prefix=f".{job_id}.", suffix=".intent", dir=root)
    try:
        os.fchmod(fd, contract.INTENT_MODE)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if recovery.regular_file_bytes(target, root=root) != raw:
                raise EvidenceHold("enrichment_conflict", str(target)) from None
    finally:
        Path(temporary).unlink(missing_ok=True)
    directory = os.open(root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _durable_response(repo: Path, state_dir: Path, observation: _Observation) -> dict[str, Any]:
    """Derive the apply answer only from durable state: the intent and the ledger row."""
    found = recovery.applied_enrichment(state_dir, observation.job_id, receipt_sha256=observation.receipt_sha256)
    if found is None:
        raise EvidenceHold("enrichment_intent_invalid", str(recovery.intent_dir(state_dir)))
    path, digest, intent = found
    row = _cycle_ledger(repo)["rows"].get(observation.job_id)
    payload = {
        "job_id": intent["job_id"],
        "pass_id": intent["pass_id"],
        "expected_terminal_revision": intent["terminal_revision"],
        "expected_receipt_sha256": intent["original_receipt"]["sha256"],
        "expected_evidence_sha256": intent["evidence_sha256"],
        "original_receipt": dict(intent["original_receipt"]),
        "canonical": intent.get("canonical"),
        "intent": {"path": str(path), "sha256": digest},
        "result": intent["result"],
    }
    if not isinstance(row, dict) or row.get("completion") != intent["settlement"]["completion"]:
        return {"status": "hold", "reason": "settlement_pending", **payload}
    return {"status": "applied", **payload, "settlement": {key: row.get(key) for key in _SETTLEMENT_FIELDS}}


def _apply(repo: Path, state_dir: Path, request: dict[str, Any]) -> dict[str, Any]:
    job_id = request["job_id"]
    with _fenced(repo, state_dir, job_id):
        # CON-11: every check and the act below run under the same fence.
        observation = _authenticate(repo, state_dir, job_id)
        stale = _cas(request, observation, _CAS_FIELDS)
        if stale is not None:
            return _hold(stale, job_id)
        intent = _intent(observation)
        raw = recovery.canonical_bytes(intent)
        digest = recovery.sha256_hex(raw)
        if not _existing_intent(state_dir, job_id, digest):
            with _epoch_fence(repo, observation):
                _publish_intent(state_dir, job_id, digest, raw)
        if observation.row.get("completion") is None:
            try:
                # Module attribute lookup: the one native settlement producer.
                dispatch.settle_actual(repo, dict(observation.identity), outcome=observation.outcome, model_started=True)
            except Exception as exc:  # noqa: BLE001 — the durable intent converges on the next apply
                logger.warning("terminal evidence settlement deferred job_id=%s: %s", job_id, type(exc).__name__)
        return _durable_response(repo, state_dir, observation)


def _materialize(repo: Path, state_dir: Path, request: dict[str, Any]) -> dict[str, Any]:
    job_id = request["job_id"]
    with _fenced(repo, state_dir, job_id):
        observation = _authenticate(repo, state_dir, job_id, evidence=False)
        stale = _cas(request, observation, _MATERIALIZE_CAS_FIELDS)
        if stale is not None:
            return _hold(stale, job_id)
        missing = _missing_artifacts(observation)
        transport = "not_needed"
        artifacts: dict[str, dict[str, Any]] = {}
        if any(name in missing for name in recovery.REQUIRED_ARTIFACTS):
            # Remote I/O happens under the per-job/pass fences only, never under the DB lock.
            staged = materialization.stage(
                repo=repo,
                state_dir=state_dir,
                job_id=job_id,
                spec=observation.spec,
                rc=observation.rc,
                receipt=observation.receipt,
            )
            transport = "collected"
            with _epoch_fence(repo, observation):
                artifacts = materialization.publish(staged, observation.artifact_paths, evidence_root=state_dir)
        else:
            for name, path in sorted(observation.artifact_paths.items()):
                if name in missing:
                    continue
                data = recovery.regular_file_bytes(path, root=state_dir)
                artifacts[name] = {"path": str(path), "sha256": recovery.sha256_hex(data), "bytes": len(data)}
        return {
            "status": "materialized",
            "job_id": job_id,
            "pass_id": observation.pass_id,
            "rc": observation.rc,
            "transport": transport,
            "artifacts": artifacts,
            "effects": {"rewritten": [], "reserved": False, "executed": False, "stamped": False},
        }


_OPERATIONS = {
    "inspect": (),
    "preview": (),
    "materialize": _MATERIALIZE_CAS_FIELDS,
    "apply": _CAS_FIELDS,
}


def repair(request: dict, *, repo: Path, state_dir: Path) -> dict:
    """Typed public seam: read-only ``inspect``/``preview`` or fenced ``materialize``/``apply``."""
    job_id = request.get("job_id") if isinstance(request, dict) else None
    job_id = job_id if isinstance(job_id, str) and JOB_ID_RE.fullmatch(job_id) else None
    try:
        operation = request.get("operation") if isinstance(request, dict) else None
        if operation not in _OPERATIONS or job_id is None:
            return _hold("invalid_protocol", job_id)
        fields = _OPERATIONS[operation]
        if set(request) - {"operation", "job_id", *(f"expected_{field}" for field in fields)} or any(
            not isinstance(request.get(f"expected_{field}"), str) or not _SHA.fullmatch(request[f"expected_{field}"])
            for field in fields
        ):
            return _hold("invalid_protocol", job_id)
        repo, state_dir = Path(repo).resolve(), Path(state_dir)
        if operation == "inspect":
            return _inspect(repo, state_dir, job_id)
        if operation == "preview":
            return _preview(repo, state_dir, job_id)
        if operation == "materialize":
            return _materialize(repo, state_dir, request)
        return _apply(repo, state_dir, request)
    except EvidenceHold as exc:
        extra: dict[str, Any] = {"required_host_evidence": exc.needed} if exc.needed else {}
        if isinstance(getattr(exc, "transport_rc", None), int):
            extra["transport_rc"] = exc.transport_rc  # type: ignore[attr-defined]
        return _hold(exc.reason, job_id, **extra)
    except (_Hold, OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError, sqlite3.Error,
            subprocess.SubprocessError) as exc:
        # Never interpolate arbitrary exception text from producer bytes.
        return _hold(str(exc) if isinstance(exc, _Hold) else type(exc).__name__, job_id)


def main() -> None:
    try:
        request = json.load(sys.stdin)
        from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415

        repo = Path.cwd()
        # The state authority is deployment configuration, never a request field.
        result = repair(request, repo=repo, state_dir=RuntimeConfig.for_repo(repo).state_dir)
    except (ValueError, TypeError, OSError):
        result = _hold("invalid_protocol")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
