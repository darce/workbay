"""Startup reconciliation for detached remote jobs.

``orchestrator_daemon.orchestrator_loop`` calls ``emit_daemon_startup_warning``
and exposes no startup hook this module can join without editing that file.
Call ``reconcile_remote_jobs_at_startup`` once at daemon startup.

The pass attempt is the fencing token (RES-10, lexicons/engineering.md:121): a
presented fence with no job_id or attempt (``missing_fence``), a non-int
attempt (``malformed_fence``; a bool is not an int), a different job_id
(``fence_mismatch``), or an older attempt (``stale_attempt``) is refused and
never applied. VM jobs the ledger does not contain are written as typed orphans
and collected under the quarantine directory; their results are not applied.
Legacy attached sandboxes
are left to the rescue_probe path. A ledger read error other than
``job_not_found`` sets ``ok`` false and refuses the row as ``ledger_read_error``
with the exception class. A record with no spec identity is refused as
``spec_identity_missing``.

Exactly-once completion is a claim on the adoption record (RES-10). The claim
is written under the adoption lock before ``complete_pass`` and cleared only
after that call returns. Each claim carries a unique fence token (RES-10,
lexicons/engineering.md:121). While the owner is inside ``complete_pass`` it
renews that lease through the whole completion budget (status, artifact
collection, configured engine timeout, and margin). Production completion
enforces the same deadline before persistence and holds a per-job lock while
synchronous calls unwind, including after lease expiry. Renew, finalize, and release compare
the token, so a stale owner cannot finalize after a takeover and a takeover
cannot land while the owner is still renewing. A finalize whose token does not
match is refused as ``stale_fence`` and is not recorded as applied (RES-13,
lexicons/engineering.md:124). A crash leaves ``state=applying`` with the owner
pid, the fence token, a monotonic timestamp, and a lease. A live owner (pid
alive and lease unexpired) is refused as ``claim_held``. A dead owner, including
one whose lease has already expired, marks an already terminal ledger row
applied without calling ``complete_pass`` again, and otherwise retries that
idempotent completion. Adoption records with no claim field stay readable.

A failed renew is a suspicion and not proof that the fence is lost (RES-21).
The heartbeat retries every ``_RENEW_RETRY_SECONDS`` until it is stopped or the
completion deadline passes (RES-13), and stops early only when the record is
applied or another owner holds the fence token. A private enum distinguishes
renewed, applied, absent, other_owner, and invalid. Uncertain outcomes become
a typed ``RemoteJobReconcileError`` (``adoption_unreadable``,
``claim_lease_invalid``, ``claim_absent``) so callers can retry. Every retry and every give-up is logged with an ``event=`` name. A
renew error before ``complete_pass`` releases the claim, records a
``completion_errors`` entry and lets the sweep continue with the next job.

The VM sweep subprocess has a bounded timeout (RES-02). A failed sweep is
reported as failed and is not treated as an empty VM (OBS-08). Any unreadable
line fails the whole sweep with ``sweep_status`` ``failed`` and ``sweep_error``
``sweep_unreadable``: an object line that is not JSON, a row with a bad
``job_id``, a non-object JSON value such as ``[]`` or ``null``, and a fragment
of a row. ``invalid_lines`` counts them, so a partial inventory is never
applied (OBS-08, lexicons/engineering.md:478). Only a line that is neither JSON
nor an inventory fragment is agent log noise, counted in ``ingest_skips``.
A report that still reaches ingestion unreadable is refused as
``report_unreadable`` and sets ``ok`` false. Quarantine
records older than ``QUARANTINE_RETENTION_SECONDS`` are removed in the same
pass that records new orphans (RES-07, lexicons/engineering.md:118). A directory
counts as purged only once it is gone. A failed removal is a warning and a
``purge_failed`` entry, and a job rewritten earlier in the same pass is kept.
"""

from __future__ import annotations

import errno
import fcntl
import json
import logging
import os
import secrets
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, NamedTuple, Sequence

from .lane_lifecycle_contracts import JOB_ID_RE
from .remote_job_ledger import RemoteJobLedger, RemoteJobLedgerError

_logger = logging.getLogger(__name__)

SWEEP_TIMEOUT_SECONDS = 30.0
QUARANTINE_RETENTION_SECONDS = 7 * 24 * 3600
ORPHAN_KIND = "vm_job_missing_from_ledger"
SWEEP_UNREADABLE = "sweep_unreadable"
_TERMINAL_STATES = frozenset({"done", "lost"})
_LEGACY_ATTACHED_KINDS = frozenset({"attached", "attached_sandbox", "legacy_attached"})
_LOCK_WAIT_SECONDS = 5.0
_LOCK_POLL_SECONDS = 0.01
_CLAIM_LEASE_SECONDS = 120.0
# Pause after a failed renew. The retry never waits longer than a renew period
# (lease / 3), so a transient failure cannot let the lease lapse unnoticed.
_RENEW_RETRY_SECONDS = 2.0
# Keep persistent renew failures visible without flooding logs for long jobs.
_RENEW_RETRY_LOG_EVERY = 30
# Compatibility bound for injected completion callbacks. Production derives its
# budget from completion_timeout_seconds and enforces it in complete_remote_pass.
_COMPLETION_DEADLINE_SECONDS = 360.0
_LEDGER_TERMINAL_STAGES = frozenset({"completed", "dead_letter"})
_thread_locks: dict[str, threading.Lock] = {}
_thread_locks_guard = threading.Lock()
# Held by tests while the adoption lock is already held, so a second caller
# can prove it cannot enter the claim write.
_claim_critical_hook: Callable[[], None] | None = None


class RemoteJobReconcileError(Exception):
    """Typed refusal for a reconciler precondition or lock wait."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def reconcile_remote_jobs_at_startup(
    *,
    state_dir: str | os.PathLike[str],
    orchestrator_root: str | os.PathLike[str],
    vm_jobs: Sequence[Mapping[str, Any]] | None = None,
    sweep: Callable[[], object] | None = None,
    sweep_argv: Sequence[str] | None = None,
    sweep_cwd: str | None = None,
    sweep_timeout: float = SWEEP_TIMEOUT_SECONDS,
    complete_pass: Callable[..., object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Re-adopt running ledger jobs and quarantine VM jobs missing from the ledger.

    ``vm_jobs`` is an in-process sweep report. Otherwise ``sweep`` or
    ``sweep_argv`` supplies that report. With none of them set, ledger jobs are
    still re-adopted and ``orphans_known`` stays false: the VM was not observed.
    """

    resolved_state = Path(state_dir)
    resolved_root = Path(orchestrator_root)
    now = _clock_now(clock)
    result: dict[str, Any] = {
        "ok": True,
        "adopted": [],
        "completed": [],
        "refused": [],
        "orphans": [],
        "skipped_legacy_attached": [],
        "sweep_status": "not_run",
        "sweep_error": None,
        "orphans_known": False,
        "invalid_lines": 0,
        "ingest_skips": 0,
        "purged": [],
        "purge_failures": [],
        "completion_errors": [],
    }
    ledger = RemoteJobLedger(resolved_state / "remote-jobs")
    applied_ids: set[str] = set()
    rewritten_ids: set[str] = set()
    _readopt_running_jobs(
        ledger,
        state_dir=resolved_state,
        now=now,
        result=result,
    )
    jobs = _load_sweep(
        result,
        vm_jobs=vm_jobs,
        sweep=sweep,
        sweep_argv=sweep_argv,
        sweep_cwd=sweep_cwd,
        sweep_timeout=sweep_timeout,
    )
    if jobs is None:
        return result
    known = _known_job_ids(ledger)
    finisher = complete_pass or _default_complete
    for report in jobs:
        _ingest_report(
            report,
            ledger=ledger,
            known=known,
            state_dir=resolved_state,
            orchestrator_root=resolved_root,
            now=now,
            result=result,
            complete_pass=finisher,
            applied_ids=applied_ids,
            rewritten_ids=rewritten_ids,
        )
    purged, purge_failures = _purge_quarantine(
        resolved_state / "remote-jobs" / "quarantine",
        now,
        skip_ids=rewritten_ids,
    )
    result["purged"] = purged
    result["purge_failures"] = purge_failures
    if purge_failures:
        result["ok"] = False
    return result


def _readopt_running_jobs(
    ledger: RemoteJobLedger,
    *,
    state_dir: Path,
    now: datetime,
    result: dict[str, Any],
) -> None:
    seen: set[str] = set()
    for record in ledger.open_jobs():
        identity = _spec_identity(record)
        if identity is None:
            job_id, _pass_id, attempt = _spec_identity_parts(record)
            _refuse_missing_spec_identity(result, job_id=job_id, attempt=attempt)
            continue
        job_id, pass_id, attempt = identity
        pass_state = _pass_state(state_dir, pass_id)
        if pass_state.get("status") != "running":
            continue
        fence_reason = _fence_reason(pass_state, job_id=job_id, attempt=attempt)
        if fence_reason is not None:
            _refuse_presented(
                result,
                pass_state,
                job_id=job_id,
                attempt=attempt,
                reason=fence_reason,
            )
            continue
        _write_adoption(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=False,
        )
        if job_id not in seen:
            seen.add(job_id)
            result["adopted"].append({"job_id": job_id, "attempt": attempt, "pass_id": pass_id})


def _ingest_report(
    report: object,
    *,
    ledger: RemoteJobLedger,
    known: set[str],
    state_dir: Path,
    orchestrator_root: Path,
    now: datetime,
    result: dict[str, Any],
    complete_pass: Callable[..., object],
    applied_ids: set[str],
    rewritten_ids: set[str],
) -> None:
    job_id = _ingest_job_id(report, result)
    if job_id is None or not isinstance(report, Mapping):
        return  # _ingest_job_id already refused it
    if _legacy_attached(report):
        if job_id not in result["skipped_legacy_attached"]:
            result["skipped_legacy_attached"].append(job_id)
        return
    if job_id not in known:
        _quarantine_orphan(state_dir, report, job_id=job_id, now=now, result=result, rewritten_ids=rewritten_ids)
        return
    record = _ingest_ledger_record(
        ledger, report, job_id=job_id, state_dir=state_dir, now=now, result=result, rewritten_ids=rewritten_ids
    )
    if record is None:
        return
    _ingest_known_report(
        report,
        record,
        job_id=job_id,
        state_dir=state_dir,
        orchestrator_root=orchestrator_root,
        now=now,
        result=result,
        complete_pass=complete_pass,
        applied_ids=applied_ids,
    )


def _ingest_job_id(report: object, result: dict[str, Any]) -> str | None:
    """Return the report's job_id, or refuse the report as ``report_unreadable``.

    ``_validate_sweep_rows`` already fails the sweep for such a row, so this is
    the second line of defence: an unreadable report is never skipped silently.
    """

    job_id = report.get("job_id") if isinstance(report, Mapping) else None
    if isinstance(job_id, str) and JOB_ID_RE.fullmatch(job_id) is not None:
        return job_id
    result["ok"] = False
    result["orphans_known"] = False
    result["invalid_lines"] += 1
    result["refused"].append({"reason": "report_unreadable"})
    _logger.warning(
        "remote job reconcile refused event=remote_job_report_unreadable reason=report_unreadable report_type=%s",
        type(report).__name__,
    )
    return None


def _ingest_ledger_record(
    ledger: RemoteJobLedger,
    report: Mapping[str, Any],
    *,
    job_id: str,
    state_dir: Path,
    now: datetime,
    result: dict[str, Any],
    rewritten_ids: set[str],
) -> Mapping[str, Any] | None:
    try:
        return ledger.read_job(job_id)
    except RemoteJobLedgerError as exc:
        if exc.reason == "job_not_found":
            _quarantine_orphan(state_dir, report, job_id=job_id, now=now, result=result, rewritten_ids=rewritten_ids)
            return None
        _refuse_ledger_read(result, report, job_id, exc)
        return None
    except Exception as exc:  # noqa: BLE001 — a ledger read failure must stay visible
        _refuse_ledger_read(result, report, job_id, exc)
        return None


def _ingest_known_report(
    report: Mapping[str, Any],
    record: Mapping[str, Any],
    *,
    job_id: str,
    state_dir: Path,
    orchestrator_root: Path,
    now: datetime,
    result: dict[str, Any],
    complete_pass: Callable[..., object],
    applied_ids: set[str],
) -> None:
    identity = _spec_identity(record)
    if identity is None:
        _refuse(result, job_id=job_id, attempt=_presented_attempt(report, 0), reason="spec_identity_missing")
        return
    _job_id, pass_id, attempt = identity
    if not _ingest_fence_allows(report, state_dir, result, job_id=job_id, pass_id=pass_id, attempt=attempt):
        return
    _write_adoption(state_dir, job_id=job_id, attempt=attempt, pass_id=pass_id, now=now, applied=False)
    if report.get("state") not in _TERMINAL_STATES:
        return
    _apply_once(
        job_id,
        attempt=attempt,
        pass_id=pass_id,
        state_dir=state_dir,
        orchestrator_root=orchestrator_root,
        now=now,
        result=result,
        complete_pass=complete_pass,
        applied_ids=applied_ids,
    )


def _ingest_fence_allows(
    report: Mapping[str, Any],
    state_dir: Path,
    result: dict[str, Any],
    *,
    job_id: str,
    pass_id: str,
    attempt: int,
) -> bool:
    pass_state = _pass_state(state_dir, pass_id)
    # RES-10: the pass attempt is the only token allowed to apply, including
    # after this same startup has already finished the current attempt.
    fence_reason = _fence_reason(pass_state, job_id=job_id, attempt=attempt)
    if fence_reason is not None:
        _refuse_presented(result, pass_state, job_id=job_id, attempt=attempt, reason=fence_reason)
        return False
    report_reason = _fence_reason(report, job_id=job_id, attempt=attempt)
    if report_reason is not None:
        _refuse_presented(result, report, job_id=job_id, attempt=attempt, reason=report_reason)
        return False
    return pass_state.get("status") == "running"


class _Renew(Enum):
    renewed = "renewed"
    applied = "applied"
    absent = "absent"
    other_owner = "other_owner"
    invalid = "invalid"


@dataclass(frozen=True)
class _HeldClaim:
    """A completion claim this process owns, with the fence token that proves it (RES-10)."""

    state_dir: Path
    job_id: str
    attempt: int
    pass_id: str
    now: datetime
    fence_token: str

    def _fence(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "attempt": self.attempt,
            "pass_id": self.pass_id,
            "now": self.now,
            "fence_token": self.fence_token,
        }

    def renew(self) -> tuple[_Renew, float | None]:
        return _renew_completion_claim(self.state_dir, **self._fence())

    def release(self) -> bool:
        return _release_own_claim(self.state_dir, **self._fence())

    def finish(self) -> bool:
        return _finish_applied_claim(self.state_dir, **self._fence())

    def start_heartbeat(
        self, stop: threading.Event, started: float, completion_deadline: float | None
    ) -> threading.Thread:
        beater = threading.Thread(
            target=_heartbeat_completion_claim,
            kwargs={
                **self._fence(),
                "state_dir": self.state_dir,
                "started": started,
                "stop": stop,
                "completion_deadline": completion_deadline,
            },
            name="remote-job-claim-heartbeat",
            daemon=True,
        )
        beater.start()
        return beater


class _Completion(NamedTuple):
    """What ran under the claim: a lost fence, a failure and its phase, or the outcome."""

    lost_fence: bool = False
    error: Exception | None = None
    phase: str = "complete_pass"
    outcome: object = None


class _Release(Enum):
    RELEASED = "released"
    NOT_OWNED = "not_owned"
    FAILED = "failed"


def _apply_once(
    job_id: str,
    *,
    attempt: int,
    pass_id: str,
    state_dir: Path,
    orchestrator_root: Path,
    now: datetime,
    result: dict[str, Any],
    complete_pass: Callable[..., object],
    applied_ids: set[str],
) -> None:
    if job_id in applied_ids:
        return
    try:
        budget = _completion_budget(complete_pass, state_dir, pass_id)
    except ValueError as exc:
        result["ok"] = False
        result["completion_errors"].append({"job_id": job_id, "reason": str(exc)})
        return
    claim = _acquire_for_completion(
        job_id, attempt=attempt, pass_id=pass_id, state_dir=state_dir, now=now, result=result, applied_ids=applied_ids
    )
    if claim is not None:
        _complete_under_claim(
            claim,
            orchestrator_root=orchestrator_root,
            result=result,
            complete_pass=complete_pass,
            applied_ids=applied_ids,
            completion_budget=budget,
        )


def _completion_budget(complete_pass: Callable[..., object], state_dir: Path, pass_id: str) -> float | None:
    """Production completion carries its configured budget; injected callbacks keep the compatibility bound."""

    if complete_pass is not _default_complete:
        return None
    from .offload_pass import completion_timeout_seconds  # noqa: PLC0415

    return completion_timeout_seconds(_pass_state(state_dir, pass_id))


def _acquire_for_completion(
    job_id: str,
    *,
    attempt: int,
    pass_id: str,
    state_dir: Path,
    now: datetime,
    result: dict[str, Any],
    applied_ids: set[str],
) -> _HeldClaim | None:
    """Return the claim only when this call acquired it; any other decision is settled here."""

    decision, fence_token = _acquire_completion_claim(
        state_dir, job_id=job_id, attempt=attempt, pass_id=pass_id, now=now
    )
    if decision == "skip":
        return None
    if decision == "held":
        _refuse(result, job_id=job_id, attempt=attempt, reason="claim_held")
        return None
    if decision == "recovered":
        applied_ids.add(job_id)
        result["completed"].append(job_id)
        return None
    if not isinstance(fence_token, str) or not fence_token:
        _refuse(result, job_id=job_id, attempt=attempt, reason="stale_fence")
        result["ok"] = False
        return None
    return _HeldClaim(state_dir, job_id, attempt, pass_id, now, fence_token)


def _complete_under_claim(
    claim: _HeldClaim,
    *,
    orchestrator_root: Path,
    result: dict[str, Any],
    complete_pass: Callable[..., object],
    applied_ids: set[str],
    completion_budget: float | None,
) -> None:
    """Run ``complete_pass`` while a heartbeat keeps the lease, then leave through the fence (RES-10)."""

    stop = threading.Event()
    started = time.monotonic()
    deadline = started + completion_budget if completion_budget is not None else None
    beater = claim.start_heartbeat(stop, started, deadline)
    try:
        done = _run_under_claim(
            claim,
            orchestrator_root=orchestrator_root,
            complete_pass=complete_pass,
            started=started,
            completion_deadline=deadline,
        )
    finally:
        _stop_claim_heartbeat(stop, beater)
    if done.lost_fence:
        _refuse_stale_fence(result, claim)
    elif done.error is not None:
        _fail_completion(result, claim, done)
    elif not isinstance(done.outcome, dict) or done.outcome.get("terminal") is not True:
        _settle_nonterminal_claim(result, claim)
    else:
        _finalize_completion(result, claim, applied_ids)


def _settle_nonterminal_claim(result: dict[str, Any], claim: _HeldClaim) -> None:
    released = _release_claim(claim)
    match released:
        case _Release.RELEASED:
            return
        case _Release.NOT_OWNED:
            _refuse_stale_fence(result, claim)
        case _Release.FAILED:
            result["ok"] = False
            result["completion_errors"].append({"job_id": claim.job_id, "reason": "claim_release_failed"})


def _finalize_completion(result: dict[str, Any], claim: _HeldClaim, applied_ids: set[str]) -> None:
    try:
        finished = claim.finish()
    except Exception as exc:  # noqa: BLE001 — finalization failure must not stop the sweep
        result["ok"] = False
        result["completion_errors"].append(
            {"job_id": claim.job_id, "reason": f"finalize: {type(exc).__name__}: {exc}"[:500]}
        )
        _logger.warning(
            "remote job reconcile finalize failed event=remote_job_claim_finalize_failed job_id=%s reason=%s",
            claim.job_id,
            getattr(exc, "reason", type(exc).__name__),
        )
        return
    if finished:
        applied_ids.add(claim.job_id)
        result["completed"].append(claim.job_id)
    else:
        _refuse_stale_fence(result, claim)


def _run_under_claim(
    claim: _HeldClaim,
    *,
    orchestrator_root: Path,
    complete_pass: Callable[..., object],
    started: float,
    completion_deadline: float | None,
) -> _Completion:
    """Pre-flight renew, then ``complete_pass``. A renew error is a failure, not a lost fence."""

    phase = "renew"
    try:
        if _within_completion_deadline(started):
            outcome, _lease = claim.renew()
            match outcome:
                case _Renew.applied | _Renew.other_owner:
                    return _Completion(lost_fence=True)
                case _Renew.invalid:
                    raise RemoteJobReconcileError("claim_lease_invalid")
                case _Renew.absent:
                    raise RemoteJobReconcileError("claim_absent")
                case _Renew.renewed:
                    pass
        phase = "complete_pass"
        kwargs: dict[str, Any] = {"state_dir": claim.state_dir, "orchestrator_root": orchestrator_root}
        if completion_deadline is not None:
            kwargs["completion_deadline"] = completion_deadline
        return _Completion(outcome=complete_pass(claim.job_id, **kwargs))
    except Exception as exc:  # noqa: BLE001 — a renew or completion failure must stay visible and retryable
        return _Completion(error=exc, phase=phase)


def _refuse_stale_fence(result: dict[str, Any], claim: _HeldClaim) -> None:
    _refuse(result, job_id=claim.job_id, attempt=claim.attempt, reason="stale_fence")
    result["ok"] = False


def _fail_completion(result: dict[str, Any], claim: _HeldClaim, done: _Completion) -> None:
    """Record a failed pre-flight renew or completion and give the claim back (RES-13)."""

    error_type = type(done.error).__name__
    _logger.warning(
        "remote job reconcile completion failed job_id=%s phase=%s error_type=%s", claim.job_id, done.phase, error_type
    )
    released = _release_claim(claim)
    reason = f"{error_type}: {done.error}"[:500]
    if released is _Release.FAILED:
        reason += "; claim_release_failed"
    result["ok"] = False
    result["completion_errors"].append({"job_id": claim.job_id, "reason": reason})
    if released is _Release.NOT_OWNED and getattr(done.error, "reason", None) != "claim_absent":
        _refuse_stale_fence(result, claim)


def _release_claim(claim: _HeldClaim) -> _Release:
    """Clear our claim. A release that raises is logged and reported, never raised."""

    try:
        owned = claim.release()
    except Exception as exc:  # noqa: BLE001 — a failed release must not stop the sweep
        _logger.warning(
            "remote job reconcile claim release failed event=remote_job_claim_release_failed job_id=%s error_type=%s",
            claim.job_id,
            type(exc).__name__,
        )
        return _Release.FAILED
    return _Release.RELEASED if owned else _Release.NOT_OWNED


def _quarantine_orphan(
    state_dir: Path,
    report: Mapping[str, Any],
    *,
    job_id: str,
    now: datetime,
    result: dict[str, Any],
    rewritten_ids: set[str],
) -> None:
    quarantine_root = state_dir / "remote-jobs" / "quarantine"
    destination = quarantine_root / job_id
    record_path = destination / "orphan.json"
    with _locked(quarantine_root / ".lock"):
        previous = _read_json(record_path) or {}
        recorded_at = previous.get("recorded_at")
        if not isinstance(recorded_at, str) or _parse_time(recorded_at) is None:
            recorded_at = now.isoformat()
        attempt = report.get("attempt")
        if isinstance(attempt, bool) or not isinstance(attempt, int):
            attempt = None
        rc = report.get("rc")
        if isinstance(rc, bool) or not isinstance(rc, int):
            rc = None
        payload = {
            "kind": ORPHAN_KIND,
            "job_id": job_id,
            "attempt": attempt,
            "state": report.get("state") if isinstance(report.get("state"), str) else None,
            "rc": rc,
            "applied": False,
            "recorded_at": recorded_at,
            "reason": "not_in_ledger",
        }
        _write_json(record_path, payload)
        artifact = report.get("artifact")
        if isinstance(artifact, str):
            artifact = artifact.encode("utf-8")
        if isinstance(artifact, bytes):
            _write_bytes(destination / "artifact.bin", artifact)
    rewritten_ids.add(job_id)
    result["orphans"].append({"job_id": job_id, "kind": ORPHAN_KIND, "path": str(destination)})


def _purge_quarantine(
    quarantine_root: Path,
    now: datetime,
    *,
    skip_ids: set[str],
) -> tuple[list[str], list[dict[str, Any]]]:
    if not quarantine_root.is_dir():
        return [], []
    removed: list[str] = []
    failures: list[dict[str, Any]] = []
    retention = timedelta(seconds=QUARANTINE_RETENTION_SECONDS)
    with _locked(quarantine_root / ".lock"):
        for child in sorted(path for path in quarantine_root.iterdir() if path.is_dir()):
            if JOB_ID_RE.fullmatch(child.name) is None:
                continue
            if child.name in skip_ids:
                continue
            record = _read_json(child / "orphan.json")
            recorded_at = _parse_time(record.get("recorded_at")) if isinstance(record, dict) else None
            if recorded_at is None or now - recorded_at < retention:
                continue
            error_type = _unlink_quarantine(child)
            if error_type is not None or child.exists():
                if error_type is None:
                    error_type = _warn_purge_failure(child, "OSError")
                failures.append({"job_id": child.name, "reason": "purge_failed", "error_type": error_type})
                continue
            removed.append(child.name)
    return removed, failures


def _load_sweep(
    result: dict[str, Any],
    *,
    vm_jobs: Sequence[Mapping[str, Any]] | None,
    sweep: Callable[[], object] | None,
    sweep_argv: Sequence[str] | None,
    sweep_cwd: str | None,
    sweep_timeout: float,
) -> list[Mapping[str, Any]] | None:
    if vm_jobs is not None:
        if isinstance(vm_jobs, (str, bytes)) or not isinstance(vm_jobs, Sequence):
            _fail_sweep(result, "invalid_sweep")
            return None
        jobs, invalid = _validate_sweep_rows(vm_jobs)
        result["invalid_lines"] = invalid
        if invalid:
            _fail_sweep(result, SWEEP_UNREADABLE)
            return None
        result["sweep_status"] = "provided"
        result["orphans_known"] = True
        return jobs
    if sweep is not None:
        try:
            loaded = sweep()
        except Exception as exc:  # noqa: BLE001 — sweep failure must not look like an empty VM
            _logger.warning("remote job reconcile sweep failed error_type=%s", type(exc).__name__)
            _fail_sweep(result, f"{type(exc).__name__}: {exc}")
            return None
        if isinstance(loaded, (str, bytes)) or not isinstance(loaded, Sequence):
            _fail_sweep(result, "invalid_sweep")
            return None
        jobs, invalid = _validate_sweep_rows(loaded)
        result["invalid_lines"] = invalid
        if invalid:
            _fail_sweep(result, SWEEP_UNREADABLE)
            return None
        result["sweep_status"] = "provided"
        result["orphans_known"] = True
        return jobs
    if sweep_argv is not None:
        return _run_sweep_command(
            result,
            sweep_argv=sweep_argv,
            sweep_cwd=sweep_cwd,
            sweep_timeout=sweep_timeout,
        )
    return None


def _validate_sweep_rows(rows: Sequence[object]) -> tuple[list[Mapping[str, Any]], int]:
    jobs: list[Mapping[str, Any]] = []
    invalid = 0
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or not isinstance(row.get("job_id"), str)
            or JOB_ID_RE.fullmatch(row.get("job_id")) is None
        ):
            invalid += 1
            continue
        jobs.append(row)
    return jobs, invalid


def _run_sweep_command(
    result: dict[str, Any],
    *,
    sweep_argv: Sequence[str],
    sweep_cwd: str | None,
    sweep_timeout: float,
) -> list[Mapping[str, Any]] | None:
    if isinstance(sweep_timeout, bool) or not isinstance(sweep_timeout, (int, float)) or sweep_timeout <= 0:
        _fail_sweep(result, "invalid_timeout")
        return None
    if not sweep_argv or not isinstance(sweep_cwd, str) or not sweep_cwd:
        _fail_sweep(result, "invalid_sweep_command")
        return None
    command = [str(part) for part in sweep_argv]
    try:
        completed = subprocess.run(
            command,
            cwd=sweep_cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=float(sweep_timeout),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _logger.warning("remote job reconcile sweep failed error_type=%s", type(exc).__name__)
        _fail_sweep(result, "sweep_timeout")
        return None
    except OSError as exc:
        _logger.warning("remote job reconcile sweep failed error_type=%s", type(exc).__name__)
        _fail_sweep(result, f"{type(exc).__name__}: {exc}")
        return None
    if completed.returncode != 0:
        _fail_sweep(result, f"sweep_exit:{completed.returncode}")
        return None
    jobs, invalid, skipped = _jobs_from_sweep_stdout(completed.stdout or "")
    result["invalid_lines"] = invalid
    result["ingest_skips"] = skipped
    if invalid:
        _fail_sweep(result, SWEEP_UNREADABLE)
        return None
    result["sweep_status"] = "provided"
    result["orphans_known"] = True
    return jobs


def _jobs_from_sweep_stdout(stdout: str) -> tuple[list[Mapping[str, Any]], int, int]:
    rows: list[object] = []
    invalid = 0
    skipped = 0
    for line in stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        if not text.startswith("{"):
            if _is_inventory_text(text):
                invalid += 1
                _logger.warning("remote job reconcile sweep line unreadable reason=non_object_row")
            else:
                skipped += 1
            continue
        try:
            payload = json.loads(text)
        except (ValueError, RecursionError) as exc:
            invalid += 1
            _logger.warning("remote job reconcile sweep line unreadable error_type=%s", type(exc).__name__)
            continue
        rows.append(payload)
    jobs, invalid_rows = _validate_sweep_rows(rows)
    return jobs, invalid + invalid_rows, skipped


def _is_inventory_text(text: str) -> bool:
    """True for JSON values or fragments starting with an array or job_id key.

    A bare ``[]`` or ``null`` is a non-object report and a ``"job_id"`` fragment is
    a row cut at its head. Dropping either would let a truncated sweep pass as a
    complete one (OBS-08), so only a line that is neither is log noise.
    """

    if text.startswith(("[", '"job_id"')):
        return True
    try:
        json.loads(text)
    except json.JSONDecodeError:
        return False
    except (ValueError, RecursionError):
        return True
    return True


def _fail_sweep(result: dict[str, Any], error: str) -> None:
    result["ok"] = False
    result["sweep_status"] = "failed"
    result["sweep_error"] = error
    result["orphans_known"] = False


def _default_complete(
    job_id: str, *, state_dir: Path, orchestrator_root: Path, completion_deadline: float | None = None
) -> dict[str, Any]:
    from .offload_pass import complete_remote_pass  # noqa: PLC0415

    outcome = complete_remote_pass(
        job_id, state_dir=state_dir, orchestrator_root=orchestrator_root, completion_deadline=completion_deadline
    )
    if not isinstance(outcome, dict):
        return {"ok": False, "terminal": False, "job_id": job_id}
    return outcome


def _pass_state(state_dir: Path, pass_id: str) -> dict[str, Any]:
    from .offload_pass import read_pass_state  # noqa: PLC0415

    loaded = read_pass_state(state_dir, pass_id)
    return loaded if isinstance(loaded, dict) else {}


def _spec_identity(record: Mapping[str, Any]) -> tuple[str, str, int] | None:
    job_id, pass_id, attempt = _spec_identity_parts(record)
    if job_id is None or pass_id is None or attempt is None:
        return None
    return job_id, pass_id, attempt


def _spec_identity_parts(record: Mapping[str, Any]) -> tuple[str | None, str | None, int | None]:
    spec = record.get("spec")
    if not isinstance(spec, Mapping):
        return None, None, None
    job_id = spec.get("job_id")
    pass_id = spec.get("pass_id")
    attempt = spec.get("attempt")
    valid_job_id = job_id if isinstance(job_id, str) and JOB_ID_RE.fullmatch(job_id) is not None else None
    valid_pass_id = pass_id if isinstance(pass_id, str) and pass_id else None
    valid_attempt = attempt if not isinstance(attempt, bool) and isinstance(attempt, int) and attempt >= 0 else None
    return valid_job_id, valid_pass_id, valid_attempt


def _refuse_missing_spec_identity(result: dict[str, Any], *, job_id: str | None, attempt: int | None) -> None:
    if job_id is not None and attempt is not None:
        _refuse(result, job_id=job_id, attempt=attempt, reason="spec_identity_missing")
        return
    row: dict[str, Any] = {"reason": "spec_identity_missing"}
    if job_id is not None:
        row["job_id"] = job_id
    if attempt is not None:
        row["attempt"] = attempt
    result["refused"].append(row)
    if job_id is None:
        _logger.warning("remote job reconcile refused reason=spec_identity_missing")
    else:
        _logger.warning("remote job reconcile refused job_id=%s reason=spec_identity_missing", job_id)


def _fence_matches(pass_state: Mapping[str, Any], job_id: str, attempt: int) -> bool:
    """Return True only when ``pass_state`` carries this job's current attempt.

    A missing job_id or attempt is not a match. A bool is not an int (RES-10).
    """

    fenced_job = pass_state.get("job_id")
    if "job_id" not in pass_state or not isinstance(fenced_job, str) or not fenced_job:
        return False
    if "attempt" not in pass_state or pass_state.get("attempt") is None:
        return False
    fenced_attempt = pass_state.get("attempt")
    if isinstance(fenced_attempt, bool) or not isinstance(fenced_attempt, int):
        return False
    if fenced_job != job_id:
        return False
    if fenced_attempt != attempt:
        return False
    return True


def _fence_reason(presented: Mapping[str, Any], *, job_id: str, attempt: int) -> str | None:
    """Return the typed fence refusal, or None when the fence is current.

    ``missing_fence`` — no job_id, or no attempt.
    ``malformed_fence`` — attempt is present but not an int (a bool is not an int).
    ``fence_mismatch`` — job_id differs from the row.
    ``stale_attempt`` — attempt is older than the row's current attempt.
    A newer attempt is not the current token and is refused the same way.
    """

    if _fence_matches(presented, job_id, attempt):
        return None
    presented_job = presented.get("job_id")
    if "job_id" not in presented or presented_job is None or presented_job == "":
        return "missing_fence"
    if not isinstance(presented_job, str):
        return "malformed_fence"
    if "attempt" not in presented or presented.get("attempt") is None:
        return "missing_fence"
    presented_attempt = presented.get("attempt")
    if isinstance(presented_attempt, bool) or not isinstance(presented_attempt, int):
        return "malformed_fence"
    if presented_job != job_id:
        return "fence_mismatch"
    if presented_attempt < attempt:
        return "stale_attempt"
    return "stale_attempt"


def _legacy_attached(report: Mapping[str, Any]) -> bool:
    if report.get("legacy_attached") is True:
        return True
    kind = report.get("sandbox_kind")
    if kind is None:
        kind = report.get("kind")
    return isinstance(kind, str) and kind in _LEGACY_ATTACHED_KINDS


def _presented_attempt(presented: Mapping[str, Any], fallback: int) -> int:
    value = presented.get("attempt")
    if isinstance(value, bool) or not isinstance(value, int):
        return fallback
    return value


def _refuse_presented(
    result: dict[str, Any],
    presented: Mapping[str, Any],
    *,
    job_id: str,
    attempt: int,
    reason: str,
) -> None:
    _refuse(result, job_id=job_id, attempt=_presented_attempt(presented, attempt), reason=reason)


def _refuse(
    result: dict[str, Any],
    *,
    job_id: str,
    attempt: int,
    reason: str,
    error_type: str | None = None,
) -> None:
    row: dict[str, Any] = {"job_id": job_id, "attempt": attempt, "reason": reason}
    if error_type is None:
        _logger.warning("remote job reconcile refused job_id=%s reason=%s", job_id, reason)
    else:
        row["error_type"] = error_type
        _logger.warning(
            "remote job reconcile refused job_id=%s reason=%s error_type=%s",
            job_id,
            reason,
            error_type,
        )
    result["refused"].append(row)


def _refuse_ledger_read(
    result: dict[str, Any],
    report: Mapping[str, Any],
    job_id: str,
    exc: BaseException,
) -> None:
    result["ok"] = False
    _refuse(
        result,
        job_id=job_id,
        attempt=_presented_attempt(report, 0),
        reason="ledger_read_error",
        error_type=type(exc).__name__,
    )


def _adoption_path(state_dir: Path, job_id: str) -> Path:
    return state_dir / "remote-jobs" / "adoptions" / f"{job_id}.json"


def _adoption_applied(state_dir: Path, job_id: str, attempt: int) -> bool:
    record = _read_json(_adoption_path(state_dir, job_id))
    if not isinstance(record, dict):
        return False
    return record.get("applied") is True and record.get("applied_attempt") == attempt


def _claim_now_ns() -> int:
    """The monotonic clock every claim stamp and lease check reads (one seam for tests)."""

    return time.monotonic_ns()


def _new_claim(now: datetime) -> dict[str, Any]:
    return {
        "state": "applying",
        "owner_pid": os.getpid(),
        "fence_token": secrets.token_hex(16),
        "monotonic_ns": _claim_now_ns(),
        "lease_seconds": _CLAIM_LEASE_SECONDS,
        "claimed_at": now.isoformat(),
    }


def _claim_is_live(claim: object) -> bool:
    """True when the owner pid is alive and the monotonic lease has not expired."""

    if not isinstance(claim, Mapping) or claim.get("state") != "applying":
        return False
    pid = claim.get("owner_pid")
    stamp = claim.get("monotonic_ns")
    lease = claim.get("lease_seconds")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    if isinstance(stamp, bool) or not isinstance(stamp, int) or stamp < 0:
        return False
    if isinstance(lease, bool) or not isinstance(lease, (int, float)) or lease <= 0:
        return False
    now_ns = _claim_now_ns()
    if now_ns < stamp:
        return False
    if now_ns - stamp >= int(float(lease) * 1_000_000_000):
        return False
    return _pid_alive(pid)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        _logger.warning("remote job reconcile pid probe failed pid=%s error_type=%s", pid, type(exc).__name__)
        return False
    return True


def _ledger_is_terminal(state_dir: Path, job_id: str) -> bool:
    ledger = RemoteJobLedger(state_dir / "remote-jobs")
    try:
        record = ledger.read_job(job_id)
    except RemoteJobLedgerError as exc:
        _logger.warning(
            "remote job reconcile ledger read failed job_id=%s error_type=%s",
            job_id,
            type(exc).__name__,
        )
        return False
    stage = record.get("stage")
    return isinstance(stage, str) and stage in _LEDGER_TERMINAL_STAGES


def _pause_claim_window() -> None:
    hook = _claim_critical_hook
    if hook is not None:
        hook()


def _acquire_completion_claim(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
) -> tuple[str, str | None]:
    """Claim the completion, or report that a live owner already holds it.

    The claim is durable before this returns ``"acquired"``. ``complete_pass``
    runs only after the adoption lock is released. The second item is the fence
    token of the claim this call installed.
    """

    path = _adoption_path(state_dir, job_id)
    with _locked(path.parent / ".lock"):
        if _adoption_applied(state_dir, job_id, attempt):
            return "skip", None
        current = _read_json(path) or {}
        claim = current.get("claim")
        if _claim_is_live(claim):
            return "held", None
        if isinstance(claim, dict) and claim.get("state") == "applying" and _ledger_is_terminal(state_dir, job_id):
            _write_adoption_unlocked(
                state_dir,
                job_id=job_id,
                attempt=attempt,
                pass_id=pass_id,
                now=now,
                applied=True,
                keep_claim=False,
            )
            return "recovered", None
        _pause_claim_window()
        fresh = _new_claim(now)
        _write_adoption_unlocked(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=False,
            claim=fresh,
            keep_claim=False,
        )
        return "acquired", fresh["fence_token"]


def _claim_token_matches(claim: object, fence_token: str) -> bool:
    if not isinstance(fence_token, str) or not fence_token:
        return False
    if not isinstance(claim, Mapping) or claim.get("state") != "applying":
        return False
    token = claim.get("fence_token")
    return isinstance(token, str) and token == fence_token


def _stored_lease_seconds(claim: Mapping[str, Any]) -> float | None:
    lease = claim.get("lease_seconds")
    if isinstance(lease, bool) or not isinstance(lease, (int, float)) or lease <= 0:
        return None
    return float(lease)


def _read_adoption_strict(path: Path) -> dict[str, Any]:
    """Read an adoption record. Absent is ``{}``; an unreadable one is a typed error.

    ``_read_json`` maps both to ``None``, which reads as "no claim" and would end
    fencing on a transient read failure.
    """

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise RemoteJobReconcileError("adoption_unreadable") from exc
    if not isinstance(payload, dict):
        raise RemoteJobReconcileError("adoption_unreadable")
    return payload


def _within_completion_deadline(started: float) -> bool:
    return time.monotonic() - started < _COMPLETION_DEADLINE_SECONDS


def _renew_completion_claim(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    fence_token: str,
) -> tuple[_Renew, float | None]:
    """Renew an owned claim; distinguish confirmed loss from absent or invalid state.

    Read and write errors propagate for bounded retries (RES-21). A malformed
    present claim returns invalid, which callers report as claim_lease_invalid.
    """

    path = _adoption_path(state_dir, job_id)
    with _locked(path.parent / ".lock"):
        current = _read_adoption_strict(path)
        if current.get("applied") is True:
            return _Renew.applied, None
        if "claim" not in current:
            return _Renew.absent, None
        claim = current["claim"]
        if not isinstance(claim, dict) or claim.get("state") != "applying":
            return _Renew.invalid, None
        token = claim.get("fence_token")
        owner = claim.get("owner_pid")
        lease = _stored_lease_seconds(claim)
        if not isinstance(token, str) or not token or type(owner) is not int or owner <= 0 or lease is None:
            return _Renew.invalid, None
        if token != fence_token or owner != os.getpid():
            return _Renew.other_owner, None
        updated = dict(claim)
        updated["monotonic_ns"] = _claim_now_ns()
        _write_adoption_unlocked(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=False,
            claim=updated,
            keep_claim=False,
        )
        return _Renew.renewed, lease


def _heartbeat_completion_claim(
    *,
    state_dir: Path,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    fence_token: str,
    started: float,
    stop: threading.Event,
    completion_deadline: float | None = None,
) -> None:
    """Renew until stopped or the completion deadline, or until the fence is not ours.

    A renew that raises is a suspicion, not proof the fence is lost (RES-21): it is
    retried every ``_RENEW_RETRY_SECONDS`` until ``stop`` or the deadline (RES-13).
    Only an applied record or another owner's token ends fencing early (RES-10).
    Retry warnings are rate limited, while every give-up is logged with an ``event=`` name.
    """

    deadline = completion_deadline if completion_deadline is not None else started + _COMPLETION_DEADLINE_SECONDS
    cadence = _CLAIM_LEASE_SECONDS / 3.0
    retry_count = 0
    while not stop.is_set():
        # Production completion enforces this deadline at mutation boundaries;
        # its per-job lock contains any synchronous call still unwinding after it.
        if time.monotonic() >= deadline:
            _log_renew_gave_up(job_id, "completion_deadline")
            return
        try:
            outcome, lease = _renew_completion_claim(
                state_dir, job_id=job_id, attempt=attempt, pass_id=pass_id, now=now, fence_token=fence_token
            )
            match outcome:
                case _Renew.applied | _Renew.other_owner:
                    _log_renew_gave_up(job_id, "fence_not_owned")
                    return
                case _Renew.invalid:
                    raise RemoteJobReconcileError("claim_lease_invalid")
                case _Renew.absent:
                    raise RemoteJobReconcileError("claim_absent")
                case _Renew.renewed:
                    cadence = lease / 3.0 if lease else cadence
        except Exception as exc:  # noqa: BLE001 — a failed renew must not end fencing; it is retried
            pause = min(_RENEW_RETRY_SECONDS, cadence)
            retry_count += 1
            if retry_count == 1 or retry_count % _RENEW_RETRY_LOG_EVERY == 0:
                _logger.warning(
                    "remote job reconcile claim renew failed event=remote_job_claim_renew_retry "
                    "job_id=%s error_type=%s reason=%s retry_in=%.2f retry=%s",
                    job_id,
                    type(exc).__name__,
                    getattr(exc, "reason", "none"),
                    pause,
                    retry_count,
                )
        else:
            pause = cadence
        if stop.wait(min(pause, max(0.0, deadline - time.monotonic()))):
            return


def _log_renew_gave_up(job_id: str, reason: str) -> None:
    _logger.warning(
        "remote job reconcile claim heartbeat gave up event=remote_job_claim_renew_gave_up job_id=%s reason=%s",
        job_id,
        reason,
    )


def _stop_claim_heartbeat(stop: threading.Event, beater: threading.Thread) -> None:
    stop.set()
    beater.join(timeout=_LOCK_WAIT_SECONDS + 1.0)
    if beater.is_alive():
        _logger.warning("remote job reconcile claim heartbeat still running")


def _finish_applied_claim(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    fence_token: str,
) -> bool:
    """Mark the adoption applied only when ``fence_token`` still owns it."""

    path = _adoption_path(state_dir, job_id)
    with _locked(path.parent / ".lock"):
        current = _read_adoption_strict(path)
        claim = current.get("claim")
        if not _claim_token_matches(claim, fence_token) or not isinstance(claim, dict):
            _logger.warning("remote job reconcile stale fence finalize job_id=%s", job_id)
            return False
        if claim.get("owner_pid") != os.getpid():
            _logger.warning("remote job reconcile stale fence finalize job_id=%s", job_id)
            return False
        _write_adoption_unlocked(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=True,
            keep_claim=False,
        )
        return True


def _release_own_claim(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    fence_token: str,
) -> bool:
    path = _adoption_path(state_dir, job_id)
    with _locked(path.parent / ".lock"):
        current = _read_adoption_strict(path)
        claim = current.get("claim")
        if not _claim_token_matches(claim, fence_token) or not isinstance(claim, dict):
            return False
        if claim.get("owner_pid") != os.getpid():
            return False
        if current.get("applied") is True:
            return False
        _write_adoption_unlocked(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=False,
            keep_claim=False,
        )
        return True


def _write_adoption(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    applied: bool,
    claim: Mapping[str, Any] | None = None,
    keep_claim: bool = True,
) -> None:
    path = _adoption_path(state_dir, job_id)
    with _locked(path.parent / ".lock"):
        _write_adoption_unlocked(
            state_dir,
            job_id=job_id,
            attempt=attempt,
            pass_id=pass_id,
            now=now,
            applied=applied,
            claim=claim,
            keep_claim=keep_claim,
        )


def _write_adoption_unlocked(
    state_dir: Path,
    *,
    job_id: str,
    attempt: int,
    pass_id: str,
    now: datetime,
    applied: bool,
    claim: Mapping[str, Any] | None = None,
    keep_claim: bool = True,
) -> None:
    path = _adoption_path(state_dir, job_id)
    current = _read_json(path) or {}
    adopted_at = current.get("adopted_at")
    if not isinstance(adopted_at, str):
        adopted_at = now.isoformat()
    already = current.get("applied") is True and current.get("applied_attempt") == attempt
    payload: dict[str, Any] = {
        "job_id": job_id,
        "attempt": attempt,
        "pass_id": pass_id,
        "adopted_at": adopted_at,
        "applied": True if applied or already else False,
    }
    if payload["applied"] is True:
        payload["applied_attempt"] = attempt
    elif claim is not None:
        payload["claim"] = dict(claim)
    elif keep_claim and isinstance(current.get("claim"), dict):
        payload["claim"] = current["claim"]
    _write_json(path, payload)


def _known_job_ids(ledger: RemoteJobLedger) -> set[str]:
    if not ledger.ledger_dir.is_dir():
        return set()
    return {path.stem for path in ledger.ledger_dir.glob("j*.json") if JOB_ID_RE.fullmatch(path.stem)}


def _clock_now(clock: Callable[[], datetime] | None) -> datetime:
    current = clock() if clock is not None else datetime.now(timezone.utc)
    if not isinstance(current, datetime):
        raise RemoteJobReconcileError("invalid_clock")
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        _logger.warning("remote job reconcile unreadable timestamp error_type=%s", type(exc).__name__)
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _warn_purge_failure(directory: Path, error_type: str) -> str:
    _logger.warning(
        "remote job quarantine purge failed job_id=%s error_type=%s",
        directory.name,
        error_type,
    )
    return error_type


def _unlink_quarantine(directory: Path) -> str | None:
    """Remove one quarantine directory. Return the exception class if it remains."""

    allowed = {"orphan.json", "artifact.bin"}
    try:
        entries = list(directory.iterdir())
    except OSError as exc:
        return _warn_purge_failure(directory, type(exc).__name__)
    if any(entry.name not in allowed for entry in entries):
        return _warn_purge_failure(directory, "OSError")
    try:
        for entry in entries:
            try:
                entry.unlink()
            except FileNotFoundError:
                continue
        directory.rmdir()
    except OSError as exc:
        if not directory.exists():
            return None
        return _warn_purge_failure(directory, type(exc).__name__)
    if directory.exists():
        return _warn_purge_failure(directory, "OSError")
    return None


def _thread_lock_for(path: Path) -> threading.Lock:
    key = os.fspath(path)
    with _thread_locks_guard:
        lock = _thread_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _thread_locks[key] = lock
        return lock


@contextmanager
def _locked(path: Path, *, timeout_seconds: float | None = None) -> Iterator[None]:
    """Bound the lock wait so a stuck holder cannot stall startup (RES-02).

    ``flock`` excludes other processes. A thread lock excludes other threads in
    this process, because ``flock`` is per-process and would not.
    """

    thread_lock = _thread_lock_for(path)
    wait = _LOCK_WAIT_SECONDS if timeout_seconds is None else min(_LOCK_WAIT_SECONDS, max(0.0, timeout_seconds))
    deadline = time.monotonic() + wait
    if not thread_lock.acquire(timeout=wait):
        raise RemoteJobReconcileError("reconcile_lock_timeout")
    descriptor = -1
    locked = False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise
                if time.monotonic() >= deadline:
                    raise RemoteJobReconcileError("reconcile_lock_timeout")
                time.sleep(_LOCK_POLL_SECONDS)
            else:
                locked = True
                break
        yield
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        if descriptor >= 0:
            os.close(descriptor)
        thread_lock.release()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _logger.warning(
            "remote job reconcile unreadable json path=%s error_type=%s",
            path,
            type(exc).__name__,
        )
        return None
    return payload if isinstance(payload, dict) else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    _write_bytes(path, body)


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short atomic file write")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temp_name, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
