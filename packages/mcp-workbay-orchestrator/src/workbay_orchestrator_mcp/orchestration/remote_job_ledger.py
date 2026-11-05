"""Crash-safe ledger and wake queue for remote jobs.

The ledger is a collection of atomic JSON records under ``.task-state``. Wake
delivery is at-least-once: handlers must be idempotent, and the byte cursor is
persisted only after a handler returns successfully. Consumed wake bytes are
compacted past a size threshold so a pending scan reads only the cursor suffix.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import math
import os
import tempfile
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable, Iterator

from .lane_lifecycle_contracts import JOB_ID_RE, ContractError, RemoteJobSpec, WakeEvent

_MAX_HANDLER_ATTEMPTS = 5
COMPLETION_DEFERRAL_MAX_AGE_SECONDS = 6 * 60 * 60
_LOCK_RETRY_INTERVAL_SECONDS = 0.01
_WAKE_LOCK_TIMEOUT_SECONDS = 5.0
_INVALID_WAKE_SAMPLE_LIMIT = 8
_INVALID_WAKE_SAMPLE_BYTES = 256
# Consumed prefixes are rewritten out only above this size (RES-07).
WAKE_COMPACT_THRESHOLD_BYTES = 64 * 1024
_UNDECODABLE_WAKE_ERRORS = (ContractError, TypeError, ValueError, RecursionError, UnicodeDecodeError)


def _decode_wake_line(line: bytes) -> WakeEvent | None:
    """Return one wake, or None when the complete line is undecodable.

    ContractError, TypeError, ValueError, RecursionError, and UnicodeDecodeError
    are invalid lines. Callers count them; they do not escape the scan (REF-26, RES-13).
    """
    try:
        event = WakeEvent.from_json(line.decode("utf-8"))
    except _UNDECODABLE_WAKE_ERRORS:
        return None
    if not isinstance(event, WakeEvent):
        return None
    return event


class RemoteJobLedgerError(Exception):
    """Typed refusal for invalid ledger state or lifecycle transitions."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class LedgerLockTimeout(RemoteJobLedgerError):
    """Typed refusal raised when a bounded ledger lock wait expires."""

    def __init__(self) -> None:
        super().__init__("ledger_lock_timeout")


class RemoteCompletionDeferred(Exception):
    """Completion remains retryable without charging a wake handler failure."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class PendingWakeIds:
    """Job ids queued past a valid wake cursor.

    ``invalid_lines`` counts complete lines the WakeEvent decoder refused.
    An empty ``job_ids`` with ``invalid_lines == 0`` is a real empty pending
    set. It is not an invalid cursor (OBS-08).
    """

    job_ids: frozenset[str]
    invalid_lines: int
    offset: int
    size: int


@dataclass(frozen=True, slots=True)
class InvalidWakeCursor:
    """Typed refusal: the stored cursor is past EOF or not on a line boundary."""

    reason: str
    offset: int
    size: int


class RemoteJobLedger:
    """Durable remote-job records and a write-before-advance wake queue.

    ``state_dir`` is the ledger root, usually ``.task-state/remote-jobs``.
    Each job update uses a same-directory temporary file, fsync, atomic rename,
    and parent-directory fsync. Wake appends are serialized by ``flock``. Typed
    completion deferrals are also stored durably by job so they do not hold up
    the shared wake cursor.
    """

    def __init__(self, state_dir: str | os.PathLike[str]) -> None:
        self.state_dir = Path(state_dir)
        self.ledger_dir = self.state_dir / "ledger"
        self.wake_path = self.state_dir / "wake.jsonl"
        self.torn_path = self.state_dir / "wake.torn"
        self.cursor_path = self.state_dir / "wake.cursor"
        self.deferrals_path = self.state_dir / "wake.deferrals.json"
        self.deferrals_lock_path = self.state_dir / "wake.deferrals.lock"
        self.compact_marker_path = self.state_dir / "wake.compact"
        self._intent_lock_deadline: ContextVar[float | None] = ContextVar(
            f"remote_job_ledger_intent_lock_deadline_{id(self)}", default=None
        )

    @contextmanager
    def intent_lock_deadline(self, deadline: float) -> Iterator[None]:
        """Set the monotonic lock deadline for ledger writes in this context."""
        token = self._intent_lock_deadline.set(deadline)
        try:
            yield
        finally:
            self._intent_lock_deadline.reset(token)

    def record_intent(self, spec: RemoteJobSpec) -> dict:
        """Durably record submit intent before a remote submit is attempted."""
        if not isinstance(spec, RemoteJobSpec):
            raise RemoteJobLedgerError("invalid_spec")
        self._validate_job_id(spec.job_id)
        self._ensure_layout()
        path = self._job_path(spec.job_id)
        with self._locked(self.ledger_dir / ".ledger.lock", deadline=self._intent_lock_deadline.get()):
            if path.exists():
                existing = self._read_json(path)
                if existing.get("spec") != json.loads(spec.to_json()):
                    raise RemoteJobLedgerError("job_id_conflict")
                return self._copy_record(existing)
            now = _utc_now()
            record = {
                "spec": json.loads(spec.to_json()),
                "stage": "intent",
                "created_at": spec.created_at,
                "intent_at": now,
                "updated_at": now,
                "attempts": 0,
                "unit": None,
                "rc": None,
            }
            self._write_json_atomic(path, record)
            return self._copy_record(record)

    def mark_submit_ambiguous(self, job_id: str) -> dict:
        """Keep an uncertain submit open for reconciliation, never mark it done."""

        def update(record: dict) -> None:
            record["submit_state"] = "ambiguous"

        return self._update_job(job_id, update)

    def mark_submitted(self, job_id: str, unit: str) -> dict:
        """Record the remote unit once submit has returned successfully."""
        if not isinstance(unit, str) or not unit:
            raise RemoteJobLedgerError("invalid_unit")

        def update(record: dict) -> None:
            stage = record["stage"]
            if stage == "intent":
                record["stage"] = "submitted"
                record["unit"] = unit
                record["submitted_at"] = _utc_now()
            elif stage in {"submitted", "done", "completed"}:
                existing_unit = record.get("unit")
                if existing_unit is None:
                    record["unit"] = unit
                elif existing_unit != unit:
                    raise RemoteJobLedgerError("unit_conflict")
            else:
                raise RemoteJobLedgerError("invalid_transition")

        return self._update_job(job_id, update)

    def mark_done(self, job_id: str, rc: int, *, lost: bool = False) -> dict:
        """Record the remote process exit code and any durable lost classification."""
        if isinstance(rc, bool) or not isinstance(rc, int):
            raise RemoteJobLedgerError("invalid_return_code")
        if not isinstance(lost, bool):
            raise RemoteJobLedgerError("invalid_remote_state")

        def update(record: dict) -> None:
            stage = record["stage"]
            if stage in {"intent", "submitted"}:
                record["stage"] = "done"
                record["rc"] = rc
                record["done_at"] = _utc_now()
                if lost:
                    record["remote_state"] = "lost"
            elif stage in {"done", "completed"}:
                if record.get("rc") != rc:
                    raise RemoteJobLedgerError("return_code_conflict")
            else:
                raise RemoteJobLedgerError("invalid_transition")

        return self._update_job(job_id, update)

    def mark_completed(self, job_id: str) -> dict:
        """Mark a done job as fully collected and handled."""

        def update(record: dict) -> None:
            if record["stage"] == "done":
                record["stage"] = "completed"
                record["completed_at"] = _utc_now()
                record.pop("completion_deferred_at", None)
                record.pop("completion_deferred_reason", None)
            elif record["stage"] != "completed":
                raise RemoteJobLedgerError("invalid_transition")

        record = self._update_job(job_id, update)
        self._remove_completion_deferral(job_id)
        return record

    def read_job(self, job_id: str) -> dict:
        """Return a detached copy of one durable record."""
        self._validate_job_id(job_id)
        try:
            return self._copy_record(self._read_json(self._job_path(job_id)))
        except FileNotFoundError as exc:
            raise RemoteJobLedgerError("job_not_found") from exc

    def open_jobs(self) -> list[dict]:
        """Return job records that have not completed or entered dead letter."""
        self._ensure_layout()
        records = []
        for path in sorted(self.ledger_dir.glob("j*.json")):
            record = self._read_json(path)
            if record.get("stage") not in {"completed", "dead_letter"}:
                records.append(self._copy_record(record))
        return records

    def deferred_jobs(self) -> dict[str, dict]:
        """Return a detached snapshot of jobs awaiting completion retry."""
        self._ensure_layout()
        with self._locked(self.deferrals_lock_path):
            return self._copy_record(self._read_deferrals())

    def has_deferred_jobs(self) -> bool:
        """Return whether any completion deferrals need a retry."""
        return bool(self.deferred_jobs())

    def enqueue_wake(self, event: WakeEvent) -> None:
        """Append one wake event, repairing an interrupted prior append first."""
        if not isinstance(event, WakeEvent):
            raise RemoteJobLedgerError("invalid_wake_event")
        try:
            event = WakeEvent.from_json(event.to_json())
        except (ContractError, TypeError, ValueError, RecursionError) as exc:
            raise RemoteJobLedgerError("invalid_wake_event") from exc
        self._validate_job_id(event.job_id)
        self._ensure_layout()
        line = (event.to_json() + "\n").encode("utf-8")
        lock_path = self.state_dir / "wake.lock"
        with self._locked(lock_path, deadline=self._wake_lock_deadline()):
            self._repair_torn_tail_locked()
            existed = self.wake_path.exists()
            fd = os.open(self.wake_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                written = os.write(fd, line)
                if written != len(line):
                    raise OSError("short wake event append")
                os.fsync(fd)
            finally:
                os.close(fd)
            if not existed:
                _fsync_directory(self.state_dir)

    def drain_wake(
        self,
        handler: Callable[[WakeEvent], object],
        *,
        max_events: int | None = None,
        should_stop: Callable[[], bool] | None = None,
        deferral_max_age_seconds: float = COMPLETION_DEFERRAL_MAX_AGE_SECONDS,
    ) -> dict:
        """Retry durable deferrals, then deliver queued wakes.

        The cursor makes delivery at-least-once, so ``handler`` must be
        idempotent. Failed wake handlers increment their job's attempt count;
        the fifth failure durably dead-letters the job before advancing the
        cursor. A typed deferral is atomically written to the per-job deferral
        set before advancing the cursor, so it cannot block later wake events.
        Saved deferrals are retried in due-time order before new wake entries.
        Deferrals at least ``deferral_max_age_seconds`` old surface as ``completion_stalled``
        entries without dead-lettering. A final unterminated line is ignored
        and returned as ``torn_tail``.

        ``max_events`` is a nonnegative exact int or None (unlimited), and caps
        deferred handler attempts and wake records, including already-dead-lettered
        skips, using one shared budget. Pending retries rotate by their last attempt
        time (or first deferral time for older records). ``should_stop`` is checked
        before each handler call. This supplies the daemon's bounded wake drain
        contract (GRPH-32), with
        cooperative deadline/cancellation checks, not a timeout for a running
        handler or blocking storage operation. Retry limits (RES-06) and
        durable-before-cursor ordering (DATA-14) hold for bounded drains as
        well. Stop callback exceptions propagate without charging a handler
        attempt.
        """
        if not callable(handler):
            raise RemoteJobLedgerError("invalid_handler")
        if max_events is not None and (type(max_events) is not int or max_events < 0):
            raise RemoteJobLedgerError("invalid_max_events")
        if should_stop is not None and not callable(should_stop):
            raise RemoteJobLedgerError("invalid_should_stop")
        if (
            type(deferral_max_age_seconds) not in {int, float}
            or not math.isfinite(deferral_max_age_seconds)
            or deferral_max_age_seconds < 0
        ):
            raise RemoteJobLedgerError("invalid_deferral_max_age_seconds")
        self._ensure_layout()
        result = {
            "processed": 0,
            "dead_lettered": 0,
            "torn_tail": None,
            "failure": None,
        }
        budget_used = 0
        with self._locked(self.state_dir / "wake.drain.lock", deadline=self._wake_lock_deadline()):
            try:
                if self._maintenance_due():
                    self._maintain_wake_journal()
                if not self.cursor_path.exists():
                    self._write_cursor(0)
                offset = self._read_cursor()
                try:
                    data = self.wake_path.read_bytes()
                except FileNotFoundError:
                    data = b""
                if offset > len(data) or (offset and data[offset - 1 : offset] != b"\n"):
                    raise RemoteJobLedgerError("invalid_cursor")

                deferred_at_start = self.deferred_jobs()
                reexamined_ids = set(deferred_at_start)
                due_deferrals = sorted(
                    deferred_at_start.items(),
                    key=lambda item: (
                        datetime.fromisoformat(
                            item[1].get("last_attempt_at", item[1]["first_deferred_at"])
                        ).timestamp(),
                        item[1]["attempts"],
                        item[0],
                    ),
                )
                for job_id, deferred in due_deferrals:
                    if max_events is not None and budget_used >= max_events:
                        break
                    if should_stop is not None and should_stop():
                        break
                    try:
                        saved = self.read_job(job_id)
                    except RemoteJobLedgerError as exc:
                        if exc.reason != "job_not_found":
                            raise
                        continue
                    if saved.get("stage") in {"completed", "dead_letter"}:
                        self._remove_completion_deferral(job_id)
                        continue
                    event = self._wake_event_from_deferral(deferred)
                    budget_used += 1
                    try:
                        handler(event)
                    except RemoteCompletionDeferred as exc:
                        self._record_completion_deferral(event, exc.reason)
                    except Exception as exc:
                        self._increment_deferred_attempts(job_id)
                        attempts = self._record_handler_failure(job_id, exc)
                        if attempts >= _MAX_HANDLER_ATTEMPTS:
                            result["dead_lettered"] += 1
                        else:
                            result["failure"] = {
                                "job_id": job_id,
                                "attempts": attempts,
                                "reason": _exception_reason(exc),
                            }
                    else:
                        self._remove_completion_deferral(job_id)
                        result["processed"] += 1

                # A crash can leave a durable deferral and its original wake at
                # the cursor. Re-read after retries so that event is consumed
                # without invoking the handler twice in one drain.
                offset = self._read_cursor()
                if offset > len(data) or (offset and data[offset - 1 : offset] != b"\n"):
                    raise RemoteJobLedgerError("invalid_cursor")

                tail_start = data.rfind(b"\n") + 1
                if tail_start < len(data):
                    result["torn_tail"] = data[tail_start:].decode("utf-8", errors="replace")

                position = offset
                while position < len(data):
                    newline = data.find(b"\n", position)
                    if newline < 0:
                        break
                    if max_events is not None and budget_used >= max_events:
                        break
                    if should_stop is not None and should_stop():
                        break
                    next_offset = newline + 1
                    event = _decode_wake_line(data[position:newline])
                    if event is None:
                        result.setdefault("invalid_lines", 0)
                        result["invalid_lines"] += 1
                        samples = result.setdefault("invalid_samples", [])
                        if len(samples) < _INVALID_WAKE_SAMPLE_LIMIT:
                            sample = data[position:newline][:_INVALID_WAKE_SAMPLE_BYTES].decode(
                                "utf-8", errors="replace"
                            )
                            samples.append(sample)
                        self._write_cursor(next_offset)
                        result["processed"] += 1
                        budget_used += 1
                        position = next_offset
                        continue
                    try:
                        saved = self.read_job(event.job_id)
                    except RemoteJobLedgerError as exc:
                        if exc.reason != "job_not_found":
                            raise
                        saved = None
                    if saved is not None and saved["stage"] == "dead_letter":
                        self._write_cursor(next_offset)
                        result["processed"] += 1
                        budget_used += 1
                        position = next_offset
                        continue
                    if event.job_id in reexamined_ids:
                        self._write_cursor(next_offset)
                        budget_used += 1
                        position = next_offset
                        continue
                    try:
                        handler(event)
                    except RemoteCompletionDeferred as exc:
                        self._record_completion_deferral(event, exc.reason)
                        reexamined_ids.add(event.job_id)
                        # Persisting the deferral before this cursor move is the
                        # crash boundary: either the wake remains queued, or its
                        # complete retry data is durable in the deferral set.
                        self._write_cursor(next_offset)
                        budget_used += 1
                        position = next_offset
                        continue
                    except Exception as exc:
                        attempts = self._record_handler_failure(event.job_id, exc)
                        if attempts >= _MAX_HANDLER_ATTEMPTS:
                            self._write_cursor(next_offset)
                            result["processed"] += 1
                            budget_used += 1
                            result["dead_lettered"] += 1
                            position = next_offset
                            continue
                        result["failure"] = {
                            "job_id": event.job_id,
                            "attempts": attempts,
                            "reason": _exception_reason(exc),
                        }
                        return result
                    self._write_cursor(next_offset)
                    result["processed"] += 1
                    budget_used += 1
                    position = next_offset
            finally:
                self._report_completion_deferrals(result, deferral_max_age_seconds)
                if self._maintenance_due():
                    self._maintain_wake_journal()
        return result

    def read_pending_wakes(self) -> PendingWakeIds | InvalidWakeCursor:
        """Return pending wake job ids, or a typed ``invalid_cursor`` error.

        Validity matches ``drain_wake``: the offset is inside the file and on
        a line boundary. An invalid cursor is never rewritten. The scan seeks
        to the cursor and reads the suffix, including the single preceding
        boundary byte, without loading the consumed prefix. Above
        ``WAKE_COMPACT_THRESHOLD_BYTES``, consumed bytes are compacted under
        the append and drain locks before the scan (RES-07).
        """
        with self._locked(self.state_dir / "wake.drain.lock", deadline=self._wake_lock_deadline()):
            if self._maintenance_due():
                self._maintain_wake_journal()
            # Maintenance may replace the inode and reset the cursor. The scan
            # validates the cursor against the post-maintenance file while the
            # same drain lock still excludes compaction/recovery peers.
            return self._scan_pending_suffix()

    def _scan_pending_suffix(self) -> PendingWakeIds | InvalidWakeCursor:
        try:
            with self.wake_path.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                located = self._validated_suffix(stream, size)
        except FileNotFoundError:
            located = self._validated_suffix(None, 0)
            size = 0
        if isinstance(located, InvalidWakeCursor):
            return located
        offset, suffix = located
        return self._pending_ids_from(suffix, offset, size)

    def _validated_suffix(self, stream: BinaryIO | None, size: int) -> tuple[int, bytes] | InvalidWakeCursor:
        """Seek to a validated cursor and return ``(offset, suffix)``."""
        try:
            offset = 0 if not self.cursor_path.exists() else self._read_cursor()
        except RemoteJobLedgerError as exc:
            if exc.reason != "invalid_cursor":
                raise
            return InvalidWakeCursor("invalid_cursor", _coerce_cursor_offset(self.cursor_path), size)
        if offset > size or offset < 0:
            return InvalidWakeCursor("invalid_cursor", offset, size)
        if offset:
            if stream is None:
                return InvalidWakeCursor("invalid_cursor", offset, size)
            stream.seek(offset - 1)
            if stream.read(1) != b"\n":
                return InvalidWakeCursor("invalid_cursor", offset, size)
        if stream is None:
            return offset, b""
        stream.seek(offset)
        return offset, stream.read()

    def _pending_ids_from(self, suffix: bytes, offset: int, size: int) -> PendingWakeIds:
        pending: set[str] = set()
        invalid_lines = 0
        position = 0
        while position < len(suffix):
            newline = suffix.find(b"\n", position)
            if newline < 0:
                break
            event = _decode_wake_line(suffix[position:newline])
            if event is None:
                invalid_lines += 1
            else:
                pending.add(event.job_id)
            position = newline + 1
        return PendingWakeIds(frozenset(pending), invalid_lines, offset, size)

    def _maintenance_due(self) -> bool:
        if self.compact_marker_path.exists():
            return True
        try:
            size = self.wake_path.stat().st_size
        except OSError:
            return False
        return size > WAKE_COMPACT_THRESHOLD_BYTES

    def _maintain_wake_journal(self) -> None:
        """Recover or compact. Caller holds ``wake.drain.lock``; this takes ``wake.lock``.

        ``enqueue_wake`` takes only the append lock, so drain-then-append cannot deadlock.
        """
        with self._locked(self.state_dir / "wake.lock", deadline=self._wake_lock_deadline()):
            self._recover_interrupted_compact()
            self._compact_consumed_prefix()

    def _read_compact_marker(self) -> tuple[str, int, int] | None:
        try:
            text = self.compact_marker_path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError):
            self._discard_compact_marker()
            return None
        parts = text.split(" ")
        if len(parts) != 3 or len(parts[0]) != 64 or not parts[1].isdecimal() or not parts[2].isdecimal():
            self._discard_compact_marker()
            return None
        return parts[0], int(parts[1]), int(parts[2])

    def _hash_wake_file(self) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        try:
            with self.wake_path.open("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                    size += len(chunk)
        except FileNotFoundError:
            return hashlib.sha256(b"").hexdigest(), 0
        return digest.hexdigest(), size

    def _recover_interrupted_compact(self) -> None:
        """Finish a crash between wake rename and cursor rewrite, or drop a stale marker.

        The marker records the pre-compact cursor and the hash of the suffix that
        the rename installs. A crash before the rename leaves the original file,
        so the hash does not match and the cursor still names unconsumed bytes.
        A crash after the rename matches the hash while the cursor is still the
        old offset: rewriting it to 0 delivers that suffix once and does not
        replay the dropped prefix.
        """
        marker = self._read_compact_marker()
        if marker is None:
            return
        digest, length, old_cursor = marker
        try:
            current = self._read_cursor()
        except RemoteJobLedgerError:
            try:
                self._record_compact_recovery_failure(marker, reason="invalid_cursor")
            finally:
                self._discard_compact_marker()
            return
        if current != old_cursor:
            self._discard_compact_marker()
            return
        file_digest, file_size = self._hash_wake_file()
        if file_digest == digest and file_size == length and old_cursor > 0:
            # _write_cursor atomically fsyncs the cursor and state directory.
            # Only then is it safe to remove the recovery marker.
            self._write_cursor(0)
            self._discard_compact_marker()
            self._clear_compact_recovery_failure()
            return
        try:
            self._record_compact_recovery_failure(
                marker,
                reason="compact_hash_mismatch",
                actual_hash=file_digest,
                actual_size=file_size,
            )
        finally:
            self._discard_compact_marker()

    def _compact_consumed_prefix(self) -> None:
        """Rewrite the journal to its unconsumed suffix, then move the cursor to 0.

        Order is marker, temp file, fsync, rename, cursor rewrite. A crash before
        the rename keeps the original bytes. A crash after the rename and before
        the cursor rewrite is recovered from the marker.
        """
        try:
            with self.wake_path.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                if size <= WAKE_COMPACT_THRESHOLD_BYTES:
                    return
                try:
                    offset = self._read_cursor()
                except RemoteJobLedgerError:
                    return
                if offset <= 0 or offset > size:
                    return
                stream.seek(offset - 1)
                if stream.read(1) != b"\n":
                    return
                stream.seek(offset)
                suffix = stream.read()
        except FileNotFoundError:
            return
        digest = hashlib.sha256(suffix).hexdigest()
        marker = f"{digest} {len(suffix)} {offset}\n".encode("ascii")
        self._write_bytes_atomic(self.compact_marker_path, marker)
        self._write_bytes_atomic(self.wake_path, suffix)
        # _write_cursor fsyncs both the cursor file and its containing directory
        # before the marker is removed, preserving the crash-recovery boundary.
        self._write_cursor(0)
        self._discard_compact_marker()
        self._clear_compact_recovery_failure()

    def _record_compact_recovery_failure(
        self,
        marker: tuple[str, int, int],
        *,
        reason: str,
        actual_hash: str | None = None,
        actual_size: int | None = None,
    ) -> None:
        """Persist one bounded latest failure record, replacing earlier detail."""
        expected_hash, expected_size, cursor = marker
        record = {
            "reason": reason,
            "expected_hash": expected_hash,
            "expected_size": expected_size,
            "cursor": cursor,
            "actual_hash": actual_hash,
            "actual_size": actual_size,
        }
        self._write_bytes_atomic(
            self.state_dir / "wake.compact.failure",
            (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii"),
        )

    def _clear_compact_recovery_failure(self) -> None:
        path = self.state_dir / "wake.compact.failure"
        try:
            os.unlink(path)
        except FileNotFoundError:
            return
        _fsync_directory(self.state_dir)

    def _discard_compact_marker(self) -> None:
        try:
            os.unlink(self.compact_marker_path)
        except FileNotFoundError:
            return
        _fsync_directory(self.state_dir)

    @staticmethod
    def _wake_lock_deadline() -> float:
        """Bound waits for wake append/drain locks independently of ledger writes."""
        return time.monotonic() + _WAKE_LOCK_TIMEOUT_SECONDS

    def _record_handler_failure(self, job_id: str, exc: Exception) -> int:
        reason = _exception_reason(exc)

        def update(record: dict) -> None:
            record["attempts"] = int(record.get("attempts", 0)) + 1
            record["last_error"] = reason
            if record["attempts"] >= _MAX_HANDLER_ATTEMPTS:
                record["stage"] = "dead_letter"
                record["dead_letter_at"] = _utc_now()

        record = self._update_job(job_id, update)
        if record["attempts"] >= _MAX_HANDLER_ATTEMPTS:
            self._remove_completion_deferral(job_id)
        return int(record["attempts"])

    def _record_completion_deferral(self, event: WakeEvent, reason: str) -> dict:
        """Atomically retain a retryable wake before its cursor is advanced."""

        job_id = event.job_id

        def update(record: dict) -> None:
            record.setdefault("completion_deferred_at", _utc_now())
            record["completion_deferred_reason"] = reason

        job_record = self._update_job(job_id, update)
        with self._locked(self.deferrals_lock_path):
            records = self._read_deferrals()
            previous = records.get(job_id, {})
            deferred = {
                "job_id": job_id,
                "reason": reason,
                "first_deferred_at": previous.get("first_deferred_at", job_record["completion_deferred_at"]),
                "last_attempt_at": _utc_now(),
                "attempts": int(previous.get("attempts", 0)) + 1,
                # Keep the original signal so a deferred job remains retryable
                # after its wake bytes are compacted from the shared journal.
                "event_json": event.to_json(),
            }
            records[job_id] = deferred
            self._write_deferrals(records)
            return self._copy_record(deferred)

    def _increment_deferred_attempts(self, job_id: str) -> None:
        with self._locked(self.deferrals_lock_path):
            records = self._read_deferrals()
            deferred = records.get(job_id)
            if deferred is None:
                return
            deferred["attempts"] += 1
            deferred["last_attempt_at"] = _utc_now()
            self._write_deferrals(records)

    def _remove_completion_deferral(self, job_id: str) -> None:
        self._validate_job_id(job_id)
        self._ensure_layout()
        with self._locked(self.deferrals_lock_path):
            records = self._read_deferrals()
            if job_id in records:
                del records[job_id]
                self._write_deferrals(records)

    def _read_deferrals(self) -> dict[str, dict]:
        try:
            payload = json.loads(self.deferrals_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise RemoteJobLedgerError("invalid_deferred_state") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise RemoteJobLedgerError("invalid_deferred_state")
        records = payload.get("jobs")
        if not isinstance(records, dict):
            raise RemoteJobLedgerError("invalid_deferred_state")
        checked: dict[str, dict] = {}
        for job_id, deferred in records.items():
            try:
                self._validate_job_id(job_id)
                if not isinstance(deferred, dict):
                    raise ValueError("invalid deferred record")
                if deferred.get("job_id") != job_id:
                    raise ValueError("deferred job id mismatch")
                if not isinstance(deferred.get("reason"), str) or not deferred["reason"]:
                    raise ValueError("invalid deferred reason")
                if not isinstance(deferred.get("first_deferred_at"), str):
                    raise ValueError("invalid deferred timestamp")
                datetime.fromisoformat(deferred["first_deferred_at"])
                if "last_attempt_at" in deferred:
                    if not isinstance(deferred["last_attempt_at"], str):
                        raise ValueError("invalid retry timestamp")
                    datetime.fromisoformat(deferred["last_attempt_at"])
                attempts = deferred.get("attempts")
                if type(attempts) is not int or attempts < 1:
                    raise ValueError("invalid deferred attempts")
                event_json = deferred.get("event_json")
                if not isinstance(event_json, str):
                    raise ValueError("invalid deferred event")
                event = WakeEvent.from_json(event_json)
                if event.job_id != job_id:
                    raise ValueError("deferred event job id mismatch")
            except (ContractError, TypeError, ValueError, RecursionError, RemoteJobLedgerError) as exc:
                raise RemoteJobLedgerError("invalid_deferred_state") from exc
            checked[job_id] = self._copy_record(deferred)
        return checked

    def _write_deferrals(self, records: dict[str, dict]) -> None:
        if records:
            self._write_json_atomic(self.deferrals_path, {"schema_version": 1, "jobs": records})
            return
        try:
            os.unlink(self.deferrals_path)
        except FileNotFoundError:
            return
        _fsync_directory(self.state_dir)

    @staticmethod
    def _wake_event_from_deferral(deferred: dict) -> WakeEvent:
        try:
            return WakeEvent.from_json(deferred["event_json"])
        except (ContractError, KeyError, TypeError, ValueError, RecursionError) as exc:
            raise RemoteJobLedgerError("invalid_deferred_state") from exc

    def _report_completion_deferrals(self, result: dict, max_age_seconds: float) -> None:
        deferred_jobs = self.deferred_jobs()
        if not deferred_jobs:
            return
        result["deferred"] = {job_id: item["reason"] for job_id, item in deferred_jobs.items()}
        now = datetime.fromisoformat(_utc_now())
        stalled = {}
        for job_id, item in deferred_jobs.items():
            age = max(0.0, (now - datetime.fromisoformat(item["first_deferred_at"])).total_seconds())
            if age >= max_age_seconds:
                stalled[job_id] = {
                    "type": "completion_stalled",
                    "reason": item["reason"],
                    "age_seconds": age,
                    "deferred_at": item["first_deferred_at"],
                }
        if stalled:
            result["completion_stalled"] = stalled

    def _update_job(self, job_id: str, update: Callable[[dict], None]) -> dict:
        self._validate_job_id(job_id)
        self._ensure_layout()
        path = self._job_path(job_id)
        with self._locked(self.ledger_dir / ".ledger.lock", deadline=self._intent_lock_deadline.get()):
            try:
                record = self._read_json(path)
            except FileNotFoundError as exc:
                raise RemoteJobLedgerError("job_not_found") from exc
            before = self._copy_record(record)
            update(record)
            if record != before:
                record["updated_at"] = _utc_now()
                self._write_json_atomic(path, record)
            return self._copy_record(record)

    def _job_path(self, job_id: str) -> Path:
        return self.ledger_dir / f"{job_id}.json"

    def _validate_job_id(self, job_id: str) -> None:
        if not isinstance(job_id, str) or JOB_ID_RE.fullmatch(job_id) is None:
            raise RemoteJobLedgerError("invalid_job_id")

    def _ensure_layout(self) -> None:
        _mkdir_durable(self.state_dir)
        _mkdir_durable(self.ledger_dir)

    def _read_cursor(self) -> int:
        try:
            text = self.cursor_path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return 0
        if not text.isdecimal():
            raise RemoteJobLedgerError("invalid_cursor")
        return int(text)

    def _write_cursor(self, offset: int) -> None:
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise RemoteJobLedgerError("invalid_cursor")
        self._write_bytes_atomic(self.cursor_path, f"{offset}\n".encode("ascii"))

    def _repair_torn_tail_locked(self) -> bytes | None:
        try:
            with self.wake_path.open("rb") as stream:
                stream.seek(0, os.SEEK_END)
                size = stream.tell()
                if size == 0:
                    return None
                stream.seek(-1, os.SEEK_END)
                if stream.read(1) == b"\n":
                    return None
                stream.seek(0)
                data = stream.read()
        except FileNotFoundError:
            return None

        tail_start = data.rfind(b"\n") + 1
        tail = data[tail_start:]
        stamp = f"--- {_utc_now()} ---\n".encode("ascii")
        torn_existed = self.torn_path.exists()
        separator = b""
        if torn_existed:
            with self.torn_path.open("rb") as torn_stream:
                torn_stream.seek(0, os.SEEK_END)
                if torn_stream.tell():
                    torn_stream.seek(-1, os.SEEK_END)
                    if torn_stream.read(1) != b"\n":
                        separator = b"\n"
        torn_fd = os.open(self.torn_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            archive = separator + stamp + tail + (b"" if tail.endswith(b"\n") else b"\n")
            view = memoryview(archive)
            while view:
                written = os.write(torn_fd, view)
                if written <= 0:
                    raise OSError("short torn-tail archive write")
                view = view[written:]
            os.fsync(torn_fd)
        finally:
            os.close(torn_fd)
        if not torn_existed:
            _fsync_directory(self.state_dir)

        wake_fd = os.open(self.wake_path, os.O_WRONLY)
        try:
            os.ftruncate(wake_fd, tail_start)
            os.fsync(wake_fd)
        finally:
            os.close(wake_fd)
        return tail

    @contextmanager
    def _locked(self, path: Path, *, deadline: float | None = None) -> Iterator[None]:
        """Acquire a ledger lock, polling non-blockingly when a deadline is supplied."""
        self._ensure_layout()
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        locked = False
        try:
            if deadline is None:
                fcntl.flock(fd, fcntl.LOCK_EX)
                locked = True
            else:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise LedgerLockTimeout()
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError as exc:
                        if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                            raise
                        time.sleep(min(_LOCK_RETRY_INTERVAL_SECONDS, remaining))
                    else:
                        locked = True
                        if time.monotonic() >= deadline:
                            raise LedgerLockTimeout()
                        break
            yield
        finally:
            try:
                if locked:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _read_json(self, path: Path) -> dict:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RemoteJobLedgerError("invalid_ledger_json") from exc
        if not isinstance(value, dict):
            raise RemoteJobLedgerError("invalid_ledger_record")
        return value

    def _write_json_atomic(self, path: Path, value: dict) -> None:
        payload = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        self._write_bytes_atomic(path, payload)

    def _write_bytes_atomic(self, path: Path, payload: bytes) -> None:
        self._ensure_layout()
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short atomic file write")
                view = view[written:]
            os.fsync(fd)
            os.close(fd)
            fd = -1
            os.replace(temp_name, path)
            _fsync_directory(path.parent)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass

    @staticmethod
    def _copy_record(record: dict) -> dict:
        return json.loads(json.dumps(record))


def _coerce_cursor_offset(path: Path) -> int:
    """Best-effort numeric offset for an already-invalid cursor file."""
    try:
        text = path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return -1
    if text.isdecimal() or (text.startswith("-") and text[1:].isdecimal()):
        return int(text)
    return -1


def _mkdir_durable(path: Path) -> None:
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            break
        current = parent
    for directory in reversed(missing):
        try:
            directory.mkdir()
        except FileExistsError:
            if not directory.is_dir():
                raise
        _fsync_directory(directory.parent)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _exception_reason(exc: Exception) -> str:
    detail = str(exc).strip().replace("\n", " ")[:500]
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
