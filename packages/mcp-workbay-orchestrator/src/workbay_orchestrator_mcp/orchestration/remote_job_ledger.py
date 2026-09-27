"""Crash-safe ledger and wake queue for remote jobs.

The ledger is a collection of atomic JSON records under ``.task-state``. Wake
delivery is at-least-once: handlers must be idempotent, and the byte cursor is
persisted only after a handler returns successfully.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from .lane_lifecycle_contracts import JOB_ID_RE, ContractError, RemoteJobSpec, WakeEvent

_MAX_HANDLER_ATTEMPTS = 5


class RemoteJobLedgerError(Exception):
    """Typed refusal for invalid ledger state or lifecycle transitions."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class RemoteJobLedger:
    """Durable remote-job records and a write-before-advance wake queue.

    ``state_dir`` is the ledger root, usually ``.task-state/remote-jobs``.
    Each job update uses a same-directory temporary file, fsync, atomic rename,
    and parent-directory fsync. Wake appends are serialized by ``flock``.
    """

    def __init__(self, state_dir: str | os.PathLike[str]) -> None:
        self.state_dir = Path(state_dir)
        self.ledger_dir = self.state_dir / "ledger"
        self.wake_path = self.state_dir / "wake.jsonl"
        self.torn_path = self.state_dir / "wake.torn"
        self.cursor_path = self.state_dir / "wake.cursor"

    def record_intent(self, spec: RemoteJobSpec) -> dict:
        """Durably record submit intent before a remote submit is attempted."""
        if not isinstance(spec, RemoteJobSpec):
            raise RemoteJobLedgerError("invalid_spec")
        self._validate_job_id(spec.job_id)
        self._ensure_layout()
        path = self._job_path(spec.job_id)
        with self._locked(self.ledger_dir / ".ledger.lock"):
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

    def mark_done(self, job_id: str, rc: int) -> dict:
        """Record the remote process exit code."""
        if isinstance(rc, bool) or not isinstance(rc, int):
            raise RemoteJobLedgerError("invalid_return_code")

        def update(record: dict) -> None:
            stage = record["stage"]
            if stage in {"intent", "submitted"}:
                record["stage"] = "done"
                record["rc"] = rc
                record["done_at"] = _utc_now()
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
            elif record["stage"] != "completed":
                raise RemoteJobLedgerError("invalid_transition")

        return self._update_job(job_id, update)

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
        with self._locked(lock_path):
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
    ) -> dict:
        """Deliver queued wakes and move the cursor only after durable handling.

        The cursor makes delivery at-least-once, so ``handler`` must be
        idempotent. Failed handlers increment their job's attempt count; the
        fifth failure durably dead-letters the job before advancing the cursor.
        A final unterminated line is ignored and returned as ``torn_tail``.

        ``max_events`` is a nonnegative exact int or None (unlimited), and caps
        processed records, including already-dead-lettered skips. ``should_stop``
        is checked before each complete event; deferred events consume no
        attempts or cursor progress (RES-09). This supplies the daemon's bounded
        drain contract (GRPH-32), with cooperative deadline/cancellation checks,
        not a timeout for a running handler or blocking storage operation.
        Retry limits (RES-06) and durable-before-cursor ordering (DATA-14) hold
        for bounded drains as well. Stop callback exceptions propagate without
        charging a handler attempt.
        """
        if not callable(handler):
            raise RemoteJobLedgerError("invalid_handler")
        if max_events is not None and (type(max_events) is not int or max_events < 0):
            raise RemoteJobLedgerError("invalid_max_events")
        if should_stop is not None and not callable(should_stop):
            raise RemoteJobLedgerError("invalid_should_stop")
        self._ensure_layout()
        result = {"processed": 0, "dead_lettered": 0, "torn_tail": None, "failure": None}
        with self._locked(self.state_dir / "wake.drain.lock"):
            if not self.cursor_path.exists():
                self._write_cursor(0)
            offset = self._read_cursor()
            try:
                data = self.wake_path.read_bytes()
            except FileNotFoundError:
                data = b""
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
                if max_events is not None and result["processed"] >= max_events:
                    break
                if should_stop is not None and should_stop():
                    break
                next_offset = newline + 1
                try:
                    event = WakeEvent.from_json(data[position:newline].decode("utf-8"))
                except (UnicodeDecodeError, ContractError) as exc:
                    raise RemoteJobLedgerError("invalid_wake_event_line") from exc
                try:
                    saved = self.read_job(event.job_id)
                except RemoteJobLedgerError as exc:
                    if exc.reason != "job_not_found":
                        raise
                    saved = None
                if saved is not None and saved["stage"] == "dead_letter":
                    self._write_cursor(next_offset)
                    result["processed"] += 1
                    position = next_offset
                    continue
                try:
                    handler(event)
                except Exception as exc:
                    attempts = self._record_handler_failure(event.job_id, exc)
                    if attempts >= _MAX_HANDLER_ATTEMPTS:
                        self._write_cursor(next_offset)
                        result["processed"] += 1
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
                position = next_offset
        return result

    def _record_handler_failure(self, job_id: str, exc: Exception) -> int:
        reason = _exception_reason(exc)

        def update(record: dict) -> None:
            record["attempts"] = int(record.get("attempts", 0)) + 1
            record["last_error"] = reason
            if record["attempts"] >= _MAX_HANDLER_ATTEMPTS:
                record["stage"] = "dead_letter"
                record["dead_letter_at"] = _utc_now()

        record = self._update_job(job_id, update)
        return int(record["attempts"])

    def _update_job(self, job_id: str, update: Callable[[dict], None]) -> dict:
        self._validate_job_id(job_id)
        self._ensure_layout()
        path = self._job_path(job_id)
        with self._locked(self.ledger_dir / ".ledger.lock"):
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
    def _locked(self, path: Path) -> Iterator[None]:
        self._ensure_layout()
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
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
