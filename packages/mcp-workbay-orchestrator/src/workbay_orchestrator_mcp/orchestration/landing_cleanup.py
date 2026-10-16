"""Atomic, replayable cleanup intents for receipt-bearing lane landings."""

from __future__ import annotations

import base64
import binascii
import bisect
import fcntl
import hashlib
import json
import logging
import math
import os
import random
import re
import stat
import subprocess
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_IDENTITY_FIELDS = (
    "task_ref",
    "lane_id",
    "expected_tip",
    "integration_ref",
    "landing_commit",
    "run_id",
)
_MUTABLE_FIELDS = frozenset(
    {
        "local_status",
        "remote_status",
        "local_result",
        "remote_result",
        "completed_at",
        "attempts",
        "last_error",
        "next_attempt_at",
    }
)
_TERMINAL_CLEANUP_STATES = frozenset({"complete", "skipped"})
_INTENT_FILENAME = re.compile(r"^([0-9a-f]{64})\.json$")
_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_INTENT_FILES = 50_000
_MAX_INTENT_BYTES = 256 * 1024
_MAX_PAGE_SCAN = 2_000
LOCAL_REPLAY_MIN_REMAINING_S = 15.0  # A started retire has a 15-second operation budget.
_LOGGER = logging.getLogger(__name__)


class CleanupIntentError(RuntimeError):
    """A cleanup intent could not be read or durably written."""


class StaleIntentRevision(CleanupIntentError):
    """A caller tried to update a newer cleanup-intent revision."""


def _monotonic() -> float:
    """Clock seam for bounded drain callers and deterministic tests."""

    return time.monotonic()


def _read_bounded_intent(path: Path, *, expected_id: str) -> dict[str, Any]:
    """Read one regular, size-bounded intent without following links or FIFOs."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise CleanupIntentError(f"cleanup_intent_read_failed:{type(exc).__name__}") from exc
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise CleanupIntentError("cleanup_intent_not_regular_file")
        if file_stat.st_size < 0 or file_stat.st_size > _MAX_INTENT_BYTES:
            raise CleanupIntentError("cleanup_intent_size_invalid")
        chunks: list[bytes] = []
        remaining = _MAX_INTENT_BYTES + 1
        while remaining:
            try:
                chunk = os.read(fd, min(64 * 1024, remaining))
            except OSError as exc:
                raise CleanupIntentError(f"cleanup_intent_read_failed:{type(exc).__name__}") from exc
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > _MAX_INTENT_BYTES:
            raise CleanupIntentError("cleanup_intent_size_invalid")
    finally:
        os.close(fd)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise CleanupIntentError(f"cleanup_intent_read_failed:{type(exc).__name__}") from exc
    if not isinstance(payload, dict) or payload.get("intent_id") != expected_id:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    return payload


def _validated_next_attempt_at(value: object) -> float | None:
    """Reject invalid retry times before any reader or writer converts them."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CleanupIntentError("cleanup_intent_next_attempt_invalid")
    try:
        seconds = float(value)
    except OverflowError as exc:
        raise CleanupIntentError("cleanup_intent_next_attempt_invalid") from exc
    if not math.isfinite(seconds):
        raise CleanupIntentError("cleanup_intent_next_attempt_invalid")
    return seconds


def _validate_listed_intent(payload: Mapping[str, Any], *, filename_id: str) -> dict[str, Any]:
    """Check the complete immutable identity and the durable terminal evidence."""

    identity_raw = payload.get("identity")
    if not isinstance(identity_raw, Mapping):
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    if set(identity_raw) != set(_IDENTITY_FIELDS):
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    identity = _identity(identity_raw)
    intent_id, normalized = _intent_id(identity)
    if normalized != dict(identity_raw) or intent_id != filename_id or payload.get("intent_id") != filename_id:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")

    if type(payload.get("schema_version")) is not int or payload.get("schema_version") != 1:
        raise CleanupIntentError("cleanup_intent_schema_unsupported")
    revision = payload.get("revision")
    if type(revision) is not int or revision <= 0:
        raise CleanupIntentError("cleanup_intent_revision_invalid")
    if _FULL_SHA.fullmatch(identity["expected_tip"]) is None or _FULL_SHA.fullmatch(identity["landing_commit"]) is None:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")

    lane_row = payload.get("lane_row")
    if not isinstance(lane_row, Mapping):
        raise CleanupIntentError("cleanup_intent_lane_snapshot_invalid")
    snapshot_tip = lane_row.get("branch_tip_sha")
    legacy_tip_is_receipt_bound = (
        snapshot_tip is None and lane_row.get("landing_commit_sha") == identity["landing_commit"]
    )
    if (
        lane_row.get("task_ref") != identity["task_ref"]
        or lane_row.get("lane_id") != identity["lane_id"]
        or (snapshot_tip != identity["expected_tip"] and not legacy_tip_is_receipt_bound)
    ):
        raise CleanupIntentError("cleanup_intent_lane_snapshot_mismatch")

    statuses = (payload.get("local_status"), payload.get("remote_status"))
    if any(not isinstance(status, str) or status not in {"pending", "complete", "skipped"} for status in statuses):
        raise CleanupIntentError("cleanup_intent_status_invalid")
    if any(status == "skipped" for status in statuses):
        # No status-only or synthetic skip is completion evidence for cleanup.
        raise CleanupIntentError("cleanup_intent_skipped_unverified")
    results = (payload.get("local_result"), payload.get("remote_result"))
    for status, result in zip(statuses, results, strict=True):
        if status == "pending":
            if result is not None and not isinstance(result, Mapping):
                raise CleanupIntentError("cleanup_intent_result_invalid")
        elif not isinstance(result, Mapping) or result.get("ok") is not True or result.get("status") != "complete":
            raise CleanupIntentError("cleanup_intent_completion_unverified")
    completed_at = payload.get("completed_at")
    if all(status == "complete" for status in statuses):
        if type(completed_at) is not int or completed_at <= 0:
            raise CleanupIntentError("cleanup_intent_completion_unverified")
    elif completed_at is not None:
        raise CleanupIntentError("cleanup_intent_completion_timestamp_invalid")
    attempts = payload.get("attempts", 0)
    if type(attempts) is not int or attempts < 0:
        raise CleanupIntentError("cleanup_intent_attempts_invalid")
    last_error = payload.get("last_error")
    if last_error is not None and not isinstance(last_error, str):
        raise CleanupIntentError("cleanup_intent_last_error_invalid")
    _validated_next_attempt_at(payload.get("next_attempt_at"))
    return dict(payload)


def _cleanup_dir(root: Path | str) -> Path:
    repo = Path(root).expanduser().resolve()
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--git-common-dir"],
            capture_output=True,
            text=True,
            check=False,
            timeout=3,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CleanupIntentError(f"git_common_dir_unavailable:{type(exc).__name__}") from exc
    if result.returncode == 0 and result.stdout.strip():
        common_dir = Path(result.stdout.strip())
        if not common_dir.is_absolute():
            common_dir = repo / common_dir
        directory = common_dir.resolve() / "workbay-landing-cleanup"
    elif (repo / ".git").exists():
        raise CleanupIntentError("git_common_dir_unavailable")
    else:
        # Unit callers may use a fresh state directory. Production callers
        # always resolve a git common directory above.
        directory = repo / ".workbay" / "landing-cleanup"
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise CleanupIntentError(f"cleanup_intent_directory_unavailable:{type(exc).__name__}") from exc
    return directory


def _identity(value: Mapping[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for field in _IDENTITY_FIELDS:
        raw = value.get(field)
        if not isinstance(raw, str) or not raw.strip():
            raise CleanupIntentError(f"cleanup_identity_invalid:{field}")
        normalized[field] = raw.strip()
    return normalized


def _intent_path(root: Path | str, intent_id: str) -> Path:
    if len(intent_id) != 64 or any(char not in "0123456789abcdef" for char in intent_id):
        raise CleanupIntentError("cleanup_intent_id_invalid")
    return _cleanup_dir(root) / f"{intent_id}.json"


def _intent_id(identity: Mapping[str, Any]) -> tuple[str, dict[str, str]]:
    normalized = _identity(identity)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), normalized


def _locked(path: Path):
    handle = path.with_suffix(".lock").open("a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise CleanupIntentError("cleanup_intent_lock_held") from None
    return handle


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
    temporary: Path | None = None
    try:
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(name)
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        raise CleanupIntentError(f"cleanup_intent_write_failed:{type(exc).__name__}") from exc


def _read_path(path: Path, *, expected_id: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, ValueError, RecursionError) as exc:
        raise CleanupIntentError(f"cleanup_intent_read_failed:{type(exc).__name__}") from exc
    if not isinstance(payload, dict) or payload.get("intent_id") != expected_id:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    return payload


def ensure_intent(
    root: Path | str,
    *,
    identity: Mapping[str, Any],
    lane_row: Mapping[str, Any],
) -> dict[str, Any]:
    """Create or return the immutable intent for one verified landing."""
    intent_id, normalized = _intent_id(identity)
    path = _intent_path(root, intent_id)
    lock = _locked(path)
    try:
        payload = _read_path(path, expected_id=intent_id)
        if payload is not None:
            if payload.get("identity") != normalized:
                raise CleanupIntentError("cleanup_intent_identity_conflict")
            return payload
        row_snapshot = dict(lane_row)
        now = time.time_ns()
        payload = {
            "schema_version": 1,
            "intent_id": intent_id,
            "revision": 1,
            "identity": normalized,
            "lane_row": row_snapshot,
            "local_status": "pending",
            "remote_status": "pending",
            "local_result": None,
            "remote_result": None,
            "attempts": 0,
            "last_error": None,
            "next_attempt_at": None,
            "created_at_ns": now,
            "updated_at_ns": now,
            "completed_at": None,
        }
        _atomic_write(path, payload)
        return payload
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def load_intent(root: Path | str, intent_id: str) -> dict[str, Any] | None:
    path = _intent_path(root, intent_id)
    payload = _read_path(path, expected_id=intent_id)
    return dict(payload) if payload is not None else None


def find_intent(root: Path | str, *, identity: Mapping[str, Any]) -> dict[str, Any] | None:
    intent_id, _normalized = _intent_id(identity)
    return load_intent(root, intent_id)


def _encode_cursor(directory_stat: os.stat_result, after: str) -> str:
    state = {
        "v": 1,
        "dev": directory_stat.st_dev,
        "ino": directory_stat.st_ino,
        "mtime_ns": directory_stat.st_mtime_ns,
        "after": after,
    }
    encoded = json.dumps(state, sort_keys=True, separators=(",", ":")).encode("ascii")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str, directory_stat: os.stat_result, names: list[str]) -> str:
    try:
        encoded = cursor.encode("ascii")
        state = json.loads(base64.urlsafe_b64decode(encoded + b"=" * (-len(encoded) % 4)).decode("ascii"))
    except (UnicodeError, ValueError, binascii.Error, json.JSONDecodeError) as exc:
        raise CleanupIntentError("cleanup_intent_cursor_invalid") from exc
    if (
        not isinstance(state, dict)
        or state.get("v") != 1
        or type(state.get("dev")) is not int
        or type(state.get("ino")) is not int
        or type(state.get("mtime_ns")) is not int
        or state.get("dev") != directory_stat.st_dev
        or state.get("ino") != directory_stat.st_ino
        or state.get("mtime_ns") != directory_stat.st_mtime_ns
    ):
        raise CleanupIntentError("cleanup_intent_cursor_stale")
    after = state.get("after")
    if not isinstance(after, str) or after not in names:
        raise CleanupIntentError("cleanup_intent_cursor_invalid")
    return after


def read_pending_intents(
    root: Path | str,
    *,
    task_ref: str | None = None,
    integration_ref: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
    budget_seconds: float = 0.1,
    include_completed: bool = False,
    quarantine_invalid: bool = False,
) -> dict[str, Any]:
    """Read one bounded page of unresolved cleanup obligations.

    The page may be scoped to a task and integration ref; omitting either
    scope matches every value for that field. It includes intents with either
    local or remote cleanup still pending. Callers that must inspect terminal
    records may set ``include_completed``. The opaque
    ``cursor`` is returned unchanged in shape for the next call; ``has_more``
    marks additional journal entries, including entries belonging to other
    tasks that must be safely passed over. The directory scan, JSON file size,
    and per-page candidate count are all bounded. Cursors are invalidated by a
    journal directory change so a caller must restart the scan rather than
    accidentally skipping an intent written during pagination.

    Malformed identities, statuses, result evidence, paths, or reads raise
    :class:`CleanupIntentError` by default; callers must treat that as unknown
    cleanup state. Callers that explicitly set ``quarantine_invalid`` receive
    invalid file names and reasons in ``quarantined_intents`` and the files are
    skipped without changing them. Each file is opened as a nonblocking regular
    file with no symlink following, so a planted FIFO or symlink cannot hang or
    escape the journal.
    """

    if task_ref is not None and (not isinstance(task_ref, str) or not task_ref.strip()):
        raise CleanupIntentError("cleanup_intent_task_scope_invalid")
    if integration_ref is not None and (not isinstance(integration_ref, str) or not integration_ref.strip()):
        raise CleanupIntentError("cleanup_intent_integration_scope_invalid")
    if type(limit) is not int or limit <= 0 or limit > 1_000:
        raise CleanupIntentError("cleanup_intent_page_limit_invalid")
    if type(include_completed) is not bool:
        raise CleanupIntentError("cleanup_intent_include_completed_invalid")
    if type(quarantine_invalid) is not bool:
        raise CleanupIntentError("cleanup_intent_quarantine_invalid")
    if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)):
        raise CleanupIntentError("cleanup_intent_budget_invalid")
    if not math.isfinite(float(budget_seconds)) or budget_seconds <= 0:
        raise CleanupIntentError("cleanup_intent_budget_invalid")

    deadline = _monotonic() + float(budget_seconds)
    directory = _cleanup_dir(root)
    try:
        directory_stat = directory.stat()
        names: list[str] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if _monotonic() >= deadline:
                    raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
                if entry.name.endswith(".json"):
                    names.append(entry.name)
                    if len(names) > _MAX_INTENT_FILES:
                        raise CleanupIntentError("cleanup_intent_scan_limit_exceeded")
        names.sort()
    except CleanupIntentError:
        raise
    except OSError as exc:
        raise CleanupIntentError(f"cleanup_intent_scan_failed:{type(exc).__name__}") from exc

    if cursor is None:
        start = 0
    else:
        if not isinstance(cursor, str) or len(cursor) > 1_024:
            raise CleanupIntentError("cleanup_intent_cursor_invalid")
        after = _decode_cursor(cursor, directory_stat, names)
        start = bisect.bisect_right(names, after)

    page: list[dict[str, Any]] = []
    quarantined: list[dict[str, str]] = []
    scanned = 0
    scan_cap = min(_MAX_PAGE_SCAN, max(64, limit * 8))
    index = start
    while index < len(names) and scanned < scan_cap and len(page) < limit:
        if _monotonic() >= deadline:
            raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
        name = names[index]
        match = _INTENT_FILENAME.fullmatch(name)
        try:
            if match is None:
                raise CleanupIntentError("cleanup_intent_filename_invalid")
            payload = _read_bounded_intent(directory / name, expected_id=match.group(1))
            payload = _validate_listed_intent(payload, filename_id=match.group(1))
        except CleanupIntentError as exc:
            if not quarantine_invalid:
                raise
            quarantined.append(
                {
                    "intent_id": match.group(1) if match is not None else name,
                    "reason": str(exc),
                }
            )
            index += 1
            scanned += 1
            continue
        identity = payload["identity"]
        assert isinstance(identity, dict)
        if (task_ref is None or identity["task_ref"] == task_ref) and (
            integration_ref is None or identity["integration_ref"] == integration_ref
        ):
            if include_completed or payload["local_status"] == "pending" or payload["remote_status"] == "pending":
                page.append(payload)
        index += 1
        scanned += 1

    if _monotonic() >= deadline:
        raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")

    has_more = index < len(names)
    next_cursor = _encode_cursor(directory_stat, names[index - 1]) if has_more and index > start else cursor
    if has_more and index == start:
        # A page can only stop without advancing when its deadline has expired;
        # report uncertainty rather than return a cursor that repeats forever.
        raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
    return {
        "intents": page,
        "cursor": next_cursor,
        "has_more": has_more,
        "quarantined_intents": quarantined,
    }


def read_verified_intent(root: Path | str, *, identity: Mapping[str, Any]) -> dict[str, Any] | None:
    """Load one identity-hashed intent using the bounded, no-follow reader."""

    intent_id, normalized = _intent_id(identity)
    path = _intent_path(root, intent_id)
    try:
        payload = _read_bounded_intent(path, expected_id=intent_id)
    except CleanupIntentError as exc:
        if str(exc).startswith("cleanup_intent_read_failed:FileNotFoundError"):
            return None
        raise
    if payload.get("identity") != normalized:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    return _validate_listed_intent(payload, filename_id=intent_id)


def update_intent(
    root: Path | str,
    intent_id: str,
    *,
    expected_revision: int,
    **updates: Any,
) -> dict[str, Any]:
    """Durably CAS one result; duplicate updates remain idempotent."""
    unknown = set(updates) - _MUTABLE_FIELDS
    if unknown:
        raise CleanupIntentError(f"cleanup_intent_field_unwritable:{','.join(sorted(unknown))}")
    for field in ("local_status", "remote_status"):
        if field in updates and updates[field] not in {"pending", *_TERMINAL_CLEANUP_STATES}:
            raise CleanupIntentError(f"cleanup_intent_status_invalid:{field}")
    if "attempts" in updates and (type(updates["attempts"]) is not int or updates["attempts"] < 0):
        raise CleanupIntentError("cleanup_intent_attempts_invalid")
    if "last_error" in updates and updates["last_error"] is not None and not isinstance(updates["last_error"], str):
        raise CleanupIntentError("cleanup_intent_last_error_invalid")
    if "next_attempt_at" in updates:
        _validated_next_attempt_at(updates["next_attempt_at"])
    path = _intent_path(root, intent_id)
    lock = _locked(path)
    try:
        payload = _read_path(path, expected_id=intent_id)
        if payload is None:
            raise CleanupIntentError("cleanup_intent_missing")
        current_revision = payload.get("revision")
        if current_revision != expected_revision:
            # Repeating the same transition after losing the acknowledgement is safe.
            if all(payload.get(key) == value for key, value in updates.items()):
                return payload
            raise StaleIntentRevision("cleanup_intent_revision_changed")
        payload.update(updates)
        payload["revision"] = expected_revision + 1
        payload["updated_at_ns"] = time.time_ns()
        local_done = payload.get("local_status") in _TERMINAL_CLEANUP_STATES
        remote_done = payload.get("remote_status") in _TERMINAL_CLEANUP_STATES
        if local_done and remote_done:
            payload["completed_at"] = payload.get("completed_at") or payload["updated_at_ns"]
        _atomic_write(path, payload)
        return payload
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _seconds(now: object) -> float:
    if now is None:
        return time.time()
    if isinstance(now, bool):
        raise CleanupIntentError("cleanup_intent_clock_invalid")
    if isinstance(now, (int, float)) and math.isfinite(float(now)):
        return float(now)
    timestamp = getattr(now, "timestamp", None)
    if callable(timestamp):
        value = timestamp()
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
            return float(value)
    raise CleanupIntentError("cleanup_intent_clock_invalid")


def _registered_lane_rows() -> list[dict[str, Any]]:
    """Return a complete current ownership snapshot for safe remote reaping."""

    from workbay_handoff_mcp import lanes_recording

    raw = lanes_recording.list_lanes(all_tasks=True, status="all", limit=10_000, after_id=None)
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CleanupIntentError("cleanup_lane_registry_unreadable") from exc
    if not isinstance(raw, Mapping) or raw.get("ok") is False:
        raise CleanupIntentError("cleanup_lane_registry_unreadable")
    data = raw.get("data", raw)
    rows = data.get("lanes") if isinstance(data, Mapping) else None
    if not isinstance(rows, list) or data.get("has_more") is not False:
        raise CleanupIntentError("cleanup_lane_registry_incomplete")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _budgeted_remote_runner(root: Path, deadline: float):
    def run(script: str, *, timeout: float | None = None) -> Any:
        remaining = deadline - _monotonic()
        if remaining <= 0:
            raise TimeoutError("cleanup_intent_drain_budget_exhausted")
        from .lane_census import _default_sandbox_ssh_runner

        request_timeout = max(1.0, remaining) if timeout is None else min(float(timeout), max(1.0, remaining))
        return _default_sandbox_ssh_runner(script, timeout=request_timeout, workspace_root=root)

    return run


def _remote_reap_result(
    root: Path,
    intent: Mapping[str, Any],
    rows: list[dict[str, Any]],
    *,
    deadline: float,
) -> dict[str, Any]:
    identity = intent.get("identity")
    lane_row = intent.get("lane_row")
    if not isinstance(identity, Mapping) or not isinstance(lane_row, Mapping):
        return {"ok": False, "status": "pending", "reason": "cleanup_intent_snapshot_invalid"}
    task_ref = identity.get("task_ref")
    lane_id = identity.get("lane_id")
    expected_tip = identity.get("expected_tip")
    landing_commit = identity.get("landing_commit")
    branch = lane_row.get("branch")
    if not all(
        isinstance(value, str) and value.strip() for value in (task_ref, lane_id, expected_tip, landing_commit, branch)
    ):
        return {"ok": False, "status": "pending", "reason": "cleanup_intent_snapshot_incomplete"}

    backend = lane_row.get("backend")
    if not isinstance(backend, str) or not backend.strip():
        current = next(
            (
                row
                for row in rows
                if row.get("task_ref") == task_ref
                and row.get("lane_id") == lane_id
                and row.get("branch") == branch
                and row.get("branch_tip_sha") == expected_tip
                and row.get("landing_commit_sha") == landing_commit
                and row.get("status") == "merged"
            ),
            None,
        )
        backend = current.get("backend") if current is not None else None
    if not isinstance(backend, str) or not backend.strip():
        return {"ok": False, "status": "pending", "reason": "lane_backend_unavailable"}
    from .worker_daemon_ctl import is_remote_worker_backend

    remote_backend = is_remote_worker_backend(backend)
    if remote_backend is None:
        return {"ok": False, "status": "pending", "reason": "remote_backend_classification_failed"}
    if not remote_backend:
        return {"ok": True, "status": "complete", "reason": "local_backend"}

    from . import remote_sandbox_reap

    remote_runner = _budgeted_remote_runner(root, deadline)
    marker = remote_sandbox_reap.write_merged_marker(
        ({"branch": branch, "receipt_sha": landing_commit},), repo_root=root, runner=remote_runner
    )
    if getattr(marker, "ok", False) is not True:
        marker_error = getattr(marker, "error", "")
        return {
            "ok": False,
            "status": "pending",
            "reason": f"merged_marker_write_unverified:{marker_error}"[:500],
        }
    protected_rows = [row for row in rows if not (row.get("task_ref") == task_ref and row.get("lane_id") == lane_id)]
    protected_lane = dict(lane_row)
    protected_lane.update(
        {
            "task_ref": task_ref,
            "lane_id": lane_id,
            "branch_tip_sha": expected_tip,
            "landing_commit_sha": landing_commit,
            "status": "merged",
        }
    )
    protected_rows.append(protected_lane)
    result = remote_sandbox_reap.reap_remote_lane_sandbox(
        task_ref,
        lane_id=lane_id,
        branch=branch,
        expected_tip=expected_tip,
        primary_repo=root,
        rows=protected_rows,
        ssh_runner=remote_runner,
    )
    if isinstance(result, dict):
        return result
    return {"ok": False, "status": "pending", "reason": "unreadable_remote_result"}


SCAN_FRACTION = 0.25
STALE_REPLAYS_PER_DRAIN = 2


def _cleanup_retry_delay(failed_attempts: int, *, stale: bool) -> float:
    """Double a minute-scale retry up to its cap, then clamp bounded jitter."""

    cap = 86_400.0 if stale else 3_600.0
    # A float base keeps int ** int from inferring Any (mypy no-any-return).
    doublings = min(max(failed_attempts - 1, 0), 11)
    delay = min(cap, 60.0 * 2.0**doublings)
    return min(cap, delay * random.uniform(0.8, 1.2))


def _save_cleanup_replay(root: Path | str, intent: Mapping[str, Any], **updates: Any) -> tuple[dict[str, Any], bool]:
    """Keep durable state on CAS or lock contention; never hold a replay lock."""

    try:
        return update_intent(
            root, str(intent["intent_id"]), expected_revision=int(intent["revision"]), **updates
        ), False
    except CleanupIntentError as exc:
        if not isinstance(exc, StaleIntentRevision) and str(exc) != "cleanup_intent_lock_held":
            raise
        refreshed = read_verified_intent(root, identity=intent["identity"])
        if refreshed is None:
            raise CleanupIntentError("cleanup_intent_missing_after_replay")
        complete = (
            refreshed["local_status"] in _TERMINAL_CLEANUP_STATES
            and refreshed["remote_status"] in _TERMINAL_CLEANUP_STATES
        )
        return refreshed, not complete


def _cleanup_created_at_ns(intent: Mapping[str, Any]) -> int:
    """Order an absent/unobserved creation time accepted by the reader as oldest."""

    value = intent.get("created_at_ns")
    return value if type(value) is int else 0


def _cleanup_age_seconds(intent: Mapping[str, Any], now_seconds: float) -> float:
    """An unobserved creation time is age zero, so a first attempt stays replayable."""

    created_ns = intent.get("created_at_ns")
    if type(created_ns) is not int:
        return 0.0
    return max(0.0, now_seconds - created_ns / 1_000_000_000)


def _cleanup_remote_owner_refusal(intent: Mapping[str, Any], rows: list[dict[str, Any]]) -> str | None:
    """Never let the historical receipt snapshot replace or clear a different current owner.

    The sandbox key derives from the branch, so every row on the same branch maps
    to the same remote sandbox, whichever task or lane owns it. The owner check
    runs before any marker write, so a refusal leaves no marker behind.
    """

    from .remote_sandbox_reap import TERMINAL_ROW_STATUSES, _branch_identity, _derive_lane_key

    identity = intent["identity"]
    snapshot = intent["lane_row"]
    snapshot_branch = snapshot.get("branch")
    snapshot_key = (
        _derive_lane_key(_branch_identity(snapshot_branch))
        if isinstance(snapshot_branch, str) and snapshot_branch.strip()
        else None
    )
    for row in rows:
        same_owner = row.get("task_ref") == identity["task_ref"] and row.get("lane_id") == identity["lane_id"]
        row_branch = row.get("branch")
        shares_key = (
            snapshot_key is not None
            and isinstance(row_branch, str)
            and row_branch.strip() != ""
            and _derive_lane_key(_branch_identity(row_branch)) == snapshot_key
        )
        if not same_owner and not shares_key:
            continue
        if row.get("status") not in TERMINAL_ROW_STATUSES:
            return "cleanup_lane_owner_nonterminal" if same_owner else "cleanup_lane_key_owner_nonterminal"
        if not same_owner:
            return "cleanup_lane_key_owner_foreign"
        if (
            row.get("branch") != snapshot.get("branch")
            or row.get("branch_tip_sha") != identity["expected_tip"]
            or row.get("landing_commit_sha") != identity["landing_commit"]
            # Worker run/pass identifiers, when recorded, belong to the snapshot;
            # the intent's run_id instead names the verified landing receipt.
            or any(row.get(field) != snapshot.get(field) for field in ("run_id", "pass_id"))
        ):
            return "cleanup_lane_owner_identity_mismatch"
    # Archived rows may be absent; the unchanged reaper still requires the
    # pinned landing product and protects every remaining registry owner.
    return None


def drain_pending_cleanup_intents(
    root: Path | str,
    *,
    max_intents: int = 10,
    budget_seconds: float = 30,
    now: object = None,
) -> dict[str, Any]:
    """Replay due cleanup obligations across every task, reserving stale service.

    A completed journal record is reported as ``already_complete`` without
    repeating either cleanup half. Pending records are retried at most
    ``max_intents`` times in one invocation; the wall-clock budget also bounds
    scanning and replay. The drain may overrun its budget by at most one started
    cleanup step, because a started retire is never interrupted. Retry failures
    stay durable and use capped exponential backoff with bounded jitter. Due
    stale remote-only records get the first slots, capped independently at two;
    stale local obligations remain visible without unsafe landing replay.
    """

    if type(max_intents) is not int or max_intents <= 0:
        raise CleanupIntentError("cleanup_intent_drain_limit_invalid")
    if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)):
        raise CleanupIntentError("cleanup_intent_budget_invalid")
    if not math.isfinite(float(budget_seconds)) or budget_seconds <= 0:
        raise CleanupIntentError("cleanup_intent_budget_invalid")
    started = _monotonic()
    deadline = started + float(budget_seconds)
    scan_budget = SCAN_FRACTION * float(budget_seconds)
    if budget_seconds > LOCAL_REPLAY_MIN_REMAINING_S:
        scan_budget = min(scan_budget, float(budget_seconds) - LOCAL_REPLAY_MIN_REMAINING_S)
    scan_deadline = started + scan_budget
    explicit_now = now is not None
    now_seconds = _seconds(now)
    pages: list[dict[str, Any]] = []
    quarantined: list[dict[str, str]] = []
    quarantined_count = 0
    quarantined_by_reason: dict[str, int] = {}
    cursor: str | None = None
    try:
        while True:
            remaining = deadline - _monotonic()
            if remaining <= 0:
                raise CleanupIntentError("cleanup_intent_drain_budget_exhausted")
            scan_remaining = scan_deadline - _monotonic()
            if scan_remaining <= 0:
                raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
            # Each page still re-enumerates/sorts the directory (O(pages * N log N));
            # improving that scaling requires a separate cursor/journal change.
            page = read_pending_intents(
                root,
                task_ref=None,
                integration_ref=None,
                cursor=cursor,
                limit=1_000,
                budget_seconds=min(scan_remaining, remaining),
                include_completed=True,
                quarantine_invalid=True,
            )
            intents = page.get("intents")
            if not isinstance(intents, list) or any(not isinstance(intent, dict) for intent in intents):
                raise CleanupIntentError("cleanup_intent_page_invalid")
            pages.extend(intents)
            page_quarantined = page.get("quarantined_intents")
            if not isinstance(page_quarantined, list) or any(
                not isinstance(item, dict)
                or not isinstance(item.get("intent_id"), str)
                or not isinstance(item.get("reason"), str)
                for item in page_quarantined
            ):
                raise CleanupIntentError("cleanup_intent_page_invalid")
            quarantined_count += len(page_quarantined)
            for item in page_quarantined:
                reason = item["reason"]
                quarantined_by_reason[reason] = quarantined_by_reason.get(reason, 0) + 1
                if len(quarantined) < 50:
                    quarantined.append(item)
            if _monotonic() >= scan_deadline:
                raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
            if page.get("has_more") is not True:
                break
            next_cursor = page.get("cursor")
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise CleanupIntentError("cleanup_intent_pagination_stalled")
            cursor = next_cursor
    finally:
        if quarantined_count:
            _LOGGER.warning(
                "Quarantined %d invalid cleanup intents during drain; reasons=%s",
                quarantined_count,
                dict(sorted(quarantined_by_reason.items())),
            )

    completed_records = [
        item for item in pages if item.get("local_status") == "complete" and item.get("remote_status") == "complete"
    ]
    pending_records = [
        item for item in pages if item.get("local_status") == "pending" or item.get("remote_status") == "pending"
    ]
    pending_records.sort(
        key=lambda item: (
            _cleanup_created_at_ns(item),
            str(item.get("intent_id") or ""),
        )
    )
    result: dict[str, Any] = {
        "ok": True,
        "replayed": 0,
        "completed": 0,
        "already_complete": len(completed_records),
        "still_pending": len(pending_records),
        "local_completed": 0,
        "local_still_pending": sum(1 for item in pending_records if item.get("local_status") == "pending"),
        "skipped_not_due": 0,
        "skipped_budget": 0,
        "stale_cleanup_intents": [],
        "stale_count": 0,
        "contended": 0,
        "last_error_counts": {},
        "quarantined_cleanup_intents": quarantined,
        "quarantined_cleanup_intent_count": quarantined_count,
        "quarantined_cleanup_intents_by_reason": dict(sorted(quarantined_by_reason.items())),
        "elapsed_ms": 0,
    }
    stale: list[dict[str, Any]] = result["stale_cleanup_intents"]
    fresh: list[dict[str, Any]] = []
    stale_remote: list[dict[str, Any]] = []
    stale_ids: set[str] = set()
    latest = {str(intent["intent_id"]): intent for intent in pending_records}

    def save(intent: Mapping[str, Any], **updates: Any) -> None:
        refreshed, contended = _save_cleanup_replay(root, intent, **updates)
        latest[str(intent["intent_id"])] = refreshed
        result["contended"] += int(contended)

    for intent in pending_records:
        age_seconds = _cleanup_age_seconds(intent, now_seconds)
        raw_attempts = intent.get("attempts", 0)
        attempts = raw_attempts if type(raw_attempts) is int and raw_attempts >= 0 else 0
        identity = intent.get("identity")
        identity = identity if isinstance(identity, Mapping) else {}
        lane_id = intent.get("lane_row", {}).get("lane_id") if isinstance(intent.get("lane_row"), Mapping) else None
        task_ref = identity.get("task_ref")
        last_error = intent.get("last_error")
        is_stale = attempts >= 8 or age_seconds > 24 * 60 * 60
        if is_stale:
            stale.append(
                {
                    "intent_id": intent.get("intent_id"),
                    "lane": lane_id,
                    "task_ref": task_ref,
                    "age_s": age_seconds,
                    "attempts": attempts,
                    "last_error": last_error,
                    **({"reason": "stale_local_replay_unsupported"} if intent.get("local_status") == "pending" else {}),
                }
            )
        local_pending = intent.get("local_status") == "pending"
        remote_pending = intent.get("remote_status") == "pending"
        if not local_pending and not remote_pending:
            continue
        if local_pending and isinstance(last_error, str) and last_error.startswith("predecessor_retire_preflight_"):
            # A predecessor refusal must not be replayed through lane_land:
            # that replay can close the row before the canonical retirement
            # checks run again. prepare_contained_predecessors owns retries and
            # clears this marker only after a fresh successful preflight.
            result["skipped_not_due"] += 1
            continue
        if is_stale and local_pending:
            # Receipt-bound stale local replay belongs to drainreplay-01.
            continue
        if not local_pending and intent.get("local_status") != "complete":
            continue
        next_attempt_at = _validated_next_attempt_at(intent.get("next_attempt_at"))
        if next_attempt_at is not None and next_attempt_at > now_seconds:
            result["skipped_not_due"] += 1
            continue
        if is_stale:
            stale_remote.append(intent)
            stale_ids.add(str(intent["intent_id"]))
        else:
            fresh.append(intent)

    result["stale_count"] = len(stale)
    stale_remote.sort(
        key=lambda item: (
            item["next_attempt_at"] if item.get("next_attempt_at") is not None else float("-inf"),
            _cleanup_created_at_ns(item),
            item["intent_id"],
        )
    )
    # Serve a due stale remote slot first, even when fresh work fills the limit.
    selected = stale_remote[: min(STALE_REPLAYS_PER_DRAIN, max_intents)]
    selected += fresh[: max_intents - len(selected)]
    for intent in selected:
        remaining = deadline - _monotonic()
        local_pending = intent.get("local_status") == "pending"
        if remaining <= 0 or (local_pending and remaining < LOCAL_REPLAY_MIN_REMAINING_S):
            result["skipped_budget"] += 1
            continue
        attempts = intent.get("attempts", 0)
        identity = intent["identity"]
        is_stale = str(intent["intent_id"]) in stale_ids

        if local_pending:
            # Reuse the saved-receipt landing path; skip only its nested global drain.
            from . import lane_land

            replay_error: str | None = None
            if not all(
                isinstance(value, str) and value.strip()
                for value in (
                    identity.get("task_ref"),
                    identity.get("lane_id"),
                    identity.get("expected_tip"),
                    identity.get("integration_ref"),
                    identity.get("run_id"),
                )
            ):
                replay_error = "cleanup_intent_identity_invalid"
            else:
                try:
                    replay_result = lane_land.land(
                        str(identity["task_ref"]),
                        str(identity["lane_id"]),
                        str(identity["expected_tip"]),
                        str(identity["run_id"]),
                        integration_ref=str(identity["integration_ref"]),
                        orchestrator_root=Path(root).expanduser().resolve(),
                        _drain_cleanup=False,
                        _cleanup_deadline=deadline,
                    )
                    if isinstance(replay_result, Mapping):
                        detail = replay_result.get("detail")
                        if isinstance(detail, str) and detail.startswith("predecessor_retire_preflight_"):
                            replay_error = " ".join(detail.splitlines())[:500]
                except Exception as exc:
                    message = " ".join(str(exc).splitlines())[:400]
                    replay_error = f"local_cleanup_replay_failed:{type(exc).__name__}:{message}"

            refreshed = load_intent(root, str(intent["intent_id"]))
            if refreshed is None:
                raise CleanupIntentError("cleanup_intent_missing_after_local_replay")
            latest_attempts = refreshed.get("attempts", attempts)
            if type(latest_attempts) is not int or latest_attempts < 0:
                latest_attempts = attempts
            failed_attempts = latest_attempts + 1
            local_status = refreshed.get("local_status")
            remote_status = refreshed.get("remote_status")
            fully_complete = local_status in {"complete", "skipped"} and remote_status in {"complete", "skipped"}
            if fully_complete:
                save(
                    refreshed,
                    attempts=failed_attempts,
                    last_error=None,
                    next_attempt_at=None,
                )
            else:
                local_result = refreshed.get("local_result")
                local_result = local_result if isinstance(local_result, Mapping) else {}
                remote_result = refreshed.get("remote_result")
                remote_result = remote_result if isinstance(remote_result, Mapping) else {}
                error = replay_error
                if error is None and local_status == "pending":
                    error = local_result.get("detail")
                    if not isinstance(error, str) or not error.strip():
                        error = local_result.get("outcome")
                    if not isinstance(error, str) or not error.strip():
                        error = "local_cleanup_incomplete"
                if error is None and remote_status == "pending":
                    error = remote_result.get("reason")
                    if not isinstance(error, str) or not error.strip():
                        error = "remote_cleanup_incomplete"
                error = " ".join(str(error).splitlines())[:500]
                delay = _cleanup_retry_delay(failed_attempts, stale=is_stale)
                retry_base = now_seconds if explicit_now else time.time()
                save(
                    refreshed,
                    attempts=failed_attempts,
                    last_error=error,
                    next_attempt_at=retry_base + delay,
                )
            result["replayed"] += 1
            continue

        try:
            # Refresh between replays too: a previous remote operation may have
            # overlapped a lane reassignment in the registry.
            rows = _registered_lane_rows()
            if _monotonic() >= deadline:
                result["skipped_budget"] += 1
                continue
            owner_refusal = _cleanup_remote_owner_refusal(intent, rows)
            if owner_refusal is not None:
                remote_result = {"ok": False, "status": "pending", "reason": owner_refusal}
            else:
                remote_result = _remote_reap_result(Path(root).expanduser().resolve(), intent, rows, deadline=deadline)
        except Exception as exc:
            message = " ".join(str(exc).splitlines())[:500]
            remote_result = {
                "ok": False,
                "status": "pending",
                "reason": f"remote_cleanup_failed:{type(exc).__name__}:{message}",
            }
        result["replayed"] += 1
        if remote_result.get("ok") is True and remote_result.get("status") == "complete":
            save(
                intent,
                remote_status="complete",
                remote_result=remote_result,
                attempts=attempts + 1,
                last_error=None,
                next_attempt_at=None,
            )
            continue

        error = remote_result.get("reason")
        if not isinstance(error, str) or not error.strip():
            error = "remote_cleanup_incomplete"
        error = " ".join(error.splitlines())[:500]
        failed_attempts = attempts + 1
        delay = _cleanup_retry_delay(failed_attempts, stale=is_stale)
        retry_base = now_seconds if explicit_now else time.time()
        save(
            intent,
            remote_result={"ok": False, "status": "pending", "reason": error},
            attempts=failed_attempts,
            last_error=error,
            next_attempt_at=retry_base + delay,
        )

    unresolved = [
        item
        for item in latest.values()
        if item.get("local_status") == "pending" or item.get("remote_status") == "pending"
    ]
    result["still_pending"] = len(unresolved)
    result["completed"] = len(pending_records) - len(unresolved)
    result["local_still_pending"] = sum(item.get("local_status") == "pending" for item in unresolved)
    result["local_completed"] = sum(
        item.get("local_status") == "pending"
        and latest[str(item["intent_id"])].get("local_status") in _TERMINAL_CLEANUP_STATES
        for item in pending_records
    )
    error_counts: dict[str, int] = {}
    for item in unresolved:
        error = item.get("last_error")
        key = "none" if error is None else str(error).split(":", 1)[0][:80]
        error_counts[key] = error_counts.get(key, 0) + 1
    result["last_error_counts"] = dict(sorted(error_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:10])
    if unresolved:
        _LOGGER.warning(
            "Cleanup intents pending after drain: unresolved=%d stale_count=%d replayed=%d completed=%d last_error_counts=%s",
            len(unresolved),
            result["stale_count"],
            result["replayed"],
            result["completed"],
            dict(list(result["last_error_counts"].items())[:3]),
        )
    result["elapsed_ms"] = max(0, int((_monotonic() - started) * 1_000))
    return result
