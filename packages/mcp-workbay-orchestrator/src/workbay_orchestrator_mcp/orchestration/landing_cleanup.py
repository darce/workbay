"""Atomic, replayable cleanup intents for receipt-bearing lane landings."""

from __future__ import annotations

import base64
import binascii
import bisect
import fcntl
import hashlib
import json
import math
import os
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
_MUTABLE_FIELDS = frozenset({"local_status", "remote_status", "local_result", "remote_result", "completed_at"})
_TERMINAL_CLEANUP_STATES = frozenset({"complete", "skipped"})
_INTENT_FILENAME = re.compile(r"^([0-9a-f]{64})\.json$")
_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_INTENT_FILES = 50_000
_MAX_INTENT_BYTES = 256 * 1024
_MAX_PAGE_SCAN = 2_000


class CleanupIntentError(RuntimeError):
    """A cleanup intent could not be read or durably written."""


class StaleIntentRevision(CleanupIntentError):
    """A caller tried to update a newer cleanup-intent revision."""


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
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CleanupIntentError(f"cleanup_intent_read_failed:{type(exc).__name__}") from exc
    if not isinstance(payload, dict) or payload.get("intent_id") != expected_id:
        raise CleanupIntentError("cleanup_intent_identity_corrupt")
    return payload


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
    if (
        lane_row.get("task_ref") != identity["task_ref"]
        or lane_row.get("lane_id") != identity["lane_id"]
        or lane_row.get("branch_tip_sha") != identity["expected_tip"]
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
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
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
    task_ref: str,
    integration_ref: str,
    cursor: str | None = None,
    limit: int = 100,
    budget_seconds: float = 0.1,
    include_completed: bool = False,
) -> dict[str, Any]:
    """Read one bounded page of unresolved cleanup obligations.

    The page is scoped to an exact task and integration ref and includes only
    intents with either local or remote cleanup still pending. Callers that
    must verify a just-completed identity may set ``include_completed``; valid
    terminal records are then included for that exact scope. The opaque
    ``cursor`` is returned unchanged in shape for the next call; ``has_more``
    marks additional journal entries, including entries belonging to other
    tasks that must be safely passed over. The directory scan, JSON file size,
    and per-page candidate count are all bounded. Cursors are invalidated by a
    journal directory change so a caller must restart the scan rather than
    accidentally skipping an intent written during pagination.

    Malformed identities, statuses, result evidence, paths, or reads raise
    :class:`CleanupIntentError`; callers must treat that as unknown cleanup
    state. Each file is opened as a nonblocking regular file with no symlink
    following, so a planted FIFO or symlink cannot hang or escape the journal.
    """

    if not isinstance(task_ref, str) or not task_ref.strip():
        raise CleanupIntentError("cleanup_intent_task_scope_invalid")
    if not isinstance(integration_ref, str) or not integration_ref.strip():
        raise CleanupIntentError("cleanup_intent_integration_scope_invalid")
    if type(limit) is not int or limit <= 0 or limit > 1_000:
        raise CleanupIntentError("cleanup_intent_page_limit_invalid")
    if type(include_completed) is not bool:
        raise CleanupIntentError("cleanup_intent_include_completed_invalid")
    if isinstance(budget_seconds, bool) or not isinstance(budget_seconds, (int, float)):
        raise CleanupIntentError("cleanup_intent_budget_invalid")
    if not math.isfinite(float(budget_seconds)) or budget_seconds <= 0:
        raise CleanupIntentError("cleanup_intent_budget_invalid")

    deadline = time.monotonic() + float(budget_seconds)
    directory = _cleanup_dir(root)
    try:
        directory_stat = directory.stat()
        names: list[str] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if time.monotonic() >= deadline:
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
    scanned = 0
    scan_cap = min(_MAX_PAGE_SCAN, max(64, limit * 8))
    index = start
    while index < len(names) and scanned < scan_cap and len(page) < limit:
        if time.monotonic() >= deadline:
            raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
        name = names[index]
        match = _INTENT_FILENAME.fullmatch(name)
        if match is None:
            raise CleanupIntentError("cleanup_intent_filename_invalid")
        payload = _read_bounded_intent(directory / name, expected_id=match.group(1))
        payload = _validate_listed_intent(payload, filename_id=match.group(1))
        identity = payload["identity"]
        assert isinstance(identity, dict)
        if identity["task_ref"] == task_ref and identity["integration_ref"] == integration_ref:
            if include_completed or payload["local_status"] == "pending" or payload["remote_status"] == "pending":
                page.append(payload)
        index += 1
        scanned += 1

    has_more = index < len(names)
    next_cursor = _encode_cursor(directory_stat, names[index - 1]) if has_more and index > start else cursor
    if has_more and index == start:
        # A page can only stop without advancing when its deadline has expired;
        # report uncertainty rather than return a cursor that repeats forever.
        raise CleanupIntentError("cleanup_intent_scan_budget_exhausted")
    return {"intents": page, "cursor": next_cursor, "has_more": has_more}


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
