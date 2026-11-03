"""Derived per-lane index for durable offload pass records.

Pass JSON files remain the source of truth. One append-only JSONL ledger maps
pass ids to their task and lane and retains prior identities as doubt rows
when sources contradict membership. Readers compare the ledger ids with a
stat-only listing of record filenames and parse the set difference to heal
missed appends without relying on filesystem timestamps.
"""

from __future__ import annotations

import fcntl
import json
import logging
import math
import os
import re
import stat
import tempfile
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from json.decoder import scanstring
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

_INDEX_DIRNAME = "offload-pass-index"
_LEDGER_FILENAME = "ledger.jsonl"
_PASS_PREFIX = "offload-pass-"
_PASS_SUFFIX = ".json"
_LEGACY_HIGH_WATER_FILENAME = "high-water-ns"
_LEGACY_MEMBERSHIP_DIRNAME = "membership"
_INDEX_LOCK_TIMEOUT_ENV = "WORKBAY_OFFLOAD_PASS_INDEX_LOCK_TIMEOUT_SECONDS"
_DEFAULT_INDEX_LOCK_TIMEOUT_SECONDS = 3.0
_INDEX_LOCK_RETRY_SECONDS = 0.025
_PASS_ID_PATTERN = re.compile(r"[^/\\\x00-\x1f]+")


@dataclass(frozen=True)
class LanePassRecord:
    """One parsed pass belonging to a lane, with its filesystem mtime."""

    pass_id: str
    payload: dict[str, object]
    mtime: float


@dataclass(frozen=True)
class LanePassRecords:
    """The matching lane records and whether the lookup had complete evidence."""

    records: tuple[LanePassRecord, ...]
    status: Literal["ok", "unknown"]
    doubtful_pass_ids: tuple[str, ...] = ()
    sources_complete: bool = True
    doubt_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class _LedgerSnapshot:
    entries: dict[str, tuple[str, str]]
    needs_rebuild: bool
    # (pass id, task, lane, reason); separate from current source membership.
    doubts: set[tuple[str, str, str, str]] = field(default_factory=set)
    rewrite_blocked: bool = False
    unreadable: bool = False
    # Last membership row's position in this ledger inode. A duplicate append
    # is a new publication even when its task/lane identity stays unchanged.
    publications: dict[str, tuple[int, int, int]] = field(default_factory=dict)
    # (device, inode, byte offset just past the last complete line), or None
    # when no readable ledger file supplied the snapshot.
    ledger_end: tuple[int, int, int] | None = None
    # Number of complete JSONL rows parsed, including duplicate and doubt rows.
    rows: int = 0


class IndexLockTimeout(TimeoutError):
    """The pass-index directory lock was not available before its deadline."""


def _ledger_path(state_dir: Path) -> Path:
    return Path(state_dir) / _INDEX_DIRNAME / _LEDGER_FILENAME


def _valid_pass_id(pass_id: str) -> bool:
    return _PASS_ID_PATTERN.fullmatch(pass_id) is not None


def _resolve_index_lock_timeout(index_lock_timeout_seconds: float | None) -> float:
    if index_lock_timeout_seconds is None:
        timeout_value = os.environ.get(
            _INDEX_LOCK_TIMEOUT_ENV,
            str(_DEFAULT_INDEX_LOCK_TIMEOUT_SECONDS),
        )
        try:
            index_lock_timeout_seconds = float(timeout_value)
        except ValueError as exc:
            raise ValueError(f"{_INDEX_LOCK_TIMEOUT_ENV} must be a non-negative number") from exc
    if not math.isfinite(index_lock_timeout_seconds) or index_lock_timeout_seconds < 0:
        raise ValueError("index_lock_timeout_seconds must be a finite non-negative number")
    return index_lock_timeout_seconds


@contextmanager
def _index_lock(state_dir: Path, *, timeout: float = _DEFAULT_INDEX_LOCK_TIMEOUT_SECONDS) -> Iterator[Path]:
    """Lock the stable index directory inode across append and ledger replace."""

    index_dir = Path(state_dir) / _INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(index_dir, os.O_RDONLY)
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise IndexLockTimeout(f"index_lock_timeout after {timeout:g}s for {index_dir}") from exc
                time.sleep(min(_INDEX_LOCK_RETRY_SECONDS, remaining))
        yield index_dir
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _remove_legacy_indexes(index_dir: Path) -> None:
    """Remove the obsolete lane shards, reverse shards, and mtime watermark."""

    for path in index_dir.glob("*.ids"):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    membership_dir = index_dir / _LEGACY_MEMBERSHIP_DIRNAME
    if membership_dir.is_dir():
        for path in membership_dir.glob("*.ids"):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        try:
            membership_dir.rmdir()
        except OSError:
            pass
    try:
        (index_dir / _LEGACY_HIGH_WATER_FILENAME).unlink()
    except FileNotFoundError:
        pass


class _DuplicateLedgerKey(ValueError):
    """A repeated key makes the fallback row's identity ambiguous."""


def _unique_ledger_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    row: dict[str, object] = {}
    for key, value in pairs:
        if key in row:
            raise _DuplicateLedgerKey("duplicate ledger key")
        row[key] = value
    return row


def _read_ledger(path: Path) -> _LedgerSnapshot:
    """Read complete ledger lines; a torn final line is ignored."""

    try:
        with path.open("rb") as ledger:
            metadata = os.fstat(ledger.fileno())
            data = ledger.read()
    except FileNotFoundError:
        return _LedgerSnapshot({}, True)
    except OSError:
        return _LedgerSnapshot({}, True, rewrite_blocked=True, unreadable=True)

    if data and not data.endswith(b"\n"):
        data = data[: data.rfind(b"\n") + 1]
    ledger_end = (metadata.st_dev, metadata.st_ino, len(data))
    rows = len(data.splitlines())

    entries: dict[str, tuple[str, str]] = {}
    publications: dict[str, tuple[int, int, int]] = {}
    doubts: set[tuple[str, str, str, str]] = set()
    corrupt = False
    rewrite_blocked = False
    for position, raw_line in enumerate(data.splitlines()):
        if not raw_line:
            corrupt = True
            continue
        malformed_row = False
        try:
            row = _parse_ledger_line(raw_line)
        except (UnicodeError, ValueError):
            corrupt = True
            malformed_row = True
            # Recover identities from a doubt row with a non-string value.
            # Unknown identities cannot safely be erased by a source rebuild.
            try:
                row = json.loads(raw_line, object_pairs_hook=_unique_ledger_pairs)
            except _DuplicateLedgerKey:
                rewrite_blocked = True
                continue
            except (UnicodeError, ValueError):
                if b'"d"' in raw_line:
                    rewrite_blocked = True
                continue
            if not isinstance(row, dict) or "d" not in row:
                continue
        pass_id = row.get("p")
        task_ref = row.get("t")
        lane_id = row.get("l")
        if (
            not isinstance(pass_id, str)
            or not _valid_pass_id(pass_id)
            or not isinstance(task_ref, str)
            or not task_ref
            or not isinstance(lane_id, str)
            or not lane_id
        ):
            corrupt = True
            if "d" in row:
                rewrite_blocked = True
            continue
        if "d" in row:
            reason = row["d"]
            if malformed_row or reason not in ("ledger_source_contradiction", "doubt_row_malformed"):
                corrupt = True
                reason = "doubt_row_malformed"
            doubts.add((pass_id, task_ref, lane_id, reason))
            continue
        entry = (task_ref, lane_id)
        previous = entries.get(pass_id)
        if previous is not None and previous != entry:
            corrupt = True
            # Keep the last appended identity and evidence for both lanes.
            # Discarding this row hides concurrent publication from a rebuild
            # whose source observations predate the append.
            doubts.add((pass_id, *previous, "ledger_source_contradiction"))
            doubts.add((pass_id, *entry, "ledger_source_contradiction"))
        entries[pass_id] = entry
        publications[pass_id] = (metadata.st_dev, metadata.st_ino, position)
    return _LedgerSnapshot(
        entries,
        corrupt,
        doubts,
        rewrite_blocked,
        publications=publications,
        ledger_end=ledger_end,
        rows=rows,
    )


def _read_ledger_tail(path: Path, snapshot: _LedgerSnapshot) -> _LedgerSnapshot:
    """Merge a clean membership-only append tail, falling back on any doubt."""

    ledger_end = snapshot.ledger_end
    if (
        ledger_end is None
        or snapshot.needs_rebuild
        or snapshot.doubts
        or snapshot.rewrite_blocked
        or snapshot.unreadable
    ):
        return _read_ledger(path)

    device, inode, offset = ledger_end
    try:
        with path.open("rb") as ledger:
            metadata = os.fstat(ledger.fileno())
            if (metadata.st_dev, metadata.st_ino) != (device, inode) or metadata.st_size < offset:
                return _read_ledger(path)
            ledger.seek(offset)
            tail = ledger.read()
            after_read = os.fstat(ledger.fileno())
        current = path.stat()
    except OSError:
        return _read_ledger(path)

    if (
        (after_read.st_dev, after_read.st_ino) != (device, inode)
        or (current.st_dev, current.st_ino) != (device, inode)
        or after_read.st_size != offset + len(tail)
        or current.st_size != after_read.st_size
        or (tail and not tail.endswith(b"\n"))
    ):
        return _read_ledger(path)
    if not tail:
        return snapshot

    entries = dict(snapshot.entries)
    publications = dict(snapshot.publications)
    tail_rows = tail.splitlines()
    for row_offset, raw_line in enumerate(tail_rows):
        if not raw_line:
            return _read_ledger(path)
        try:
            row = _parse_ledger_line(raw_line)
        except (UnicodeError, ValueError):
            return _read_ledger(path)
        if "d" in row:
            return _read_ledger(path)
        pass_id = row.get("p")
        task_ref = row.get("t")
        lane_id = row.get("l")
        if (
            not isinstance(pass_id, str)
            or not _valid_pass_id(pass_id)
            or not isinstance(task_ref, str)
            or not task_ref
            or not isinstance(lane_id, str)
            or not lane_id
        ):
            return _read_ledger(path)
        pair = (task_ref, lane_id)
        previous = entries.get(pass_id)
        if previous is not None and previous != pair:
            return _read_ledger(path)
        entries[pass_id] = pair
        publications[pass_id] = (device, inode, snapshot.rows + row_offset)

    return _LedgerSnapshot(
        entries,
        snapshot.needs_rebuild,
        set(snapshot.doubts),
        snapshot.rewrite_blocked,
        snapshot.unreadable,
        publications,
        (device, inode, offset + len(tail)),
        snapshot.rows + len(tail_rows),
    )


def _parse_ledger_line(raw_line: bytes) -> dict[str, str]:
    """Decode membership or doubt JSON strings without invoking payload ``json.loads``."""

    text = raw_line.decode("utf-8")
    position = 0

    def skip_space() -> None:
        nonlocal position
        while position < len(text) and text[position] in " \t\r\n":
            position += 1

    def parse_string() -> str:
        nonlocal position
        skip_space()
        if position >= len(text) or text[position] != '"':
            raise ValueError("expected JSON string")
        value, position_after = scanstring(text, position + 1, True)
        position = position_after
        return value

    skip_space()
    if position >= len(text) or text[position] != "{":
        raise ValueError("expected JSON object")
    position += 1
    row: dict[str, str] = {}
    while True:
        key = parse_string()
        skip_space()
        if position >= len(text) or text[position] != ":":
            raise ValueError("expected JSON key separator")
        position += 1
        value = parse_string()
        if key not in {"p", "t", "l", "d"} or key in row:
            raise ValueError("unexpected or duplicate ledger key")
        row[key] = value
        skip_space()
        if position >= len(text):
            raise ValueError("unterminated JSON object")
        separator = text[position]
        position += 1
        if separator == "}":
            break
        if separator != ",":
            raise ValueError("expected JSON member separator")
    skip_space()
    if position != len(text) or set(row) not in ({"p", "t", "l"}, {"p", "t", "l", "d"}):
        raise ValueError("invalid ledger object")
    return row


def _truncate_torn_suffix(descriptor: int) -> None:
    """Discard an incomplete final line before a later append joins it."""

    end = os.lseek(descriptor, 0, os.SEEK_END)
    if end == 0:
        return
    os.lseek(descriptor, end - 1, os.SEEK_SET)
    if os.read(descriptor, 1) == b"\n":
        return

    cursor = end
    while cursor:
        start = max(0, cursor - 4096)
        os.lseek(descriptor, start, os.SEEK_SET)
        chunk = os.read(descriptor, cursor - start)
        newline = chunk.rfind(b"\n")
        if newline >= 0:
            os.ftruncate(descriptor, start + newline + 1)
            return
        cursor = start
    os.ftruncate(descriptor, 0)


def _append_ledger_entry_locked(path: Path, *, task_ref: str, lane_id: str, pass_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_APPEND, 0o600)
    try:
        _truncate_torn_suffix(descriptor)
        line = (
            json.dumps(
                {"p": pass_id, "t": task_ref, "l": lane_id},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        written = os.write(descriptor, line)
        if written != len(line):
            raise OSError("short write while appending offload pass ledger")
    finally:
        os.close(descriptor)


def _replace_ledger_locked(
    path: Path,
    entries: dict[str, tuple[str, str]],
    doubts: Iterable[tuple[str, str, str, str]] = (),
) -> None:
    """Write a fresh ledger and atomically cut it over while the dir is locked."""

    body = b"".join(
        json.dumps(
            {"p": pass_id, "t": task_ref, "l": lane_id},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for pass_id, (task_ref, lane_id) in sorted(entries.items())
    )
    body += b"".join(
        json.dumps(
            {"p": pass_id, "t": task_ref, "l": lane_id, "d": reason},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for pass_id, task_ref, lane_id, reason in sorted(set(doubts))
    )
    descriptor, temp_name = tempfile.mkstemp(prefix=f"{path.name}.tmp-", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as temp_file:
            temp_file.write(body)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def record_pass_id(
    state_dir: Path,
    *,
    task_ref: str,
    lane_id: str,
    pass_id: str,
    index_lock_timeout_seconds: float | None = None,
) -> None:
    """Append one pass mapping; duplicate rows are harmless and deduped on read.

    The payload must already be atomically published before this function is
    called. A duplicate append is safe because ledger reads deduplicate by id.
    """

    if not _valid_pass_id(pass_id):
        raise ValueError("pass_id must be a non-empty filename-safe single line")
    if not isinstance(task_ref, str) or not task_ref or not isinstance(lane_id, str) or not lane_id:
        raise ValueError("task_ref and lane_id must be non-empty strings")
    timeout = (
        _DEFAULT_INDEX_LOCK_TIMEOUT_SECONDS
        if index_lock_timeout_seconds is None
        else _resolve_index_lock_timeout(index_lock_timeout_seconds)
    )
    with _index_lock(state_dir, timeout=timeout) as index_dir:
        _remove_legacy_indexes(index_dir)
        _append_ledger_entry_locked(
            _ledger_path(state_dir),
            task_ref=task_ref,
            lane_id=lane_id,
            pass_id=pass_id,
        )


def _heal_pass_ids(
    state_dir: Path, *, entries: Mapping[str, tuple[str, str]], index_lock_timeout_seconds: float
) -> None:
    """Validate a source batch against one locked snapshot within the reader budget."""
    if not entries:
        return
    deadline = time.monotonic() + index_lock_timeout_seconds
    with _index_lock(state_dir, timeout=index_lock_timeout_seconds):
        path = _ledger_path(state_dir)
        latest = _read_ledger(path)
        if latest.rewrite_blocked:
            return
        doubts = set(latest.doubts)
        for pass_id, pair in entries.items():
            previous = latest.entries.get(pass_id)
            if previous is not None and previous != pair:
                doubts.add((pass_id, *previous, "ledger_source_contradiction"))
                doubts.add((pass_id, *pair, "ledger_source_contradiction"))
        # Persist conflicts before optional healing; never replace a newer
        # membership with the source observation collected outside this lock.
        if doubts != latest.doubts:
            _replace_ledger_locked(path, latest.entries, doubts)
        for pass_id, (task_ref, lane_id) in entries.items():
            if time.monotonic() >= deadline:
                raise IndexLockTimeout("index_lock_timeout during ledger healing")
            if not _valid_pass_id(pass_id):
                raise ValueError("pass_id must be a non-empty filename-safe single line")
            if pass_id not in latest.entries:
                _append_ledger_entry_locked(path, task_ref=task_ref, lane_id=lane_id, pass_id=pass_id)


def _parse_record(path: Path) -> tuple[dict[str, object], os.stat_result] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = path.stat()
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload, metadata


def _stat_pass_files(state_dir: Path) -> tuple[list[tuple[Path, os.stat_result]], bool]:
    result: list[tuple[Path, os.stat_result]] = []
    try:
        paths = Path(state_dir).glob(f"{_PASS_PREFIX}*{_PASS_SUFFIX}")
        for path in paths:
            try:
                metadata = path.stat()
            except OSError:
                return result, True
            if stat.S_ISREG(metadata.st_mode):
                result.append((path, metadata))
    except OSError:
        return result, True
    return result, False


def _rebuild_from_records(
    file_entries: list[tuple[Path, os.stat_result]],
    *,
    previous_entries: dict[str, tuple[str, str]],
    doubts: set[tuple[str, str, str, str]],
    task_ref: str,
    lane_id: str,
) -> tuple[
    dict[str, tuple[str, str]],
    dict[str, LanePassRecord],
    set[str],
    Literal["ok", "unknown"],
    set[str],
]:
    entries: dict[str, tuple[str, str]] = {}
    records: dict[str, LanePassRecord] = {}
    scanned_ids: set[str] = set()
    doubtful_pass_ids: set[str] = set()
    status: Literal["ok", "unknown"] = "ok"
    target = (task_ref, lane_id)

    for path, _listed_metadata in file_entries:
        pass_id = path.name[len(_PASS_PREFIX) : -len(_PASS_SUFFIX)]
        scanned_ids.add(pass_id)
        previous = previous_entries.get(pass_id)
        parsed = _parse_record(path)
        if parsed is None:
            if previous is None or previous == target:
                status = "unknown"
                doubtful_pass_ids.add(pass_id)
            if previous is not None:
                # Preserve known lane identity so another lane's unreadable
                # source record does not poison this lane on its next lookup.
                entries[pass_id] = previous
            continue

        payload, metadata = parsed
        record_task = payload.get("task_ref")
        record_lane = payload.get("lane_id")
        if not isinstance(record_task, str) or not record_task or not isinstance(record_lane, str) or not record_lane:
            if previous is None or previous == target:
                status = "unknown"
                doubtful_pass_ids.add(pass_id)
            if previous is not None:
                entries[pass_id] = previous
            continue

        entry = (record_task, record_lane)
        if previous is not None and previous != entry:
            doubts.add((pass_id, *previous, "ledger_source_contradiction"))
        entries[pass_id] = entry
        if entry == target:
            records[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

    for pass_id, previous in previous_entries.items():
        if pass_id in scanned_ids:
            continue
        entries[pass_id] = previous
        if previous == target:
            status = "unknown"
            doubtful_pass_ids.add(pass_id)

    return entries, records, scanned_ids, status, doubtful_pass_ids


def lane_pass_records(
    state_dir: Path, *, task_ref: str, lane_id: str, index_lock_timeout_seconds: float | None = None
) -> LanePassRecords:
    """Return matching passes using ledger membership and source set difference.

    The ledger is read once as cheap JSONL lines. Indexed lane records are the
    only JSON payloads parsed on a warm lookup. Any record filename absent
    from the ledger is parsed regardless of mtime and appended to heal the
    ledger. A missing or corrupt ledger is rebuilt from the pass records.
    """

    timeout = _resolve_index_lock_timeout(index_lock_timeout_seconds)
    deadline = time.monotonic() + timeout
    state_dir = Path(state_dir)
    ledger_path = _ledger_path(state_dir)
    target = (task_ref, lane_id)
    status: Literal["ok", "unknown"] = "ok"
    records_by_id: dict[str, LanePassRecord] = {}
    scanned_ids: set[str] = set()
    doubtful_pass_ids: set[str] = set()
    sources_complete = True
    doubt_reasons: set[str] = set()
    listing: list[tuple[Path, os.stat_result]] | None = None
    append_ledger = True
    source_scan_fallback = False
    snapshot: _LedgerSnapshot | None = None

    try:
        with _index_lock(state_dir, timeout=timeout) as index_dir:
            _remove_legacy_indexes(index_dir)
            snapshot = _read_ledger(ledger_path)
            indexed_entries = snapshot.entries
            doubts = set(snapshot.doubts)
            if snapshot.needs_rebuild:
                listing, listing_unknown = _stat_pass_files(state_dir)
                if listing_unknown:
                    status = "unknown"
                    sources_complete = False
                else:
                    indexed_entries, records_by_id, scanned_ids, status, doubtful_pass_ids = _rebuild_from_records(
                        listing,
                        previous_entries=snapshot.entries,
                        doubts=doubts,
                        task_ref=task_ref,
                        lane_id=lane_id,
                    )
                    sources_complete = status == "ok"
                    try:
                        if not snapshot.rewrite_blocked:
                            _replace_ledger_locked(ledger_path, indexed_entries, doubts)
                    except OSError as exc:
                        status = "unknown"
                        logger.warning("could not rebuild offload pass ledger: %s", exc)
            if snapshot.rewrite_blocked:
                append_ledger = False
                status = "unknown"
                sources_complete = False
                doubt_reasons.add("ledger_unreadable" if snapshot.unreadable else "doubt_row_malformed")
            for pass_id, doubt_task, doubt_lane, reason in doubts:
                if (doubt_task, doubt_lane) == target:
                    status = "unknown"
                    sources_complete = False
                    doubtful_pass_ids.add(pass_id)
                    doubt_reasons.add(reason)
    except OSError as exc:
        # The index is a derived optimization.  If its directory or lock cannot
        # be opened, fall through to the source-file scan below and preserve
        # the fact that the index itself was unavailable.
        logger.warning("could not open offload pass index for %s: %s", state_dir, exc)
        status = "unknown"
        source_scan_fallback = True
        records_by_id = {}
        scanned_ids = set()
        doubtful_pass_ids = set()
        if isinstance(exc, IndexLockTimeout):
            doubt_reasons.add("index_lock_timeout")
        append_ledger = False
        listing = None
        # The ledger is an optimization, so a lock timeout does not stop us
        # from reading its atomic replacement. This preserves durable doubt
        # rows and known source identities while the record files remain the
        # authority for the scan below.
        snapshot = _read_ledger(ledger_path)
        indexed_entries = snapshot.entries
        doubts = set(snapshot.doubts)
        if snapshot.rewrite_blocked:
            sources_complete = False
            doubt_reasons.add("ledger_unreadable" if snapshot.unreadable else "doubt_row_malformed")
        for pass_id, doubt_task, doubt_lane, reason in doubts:
            if (doubt_task, doubt_lane) == target:
                status = "unknown"
                sources_complete = False
                doubtful_pass_ids.add(pass_id)
                doubt_reasons.add(reason)

    if listing is None:
        listing, listing_unknown = _stat_pass_files(state_dir)
        if listing_unknown:
            status = "unknown"
            sources_complete = False

    if source_scan_fallback:
        # A timed-out or unavailable index cannot tell us which files are
        # unindexed, so parse the complete listing and compare it with the
        # last readable ledger snapshot. The scan itself remains authoritative.
        _, records_by_id, scanned_ids, source_status, source_doubts = _rebuild_from_records(
            listing,
            previous_entries=indexed_entries,
            doubts=doubts,
            task_ref=task_ref,
            lane_id=lane_id,
        )
        if source_status == "unknown":
            status = "unknown"
            sources_complete = False
        doubtful_pass_ids.update(source_doubts)
        for pass_id, doubt_task, doubt_lane, reason in doubts:
            if (doubt_task, doubt_lane) == target:
                status = "unknown"
                sources_complete = False
                doubtful_pass_ids.add(pass_id)
                doubt_reasons.add(reason)
        # The full source scan already handled every listed file and every
        # prior target identity that is absent from the listing.
        indexed_entries = {}

    for pass_id, entry in indexed_entries.items():
        if entry != target or pass_id in records_by_id or pass_id in scanned_ids:
            continue
        record_path = _ledger_record_path(state_dir, pass_id)
        try:
            metadata = record_path.stat()
        except FileNotFoundError:
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        except OSError:
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        if not stat.S_ISREG(metadata.st_mode):
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        if not isinstance(payload, dict):
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        record_task = payload.get("task_ref")
        record_lane = payload.get("lane_id")
        if not isinstance(record_task, str) or not isinstance(record_lane, str):
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        if (record_task, record_lane) == target:
            records_by_id[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)
        else:
            # The ledger's lane identity and source payload contradict each
            # other; neither side is enough to establish this lane's history.
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            doubt_reasons.add("ledger_source_contradiction")
            doubts.add((pass_id, *target, "ledger_source_contradiction"))

    pending_entries: dict[str, tuple[str, str]] = {}
    indexed_ids = set(indexed_entries)
    for path, _listed_metadata in listing:
        pass_id = path.name[len(_PASS_PREFIX) : -len(_PASS_SUFFIX)]
        if pass_id in indexed_ids or pass_id in scanned_ids:
            continue
        parsed = _parse_record(path)
        if parsed is None:
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        payload, metadata = parsed
        record_task = payload.get("task_ref")
        record_lane = payload.get("lane_id")
        if not isinstance(record_task, str) or not record_task or not isinstance(record_lane, str) or not record_lane:
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            continue
        if append_ledger:
            pending_entries[pass_id] = (record_task, record_lane)
        if (record_task, record_lane) == target:
            records_by_id[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

    if pending_entries:
        try:
            _heal_pass_ids(
                state_dir,
                entries=pending_entries,
                index_lock_timeout_seconds=max(0.0, deadline - time.monotonic()),
            )
        except IndexLockTimeout:
            status = "unknown"
            doubt_reasons.add("index_lock_timeout")
        except (OSError, UnicodeError, ValueError) as exc:
            logger.warning("could not heal offload pass ledger: %s", exc)

    # Reconcile on success as well as timeout. Replacements are atomic and
    # _read_ledger ignores a torn final append, so this read needs no lock.
    latest = _read_ledger(ledger_path)
    observations = dict(indexed_entries)
    observations.update({pass_id: target for pass_id in records_by_id})
    doubts.update(
        (pass_id, *previous, "ledger_source_contradiction")
        for pass_id, pair in latest.entries.items()
        if (previous := observations.get(pass_id)) is not None and previous != pair
    )
    if doubts - latest.doubts:
        try:
            # Even after an opening timeout, try the fence with the remaining
            # budget (zero means immediately available). Save source conflicts
            # as well as concurrent membership conflicts for future readers.
            with _index_lock(state_dir, timeout=max(0.0, deadline - time.monotonic())):
                latest = _read_ledger(ledger_path)
                doubts.update(
                    (pass_id, *previous, "ledger_source_contradiction")
                    for pass_id, pair in latest.entries.items()
                    if (previous := observations.get(pass_id)) is not None and previous != pair
                )
                if latest.rewrite_blocked:
                    logger.warning("could not preserve offload pass ledger doubts: ledger rewrite blocked")
                else:
                    _replace_ledger_locked(ledger_path, latest.entries, latest.doubts | doubts)
                    latest = _read_ledger(ledger_path)
        except OSError as exc:
            status = "unknown"
            if isinstance(exc, IndexLockTimeout):
                doubt_reasons.add("index_lock_timeout")
            logger.warning("could not preserve offload pass ledger doubts: %s", exc)
    if latest.rewrite_blocked:
        status = "unknown"
        sources_complete = False
        doubt_reasons.add("ledger_unreadable" if latest.unreadable else "doubt_row_malformed")
    for pass_id, doubt_task, doubt_lane, reason in latest.doubts | doubts:
        if (doubt_task, doubt_lane) == target:
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            doubt_reasons.add(reason)
    for pass_id, entry in latest.entries.items():
        previous = indexed_entries.get(pass_id)
        if (previous == target and entry != target) or (pass_id in records_by_id and entry != target):
            records_by_id.pop(pass_id, None)
            status = "unknown"
            sources_complete = False
            doubtful_pass_ids.add(pass_id)
            doubt_reasons.add("ledger_source_contradiction")
        if entry == target and pass_id not in records_by_id and pass_id not in doubtful_pass_ids:
            parsed = _parse_record(_ledger_record_path(state_dir, pass_id))
            if parsed is None or (parsed[0].get("task_ref"), parsed[0].get("lane_id")) != target:
                status = "unknown"
                sources_complete = False
                doubtful_pass_ids.add(pass_id)
            else:
                payload, metadata = parsed
                records_by_id[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

    records = tuple(sorted(records_by_id.values(), key=lambda record: (record.mtime, record.pass_id)))
    return LanePassRecords(
        records, status, tuple(sorted(doubtful_pass_ids)), sources_complete, tuple(sorted(doubt_reasons))
    )


def _ledger_record_path(state_dir: Path, pass_id: str) -> Path:
    return Path(state_dir) / f"{_PASS_PREFIX}{pass_id}{_PASS_SUFFIX}"


@dataclass
class _LanePassRecordsBuilder:
    records: dict[str, LanePassRecord]
    status: Literal["ok", "unknown"] = "ok"
    doubtful_pass_ids: set[str] = field(default_factory=set)
    sources_complete: bool = True
    doubt_reasons: set[str] = field(default_factory=set)


class TaskLanePassRecordsReader:
    """Read several lane histories from one ledger snapshot and source listing.

    The reader owns its cache, so callers can share one instance for a bounded
    operation such as a retire sweep without retaining stale state process-wide.
    """

    def __init__(
        self,
        state_dir: Path,
        *,
        lanes_by_task: Mapping[str, Iterable[str] | None],
        index_lock_timeout_seconds: float | None = None,
    ) -> None:
        self.state_dir = Path(state_dir)
        self._index_lock_timeout_seconds = _resolve_index_lock_timeout(index_lock_timeout_seconds)
        self._all_lanes_for = {task for task, lanes in lanes_by_task.items() if lanes is None}
        self._requested_lanes = {task: None if lanes is None else set(lanes) for task, lanes in lanes_by_task.items()}
        self._builders: dict[tuple[str, str], _LanePassRecordsBuilder] = {}
        self._global_source_doubts: set[str] = set()
        self._loaded = False
        self._load_error: Exception | None = None
        self._ledger_compaction_error: Exception | None = None
        for task, lanes in self._requested_lanes.items():
            if lanes is not None:
                for lane in lanes:
                    self._ensure_pair(task, lane)

    def lane_records(self, task_ref: str, lane_id: str) -> LanePassRecords:
        self._ensure_loaded()
        builder = self._builders.get((task_ref, lane_id))
        if builder is None:
            return LanePassRecords((), "ok")
        return self._freeze(builder)

    def task_lane_pass_records(self, task_ref: str) -> dict[str, LanePassRecords]:
        self._ensure_loaded()
        return {
            lane_id: self._freeze(builder)
            for (record_task, lane_id), builder in self._builders.items()
            if record_task == task_ref
        }

    def _ensure_pair(self, task_ref: str, lane_id: str) -> _LanePassRecordsBuilder:
        return self._builders.setdefault((task_ref, lane_id), _LanePassRecordsBuilder({}))

    def _is_target(self, task_ref: str, lane_id: str) -> bool:
        if (task_ref, lane_id) in self._builders:
            return True
        if task_ref in self._all_lanes_for:
            self._ensure_pair(task_ref, lane_id)
            return True
        return False

    def _mark_unknown(
        self,
        pair: tuple[str, str],
        *,
        pass_id: str | None = None,
        source_incomplete: bool = False,
        doubt_reason: str | None = None,
    ) -> None:
        builder = self._ensure_pair(*pair)
        builder.status = "unknown"
        if source_incomplete:
            builder.sources_complete = False
        if pass_id is not None:
            builder.doubtful_pass_ids.add(pass_id)
        if doubt_reason is not None:
            builder.doubt_reasons.add(doubt_reason)

    def _mark_all_unknown(
        self,
        *,
        source_incomplete: bool = False,
        doubt_reason: str | None = None,
    ) -> None:
        for pair in tuple(self._builders):
            self._mark_unknown(
                pair,
                source_incomplete=source_incomplete,
                doubt_reason=doubt_reason,
            )

    def _ensure_loaded(self) -> None:
        if self._loaded:
            if self._load_error is not None:
                raise self._load_error
            return
        self._loaded = True
        try:
            self._load()
        except Exception as exc:  # noqa: BLE001 — callers treat an unreadable index as unknown
            self._load_error = exc
            raise

    def _load(self) -> None:
        self._index_lock_deadline = time.monotonic() + self._index_lock_timeout_seconds
        ledger_path = _ledger_path(self.state_dir)
        listing: list[tuple[Path, os.stat_result]]
        listing_unknown = False
        indexed_entries: dict[str, tuple[str, str]] = {}
        index_lock_available = False
        lock_timed_out = False
        rebuilt = False
        global_doubt_reasons: set[str] = set()
        snapshot: _LedgerSnapshot | None = None

        try:
            _remove_legacy_indexes(self.state_dir / _INDEX_DIRNAME)
            with _index_lock(self.state_dir, timeout=self._index_lock_timeout_seconds):
                index_lock_available = True
                snapshot = _read_ledger(ledger_path)
                if (
                    snapshot.rows >= 4096
                    and snapshot.rows > 2 * len(snapshot.entries)
                    and not snapshot.rewrite_blocked
                    and not snapshot.unreadable
                ):
                    uncompacted_snapshot = snapshot
                    try:
                        _replace_ledger_locked(ledger_path, snapshot.entries, snapshot.doubts)
                        compacted_snapshot = _read_ledger(ledger_path)
                        if compacted_snapshot.unreadable:
                            raise OSError("compacted offload pass ledger could not be read back")
                        snapshot = replace(compacted_snapshot, needs_rebuild=uncompacted_snapshot.needs_rebuild)
                    except Exception as exc:  # noqa: BLE001 — continue with the known-good locked snapshot
                        self._ledger_compaction_error = exc
                        snapshot = uncompacted_snapshot
                        logger.warning("could not compact offload pass ledger for %s: %s", ledger_path, exc)
        except OSError as exc:
            # The derived index can be unavailable while source records remain
            # useful. Fall back to one source listing and keep the uncertainty.
            logger.warning("could not open offload pass index for %s: %s", self.state_dir, exc)
            self._builders = {}
            self._global_source_doubts.clear()
            self._seed_explicit_lanes()
            lock_timed_out = isinstance(exc, IndexLockTimeout)
            if lock_timed_out:
                global_doubt_reasons.add("index_lock_timeout")
            # A lock timeout makes the index uncertain, not the source scan.
            # Read the atomically replaced ledger without the lock to retain
            # its target-lane doubts and known pass identities.
            snapshot = _read_ledger(ledger_path)
            index_lock_available = False

        # _index_lock protects ledger reads and writes only. Filesystem
        # listing and pass JSON parsing happen after its context has exited.
        assert snapshot is not None
        indexed_entries = snapshot.entries
        self._seed_all_task_lanes(indexed_entries)
        doubts = set(snapshot.doubts)
        self._apply_doubts(doubts)
        if snapshot.rewrite_blocked:
            global_doubt_reasons.add("ledger_unreadable" if snapshot.unreadable else "doubt_row_malformed")

        listing, listing_unknown = _stat_pass_files(self.state_dir)

        if not index_lock_available:
            # With no usable locked index snapshot, scan every source record
            # and compare it with the last readable ledger identities.
            indexed_entries = self._rebuild_from_listing(listing, snapshot.entries, doubts)
            self._apply_doubts(doubts)
            rebuilt = True
            if not lock_timed_out:
                # Preserve the existing recovery path for a non-timeout open
                # failure: a readable source record may still heal the ledger.
                healing_timed_out = self._read_unindexed_records(
                    listing,
                    indexed_ids=set(),
                    append_ledger=not snapshot.rewrite_blocked,
                )
                if healing_timed_out:
                    global_doubt_reasons.add("index_lock_timeout")
            self._reconcile_ledger(_read_ledger(ledger_path), indexed_entries, global_doubt_reasons)
        elif snapshot.needs_rebuild and not listing_unknown:
            # Rebuild from source outside the lock. Reacquire it only to read
            # the latest append-only rows and atomically replace the ledger.
            indexed_entries = self._rebuild_from_listing(listing, snapshot.entries, doubts)
            self._apply_doubts(doubts)
            rebuilt = True
            if not snapshot.rewrite_blocked:
                remaining = max(0.0, self._index_lock_deadline - time.monotonic())
                concurrent_entries: dict[str, tuple[str, str]] = {}
                try:
                    with _index_lock(self.state_dir, timeout=remaining):
                        latest = _read_ledger(ledger_path)
                        self._apply_doubts(latest.doubts)
                        concurrent_entries = {
                            pass_id: pair
                            for pass_id, pair in latest.entries.items()
                            if snapshot.publications.get(pass_id) != latest.publications.get(pass_id)
                        }
                        self._seed_all_task_lanes(concurrent_entries)
                        if latest.rewrite_blocked:
                            reason = "ledger_unreadable" if latest.unreadable else "doubt_row_malformed"
                            global_doubt_reasons.add(reason)
                            self._mark_all_unknown(source_incomplete=True, doubt_reason=reason)
                        else:
                            latest_entries = dict(indexed_entries)
                            latest_entries.update(concurrent_entries)
                            latest_doubts = set(doubts) | latest.doubts
                            for pass_id, pair in latest.entries.items():
                                rebuilt_pair = indexed_entries.get(pass_id)
                                if rebuilt_pair is not None and rebuilt_pair != pair:
                                    latest_doubts.add((pass_id, *pair, "ledger_source_contradiction"))
                                    if snapshot.publications.get(pass_id) != latest.publications.get(pass_id):
                                        # This row was published after our initial
                                        # snapshot. The out-of-lock scan cannot replace it.
                                        latest_entries[pass_id] = pair
                                        latest_doubts.add((pass_id, *rebuilt_pair, "ledger_source_contradiction"))
                            _replace_ledger_locked(ledger_path, latest_entries, latest_doubts)
                            indexed_entries = latest_entries
                            doubts = latest_doubts
                            self._apply_doubts(latest_doubts)
                except IndexLockTimeout as exc:
                    global_doubt_reasons.add("index_lock_timeout")
                    self._mark_all_unknown(doubt_reason="index_lock_timeout")
                    logger.warning("could not rebuild offload pass ledger: %s", exc)
                    latest = _read_ledger(ledger_path)
                    self._apply_doubts(latest.doubts)
                    if latest.rewrite_blocked:
                        reason = "ledger_unreadable" if latest.unreadable else "doubt_row_malformed"
                        global_doubt_reasons.add(reason)
                    concurrent_entries = {
                        pass_id: pair
                        for pass_id, pair in latest.entries.items()
                        if snapshot.publications.get(pass_id) != latest.publications.get(pass_id)
                    }
                    self._seed_all_task_lanes(concurrent_entries)
                except OSError as exc:
                    self._mark_all_unknown()
                    logger.warning("could not rebuild offload pass ledger: %s", exc)
                self._invalidate_conflicting_records(concurrent_entries)
                self._read_indexed_records(concurrent_entries)

        if not rebuilt:
            if listing_unknown:
                self._mark_all_unknown(source_incomplete=True)
            self._read_indexed_records(indexed_entries)
            healing_timed_out = self._read_unindexed_records(
                listing,
                indexed_ids=set(indexed_entries),
                append_ledger=index_lock_available and not snapshot.rewrite_blocked,
            )
            if healing_timed_out:
                global_doubt_reasons.add("index_lock_timeout")
            self._reconcile_ledger(_read_ledger_tail(ledger_path, snapshot), indexed_entries, global_doubt_reasons)

        if listing_unknown:
            # A partial listing can discover new lane builders during the
            # source pass, so mark every builder after source discovery ends.
            self._mark_all_unknown(source_incomplete=True)
        if not index_lock_available and "index_lock_timeout" not in global_doubt_reasons:
            self._mark_all_unknown()
        for reason in global_doubt_reasons:
            self._mark_all_unknown(
                source_incomplete=reason != "index_lock_timeout",
                doubt_reason=reason,
            )
        for pass_id in self._global_source_doubts:
            for pair in tuple(self._builders):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)

    def _seed_explicit_lanes(self) -> None:
        # The set was cleared by the index fallback. Recreate the explicitly
        # requested targets from the immutable fallback request list.
        for task_ref, lanes in self._requested_lanes.items():
            if lanes is not None:
                for lane_id in lanes:
                    self._ensure_pair(task_ref, lane_id)

    def _seed_all_task_lanes(self, entries: Mapping[str, tuple[str, str]]) -> None:
        for task_ref, lane_id in entries.values():
            if task_ref in self._all_lanes_for:
                self._ensure_pair(task_ref, lane_id)

    def _apply_doubts(self, doubts: Iterable[tuple[str, str, str, str]]) -> None:
        for pass_id, task_ref, lane_id, reason in doubts:
            pair = (task_ref, lane_id)
            if self._is_target(*pair):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True, doubt_reason=reason)

    def _rebuild_from_listing(
        self,
        listing: list[tuple[Path, os.stat_result]],
        previous_entries: Mapping[str, tuple[str, str]],
        doubts: set[tuple[str, str, str, str]],
    ) -> dict[str, tuple[str, str]]:
        entries: dict[str, tuple[str, str]] = {}
        scanned_ids: set[str] = set()
        for path, _listed_metadata in listing:
            pass_id = path.name[len(_PASS_PREFIX) : -len(_PASS_SUFFIX)]
            scanned_ids.add(pass_id)
            previous = previous_entries.get(pass_id)
            parsed = _parse_record(path)
            if parsed is None:
                if previous is None:
                    self._global_source_doubts.add(pass_id)
                elif self._is_target(*previous):
                    self._mark_unknown(previous, pass_id=pass_id, source_incomplete=True)
                if previous is not None:
                    entries[pass_id] = previous
                continue

            payload, metadata = parsed
            record_task = payload.get("task_ref")
            record_lane = payload.get("lane_id")
            if (
                not isinstance(record_task, str)
                or not record_task
                or not isinstance(record_lane, str)
                or not record_lane
            ):
                if previous is None:
                    self._global_source_doubts.add(pass_id)
                elif self._is_target(*previous):
                    self._mark_unknown(previous, pass_id=pass_id, source_incomplete=True)
                if previous is not None:
                    entries[pass_id] = previous
                continue

            pair = (record_task, record_lane)
            if previous is not None and previous != pair:
                doubts.add((pass_id, *previous, "ledger_source_contradiction"))
            entries[pass_id] = pair
            if self._is_target(*pair):
                self._ensure_pair(*pair).records[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

        for pass_id, previous in previous_entries.items():
            if pass_id in scanned_ids:
                continue
            entries[pass_id] = previous
            if self._is_target(*previous):
                self._mark_unknown(previous, pass_id=pass_id, source_incomplete=True)
        return entries

    def _reconcile_ledger(
        self, latest: _LedgerSnapshot, observed_entries: Mapping[str, tuple[str, str]], global_doubt_reasons: set[str]
    ) -> None:
        self._apply_doubts(latest.doubts)
        if latest.rewrite_blocked:
            global_doubt_reasons.add("ledger_unreadable" if latest.unreadable else "doubt_row_malformed")
        observations = dict(observed_entries)
        for pair, builder in self._builders.items():
            for pass_id in builder.records:
                observations.setdefault(pass_id, pair)
        conflicts = {
            (pass_id, *previous, "ledger_source_contradiction")
            for pass_id, pair in latest.entries.items()
            if (previous := observations.get(pass_id)) is not None and previous != pair
        }
        if conflicts:
            # Recovery can parse a newer source on its second pass. Preserve
            # the earlier conflict too, so fresh readers retain that evidence.
            self._apply_doubts(conflicts)
            try:
                remaining = max(0.0, self._index_lock_deadline - time.monotonic())
                with _index_lock(self.state_dir, timeout=remaining):
                    latest = _read_ledger(_ledger_path(self.state_dir))
                    if not latest.rewrite_blocked:
                        _replace_ledger_locked(_ledger_path(self.state_dir), latest.entries, latest.doubts | conflicts)
            except OSError as exc:
                if isinstance(exc, IndexLockTimeout):
                    global_doubt_reasons.add("index_lock_timeout")
                else:
                    self._mark_all_unknown()
                logger.warning("could not preserve offload pass ledger doubts: %s", exc)
            self._apply_doubts(latest.doubts)
            if latest.rewrite_blocked:
                global_doubt_reasons.add("ledger_unreadable" if latest.unreadable else "doubt_row_malformed")
        concurrent_entries = {
            pass_id: pair for pass_id, pair in latest.entries.items() if observations.get(pass_id) != pair
        }
        self._seed_all_task_lanes(concurrent_entries)
        for pass_id, pair in concurrent_entries.items():
            previous = observations.get(pass_id)
            if previous is not None:
                self._apply_doubts(
                    {
                        (pass_id, *previous, "ledger_source_contradiction"),
                        (pass_id, *pair, "ledger_source_contradiction"),
                    }
                )
        self._invalidate_conflicting_records(concurrent_entries)
        self._read_indexed_records(concurrent_entries)

    def _invalidate_conflicting_records(self, entries: Mapping[str, tuple[str, str]]) -> None:
        # A cached source observation predates these ledger identities. Drop it
        # before reading the current source so it cannot appear in both lanes.
        for pair, builder in tuple(self._builders.items()):
            for pass_id in tuple(builder.records):
                latest_pair = entries.get(pass_id)
                if latest_pair is None or latest_pair == pair:
                    continue
                del builder.records[pass_id]
                self._mark_unknown(
                    pair, pass_id=pass_id, source_incomplete=True, doubt_reason="ledger_source_contradiction"
                )
                if self._is_target(*latest_pair):
                    self._mark_unknown(
                        latest_pair, pass_id=pass_id, source_incomplete=True, doubt_reason="ledger_source_contradiction"
                    )

    def _read_indexed_records(self, indexed_entries: Mapping[str, tuple[str, str]]) -> None:
        for pass_id, pair in indexed_entries.items():
            if not self._is_target(*pair):
                continue
            # A completed publication can update the payload without changing
            # membership. Never retain the earlier cached payload if its
            # requested refresh fails to read the current source.
            self._ensure_pair(*pair).records.pop(pass_id, None)
            record_path = _ledger_record_path(self.state_dir, pass_id)
            try:
                metadata = record_path.stat()
            except OSError:
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            try:
                payload = json.loads(record_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            if not isinstance(payload, dict):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            record_task = payload.get("task_ref")
            record_lane = payload.get("lane_id")
            if not isinstance(record_task, str) or not isinstance(record_lane, str):
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            if (record_task, record_lane) != pair:
                # The ledger points this payload at this lane, so only this
                # lane inherits the contradiction as source doubt.
                self._mark_unknown(pair, pass_id=pass_id, source_incomplete=True)
                continue
            self._ensure_pair(*pair).records[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

    def _read_unindexed_records(
        self,
        listing: list[tuple[Path, os.stat_result]],
        *,
        indexed_ids: set[str],
        append_ledger: bool = True,
    ) -> bool:
        pending_entries: dict[str, tuple[str, str]] = {}
        for path, _listed_metadata in listing:
            pass_id = path.name[len(_PASS_PREFIX) : -len(_PASS_SUFFIX)]
            if pass_id in indexed_ids:
                continue
            parsed = _parse_record(path)
            if parsed is None:
                self._global_source_doubts.add(pass_id)
                continue
            payload, metadata = parsed
            record_task = payload.get("task_ref")
            record_lane = payload.get("lane_id")
            if (
                not isinstance(record_task, str)
                or not record_task
                or not isinstance(record_lane, str)
                or not record_lane
            ):
                self._global_source_doubts.add(pass_id)
                continue
            pair = (record_task, record_lane)
            if append_ledger:
                pending_entries[pass_id] = pair
            if self._is_target(*pair):
                self._ensure_pair(*pair).records[pass_id] = LanePassRecord(pass_id, payload, metadata.st_mtime)

        if pending_entries:
            try:
                _heal_pass_ids(
                    self.state_dir,
                    entries=pending_entries,
                    index_lock_timeout_seconds=max(0.0, self._index_lock_deadline - time.monotonic()),
                )
            except IndexLockTimeout:
                return True
            except (OSError, UnicodeError, ValueError) as exc:
                logger.warning("could not heal offload pass ledger: %s", exc)
        return False

    @staticmethod
    def _freeze(builder: _LanePassRecordsBuilder) -> LanePassRecords:
        records = tuple(sorted(builder.records.values(), key=lambda record: (record.mtime, record.pass_id)))
        doubtful = tuple(sorted(builder.doubtful_pass_ids))
        reasons = tuple(sorted(builder.doubt_reasons))
        return LanePassRecords(records, builder.status, doubtful, builder.sources_complete, reasons)


def task_lane_pass_records(
    state_dir: Path,
    *,
    task_ref: str,
    lane_ids: Iterable[str] | None = None,
) -> dict[str, LanePassRecords]:
    """Return all matching lanes from one pass-file listing and ledger read."""

    reader = TaskLanePassRecordsReader(state_dir, lanes_by_task={task_ref: lane_ids})
    return reader.task_lane_pass_records(task_ref)
