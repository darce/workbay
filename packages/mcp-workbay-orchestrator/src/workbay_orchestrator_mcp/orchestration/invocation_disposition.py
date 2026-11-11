"""Administrative UNKNOWN closure, independent of execution and admission.

Public CLI: ``python -m ...orchestration.invocation_disposition`` reads one JSON
request on stdin: operation=preview|apply, pass_id, and (apply only)
expected_pass_sha256 / expected_row_sha256 from preview. No token or authority
claims are accepted. Runtime configuration supplies the native state and DB.

Preview opens existing files read-only, never LifecycleLease/WorkerLock writers.
Apply takes existing invocation EX -> worker EX -> lane SH -> publication EX ->
canonical DB BEGIN IMMEDIATE, then re-derives CAS. No remote I/O, dispatch, grant,
ledger, old-pass rewrite, refund, result fabrication or history clearing occurs.
A released native worker plus an idle existing invocation sidecar establishes
only quiescent *native publication* ownership. Remote execution remains UNKNOWN;
records naming a remote engine/job require separate liveness proof and HOLD.
Absent handles do not establish quiescence. One immutable fence is the commit
point, with exact original bytes durably held before it. A replay authenticates
both. Writers check the fence inside their publication lock on every use; a
committed fence closes the pass ID itself, so a caller-minted token is refused.
Canonical owner rows are read page-by-page (DB + committed WAL frames), so the
observation is bounded by the owner table, not by the canonical DB size.

Canon v0.26.2: SECD-03 complete mediation / SECD-05 fail-safe defaults,
Security Engineering (no chapter locator on either row); DATA-14 no dual writes,
Designing Data-Intensive Applications ch. 11; GRPH-14 walkable lineage, Building
Knowledge Graphs ch. 8 + Mental Models ch. 8. This disposition is no new authority.
"""

from __future__ import annotations

import array
import contextlib
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import struct
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_SHA = re.compile(r"[0-9a-f]{64}")
_TOKEN = re.compile(r"[0-9a-f]{32}")
_LIMIT = 16 * 1024 * 1024
_OBSERVE_SECONDS = 5.0  # Wall bound for one canonical observation, never a model/turn policy.
_WAL_LIMIT = 128 * 1024 * 1024  # WAL frames are checksummed in full; DB pages are read on demand.
_PAGE_BUDGET = 262144  # Distinct b-tree/overflow page reads per observation.
_CANDIDATES = 8
_SCHEMA = "workbay/invocation-administrative-unknown@1"
_BINDING = ("task_ref", "lane_id", "worktree_path", "branch")


class DispositionHold(ValueError):
    """Bounded, machine-readable refusal; never include private input bytes."""


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise DispositionHold(reason)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(raw: bytes) -> dict[str, Any]:
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, "duplicate_json_key")
            value[key] = item
        return value

    def constant(_value):
        raise DispositionHold("nonfinite_json")

    def finite(text):
        # Finite grammar can still overflow (1e999 -> inf), at any nesting depth.
        number = float(text)
        _require(math.isfinite(number), "nonfinite_json")
        return number

    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=finite)
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, DispositionHold):
            raise
        raise DispositionHold("invalid_json") from None
    _require(isinstance(value, dict), "invalid_json_object")
    return value


@contextlib.contextmanager
def _directory(path: Path):
    """Walk every directory by descriptor: no symlink ancestor escape."""
    path = Path(os.path.abspath(path))
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = new
        yield fd
    finally:
        os.close(fd)


def _identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def ensure_state_directory(path: Path) -> None:
    """Create absent native directories by descriptor, rejecting link ancestors."""
    absolute = Path(os.path.abspath(path))
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in absolute.parts[1:]:
            try:
                new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = new
    finally:
        os.close(fd)


@contextlib.contextmanager
def _regular(path: Path):
    with _directory(path.parent) as parent:
        fd = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_NOATIME", 0), dir_fd=parent
        )
        try:
            before = os.fstat(fd)
            _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, "unsafe_regular_file")
            _require(before.st_uid == os.getuid(), "foreign_file_owner")
            _require(
                _identity(before) == _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)),
                "file_replaced",
            )
            yield fd
            _require(_identity(before) == _identity(os.fstat(fd)), "file_changed")
            _require(
                _identity(before) == _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)),
                "file_replaced",
            )
        finally:
            os.close(fd)


def regular_bytes(path: Path, *, limit: int = _LIMIT) -> bytes:
    """The same strict primitive is used by rescue, await/read, apply and writer."""
    with _regular(Path(path)) as fd:
        _require(os.fstat(fd).st_size <= limit, "record_too_large")
        raw = bytearray()
        while len(raw) <= limit:
            chunk = os.read(fd, min(65536, limit + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        _require(len(raw) <= limit, "record_too_large")
        return bytes(raw)


_LEGACY_IDENTITY_BYTE_BUDGET = 64 * 1024 * 1024


class _LegacyIdentityReader:
    """Validate JSON without retaining payloads; capture only receipt identity.

    A prefix is insufficient: a later duplicate identity or malformed suffix
    must not turn charged history into an omitted foreign receipt. Memory is
    bounded by a 64 KiB chunk, nesting depth 64, 4096-character tokens,
    and at most 128 active object keys totaling 4096 characters.
    """

    _space = re.compile(r"[ \t\r\n]+")
    _string_run = re.compile(r'[^"\\\x00-\x1f]+')

    def __init__(self, fd: int, size: int, *, include_bindings: bool):
        import codecs

        _require(size <= _LEGACY_IDENTITY_BYTE_BUDGET, "too_large_identity_unproven")
        self.fd = fd
        self.remaining = size
        self.decoder = codecs.getincrementaldecoder("utf-8")()
        self.chunk = ""
        self.pos = 0
        self.task_ref = None
        self.seen_task_ref = False
        self.key_characters = 0
        self.key_count = 0
        self.include_bindings = include_bindings
        self.binding_object = False
        self.identity = {}

    def peek(self) -> str:
        while self.pos == len(self.chunk):
            if not self.remaining:
                self.decoder.decode(b"", final=True)
                return ""
            raw = os.read(self.fd, min(65536, self.remaining))
            _require(bool(raw), "malformed")
            self.remaining -= len(raw)
            self.chunk = self.decoder.decode(raw)
            self.pos = 0
        return self.chunk[self.pos]

    def take(self, expected: str) -> None:
        _require(self.peek() == expected, "malformed")
        self.pos += 1

    def whitespace(self) -> None:
        while self.peek() and self.peek() in " \t\r\n":
            match = self._space.match(self.chunk, self.pos)
            self.pos = match.end()

    def string(self, *, capture: bool = False) -> str | None:
        self.take('"')
        parts = []
        length = 0

        def retain(text):
            nonlocal length, capture
            if capture:
                length += len(text)
                if length > 4096:
                    capture = False
                    parts.clear()
                else:
                    parts.append(text)

        while True:
            char = self.peek()
            _require(bool(char), "malformed")
            if char == '"':
                self.pos += 1
                return json.loads('"' + "".join(parts) + '"') if capture else None
            if char == "\\":
                self.pos += 1
                escape = self.peek()
                _require(bool(escape) and escape in '"\\/bfnrtu', "malformed")
                self.pos += 1
                text = "\\" + escape
                if escape == "u":
                    for _ in range(4):
                        digit = self.peek()
                        _require(bool(digit) and digit in "0123456789abcdefABCDEF", "malformed")
                        self.pos += 1
                        text += digit
                retain(text)
            else:
                match = self._string_run.match(self.chunk, self.pos)
                _require(match is not None, "malformed")
                retain(match.group())
                self.pos = match.end()

    def identity_string(self) -> str | None:
        if self.peek() == "n":
            for char in "null":
                self.take(char)
            return None
        value = self.string(capture=True)
        _require(value is not None, "too_large_identity_unproven")
        return value

    def value(self, depth: int = 0) -> None:
        _require(depth <= 64, "too_large_identity_unproven")
        self.whitespace()
        char = self.peek()
        _require(bool(char), "malformed")
        if char == "{":
            self.pos += 1
            keys = set()
            characters = 0
            self.whitespace()
            if self.peek() != "}":
                while True:
                    key = self.string(capture=True)
                    _require(key is not None, "too_large_identity_unproven")
                    _require(key not in keys, "malformed")
                    keys.add(key)
                    characters += len(key)
                    self.key_characters += len(key)
                    self.key_count += 1
                    _require(self.key_characters <= 4096 and self.key_count <= 128,
                             "too_large_identity_unproven")
                    self.whitespace()
                    self.take(":")
                    self.whitespace()
                    if depth == 0 and key == "task_ref":
                        _require(not self.seen_task_ref, "malformed")
                        self.seen_task_ref = True
                        self.task_ref = self.string(capture=True)
                        _require(bool(self.task_ref), "too_large_identity_unproven")
                    elif self.include_bindings and depth == 0 and key in ("branch", "lane_id", "review_cycle_dispatch_id"):
                        self.identity[key] = self.identity_string()
                    elif self.include_bindings and depth == 0 and key == "lane_cycle_binding":
                        if self.peek() == "n":
                            self.identity[key] = self.identity_string()
                        else:
                            _require(self.peek() == "{", "malformed")
                            self.identity[key] = {}
                            self.binding_object = True
                            self.value(depth + 1)
                            self.binding_object = False
                    elif self.binding_object and depth == 1 and key == "branch":
                        self.identity["lane_cycle_binding"][key] = self.identity_string()
                    else:
                        self.value(depth + 1)
                    self.whitespace()
                    if self.peek() == "}":
                        break
                    self.take(",")
                    self.whitespace()
            self.take("}")
            self.key_characters -= characters
            self.key_count -= len(keys)
        elif char == "[":
            self.pos += 1
            self.whitespace()
            if self.peek() != "]":
                while True:
                    self.value(depth + 1)
                    self.whitespace()
                    if self.peek() == "]":
                        break
                    self.take(",")
            self.take("]")
        elif char == '"':
            self.string()
        elif char in "tfn":
            for expected in {"t": "true", "f": "false", "n": "null"}[char]:
                self.take(expected)
        else:
            # Bounded scalar validation includes finite numbers, like _json.
            number = ""
            while self.peek() and self.peek() in "-+0123456789.eE":
                _require(len(number) < 4096, "too_large_identity_unproven")
                number += self.peek()
                self.pos += 1
            _require(bool(re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", number)), "malformed")
            if any(marker in number for marker in ".eE"):
                _require(math.isfinite(float(number)), "malformed")


def legacy_pass_identity(path: Path, *, include_bindings: bool = False) -> dict:
    """Prove a legacy identity within fixed bounds, never following symlinks.

    This supplies only a foreign-task exclusion, never same-task authority.
    The descriptor and path must remain the same regular file throughout.
    """
    try:
        with _regular(Path(path)) as fd:
            reader = _LegacyIdentityReader(fd, os.fstat(fd).st_size, include_bindings=include_bindings)
            reader.whitespace()
            _require(reader.peek() == "{", "malformed")
            reader.value()
            reader.whitespace()
            _require(reader.peek() == "", "malformed")
            _require(reader.task_ref is not None, "too_large_identity_unproven")
            return {**reader.identity, "task_ref": reader.task_ref}
    except (UnicodeError, RecursionError) as exc:
        raise DispositionHold("malformed") from exc


def legacy_pass_task_ref(path: Path) -> str:
    """Prove just the task identity when no branch/dispatch binding is needed."""
    return legacy_pass_identity(path)["task_ref"]


def read_record(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = regular_bytes(path)
    return _json(raw), raw


def _pass_path(state_dir: Path, pass_id: str) -> Path:
    _require(isinstance(pass_id, str) and bool(_ID.fullmatch(pass_id)), "invalid_pass_locator")
    return state_dir / f"offload-pass-{pass_id}.json"


def _token_hash(token: Any) -> str:
    _require(isinstance(token, str) and bool(_TOKEN.fullmatch(token)), "native_invocation_unproven")
    return _sha(token.encode())


def _custody_dir(state_dir: Path, pass_id: str) -> Path:
    _pass_path(state_dir, pass_id)
    return state_dir / "invocation-dispositions" / pass_id


def _private(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    _require(
        (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
        and info.st_uid == os.getuid()
        and not info.st_mode & (0o022 if directory else 0o077),
        "custody_unprotected",
    )
    if not directory:
        _require(info.st_nlink == 1, "custody_linked")


def _closed_keys(state_dir: Path, pass_id: str) -> list[str] | None:
    """Committed fences of this pass ID (None: no custody directory at all).

    A committed fence closes the pass *identity*: every entry is validated and
    any one of them refuses later publication, whatever token a caller brings.
    Only unreferenced ``.pending-`` staging (a pre-commit crash) is ignored.
    """
    directory = _custody_dir(state_dir, pass_id)
    try:
        with _directory(directory) as fd:
            _private(directory.parent, directory=True)
            _private(directory, directory=True)
            names = os.listdir(fd)
    except FileNotFoundError:
        return None
    _require(len(names) <= 64, "custody_invalid")
    keys = sorted(name for name in names if _SHA.fullmatch(name))
    _require(all(name in keys or name.startswith(".pending-") for name in names), "custody_invalid")
    for key in keys:
        _require(_fence_key(state_dir, pass_id, key) is not None, "custody_incomplete")
    return keys


def _fence(state_dir: Path, pass_id: str, token: str) -> dict[str, Any] | None:
    directory = _custody_dir(state_dir, pass_id)
    try:
        with _directory(directory):
            _private(directory.parent, directory=True)
            _private(directory, directory=True)
    except FileNotFoundError:
        return None
    return _fence_key(state_dir, pass_id, _token_hash(token))


def _fence_key(state_dir: Path, pass_id: str, key: str) -> dict[str, Any] | None:
    directory = _custody_dir(state_dir, pass_id) / key
    try:
        with _directory(directory):
            _private(directory, directory=True)
    except FileNotFoundError:
        return None
    path = directory / "fence.json"
    try:
        fence, _ = read_record(path)
    except FileNotFoundError:
        raise DispositionHold("custody_incomplete") from None
    _private(path)
    _require(
        set(fence)
        == {
            "schema",
            "pass_id",
            "invocation_sha256",
            "pass_sha256",
            "row_sha256",
            "binding",
            "administration",
            "observed_execution",
            "charge",
            "native_admission",
        },
        "fence_invalid",
    )
    _require(
        fence["schema"] == _SCHEMA
        and fence["pass_id"] == pass_id
        and fence["invocation_sha256"] == key
        and fence["administration"] == "closed_UNKNOWN"
        and fence["observed_execution"] == "UNKNOWN"
        and fence["charge"] == "retained"
        and fence["native_admission"] is False
        and isinstance(fence["binding"], dict)
        and isinstance(fence["pass_sha256"], str)
        and bool(_SHA.fullmatch(fence["pass_sha256"]))
        and isinstance(fence["row_sha256"], str)
        and bool(_SHA.fullmatch(fence["row_sha256"])),
        "fence_invalid",
    )
    original_path = directory / "original.json"
    original, raw = read_record(original_path)
    _private(original_path)
    _require(
        _sha(raw) == fence["pass_sha256"]
        and _token_hash(original.get("invocation_token")) == key
        and original.get("lane_cycle_binding") == fence["binding"]
        and original.get("status") == "submitted"
        and "result" not in original,
        "custody_invalid",
    )
    return fence


def administrative_view(state_dir: Path, pass_id: str, record: dict, raw: bytes) -> dict | None:
    token = record.get("invocation_token")
    if token is None:
        # Even legacy token-less reads must not silently bypass malformed custody.
        directory = _custody_dir(state_dir, pass_id)
        try:
            with _directory(directory):
                raise DispositionHold("native_invocation_unproven")
        except FileNotFoundError:
            return None
    keys = _closed_keys(state_dir, pass_id)
    if not keys:
        return None
    # A closed pass whose live bytes name another invocation was replaced
    # outside native publication: HOLD rather than show it as open.
    _require(_token_hash(token) in keys, "closed_source_changed")
    fence = _fence(state_dir, pass_id, token)
    _require(
        fence["pass_sha256"] == _sha(raw) and fence["binding"] == record.get("lane_cycle_binding"),
        "closed_source_changed",
    )
    return {
        "status": "closed_UNKNOWN",
        "observed_execution": "UNKNOWN",
        "charge": "retained",
        "native_admission": False,
        "pass_sha256": fence["pass_sha256"],
    }


def publication_allowed(
    state_dir: Path,
    pass_id: str,
    previous: dict | None,
    *,
    invocation_token: str | None,
    expected_invocation_token: str | None,
) -> bool:
    """Call only while holding the native per-pass publication lock.

    The decision and the caller's write share that lock, as does apply's
    commit rename, so there is no check-then-write window. A committed fence
    closes the pass ID: a caller-minted token is not native admission, and no
    native replacement transition exists, so new work needs a new pass ID.
    """
    keys = _closed_keys(state_dir, pass_id)
    if keys is None:
        return True
    if keys:
        return False
    # Only unreferenced pre-commit staging: anonymous writers still cannot publish.
    return invocation_token is not None or expected_invocation_token is not None


@contextlib.contextmanager
def _probe_lock(path: Path):
    """Observe or retain an existing kernel lock, never create/write a handle."""
    with contextlib.ExitStack() as stack:
        try:
            fd = stack.enter_context(_regular(path))
        except FileNotFoundError:
            yield "absent", None
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield "positive_live", None
            return
        try:
            yield "stopped", fd
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)


def _varint(data, offset: int) -> tuple[int, int]:
    value = 0
    for index in range(9):
        _require(offset + index < len(data), "canonical_snapshot_invalid")
        byte = data[offset + index]
        if index == 8:
            return (value << 8) | byte, offset + 9
        value = (value << 7) | (byte & 0x7F)
        if byte < 0x80:
            return value, offset + index + 1
    raise AssertionError("unreachable")


def _serial_size(kind: int) -> int:
    _require(kind not in (10, 11), "canonical_snapshot_invalid")
    return (0, 1, 2, 3, 4, 6, 8, 8, 0, 0)[kind] if kind < 10 else (kind - 12) // 2


def _serial_value(kind: int, raw: bytes):
    if kind == 0:
        return None
    if kind in (8, 9):
        return kind - 8
    if kind == 7:
        return struct.unpack(">d", raw)[0]
    if kind < 7:
        return int.from_bytes(raw, "big", signed=True)
    if kind % 2 == 0:
        return bytes(raw)
    try:
        return bytes(raw).decode("utf-8")
    except UnicodeDecodeError:
        raise DispositionHold("canonical_snapshot_invalid") from None


class _Pages:
    """Committed page view: DB pages overlaid by checksum-valid committed WAL frames.

    Reads only the pages a lookup touches (https://sqlite.org/fileformat2.html
    sections 1.3, 1.6, 2.3, 4.1-4.5), so allocation follows the owner table and
    WAL, never the canonical database size. Nothing native is opened for write.
    """

    def __init__(self, fd: int, wal: bytes, deadline: float):
        self.fd, self.wal, self.deadline, self.frames, self.read = fd, wal, deadline, {}, {}
        header = os.pread(fd, 100, 0)
        _require(len(header) == 100 and header.startswith(b"SQLite format 3\x00"), "canonical_database_invalid")
        size = int.from_bytes(header[16:18], "big")
        self.size = 65536 if size == 1 else size
        self.usable = self.size - header[20]
        _require(
            512 <= self.size <= 65536 and self.size & (self.size - 1) == 0 and self.usable >= 480,
            "canonical_database_invalid",
        )
        _require(int.from_bytes(header[56:60], "big") == 1, "canonical_encoding_unsupported")
        in_header = int.from_bytes(header[28:32], "big")
        valid = in_header and header[24:28] == header[92:96]
        self.count = in_header if valid else os.fstat(fd).st_size // self.size
        if wal:
            self._frames()

    def tick(self) -> None:
        _require(time.monotonic() < self.deadline, "canonical_snapshot_timeout")

    def _frames(self) -> None:
        wal = self.wal
        _require(len(wal) >= 32, "canonical_wal_invalid")
        magic, version, page_size, _sequence, salt1, salt2, check0, check1 = struct.unpack(">8I", wal[:32])
        _require(
            magic in (0x377F0682, 0x377F0683) and version == 3007000 and page_size == self.size,
            "canonical_wal_invalid",
        )
        swap = (magic == 0x377F0682) != (sys.byteorder == "little")

        def checksum(raw, state):
            words = array.array("I")
            words.frombytes(raw)
            if swap:
                words.byteswap()
            s0, s1 = state
            pairs = iter(words)
            for x in pairs:
                s0 = (s0 + x + s1) & 0xFFFFFFFF
                s1 = (s1 + next(pairs) + s0) & 0xFFFFFFFF
            return s0, s1

        state = checksum(wal[:24], (0, 0))
        _require(state == (check0, check1), "canonical_wal_invalid")
        pending = {}
        step = self.size + 24
        for offset in range(32, len(wal) - step + 1, step):
            self.tick()
            page, commit, f_salt1, f_salt2, f_check0, f_check1 = struct.unpack_from(">6I", wal, offset)
            following = checksum(wal[offset + 24 : offset + step], checksum(wal[offset : offset + 8], state))
            if (f_salt1, f_salt2) != (salt1, salt2) or following != (f_check0, f_check1):
                break  # SQLite ignores an uncommitted/stale tail after reset.
            _require(page > 0, "canonical_wal_invalid")
            state = following
            pending[page] = offset + 24
            if commit:
                self.frames.update(pending)
                pending.clear()
                self.count = commit

    def page(self, number: int) -> bytes:
        self.tick()
        _require(0 < number <= self.count, "canonical_snapshot_invalid")
        offset = self.frames.get(number)
        if offset is not None:
            return self.wal[offset : offset + self.size]
        data = os.pread(self.fd, self.size, (number - 1) * self.size)
        _require(len(data) == self.size, "canonical_snapshot_invalid")
        digest = hashlib.sha256(data).digest()
        _require(self.read.setdefault(number, digest) == digest, "canonical_snapshot_changed")
        _require(len(self.read) <= _PAGE_BUDGET, "canonical_snapshot_too_large")
        return data

    def unchanged(self) -> bool:
        """Re-read every database page used; a concurrent checkpoint/rollback HOLDs."""
        for number, digest in self.read.items():
            self.tick()
            if hashlib.sha256(os.pread(self.fd, self.size, (number - 1) * self.size)).digest() != digest:
                return False
        return True

    def payload(self, data: bytes, start: int, size: int, want: int) -> bytes:
        """First ``want`` bytes of one table-leaf payload, following overflow pages."""
        usable = self.usable
        most = usable - 35
        want = min(size, want)
        if size <= most:
            _require(start + size <= usable, "canonical_snapshot_invalid")
            return data[start : start + want]
        least = (usable - 12) * 32 // 255 - 23
        local = least + (size - least) % (usable - 4)
        local = local if local <= most else least
        _require(start + local + 4 <= usable, "canonical_snapshot_invalid")
        result = bytearray(data[start : start + local])
        following = int.from_bytes(data[start + local : start + local + 4], "big")
        hops = 0
        while len(result) < want:
            hops += 1
            _require(following and hops <= size // (usable - 4) + 1, "canonical_snapshot_invalid")
            overflow = self.page(following)
            following = int.from_bytes(overflow[:4], "big")
            result += overflow[4:usable]
        return bytes(result[:want])

    def cells(self, root: int):
        """Yield (rowid, page, payload offset, payload size) for one table b-tree."""
        stack, seen = [(root, 0)], set()
        while stack:
            number, depth = stack.pop()
            _require(number not in seen and depth < 64, "canonical_snapshot_invalid")
            seen.add(number)
            data = self.page(number)
            base = 100 if number == 1 else 0
            kind = data[base]
            _require(kind in (5, 13), "canonical_snapshot_invalid")
            count = int.from_bytes(data[base + 3 : base + 5], "big")
            head = base + (12 if kind == 5 else 8)
            _require(head + 2 * count <= self.usable, "canonical_snapshot_invalid")
            pointers = [int.from_bytes(data[head + 2 * i : head + 2 * i + 2], "big") for i in range(count)]
            if kind == 5:
                children = [int.from_bytes(data[p : p + 4], "big") for p in pointers]
                children.append(int.from_bytes(data[base + 8 : base + 12], "big"))
                stack.extend((child, depth + 1) for child in reversed(children))
                continue
            for pointer in pointers:
                _require(head + 2 * count <= pointer < self.usable, "canonical_snapshot_invalid")
                size, offset = _varint(data, pointer)
                rowid, offset = _varint(data, offset)
                _require(size <= _LIMIT, "canonical_row_too_large")
                yield rowid - (1 << 64) * (rowid >= 1 << 63), data, offset, size

    def record(self, data: bytes, offset: int, size: int, columns: int | None = None) -> list:
        """Decode a record, optionally only its first ``columns`` values."""
        raw = self.payload(data, offset, size, 4096)
        header, cursor = _varint(raw, 0)
        _require(header <= size, "canonical_snapshot_invalid")
        if header > len(raw):
            raw = self.payload(data, offset, size, header)
        kinds = []
        while cursor < header and (columns is None or len(kinds) < columns):
            kind, cursor = _varint(raw, cursor)
            kinds.append(kind)
        _require(cursor <= header, "canonical_snapshot_invalid")
        end = header + sum(_serial_size(kind) for kind in kinds)
        _require(end <= size, "canonical_snapshot_invalid")
        if end > len(raw):
            raw = self.payload(data, offset, size, end)
        values, cursor = [], header
        for kind in kinds:
            length = _serial_size(kind)
            values.append(_serial_value(kind, raw[cursor : cursor + length]))
            cursor += length
        return values


def _readonly_owner_rows(path: Path, lane_id: str) -> list[dict]:
    """Bounded read-only owner lookup coherent with committed WAL frames.

    SQLite mode=ro still updates SHM read marks, so no native handle is opened.
    Only sqlite_schema and worktree_lanes pages are read; matching rows are
    re-materialised in a private :memory: table built from the native DDL so
    SQLite itself applies column defaults, rowid aliasing and the WHERE clause.
    Apply rechecks under a real locked DB transaction; this never replaces it.
    """
    deadline = time.monotonic() + _OBSERVE_SECONDS
    wal_path, journal_path = Path(str(path) + "-wal"), Path(str(path) + "-journal")

    def optional(p, limit):
        try:
            return regular_bytes(p, limit=limit)
        except FileNotFoundError:
            return b""

    def journal() -> bytes:
        try:
            with _regular(journal_path) as fd:
                return os.pread(fd, 8, 0)
        except FileNotFoundError:
            return b""

    _require(not any(journal()), "canonical_journal_unresolved")
    with _regular(path) as fd:
        wal = optional(wal_path, _WAL_LIMIT)
        pages = _Pages(fd, wal, deadline)
        schema = [pages.record(data, offset, size) for _rowid, data, offset, size in pages.cells(1)]
        tables = [
            row
            for row in schema
            if len(row) == 5 and row[0] == "table" and isinstance(row[1], str) and row[1].lower() == "worktree_lanes"
        ]
        _require(len(tables) == 1, "canonical_schema_unsupported")
        _kind, _name, _table, root, sql = tables[0]
        _require(
            type(root) is int and isinstance(sql, str) and sql.lstrip()[:12].upper() == "CREATE TABLE",
            "canonical_schema_unsupported",
        )
        with contextlib.closing(sqlite3.connect(":memory:", timeout=0)) as memory:
            memory.execute("PRAGMA trusted_schema=OFF")
            memory.execute("PRAGMA ignore_check_constraints=ON")
            memory.execute(sql)
            columns = memory.execute("PRAGMA table_xinfo(worktree_lanes)").fetchall()
            names = [column[1] for column in columns]
            _require("lane_id" in names and all(column[6] == 0 for column in columns), "canonical_schema_unsupported")
            keys = [column for column in columns if column[5]]
            alias = names.index(keys[0][1]) if len(keys) == 1 and keys[0][2].upper() == "INTEGER" else None
            position, wanted = names.index("lane_id"), lane_id.encode().lower()
            candidates = []
            for rowid, data, offset, size in pages.cells(root):
                value = pages.record(data, offset, size, position + 1)
                found = value[position] if len(value) > position else None
                if isinstance(found, str) and found.encode().rstrip(b" ").lower() != wanted:
                    continue
                if found is None or isinstance(found, bytes):
                    continue  # Neither can equal a TEXT locator under any collation.
                values = pages.record(data, offset, size)
                _require(len(values) <= len(names), "canonical_snapshot_invalid")
                candidates.append((rowid, values))
                _require(len(candidates) <= _CANDIDATES, "canonical_owner_ambiguous")
            for rowid, values in candidates:
                listed = list(names[: len(values)])
                if alias is not None and alias < len(values) and values[alias] is None:
                    values[alias] = rowid
                else:
                    listed.append("rowid")
                    values.append(rowid)
                quoted = ",".join('"' + name.replace('"', '""') + '"' for name in listed)
                memory.execute(f"INSERT INTO worktree_lanes({quoted}) VALUES({','.join('?' * len(values))})", values)
            memory.row_factory = sqlite3.Row
            rows = [dict(r) for r in memory.execute("SELECT * FROM worktree_lanes WHERE lane_id=?", (lane_id,))]
        _require(pages.unchanged() and wal == optional(wal_path, _WAL_LIMIT), "canonical_snapshot_changed")
    _require(not any(journal()), "canonical_journal_unresolved")
    return rows


def _row(config, record, conn=None):
    from .lane_postmerge import _try_authoritative_lane_base_sha

    binding = record.get("lane_cycle_binding")
    _require(
        isinstance(binding, dict)
        and binding.get("v") == 1
        and type(binding.get("row_id")) is int
        and type(binding.get("cycle_epoch")) is int
        and isinstance(binding.get("source_base_sha"), str)
        and bool(re.fullmatch(r"[0-9a-f]{40}", binding["source_base_sha"])),
        "native_binding_unproven",
    )
    with contextlib.ExitStack():
        if conn is None:
            matches = _readonly_owner_rows(Path(config.db_path), record.get("lane_id"))
        else:
            matches = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM worktree_lanes WHERE lane_id=?", (record.get("lane_id"),)
                ).fetchall()
            ]
        _require(len(matches) == 1, "canonical_owner_ambiguous")
        row = matches[0]
        _require(
            row.get("id") == binding["row_id"]
            and row.get("cycle_epoch") == binding["cycle_epoch"]
            and all(row.get(k) == binding.get(k) for k in _BINDING)
            and all(record.get(k) == binding.get(k) for k in ("task_ref", "lane_id")),
            "canonical_binding_changed",
        )
        source, detail = _try_authoritative_lane_base_sha(
            row=row, task_ref=row["task_ref"], lane_id=row["lane_id"], repo=config.workspace_root
        )
        _require(detail is None and source == binding["source_base_sha"], "canonical_source_changed")
        return row, _sha(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def _observation(config, pass_id: str, *, conn=None):
    state_dir = Path(config.state_dir)
    record, raw = read_record(_pass_path(state_dir, pass_id))
    _require(record.get("status") == "submitted" and "result" not in record, "not_submitted_unknown")
    _token_hash(record.get("invocation_token"))
    _require(isinstance(record.get("lane_id"), str) and bool(_ID.fullmatch(record["lane_id"])), "invalid_native_lane")
    row, row_sha = _row(config, record, conn)
    view = administrative_view(state_dir, pass_id, record, raw)
    return record, raw, row, row_sha, view


def _response(pass_id, status="hold", reason=None, **extra):
    return {
        "pass_id": pass_id,
        "administration": {"status": status, "reason": reason},
        "observed_execution": "UNKNOWN",
        "charge": "retained",
        "native_admission": False,
        "rearm_ready": False,
        **extra,
    }


def _observe_locks(stack, state_dir, pass_id, record, states=None):
    if states is None:
        states = {"invocation": "not_probed", "worker": "not_probed", "remote_execution": "not_probed"}
    invocation = state_dir / f".offload-pass-{pass_id}.json.api-invocation.lifecycle.lock"
    state, _ = stack.enter_context(_probe_lock(invocation))
    states["invocation"] = state
    worker = state_dir / f"worker-{record['lane_id']}.lock"
    state, fd = stack.enter_context(_probe_lock(worker))
    states["worker"] = state
    if fd is not None:
        states["worker"] = "unknown"
        payload = _json(os.read(fd, _LIMIT + 1))
        if payload.get("released") is True:
            states["worker"] = "stopped"
    return states


def _eligibility(record, states):
    if "positive_live" in states.values():
        return "native_owner_live"
    if states["invocation"] != "stopped" or states["worker"] != "stopped":
        return "native_owner_unproven"
    # No absent remote job or later NONREVIEW receipt is treated as no-start.
    if record.get("backend") or any(
        record.get(k) for k in ("job_id", "dispatch_id", "agent_stream_path", "review_carrier_path", "progress")
    ):
        return "execution_liveness_unproven"
    return None


def preview(pass_id: str, *, state_dir: Path | None = None) -> dict:
    """Bounded read-only observation, including missing/unreachable ownership."""
    states = {"invocation": "not_probed", "worker": "not_probed", "remote_execution": "not_probed"}
    try:
        from workbay_handoff_mcp.runtime import get_runtime_config

        config = get_runtime_config()
        _require(state_dir is None or Path(state_dir) == Path(config.state_dir), "native_state_root_mismatch")
        record, raw, row, row_sha, view = _observation(config, pass_id)
        with contextlib.ExitStack() as stack:
            states = _observe_locks(stack, Path(config.state_dir), pass_id, record, states)
            reason = _eligibility(record, states)
        return _response(
            pass_id,
            "closed_UNKNOWN" if view else "hold" if reason else "preview",
            reason if not view else None,
            liveness=states,
            expected_pass_sha256=_sha(raw),
            expected_row_sha256=row_sha,
            canonical={"row_id": row["id"], "cycle_epoch": row["cycle_epoch"], "binding": record["lane_cycle_binding"]},
        )
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        if isinstance(exc, OSError):
            states = {
                k: "unreachable" if v == "not_probed" and k != "remote_execution" else v for k, v in states.items()
            }
        return _response(
            pass_id,
            reason=str(exc) if isinstance(exc, DispositionHold) else "native_evidence_unavailable",
            liveness=states,
        )


def _stage_file(path: Path, raw: bytes):
    """Only unpublished 0700 staging directories accept newly created files."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _publish(state_dir: Path, pass_id: str, raw: bytes, fence: dict):
    """Atomic directory publication keeps originals and fence inseparable.

    Interrupted staging is unreferenced, never authority. A nonempty immutable
    committed directory cannot be replaced by rename, including on replay.
    """
    directory = _make_custody(state_dir, pass_id)
    target = directory / fence["invocation_sha256"]
    existing = _fence(state_dir, pass_id, _json(raw)["invocation_token"])
    if existing is not None:
        _require(existing == fence, "custody_conflict")
        return
    temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=directory))
    try:
        _stage_file(temporary / "original.json", raw)
        _stage_file(
            temporary / "fence.json", json.dumps(fence, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        )
        with _directory(temporary) as fd:
            os.fsync(fd)
        _require(regular_bytes(_pass_path(state_dir, pass_id)) == raw, "stale_cas")
        os.rename(temporary, target)
        with _directory(directory) as fd:
            os.fsync(fd)
    finally:
        # Unpublished crash leftovers carry no authority. Ordinary exceptions
        # can remove just this call's known staging files without scanning.
        if temporary.is_dir():
            for name in ("original.json", "fence.json"):
                (temporary / name).unlink(missing_ok=True)
            temporary.rmdir()


def _make_custody(state_dir, pass_id):
    directory = _custody_dir(state_dir, pass_id)
    for path in (directory.parent, directory):
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
        _private(path, directory=True)
        with _directory(path.parent) as parent:
            os.fsync(parent)
    return directory


def apply(pass_id: str, *, expected_pass_sha256: str, expected_row_sha256: str) -> dict:
    """Fence one exact native invocation, with execution and charge UNKNOWN."""
    try:
        from workbay_handoff_mcp.runtime import get_runtime_config

        from .lane_publication_fence import lane_publication_lease
        from .offload_pass import _pass_publication_lock

        config = get_runtime_config()
        state_dir = Path(config.state_dir)
        _require(
            all(isinstance(x, str) and _SHA.fullmatch(x) for x in (expected_pass_sha256, expected_row_sha256)),
            "expected_cas_required",
        )
        record, raw, row, row_sha, view = _observation(config, pass_id)
        _require((_sha(raw), row_sha) == (expected_pass_sha256, expected_row_sha256), "stale_cas")
        if view is not None:
            # Immutable committed custody is already the disposition. A replay
            # observes it without opening a writable DB or touching sidecars.
            # Re-sync the containing directories for interruption after rename
            # but before the original caller's directory sync completed.
            directory = _custody_dir(state_dir, pass_id)
            for path in (directory / _token_hash(record["invocation_token"]), directory, directory.parent):
                with _directory(path) as fd:
                    os.fsync(fd)
            return _response(pass_id, "closed_UNKNOWN", replay=True)
        with contextlib.ExitStack() as stack:
            states = _observe_locks(stack, state_dir, pass_id, record)
            reason = _eligibility(record, states)
            _require(reason is None, reason or "native_owner_unproven")
            lease = stack.enter_context(
                lane_publication_lease(
                    state_dir, record["task_ref"], record["lane_id"], deadline=time.monotonic() + 0.5
                )
            )
            _require(not lease.borrowed, "destructive_lane_owner")
            stack.enter_context(_pass_publication_lock(_pass_path(state_dir, pass_id), deadline=time.monotonic() + 0.5))
            conn = stack.enter_context(
                contextlib.closing(sqlite3.connect(Path(config.db_path).as_uri() + "?mode=rw", uri=True, timeout=0.5))
            )
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            stack.callback(conn.rollback)
            current, current_raw, current_row, current_row_sha, current_view = _observation(config, pass_id, conn=conn)
            _require(
                current_raw == raw and current_row_sha == row_sha and current == record and current_row == row,
                "stale_cas",
            )
            # Recheck inode, marker and CAS immediately before the commit point.
            _require(_eligibility(current, states) is None, "native_owner_unproven")
            fence = {
                "schema": _SCHEMA,
                "pass_id": pass_id,
                "invocation_sha256": _token_hash(current["invocation_token"]),
                "pass_sha256": expected_pass_sha256,
                "row_sha256": expected_row_sha256,
                "binding": current["lane_cycle_binding"],
                "administration": "closed_UNKNOWN",
                "observed_execution": "UNKNOWN",
                "charge": "retained",
                "native_admission": False,
            }
            _require(regular_bytes(_pass_path(state_dir, pass_id)) == raw, "stale_cas")
            _publish(state_dir, pass_id, raw, fence)
            _require(administrative_view(state_dir, pass_id, current, raw) is not None, "custody_invalid")
            return _response(pass_id, "closed_UNKNOWN", replay=current_view is not None, liveness=states)
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        return _response(
            pass_id, reason=str(exc) if isinstance(exc, DispositionHold) else "native_evidence_unavailable"
        )


def request(value: dict) -> dict:
    """Public protocol accepts locators and CAS, never bearer or claims."""
    if not isinstance(value, dict) or set(value) - {
        "operation",
        "pass_id",
        "expected_pass_sha256",
        "expected_row_sha256",
    }:
        return _response(None, reason="unsupported_request")
    operation, pass_id = value.get("operation"), value.get("pass_id")
    if operation == "preview":
        return preview(pass_id)
    if operation == "apply":
        return apply(
            pass_id,
            expected_pass_sha256=value.get("expected_pass_sha256"),
            expected_row_sha256=value.get("expected_row_sha256"),
        )
    return _response(pass_id, reason="unsupported_operation")


def main() -> int:
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import configure_runtime

    configure_runtime(RuntimeConfig.for_repo(Path.cwd()))
    try:
        raw = sys.stdin.buffer.read(_LIMIT + 1)
        _require(len(raw) <= _LIMIT, "request_too_large")
        result = request(_json(raw))
    except (OSError, ValueError) as exc:
        result = _response(None, reason=str(exc) if isinstance(exc, DispositionHold) else "invalid_request")
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 2 if result["administration"]["status"] == "hold" else 0


if __name__ == "__main__":
    raise SystemExit(main())
