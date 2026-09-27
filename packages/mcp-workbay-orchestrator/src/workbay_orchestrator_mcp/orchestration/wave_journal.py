"""Append-only wave journals stored as Git commit chains.

Each event is a commit on ``refs/workbay/journal/<wave>``. Producers derive a
stable event id from their own sequence number, and ``update-ref`` publishes a
commit only when the observed tip is still current. A laptop can fetch that
ref and import its events into a local SQLite store more than once safely.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_WAVE_ID_RE = re.compile(r"^(?!.*\.\.)(?!.*\.lock$)(?!.*\.$)[a-z0-9][a-z0-9._-]{0,63}$")
_MAX_EVENTS_DEFAULT = 100_000
_GIT_TIMEOUT_SECONDS = 15
_SQLITE_INT64_MIN = -(1 << 63)
_SQLITE_INT64_MAX = (1 << 63) - 1
_REQUIRED_EVENT_KEYS = (
    "schema_version",
    "event_id",
    "wave",
    "kind",
    "producer",
    "producer_seq",
    "causal_parent",
    "ts",
    "fields",
)


class JournalError(ValueError):
    """A journal validation or read failure with machine-readable details."""

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def validate_wave_id(wave: str) -> str:
    """Validate and return a wave id suitable for use in a Git ref."""
    if not isinstance(wave, str) or _WAVE_ID_RE.fullmatch(wave) is None:
        raise JournalError("wave", "invalid_wave_id")
    return wave


def wave_ref(wave: str) -> str:
    """Return the local Git ref that stores *wave*'s journal."""
    return f"refs/workbay/journal/{validate_wave_id(wave)}"


def fetch_refspec(wave: str) -> str:
    """Return the fetch refspec that preserves the wave journal ref name."""
    ref = wave_ref(wave)
    return f"+{ref}:{ref}"


def _derive_event_id(wave: str, producer: str, producer_seq: int) -> str:
    identity = f"{wave}\x1f{producer}\x1f{producer_seq}".encode("utf-8")
    return "e" + hashlib.sha256(identity).hexdigest()[:24]


@dataclass(frozen=True, slots=True, kw_only=True)
class JournalEvent:
    """A versioned event envelope stored as the journal commit's blob."""

    schema_version: int = 1
    event_id: str
    wave: str
    kind: str
    producer: str
    producer_seq: int
    causal_parent: str | None
    ts: str
    fields: dict[str, Any]

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int:
            raise JournalError("schema_version", "wrong_type")
        if self.schema_version != 1:
            raise JournalError("schema_version", "unknown_schema_version")
        validate_wave_id(self.wave)
        for name in ("event_id", "kind", "producer", "ts"):
            if not isinstance(getattr(self, name), str):
                raise JournalError(name, "wrong_type")
        if type(self.producer_seq) is not int:
            raise JournalError("producer_seq", "wrong_type")
        _validate_producer_seq(self.producer_seq)
        if self.causal_parent is not None and not isinstance(self.causal_parent, str):
            raise JournalError("causal_parent", "wrong_type")
        if not isinstance(self.fields, dict):
            raise JournalError("fields", "wrong_type")
        _git_date(self.ts)
        if self.event_id != _derive_event_id(self.wave, self.producer, self.producer_seq):
            raise JournalError("event_id", "does_not_match_producer_sequence")

    def to_json(self) -> str:
        """Return the stable compact JSON representation of this event."""
        try:
            return json.dumps(
                {
                    "schema_version": self.schema_version,
                    "event_id": self.event_id,
                    "wave": self.wave,
                    "kind": self.kind,
                    "producer": self.producer,
                    "producer_seq": self.producer_seq,
                    "causal_parent": self.causal_parent,
                    "ts": self.ts,
                    "fields": self.fields,
                },
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise JournalError("fields", "not_json_serializable") from exc

    @classmethod
    def from_json(cls, value: str) -> JournalEvent:
        """Parse and validate one journal envelope."""
        if not isinstance(value, str):
            raise JournalError("json", "wrong_type")
        try:
            decoded = json.loads(value, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, ValueError) as exc:
            raise JournalError("json", "invalid_json") from exc
        if not isinstance(decoded, dict):
            raise JournalError("json", "wrong_type")
        for key in _REQUIRED_EVENT_KEYS:
            if key not in decoded:
                raise JournalError(key, "missing_key")
        unexpected = set(decoded) - set(_REQUIRED_EVENT_KEYS)
        if unexpected:
            raise JournalError("fields", "unexpected_key")
        if type(decoded["schema_version"]) is not int:
            raise JournalError("schema_version", "wrong_type")
        if decoded["schema_version"] != 1:
            raise JournalError("schema_version", "unknown_schema_version")
        for name in ("event_id", "wave", "kind", "producer", "ts"):
            if not isinstance(decoded[name], str):
                raise JournalError(name, "wrong_type")
        if type(decoded["producer_seq"]) is not int:
            raise JournalError("producer_seq", "wrong_type")
        if decoded["causal_parent"] is not None and not isinstance(decoded["causal_parent"], str):
            raise JournalError("causal_parent", "wrong_type")
        if not isinstance(decoded["fields"], dict):
            raise JournalError("fields", "wrong_type")
        return cls(**decoded)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _run_git(
    repo: str | os.PathLike[str],
    *args: str,
    input_data: bytes | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    from workbay_handoff_mcp.shared_write_context import run_subprocess

    command = ["git", "-C", os.fspath(repo), *args]
    try:
        return run_subprocess(
            command,
            input=input_data,
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise JournalError("git", "timeout") from exc
    except OSError as exc:
        raise JournalError("git", "unavailable") from exc


def _checked_git(
    repo: str | os.PathLike[str],
    *args: str,
    input_data: bytes | None = None,
    env: dict[str, str] | None = None,
) -> bytes:
    result = _run_git(repo, *args, input_data=input_data, env=env)
    if result.returncode != 0:
        raise JournalError("git", "command_failed")
    return result.stdout


def _event_id_for_payload(wave: str, producer: str, producer_seq: int) -> str:
    if not isinstance(producer, str):
        raise JournalError("producer", "wrong_type")
    if type(producer_seq) is not int:
        raise JournalError("producer_seq", "wrong_type")
    _validate_producer_seq(producer_seq)
    return _derive_event_id(wave, producer, producer_seq)


def _validate_producer_seq(producer_seq: int) -> None:
    if not _SQLITE_INT64_MIN <= producer_seq <= _SQLITE_INT64_MAX:
        raise JournalError("producer_seq", "outside_sqlite_int64")


def _same_append_payload(existing: JournalEvent, candidate: JournalEvent, *, compare_ts: bool) -> bool:
    if (
        existing.schema_version != candidate.schema_version
        or existing.event_id != candidate.event_id
        or existing.wave != candidate.wave
        or existing.kind != candidate.kind
        or existing.producer != candidate.producer
        or existing.producer_seq != candidate.producer_seq
        or existing.causal_parent != candidate.causal_parent
        or (compare_ts and existing.ts != candidate.ts)
    ):
        return False
    existing_fields = json.dumps(
        existing.fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    candidate_fields = json.dumps(
        candidate.fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return existing_fields == candidate_fields


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _git_date(ts: str) -> str:
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError as exc:
        raise JournalError("ts", "invalid_timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    seconds = int(parsed.astimezone(timezone.utc).timestamp())
    return f"@{seconds} +0000"


def _cat_batch(repo: str | os.PathLike[str], commit_ids: list[str]) -> list[bytes]:
    requests = b"".join(f"{commit_id}:event.json\n".encode("ascii") for commit_id in commit_ids)
    response = _checked_git(repo, "cat-file", "--batch", input_data=requests)
    offset = 0
    contents: list[bytes] = []
    for _commit_id in commit_ids:
        header_end = response.find(b"\n", offset)
        if header_end < 0:
            raise JournalError("event.json", "truncated_cat_file_response")
        header = response[offset:header_end].split()
        if len(header) != 3 or header[1] != b"blob":
            raise JournalError("event.json", "invalid_blob_header")
        try:
            size = int(header[2])
        except ValueError as exc:
            raise JournalError("event.json", "invalid_blob_size") from exc
        content_start = header_end + 1
        content_end = content_start + size
        if content_end >= len(response) or response[content_end : content_end + 1] != b"\n":
            raise JournalError("event.json", "truncated_blob")
        contents.append(response[content_start:content_end])
        offset = content_end + 1
    if offset != len(response):
        raise JournalError("event.json", "unexpected_cat_file_data")
    return contents


def _read_chain(
    repo: str | os.PathLike[str],
    wave: str,
    *,
    max_events: int = _MAX_EVENTS_DEFAULT,
) -> list[tuple[str, JournalEvent]]:
    ref = wave_ref(wave)
    if type(max_events) is not int or max_events < 0:
        raise JournalError("max_events", "invalid_limit")

    probe = _run_git(repo, "rev-parse", "--verify", "--quiet", ref)
    if probe.returncode == 1 and not probe.stderr:
        return []
    if probe.returncode != 0:
        raise JournalError("git", "command_failed")

    output = _checked_git(repo, "rev-list", "--parents", f"--max-count={max_events + 1}", ref)
    commits = [line.split() for line in output.decode("ascii").splitlines() if line]
    commits.reverse()
    for index, commit in enumerate(commits):
        parents = commit[1:]
        if len(parents) > 1 or (index > 0 and len(parents) != 1):
            raise JournalError("chain", "non_linear")
        if index > 0 and parents[0] != commits[index - 1][0]:
            raise JournalError("chain", "non_linear")
    if len(commits) > max_events:
        raise JournalError("max_events", "truncated")
    commit_ids = [commit[0] for commit in commits]
    if not commit_ids:
        return []

    contents = _cat_batch(repo, commit_ids)
    events: list[tuple[str, JournalEvent]] = []
    for commit_id, content in zip(commit_ids, contents, strict=True):
        try:
            event = JournalEvent.from_json(content.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise JournalError("event.json", "invalid_utf8") from exc
        if event.wave != wave:
            raise JournalError("wave", "ref_event_mismatch")
        events.append((commit_id, event))
    return events


def read(
    repo: str | os.PathLike[str],
    wave: str,
    *,
    max_events: int = _MAX_EVENTS_DEFAULT,
) -> list[JournalEvent]:
    """Read a wave's events in append order, refusing silent truncation."""
    return [event for _commit_id, event in _read_chain(repo, wave, max_events=max_events)]


def append(
    repo: str | os.PathLike[str],
    wave: str,
    kind: str,
    fields: dict[str, Any],
    *,
    producer: str,
    producer_seq: int,
    causal_parent: str | None = None,
    ts: str | None = None,
    max_retries: int = 8,
) -> dict[str, Any]:
    """Append one event with compare-and-swap publication and idempotent retry.

    If ``ts`` is omitted, retries compare the producer-supplied event content
    and reuse the first append's generated timestamp as envelope metadata.
    """
    try:
        event, payload_bytes, git_date, ts_was_provided, retries = _normalize_append_event(
            wave, kind, fields, producer, producer_seq, causal_parent, ts, max_retries
        )
        return _append_normalized(repo, event, payload_bytes, git_date, ts_was_provided, retries)
    except JournalError as exc:
        return {"ok": False, "reason": exc.reason}
    except (TypeError, ValueError):
        return {"ok": False, "reason": "invalid_event"}


def _normalize_append_event(
    wave: str,
    kind: str,
    fields: dict[str, Any],
    producer: str,
    producer_seq: int,
    causal_parent: str | None,
    ts: str | None,
    max_retries: int,
) -> tuple[JournalEvent, bytes, str, bool, int]:
    validate_wave_id(wave)
    if not isinstance(kind, str):
        raise JournalError("kind", "wrong_type")
    if not isinstance(fields, dict):
        raise JournalError("fields", "wrong_type")
    if type(max_retries) is not int or max_retries < 1:
        raise JournalError("max_retries", "invalid_limit")
    ts_was_provided = ts is not None
    timestamp = _utc_now() if ts is None else ts
    if not isinstance(timestamp, str):
        raise JournalError("ts", "wrong_type")
    git_date = _git_date(timestamp)
    cloned_fields = json.loads(
        json.dumps(fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    )
    event = JournalEvent(
        schema_version=1,
        event_id=_event_id_for_payload(wave, producer, producer_seq),
        wave=wave,
        kind=kind,
        producer=producer,
        producer_seq=producer_seq,
        causal_parent=causal_parent,
        ts=timestamp,
        fields=cloned_fields,
    )
    return event, event.to_json().encode("utf-8"), git_date, ts_was_provided, max_retries


def _append_normalized(
    repo: str | os.PathLike[str],
    event: JournalEvent,
    payload_bytes: bytes,
    git_date: str,
    ts_was_provided: bool,
    max_retries: int,
) -> dict[str, Any]:
    ref = wave_ref(event.wave)
    for _attempt in range(max_retries):
        if _is_symbolic_ref(repo, ref):
            return {"ok": False, "reason": "symbolic_ref"}
        chain = _read_chain(repo, event.wave)
        existing_result = _check_existing_event(chain, event, ts_was_provided)
        if existing_result is not None:
            return existing_result
        if event.causal_parent is not None and event.causal_parent not in {item.event_id for _, item in chain}:
            return {"ok": False, "reason": "unknown_causal_parent"}
        old_tip = chain[-1][0] if chain else None
        new_tip = _publish_event_attempt(repo, ref, old_tip, event, payload_bytes, git_date)
        if new_tip is not None:
            return {"ok": True, "duplicate": False, "event_id": event.event_id, "commit": new_tip}
    return {"ok": False, "reason": "cas_exhausted"}


def _is_symbolic_ref(repo: str | os.PathLike[str], ref: str) -> bool:
    result = _run_git(repo, "symbolic-ref", "--quiet", ref)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise JournalError("git", "command_failed")


def _check_existing_event(
    chain: list[tuple[str, JournalEvent]], event: JournalEvent, ts_was_provided: bool
) -> dict[str, Any] | None:
    chain_by_id = {item.event_id: item for _, item in chain}
    existing = chain_by_id.get(event.event_id)
    if existing is None:
        return None
    if _same_append_payload(existing, event, compare_ts=ts_was_provided):
        return {"ok": True, "duplicate": True}
    return {"ok": False, "reason": "event_id_conflict"}


def _publish_event_attempt(
    repo: str | os.PathLike[str],
    ref: str,
    old_tip: str | None,
    event: JournalEvent,
    payload_bytes: bytes,
    git_date: str,
) -> str | None:
    if old_tip is None:
        object_format = _checked_git(repo, "rev-parse", "--show-object-format").decode("ascii").strip()
        if object_format not in {"sha1", "sha256"}:
            raise JournalError("git", "unsupported_object_format")
        old_value = "0" * (40 if object_format == "sha1" else 64)
    else:
        old_value = old_tip
    blob = _checked_git(repo, "hash-object", "-w", "--stdin", input_data=payload_bytes).decode("ascii").strip()
    tree_input = f"100644 blob {blob}\tevent.json\n".encode("ascii")
    tree = _checked_git(repo, "mktree", input_data=tree_input).decode("ascii").strip()
    commit_args = ["commit-tree", tree]
    if old_tip is not None:
        commit_args.extend(("-p", old_tip))
    commit_args.extend(("-m", f"journal {event.wave} {event.event_id} {event.kind}"))
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "workbay-journal",
            "GIT_AUTHOR_EMAIL": "journal@workbay.invalid",
            "GIT_AUTHOR_DATE": git_date,
            "GIT_COMMITTER_NAME": "workbay-journal",
            "GIT_COMMITTER_EMAIL": "journal@workbay.invalid",
            "GIT_COMMITTER_DATE": git_date,
        }
    )
    new_tip = _checked_git(repo, *commit_args, env=env).decode("ascii").strip()
    published = _run_git(repo, "update-ref", "--no-deref", ref, new_tip, old_value)
    return new_tip if published.returncode == 0 else None


def _payload_hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _imported_at() -> str:
    return _utc_now()


def import_journal(
    repo: str | os.PathLike[str],
    wave: str,
    store_path: str | os.PathLike[str],
) -> dict[str, Any]:
    """Import events into SQLite in one transaction without overwriting rows."""
    events = read(repo, wave)
    inserted = 0
    duplicates = 0
    conflicts: list[str] = []
    orphans: list[str] = []
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(os.fspath(store_path), timeout=_GIT_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS journal_events (
                event_id TEXT PRIMARY KEY,
                wave TEXT NOT NULL,
                kind TEXT NOT NULL,
                producer TEXT NOT NULL,
                producer_seq INTEGER NOT NULL,
                causal_parent TEXT,
                payload_sha256 TEXT NOT NULL,
                payload TEXT NOT NULL,
                imported_at TEXT NOT NULL
            )
            """
        )
        available_ids = {str(row[0]) for row in conn.execute("SELECT event_id FROM journal_events")}
        imported_at = _imported_at()
        for event in events:
            if event.causal_parent is not None and event.causal_parent not in available_ids:
                orphans.append(event.event_id)

            payload = event.to_json()
            payload_sha256 = _payload_hash(payload)
            row = conn.execute(
                "SELECT payload_sha256, payload FROM journal_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO journal_events (
                        event_id, wave, kind, producer, producer_seq, causal_parent,
                        payload_sha256, payload, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.wave,
                        event.kind,
                        event.producer,
                        event.producer_seq,
                        event.causal_parent,
                        payload_sha256,
                        payload,
                        imported_at,
                    ),
                )
                inserted += 1
            else:
                existing_sha = row["payload_sha256"]
                existing_payload = row["payload"]
                actual_existing_sha = _payload_hash(existing_payload) if isinstance(existing_payload, str) else None
                if existing_sha == payload_sha256 and actual_existing_sha == existing_sha:
                    duplicates += 1
                else:
                    conflicts.append(event.event_id)
            available_ids.add(event.event_id)
        conn.commit()
    except sqlite3.Error:
        if conn is not None:
            conn.rollback()
        return {
            "ok": False,
            "reason": "store_failed",
            "read": len(events),
            "inserted": 0,
            "duplicates": 0,
            "conflicts": [],
            "orphans": [],
        }
    finally:
        if conn is not None:
            conn.close()
    return {
        "ok": not conflicts,
        "read": len(events),
        "inserted": inserted,
        "duplicates": duplicates,
        "conflicts": conflicts,
        "orphans": orphans,
    }
