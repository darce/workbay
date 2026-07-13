"""``project-events-replay`` subcommand (internal.2).

Drains the offline spool ``.task-state/pending-workflow-events.jsonl``
by replaying each entry through the canonical handoff CLI in original
order. Drained entries are removed; entries whose CLI invocation
returned non-zero stay in the spool so a later replay can retry them.

Claim-by-rename: the drain ``os.rename``s the live spool to a unique
``.draining.<pid>-<ts>`` snapshot before replay so concurrent ``_spool()``
appends target a fresh live file and are never overwritten.

The handler is read-mostly (no git mutation) and still emits the
canonical receipt schema so consumers can diff fixtures by-byte.
``test_result`` and ``state_sync`` entries are drained by re-issuing the
original projection through the canonical handoff CLI; ``decision`` entries
replay through the same ``event --event-kind decision`` argv shape with
full provenance (``task_ref`` / ``branch`` / ``commit_sha``).

Replay classification applies the same exit-0-on-``ok:false`` guard the
online projection path uses (internal): the handoff CLI prints its
JSON envelope and *always exits 0*, so a returncode-only check would
drain a rejected write. A zero-exit ``ok:false`` is therefore treated as
a rejection (kept spooled) — except the benign ``state_sync`` case where
the row already exists (``data.current_revision`` present, i.e. the
update needs an ``expected_revision``): that means the desired row is
live, so the entry is drained.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import json
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import resolver
import projection_queue

from . import _common

# Single-sourced from projection_queue (implementation note consolidation): keep the local
# names other handlers import, but bind them to the queue module's constants so
# the spool path and limit values live in exactly one place.
PENDING_REL = projection_queue.PENDING_EVENTS_REL
DRAINING_GLOB = projection_queue.DRAINING_GLOB
DEAD_LETTER_REL = projection_queue.DEAD_LETTER_REL
DEAD_LETTER_DRAINING_GLOB = projection_queue.DEAD_LETTER_DRAINING_GLOB
AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES = projection_queue.AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES
SPOOL_DEPTH_SUMMARY_LIMIT = projection_queue.SPOOL_DEPTH_SUMMARY_LIMIT
_REQUEUE_FLUSH_SIZE = 1_000
REPLAY_LOCK_REL = Path(".task-state") / "projection-replay.lock"
DEAD_LETTER_REPLAY_LOCK_REL = Path(".task-state") / "projection-dead-letter-replay.lock"
# Projection kinds the replay handler can re-issue through the handoff CLI. A
# dead-letter row of any other kind can never be applied, so the dead-letter drain
# tombstone-discards it rather than re-dead-lettering it into a loop.
_SUPPORTED_REPLAY_KINDS = frozenset(
    {"test_result", "state_sync", "decision", "workflow_intent"}
)


@contextlib.contextmanager
def _drain_lock(repo: Path, *, lock_rel: Path = REPLAY_LOCK_REL) -> Iterator[bool]:
    """Best-effort exclusive lock serialising concurrent drainers.

    ``maybe_auto_drain`` spawns a detached replay before nearly every lifecycle
    command, and parallel agent/worktree sessions run commands concurrently, so
    multiple replay processes coexist. Without exclusion two drainers re-claim and
    rewrite the SAME ``.draining`` snapshot and last-writer-wins clobbers the
    other's remainder (today only CLI-side ``--event-id`` idempotency rescues
    correctness; the remainder file's durability is unprotected). A non-blocking
    ``flock`` lets only one drainer touch the spool at a time; the loser no-ops
    (the holder drains it). ``flock`` releases automatically when the process
    exits, so a crashed drainer never strands a stale lock. Yields ``True`` when
    the caller holds the lock, ``False`` when another drainer already does.
    ``lock_rel`` selects the lock file so the live-spool and dead-letter drains
    serialise independently (each single-writer to its own accumulator).
    """
    lock_path = repo / lock_rel
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError:
        # Cannot even create the lock file: fail open and drain anyway rather than
        # wedge the queue (no worse than the pre-lock behaviour).
        yield True
        return
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Genuine contention: another drainer already holds the lock. No-op.
            yield False
            return
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                yield False
                return
            # The filesystem does not support flock (ENOLCK / EINVAL / EOPNOTSUPP,
            # e.g. a foreign/network mount -- plausible here given the cross-device
            # EXDEV fallback _move_file already carries). Fail OPEN and drain anyway
            # rather than wedge the queue forever (every drainer permanently no-oping
            # would let the live spool grow unbounded -- the exact OOM/disk-full this
            # task prevents). Mirrors the os.open fail-open branch above; an unlocked
            # drain is no worse than the pre-lock behaviour.
            yield True
            return
        try:
            yield True
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def live_spool_size(repo: Path) -> int:
    """Return live projection spool size without reading the spool."""
    return projection_queue.live_spool_size(repo)


def spool_depth(repo: Path, *, max_count: int | None = None) -> int:
    """Count non-empty lines in the live projection spool."""
    spool = repo / PENDING_REL
    if not spool.is_file():
        return 0
    count = 0
    with spool.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            count += 1
            if max_count is not None and count >= max_count:
                return count
    return count


def spool_depth_summary(
    repo: Path, *, limit: int = SPOOL_DEPTH_SUMMARY_LIMIT
) -> tuple[int, bool]:
    """Return ``(depth, capped)`` using at most ``limit + 1`` non-empty lines."""
    health = projection_queue.spool_health(repo, depth_limit=limit)
    return health.live_depth, health.live_depth_capped


def has_drainable_spool(repo: Path) -> bool:
    """True when a live spool or orphan ``.draining.*`` snapshot exists."""
    # Stat-only probe: skip the bounded depth scan (size + glob is enough).
    return projection_queue.spool_health(repo, include_depth=False).has_drainable_spool


def auto_drain_skip_reason(repo: Path) -> str | None:
    """Return why detached auto-drain should not claim the current spool."""
    # Stat-only probe: the skip reason needs only file sizes, never depth.
    return projection_queue.spool_health(
        repo,
        auto_drain_max_bytes=AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
        include_depth=False,
    ).auto_drain_skip_reason


def _iter_entries(path: Path) -> Iterator[dict[str, Any]]:
    yield from projection_queue.iter_jsonl_entries(path)


def _iter_replay_entries(path: Path) -> Iterator[tuple[dict[str, Any] | None, bool]]:
    """Yield replay payloads, unwrapping dead-letter rows when present."""
    if not path.is_file():
        return
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                yield None, True
                continue
            if not isinstance(raw, dict):
                yield None, True
                continue
            payload = raw.get("payload")
            if isinstance(payload, dict):
                yield payload, False
            else:
                yield raw, False


def _fsync_parent(path: Path) -> None:
    try:
        fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_entries_atomic(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, sort_keys=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_parent(path)


def _append_entries_atomic(path: Path, entries: list[dict[str, Any]]) -> None:
    """Append entries to a JSONL file and fsync, accumulating across writers.

    Used to merge per-claim remainders into a single live-path checkpoint without
    the os.replace-overwrite of _write_entries_atomic clobbering an earlier claim's
    remainder (silent data loss) when one pass drains multiple claimed snapshots.
    """
    if not entries:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, sort_keys=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_parent(path)


def _bounded_checkpoint_path(
    claim_path: Path,
    *,
    input_path: Path | None,
    checkpoint_path: Path | None,
) -> Path | None:
    if checkpoint_path is not None:
        return checkpoint_path
    if input_path is not None:
        return claim_path.with_suffix(claim_path.suffix + ".checkpoint")
    return None


def _claim_snapshots(repo: Path) -> list[Path]:
    """Claim orphan draining files first, then atomically rename the live spool."""
    state_dir = repo / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    claimed: list[Path] = []
    for orphan in sorted(state_dir.glob(DRAINING_GLOB)):
        if orphan.is_file():
            claimed.append(orphan)
    live = repo / PENDING_REL
    if live.is_file():
        claim = state_dir / f"pending-workflow-events.jsonl.draining.{os.getpid()}-{time.time_ns()}"
        try:
            os.rename(live, claim)
            claimed.append(claim)
        except OSError:
            pass
    return claimed


def _append_to_live_spool(repo: Path, entries: list[dict[str, Any]]) -> None:
    if not entries:
        return
    target = repo / PENDING_REL
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, sort_keys=False) + "\n")
        # fsync the re-appended remainder before any subsequent claim unlink so a
        # crash cannot drop it (mirrors the atomic checkpoint writers).
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_parent(target)


def _compact_state_sync(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Collapse state_sync rows to latest per task_ref while preserving order."""
    latest_index_by_task: dict[str, int] = {}
    compacted = 0
    for index, entry in enumerate(entries):
        if entry.get("kind") != "state_sync":
            continue
        task_ref = entry.get("task_ref")
        if not isinstance(task_ref, str) or not task_ref:
            continue
        if task_ref in latest_index_by_task:
            compacted += 1
        latest_index_by_task[task_ref] = index
    keep_state_sync_indexes = set(latest_index_by_task.values())
    output: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        if entry.get("kind") == "state_sync" and index not in keep_state_sync_indexes:
            continue
        output.append(entry)
    return output, compacted


# Reserved bookkeeping key stamped on a re-spooled entry to track how many times
# the backend has loudly rejected it; popped before replay and excluded from the
# dedup identity so it never reaches the handoff CLI or perturbs idempotency.
_REPLAY_ATTEMPTS_KEY = "_replay_attempts"


def _entry_attempts(entry: dict[str, Any]) -> int:
    raw = entry.get(_REPLAY_ATTEMPTS_KEY)
    return raw if isinstance(raw, int) and raw >= 0 else 0


def _without_attempts(entry: dict[str, Any]) -> dict[str, Any]:
    if _REPLAY_ATTEMPTS_KEY not in entry:
        return entry
    return {k: v for k, v in entry.items() if k != _REPLAY_ATTEMPTS_KEY}


def _dedup_key(entry: dict[str, Any]) -> str:
    """Replay-local dedup identity, ignoring the retry-attempt bookkeeping key."""
    return projection_queue.event_idempotency_key(_without_attempts(entry))


def _drain_receipt(
    *,
    drained: int,
    pending_remaining: int,
    replay_results: list[dict[str, str]],
    handoff_projection: str,
    skipped_reason: str | None = None,
    checkpoint_path: str | None = None,
    skipped_corrupt: int = 0,
    skipped_duplicate: int = 0,
    skipped_unsupported: int = 0,
    compacted: int = 0,
    dead_lettered: int = 0,
) -> dict[str, Any]:
    """Assemble the single canonical drain/replay receipt.

    One builder so a new counter is added in exactly one place instead of being
    hand-assembled at every return site (the Shotgun-Surgery hazard the split
    bounded/unbounded drain copies created).
    """
    return {
        "drained": drained,
        "pending_remaining": pending_remaining,
        "replay_results": replay_results,
        "handoff_projection": handoff_projection,
        "skipped_reason": skipped_reason,
        "checkpoint_path": checkpoint_path,
        "skipped_corrupt": skipped_corrupt,
        "skipped_duplicate": skipped_duplicate,
        "skipped_unsupported": skipped_unsupported,
        "compacted": compacted,
        "dead_lettered": dead_lettered,
    }


def _drain(
    repo: Path,
    claimed_files: list[Path],
    *,
    max_entries: int | None,
    max_seconds: float | None,
    input_path: Path | None,
    checkpoint_path: Path | None,
) -> dict[str, Any]:
    """Replay claimed spool snapshots through the canonical handoff CLI.

    Single drain loop for both the bounded (``--max-entries`` / ``--max-seconds``)
    and unbounded forms: the unbounded form is simply ``max_entries=None`` and
    ``max_seconds=None``, so the budget/time guards are inert rather than a
    duplicated ~100-line body. ``--input`` accounts ``pending_remaining`` against
    the input itself; the live-spool form re-reads the spool depth so concurrent
    foreground appends are reflected. A ``pending`` (CLI-unreachable) entry aborts
    the whole pass with the remainder kept as the durable claim; budget/time
    exhaustion stops after the current claim; a ``spooled`` (loud rejection) entry
    parks this claim's remainder and the pass continues to the next claim.
    """
    drained = 0
    replay_results: list[dict[str, str]] = []
    saw_spooled = False
    budget_left = max_entries
    started = time.monotonic()
    skipped_corrupt = 0
    skipped_duplicate = 0
    skipped_unsupported = 0
    compacted_total = 0
    dead_lettered = 0
    checkpoint_written: str | None = None
    skipped_reason: str | None = None
    seen_keys: set[str] = set()
    remaining: list[dict[str, Any]] = []
    stop = False
    unprocessed_claims: list[Path] = []
    max_attempts = projection_queue.max_replay_attempts()

    for claim_index, claim_path in enumerate(claimed_files):
        raw_entries: list[dict[str, Any]] = []
        for entry, corrupt in _iter_replay_entries(claim_path):
            if corrupt or entry is None:
                skipped_corrupt += 1
                continue
            raw_entries.append(entry)
        entries, compacted = _compact_state_sync(raw_entries)
        compacted_total += compacted
        remaining = []
        index = 0
        while index < len(entries):
            entry = entries[index]
            if budget_left is not None and budget_left <= 0:
                remaining.extend(entries[index:])
                skipped_reason = "max_entries_exhausted"
                stop = True
                break
            if max_seconds is not None and (time.monotonic() - started) >= max_seconds:
                remaining.extend(entries[index:])
                skipped_reason = "max_seconds_exceeded"
                stop = True
                break
            key = _dedup_key(entry)
            if key in seen_keys:
                skipped_duplicate += 1
                replay_results.append(
                    {"kind": str(entry.get("kind", "unknown")), "status": "skipped_duplicate"}
                )
                index += 1
                continue
            status = _replay_entry(repo, entry)
            replay_results.append(
                {"kind": str(entry.get("kind", "unknown")), "status": status}
            )
            if budget_left is not None:
                budget_left -= 1
            if status == "synced":
                drained += 1
                seen_keys.add(key)
                index += 1
                continue
            if status == "unsupported":
                skipped_unsupported += 1
                seen_keys.add(key)
                index += 1
                continue
            if status == "pending":
                # Abort the whole pass: keep this entry and everything after it as
                # the durable claim (or a checkpoint beside an --input) and never
                # re-append to the live spool, so a down backend cannot rewrite it.
                remaining.extend(entries[index:])
                if input_path is None:
                    _write_entries_atomic(claim_path, remaining)
                    checkpoint_written = None
                else:
                    destination = _bounded_checkpoint_path(
                        claim_path,
                        input_path=input_path,
                        checkpoint_path=checkpoint_path,
                    )
                    _write_entries_atomic(destination, remaining)
                    checkpoint_written = str(destination)
                return _drain_receipt(
                    drained=drained,
                    pending_remaining=len(remaining),
                    replay_results=replay_results,
                    handoff_projection="pending",
                    skipped_reason="handoff_cli_unreachable",
                    checkpoint_path=checkpoint_written,
                    skipped_corrupt=skipped_corrupt,
                    skipped_duplicate=skipped_duplicate,
                    skipped_unsupported=skipped_unsupported,
                    compacted=compacted_total,
                    dead_lettered=dead_lettered,
                )
            # status == "spooled": a loud per-payload rejection. Bump this entry's
            # attempt count; once it exceeds the retry budget, dead-letter it and
            # continue so one poison entry cannot head-of-line-block the rest.
            saw_spooled = True
            attempts = _entry_attempts(entry) + 1
            if attempts > max_attempts:
                parked = projection_queue.dead_letter(
                    repo, _without_attempts(entry), reason="max_retry_exceeded"
                )
                if not parked:
                    # Sink is at its hard cap: do NOT drop the entry from the
                    # durable claim (parked nowhere). Abort the pass as pending so
                    # the claim stays intact until the operator drains the sink
                    # (DDIA: never ACK an entry before it is durably parked).
                    replay_results[-1] = {
                        "kind": str(entry.get("kind", "unknown")),
                        "status": "pending",
                    }
                    remaining.extend(entries[index:])
                    if input_path is None:
                        _write_entries_atomic(claim_path, remaining)
                        checkpoint_written = None
                    else:
                        destination = _bounded_checkpoint_path(
                            claim_path,
                            input_path=input_path,
                            checkpoint_path=checkpoint_path,
                        )
                        _write_entries_atomic(destination, remaining)
                        checkpoint_written = str(destination)
                    return _drain_receipt(
                        drained=drained,
                        pending_remaining=len(remaining),
                        replay_results=replay_results,
                        handoff_projection="pending",
                        skipped_reason="dead_letter_sink_full",
                        checkpoint_path=checkpoint_written,
                        skipped_corrupt=skipped_corrupt,
                        skipped_duplicate=skipped_duplicate,
                        skipped_unsupported=skipped_unsupported,
                        compacted=compacted_total,
                        dead_lettered=dead_lettered,
                    )
                dead_lettered += 1
                replay_results[-1] = {
                    "kind": str(entry.get("kind", "unknown")),
                    "status": "dead_lettered",
                }
                seen_keys.add(key)
                index += 1
                continue
            # Under the retry budget: keep it spooled with the bumped count and move
            # on, so entries behind it still get attempted this pass.
            entry[_REPLAY_ATTEMPTS_KEY] = attempts
            remaining.append(entry)
            index += 1

        if remaining:
            destination = _bounded_checkpoint_path(
                claim_path,
                input_path=input_path,
                checkpoint_path=checkpoint_path,
            )
            if destination is not None:
                if checkpoint_written == str(destination):
                    # A prior claim this pass already wrote this checkpoint; append
                    # so claim N's remainder cannot clobber claim N-1's (the
                    # os.replace overwrite was silent data loss across claims).
                    _append_entries_atomic(destination, remaining)
                else:
                    _write_entries_atomic(destination, remaining)
                checkpoint_written = str(destination)
            else:
                _append_to_live_spool(repo, remaining)
        if input_path is None:
            claim_path.unlink(missing_ok=True)
        if stop:
            unprocessed_claims = claimed_files[claim_index + 1:]
            break

    if input_path is not None:
        # Account against the input itself, not the unrelated live spool, so the
        # receipt and the sink-truncation gate reflect actual input consumption.
        pending_remaining = len(remaining)
    else:
        pending_remaining = spool_depth(repo, max_count=SPOOL_DEPTH_SUMMARY_LIMIT + 1)
        if checkpoint_written is not None:
            pending_remaining += sum(
                1 for _ in projection_queue.iter_jsonl_entries(Path(checkpoint_written))
            )
        # A budget/time stop leaves the claims after the stopping one unprocessed on
        # disk as .draining orphans; count them so pending_remaining is not a silent
        # floor (they live outside the live spool and any checkpoint).
        for orphan in unprocessed_claims:
            pending_remaining += sum(
                1 for _ in projection_queue.iter_jsonl_entries(orphan)
            )
    if pending_remaining == 0:
        handoff_projection = "synced"
    elif saw_spooled:
        handoff_projection = "spooled"
    else:
        handoff_projection = "pending"
    return _drain_receipt(
        drained=drained,
        pending_remaining=pending_remaining,
        replay_results=replay_results,
        handoff_projection=handoff_projection,
        skipped_reason=skipped_reason,
        checkpoint_path=checkpoint_written,
        skipped_corrupt=skipped_corrupt,
        skipped_duplicate=skipped_duplicate,
        skipped_unsupported=skipped_unsupported,
        compacted=compacted_total,
        dead_lettered=dead_lettered,
    )


def _build_test_result_argv(
    repo: Path, payload: dict[str, Any]
) -> list[str]:
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "event",
        "--event-kind", "test_result",
        "--session", str(payload.get("session", "")),
        "--command", str(payload.get("command", "")),
    )
    argv.extend(["--event-id", _projection_event_id(payload)])
    task_ref = payload.get("task_ref")
    if isinstance(task_ref, str) and task_ref:
        argv.extend(["--task-ref", task_ref])
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    if payload.get("passed"):
        argv.append("--passed")
    exit_code = payload.get("exit_code")
    if exit_code is not None:
        argv.extend(["--exit-code", str(exit_code)])
    result = payload.get("result")
    if result:
        argv.extend(["--result", str(result)])
    return argv


def _build_state_sync_argv(repo: Path, payload: dict[str, Any]) -> list[str]:
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "set",
        "--task-ref", str(payload.get("task_ref", "")),
        "--target-branch", str(payload.get("target_branch", "")),
        "--target-worktree-path", str(payload.get("target_worktree_path", "")),
        "--status", str(payload.get("status") or "in_progress"),
    )
    argv.extend(["--event-id", _projection_event_id(payload)])
    objective = payload.get("objective")
    if isinstance(objective, str):
        argv.extend(["--objective", objective])
    task_plan_path = payload.get("task_plan_path")
    if isinstance(task_plan_path, str) and task_plan_path:
        argv.extend(["--task-plan-path", task_plan_path])
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    return argv


def _build_decision_argv(repo: Path, payload: dict[str, Any]) -> list[str]:
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "event",
        "--event-kind", "decision",
        "--session", str(payload.get("session", "")),
        "--decision", str(payload.get("decision_id", "")),
        "--rationale", str(payload.get("rationale", "")),
    )
    argv.extend(["--event-id", _projection_event_id(payload)])
    task_ref = payload.get("task_ref")
    if isinstance(task_ref, str) and task_ref:
        argv.extend(["--task-ref", task_ref])
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    return argv


def _build_workflow_intent_argv(repo: Path, payload: dict[str, Any]) -> list[str]:
    decision_id = str(payload.get("decision_id") or _projection_event_id(payload))
    skill = str(payload.get("skill") or "")
    doc = str(payload.get("doc") or "")
    argv: list[str] = _common.handoff_command_argv(
        repo,
        "event",
        "--event-kind", "decision",
        "--session", decision_id,
        "--decision", decision_id,
        "--rationale", f"invoke /{skill} for {doc}",
    )
    argv.extend(["--event-id", _projection_event_id(payload)])
    task_ref = payload.get("task_ref")
    if isinstance(task_ref, str) and task_ref:
        argv.extend(["--task-ref", task_ref])
    branch = payload.get("branch")
    if isinstance(branch, str) and branch:
        argv.extend(["--branch", branch])
    commit_sha = payload.get("commit_sha")
    if isinstance(commit_sha, str) and commit_sha:
        argv.extend(["--commit-sha", commit_sha])
    return argv


def _projection_event_id(payload: dict[str, Any]) -> str:
    event_id = payload.get("event_id")
    if isinstance(event_id, str) and event_id:
        return event_id
    return projection_queue.event_idempotency_key(payload)


_CLI_UNREACHABLE_RETURNCODES = frozenset({124, 127})


def _replay_entry(repo: Path, entry: dict[str, Any]) -> str:
    kind = entry.get("kind")
    if kind == "test_result":
        argv = _build_test_result_argv(repo, entry)
    elif kind == "state_sync":
        argv = _build_state_sync_argv(repo, entry)
    elif kind == "decision":
        argv = _build_decision_argv(repo, entry)
    elif kind == "workflow_intent":
        argv = _build_workflow_intent_argv(repo, entry)
    else:
        parked = projection_queue.dead_letter(
            repo, entry, reason="unsupported_projection_kind"
        )
        if not parked:
            # The dead-letter sink is at its hard cap so parking was shed. Do NOT
            # ack (drop) the entry from the durable claim -- that would delete it
            # from the one place it is safely stored and park it nowhere. Abort
            # the pass as pending so the claim stays intact until the operator
            # drains the dead-letter sink (DDIA: never ACK before durable park).
            return "pending"
        return "unsupported"
    proc = _common.run_subprocess(argv, timeout=_common.handoff_timeout())
    # Mirror projection._classify_returncode: a negative returncode means the CLI
    # child was signal-killed (SIGKILL/OOM -> -9, SIGTERM -> -15) -- genuine
    # unreachability under the memory pressure this change guards against, not a
    # logical ok:false rejection. It must abort the pass as "pending" (claim left
    # intact, no re-write) the same as 124/127, never be misread as "spooled".
    if proc.returncode < 0 or proc.returncode in _CLI_UNREACHABLE_RETURNCODES:
        return "pending"
    if proc.returncode != 0:
        return "spooled"
    import projection  # lazy import: avoid a handlers<->projection import cycle

    rejection = projection._rejection_data(proc.stdout)
    if rejection is None or rejection.get("current_revision") is not None:
        return "synced"
    return "spooled"


def drain_spool(
    repo: Path,
    *,
    max_entries: int | None = None,
    max_seconds: float | None = None,
    input_path: Path | None = None,
    checkpoint_path: Path | None = None,
) -> dict[str, Any]:
    """Drain claimed spool snapshots; return counters for receipts and status."""
    # Operator-driven --input replays a specific file and never claims the live
    # spool or orphan snapshots, so it neither needs nor takes the drain lock.
    if input_path is not None:
        return _drain(
            repo,
            [input_path],
            max_entries=max_entries,
            max_seconds=max_seconds,
            input_path=input_path,
            checkpoint_path=checkpoint_path,
        )

    if projection_queue.breaker_state(repo).state == "open":
        return _drain_receipt(
            drained=0,
            pending_remaining=spool_depth(repo, max_count=1),
            replay_results=[],
            handoff_projection="pending",
            skipped_reason="projection_breaker_open",
        )

    # Live-spool path: serialise concurrent drainers so two passes cannot re-claim
    # and clobber the same snapshot's remainder.
    with _drain_lock(repo) as acquired:
        if not acquired:
            depth = spool_depth(repo, max_count=SPOOL_DEPTH_SUMMARY_LIMIT + 1)
            return _drain_receipt(
                drained=0,
                pending_remaining=depth,
                replay_results=[],
                handoff_projection="synced" if depth == 0 else "pending",
                skipped_reason="projection_replay_locked",
            )
        claimed_files = _claim_snapshots(repo)
        if not claimed_files:
            return _drain_receipt(
                drained=0,
                pending_remaining=spool_depth(repo),
                replay_results=[],
                handoff_projection="synced",
            )
        return _drain(
            repo,
            claimed_files,
            max_entries=max_entries,
            max_seconds=max_seconds,
            input_path=input_path,
            checkpoint_path=checkpoint_path,
        )


def _claim_dead_letter_snapshots(repo: Path) -> list[Path]:
    """Reclaim orphan dead-letter drain snapshots, then claim the live sink.

    Sibling of :func:`_claim_snapshots` for the dead-letter accumulator. Orphan
    ``.draining.*`` files (a crash after a prior claim, or a stranded remainder
    from an older build) are picked up FIRST so nothing is left behind, then the
    live sink is claimed by rename so a concurrent ``dead_letter`` append lands on
    a fresh file. Must run under the dead-letter drain lock so a live drainer's
    in-flight claim is never mistaken for an orphan.
    """
    state_dir = repo / ".task-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    claimed: list[Path] = []
    for orphan in sorted(state_dir.glob(DEAD_LETTER_DRAINING_GLOB)):
        if orphan.is_file():
            claimed.append(orphan)
    sink = repo / DEAD_LETTER_REL
    if sink.is_file():
        claim = state_dir / (
            f"projection-dead-letter.jsonl.draining.{os.getpid()}-{time.time_ns()}"
        )
        try:
            os.rename(sink, claim)
            claimed.append(claim)
        except OSError:
            pass
    return claimed


def _append_dead_letter_entries(repo: Path, entries: list[dict[str, Any]]) -> None:
    """Re-append un-drained dead-letter rows to the live sink (fsync'd).

    Mirrors :func:`_append_to_live_spool` for the dead-letter accumulator: a
    partial drain's remainder returns to the live ``projection-dead-letter.jsonl``
    -- which ``dead_letter_count``/status/doctor read and the next backoff-gated
    pass re-claims -- instead of an orphan checkpoint nothing ever re-reads. The
    append is unconditional (never sheds on the hard cap the way ``dead_letter``
    does): these rows were already durably parked, so dropping them here would be
    the data loss this drain exists to prevent.
    """
    if not entries:
        return
    target = repo / DEAD_LETTER_REL
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, sort_keys=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_parent(target)


def _drain_dead_letter(
    repo: Path,
    claimed_files: list[Path],
    *,
    max_entries: int | None,
) -> dict[str, Any]:
    """Replay claimed dead-letter snapshots; re-append the remainder to the sink.

    Terminal-status handling so an entry can neither be stranded nor cycle forever:

    - archived-task rows (``_task_is_archived``) and unsupported-kind rows can
      never apply -> tombstone-discard with a logged line;
    - a row the backend keeps loudly rejecting (``spooled``) bumps a replay-attempt
      count and is tombstone-discarded once it exceeds the budget;
    - a backend-unreachable (``pending``) row aborts the pass, its remainder and
      the current claim's tail re-appended to the live sink for a later retry;
    - a synced row is drained.

    Every processed claim's remainder is re-appended to the live sink and the claim
    is unlinked (no 0-byte orphan). A budget/pending stop leaves later claims on
    disk as orphans that the next pass reclaims.
    """
    drained = 0
    tombstoned = 0
    replay_results: list[dict[str, str]] = []
    budget_left = max_entries
    seen_keys: set[str] = set()
    max_attempts = projection_queue.max_replay_attempts()
    live_refs = _common._live_task_refs(repo)
    archived_cache: dict[str, bool] = {}
    saw_spooled = False
    saw_pending = False
    stop = False

    for claim_path in claimed_files:
        raw_entries: list[dict[str, Any]] = []
        for entry, corrupt in _iter_replay_entries(claim_path):
            if corrupt or entry is None:
                # A corrupt dead-letter line can never replay; drop it so it does
                # not pin the sink non-empty forever (it is already the graveyard).
                continue
            raw_entries.append(entry)
        entries, _compacted = _compact_state_sync(raw_entries)
        remaining: list[dict[str, Any]] = []
        index = 0
        while index < len(entries):
            entry = entries[index]
            if budget_left is not None and budget_left <= 0:
                remaining.extend(entries[index:])
                stop = True
                break
            task_ref = entry.get("task_ref")
            if isinstance(task_ref, str) and task_ref and task_ref not in live_refs:
                is_archived = archived_cache.get(task_ref)
                if is_archived is None:
                    is_archived = _common._task_is_archived(repo, task_ref)
                    archived_cache[task_ref] = is_archived
                if is_archived:
                    tombstoned += 1
                    replay_results.append(
                        {"kind": str(entry.get("kind", "unknown")),
                         "status": "tombstoned_archived"}
                    )
                    sys.stderr.write(
                        "projection dead-letter drain: discarding event for "
                        f"archived task {task_ref} (tombstoned; no live row to "
                        "apply it to).\n"
                    )
                    index += 1
                    continue
            kind = entry.get("kind")
            if kind not in _SUPPORTED_REPLAY_KINDS:
                tombstoned += 1
                replay_results.append(
                    {"kind": str(kind), "status": "tombstoned_unsupported"}
                )
                sys.stderr.write(
                    "projection dead-letter drain: discarding unsupported-kind "
                    f"event (kind={kind!r}; cannot be replayed).\n"
                )
                index += 1
                continue
            key = _dedup_key(entry)
            if key in seen_keys:
                replay_results.append({"kind": str(kind), "status": "skipped_duplicate"})
                index += 1
                continue
            status = _replay_entry(repo, entry)
            if budget_left is not None:
                budget_left -= 1
            if status == "synced":
                drained += 1
                seen_keys.add(key)
                replay_results.append({"kind": str(kind), "status": "synced"})
                index += 1
                continue
            if status == "pending":
                # Backend unreachable: abort the pass, keep this entry and the tail
                # as the durable remainder (re-appended to the live sink below).
                remaining.extend(entries[index:])
                replay_results.append({"kind": str(kind), "status": "pending"})
                saw_pending = True
                stop = True
                break
            # status == "spooled": a loud per-payload rejection. This row is already
            # in the terminal accumulator, so re-dead-lettering it would loop; bump
            # its attempt count and tombstone-discard once the budget is exceeded.
            saw_spooled = True
            attempts = _entry_attempts(entry) + 1
            if attempts > max_attempts:
                tombstoned += 1
                seen_keys.add(key)
                replay_results.append(
                    {"kind": str(kind), "status": "tombstoned_exhausted"}
                )
                sys.stderr.write(
                    "projection dead-letter drain: discarding event after "
                    f"{attempts - 1} rejected replays (kind={kind!r}; "
                    "backend keeps refusing it).\n"
                )
                index += 1
                continue
            entry[_REPLAY_ATTEMPTS_KEY] = attempts
            replay_results.append({"kind": str(kind), "status": "spooled"})
            remaining.append(entry)
            index += 1

        # Re-append this claim's remainder to the LIVE sink (durable, re-claimable
        # next pass), then unlink the fully-processed claim so no orphan is left.
        _append_dead_letter_entries(repo, remaining)
        claim_path.unlink(missing_ok=True)
        if stop:
            break

    # pending_remaining reflects everything still parked: re-appended remainder now
    # in the live sink plus any claims left unprocessed after a budget/pending stop.
    live_remaining, _ = projection_queue.dead_letter_count(repo)
    orphan_remaining, _ = projection_queue.dead_letter_orphan_count(repo)
    pending_remaining = live_remaining + orphan_remaining
    if pending_remaining == 0:
        handoff_projection = "synced"
    elif saw_pending:
        handoff_projection = "pending"
    elif saw_spooled:
        handoff_projection = "spooled"
    else:
        handoff_projection = "pending"
    return {
        "drained": drained,
        "tombstoned": tombstoned,
        "pending_remaining": pending_remaining,
        "reclaimed_claims": len(claimed_files),
        "replay_results": replay_results,
        "handoff_projection": handoff_projection,
        "skipped_reason": None,
    }


def drain_dead_letter_sink(
    repo: Path, *, max_entries: int | None = None
) -> dict[str, Any]:
    """Reclaim and drain the dead-letter sink; re-append the remainder to it.

    Sibling of :func:`drain_spool` for the dead-letter accumulator. Serialised by a
    dedicated lock so concurrent detached drainers cannot double-reclaim the same
    orphan snapshot. Returns counters for the receipt and for tests.
    """
    with _drain_lock(repo, lock_rel=DEAD_LETTER_REPLAY_LOCK_REL) as acquired:
        if not acquired:
            live, _ = projection_queue.dead_letter_count(repo)
            orphan, _ = projection_queue.dead_letter_orphan_count(repo)
            depth = live + orphan
            return {
                "drained": 0,
                "tombstoned": 0,
                "pending_remaining": depth,
                "reclaimed_claims": 0,
                "replay_results": [],
                "handoff_projection": "synced" if depth == 0 else "pending",
                "skipped_reason": "dead_letter_replay_locked",
            }
        claimed = _claim_dead_letter_snapshots(repo)
        if not claimed:
            return {
                "drained": 0,
                "tombstoned": 0,
                "pending_remaining": 0,
                "reclaimed_claims": 0,
                "replay_results": [],
                "handoff_projection": "synced",
                "skipped_reason": None,
            }
        return _drain_dead_letter(repo, claimed, max_entries=max_entries)


def _emit(repo: Path, *, drained: int, pending_remaining: int,
          replay_results: list[dict[str, str]],
          handoff_projection: str,
          skipped_reason: str | None = None,
          checkpoint_path: str | None = None,
          skipped_corrupt: int = 0,
          skipped_duplicate: int = 0,
          skipped_unsupported: int = 0,
          compacted: int = 0,
          dead_lettered: int = 0) -> None:
    branch = resolver.current_branch(repo) or ""
    head = resolver.head_sha(repo) or ""
    derived_task_ref = resolver.derive_task_ref(
        branch, known_task_refs=_common._live_task_refs(repo)
    )
    receipt = {
        "ok": True,
        "command": "project-events-replay",
        "task_ref": derived_task_ref,
        "branch": branch,
        "worktree_path": str(repo),
        "head": head,
        "handoff_projection": handoff_projection,
        "events": ["events_replayed"],
        "drained": drained,
        "pending_remaining": pending_remaining,
        "replay_results": replay_results,
        "skipped_corrupt": skipped_corrupt,
        "skipped_duplicate": skipped_duplicate,
        "skipped_unsupported": skipped_unsupported,
        "compacted": compacted,
        "dead_lettered": dead_lettered,
    }
    if skipped_reason is not None:
        receipt["skipped_reason"] = skipped_reason
    if checkpoint_path is not None:
        receipt["checkpoint_path"] = checkpoint_path
    _common.emit(receipt)


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="lifecycle project-events-replay", add_help=True
    )
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument(
        "--max-entries",
        dest="max_entries",
        type=int,
        default=None,
        help="Optional cap on entries drained in this pass (auto-drain budget).",
    )
    parser.add_argument(
        "--max-seconds",
        dest="max_seconds",
        type=float,
        default=None,
        help="Optional wall-clock budget for replay attempts.",
    )
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Replay a specific JSONL input such as a quarantined payload or dead-letter file.",
    )
    parser.add_argument(
        "--checkpoint",
        dest="checkpoint_path",
        default=None,
        help="Write unattempted or rejected remainder rows to this checkpoint JSONL.",
    )
    parser.add_argument(
        "--drain-dead-letter",
        dest="drain_dead_letter",
        action="store_true",
        default=False,
        help=(
            "Reclaim and drain the dead-letter sink (and any orphan drain "
            "snapshots), re-appending the remainder to the live sink."
        ),
    )
    parser.add_argument(
        "--quarantine-oversized",
        dest="quarantine_oversized",
        action="store_true",
        default=False,
        help="Move an oversized live projection spool out of the hot path.",
    )
    parser.add_argument(
        "--soft-limit-bytes",
        dest="soft_limit_bytes",
        type=int,
        default=AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
        help="Live spool byte threshold used by --quarantine-oversized.",
    )
    parser.add_argument(
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Allow quarantine below --soft-limit-bytes or force quarantine payload purge.",
    )
    parser.add_argument(
        "--purge-quarantine-manifest",
        dest="purge_quarantine_manifest",
        default=None,
        help="Delete a quarantined payload named by its manifest path.",
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    # Maintenance receipts share the same provenance fields as the drain path.
    # These commands do not touch the online handoff projection at all, so the
    # projection status is reported as ``not_applicable`` rather than a
    # misleading ``synced``.
    maint_branch = resolver.current_branch(repo) or ""
    maint_task_ref = resolver.derive_task_ref(
        maint_branch, known_task_refs=_common._live_task_refs(repo)
    )
    maint_head = resolver.head_sha(repo) or ""
    if args.drain_dead_letter:
        outcome = drain_dead_letter_sink(repo, max_entries=args.max_entries)
        receipt = {
            "ok": True,
            "command": "project-events-replay",
            "task_ref": maint_task_ref,
            "branch": maint_branch,
            "worktree_path": str(repo),
            "head": maint_head,
            "handoff_projection": outcome["handoff_projection"],
            "events": ["dead_letter_events_replayed"],
            "drained": int(outcome.get("drained") or 0),
            "tombstoned": int(outcome.get("tombstoned") or 0),
            "reclaimed_claims": int(outcome.get("reclaimed_claims") or 0),
            "pending_remaining": int(outcome.get("pending_remaining") or 0),
            "replay_results": outcome.get("replay_results") or [],
        }
        if outcome.get("skipped_reason") is not None:
            receipt["skipped_reason"] = outcome["skipped_reason"]
        _common.emit(receipt)
        return 0

    if args.purge_quarantine_manifest:
        purge = projection_queue.purge_quarantined_payload(
            repo,
            Path(args.purge_quarantine_manifest),
            force=args.force,
        )
        receipt = {
            "ok": True,
            "command": "project-events-replay",
            "task_ref": maint_task_ref,
            "branch": maint_branch,
            "worktree_path": str(repo),
            "head": maint_head,
            "handoff_projection": "not_applicable",
            "events": ["projection_spool_quarantine_payload_purged"]
            if purge.get("purged")
            else ["projection_spool_quarantine_payload_purge_skipped"],
            "drained": 0,
            "pending_remaining": 0,
            "replay_results": [],
            "quarantine_purge": purge,
        }
        _common.emit(receipt)
        return 0

    if args.quarantine_oversized:
        quarantine = projection_queue.quarantine_live_spool(
            repo,
            soft_limit_bytes=args.soft_limit_bytes,
            force=args.force,
        )
        receipt = {
            "ok": True,
            "command": "project-events-replay",
            "task_ref": maint_task_ref,
            "branch": maint_branch,
            "worktree_path": str(repo),
            "head": maint_head,
            "handoff_projection": "not_applicable",
            "events": ["projection_spool_quarantined"]
            if quarantine.get("quarantined")
            else ["projection_spool_quarantine_skipped"],
            "drained": 0,
            "pending_remaining": 0,
            "replay_results": [],
            "quarantine": quarantine,
        }
        _common.emit(receipt)
        return 0

    input_path = Path(args.input_path) if args.input_path else None
    # Refuse a --input drain of an oversized payload regardless of --max-entries/
    # --max-seconds. BOTH drain_spool branches buffer the whole claim/input into a
    # list before replaying (the bounded form caps replays and wall-time, not RAM,
    # and state_sync compaction needs the full input in memory). So a quarantined
    # multi-GiB payload re-OOMs the host in either mode -- the crash this tooling
    # exists to prevent. The memory-safe recovery is to split the payload into
    # <= soft-limit chunks and replay each chunk bounded.
    if input_path is not None:
        try:
            input_size = input_path.stat().st_size
        except OSError:
            input_size = 0
        soft_limit = projection_queue._int_env(
            "WORKBAY_PROJECTION_AUTO_DRAIN_MAX_BYTES",
            AUTO_DRAIN_MAX_LIVE_SPOOL_BYTES,
        )
        if input_size > soft_limit:
            _common.emit(
                {
                    "ok": False,
                    "command": "project-events-replay",
                    "task_ref": maint_task_ref,
                    "branch": maint_branch,
                    "worktree_path": str(repo),
                    "head": maint_head,
                    "handoff_projection": "not_applicable",
                    "events": ["projection_replay_input_oversized_refused"],
                    "error": "input_replay_oversized",
                    "drained": 0,
                    "pending_remaining": 0,
                    "replay_results": [],
                    "input_size_bytes": input_size,
                    "soft_limit_bytes": soft_limit,
                    "recovery_command": (
                        f"split -C {soft_limit} -d {args.input_path} "
                        f"{args.input_path}.chunk. && for c in "
                        f"{args.input_path}.chunk.*; do make project-events-replay "
                        'LIFECYCLE_ARGS="--input $c --max-entries 500 '
                        '--checkpoint $c.checkpoint --json"; done'
                    ),
                }
            )
            return 2

    outcome = drain_spool(
        repo,
        max_entries=args.max_entries,
        max_seconds=args.max_seconds,
        input_path=input_path,
        checkpoint_path=Path(args.checkpoint_path) if args.checkpoint_path else None,
    )
    if input_path is not None and outcome.get("handoff_projection") == "synced":
        projection_queue.record_projection_success(repo)
        _write_entries_atomic(input_path, [])
    _emit(
        repo,
        drained=outcome["drained"],
        pending_remaining=outcome["pending_remaining"],
        replay_results=outcome["replay_results"],
        handoff_projection=outcome["handoff_projection"],
        skipped_reason=outcome.get("skipped_reason"),
        checkpoint_path=outcome.get("checkpoint_path"),
        skipped_corrupt=int(outcome.get("skipped_corrupt") or 0),
        skipped_duplicate=int(outcome.get("skipped_duplicate") or 0),
        skipped_unsupported=int(outcome.get("skipped_unsupported") or 0),
        compacted=int(outcome.get("compacted") or 0),
        dead_lettered=int(outcome.get("dead_lettered") or 0),
    )
    return 0
