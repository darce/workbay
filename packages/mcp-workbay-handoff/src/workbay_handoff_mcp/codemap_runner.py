"""Singleton codemap reindex runner (implementation note S2).

Acquires the handoff.db reindex lease (fcntl.flock mutual exclusion + generation
fencing [RES-10]), coalesces queued SHAs to the newest, runs
``codebase-memory-mcp cli index_repository`` once against the **live worktree**
(the SHA is a watermark/coalesce key, not a git checkout), and generation-
matched releases with ``lock_fd`` cleanup.

A second concurrent caller exits cleanly when the lease is held
(``status="held"``). That is not an error for coordination — the live holder
owns the work — but CLI callers map it to a distinct nonzero exit so
automation does not treat "deferred" as "indexed now" ([AGT-21]).

After a successful index+release, drains SHAs that arrived mid-run. The
no-progress budget (``MAX_DRAIN_ITERATIONS``) resets when an iteration shrinks
the queue; a queue that only grows or stalls under sustained traffic surfaces
as ``status="pending_remaining"`` (not ``ok``) so operators can tell full
drain from giving up with work left ([AGT-10], [OBS-08]).
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from workbay_handoff_mcp.codemap_lease import (
    RUNNER_TIMEOUT_SECONDS,
    acquire_reindex_lease,
    read_requested_shas,
    release_reindex_lease,
)
from workbay_handoff_mcp.shared_schema import connect_handoff_db

CODEMAP_CLI_NAME = "codebase-memory-mcp"
# Consecutive successful iterations that do not shrink the queue.
MAX_DRAIN_ITERATIONS = 3
# Absolute safety cap so a thrashing grow/shrink pattern cannot pin the process.
MAX_DRAIN_ABSOLUTE = 32

RunFn = Callable[..., subprocess.CompletedProcess[str]]
NowFn = Callable[[], float]
ReleaseFn = Callable[..., bool]
AcquireFn = Callable[..., Any]


@dataclass(frozen=True)
class ReindexRunResult:
    """Typed outcome of one ``run_reindex_once`` attempt. Never raised as an error."""

    status: str
    """One of: ok, held, empty, failed, timeout, fenced, cli_missing, pending_remaining."""

    target_sha: str = ""
    generation: int | None = None
    detail: str = ""
    released: bool | None = None
    remaining_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "target_sha": self.target_sha,
            "generation": self.generation,
            "detail": self.detail,
            "released": self.released,
        }
        if self.remaining_count is not None:
            payload["remaining_count"] = self.remaining_count
        return payload


def _resolve_codemap_cli() -> str | None:
    override = (os.environ.get("CODEBASE_MEMORY_MCP") or "").strip()
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
        return None
    found = shutil.which(CODEMAP_CLI_NAME)
    if found:
        return found
    local = Path.home() / ".local" / "bin" / CODEMAP_CLI_NAME
    if local.is_file() and os.access(local, os.X_OK):
        return str(local.resolve())
    return None


def _default_run(
    argv: Sequence[str],
    *,
    timeout: float,
    **_kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    # start_new_session=True puts the child in its own process group so a
    # timeout can kill the whole tree (grandchild index workers included),
    # not just the direct child ([RES-10] singleton must bound the timeout case).
    proc = subprocess.Popen(
        list(argv),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        # Reap the killed group so we do not leak a zombie, then re-raise so
        # _run_index_subprocess maps it to status="timeout".
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        raise
    return subprocess.CompletedProcess(list(argv), proc.returncode, stdout, stderr)


def _index_argv(repo_path: Path | str, *, cli_path: str | None = None) -> list[str] | None:
    """Build argv for live-tree index_repository.

    Note: the target SHA is never passed to the CLI. Indexing always walks the
    current worktree contents; queued SHAs are watermarks / coalesce keys only.
    """
    resolved = cli_path if cli_path is not None else _resolve_codemap_cli()
    if not resolved:
        return None
    body = json.dumps(
        {"repo_path": str(repo_path)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return [resolved, "cli", "index_repository", body]


def _read_lease_generation(db_path: Path | str, repo_instance_id: str) -> int | None:
    """Return the live lease generation, or None if no row."""
    conn = connect_handoff_db(db_path, read_only=True)
    try:
        row = conn.execute(
            "SELECT generation FROM codemap_reindex_lease WHERE repo_instance_id = ?",
            (repo_instance_id,),
        ).fetchone()
        if row is None:
            return None
        return int(row[0])
    finally:
        conn.close()


def _run_index_subprocess(
    run_fn: RunFn,
    argv: Sequence[str],
    *,
    target_sha: str,
    generation: int,
    timeout_seconds: float,
) -> ReindexRunResult:
    try:
        completed = run_fn(list(argv), timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        return ReindexRunResult(
            status="timeout",
            target_sha=target_sha,
            generation=generation,
            detail=f"index_repository timed out after {timeout_seconds}s: {exc}",
        )
    except FileNotFoundError as exc:
        return ReindexRunResult(
            status="cli_missing",
            target_sha=target_sha,
            generation=generation,
            detail=f"cli disappeared: {exc}",
        )
    except OSError as exc:
        return ReindexRunResult(
            status="failed",
            target_sha=target_sha,
            generation=generation,
            detail=f"os_error: {type(exc).__name__}: {exc}",
        )
    except Exception as exc:  # noqa: BLE001 — never raise into lifecycle callers
        return ReindexRunResult(
            status="failed",
            target_sha=target_sha,
            generation=generation,
            detail=f"reindex_error: {type(exc).__name__}: {exc}",
        )

    returncode = getattr(completed, "returncode", 1)
    if returncode is None or int(returncode) != 0:
        stderr = (getattr(completed, "stderr", None) or "")[:300]
        return ReindexRunResult(
            status="failed",
            target_sha=target_sha,
            generation=generation,
            detail=f"index_repository exit={returncode}: {stderr}",
        )
    return ReindexRunResult(
        status="ok",
        target_sha=target_sha,
        generation=generation,
        detail="reindex complete",
    )


def _finalize_result(
    result: ReindexRunResult,
    *,
    released: bool | None,
) -> ReindexRunResult:
    """Apply the finally-block release outcome onto the provisional result."""
    if released is None:
        return result
    if released is False:
        # Fenced out: report it. Queue must remain for the reclaiming holder.
        if result.status == "ok":
            return ReindexRunResult(
                status="fenced",
                target_sha=result.target_sha,
                generation=result.generation,
                detail=(result.detail + "; " if result.detail else "") + "release rejected (stale generation)",
                released=False,
            )
        return ReindexRunResult(
            status=result.status,
            target_sha=result.target_sha,
            generation=result.generation,
            detail=(result.detail + "; " if result.detail else "") + "release rejected (stale generation)",
            released=False,
        )
    return ReindexRunResult(
        status=result.status,
        target_sha=result.target_sha,
        generation=result.generation,
        detail=result.detail,
        released=True,
        remaining_count=result.remaining_count,
    )


def _with_drain_detail(
    result: ReindexRunResult,
    *,
    drain_iterations: int,
    remaining_count: int | None = None,
) -> ReindexRunResult:
    parts = [result.detail] if result.detail else []
    parts.append(f"drain_iterations={drain_iterations}")
    if remaining_count is not None:
        parts.append(f"remaining_count={remaining_count}")
    detail = "; ".join(p for p in parts if p)
    return ReindexRunResult(
        status=result.status,
        target_sha=result.target_sha,
        generation=result.generation,
        detail=detail,
        released=result.released,
        remaining_count=remaining_count if remaining_count is not None else result.remaining_count,
    )


def _run_reindex_attempt(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    repo_path: Path | str,
    run_fn: RunFn,
    now_ts: float | None,
    timeout_seconds: float,
    acquire_fn: AcquireFn,
    release_fn: ReleaseFn,
    cli_path: str | None,
    lock_dir: Path | None,
) -> ReindexRunResult:
    """One acquire → index → release cycle. Never raises.

    Calls the frozen lease contract:
    ``acquire_reindex_lease(db_path, *, repo_instance_id, lock_dir=, now_ts=)``
    → ``ReindexLease | None`` with ``lock_fd``;
    ``release_reindex_lease(..., lock_fd=)``.
    """
    pending_before = read_requested_shas(db_path, repo_instance_id=repo_instance_id)
    hint_sha = pending_before[-1] if pending_before else ""

    try:
        lease = acquire_fn(
            db_path,
            repo_instance_id=repo_instance_id,
            lock_dir=lock_dir,
            now_ts=now_ts,
        )
    except Exception as exc:  # noqa: BLE001
        return ReindexRunResult(
            status="failed",
            target_sha=hint_sha,
            detail=f"acquire_failed: {type(exc).__name__}: {exc}",
            released=False,
        )

    if lease is None:
        return ReindexRunResult(
            status="held",
            target_sha=hint_sha,
            detail="another holder is active",
            released=False,
        )

    # Coercion of lease fields MUST sit inside the try/finally below. A corrupt
    # generation (or lock_fd) that raised between acquire and try used to
    # escape with the flock held and violate "Never raises" ([AGT-10]).
    generation: int | None = None
    lock_fd: int | None = None
    provisional = ReindexRunResult(
        status="failed",
        detail="runner aborted before completion",
    )
    released: bool | None = None
    # Captured after acquire so release can consume exactly what this run saw.
    # Non-ok outcomes pass consumed_shas=None so the queue is fully retained.
    consumed: list[str] | None = None

    try:
        # Capture raw lock_fd *before* int() so a non-integral value still
        # reaches finally for unlock. Failed ``lock_fd = int(...)`` leaves the
        # prior (raw) binding in place; release then attempts unlock rather
        # than silently passing lock_fd=None ([AGT-10] residual leak).
        lock_fd = lease.lock_fd  # type: ignore[assignment]
        # Prefer lock_fd first so finally can unlock even if generation is bad.
        lock_fd = int(lock_fd) if lock_fd is not None else None
        generation = int(lease.generation)
        provisional = ReindexRunResult(
            status="failed",
            generation=generation,
            detail="runner aborted before completion",
        )
        pending = read_requested_shas(db_path, repo_instance_id=repo_instance_id)
        # Snapshot what this attempt is responsible for consuming on success.
        consumed_on_success = list(pending)
        if pending:
            # Newest wins: request_reindex appends; last entry is the latest SHA.
            target_sha = pending[-1]
        else:
            # Under-lease queue empty: another runner already consumed the
            # pre-acquire hint. Nothing to index — do not redundantly reindex a
            # stale hint while holding the lease. Falls through to status=empty.
            target_sha = ""

        if not target_sha:
            provisional = ReindexRunResult(
                status="empty",
                target_sha="",
                generation=generation,
                detail="no queued shas",
            )
            # Under-lease queue empty: consume nothing (consumed=[]). release
            # re-reads requested_shas inside BEGIN IMMEDIATE and computes
            # remaining = requested - consumed, DELETEing the row only when
            # remaining is empty (otherwise the placeholder UPDATE path). A sha
            # appended mid-index is preserved by that re-read, not dropped.
            consumed = []
        else:
            argv = _index_argv(repo_path, cli_path=cli_path)
            if argv is None:
                provisional = ReindexRunResult(
                    status="cli_missing",
                    target_sha=target_sha,
                    generation=generation,
                    detail=f"{CODEMAP_CLI_NAME} not on PATH",
                )
                consumed = None  # retain for retry
            else:
                # Defence-in-depth: re-check generation immediately before shelling
                # out. A fenced/stale holder must not index ([RES-10]).
                live_gen = _read_lease_generation(db_path, repo_instance_id)
                if live_gen is None or int(live_gen) != generation:
                    provisional = ReindexRunResult(
                        status="fenced",
                        target_sha=target_sha,
                        generation=generation,
                        detail="lease generation no longer held before index",
                    )
                    consumed = None  # not ours to clear
                else:
                    provisional = _run_index_subprocess(
                        run_fn,
                        argv,
                        target_sha=target_sha,
                        generation=generation,
                        timeout_seconds=float(timeout_seconds),
                    )
                    if provisional.status == "ok":
                        consumed = consumed_on_success
                    else:
                        # timeout / failed / cli_missing: retain the queue.
                        consumed = None
    except Exception as exc:  # noqa: BLE001 — never raise into lifecycle callers
        provisional = ReindexRunResult(
            status="failed",
            target_sha=provisional.target_sha or hint_sha,
            generation=generation,
            detail=f"runner_error: {type(exc).__name__}: {exc}",
        )
        # Do not claim consumption when the attempt aborted mid-flight.
        consumed = None
    finally:
        # Always release with our generation ([RES-10]). False means fenced —
        # leave the queue alone. consumed_shas=None means "consumed nothing".
        # generation=-1 when coercion failed: release fences the row but still
        # unlocks lock_fd so the flock is not leaked for process lifetime.
        try:
            released = bool(
                release_fn(
                    db_path,
                    repo_instance_id=repo_instance_id,
                    generation=generation if generation is not None else -1,
                    consumed_shas=consumed,
                    lock_fd=lock_fd,
                )
            )
        except Exception as exc:  # noqa: BLE001
            released = False
            provisional = ReindexRunResult(
                status=provisional.status,
                target_sha=provisional.target_sha,
                generation=generation,
                detail=(provisional.detail + "; " if provisional.detail else "")
                + f"release_failed: {type(exc).__name__}: {exc}",
            )

    return _finalize_result(provisional, released=released)


def run_reindex_once(
    db_path: Path | str,
    *,
    repo_instance_id: str,
    repo_path: Path | str,
    run: RunFn | None = None,
    now: NowFn | None = None,
    timeout_seconds: float = RUNNER_TIMEOUT_SECONDS,
    acquire: AcquireFn | None = None,
    release: ReleaseFn | None = None,
    cli_path: str | None = None,
    lock_dir: Path | None = None,
) -> ReindexRunResult:
    """Acquire the lease, reindex the newest queued SHA, release, and drain.

    Never raises into the caller. A live holder yields ``status="held"``
    without running the reindex or touching the other holder's lease.
    A fenced release (``release`` returns False) is reported and does
    **not** clear the queue — the reclaiming holder owns it ([RES-10]).

    After a successful index and generation-matched release, re-reads the
    pending queue and loops. The no-progress budget (``MAX_DRAIN_ITERATIONS``)
    resets when an iteration shrinks the queue. Exhausting the budget (or
    ``MAX_DRAIN_ABSOLUTE``) with SHAs still queued returns
    ``status="pending_remaining"`` — never ``ok`` — so callers cannot mistake
    a partial drain for full success ([AGT-10], [AGT-21]).

    Indexing is always against the live worktree; the queued SHA is a
    watermark only (see ``_index_argv``).
    """
    run_fn = run or _default_run
    acquire_fn = acquire or acquire_reindex_lease
    release_fn = release or release_reindex_lease
    now_ts = float(now()) if now is not None else None

    final: ReindexRunResult | None = None
    last_ok: ReindexRunResult | None = None
    drain_iterations = 0
    no_progress_left = MAX_DRAIN_ITERATIONS
    prev_remaining_len: int | None = None

    while drain_iterations < MAX_DRAIN_ABSOLUTE and no_progress_left > 0:
        drain_iterations += 1
        final = _run_reindex_attempt(
            db_path,
            repo_instance_id=repo_instance_id,
            repo_path=repo_path,
            run_fn=run_fn,
            now_ts=now_ts,
            timeout_seconds=float(timeout_seconds),
            acquire_fn=acquire_fn,
            release_fn=release_fn,
            cli_path=cli_path,
            lock_dir=lock_dir,
        )
        # Drain only after a successful index + successful release. Non-ok
        # outcomes retain the queue for a later retry and must not busy-loop.
        if final.status != "ok" or final.released is not True:
            if last_ok is not None:
                # A prior iteration in THIS run indexed and released. A later
                # non-ok attempt (held/empty/…) defers the remainder; it must
                # not erase the completed work by reporting the bare non-ok
                # status. Surface the work done + what is left.
                remaining_after = read_requested_shas(db_path, repo_instance_id=repo_instance_id)
                if remaining_after:
                    return ReindexRunResult(
                        status="pending_remaining",
                        target_sha=last_ok.target_sha,
                        generation=last_ok.generation,
                        detail=(f"{last_ok.detail}; " if last_ok.detail else "")
                        + f"drain_iterations={drain_iterations}; "
                        + f"deferred_after_success={final.status}; "
                        + f"remaining_count={len(remaining_after)}",
                        released=True,
                        remaining_count=len(remaining_after),
                    )
                return _with_drain_detail(last_ok, drain_iterations=drain_iterations)
            return _with_drain_detail(final, drain_iterations=drain_iterations)

        last_ok = final
        remaining = read_requested_shas(db_path, repo_instance_id=repo_instance_id)
        if not remaining:
            return _with_drain_detail(final, drain_iterations=drain_iterations)

        cur_len = len(remaining)
        if prev_remaining_len is not None and cur_len < prev_remaining_len:
            # Queue shrank: real progress — reset the no-progress budget.
            no_progress_left = MAX_DRAIN_ITERATIONS
        else:
            no_progress_left -= 1
        prev_remaining_len = cur_len

    assert final is not None
    remaining = read_requested_shas(db_path, repo_instance_id=repo_instance_id)
    remaining_count = len(remaining)
    if remaining_count > 0 and final.status == "ok" and final.released is True:
        # Drain budget exhausted with work left — loud partial outcome ([AGT-10]).
        return ReindexRunResult(
            status="pending_remaining",
            target_sha=final.target_sha,
            generation=final.generation,
            detail=(f"{final.detail}; " if final.detail else "")
            + f"drain_iterations={drain_iterations}; remaining_count={remaining_count}",
            released=True,
            remaining_count=remaining_count,
        )
    return _with_drain_detail(
        final,
        drain_iterations=drain_iterations,
        remaining_count=remaining_count if remaining_count else None,
    )


__all__ = [
    "CODEMAP_CLI_NAME",
    "MAX_DRAIN_ABSOLUTE",
    "MAX_DRAIN_ITERATIONS",
    "ReindexRunResult",
    "run_reindex_once",
]
