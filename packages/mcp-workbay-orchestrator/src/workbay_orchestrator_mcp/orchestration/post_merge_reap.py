"""Durable continuation for the bounded post-merge review sweep."""

from __future__ import annotations

import json
import time
import uuid
from bisect import bisect_right
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.runtime import get_runtime_config
from workbay_handoff_mcp.shared_schema import _get_db_connection

from .worker_lock import release_worker_lock, try_acquire_worker_lock

REVIEW_PHASE = "review_stage_reap"
REVIEW_INVOCATION_LOCK_HELD = "review_invocation_lock_held"
_PENDING_SESSION = "post-merge-reap-pending"
_SELECT = "SELECT rationale FROM decisions WHERE task_ref = ? AND session = ? AND decision = ?"
_UPSERT = """
    INSERT INTO decisions (task_ref, session, decision, rationale, decision_origin)
    VALUES (?, ?, ?, ?, 'system')
    ON CONFLICT(task_ref, decision, session) DO UPDATE SET rationale = excluded.rationale
"""


def _scope(root: Path, session: str, phase: str) -> tuple[str, str, str]:
    return "", session, json.dumps([str(root), phase], ensure_ascii=True)


def schedule_candidates(
    keys: Iterable[str],
    *,
    root: Path,
    phase: str,
    deadline: float,
    exclude: Iterable[str] = (),
    request: str | None = None,
) -> Iterator[str]:
    """Enter each candidate at most once per scan, starting after the durable cursor."""
    remaining = sorted(set(keys).difference(exclude))
    scope = _scope(root, "post-merge-reap-cursor", phase)
    pending_scope = _scope(root, _PENDING_SESSION, phase)
    while remaining:
        # This commits entry, so a slow probe cannot starve the next candidate.
        # The cursor is an ordering hint, never proof of phase completion.
        with _get_db_connection(begin_immediate=True, deadline=deadline) as conn:
            if request is not None:
                pending = conn.execute(_SELECT, pending_scope).fetchone()
                if pending is None or pending["rationale"] != request:
                    return  # A new merge supplied a different set of invocation holds.
            row = conn.execute(_SELECT, scope).fetchone()
            try:
                cursor = json.loads(row["rationale"]) if row else None
            except (TypeError, ValueError):
                cursor = None
            index = bisect_right(remaining, cursor) if isinstance(cursor, str) else 0
            key = remaining.pop(index % len(remaining))
            conn.execute(_UPSERT, (*scope, json.dumps(key, ensure_ascii=True)))
        yield key


def _review_action_lock_path(root: Path) -> Path:
    # Both callers use the primary worktree, including invocations from a linked checkout.
    return root / ".git" / "workbay-review-invocation.lock"


def _review_intake_lock_path(root: Path) -> Path:
    return root / ".git" / "workbay-review-intake.lock"


def request_review_sweep(root: Path, *, worktree: Path, branch: str, deadline: float) -> None:
    """Publish before scanning and retain invocation holds from coalesced merges."""
    scope = _scope(root, _PENDING_SESSION, REVIEW_PHASE)
    hold = [str(worktree.resolve()), branch]
    lock_path = _review_intake_lock_path(root)
    handle = None
    try:
        while handle is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("review invocation hold publication deadline exceeded")
            handle = try_acquire_worker_lock(lock_path)
            if handle is None:
                time.sleep(min(0.01, remaining))
        with _get_db_connection(begin_immediate=True, deadline=deadline) as conn:
            row = conn.execute(_SELECT, scope).fetchone()
            holds = json.loads(row["rationale"])["holds"] if row else []
            if hold not in holds:
                holds.append(hold)
            request = {"generation": uuid.uuid4().hex, "holds": holds}
            conn.execute(_UPSERT, (*scope, json.dumps(request, ensure_ascii=True)))
    finally:
        if handle is not None:
            release_worker_lock(handle)


@contextmanager
def review_sweep_action(root: Path, *, branch: str, worktree: str | None, deadline: float | None) -> Iterator[str]:
    """Admit one native retirement against the current journal and serialize its effects."""
    action = None
    try:
        action = try_acquire_worker_lock(_review_action_lock_path(root))
        if action is None:
            yield REVIEW_INVOCATION_LOCK_HELD
            return
        # Retirement takes the action lock before intake. Publishers take only
        # intake, so they can commit while an admitted retirement runs.
        intake = None
        try:
            intake = try_acquire_worker_lock(_review_intake_lock_path(root))
            if intake is None:
                yield REVIEW_INVOCATION_LOCK_HELD
                return
            with _get_db_connection(deadline=deadline) as conn:
                row = conn.execute(_SELECT, _scope(root, _PENDING_SESSION, REVIEW_PHASE)).fetchone()
            holds = json.loads(row["rationale"])["holds"] if row else []
            path = Path(worktree).resolve() if worktree else None
            held = any(
                branch == held_branch or (path is not None and path == Path(held_path).resolve())
                for held_path, held_branch in holds
            )
        finally:
            # Admission is ordered here. Close SQLite and release intake before
            # Git so later publications can fence subsequent retirements.
            if intake is not None:
                release_worker_lock(intake)
        yield "review_invocation_hold" if held else ""
    finally:
        if action is not None:
            release_worker_lock(action)


def _review_scheduler(keys: Iterable[str], *, root: Path, deadline: float, request: str) -> Iterator[str]:
    holds = json.loads(request)["holds"]
    excluded = {branch for _, branch in holds}
    linked_paths = {Path(path).resolve() for path, _ in holds} - {root}
    if linked_paths:
        # Replay may run from the primary checkout after a protected linked
        # worktree changed branches. Preserve the path as well as its old ref.
        from .review_stage_reap import _worktree_registry  # noqa: PLC0415

        registry, reason = _worktree_registry(root)
        if registry is None:
            raise RuntimeError(reason)
        excluded.update(
            entry["branch"]
            for entry in registry
            if entry.get("branch") and Path(entry["path"]).resolve() in linked_paths
        )
    yield from schedule_candidates(
        keys, root=root, phase=REVIEW_PHASE, deadline=deadline, exclude=excluded, request=request
    )


def review_sweep_complete(result: dict[str, Any]) -> bool:
    return not (result.get("cleanup_pending") or result.get("failures")) and (
        result.get("ok") is True or (result.get("error") == "main_ref_unavailable" and not result.get("kept"))
    )


def consume_review_sweep(
    root: Path,
    *,
    deadline: float,
    run: Callable[[Callable[[Iterable[str]], Iterable[str]]], dict[str, Any] | None],
) -> dict[str, Any] | None:
    """Run one generation under a shared foreground/maintenance kernel lock."""
    db_path = get_runtime_config().db_path
    if not db_path.is_file():
        return None
    scope = _scope(root, _PENDING_SESSION, REVIEW_PHASE)
    with _get_db_connection(deadline=deadline) as conn:
        if conn.execute(_SELECT, scope).fetchone() is None:
            return None
    handle = try_acquire_worker_lock(db_path.with_suffix(".post-merge-review.lock"))
    if handle is None:
        return None  # The durable request survives the other consumer.
    try:
        with _get_db_connection(deadline=deadline) as conn:
            row = conn.execute(_SELECT, scope).fetchone()
        if row is None:
            return None
        request = row["rationale"]
        schedule = partial(_review_scheduler, root=root, deadline=deadline, request=request)
        result = run(schedule)
        if isinstance(result, dict) and review_sweep_complete(result):
            # Publication can race this scan. Match the payload containing our
            # unique generation so an old completion cannot clear a new merge.
            with _get_db_connection(deadline=deadline) as conn:
                conn.execute(
                    "DELETE FROM decisions WHERE task_ref = ? AND session = ? AND decision = ? AND rationale = ?",
                    (*scope, request),
                )
        return result
    finally:
        release_worker_lock(handle)
