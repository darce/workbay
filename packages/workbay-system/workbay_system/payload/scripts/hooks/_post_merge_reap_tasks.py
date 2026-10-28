#!/usr/bin/env python3
"""Best-effort auto-reap of closeable tasks after a merge into an integration branch.

Invoked by ``scripts/hooks/git/post-merge`` when the current branch is ``main``,
``master``, or any ``integration_branch`` registered in
``config/lane-orchestration/*.json``. Identifies task rows whose
``target_branch`` tip matches the commit just merged into the integration
branch and runs a scoped ``reap_tasks(apply=True, task_ref=...)``. Rows that
are active or ambiguous are left untouched — the reaper is conservative by
construction. Failures are recorded to the shared hook-failure sink and the
process still exits 0: a git hook must never break a merge.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

_INTEGRATION_BRANCHES = frozenset({"main", "master"})
_SOURCE = "post-merge-reap"
_ROWLESS_REAP_TIMEOUT_S = 10.0
_RUN_FAILURES: list[dict[str, str | None]] | None = None
# Signals whose Python handlers can unwind the hook; defer them while mutating
# or restoring process-global signal state, including the default SIGINT handler.
_DEFERRED_SIGNALS = frozenset({signal.SIGALRM, signal.SIGTERM, signal.SIGINT})
# Phase the run is in, and the first termination signal with the phase it hit.
_RUN_STATE: dict[str, object] = {"phase": "idle", "signal": None, "interrupted": None}


def _on_main_thread() -> bool:
    """Signal handlers and ITIMER_REAL belong to the main thread only."""
    return threading.current_thread() is threading.main_thread()


def _consume_pending(signum: int) -> bool:
    """Discard one pending ``signum`` without running its handler; True if one was pending.

    darwin CPython has no ``signal.sigtimedwait``. There, setting a signal's disposition
    to SIG_IGN discards a pending instance (POSIX), so the handler is swapped out and
    straight back. Callers hold ``signum`` blocked, so nothing runs in between, and the
    signal mask is never touched.
    """
    if hasattr(signal, "sigtimedwait"):
        return signal.sigtimedwait({signum}, 0.0) is not None
    if signum not in signal.sigpending():
        return False
    previous = signal.getsignal(signum)
    try:
        signal.signal(signum, signal.SIG_IGN)
    finally:
        signal.signal(signum, previous)
    return True


def _hooks_dir() -> str:
    return str(Path(__file__).resolve().parent)


def _ensure_hooks_dir_on_path() -> None:
    hook_dir = _hooks_dir()
    if hook_dir not in sys.path:
        sys.path.insert(0, hook_dir)


def _record_failure(*, kind: str, detail: str, task_ref: str | None = None) -> None:
    """Append one durable failure record; never raise into the hook.

    When ``task_ref`` is known it is emitted as a structured ``task_ref=`` token
    inside ``detail=`` so a later successful pass can purge *only* that task's
    own records (never a foreign source, never an unrelated soft-failure).
    """
    if isinstance(_RUN_FAILURES, list):
        _RUN_FAILURES.append({"kind": kind, "detail": detail, "task_ref": task_ref})
        return
    try:
        _ensure_hooks_dir_on_path()
        from _hook_failure_sink import record_hook_failure

        if task_ref:
            # Structured token first so selective purge can match without
            # substring-matching free-text detail from other writers.
            detail = f"task_ref={task_ref} {detail}"
        record_hook_failure(source=_SOURCE, kind=kind, detail=detail)
    except Exception:  # noqa: BLE001 -- sink must never break the merge
        pass


def _flush_failure_sink() -> None:
    """Write this run's failures as one bounded record, if any."""
    records = _RUN_FAILURES
    if not records:
        return
    try:
        refs = sorted({entry["task_ref"] for entry in records if entry.get("task_ref")})
        unscoped = sum(1 for entry in records if not entry.get("task_ref"))
        exceptions = sum(1 for entry in records if entry.get("kind") == "exception")
        kind = "exception" if exceptions else "returncode"
        first = str(records[0].get("detail") or "").replace("task_ref=", "task_ref:")
        visible_refs = list(refs)
        dropped = 0
        scope = f"task_ref={refs[0]} " if len(refs) == 1 and unscoped == 0 else ""

        def _header() -> str:
            items = list(visible_refs)
            if dropped:
                items.append(f"+{dropped}")
            refs_text = ",".join(items) if items else "-"
            return f"{scope}failures={len(records)} exceptions={exceptions} unscoped={unscoped} task_refs={refs_text}"

        while len(_header()) > 400 and visible_refs:
            visible_refs.pop()
            dropped += 1
        detail = f"{_header()} first={first}"

        _ensure_hooks_dir_on_path()
        from _hook_failure_sink import record_hook_failure

        record_hook_failure(source=_SOURCE, kind=kind, detail=detail)
    except Exception:  # noqa: BLE001 -- failure reporting must never break the merge
        pass


def _is_own_record(line: str) -> bool:
    """True when ``line`` is one of THIS hook's sink records.

    Matches on field POSITION, not substring. The record shape is
    ``ts=<iso> source=<name> kind=<kind> detail=<one line>`` and ``detail`` is
    free text, so an unanchored ``" source=post-merge-reap "`` test would also
    match another writer's record whose detail happens to quote ours — deleting
    a foreign record inside the very function written to stop cross-source
    deletion.
    """
    fields = line.split()
    return len(fields) > 1 and fields[1] == f"source={_SOURCE}"


def _task_ref_from_own_record(line: str) -> str | None:
    """Extract the structured ``task_ref=`` token from an own-source record."""
    if not _is_own_record(line):
        return None
    for tok in line.split():
        if tok.startswith("task_ref="):
            value = tok[len("task_ref=") :]
            return value or None
        # detail= may glue the token: detail=task_ref=TASK-X
        if tok.startswith("detail=task_ref="):
            value = tok[len("detail=task_ref=") :]
            return value or None
    return None


def _aggregate_is_resolved(line: str, resolved_task_refs: set[str]) -> bool | None:
    """Return whether an aggregate is safely purged; None means legacy row."""
    fields = line.split()
    unscoped_shape = len(fields) > 3 and fields[3].startswith("detail=failures=")
    scoped_shape = len(fields) > 4 and fields[3].startswith("detail=task_ref=") and fields[4].startswith("failures=")
    if not (unscoped_shape or scoped_shape):
        return None

    header: list[str] = []
    for field in fields[3:]:
        if field.startswith("first="):
            break
        header.append(field)

    parsed: dict[str, str] = {}
    for field in header:
        if field.startswith("detail=task_ref=") or field.startswith("detail=failures="):
            continue
        key, separator, value = field.partition("=")
        if separator and key in {"unscoped", "task_refs"}:
            if key in parsed:
                return False
            parsed[key] = value
    refs_value = parsed.get("task_refs")
    unscoped_value = parsed.get("unscoped")
    if refs_value is None or unscoped_value is None:
        return False
    try:
        unscoped = int(unscoped_value)
    except ValueError:
        return False
    refs = refs_value.split(",")
    if any(not ref for ref in refs):
        return False
    complete = not any(ref.startswith("+") for ref in refs)
    if not complete or not refs or refs == ["-"] or unscoped != 0:
        return False
    return all(ref in resolved_task_refs for ref in refs)


def _purge_resolved_hook_failures_safe(resolved_task_refs: set[str]) -> None:
    """Drop own-source records only for task_refs this pass proved resolved.

    Never purges when ``resolved_task_refs`` is empty (a pass that reaped
    nothing is not evidence that any prior failure is fixed). Never drops
    foreign sources' rows or own rows for task_refs not in the resolved set.
    Rewrite is flock-serialized with the sink's append/compaction path.
    """
    if not resolved_task_refs:
        return
    try:
        _ensure_hooks_dir_on_path()
        from _hook_failure_sink import (
            resolve_hook_failures_log_path,
            rewrite_hook_failures_under_lock,
        )

        path = resolve_hook_failures_log_path()
        if not path or not os.path.isfile(path):
            return

        def keep(line: str) -> bool:
            if not _is_own_record(line):
                return True
            aggregate_resolved = _aggregate_is_resolved(line, resolved_task_refs)
            if aggregate_resolved is not None:
                return not aggregate_resolved
            tr = _task_ref_from_own_record(line)
            # Own record without a parseable task_ref: leave it (conservative).
            if tr is None:
                return True
            # Drop only when this pass successfully reaped that task_ref.
            return tr not in resolved_task_refs

        rewrite_hook_failures_under_lock(path, keep)
    except Exception:  # noqa: BLE001 -- purge must never break the merge
        pass


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False, cwd=cwd)
    except (subprocess.TimeoutExpired, OSError):
        # A hung or unspawnable git call must never break the merge; surface it
        # to callers as a non-zero result they already treat as "unknown".
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="")


def _repo_root() -> Path:
    proc = _run(["git", "rev-parse", "--show-toplevel"], Path.cwd())
    if proc.returncode != 0 or not proc.stdout.strip():
        return Path.cwd()
    return Path(proc.stdout.strip())


def _current_branch(repo: Path) -> str:
    proc = _run(["git", "branch", "--show-current"], repo)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _add_integration_branch_values(collected: set[str], value: object) -> None:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            collected.add(stripped)
        return
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                stripped = item.strip()
                if stripped:
                    collected.add(stripped)


def _registered_integration_branches(repo: Path) -> set[str]:
    """Collect ``integration_branch`` values from lane-orchestration manifests.

    Unreadable files are skipped and logged so a corrupt sibling cannot block
    the hook or the merge.
    """
    collected: set[str] = set()
    config_dir = repo / "config" / "lane-orchestration"
    try:
        paths = sorted(config_dir.glob("*.json"))
    except Exception as exc:  # noqa: BLE001 -- never break the merge
        print(
            f"post-merge-reap: skip unreadable lane-orchestration dir: {exc}",
            file=sys.stderr,
        )
        return collected
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 -- skip and log
            print(
                f"post-merge-reap: skip unreadable lane-orchestration file {path.name}: {exc}",
                file=sys.stderr,
            )
            continue
        if not isinstance(payload, dict):
            continue
        _add_integration_branch_values(collected, payload.get("integration_branch"))
    return collected


def _is_eligible_integration_branch(repo: Path, branch: str) -> bool:
    if not branch:
        return False
    if branch in _INTEGRATION_BRANCHES:
        return True
    return branch in _registered_integration_branches(repo)


def _commit_sha(repo: Path, ref: str) -> str:
    proc = _run(["git", "rev-parse", ref], repo)
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _parents(repo: Path, ref: str = "HEAD") -> list[str]:
    """Return the parent SHAs of ``ref`` in order (first parent first)."""
    proc = _run(["git", "cat-file", "-p", ref], repo)
    if proc.returncode != 0:
        return []
    parents: list[str] = []
    for line in proc.stdout.splitlines():
        if line.startswith("parent "):
            parents.append(line.split(" ", 1)[1].strip())
        elif not line.strip():
            break  # blank line terminates the commit header
    return parents


def _merged_tips(repo: Path) -> list[str]:
    """Return SHAs for branch tips that were just merged into HEAD.

    A real merge commit records the pre-merge tip as its *first* parent and sets
    ``ORIG_HEAD`` to that same commit; the merged branch tips are therefore the
    remaining parents (``HEAD^2..HEAD^N`` for an octopus merge). Discriminating
    on ``first_parent == ORIG_HEAD`` rather than raw parent count matters when
    the merged branch's own tip is itself a merge commit and the integration
    branch is fast-forwarded onto it: there HEAD has two parents but the whole
    tip (HEAD) was merged, not its second parent.
    """
    orig = _commit_sha(repo, "ORIG_HEAD")
    head = _commit_sha(repo, "HEAD")
    if not head:
        return []
    parents = _parents(repo)
    if orig and parents and parents[0] == orig and len(parents) > 1:
        # True merge commit (incl. octopus): every non-first parent is a tip.
        return parents[1:]
    if orig and orig != head:
        # Fast-forward: HEAD advanced to the merged tip; no new commit created.
        return [head]
    return []


def _resolve_branch_tip(repo: Path, branch: str) -> str:
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        proc = _run(["git", "rev-parse", ref], repo)
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    proc = _run(["git", "rev-parse", branch], repo)
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout.strip()
    return ""


def _live_task_refs_for_merged_tips(repo: Path, tips: set[str]) -> list[str]:
    from workbay_handoff_mcp.api import list_handoff_rows
    from workbay_handoff_mcp.shared_primitives import LIVE_ACTIVE_STATUSES

    if not tips:
        return []

    refs: list[str] = []
    rows = list_handoff_rows(status_filter=list(LIVE_ACTIVE_STATUSES))
    for row in rows:
        if not isinstance(row, dict):
            continue
        target_branch = row.get("target_branch")
        if not isinstance(target_branch, str) or not target_branch:
            continue
        tip = _resolve_branch_tip(repo, target_branch)
        if tip not in tips:
            continue
        ref = row.get("task_ref")
        if isinstance(ref, str) and ref and ref not in refs:
            refs.append(ref)
    return refs


def _reap_scoped(task_ref: str, integration_ref: str) -> bool:
    """Reap one task. Return True when the call reported a soft failure.

    ``reap_tasks`` reports per-stage problems in a ``failed`` list rather than
    by raising, so an exception-only guard treats them as success. Printing them
    to stderr is not durable — git discards hook output — so each one is also
    written to the shared sink ([AGT-10]: a swallowed error that keeps the
    session alive must still land in a log). The boolean is what stops a pass
    with soft failures from being treated as clean by the caller's purge.
    """
    from workbay_handoff_mcp.api import reap_tasks

    result = reap_tasks(apply=True, task_ref=task_ref, integration_ref=integration_ref)
    failed = []
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            failed = data.get("failed") or []
        else:
            failed = result.get("failed") or []
    saw_failure = False
    for entry in failed:
        if not isinstance(entry, dict):
            continue
        saw_failure = True
        entry_ref = entry.get("task_ref", task_ref) or task_ref
        detail = f"stage={entry.get('stage', '?')}: {entry.get('error', '')}"
        print(
            f"post-merge-reap: failed task_ref={entry_ref} {detail}",
            file=sys.stderr,
        )
        _record_failure(
            kind="returncode",
            detail=detail,
            task_ref=str(entry_ref) if entry_ref else task_ref,
        )
    return saw_failure


def _close_merged_lanes_safe(repo: Path) -> None:
    """Bind merge → worktree_lanes terminal via close_offload_lane_merged.

    Best-effort: ImportError / exceptions are recorded and swallowed so the
    merge never fails. Does not delete worktrees or branches.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            close_lanes_merged_into_integration,
        )
    except ImportError as exc:
        detail = f"lane_merge_close ImportError: {exc}"
        _record_failure(kind="exception", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return
    try:
        branch = _current_branch(repo)
        result = close_lanes_merged_into_integration(
            orchestrator_root=repo,
            integration_refs=[branch] if branch else None,
        )
    except Exception as exc:  # noqa: BLE001 -- never break the merge
        detail = f"lane_merge_close {type(exc).__name__}: {exc}"
        _record_failure(kind="exception", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return
    if not isinstance(result, dict):
        detail = "lane_merge_close: non-dict result"
        _record_failure(kind="returncode", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return
    if result.get("ok") is not True:
        detail = f"lane_merge_close: {result.get('error') or 'ok!=True'}"
        _record_failure(kind="returncode", detail=detail)
        # Print whenever the sweep is not successful — not only when failed[]
        # is non-empty. Structured list failures return ok=False with failed=[]
        # and used to look identical to a clean no-op on the console [OBS-08].
        print(
            f"post-merge-reap: lane merge-close reported failure: {result.get('error') or 'ok!=True'}",
            file=sys.stderr,
        )
    failed_entries = result.get("failed") or []
    for entry in failed_entries:
        if not isinstance(entry, dict):
            continue
        entry_ref = entry.get("task_ref")
        _record_failure(
            kind="returncode",
            detail=f"lane_merge_close lane={entry.get('lane_id')}: {entry.get('error')}",
            task_ref=str(entry_ref) if entry_ref else None,
        )
    closed = result.get("closed") or []
    if closed:
        print(
            f"post-merge-reap: closed {len(closed)} lane(s) via ancestry binder",
            file=sys.stderr,
        )
    # Surface per-lane binder failures on the console even when nothing closed.
    if failed_entries:
        print(
            f"post-merge-reap: lane merge-close reported {len(failed_entries)} failure(s)",
            file=sys.stderr,
        )

    skipped = result.get("skipped") or []
    if skipped:
        reason_counts: dict[str, int] = {}
        for entry in skipped:
            if not isinstance(entry, dict):
                reason = "unknown"
            else:
                reason = str(entry.get("reason") or "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        # Stable order for the histogram so sessions are comparable.
        parts = [f"{reason}={reason_counts[reason]}" for reason in sorted(reason_counts)]
        histogram = ", ".join(parts)
        # Console only. A held lane is the binder working as designed, not a
        # failure: routing routine holds into the failure sink would append an
        # unpurgeable record on essentially every integration merge and bury
        # the real failures it exists to preserve.
        print(
            f"post-merge-reap: held {len(skipped)} lane(s): {histogram}",
            file=sys.stderr,
        )


class _RowlessReapWatchdog(BaseException):
    """Escape best-effort Exception handlers as soon as the watchdog fires."""


def _reap_rowless_registry_safe(repo: Path) -> None:
    """Run the narrow row-less registry arm under the hook's bounded policy."""
    try:
        from workbay_orchestrator_mcp.lane_reaping import (  # noqa: PLC0415
            reap_merged_registry_worktrees,
        )
    except ImportError as exc:
        detail = f"rowless_registry_reap ImportError: {exc}"
        _record_failure(kind="exception", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return

    try:
        if not _on_main_thread():
            # Off the main thread signal.signal fails after setitimer already took
            # the caller's timer. Touch no signal state; the deadline still bounds
            # every subprocess and SQLite wait.
            detail = (
                "rowless_registry_reap degraded_watchdog: off main thread, SIGALRM watchdog not armed; "
                f"bounded by the {_ROWLESS_REAP_TIMEOUT_S:g}s deadline only"
            )
            _record_failure(kind="returncode", detail=detail)
            print(f"post-merge-reap: {detail}", file=sys.stderr)
            result = reap_merged_registry_worktrees(
                apply=True, repo_root=repo, rowless_only=True, deadline=time.monotonic() + _ROWLESS_REAP_TIMEOUT_S
            )
        else:
            # Capture every piece of the caller's state before changing any of it.
            # An interrupt during capture has nothing to restore.
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            previous_handler = signal.getsignal(signal.SIGALRM)
            previous_timer = signal.getitimer(signal.ITIMER_REAL)
            previous_alarm_pending = signal.SIGALRM in signal.sigpending()
            started = time.monotonic()
            watchdog_fired = alarm_pending = alarm_replayed = restored = False
            restore_error: BaseException | None = None
            # Mark before mutate: each flag is set before its call, because an
            # interrupt can land inside the call after it took effect or right
            # after it returns. Restoring an unchanged piece is a no-op.
            mask_changed = handler_changed = timer_changed = watchdog_armed = False

            def _timeout(_signum: int, _frame: object) -> None:
                nonlocal watchdog_fired
                watchdog_fired = True
                # Another thread can receive SIGALRM and schedule this Python callback
                # even while this thread blocks it. Honor the mask during restoration.
                if signal.SIGALRM not in signal.pthread_sigmask(signal.SIG_BLOCK, set()):
                    raise _RowlessReapWatchdog(f"rowless registry reap exceeded {_ROWLESS_REAP_TIMEOUT_S:g}s")

            def _restore() -> None:
                """One pass back to the captured state; every step runs even when an earlier one fails."""
                nonlocal alarm_pending, alarm_replayed, restore_error, restored
                error: BaseException | None = None
                drained = False
                try:
                    if mask_changed:
                        signal.pthread_sigmask(signal.SIG_BLOCK, _DEFERRED_SIGNALS)
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = exc
                try:
                    if timer_changed:
                        signal.setitimer(signal.ITIMER_REAL, 0.0)
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                try:
                    # Drain both thread- and process-pending alarms before restoring
                    # ownership. Replay a caller's pre-existing notification below.
                    # Before the watchdog is armed a pending alarm is the caller's.
                    while watchdog_armed and _consume_pending(signal.SIGALRM):
                        alarm_pending = drained = True
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                try:
                    if handler_changed:
                        signal.signal(signal.SIGALRM, previous_handler)
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                try:
                    if timer_changed and previous_timer[0] > 0:
                        remaining = max(0.001, previous_timer[0] - (time.monotonic() - started))
                        signal.setitimer(signal.ITIMER_REAL, remaining, previous_timer[1])
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                try:
                    # Replay the caller's alarm once; a later pass repeats it only
                    # when it drained the earlier replay back out.
                    if watchdog_armed and previous_alarm_pending and (drained or not alarm_replayed):
                        alarm_replayed = True
                        signal.raise_signal(signal.SIGALRM)
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                try:
                    if mask_changed:
                        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                except BaseException as exc:  # noqa: BLE001 -- keep restoring
                    error = error or exc
                restore_error = restore_error or error
                restored = error is None

            # Restoration covers installation: a timer that fires while the handler is being
            # installed or armed still gets the previous handler and periodic timer back.
            try:
                try:
                    try:
                        mask_changed = True
                        signal.pthread_sigmask(signal.SIG_BLOCK, _DEFERRED_SIGNALS)
                        timer_changed = True
                        signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
                        # The caller's timer can expire between the read-only snapshot
                        # and disarm. Preserve that notification before owning SIGALRM.
                        previous_alarm_pending = previous_alarm_pending or signal.SIGALRM in signal.sigpending()
                        handler_changed = True
                        signal.signal(signal.SIGALRM, _timeout)
                        watchdog_armed = True
                        signal.setitimer(signal.ITIMER_REAL, _ROWLESS_REAP_TIMEOUT_S)
                        # A caller alarm already pending stays blocked for the caller;
                        # expiry is then measured by elapsed time below.
                        reap_mask = previous_mask | {signal.SIGALRM} if previous_alarm_pending else previous_mask
                        signal.pthread_sigmask(signal.SIG_SETMASK, reap_mask)
                        result = reap_merged_registry_worktrees(
                            apply=True, repo_root=repo, rowless_only=True, deadline=started + _ROWLESS_REAP_TIMEOUT_S
                        )
                    finally:
                        # Keep restoration in the outer finally: if the one-shot watchdog
                        # interrupts entry to this mask, restoration must still run.
                        if mask_changed:
                            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                finally:
                    _restore()
            finally:
                # An interrupt can cut the pass above short, even between its guarded
                # steps (a SIGINT taken by another thread still raises on this one).
                # Every step converges on the captured state, so it simply runs again.
                # The first error is raised only after the mask is back.
                if not restored:
                    _restore()
                if restore_error is not None:
                    raise restore_error
            expired = watchdog_armed and (
                watchdog_fired
                or (time.monotonic() - started >= _ROWLESS_REAP_TIMEOUT_S if previous_alarm_pending else alarm_pending)
            )
            if expired:
                raise _RowlessReapWatchdog(f"rowless registry reap exceeded {_ROWLESS_REAP_TIMEOUT_S:g}s")
    except (Exception, _RowlessReapWatchdog) as exc:  # noqa: BLE001 -- never break the merge
        detail = f"rowless_registry_reap {type(exc).__name__}: {exc}"
        _record_failure(kind="exception", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return

    if not isinstance(result, dict):
        detail = "rowless_registry_reap: non-dict result"
        _record_failure(kind="returncode", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
        return
    if result.get("ok") is not True:
        detail = f"rowless_registry_reap: {result.get('error') or 'ok!=True'}"
        _record_failure(kind="returncode", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)
    for entry in result.get("failed") or []:
        if not isinstance(entry, dict):
            continue
        detail = f"rowless_registry_reap stage={entry.get('stage', '?')}: {entry.get('detail', '')}"
        _record_failure(kind="returncode", detail=detail)
        print(f"post-merge-reap: {detail}", file=sys.stderr)


def _note_termination(signum: int) -> None:
    if _RUN_STATE["signal"] is None:
        _RUN_STATE["signal"] = signum
        _RUN_STATE["interrupted"] = _RUN_STATE["phase"]


def _handle_sigterm(signum: int, _frame: object) -> None:
    """Note the signal and the phase it hit; the unwinding path reports it."""
    _note_termination(signum)
    if signum in signal.pthread_sigmask(signal.SIG_BLOCK, set()):
        # Another thread took it while this thread defers it: re-pend it here so
        # unblocking raises once the deferred section has finished.
        signal.pthread_kill(threading.get_ident(), signum)
        return
    raise SystemExit(128 + signum)


def _finish_run(saved: dict[str, object]) -> None:
    """Consume owned SIGTERM and report the run exactly once.

    Runs with the owned signals blocked. main() owns their restoration, including
    when an interrupt prevents this function from starting or finishing.
    """
    global _RUN_FAILURES
    first_error: BaseException | None = None
    if "handlers" in saved:
        try:
            # Ownership ends here. A SIGTERM still pending arrived while this run
            # deferred it; one the caller itself blocks stays the caller's.
            mask = saved["mask"]
            if isinstance(mask, set) and signal.SIGTERM not in mask:
                if _consume_pending(signal.SIGTERM):
                    _note_termination(signal.SIGTERM)
        except BaseException as exc:  # noqa: BLE001 -- keep restoring
            first_error = first_error or exc
    signum = _RUN_STATE["signal"]
    try:
        if isinstance(signum, int) and isinstance(_RUN_FAILURES, list):
            name = signal.Signals(signum).name
            detail = f"terminated signal={name} phase={_RUN_STATE['interrupted']}"
            _RUN_FAILURES.insert(0, {"kind": "exception", "detail": detail, "task_ref": None})
        _flush_failure_sink()
    except BaseException as exc:  # noqa: BLE001 -- keep restoring
        first_error = first_error or exc
    _RUN_FAILURES = None
    if isinstance(signum, int):
        raise SystemExit(128 + signum) from first_error
    if first_error is not None:
        raise first_error


def _main_body(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if args and args[0] == "1":
        # Squash merges do not preserve a second parent; identifying the
        # source branch/task is unreliable — skip auto-reap.
        return 0

    repo = _repo_root()
    branch = _current_branch(repo)
    if not _is_eligible_integration_branch(repo, branch):
        return 0

    try:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    except ImportError as exc:
        _record_failure(kind="exception", detail=f"ImportError: {exc}")
        return 0

    try:
        configure_runtime(RuntimeConfig.for_repo(repo))
    except Exception as exc:  # noqa: BLE001 -- degrade loudly, never break merge
        _record_failure(
            kind="exception",
            detail=f"configure_runtime: {type(exc).__name__}: {exc}",
        )
        return 0

    # MERGECLOSE-01: ancestry binder runs on every integration post-merge,
    # independent of tip discovery. Squash is already short-circuited above.
    _RUN_STATE["phase"] = "lane_merge_close"
    _close_merged_lanes_safe(repo)
    _RUN_STATE["phase"] = "rowless_registry_reap"
    _reap_rowless_registry_safe(repo)
    _RUN_STATE["phase"] = "task_reap"

    # Discovery (list_handoff_rows import/query) and reap run under one broad
    # guard: a DB, import, or git error here must never escape and break the
    # merge — the shell ``|| true`` is a backstop, not the contract. Failures
    # are recorded to the shared sink ([AGT-10]).
    # Track only task_refs this pass *successfully* reaped; purge is scoped
    # to those refs alone (never "all own records" on a clean-looking pass).
    resolved_task_refs: set[str] = set()
    try:
        tips = set(_merged_tips(repo))
        if not tips:
            # No-op pass: nothing was reaped, so this is NOT evidence that any
            # previously recorded failure is resolved. Do not purge here.
            return 0
        for task_ref in _live_task_refs_for_merged_tips(repo, tips):
            try:
                if _reap_scoped(task_ref, integration_ref=branch):
                    # Soft failure recorded; do not mark resolved.
                    continue
                resolved_task_refs.add(task_ref)
            except Exception as exc:  # noqa: BLE001 -- per-task; continue others
                _record_failure(
                    kind="exception",
                    detail=f"{type(exc).__name__}: {exc}",
                    task_ref=task_ref,
                )
                continue
    except Exception as exc:  # noqa: BLE001 -- degrade loudly, never break merge
        _record_failure(kind="exception", detail=f"{type(exc).__name__}: {exc}")
        return 0

    # Purge only records this pass proved resolved. Empty set → no-op
    # (covers zero matching task rows and all-failed passes).
    _RUN_STATE["phase"] = "purge"
    _purge_resolved_hook_failures_safe(resolved_task_refs)
    return 0


def main(argv: list[str] | None = None) -> int:
    # Harness-agnostic interpreter self-heal: re-exec under project .venv so
    # bare ``python3`` (no workbay_handoff_mcp) does not silent-no-op. Non-fatal
    # if the sibling ``_interp`` helper is unavailable. See scripts/hooks/_interp.py.
    try:
        _ensure_hooks_dir_on_path()
        from _interp import ensure_deps_interpreter

        ensure_deps_interpreter()
    except Exception:  # noqa: BLE001 -- fail open if _interp missing
        pass

    global _RUN_FAILURES
    _RUN_FAILURES = []
    _RUN_STATE.update(phase="startup", signal=None, interrupted=None)
    owner = _on_main_thread()
    saved: dict[str, object] = {}
    restore_needed = restored = False
    first_error: BaseException | None = None

    def _restore_signals() -> None:
        """Converge on the caller's state, even if an earlier step failed."""
        nonlocal restored, first_error
        error: BaseException | None = None
        for signum, handler in saved["handlers"].items():
            try:
                signal.signal(signum, handler)
            except BaseException as exc:  # noqa: BLE001 -- keep restoring
                error = error or exc
        try:
            signal.pthread_sigmask(signal.SIG_SETMASK, saved["mask"])
        except BaseException as exc:  # noqa: BLE001 -- keep restoring
            error = error or exc
        first_error = first_error or error
        restored = error is None

    def _finalize() -> None:
        _RUN_STATE["phase"] = "finalize"
        try:
            if restore_needed:
                signal.pthread_sigmask(signal.SIG_BLOCK, _DEFERRED_SIGNALS)
        finally:
            _finish_run(saved)

    try:
        try:
            if owner:
                # Capture everything before mutating anything; restoring an
                # unchanged piece is safe if a call is interrupted before acting.
                saved["mask"] = signal.pthread_sigmask(signal.SIG_BLOCK, set())
                saved["handlers"] = {sig: signal.getsignal(sig) for sig in _DEFERRED_SIGNALS}
                restore_needed = True
                signal.pthread_sigmask(signal.SIG_BLOCK, _DEFERRED_SIGNALS)
                signal.signal(signal.SIGTERM, _handle_sigterm)
                signal.pthread_sigmask(signal.SIG_SETMASK, saved["mask"])
            else:
                detail = "post_merge_reap degraded_watchdog: off main thread, SIGTERM handler not installed"
                _record_failure(kind="returncode", detail=detail)
                print(f"post-merge-reap: {detail}", file=sys.stderr)
            result = _main_body(argv)
        finally:
            _finalize()
            if restore_needed:
                _restore_signals()
    except BaseException as exc:  # noqa: BLE001 -- re-raise after restoring
        first_error = first_error or exc
    finally:
        # This scope owns both blocks and the entire _finish_run call. A worker
        # can schedule a Python interrupt despite this thread's mask, even at
        # helper entry or between restoration steps. Repeat the idempotent pass
        # if it was interrupted; never delegate our restoration to _finish_run.
        if restore_needed and not restored:
            _restore_signals()
        if first_error is not None:
            raise first_error
    return result


if __name__ == "__main__":
    sys.exit(main())
